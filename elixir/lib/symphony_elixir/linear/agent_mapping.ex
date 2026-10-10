defmodule SymphonyElixir.Linear.AgentMapping do
  @moduledoc """
  Maps installed Linear cloud identities to pinned agent definitions.

  The coordinator owns persistence, authentication and writer reconciliation. This
  module performs no external operations and never chooses a replacement model.
  Workstream selection stays independent of cloud-agent selection.
  """

  alias SymphonyElixir.AgentReadiness
  alias SymphonyElixir.Linear.Webhook

  @cloud_names %{
    "default-cloud" => "DefaultCloud",
    "high-effort-cloud" => "HighEffortCloud",
    "daybreak-cloud" => "DaybreakCloud"
  }
  @review_names %{"default-review" => "DefaultReview", "daybreak-review" => "DaybreakReview"}
  @identity_fields ~w(app_user_id oauth_client_id agent_id webhook_secret_env)
  @credential_fields ~w(token_env client_secret_env)

  @doc "Validates optional additional cloud identities, leaving the existing default config intact."
  @spec validate(map()) :: {:ok, map()} | {:error, atom()}
  def validate(config) when is_map(config) do
    additional = Map.get(config, "cloud_agents", [])

    if nonempty?(config["organization_id"]) and is_list(additional) and length(additional) <= 2 do
      primary = Map.take(config, @identity_fields ++ @credential_fields)
      identities = [primary | additional]

      if Enum.all?(identities, &valid_identity?/1) and unique_identities?(identities) do
        {:ok, config}
      else
        {:error, :invalid_cloud_agent_mapping}
      end
    else
      {:error, :invalid_cloud_agent_mapping}
    end
  end

  def validate(_config), do: {:error, :invalid_cloud_agent_mapping}

  @doc "Returns per-app credentials and identity over the same backend/workstream configuration."
  @spec routes(map()) :: {:ok, [map()]} | {:error, atom()}
  def routes(config) do
    with {:ok, config} <- validate(config) do
      base = Map.drop(config, ["cloud_agents" | @credential_fields])
      primary = Map.take(config, @identity_fields ++ @credential_fields)
      {:ok, Enum.map([primary | Map.get(config, "cloud_agents", [])], &Map.merge(base, &1))}
    end
  end

  @doc "Selects an exact organization/app/client tuple from an already authenticated event."
  @spec select(map(), map()) :: {:ok, map()} | {:error, atom()}
  def select(event, config) when is_map(event) do
    with {:ok, routes} <- routes(config) do
      case Enum.find(routes, &matches?(event, &1)) do
        nil -> {:error, :unknown_cloud_agent_identity}
        route -> {:ok, route}
      end
    end
  end

  def select(_event, _config), do: {:error, :unknown_cloud_agent_identity}

  @doc "Authenticates each app with its own signing key before routing; keys are supplied by Config."
  @spec verify(term(), term(), map(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def verify(raw_body, headers, config, secrets, opts \\ []) do
    with {:ok, routes} <- routes(config),
         :ok <- signing_secrets_ready(routes, secrets) do
      routes
      |> Enum.map(fn route ->
        case Webhook.verify(raw_body, headers, secrets[route["webhook_secret_env"]], opts) do
          {:ok, event} -> authenticate_route(event, route)
          error -> error
        end
      end)
      |> verified_result()
    else
      {:error, _} = error -> error
    end
  end

  defp signing_secrets_ready(routes, secrets) do
    values = Enum.map(routes, &secrets[&1["webhook_secret_env"]])

    cond do
      not Enum.all?(values, &nonempty?/1) -> {:error, :signing_secret_unavailable}
      length(values) != length(Enum.uniq(values)) -> {:error, :ambiguous_signing_secrets}
      true -> :ok
    end
  end

  defp authenticate_route(%{kind: :issue_updated} = event, route) do
    if event.organization_id == route["organization_id"],
      do: {:ok, Map.put(event, :verified_app_user_id, route["app_user_id"])},
      else: {:error, :unknown_cloud_agent_identity}
  end

  defp authenticate_route(event, route) do
    if matches?(event, route),
      do: {:ok, Map.put(event, :verified_app_user_id, route["app_user_id"])},
      else: {:error, :unknown_cloud_agent_identity}
  end

  defp verified_result(results) do
    Enum.find(results, &match?({:ok, _}, &1)) ||
      Enum.find(results, &(&1 != {:error, :invalid_signature})) ||
      {:error, :invalid_signature}
  end

  @doc "Binds cloud stages to one explicitly selected definition, preserving explicit review stages."
  @spec bind(map(), String.t()) :: {:ok, map()} | {:error, atom()}
  def bind(definition, agent_id) do
    with {:ok, selected} <- cloud_agent(definition, agent_id),
         %{type: :agent, agent: previous_id} <- definition.stages[definition.entry],
         {:ok, _previous} <- cloud_agent(definition, previous_id) do
      stages =
        Map.new(definition.stages, fn {id, stage} ->
          if stage.type == :agent and stage.agent == previous_id,
            do: {id, %{stage | agent: agent_id}},
            else: {id, stage}
        end)

      selection = selection(agent_id, selected)
      {:ok, definition |> Map.put(:stages, stages) |> Map.put(:agent_selection, selection)}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_cloud_agent_entry}
    end
  end

  @doc "Resolves an explicitly configured internal reviewer, never through Linear delegation."
  @spec reviewer(map(), String.t()) :: {:ok, map()} | {:error, atom()}
  def reviewer(definition, agent_id) do
    case named_agent(definition, agent_id, @review_names) do
      {:ok, agent} -> {:ok, selection(agent_id, agent)}
      _ -> {:error, :unknown_internal_review_agent}
    end
  end

  @doc """
  Prepares an idempotent reassignment after the coordinator has reconciled every writer.

  The returned run and its owning Linear task must be committed atomically before
  redispatch. Executing, uncertain or incompletely stopped operations reject. No
  files or new definitions are loaded: reassignment uses the run's pinned resources.
  """
  @spec reassign(map(), String.t(), String.t()) :: {:ok, map()} | {:error, atom()}
  def reassign(run, agent_id, transition_id) when is_binary(transition_id) and transition_id != "" do
    history = Map.get(run, :agent_transitions, [])

    case Enum.find(history, &(&1.id == transition_id)) do
      %{to: %{agent_id: ^agent_id}} -> {:ok, run}
      nil -> prepare_reassignment(run, agent_id, transition_id, history)
      _ -> {:error, :agent_transition_conflict}
    end
  end

  def reassign(_run, _agent_id, _transition_id), do: {:error, :invalid_agent_transition}

  defp prepare_reassignment(run, agent_id, transition_id, history) do
    with :ok <- safe_to_reassign(run),
         {:ok, definition} <- bind(run.definition, agent_id),
         :ok <- AgentReadiness.dispatch(definition.agents[agent_id]) do
      previous_id = run.definition.stages[run.definition.entry].agent
      previous = selection(previous_id, run.definition.agents[previous_id])

      transition = %{
        id: transition_id,
        from: previous,
        to: definition.agent_selection,
        previous_session_id: run.session_id,
        previous_thread_id: run.thread_id,
        previous_cancellation: Map.get(run, :cancellation)
      }

      updated = run |> Map.put(:definition, definition) |> Map.put(:agent_transitions, history ++ [transition])
      updated = Map.put(updated, :execution, Map.put(run.execution, :authentication_reference, definition.agents[agent_id].authentication.reference))
      updated = Map.merge(updated, %{retry_operation_id: nil, session_id: nil, thread_id: nil})

      # A stopped attempt is retained in history; the next launch gets a new attempt.
      updated =
        if run.status == :stopped do
          if definition.stages[run.stage_id].type == :human_wait and is_map(run.pending_wait),
            do: Map.merge(updated, %{status: :waiting_for_answer, phase: :waiting_for_answer, cancellation: nil}),
            else: Map.merge(updated, %{status: :ready, phase: :queued, current_attempt_id: nil, cancellation: nil})
        else
          updated
        end

      {:ok, updated}
    end
  end

  defp safe_to_reassign(run) do
    cond do
      run.status not in [:ready, :waiting_for_answer, :stopped] -> {:error, :run_not_reassignable}
      not is_binary(run.stage_id) or not Map.has_key?(run.definition.stages, run.stage_id) -> {:error, :run_not_reassignable}
      not Enum.all?(run.operations, fn {_id, operation} -> quiescent?(operation) end) -> {:error, :previous_writer_not_reconciled}
      true -> :ok
    end
  end

  defp quiescent?(%{status: status} = operation) when status in [:completed, :not_applied],
    do: not Map.has_key?(operation, :reconciliation)

  defp quiescent?(%{status: :canceled, cancellation: %{termination: :terminated}} = operation),
    do: not Map.has_key?(operation, :reconciliation)

  defp quiescent?(_operation), do: false

  defp cloud_agent(definition, agent_id) do
    case named_agent(definition, agent_id, @cloud_names) do
      {:ok, agent} -> {:ok, agent}
      _ -> {:error, :unknown_cloud_agent_definition}
    end
  end

  defp named_agent(%{agents: agents}, agent_id, names) do
    case {names[agent_id], agents[agent_id]} do
      {name, %{name: name} = agent} when is_binary(name) -> {:ok, agent}
      _ -> {:error, :unknown_agent_definition}
    end
  end

  defp named_agent(_definition, _agent_id, _names), do: {:error, :unknown_agent_definition}

  defp selection(agent_id, agent) do
    %{
      agent_id: agent_id,
      name: agent.name,
      revision: agent.revision,
      requested: AgentReadiness.requested(agent),
      resources: Enum.map(agent.instructions ++ agent.skills, &Map.take(&1, [:path, :sha256]))
    }
  end

  defp valid_identity?(identity) when is_map(identity) do
    Enum.all?(@identity_fields, &nonempty?(identity[&1])) and
      Map.has_key?(@cloud_names, identity["agent_id"]) and
      Enum.all?(Map.keys(identity), &(&1 in (@identity_fields ++ @credential_fields))) and
      Enum.count(@credential_fields, &Map.has_key?(identity, &1)) == 1 and
      environment_name?(identity["webhook_secret_env"]) and
      Enum.all?(@credential_fields, fn key -> not Map.has_key?(identity, key) or environment_name?(identity[key]) end) and
      identity["webhook_secret_env"] != (identity["client_secret_env"] || identity["token_env"])
  end

  defp valid_identity?(_identity), do: false

  defp unique_identities?(identities) do
    credentials = Enum.map(identities, &(&1["client_secret_env"] || &1["token_env"]))
    signing = Enum.map(identities, & &1["webhook_secret_env"])

    length(credentials ++ signing) == length(Enum.uniq(credentials ++ signing)) and
      Enum.all?(~w(app_user_id oauth_client_id agent_id webhook_secret_env), fn key ->
        values = Enum.map(identities, & &1[key])
        length(values) == length(Enum.uniq(values))
      end)
  end

  defp matches?(event, route),
    do:
      event[:organization_id] == route["organization_id"] and
        event[:app_user_id] == route["app_user_id"] and event[:oauth_client_id] == route["oauth_client_id"]

  defp nonempty?(value), do: is_binary(value) and String.trim(value) != ""
  defp environment_name?(value), do: is_binary(value) and Regex.match?(~r/\A[A-Z][A-Z0-9_]*\z/, value)
end
