defmodule SymphonyElixir.Linear.Delegation do
  @moduledoc "Native delegation policy and service-owned Linear operations."

  alias SymphonyElixir.Linear.{AgentMapping, Client}
  alias SymphonyElixir.Tracker.Issue
  alias SymphonyElixir.{WorkerOperation, WorkstreamRunner}

  @required ~w(organization_id team_id app_user_id oauth_client_id webhook_secret_env store_path workspace_root workstream_path agent_id rig_label)
  @optional ~w(workspaces child_workspaces codex_command reconcile_interval_ms worker_control token_env client_secret_env cloud_agents secret_environment_names validation)

  @spec validate(map()) :: {:ok, map()} | {:error, atom()}
  def validate(config) when is_map(config) do
    workspaces = config["workspaces"] || %{}
    child_workspaces = config["child_workspaces"] || %{}

    if valid_fields?(config) and valid_paths?(config) and valid_environment?(config) and
         valid_workspaces?(workspaces) and valid_workspaces?(child_workspaces) and valid_optional?(config) and
         match?({:ok, _}, AgentMapping.validate(config)) do
      {:ok, config |> Map.put("workspaces", workspaces) |> Map.put("child_workspaces", child_workspaces)}
    else
      {:error, :invalid_linear_delegation_config}
    end
  end

  def validate(_), do: {:error, :invalid_linear_delegation_config}

  @spec accepts?(map(), map()) :: boolean()
  def accepts?(event, config) do
    case AgentMapping.routes(config) do
      {:ok, routes} ->
        Enum.any?(routes, fn route ->
          event.organization_id == route["organization_id"] and
            ((event.kind == :issue_updated and (is_nil(event[:verified_app_user_id]) or event[:verified_app_user_id] == route["app_user_id"])) or
               (event.app_user_id == route["app_user_id"] and event.oauth_client_id == route["oauth_client_id"]))
        end)

      _ ->
        false
    end
  end

  @spec eligible?(Issue.t(), map()) :: boolean()
  def eligible?(issue, config) do
    issue.team_id == config["team_id"] and issue.delegate_id == config["app_user_id"] and
      String.starts_with?(issue.identifier || "", "DEV-") and
      config["rig_label"] in issue.labels and issue.state in ["Backlog", "Todo", "In Progress"] and
      issue.state_type not in ["completed", "canceled"]
  end

  @spec inspect_event(map(), map(), keyword()) :: {:ok, Issue.t()} | {:error, atom()}
  def inspect_event(event, config, opts) do
    fetch = Keyword.get(opts, :linear_issue_fetcher, &fetch_issue/2)

    with :ok <- credential_environment(config),
         {:ok, %Issue{} = issue} <- fetch.(event.issue_id, config),
         true <- issue.id == event.issue_id and eligible?(issue, config),
         :ok <- inspect_session(event, config, opts) do
      {:ok, issue}
    else
      false -> {:error, :not_delegated_or_out_of_scope}
      {:error, :credential_environment_isolation_unavailable} = error -> error
      {:error, _} -> {:error, :linear_scope_check_failed}
      _ -> {:error, :linear_scope_check_failed}
    end
  end

  @spec prepare(Issue.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def prepare(issue, config, opts) do
    inputs = %{"task" => "Repository: RigCodeAI/rig\n#{issue.identifier}: #{issue.title}\n#{issue.description || ""}\nDo not publish, push, or merge this candidate."}
    workspace = config["workspaces"][issue.id]
    child_workspace = (config["child_workspaces"] || %{})[issue.id]

    execution = [
      workspace: workspace,
      workspace_root: config["workspace_root"],
      issue_id: issue.id,
      cloud_agent_id: config["agent_id"],
      secret_environment_names: AgentMapping.credential_environment_names(config) ++ ["FACTORY_LINEAR_SECRET_NAMES_JSON"]
    ]

    execution = if is_binary(child_workspace), do: Keyword.put(execution, :child_workspace, child_workspace), else: execution
    execution = if config["codex_command"], do: Keyword.put(execution, :codex_command, config["codex_command"]), else: execution
    execution = if config["worker_control"], do: Keyword.put(execution, :worker_control, config["worker_control"]), else: execution
    execution = add_validation_options(execution, config["validation"])

    with true <- is_binary(workspace),
         {:ok, definition, _workspace} <- WorkstreamRunner.prepare(config["workstream_path"], inputs, execution),
         true <- definition.name == "software-change",
         %{type: :agent, agent: agent} <- definition.stages[definition.entry],
         true <- agent == config["agent_id"] and Map.has_key?(definition.agents, agent),
         :ok <- qualify(definition, agent, config, opts) do
      execution = Keyword.put(execution, :definition_sha256, definition_digest(definition))
      execution = pin_agent_authentication(execution, definition.agents[agent])
      {:ok, %{path: config["workstream_path"], inputs: inputs, execution: execution, agent_id: agent}}
    else
      false -> {:error, :unknown_workstream_agent_or_workspace}
      {:error, _} = error -> error
      _ -> {:error, :invalid_delegation_entry}
    end
  end

  @spec definition_digest(map()) :: String.t()
  def definition_digest(definition), do: :crypto.hash(:sha256, :erlang.term_to_binary(definition)) |> Base.encode16(case: :lower)

  @spec qualify(map(), String.t(), map(), keyword()) :: :ok | {:error, term()}
  def qualify(definition, agent, config, opts) do
    readiness = Keyword.get(opts, :linear_readiness, &qualified_agent/2)
    control = Keyword.get(opts, :linear_execution_control, fn _definition, _agent -> qualify_worker_control(config["worker_control"]) end)
    with :ok <- credential_environment(config), :ok <- readiness.(definition, agent), do: control.(definition, agent)
  end

  @doc "Qualifies a prepared child against the task's pinned native route and parent selection."
  @spec qualified_child(map(), map(), map(), map(), keyword()) :: :ok | {:error, term()}
  def qualified_child(%{definition: child_definition} = child, %{definition: parent_definition} = parent, task, route, opts)
      when is_map(child_definition) and is_map(parent_definition) and is_map(task) and is_map(route) and is_list(opts) do
    with {:ok, agent_id} <- child_agent_id(child_definition, parent_definition, task, route),
         :ok <- child_workspace_matches(child, parent, task, route),
         :ok <- shared_credential_exclusions(child, parent),
         :ok <- execution_isolated(child, route),
         :ok <- qualify(child_definition, agent_id, route, opts) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :native_child_not_qualified}
    end
  rescue
    _ -> {:error, :native_child_not_qualified}
  end

  def qualified_child(_child, _parent, _task, _route, _opts), do: {:error, :native_child_not_qualified}

  defp qualify_worker_control(nil), do: {:error, :worker_execution_control_unavailable}
  defp qualify_worker_control(control), do: WorkerOperation.qualify(control)

  defp pin_agent_authentication(execution, %{authentication: %{reference: reference}}),
    do: Keyword.put(execution, :authentication_reference, reference)

  defp pin_agent_authentication(execution, _agent), do: execution

  @doc "Rejects an existing run whose pinned launch can inherit newly configured service secrets."
  @spec execution_isolated(map(), map()) :: :ok | {:error, atom()}
  def execution_isolated(run, config) do
    excluded = ~w(LINEAR_API_KEY LINEAR_API_TOKEN OAUTH_TOKEN) ++ Map.get(run.execution, :secret_environment_names, [])

    if Enum.all?(AgentMapping.credential_environment_names(config), &(&1 in excluded)),
      do: :ok,
      else: {:error, :credential_environment_isolation_unavailable}
  end

  @spec acknowledge(map(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def acknowledge(task, config, opts) do
    ack = Keyword.get(opts, :linear_acknowledger, &acknowledge_session/2)
    ack.(task, config)
  end

  @spec publish(map(), map(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def publish(task, activity, config, opts) do
    publisher =
      Keyword.get(opts, :linear_publisher, fn task, activity, config ->
        Client.publish_agent_activity(task.session_id, activity.id, activity.content, client_opts(config, task.issue_id))
      end)

    publisher.(task, activity, config)
  end

  @spec new_activity_id() :: String.t()
  def new_activity_id do
    <<a::32, b::16, _::4, c::12, _::2, d::14, e::48>> = :crypto.strong_rand_bytes(16)
    Enum.join([hex(a, 8), hex(b, 4), hex(0x4000 + c, 4), hex(0x8000 + d, 4), hex(e, 12)], "-")
  end

  defp hex(number, length), do: number |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(length, "0")

  defp inspect_session(%{kind: :issue_updated}, _config, _opts), do: :ok

  defp inspect_session(event, config, opts) do
    fetch = Keyword.get(opts, :linear_session_fetcher, fn id, config -> fetch_session(id, config, event.issue_id) end)

    case fetch.(event.session_id, config) do
      {:ok, %{"id" => id, "issueId" => issue_id, "appUser" => %{"id" => app}, "dismissedAt" => nil}}
      when id == event.session_id and issue_id == event.issue_id ->
        if app == config["app_user_id"], do: :ok, else: {:error, :wrong_session_agent}

      _ ->
        {:error, :session_not_active}
    end
  end

  defp client_opts(config, run_id) do
    if config["client_secret_env"],
      do: SymphonyElixir.Linear.OAuth.client_opts(config, run_id),
      else: [tracker_settings: %{endpoint: "https://api.linear.app/graphql", api_key: SymphonyElixir.Config.linear_token(config)}]
  end

  defp fetch_issue(id, config), do: Client.fetch_delegated_issue(id, client_opts(config, id))
  defp fetch_session(id, config, run_id), do: Client.fetch_agent_session(id, client_opts(config, run_id))

  defp acknowledge_session(task, config) do
    body = "Received delegated Rig work. Run #{task.run_id || "pending"}; publication is disabled."
    body = if task[:evidence_url], do: body <> "\n\nPrivate conversation and test evidence: #{task.evidence_url}", else: body
    Client.acknowledge_agent_session(task.session_id, task.activity_id, body, client_opts(config, task.issue_id))
  end

  defp qualified_agent(definition, agent) do
    module = SymphonyElixir.AgentReadiness

    if Code.ensure_loaded?(module) and function_exported?(module, :dispatch, 1),
      do: apply(module, :dispatch, [definition.agents[agent]]),
      else: {:error, :worker_readiness_unavailable}
  end

  defp valid_fields?(config) do
    Enum.all?(@required, &(is_binary(config[&1]) and config[&1] != "")) and
      Enum.all?(Map.keys(config), &(&1 in (@required ++ @optional))) and
      Enum.count(~w(token_env client_secret_env), &(is_binary(config[&1]) and config[&1] != "")) == 1
  end

  defp valid_paths?(config), do: Enum.all?(~w(store_path workspace_root workstream_path), &absolute?(config[&1]))
  defp absolute?(path), do: is_binary(path) and Path.type(path) == :absolute
  defp valid_environment?(config), do: Enum.all?([config["webhook_secret_env"], config["client_secret_env"] || config["token_env"]], &environment_name?/1)
  defp environment_name?(name), do: is_binary(name) and Regex.match?(~r/^[A-Z][A-Z0-9_]*$/, name)
  defp valid_workspaces?(workspaces), do: is_map(workspaces) and Enum.all?(workspaces, fn {id, path} -> is_binary(id) and absolute?(path) end)

  defp child_agent_id(child_definition, parent_definition, task, route) do
    agent_id = route["agent_id"]
    child_selection = child_definition[:agent_selection]
    parent_selection = parent_definition[:agent_selection]

    with true <- is_binary(agent_id) and task[:agent_id] == agent_id,
         true <- child_definition.name == "software-change",
         %{type: :agent, agent: ^agent_id} <- child_definition.stages[child_definition.entry],
         true <- Map.has_key?(child_definition.agents, agent_id),
         %{agent_id: ^agent_id} <- parent_selection,
         %{agent_id: ^agent_id} <- child_selection,
         true <- child_selection == parent_selection,
         {:ok, rebound_parent} <- AgentMapping.bind(parent_definition, agent_id),
         {:ok, rebound_child} <- AgentMapping.bind(child_definition, agent_id),
         true <- rebound_parent.agent_selection == parent_selection,
         true <- rebound_child.agent_selection == child_selection do
      {:ok, agent_id}
    else
      _ -> {:error, :native_child_agent_mismatch}
    end
  end

  defp child_workspace_matches(child, parent, task, route) do
    issue_id = task[:issue_id]
    configured = (route["child_workspaces"] || %{})[issue_id]
    pinned = get_in(parent, [:execution, :child_preparation, :workspace])
    workspace_root = route["workspace_root"]

    with true <- is_binary(issue_id) and is_binary(configured) and is_binary(pinned),
         true <- Path.expand(configured) == Path.expand(pinned),
         true <- Path.expand(child.workspace) == Path.expand(configured),
         true <- not workspaces_overlap?(child.workspace, parent.workspace),
         true <- Path.expand(get_in(parent, [:execution, :workspace_root])) == Path.expand(workspace_root),
         true <- Path.expand(get_in(child, [:execution, :workspace_root])) == Path.expand(workspace_root),
         {:ok, _workspace} <- WorkstreamRunner.validate_workspace(child.workspace, workspace_root, child.definition) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :child_workspace_unconfigured_or_mismatched}
    end
  end

  defp shared_credential_exclusions(child, parent) do
    child_names = get_in(child, [:execution, :secret_environment_names])
    parent_names = get_in(parent, [:execution, :secret_environment_names])

    if is_list(child_names) and is_list(parent_names) and Enum.sort(child_names) == Enum.sort(parent_names),
      do: :ok,
      else: {:error, :child_credential_exclusions_mismatch}
  end

  defp workspaces_overlap?(left, right) when is_binary(left) and is_binary(right) do
    left = Path.expand(left)
    right = Path.expand(right)
    left == right or within_path?(left, right) or within_path?(right, left)
  end

  defp workspaces_overlap?(_left, _right), do: true

  defp within_path?(path, root), do: String.starts_with?(path, String.trim_trailing(root, "/") <> "/")

  defp valid_optional?(config) do
    interval = config["reconcile_interval_ms"] || 5_000

    is_integer(interval) and interval >= 1_000 and (is_nil(config["codex_command"]) or is_binary(config["codex_command"])) and
      (is_nil(config["worker_control"]) or (is_nil(config["codex_command"]) and WorkerOperation.validate_config(config["worker_control"]) == :ok)) and
      (is_nil(config["secret_environment_names"]) or (is_list(config["secret_environment_names"]) and Enum.all?(config["secret_environment_names"], &environment_name?/1))) and
      valid_validation?(config["validation"])
  end

  defp valid_validation?(nil), do: true

  defp valid_validation?(config) when is_map(config) do
    Enum.sort(Map.keys(config)) == Enum.sort(~w(policy archive scratch candidates base)) and
      Enum.all?(~w(policy archive scratch candidates), &absolute?(config[&1])) and
      is_binary(config["base"]) and Regex.match?(~r/^[a-f0-9]{40}(?:[a-f0-9]{24})?$/, config["base"])
  end

  defp valid_validation?(_config), do: false

  defp add_validation_options(execution, nil), do: execution

  defp add_validation_options(execution, config) do
    Keyword.merge(execution,
      validation_policy: config["policy"],
      validation_archive: config["archive"],
      validation_scratch: config["scratch"],
      validation_candidates: config["candidates"],
      validation_base: config["base"]
    )
  end

  defp credential_environment(config) do
    if Enum.all?(AgentMapping.credential_environment_names(config), &environment_name?/1),
      do: :ok,
      else: {:error, :credential_environment_isolation_unavailable}
  end
end
