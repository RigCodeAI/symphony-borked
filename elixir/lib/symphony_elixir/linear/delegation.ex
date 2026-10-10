defmodule SymphonyElixir.Linear.Delegation do
  @moduledoc "Native delegation policy and service-owned Linear operations."

  alias SymphonyElixir.Linear.Client
  alias SymphonyElixir.Tracker.Issue
  alias SymphonyElixir.{WorkerOperation, WorkstreamRunner}

  @required ~w(organization_id team_id app_user_id oauth_client_id webhook_secret_env store_path workspace_root workstream_path agent_id rig_label)
  @optional ~w(workspaces codex_command reconcile_interval_ms worker_control token_env client_secret_env)

  @spec validate(map()) :: {:ok, map()} | {:error, atom()}
  def validate(config) when is_map(config) do
    workspaces = config["workspaces"] || %{}

    if valid_fields?(config) and valid_paths?(config) and valid_environment?(config) and
         valid_workspaces?(workspaces) and valid_optional?(config) do
      {:ok, Map.put(config, "workspaces", workspaces)}
    else
      {:error, :invalid_linear_delegation_config}
    end
  end

  def validate(_), do: {:error, :invalid_linear_delegation_config}

  @spec accepts?(map(), map()) :: boolean()
  def accepts?(event, config) do
    event.organization_id == config["organization_id"] and
      (event.kind == :issue_updated or
         (event.app_user_id == config["app_user_id"] and event.oauth_client_id == config["oauth_client_id"]))
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
    execution = [workspace: workspace, workspace_root: config["workspace_root"], issue_id: issue.id]
    execution = if config["codex_command"], do: Keyword.put(execution, :codex_command, config["codex_command"]), else: execution
    execution = if config["worker_control"], do: Keyword.put(execution, :worker_control, config["worker_control"]), else: execution

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

  defp qualify_worker_control(nil), do: {:error, :worker_execution_control_unavailable}
  defp qualify_worker_control(control), do: WorkerOperation.qualify(control)

  defp pin_agent_authentication(execution, %{authentication: %{reference: reference}}),
    do: Keyword.put(execution, :authentication_reference, reference)

  defp pin_agent_authentication(execution, _agent), do: execution

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
      else: [tracker_settings: %{endpoint: "https://api.linear.app/graphql", api_key: System.get_env(config["token_env"])}]
  end

  defp fetch_issue(id, config), do: Client.fetch_delegated_issue(id, client_opts(config, id))
  defp fetch_session(id, config, run_id), do: Client.fetch_agent_session(id, client_opts(config, run_id))

  defp acknowledge_session(task, config) do
    body = "Received delegated Rig work. Run #{task.run_id || "pending"}; publication is disabled."
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

  defp valid_optional?(config) do
    interval = config["reconcile_interval_ms"] || 5_000

    is_integer(interval) and interval >= 1_000 and (is_nil(config["codex_command"]) or is_binary(config["codex_command"])) and
      (is_nil(config["worker_control"]) or (is_nil(config["codex_command"]) and WorkerOperation.validate_config(config["worker_control"]) == :ok))
  end

  defp credential_environment(config) do
    # Until the shared runtime supports an explicit strip list, only names already
    # removed by its dynamic-tools-disabled launch are allowed.
    names = ~w(LINEAR_API_KEY LINEAR_API_TOKEN OAUTH_TOKEN)

    secret_env = config["client_secret_env"] || config["token_env"]

    if secret_env in names and config["webhook_secret_env"] in names and secret_env != config["webhook_secret_env"],
      do: :ok,
      else: {:error, :credential_environment_isolation_unavailable}
  end
end
