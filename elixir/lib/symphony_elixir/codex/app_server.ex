defmodule SymphonyElixir.Codex.AppServer do
  @moduledoc """
  Minimal client for the Codex app-server JSON-RPC 2.0 stream over stdio.
  """

  require Logger
  alias SymphonyElixir.{AgentReadiness, Codex.DynamicTool, Config, PathSafety, SSH, WorkerOperation}

  @initialize_id 1
  @thread_start_id 2
  @turn_start_id 3
  @port_line_bytes 1_048_576
  @max_stream_log_bytes 1_000
  @factory_tool_names ["factory_question", "factory_wait"]
  @disabled_dynamic_tool_secret_names [
    "LINEAR_API_KEY",
    "LINEAR_API_TOKEN",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "GITHUB_ENTERPRISE_TOKEN",
    "GH_ENTERPRISE_TOKEN",
    "GITLAB_PAT",
    "GITLAB_ACCESS_TOKEN",
    "GITLAB_TOKEN",
    "OAUTH_TOKEN",
    "JIRA_API_TOKEN",
    "ASANA_PAT",
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "CODEX_ACCESS_TOKEN"
  ]
  @type session :: %{
          port: port(),
          external_operation: map() | nil,
          worker_control: map() | nil,
          metadata: map(),
          approval_policy: String.t() | map(),
          auto_approve_requests: boolean(),
          thread_sandbox: String.t(),
          turn_sandbox_policy: map(),
          thread_id: String.t(),
          workspace: Path.t(),
          worker_host: String.t() | nil,
          dynamic_tool_binding: map(),
          dynamic_tools_enabled: boolean(),
          factory_tools_enabled: boolean(),
          requested_model: String.t() | nil,
          effective_model: String.t() | nil,
          reasoning_effort: String.t() | nil,
          read_timeout_ms: pos_integer() | nil,
          turn_timeout_ms: pos_integer() | nil,
          agent: map() | nil,
          runtime: map() | nil,
          configured_effort: String.t() | nil,
          saved_daybreak: boolean() | nil
        }
  @type invocation_settings :: %{
          model: String.t() | nil,
          reasoning_effort: String.t() | nil,
          read_timeout_ms: pos_integer() | nil,
          turn_timeout_ms: pos_integer() | nil
        }

  @spec run(Path.t(), String.t(), map(), keyword()) ::
          {:ok, map()} | {:waiting, map()} | {:error, term()} | {:uncertain, term()}
  def run(workspace, prompt, issue, opts \\ []) do
    with {:ok, session} <- start_session(workspace, opts) do
      result =
        try do
          {:returned, run_turn(session, prompt, issue, opts)}
        catch
          kind, reason ->
            {:raised, kind, reason, __STACKTRACE__}
        end

      case stop_session(session) do
        :ok -> return_session_result(result)
        {:error, :external_termination_unknown} -> {:uncertain, :external_termination_unknown}
      end
    end
  end

  defp return_session_result({:returned, result}), do: result
  defp return_session_result({:raised, kind, reason, stacktrace}), do: :erlang.raise(kind, reason, stacktrace)

  @spec start_session(Path.t(), keyword()) :: {:ok, session()} | {:error, term()} | {:uncertain, term()}
  def start_session(workspace, opts \\ []) do
    start_session_impl(workspace, opts, false)
  end

  defp start_session_impl(workspace, opts, qualification_probe) do
    worker_host = if opts[:worker_control], do: opts[:worker_control]["host"], else: Keyword.get(opts, :worker_host)
    agent = Keyword.get(opts, :agent)

    with :ok <- dispatchable_agent(agent, qualification_probe),
         :ok <- named_options(agent, opts),
         {:ok, expanded_workspace} <-
           validate_workspace_cwd(workspace, worker_host, Keyword.get(opts, :workspace_root)),
         {:ok, command} <- resolve_command(opts),
         {:ok, session_policies} <- session_policies(expanded_workspace, worker_host, opts),
         {:ok, initial_binding, dynamic_tools_enabled} <- dynamic_tool_binding(opts),
         {:ok, dynamic_tool_binding} <- exclude_environment_names(initial_binding, opts),
         {:ok, factory_tools_enabled} <- factory_tools_enabled(opts),
         {:ok, requested_model} <- requested_model(opts),
         {:ok, reasoning_effort} <- reasoning_effort(opts),
         {:ok, read_timeout_ms} <- timeout_setting(opts, :read_timeout_ms),
         {:ok, turn_timeout_ms} <- timeout_setting(opts, :turn_timeout_ms),
         {:ok, port, external_operation} <- start_operation_port(expanded_workspace, worker_host, dynamic_tool_binding, command, opts) do
      metadata = port_metadata(port, worker_host)

      invocation_settings =
        invocation_settings(requested_model, reasoning_effort, read_timeout_ms, turn_timeout_ms)
        |> Map.put(:agent, agent)
        |> Map.put(:authentication_reference, opts[:authentication_reference])
        |> Map.put(:factory_tools_enabled, factory_tools_enabled)

      case safe_session_start(fn ->
             do_start_session(
               port,
               expanded_workspace,
               session_policies,
               dynamic_tool_binding,
               invocation_settings
             )
           end) do
        {:ok, %{thread_id: thread_id, effective_model: effective_model} = started} ->
          {:ok,
           %{
             port: port,
             external_operation: external_operation,
             worker_control: opts[:worker_control],
             metadata: metadata,
             approval_policy: session_policies.approval_policy,
             auto_approve_requests: session_policies.approval_policy == "never" and not factory_tools_enabled,
             thread_sandbox: session_policies.thread_sandbox,
             turn_sandbox_policy: session_policies.turn_sandbox_policy,
             thread_id: thread_id,
             workspace: expanded_workspace,
             worker_host: worker_host,
             dynamic_tool_binding: dynamic_tool_binding,
             dynamic_tools_enabled: dynamic_tools_enabled,
             factory_tools_enabled: factory_tools_enabled,
             requested_model: requested_model,
             effective_model: effective_model,
             reasoning_effort: reasoning_effort,
             read_timeout_ms: read_timeout_ms,
             turn_timeout_ms: turn_timeout_ms,
             agent: agent,
             runtime: Map.get(started, :runtime),
             configured_effort: Map.get(started, :configured_effort),
             saved_daybreak: Map.get(started, :saved_daybreak)
           }}

        {:error, reason} ->
          stop_port(port)

          case stop_external_operation(opts[:worker_control], external_operation) do
            :ok -> {:error, reason}
            {:error, :external_termination_unknown} -> {:uncertain, :external_termination_unknown}
          end
      end
    end
  end

  defp safe_session_start(start) do
    start.()
  rescue
    _ -> {:error, :session_initialization_failed}
  catch
    _, _ -> {:error, :session_initialization_failed}
  end

  @spec run_turn(session(), String.t(), map(), keyword()) ::
          {:ok, map()} | {:waiting, map()} | {:error, term()}
  def run_turn(
        %{
          port: port,
          metadata: metadata,
          approval_policy: approval_policy,
          auto_approve_requests: auto_approve_requests,
          turn_sandbox_policy: turn_sandbox_policy,
          thread_id: thread_id,
          workspace: workspace,
          dynamic_tool_binding: dynamic_tool_binding,
          dynamic_tools_enabled: dynamic_tools_enabled,
          factory_tools_enabled: factory_tools_enabled,
          effective_model: effective_model,
          reasoning_effort: session_reasoning_effort,
          read_timeout_ms: read_timeout_ms,
          turn_timeout_ms: turn_timeout_ms
        } = session,
        prompt,
        issue,
        opts \\ []
      ) do
    on_message = Keyword.get(opts, :on_message, &default_on_message/1)

    tool_executor = fn tool, arguments ->
      cond do
        tool == "__factory_native_request_user_input__" and not factory_tools_enabled ->
          :unavailable

        factory_tools_enabled and tool in @factory_tool_names ->
          execute_factory_tool(tool, arguments, opts)

        factory_tools_enabled and tool == "__factory_native_request_user_input__" ->
          execute_native_user_input(arguments, opts)

        dynamic_tools_enabled ->
          Keyword.get(opts, :tool_executor, fn name, args ->
            DynamicTool.execute(name, args, dynamic_tool_binding, issue: issue)
          end).(tool, arguments)

        true ->
          %{"success" => false, "error" => "dynamic tools are disabled"}
      end
    end

    reasoning_effort = Keyword.get(opts, :reasoning_effort, session_reasoning_effort)

    agent = Map.get(session, :agent)

    case start_selected_turn(agent, session_reasoning_effort, reasoning_effort, fn ->
           start_turn(
             port,
             thread_id,
             prompt,
             issue,
             workspace,
             approval_policy,
             turn_sandbox_policy,
             %{reasoning_effort: reasoning_effort, read_timeout_ms: read_timeout_ms, agent: agent}
           )
         end) do
      {:ok, turn_id} ->
        session_id = "#{thread_id}-#{turn_id}"
        Logger.info("Codex session started for #{issue_context(issue)} session_id=#{session_id}")

        emit_message(
          on_message,
          :session_started,
          %{
            session_id: session_id,
            thread_id: thread_id,
            turn_id: turn_id
          },
          metadata
        )

        case await_turn_completion(
               port,
               on_message,
               tool_executor,
               auto_approve_requests,
               if(opts[:absolute_turn_timeout], do: {:deadline, System.monotonic_time(:millisecond) + turn_timeout_ms}, else: turn_timeout_ms),
               factory_tools_enabled
             ) do
          {:ok, result} ->
            Logger.info("Codex session completed for #{issue_context(issue)} session_id=#{session_id}")

            run_result = %{
              result: result,
              session_id: session_id,
              thread_id: thread_id,
              turn_id: turn_id
            }

            run_result = put_if_present(run_result, :model, effective_model)
            run_result = put_if_present(run_result, :reasoning_effort, reasoning_effort)

            {:ok, run_result}

          {:waiting, wait_id} ->
            {:waiting, %{wait_id: wait_id, thread_id: thread_id, session_id: session_id}}

          {:error, reason} ->
            Logger.warning("Codex session ended with error for #{issue_context(issue)} session_id=#{session_id}: #{inspect(reason)}")

            emit_message(
              on_message,
              :turn_ended_with_error,
              %{
                session_id: session_id,
                reason: reason
              },
              metadata
            )

            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("Codex session failed for #{issue_context(issue)}: #{inspect(reason)}")
        emit_message(on_message, :startup_failed, %{reason: reason}, metadata)
        {:error, reason}
    end
  end

  @doc """
  Attempts one bounded operator qualification turn, separately from workstream dispatch.
  Daybreak probes may attempt an advertised program but never qualify it from a saved toggle.
  """
  @spec qualify(Path.t(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def qualify(workspace, prompt, issue, opts) do
    with {:ok, session} <- start_session_impl(workspace, Keyword.merge(opts, dynamic_tools: false, read_timeout_ms: 30_000, turn_timeout_ms: 90_000), true) do
      tag = make_ref()

      callback = fn event ->
        case event do
          %{payload: %{"method" => "model/verification", "params" => params}} ->
            send(self(), {tag, %{verification: Map.take(params, ["threadId", "turnId", "verifications"])}})

          %{payload: %{"method" => "item/completed", "params" => %{"item" => %{"type" => "agentMessage", "text" => text}}}} when is_binary(text) ->
            send(self(), {tag, %{assistant_output: String.slice(text, 0, 1024)}})

          _ ->
            :ok
        end
      end

      try do
        result = run_turn(session, prompt, issue, on_message: callback, absolute_turn_timeout: true)

        {:ok,
         %{
           runtime: session.runtime,
           configured: %{model: session.effective_model, reasoning_effort: session.configured_effort, daybreak: session.saved_daybreak},
           effective: %{model: nil, reasoning_effort: nil, cyber_access_program: nil},
           turn: qualification_result(result),
           observations: qualification_messages(tag, [])
         }}
      after
        stop_session(session)
      end
    else
      {:error, {:agent_not_ready, reason, runtime}} ->
        {:ok,
         %{
           runtime: runtime,
           configured: nil,
           effective: %{model: nil, reasoning_effort: nil, cyber_access_program: nil},
           turn: %{status: :blocked, reason: qualification_error(reason)},
           observations: []
         }}

      {:error, _} = error ->
        error
    end
  end

  defp qualification_result({:ok, result}), do: Map.take(result, [:thread_id, :turn_id, :session_id]) |> Map.put(:status, :completed)
  defp qualification_result({:error, reason}), do: %{status: :blocked, reason: qualification_error(reason)}

  @doc "Returns a bounded diagnostic code without serializing runtime messages or credential values."
  @spec qualification_error(term()) :: String.t()
  def qualification_error(reason) when is_atom(reason), do: Atom.to_string(reason)

  def qualification_error({tag, reason}) when is_atom(tag) and is_atom(reason),
    do: "#{tag}:#{reason}"

  def qualification_error(reason) when is_tuple(reason) and tuple_size(reason) > 0 do
    case elem(reason, 0) do
      tag when is_atom(tag) -> Atom.to_string(tag)
      _ -> "runtime_request_rejected"
    end
  end

  def qualification_error(_reason), do: "runtime_request_rejected"

  defp qualification_messages(tag, acc) do
    receive do
      {^tag, message} -> qualification_messages(tag, [message | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp dispatchable_agent(nil, true), do: {:error, :qualification_agent_required}
  defp dispatchable_agent(nil, false), do: :ok
  defp dispatchable_agent(agent, true), do: AgentReadiness.validate_definition(agent)
  defp dispatchable_agent(agent, false), do: AgentReadiness.dispatch(agent)

  defp named_options(nil, _opts), do: :ok

  defp named_options(agent, opts) do
    cond do
      opts[:model] != agent.model or opts[:reasoning_effort] != agent.reasoning_effort -> {:error, :agent_settings_override}
      agent.authentication.reference != "inherited" and opts[:authentication_reference] != agent.authentication.reference -> {:error, :agent_authentication_reference_mismatch}
      true -> :ok
    end
  end

  defp start_selected_turn(nil, _session_effort, _effort, start), do: start.()
  defp start_selected_turn(_agent, effort, effort, start), do: start.()
  defp start_selected_turn(_agent, _session_effort, _effort, _start), do: {:error, :agent_settings_override}

  defp validate_named_thread(nil, _model, _effort, _daybreak), do: :ok

  defp validate_named_thread(agent, model, effort, daybreak) do
    cond do
      model != agent.model -> {:error, :effective_model_unavailable}
      effort != agent.reasoning_effort -> {:error, :configured_effort_mismatch}
      daybreak != agent.daybreak -> {:error, :saved_daybreak_mismatch}
      true -> :ok
    end
  end

  defp named_runtime(_port, %{agent: nil}), do: {:ok, nil}

  defp named_runtime(port, settings) do
    with {:ok, account} <- request(port, 101, "account/read", %{"refreshToken" => false}, settings),
         {:ok, models} <- model_catalog(port, settings, nil, [], 0),
         {:ok, limits} <- request(port, 103, "account/rateLimits/read", %{}, settings),
         {:ok, config} <- request(port, 104, "config/read", %{"includeLayers" => true}, settings) do
      runtime = %{
        authentication_reference: settings.authentication_reference,
        account: %{type: get_in(account, ["account", "type"]), plan_type: get_in(account, ["account", "planType"])},
        models: models,
        limits: Map.take(limits, ["ordinaryUsageAllowed", "rateLimits", "rateLimitsByLimitId"]),
        configuration: %{
          values: Map.take(Map.get(config, "config", %{}), ["model", "model_provider", "model_reasoning_effort", "forced_login_method"]),
          layers: Enum.map(Map.get(config, "layers") || [], &Map.take(&1, ["name", "version"]))
        }
      }

      case AgentReadiness.check(settings.agent, runtime) do
        :ok -> {:ok, runtime}
        {:error, reason} -> {:error, {:agent_not_ready, reason, runtime}}
      end
    end
  end

  defp model_catalog(_port, _settings, _cursor, _models, 8), do: {:error, :model_catalog_limit}

  defp model_catalog(port, settings, cursor, models, page) do
    params = %{"includeHidden" => true, "limit" => 100}
    params = if cursor, do: Map.put(params, "cursor", cursor), else: params

    with {:ok, result} <- request(port, 102, "model/list", params, settings),
         data when is_list(data) <- result["data"] do
      models = models ++ Enum.map(data, &Map.take(&1, ["id", "model", "supportedReasoningEfforts", "availableAccessPrograms"]))

      case result["nextCursor"] do
        nil -> {:ok, models}
        next when is_binary(next) -> model_catalog(port, settings, next, models, page + 1)
        _ -> {:error, :invalid_model_catalog_cursor}
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_model_catalog}
    end
  end

  defp response_timeout(%{agent: nil, read_timeout_ms: timeout_ms}), do: timeout_ms

  defp response_timeout(%{read_timeout_ms: timeout_ms}) do
    {:deadline, System.monotonic_time(:millisecond) + (timeout_ms || Config.settings!().codex.read_timeout_ms)}
  end

  defp request(port, id, method, params, settings) do
    send_message(port, %{"id" => id, "method" => method, "params" => params})
    await_response(port, id, response_timeout(settings))
  end

  @spec stop_session(session()) :: :ok | {:error, :external_termination_unknown}
  def stop_session(%{port: port} = session) when is_port(port) do
    stop_port(port)
    stop_external_operation(Map.get(session, :worker_control), Map.get(session, :external_operation))
  end

  defp stop_external_operation(_control, nil), do: :ok

  defp stop_external_operation(control, identity) do
    case WorkerOperation.stop(control, identity) do
      :terminated -> :ok
      :unknown -> {:error, :external_termination_unknown}
    end
  end

  defp resolve_command(opts) do
    command = Keyword.get_lazy(opts, :command, fn -> Config.settings!().codex.command end)

    if is_binary(command) and String.trim(command) != "" do
      {:ok, command}
    else
      {:error, {:invalid_codex_command, command}}
    end
  end

  defp dynamic_tool_binding(opts) do
    case Keyword.get(opts, :dynamic_tools, true) do
      true ->
        {:ok, DynamicTool.bind(), true}

      false ->
        {:ok,
         %{
           tool_specs: [],
           secret_environment_names: @disabled_dynamic_tool_secret_names
         }, false}

      value ->
        {:error, {:invalid_dynamic_tools_option, value}}
    end
  end

  defp factory_tools_enabled(opts) do
    case Keyword.get(opts, :factory_tools, false) do
      false ->
        {:ok, false}

      true ->
        if is_function(Keyword.get(opts, :on_question), 1) and is_function(Keyword.get(opts, :on_wait), 1) do
          {:ok, true}
        else
          {:error, :factory_tools_require_question_and_wait_callbacks}
        end

      value ->
        {:error, {:invalid_factory_tools_option, value}}
    end
  end

  defp factory_tool_specs(false), do: []

  defp factory_tool_specs(true) do
    [
      %{
        "type" => "function",
        "name" => "factory_question",
        "description" => "Record a clarification question for the human while you continue independent work; the service publishes it in Linear.",
        "inputSchema" => %{
          "type" => "object",
          "properties" => %{"prompt" => %{"type" => "string", "description" => "The specific information needed to continue."}},
          "required" => ["prompt"],
          "additionalProperties" => false
        }
      },
      %{
        "type" => "function",
        "name" => "factory_wait",
        "description" => "Wait at an input boundary for the answer to a factory_question or a pending clarification from prior continuation context.",
        "inputSchema" => %{
          "type" => "object",
          "properties" => %{"question_id" => %{"type" => "string", "description" => "An id returned by factory_question or listed under pending_clarifications in prior continuation context."}},
          "required" => ["question_id"],
          "additionalProperties" => false
        }
      }
    ]
  end

  defp execute_factory_tool("factory_question", arguments, opts) do
    prompt = get_argument(arguments, "prompt")

    if is_binary(prompt) and String.trim(prompt) != "" do
      case invoke_callback(opts[:on_question], [%{prompt: prompt}], :question_callback_failed) do
        {:ok, %{id: id}} when is_binary(id) ->
          output = "Clarification question recorded with id #{id}; the service will publish it in Linear. Continue independent work, then call factory_wait with this id at the input boundary."
          success_tool_result(output)

        {:ok, %{"id" => id}} when is_binary(id) ->
          output = "Clarification question recorded with id #{id}; the service will publish it in Linear. Continue independent work, then call factory_wait with this id at the input boundary."
          success_tool_result(output)

        {:error, reason} ->
          {:error, {:question_callback_failed, reason}}

        other ->
          {:error, {:invalid_question_callback_result, other}}
      end
    else
      {:error, :invalid_factory_question_prompt}
    end
  end

  defp execute_factory_tool("factory_wait", arguments, opts) do
    id = get_argument(arguments, "question_id")

    if is_binary(id) and String.trim(id) != "" do
      case invoke_callback(opts[:on_wait], [id], :wait_callback_failed) do
        {:answered, %{body: body}} when is_binary(body) ->
          success_tool_result("Human clarification: " <> body)

        :wait ->
          {:stop, id}

        {:error, reason} ->
          {:error, {:wait_callback_failed, reason}}

        other ->
          {:error, {:invalid_wait_callback_result, other}}
      end
    else
      {:error, :invalid_factory_question_id}
    end
  end

  defp execute_native_user_input(%{"questions" => [question]}, opts) when is_map(question) do
    question_id = get_argument(question, "id")

    cond do
      get_argument(question, "isSecret") == true ->
        {:error, :unsupported_sensitive_input}

      native_approval_question?(question) ->
        {:error, :unsupported_native_approval}

      not is_binary(question_id) or not is_binary(get_argument(question, "question")) ->
        {:error, :unsupported_native_input}

      true ->
        prompt = native_question_prompt(question)

        case invoke_callback(opts[:on_question], [%{prompt: prompt}], :question_callback_failed) do
          {:ok, %{id: service_id}} when is_binary(service_id) ->
            answer_native_question(service_id, question_id, opts)

          {:ok, %{"id" => service_id}} when is_binary(service_id) ->
            answer_native_question(service_id, question_id, opts)

          {:error, reason} ->
            {:error, {:question_callback_failed, reason}}

          other ->
            {:error, {:invalid_question_callback_result, other}}
        end
    end
  end

  defp execute_native_user_input(%{"questions" => questions}, _opts) when is_list(questions) do
    cond do
      Enum.any?(questions, &(get_argument(&1, "isSecret") == true)) -> {:error, :unsupported_sensitive_input}
      Enum.any?(questions, &native_approval_question?/1) -> {:error, :unsupported_native_approval}
      true -> {:error, :unsupported_native_input}
    end
  end

  defp execute_native_user_input(_params, _opts), do: {:error, :unsupported_native_input}

  defp native_approval_question?(question) when is_map(question) do
    id = get_argument(question, "id") || ""
    header = get_argument(question, "header") || ""
    prompt = get_argument(question, "question") || ""
    normalized = String.downcase(header <> " " <> prompt)
    options = get_argument(question, "options") || []
    labels = Enum.map_join(options, " ", fn option -> get_argument(option, "label") || "" end) |> String.downcase()

    String.starts_with?(id, "mcp_tool_call_approval_") or
      String.contains?(String.downcase(header), ["approve", "approval", "permission"]) or
      String.contains?(normalized, ["allow this action", "request approval", "permission to", "approve this"]) or
      (String.contains?(labels, "deny") and
         (String.contains?(labels, "approve") or String.contains?(labels, "allow")))
  end

  defp native_approval_question?(_question), do: false

  defp answer_native_question(service_id, native_question_id, opts) do
    case invoke_callback(opts[:on_wait], [service_id], :wait_callback_failed) do
      {:answered, %{body: body}} when is_binary(body) ->
        {:native_answer, %{"answers" => %{native_question_id => %{"answers" => [body]}}}}

      :wait ->
        {:stop, service_id}

      {:error, reason} ->
        {:error, {:wait_callback_failed, reason}}

      other ->
        {:error, {:invalid_wait_callback_result, other}}
    end
  end

  defp native_question_prompt(question) do
    header = get_argument(question, "header")
    text = get_argument(question, "question")
    options = get_argument(question, "options")

    option_text =
      case options do
        values when is_list(values) and values != [] ->
          labels = Enum.map_join(values, ", ", fn option -> get_argument(option, "label") || "" end)
          "\nChoices offered by Codex: " <> labels

        _ ->
          ""
      end

    base =
      [header, text]
      |> Enum.reject(&(not is_binary(&1) or String.trim(&1) == ""))
      |> Enum.join("\n")

    base <> option_text
  end

  defp invoke_callback(callback, arguments, failure) when is_function(callback) do
    apply(callback, arguments)
  rescue
    _ -> {:error, failure}
  catch
    _, _ -> {:error, failure}
  end

  defp invoke_callback(_callback, _arguments, failure), do: {:error, failure}

  defp success_tool_result(output) do
    %{"success" => true, "output" => output, "contentItems" => dynamic_tool_content_items(output)}
  end

  defp get_argument(map, key) when is_map(map), do: Map.get(map, key)
  defp get_argument(_map, _key), do: nil

  # Additional names are exclusions only; values are never forwarded.
  defp exclude_environment_names(binding, opts) do
    names = Keyword.get(opts, :secret_environment_names, [])

    if is_list(names) and valid_environment_names(names) == names do
      {:ok, Map.update!(binding, :secret_environment_names, &Enum.uniq(&1 ++ names))}
    else
      {:error, :invalid_secret_environment_names}
    end
  end

  defp requested_model(opts) do
    case Keyword.get(opts, :model) do
      nil -> {:ok, nil}
      model when is_binary(model) and byte_size(model) > 0 -> {:ok, model}
      value -> {:error, {:invalid_codex_model, value}}
    end
  end

  defp invocation_settings(model, reasoning_effort, read_timeout_ms, turn_timeout_ms) do
    %{
      model: model,
      reasoning_effort: reasoning_effort,
      read_timeout_ms: read_timeout_ms,
      turn_timeout_ms: turn_timeout_ms
    }
  end

  defp returned_thread_model(response_payload, thread_payload) do
    returned_model(response_payload) || returned_model(thread_payload)
  end

  defp returned_model(%{"model" => model}) when is_binary(model), do: model
  defp returned_model(%{"model" => %{"id" => model}}) when is_binary(model), do: model
  defp returned_model(_payload), do: nil

  defp reasoning_effort(opts) do
    case Keyword.get(opts, :reasoning_effort) do
      nil -> {:ok, nil}
      effort when is_binary(effort) and byte_size(effort) > 0 -> {:ok, effort}
      value -> {:error, {:invalid_reasoning_effort, value}}
    end
  end

  defp timeout_setting(opts, option_name) do
    case Keyword.fetch(opts, option_name) do
      :error -> {:ok, nil}
      {:ok, timeout} when is_integer(timeout) and timeout > 0 -> {:ok, timeout}
      {:ok, timeout} -> {:error, {:invalid_timeout, option_name, timeout}}
    end
  end

  defp validate_runtime_settings(%{
         approval_policy: approval_policy,
         thread_sandbox: thread_sandbox,
         turn_sandbox_policy: turn_sandbox_policy
       })
       when (is_binary(approval_policy) or is_map(approval_policy)) and is_binary(thread_sandbox) and
              is_map(turn_sandbox_policy) do
    {:ok,
     %{
       approval_policy: approval_policy,
       thread_sandbox: thread_sandbox,
       turn_sandbox_policy: turn_sandbox_policy
     }}
  end

  defp validate_runtime_settings(settings), do: {:error, {:invalid_runtime_settings, settings}}

  defp validate_workspace_cwd(workspace, nil, workspace_root) when is_binary(workspace) do
    expanded_workspace = Path.expand(workspace)

    expanded_root_result =
      case workspace_root do
        nil -> {:ok, Config.local_workspace_root()}
        root when is_binary(root) and root != "" -> {:ok, Path.expand(root)}
        other -> {:error, {:invalid_workspace_root, other}}
      end

    with {:ok, expanded_root} <- expanded_root_result,
         {:ok, canonical_workspace} <- PathSafety.canonicalize(expanded_workspace),
         {:ok, canonical_root} <- PathSafety.canonicalize(expanded_root) do
      canonical_root_prefix = canonical_root <> "/"
      expanded_root_prefix = expanded_root <> "/"

      cond do
        canonical_workspace == canonical_root ->
          {:error, {:invalid_workspace_cwd, :workspace_root, canonical_workspace}}

        String.starts_with?(canonical_workspace <> "/", canonical_root_prefix) ->
          {:ok, canonical_workspace}

        String.starts_with?(expanded_workspace <> "/", expanded_root_prefix) ->
          {:error, {:invalid_workspace_cwd, :symlink_escape, expanded_workspace, canonical_root}}

        true ->
          {:error, {:invalid_workspace_cwd, :outside_workspace_root, canonical_workspace, canonical_root}}
      end
    else
      {:error, {:path_canonicalize_failed, path, reason}} ->
        {:error, {:invalid_workspace_cwd, :path_unreadable, path, reason}}
    end
  end

  defp validate_workspace_cwd(workspace, worker_host, _workspace_root)
       when is_binary(workspace) and is_binary(worker_host) do
    cond do
      String.trim(workspace) == "" ->
        {:error, {:invalid_workspace_cwd, :empty_remote_workspace, worker_host}}

      String.contains?(workspace, ["\n", "\r", <<0>>]) ->
        {:error, {:invalid_workspace_cwd, :invalid_remote_workspace, worker_host, workspace}}

      true ->
        {:ok, workspace}
    end
  end

  defp start_operation_port(workspace, worker_host, binding, command, opts) do
    case opts[:worker_control] do
      nil ->
        case start_port(workspace, worker_host, binding, command) do
          {:ok, port} -> {:ok, port, nil}
          error -> error
        end

      control ->
        WorkerOperation.start(workspace, opts[:operation_id], control, opts[:on_process_start])
    end
  end

  defp start_port(workspace, nil, dynamic_tool_binding, command) do
    executable = System.find_executable("bash")

    if is_nil(executable) do
      {:error, :bash_not_found}
    else
      port =
        Port.open(
          {:spawn_executable, String.to_charlist(executable)},
          [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            args: [
              ~c"-lc",
              String.to_charlist(local_launch_command(dynamic_tool_binding, command))
            ],
            cd: String.to_charlist(workspace),
            env: tracker_secret_port_env(dynamic_tool_binding),
            line: @port_line_bytes
          ]
        )

      {:ok, port}
    end
  end

  defp start_port(workspace, worker_host, dynamic_tool_binding, command)
       when is_binary(worker_host) do
    remote_command = remote_launch_command(workspace, dynamic_tool_binding, command)
    SSH.start_port(worker_host, remote_command, line: @port_line_bytes)
  end

  defp local_launch_command(dynamic_tool_binding, command) do
    [
      tracker_secret_unset_command(dynamic_tool_binding),
      "exec #{command}"
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" && ")
  end

  defp remote_launch_command(workspace, dynamic_tool_binding, command) when is_binary(workspace) do
    [
      "cd #{shell_escape(workspace)}",
      tracker_secret_unset_command(dynamic_tool_binding),
      "exec #{command}"
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" && ")
  end

  defp tracker_secret_port_env(dynamic_tool_binding) do
    dynamic_tool_binding.secret_environment_names
    |> valid_environment_names()
    |> Enum.map(fn name -> {String.to_charlist(name), false} end)
  end

  defp tracker_secret_unset_command(dynamic_tool_binding) do
    case dynamic_tool_binding.secret_environment_names |> valid_environment_names() do
      [] -> nil
      names -> "unset " <> Enum.join(names, " ")
    end
  end

  defp valid_environment_names(names) do
    Enum.filter(names, fn name ->
      is_binary(name) and String.match?(name, ~r/^[A-Za-z_][A-Za-z0-9_]*$/)
    end)
  end

  defp port_metadata(port, worker_host) when is_port(port) do
    base_metadata =
      case :erlang.port_info(port, :os_pid) do
        {:os_pid, os_pid} ->
          %{codex_app_server_pid: to_string(os_pid)}

        _ ->
          %{}
      end

    case worker_host do
      host when is_binary(host) -> Map.put(base_metadata, :worker_host, host)
      _ -> base_metadata
    end
  end

  defp send_initialize(port, read_timeout_ms) do
    payload = %{
      "method" => "initialize",
      "id" => @initialize_id,
      "params" => %{
        "capabilities" => %{
          "experimentalApi" => true
        },
        "clientInfo" => %{
          "name" => "symphony-orchestrator",
          "title" => "Symphony Orchestrator",
          "version" => "0.1.0"
        }
      }
    }

    send_message(port, payload)

    with {:ok, response} <- await_response(port, @initialize_id, read_timeout_ms) do
      send_message(port, %{"method" => "initialized", "params" => %{}})
      {:ok, response}
    end
  end

  defp session_policies(workspace, nil, opts) do
    case Keyword.fetch(opts, :runtime_settings) do
      {:ok, runtime_settings} -> validate_runtime_settings(runtime_settings)
      :error -> Config.codex_runtime_settings(workspace)
    end
  end

  defp session_policies(workspace, worker_host, opts) when is_binary(worker_host) do
    case Keyword.fetch(opts, :runtime_settings) do
      {:ok, runtime_settings} -> validate_runtime_settings(runtime_settings)
      :error -> Config.codex_runtime_settings(workspace, remote: true)
    end
  end

  defp do_start_session(port, workspace, session_policies, dynamic_tool_binding, settings) do
    with {:ok, initialize_info} <- send_initialize(port, response_timeout(settings)),
         {:ok, runtime} <- named_runtime(port, settings),
         {:ok, started} <- start_thread(port, workspace, session_policies, dynamic_tool_binding, settings) do
      runtime = if runtime, do: Map.put(runtime, :server, Map.take(initialize_info, ["userAgent", "platformOs", "platformFamily"])), else: nil
      {:ok, Map.put(started, :runtime, runtime)}
    end
  end

  defp start_thread(port, workspace, session_policies, dynamic_tool_binding, settings) do
    requested_model = settings.model

    params = %{
      "approvalPolicy" => session_policies.approval_policy,
      "sandbox" => session_policies.thread_sandbox,
      "cwd" => workspace,
      "dynamicTools" => dynamic_tool_binding.tool_specs ++ factory_tool_specs(settings.factory_tools_enabled)
    }

    params =
      if is_binary(requested_model), do: Map.put(params, "model", requested_model), else: params

    params =
      if settings.agent do
        params
        |> Map.put("allowProviderModelFallback", false)
        |> Map.put("daybreakEnabled", settings.agent.daybreak)
        |> Map.put("config", %{"model_reasoning_effort" => settings.agent.reasoning_effort})
      else
        params
      end

    send_message(port, %{
      "method" => "thread/start",
      "id" => @thread_start_id,
      "params" => params
    })

    case await_response(port, @thread_start_id, response_timeout(settings)) do
      {:ok, response_payload} ->
        started_thread_response(response_payload, requested_model, settings.agent)

      other ->
        other
    end
  end

  defp started_thread_response(%{"thread" => %{"id" => thread_id} = thread_payload} = response_payload, requested_model, agent) do
    observed_model = returned_thread_model(response_payload, thread_payload)
    effective_model = if agent, do: observed_model, else: observed_model || requested_model
    effort = response_payload["reasoningEffort"] || thread_payload["reasoningEffort"]
    saved_daybreak = Map.get(thread_payload, "daybreakEnabled")

    with :ok <- validate_returned_model(requested_model, effective_model),
         :ok <- validate_named_thread(agent, effective_model, effort, saved_daybreak) do
      {:ok, %{thread_id: thread_id, effective_model: effective_model, configured_effort: effort, saved_daybreak: saved_daybreak}}
    end
  end

  defp started_thread_response(%{"thread" => thread_payload}, _requested_model, _agent) do
    {:error, {:invalid_thread_payload, thread_payload}}
  end

  defp started_thread_response(response_payload, _requested_model, _agent) do
    {:error, {:invalid_thread_response, response_payload}}
  end

  defp validate_returned_model(requested_model, effective_model)
       when is_binary(requested_model) and is_binary(effective_model) and
              requested_model != effective_model do
    {:error, {:codex_model_mismatch, requested_model, effective_model}}
  end

  defp validate_returned_model(_requested_model, _effective_model), do: :ok

  defp start_turn(
         port,
         thread_id,
         prompt,
         issue,
         workspace,
         approval_policy,
         turn_sandbox_policy,
         %{reasoning_effort: reasoning_effort, read_timeout_ms: read_timeout_ms, agent: agent}
       ) do
    params = %{
      "threadId" => thread_id,
      "input" => [
        %{
          "type" => "text",
          "text" => prompt
        }
      ],
      "cwd" => workspace,
      "title" => "#{issue.identifier}: #{issue.title}",
      "approvalPolicy" => approval_policy,
      "sandboxPolicy" => turn_sandbox_policy
    }

    params =
      if is_binary(reasoning_effort), do: Map.put(params, "effort", reasoning_effort), else: params

    params =
      if agent do
        params
        |> Map.put("model", agent.model)
        |> Map.put("cyberAccessProgram", AgentReadiness.requested(agent).cyber_access_program)
      else
        params
      end

    send_message(port, %{
      "method" => "turn/start",
      "id" => @turn_start_id,
      "params" => params
    })

    case await_response(port, @turn_start_id, response_timeout(%{agent: agent, read_timeout_ms: read_timeout_ms})) do
      {:ok, %{"turn" => %{"id" => turn_id}}} -> {:ok, turn_id}
      other -> other
    end
  end

  defp await_turn_completion(
         port,
         on_message,
         tool_executor,
         auto_approve_requests,
         turn_timeout_ms,
         factory_tools_enabled
       ) do
    timeout_ms = turn_timeout_ms || Config.settings!().codex.turn_timeout_ms

    receive_loop(
      port,
      on_message,
      timeout_ms,
      "",
      tool_executor,
      auto_approve_requests,
      factory_tools_enabled
    )
  end

  defp receive_loop(port, on_message, timeout_ms, pending_line, tool_executor, auto_approve_requests, factory_tools_enabled) do
    remaining_ms = remaining_turn_timeout(timeout_ms)

    if remaining_ms == 0 do
      {:error, :turn_timeout}
    else
      receive do
        {^port, {:data, {:eol, chunk}}} ->
          complete_line = pending_line <> to_string(chunk)
          handle_incoming(port, on_message, complete_line, timeout_ms, tool_executor, auto_approve_requests, factory_tools_enabled)

        {^port, {:data, {:noeol, chunk}}} ->
          receive_loop(
            port,
            on_message,
            timeout_ms,
            pending_line <> to_string(chunk),
            tool_executor,
            auto_approve_requests,
            factory_tools_enabled
          )

        {^port, {:exit_status, status}} ->
          {:error, {:port_exit, status}}
      after
        remaining_ms ->
          {:error, :turn_timeout}
      end
    end
  end

  defp remaining_turn_timeout({:deadline, deadline}), do: max(0, deadline - System.monotonic_time(:millisecond))
  defp remaining_turn_timeout(timeout_ms), do: timeout_ms

  defp handle_incoming(port, on_message, data, timeout_ms, tool_executor, auto_approve_requests, factory_tools_enabled) do
    payload_string = to_string(data)

    case Jason.decode(payload_string) do
      {:ok, %{"method" => "turn/completed"} = payload} ->
        emit_turn_event(on_message, :turn_completed, payload, payload_string, port, payload)

        case get_in(payload, ["params", "turn", "status"]) do
          status when status in ["failed", "interrupted"] -> {:error, {:turn_not_completed, status}}
          _ -> {:ok, :turn_completed}
        end

      {:ok, %{"method" => "model/rerouted", "params" => params}} ->
        {:error, {:codex_model_rerouted, Map.take(params, ["fromModel", "toModel", "reason"])}}

      {:ok, %{"method" => "turn/failed", "params" => _} = payload} ->
        emit_turn_event(
          on_message,
          :turn_failed,
          payload,
          payload_string,
          port,
          Map.get(payload, "params")
        )

        {:error, {:turn_failed, Map.get(payload, "params")}}

      {:ok, %{"method" => "turn/cancelled", "params" => _} = payload} ->
        emit_turn_event(
          on_message,
          :turn_cancelled,
          payload,
          payload_string,
          port,
          Map.get(payload, "params")
        )

        {:error, {:turn_cancelled, Map.get(payload, "params")}}

      {:ok, %{"method" => method} = payload}
      when is_binary(method) ->
        handle_turn_method(
          port,
          on_message,
          payload,
          payload_string,
          method,
          timeout_ms,
          tool_executor,
          auto_approve_requests,
          factory_tools_enabled
        )

      {:ok, payload} ->
        emit_message(
          on_message,
          :other_message,
          %{
            payload: payload,
            raw: payload_string
          },
          metadata_from_message(port, payload)
        )

        receive_loop(port, on_message, timeout_ms, "", tool_executor, auto_approve_requests, factory_tools_enabled)

      {:error, _reason} ->
        log_non_json_stream_line(payload_string, "turn stream")

        if protocol_message_candidate?(payload_string) do
          emit_message(
            on_message,
            :malformed,
            %{
              payload: payload_string,
              raw: payload_string
            },
            metadata_from_message(port, %{raw: payload_string})
          )
        end

        receive_loop(port, on_message, timeout_ms, "", tool_executor, auto_approve_requests, factory_tools_enabled)
    end
  end

  defp emit_turn_event(on_message, event, payload, payload_string, port, payload_details) do
    emit_message(
      on_message,
      event,
      %{
        payload: payload,
        raw: payload_string,
        details: payload_details
      },
      metadata_from_message(port, payload)
    )
  end

  defp handle_turn_method(
         port,
         on_message,
         payload,
         payload_string,
         method,
         timeout_ms,
         tool_executor,
         auto_approve_requests,
         factory_tools_enabled
       ) do
    metadata = metadata_from_message(port, payload)

    case maybe_handle_approval_request(
           port,
           method,
           payload,
           payload_string,
           on_message,
           metadata,
           tool_executor,
           auto_approve_requests
         ) do
      :input_required ->
        emit_message(
          on_message,
          :turn_input_required,
          %{payload: payload, raw: payload_string},
          metadata
        )

        if factory_tools_enabled do
          {:error, {:unsupported_native_input, method}}
        else
          {:error, {:turn_input_required, payload}}
        end

      :approved ->
        receive_loop(port, on_message, timeout_ms, "", tool_executor, auto_approve_requests, factory_tools_enabled)

      {:waiting, wait_id} ->
        {:waiting, wait_id}

      {:error, reason} ->
        {:error, reason}

      :approval_required ->
        emit_message(
          on_message,
          :approval_required,
          %{payload: payload, raw: payload_string},
          metadata
        )

        if factory_tools_enabled do
          {:error, {:unsupported_native_approval, method}}
        else
          {:error, {:approval_required, payload}}
        end

      :unhandled ->
        if needs_input?(method, payload) do
          emit_message(
            on_message,
            :turn_input_required,
            %{payload: payload, raw: payload_string},
            metadata
          )

          if factory_tools_enabled do
            {:error, {:unsupported_native_input, method}}
          else
            {:error, {:turn_input_required, payload}}
          end
        else
          emit_message(
            on_message,
            :notification,
            %{
              payload: payload,
              raw: payload_string
            },
            metadata
          )

          Logger.debug("Codex notification: #{inspect(method)}")
          receive_loop(port, on_message, timeout_ms, "", tool_executor, auto_approve_requests, factory_tools_enabled)
        end
    end
  end

  defp maybe_handle_approval_request(
         port,
         "item/commandExecution/requestApproval",
         %{"id" => id} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         auto_approve_requests
       ) do
    approve_or_require(
      port,
      id,
      "acceptForSession",
      payload,
      payload_string,
      on_message,
      metadata,
      auto_approve_requests
    )
  end

  defp maybe_handle_approval_request(
         port,
         "item/tool/call",
         %{"id" => id, "params" => params} = payload,
         payload_string,
         on_message,
         metadata,
         tool_executor,
         _auto_approve_requests
       ) do
    tool_name = tool_call_name(params)
    arguments = tool_call_arguments(params)

    result =
      tool_name
      |> tool_executor.(arguments)

    case result do
      {:stop, wait_id} ->
        {:waiting, wait_id}

      {:error, reason} ->
        {:error, reason}

      result ->
        result = normalize_dynamic_tool_result(result)
        send_message(port, %{"id" => id, "result" => result})

        event =
          case result do
            %{"success" => true} -> :tool_call_completed
            _ when is_nil(tool_name) -> :unsupported_tool_call
            _ -> :tool_call_failed
          end

        emit_message(on_message, event, %{payload: payload, raw: payload_string}, metadata)
        :approved
    end
  end

  defp maybe_handle_approval_request(
         port,
         "execCommandApproval",
         %{"id" => id} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         auto_approve_requests
       ) do
    approve_or_require(
      port,
      id,
      "approved_for_session",
      payload,
      payload_string,
      on_message,
      metadata,
      auto_approve_requests
    )
  end

  defp maybe_handle_approval_request(
         port,
         "applyPatchApproval",
         %{"id" => id} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         auto_approve_requests
       ) do
    approve_or_require(
      port,
      id,
      "approved_for_session",
      payload,
      payload_string,
      on_message,
      metadata,
      auto_approve_requests
    )
  end

  defp maybe_handle_approval_request(
         port,
         "item/fileChange/requestApproval",
         %{"id" => id} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         auto_approve_requests
       ) do
    approve_or_require(
      port,
      id,
      "acceptForSession",
      payload,
      payload_string,
      on_message,
      metadata,
      auto_approve_requests
    )
  end

  defp maybe_handle_approval_request(
         port,
         "item/tool/requestUserInput",
         %{"id" => id, "params" => params} = payload,
         payload_string,
         on_message,
         metadata,
         tool_executor,
         auto_approve_requests
       ) do
    case tool_executor.("__factory_native_request_user_input__", params) do
      {:native_answer, answer} ->
        send_message(port, %{"id" => id, "result" => answer})
        emit_message(on_message, :native_input_answered, %{payload: payload, raw: payload_string}, metadata)
        :approved

      {:stop, wait_id} ->
        {:waiting, wait_id}

      {:error, reason} ->
        {:error, reason}

      :unavailable ->
        maybe_auto_answer_tool_request_user_input(
          port,
          id,
          params,
          payload,
          payload_string,
          on_message,
          metadata,
          auto_approve_requests
        )

      _ ->
        :input_required
    end
  end

  defp maybe_handle_approval_request(
         _port,
         _method,
         _payload,
         _payload_string,
         _on_message,
         _metadata,
         _tool_executor,
         _auto_approve_requests
       ) do
    :unhandled
  end

  defp normalize_dynamic_tool_result(%{"success" => success} = result) when is_boolean(success) do
    output =
      case Map.get(result, "output") do
        existing_output when is_binary(existing_output) -> existing_output
        _ -> dynamic_tool_output(result)
      end

    content_items =
      case Map.get(result, "contentItems") do
        existing_items when is_list(existing_items) -> existing_items
        _ -> dynamic_tool_content_items(output)
      end

    result
    |> Map.put("output", output)
    |> Map.put("contentItems", content_items)
  end

  defp normalize_dynamic_tool_result(result) do
    %{
      "success" => false,
      "output" => inspect(result),
      "contentItems" => dynamic_tool_content_items(inspect(result))
    }
  end

  defp dynamic_tool_output(%{"contentItems" => [%{"text" => text} | _]}) when is_binary(text), do: text
  defp dynamic_tool_output(result), do: Jason.encode!(result, pretty: true)

  defp dynamic_tool_content_items(output) when is_binary(output) do
    [
      %{
        "type" => "inputText",
        "text" => output
      }
    ]
  end

  defp approve_or_require(
         port,
         id,
         decision,
         payload,
         payload_string,
         on_message,
         metadata,
         true
       ) do
    send_message(port, %{"id" => id, "result" => %{"decision" => decision}})

    emit_message(
      on_message,
      :approval_auto_approved,
      %{payload: payload, raw: payload_string, decision: decision},
      metadata
    )

    :approved
  end

  defp approve_or_require(
         _port,
         _id,
         _decision,
         _payload,
         _payload_string,
         _on_message,
         _metadata,
         false
       ) do
    :approval_required
  end

  defp maybe_auto_answer_tool_request_user_input(
         port,
         id,
         params,
         payload,
         payload_string,
         on_message,
         metadata,
         true
       ) do
    case tool_request_user_input_approval_answers(params) do
      {:ok, answers, decision} ->
        send_message(port, %{"id" => id, "result" => %{"answers" => answers}})

        emit_message(
          on_message,
          :approval_auto_approved,
          %{payload: payload, raw: payload_string, decision: decision},
          metadata
        )

        :approved

      :error ->
        :input_required
    end
  end

  defp maybe_auto_answer_tool_request_user_input(
         _port,
         _id,
         _params,
         _payload,
         _payload_string,
         _on_message,
         _metadata,
         false
       ),
       do: :input_required

  defp tool_request_user_input_approval_answers(%{"questions" => questions}) when is_list(questions) do
    answers =
      Enum.reduce_while(questions, %{}, fn question, acc ->
        case tool_request_user_input_approval_answer(question) do
          {:ok, question_id, answer_label} ->
            {:cont, Map.put(acc, question_id, %{"answers" => [answer_label]})}

          :error ->
            {:halt, :error}
        end
      end)

    case answers do
      :error -> :error
      answer_map when map_size(answer_map) > 0 -> {:ok, answer_map, "Approve this Session"}
      _ -> :error
    end
  end

  defp tool_request_user_input_approval_answers(_params), do: :error

  defp tool_request_user_input_approval_answer(%{"id" => question_id, "options" => options})
       when is_binary(question_id) and is_list(options) do
    if String.starts_with?(question_id, "mcp_tool_call_approval_") do
      case tool_request_user_input_approval_option_label(options) do
        nil -> :error
        answer_label -> {:ok, question_id, answer_label}
      end
    else
      :error
    end
  end

  defp tool_request_user_input_approval_answer(_question), do: :error

  defp tool_request_user_input_approval_option_label(options) do
    options
    |> Enum.map(&tool_request_user_input_option_label/1)
    |> Enum.reject(&is_nil/1)
    |> case do
      labels ->
        Enum.find(labels, &(&1 == "Approve this Session")) ||
          Enum.find(labels, &(&1 == "Approve Once")) ||
          Enum.find(labels, &approval_option_label?/1)
    end
  end

  defp tool_request_user_input_option_label(%{"label" => label}) when is_binary(label), do: label
  defp tool_request_user_input_option_label(_option), do: nil

  defp approval_option_label?(label) when is_binary(label) do
    normalized_label =
      label
      |> String.trim()
      |> String.downcase()

    String.starts_with?(normalized_label, "approve") or String.starts_with?(normalized_label, "allow")
  end

  defp await_response(port, request_id, timeout_ms) do
    timeout_ms = timeout_ms || Config.settings!().codex.read_timeout_ms
    with_timeout_response(port, request_id, timeout_ms, "")
  end

  defp with_timeout_response(port, request_id, timeout_ms, pending_line) do
    remaining_ms = remaining_turn_timeout(timeout_ms)

    if remaining_ms == 0 do
      {:error, :response_timeout}
    else
      receive do
        {^port, {:data, {:eol, chunk}}} ->
          complete_line = pending_line <> to_string(chunk)
          handle_response(port, request_id, complete_line, timeout_ms)

        {^port, {:data, {:noeol, chunk}}} ->
          with_timeout_response(port, request_id, timeout_ms, pending_line <> to_string(chunk))

        {^port, {:exit_status, status}} ->
          {:error, {:port_exit, status}}
      after
        remaining_ms ->
          {:error, :response_timeout}
      end
    end
  end

  defp handle_response(port, request_id, data, timeout_ms) do
    payload = to_string(data)

    case Jason.decode(payload) do
      {:ok, %{"id" => ^request_id, "error" => error}} ->
        {:error, {:response_error, error}}

      {:ok, %{"id" => ^request_id, "result" => result}} ->
        {:ok, result}

      {:ok, %{"id" => ^request_id} = response_payload} ->
        {:error, {:response_error, response_payload}}

      {:ok, %{} = other} ->
        Logger.debug("Ignoring message while waiting for response: #{inspect(other)}")
        with_timeout_response(port, request_id, timeout_ms, "")

      {:error, _} ->
        log_non_json_stream_line(payload, "response stream")
        with_timeout_response(port, request_id, timeout_ms, "")
    end
  end

  defp log_non_json_stream_line(data, stream_label) do
    text =
      data
      |> to_string()
      |> String.trim()
      |> String.slice(0, @max_stream_log_bytes)

    if text != "" do
      if String.match?(text, ~r/\b(error|warn|warning|failed|fatal|panic|exception)\b/i) do
        Logger.warning("Codex #{stream_label} output: #{text}")
      else
        Logger.debug("Codex #{stream_label} output: #{text}")
      end
    end
  end

  defp protocol_message_candidate?(data) do
    data
    |> to_string()
    |> String.trim_leading()
    |> String.starts_with?("{")
  end

  defp issue_context(%{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp stop_port(port) when is_port(port) do
    case :erlang.port_info(port) do
      :undefined ->
        :ok

      _ ->
        try do
          Port.close(port)
          :ok
        rescue
          ArgumentError ->
            :ok
        end
    end
  end

  defp emit_message(on_message, event, details, metadata) when is_function(on_message, 1) do
    message = metadata |> Map.merge(details) |> Map.put(:event, event) |> Map.put(:timestamp, DateTime.utc_now())
    on_message.(message)
  end

  defp metadata_from_message(port, payload) do
    port |> port_metadata(nil) |> maybe_set_usage(payload)
  end

  defp maybe_set_usage(metadata, payload) when is_map(payload) do
    usage = Map.get(payload, "usage") || Map.get(payload, :usage)

    if is_map(usage) do
      Map.put(metadata, :usage, usage)
    else
      metadata
    end
  end

  defp maybe_set_usage(metadata, _payload), do: metadata

  defp shell_escape(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
  end

  defp default_on_message(_message), do: :ok

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp tool_call_name(params) when is_map(params) do
    case Map.get(params, "tool") || Map.get(params, :tool) || Map.get(params, "name") || Map.get(params, :name) do
      name when is_binary(name) ->
        case String.trim(name) do
          "" -> nil
          trimmed -> trimmed
        end

      _ ->
        nil
    end
  end

  defp tool_call_name(_params), do: nil

  defp tool_call_arguments(params) when is_map(params) do
    Map.get(params, "arguments") || Map.get(params, :arguments) || %{}
  end

  defp tool_call_arguments(_params), do: %{}

  defp send_message(port, message) do
    line = Jason.encode!(message) <> "\n"
    Port.command(port, line)
  end

  defp needs_input?("mcpServer/elicitation/request", payload) when is_map(payload), do: true

  defp needs_input?(method, payload)
       when is_binary(method) and is_map(payload) do
    String.starts_with?(method, "turn/") && input_required_method?(method, payload)
  end

  defp needs_input?(_method, _payload), do: false

  defp input_required_method?(method, payload) when is_binary(method) do
    method in [
      "turn/input_required",
      "turn/needs_input",
      "turn/need_input",
      "turn/request_input",
      "turn/request_response",
      "turn/provide_input",
      "turn/approval_required"
    ] || request_payload_requires_input?(payload)
  end

  defp request_payload_requires_input?(payload) do
    params = Map.get(payload, "params")
    needs_input_field?(payload) || needs_input_field?(params)
  end

  defp needs_input_field?(payload) when is_map(payload) do
    Map.get(payload, "requiresInput") == true or
      Map.get(payload, "needsInput") == true or
      Map.get(payload, "input_required") == true or
      Map.get(payload, "inputRequired") == true or
      Map.get(payload, "type") == "input_required" or
      Map.get(payload, "type") == "needs_input"
  end

  defp needs_input_field?(_payload), do: false
end
