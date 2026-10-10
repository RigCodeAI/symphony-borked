defmodule SymphonyElixir.Orchestrator do
  @moduledoc """
  Polls the configured issue tracker and dispatches repository copies to Codex-backed workers.
  """

  use GenServer
  require Logger
  import Bitwise, only: [<<<: 2]

  alias SymphonyElixir.{AgentRunner, Config, StatusDashboard, Tracker, Workspace, WorkstreamRun, WorkstreamRunner, WorkstreamStore}
  alias SymphonyElixir.Tracker.Issue
  alias SymphonyElixir.Linear.Coordinator, as: LinearCoordinator

  @continuation_retry_delay_ms 1_000
  @failure_retry_base_ms 10_000
  # Slightly above the dashboard render interval so "checking now…" can render.
  @poll_transition_render_delay_ms 20
  @empty_codex_totals %{
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    seconds_running: 0
  }

  defmodule State do
    @moduledoc """
    Runtime state for the orchestrator polling loop.
    """

    defstruct [
      :poll_interval_ms,
      :max_concurrent_agents,
      :next_poll_due_at_ms,
      :poll_check_in_progress,
      :tick_timer_ref,
      :tick_token,
      workstreams: nil,
      polling_enabled: true,
      task_supervisor: SymphonyElixir.TaskSupervisor,
      running: %{},
      completed: MapSet.new(),
      claimed: MapSet.new(),
      blocked: %{},
      linear: nil,
      retry_attempts: %{},
      codex_totals: nil,
      codex_rate_limits: nil
    ]
  end

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    case initial_orchestrator_settings(opts) do
      {:ok, config} ->
        now_ms = System.monotonic_time(:millisecond)

        state = %State{
          poll_interval_ms: config.polling.interval_ms,
          max_concurrent_agents: config.agent.max_concurrent_agents,
          next_poll_due_at_ms: now_ms,
          poll_check_in_progress: false,
          tick_timer_ref: nil,
          tick_token: nil,
          task_supervisor: Keyword.get(opts, :task_supervisor, SymphonyElixir.TaskSupervisor),
          codex_totals: @empty_codex_totals,
          codex_rate_limits: nil
        }

        initialized = with {:ok, state} <- init_workstreams(state, opts), do: LinearCoordinator.init(state, opts)

        case initialized do
          {:ok, state} ->
            state = recover_workstreams(state)

            if state.polling_enabled do
              run_terminal_workspace_cleanup()
              {:ok, schedule_tick(state, 0)}
            else
              send(self(), :advance_workstreams)
              {:ok, state}
            end

          {:error, reason} ->
            {:stop, reason}
        end

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(message, %{polling_enabled: false} = state) when message in [:tick, :run_poll_cycle], do: {:noreply, state}
  def handle_info({:tick, _token}, %{polling_enabled: false} = state), do: {:noreply, state}

  def handle_info(:advance_workstreams, %{workstreams: %{opts: opts}} = state) do
    if Keyword.get(opts, :auto_advance, true), do: {:noreply, advance_workstreams(state)}, else: {:noreply, state}
  end

  def handle_info(:advance_workstreams, state), do: {:noreply, state}

  def handle_info(:linear_process, state), do: {:noreply, LinearCoordinator.process(state, &stop_linear_workstream/2)}
  def handle_info(:linear_tick, state), do: {:noreply, LinearCoordinator.process(LinearCoordinator.expire(state), &stop_linear_workstream/2)}

  def handle_info({:linear_result, token, result}, state) do
    {:noreply, LinearCoordinator.result(state, token, result, &queue_durable_workstream/6, &stop_linear_workstream/2)}
  end

  def handle_info({ref, {:workstream_canceled, run_id, attempt_id, outcome}}, state) when is_reference(ref) do
    {:noreply, accept_workstream_cancellation(state, ref, run_id, attempt_id, outcome)}
  end

  def handle_info({:workstream_result, run_id, attempt_id, pid, result}, state) do
    {:noreply, accept_workstream_result(state, run_id, attempt_id, pid, result)}
  end

  def handle_info({:tick, tick_token}, %{tick_token: tick_token} = state)
      when is_reference(tick_token) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info({:tick, _tick_token}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info(:run_poll_cycle, state) do
    state = refresh_runtime_config(state)
    state = maybe_dispatch(state)
    state = schedule_tick(state, state.poll_interval_ms)
    state = %{state | poll_check_in_progress: false}

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{running: running} = state
      ) do
    case find_issue_id_for_ref(running, ref) do
      nil ->
        case cancellation_down(state, ref) do
          {:handled, updated} ->
            {:noreply, updated}

          :unhandled ->
            case LinearCoordinator.down(state, ref) do
              :unhandled -> {:noreply, workstream_worker_down(state, ref, reason)}
              updated -> {:noreply, updated}
            end
        end

      issue_id ->
        {running_entry, state} = pop_running_entry(state, issue_id)
        state = record_session_completion_totals(state, running_entry)
        session_id = running_entry_session_id(running_entry)

        state = handle_agent_down(reason, state, issue_id, running_entry, session_id)

        Logger.info("Agent task finished for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}")

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info({:worker_runtime_info, issue_id, runtime_info}, %{running: running} = state)
      when is_binary(issue_id) and is_map(runtime_info) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        updated_running_entry =
          running_entry
          |> maybe_put_runtime_value(:worker_host, runtime_info[:worker_host])
          |> maybe_put_runtime_value(:workspace_path, runtime_info[:workspace_path])

        notify_dashboard()
        {:noreply, %{state | running: Map.put(running, issue_id, updated_running_entry)}}
    end
  end

  def handle_info(
        {:codex_worker_update, issue_id, %{event: _, timestamp: _} = update},
        %{running: running} = state
      ) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        {updated_running_entry, token_delta} = integrate_codex_update(running_entry, update)

        state =
          state
          |> apply_codex_token_delta(token_delta)
          |> apply_codex_rate_limits(update)

        notify_dashboard()
        {:noreply, %{state | running: Map.put(running, issue_id, updated_running_entry)}}
    end
  end

  def handle_info({:codex_worker_update, _issue_id, _update}, state), do: {:noreply, state}

  def handle_info({:retry_issue, issue_id, retry_token}, state) do
    result =
      case pop_retry_attempt_state(state, issue_id, retry_token) do
        {:ok, attempt, metadata, state} -> handle_retry_issue(state, issue_id, attempt, metadata)
        :missing -> {:noreply, state}
      end

    notify_dashboard()
    result
  end

  def handle_info({:retry_issue, _issue_id}, state), do: {:noreply, state}

  def handle_info(msg, state) do
    Logger.debug("Orchestrator ignored message: #{inspect(msg)}")
    {:noreply, state}
  end

  @spec receive_linear_event(GenServer.server(), map()) :: :ok | {:duplicate, map()} | {:error, term()}
  def receive_linear_event(server, event), do: GenServer.call(server, {:linear_event, event}, 4_000)

  defp handle_agent_down(:normal, state, issue_id, running_entry, session_id) do
    if input_required_blocker?(running_entry) do
      block_input_required_agent_down(state, issue_id, running_entry, session_id, :normal)
    else
      Logger.info("Agent task completed for issue_id=#{issue_id} session_id=#{session_id}; scheduling active-state continuation check")

      state
      |> complete_issue(issue_id)
      |> schedule_issue_retry(issue_id, 1, %{
        identifier: running_entry.identifier,
        issue_url: running_entry.issue.url,
        delay_type: :continuation,
        worker_host: Map.get(running_entry, :worker_host),
        workspace_path: Map.get(running_entry, :workspace_path)
      })
    end
  end

  defp handle_agent_down(reason, state, issue_id, running_entry, session_id) do
    if input_required_blocker?(running_entry) do
      block_input_required_agent_down(state, issue_id, running_entry, session_id, reason)
    else
      retry_agent_down(state, issue_id, running_entry, session_id, reason)
    end
  end

  defp block_input_required_agent_down(state, issue_id, running_entry, session_id, reason) do
    error = blocker_error(running_entry, "agent exited: #{inspect(reason)}")

    Logger.warning("Agent task blocked for issue_id=#{issue_id} issue_identifier=#{running_entry.identifier} session_id=#{session_id}: #{error}")

    block_issue_from_entry(state, issue_id, running_entry, error)
  end

  defp retry_agent_down(state, issue_id, running_entry, session_id, reason) do
    Logger.warning("Agent task exited for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}; scheduling retry")

    next_attempt = next_retry_attempt_from_running(running_entry)

    schedule_issue_retry(state, issue_id, next_attempt, %{
      identifier: running_entry.identifier,
      issue_url: running_entry.issue.url,
      error: "agent exited: #{inspect(reason)}",
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path)
    })
  end

  defp maybe_dispatch(%State{} = state) do
    state =
      state
      |> reconcile_running_issues()
      |> reconcile_blocked_issues()

    with :ok <- Config.validate!(),
         {:ok, issues} <- Tracker.fetch_issues_by_states(Config.settings!().tracker.active_states),
         true <- available_slots(state) > 0 do
      choose_issues(issues, state)
    else
      {:error, :missing_linear_api_token} ->
        Logger.error("Tracker API token missing in WORKFLOW.md")
        state

      {:error, :missing_linear_project_slug} ->
        Logger.error("Tracker project scope missing in WORKFLOW.md")
        state

      {:error, :missing_tracker_kind} ->
        Logger.error("Tracker kind missing in WORKFLOW.md")

        state

      {:error, {:unsupported_tracker_kind, kind}} ->
        Logger.error("Unsupported tracker kind in WORKFLOW.md: #{inspect(kind)}")

        state

      {:error, {:invalid_workflow_config, message}} ->
        Logger.error("Invalid WORKFLOW.md config: #{message}")
        state

      {:error, {:missing_workflow_file, path, reason}} ->
        Logger.error("Missing WORKFLOW.md at #{path}: #{inspect(reason)}")
        state

      {:error, :workflow_front_matter_not_a_map} ->
        Logger.error("Failed to parse WORKFLOW.md: workflow front matter must decode to a map")
        state

      {:error, {:workflow_parse_error, reason}} ->
        Logger.error("Failed to parse WORKFLOW.md: #{inspect(reason)}")
        state

      {:error, reason} ->
        Logger.error("Failed to fetch from issue tracker: #{inspect(reason)}")
        state

      false ->
        state
    end
  end

  defp reconcile_running_issues(%State{} = state) do
    state = reconcile_stalled_running_issues(state)
    running_ids = Map.keys(state.running)

    if running_ids == [] do
      state
    else
      case Tracker.fetch_issues_by_ids(running_ids) do
        {:ok, issues} ->
          issues
          |> reconcile_running_issue_states(
            state,
            active_state_set(),
            terminal_state_set()
          )
          |> reconcile_missing_running_issue_ids(running_ids, issues)

        {:error, reason} ->
          Logger.debug("Failed to refresh running issue states: #{inspect(reason)}; keeping active workers")

          state
      end
    end
  end

  defp reconcile_blocked_issues(%State{} = state) do
    blocked_ids = Map.keys(state.blocked)

    if blocked_ids == [] do
      state
    else
      case Tracker.fetch_issues_by_ids(blocked_ids) do
        {:ok, issues} ->
          issues
          |> reconcile_blocked_issue_states(
            state,
            active_state_set(),
            terminal_state_set()
          )
          |> reconcile_missing_blocked_issue_ids(blocked_ids, issues)

        {:error, reason} ->
          Logger.debug("Failed to refresh blocked issue states: #{inspect(reason)}; keeping blocked issues")

          state
      end
    end
  end

  @doc false
  @spec reconcile_issue_states_for_test([Issue.t()], term()) :: term()
  def reconcile_issue_states_for_test(issues, %State{} = state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  def reconcile_issue_states_for_test(issues, state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec reconcile_blocked_issue_states_for_test([Issue.t()], term()) :: term()
  def reconcile_blocked_issue_states_for_test(issues, %State{} = state) when is_list(issues) do
    reconcile_blocked_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec handle_retry_issue_lookup_for_test(Issue.t(), term(), String.t(), non_neg_integer(), map()) ::
          term()
  def handle_retry_issue_lookup_for_test(%Issue{} = issue, %State{} = state, issue_id, attempt, metadata)
      when is_binary(issue_id) and is_integer(attempt) and attempt >= 0 and is_map(metadata) do
    {:noreply, updated_state} = handle_retry_issue_lookup(issue, state, issue_id, attempt, metadata)
    updated_state
  end

  @doc false
  @spec should_dispatch_issue_for_test(Issue.t(), term()) :: boolean()
  def should_dispatch_issue_for_test(%Issue{} = issue, %State{} = state) do
    should_dispatch_issue?(issue, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec revalidate_issue_for_dispatch_for_test(Issue.t(), ([String.t()] -> term())) ::
          {:ok, Issue.t()} | {:skip, Issue.t() | :missing} | {:error, term()}
  def revalidate_issue_for_dispatch_for_test(%Issue{} = issue, issue_fetcher)
      when is_function(issue_fetcher, 1) do
    revalidate_issue_for_dispatch(issue, issue_fetcher, terminal_state_set())
  end

  @doc false
  @spec sort_issues_for_dispatch_for_test([Issue.t()]) :: [Issue.t()]
  def sort_issues_for_dispatch_for_test(issues) when is_list(issues) do
    sort_issues_for_dispatch(issues)
  end

  @doc false
  @spec select_worker_host_for_test(term(), String.t() | nil) :: String.t() | nil | :no_worker_capacity
  def select_worker_host_for_test(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host)
  end

  defp reconcile_running_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_running_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_running_issue_states(
      rest,
      reconcile_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, true)

      !issue_routable?(issue) ->
        Logger.info("Issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; stopping active agent")

        terminate_running_issue(state, issue.id, false)

      active_issue_state?(issue.state, active_states) ->
        refresh_running_issue_state(state, issue)

      true ->
        Logger.info("Issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, false)
    end
  end

  defp reconcile_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp reconcile_blocked_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_blocked_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_blocked_issue_states(
      rest,
      reconcile_blocked_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_blocked_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Blocked issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; releasing block")
        cleanup_issue_workspace(issue, Map.get(state.blocked, issue.id, %{}))
        release_issue_claim(state, issue.id)

      !issue_routable?(issue) ->
        Logger.info("Blocked issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; releasing block")
        release_issue_claim(state, issue.id)

      active_issue_state?(issue.state, active_states) ->
        refresh_blocked_issue_state(state, issue)

      true ->
        Logger.info("Blocked issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; releasing block")
        release_issue_claim(state, issue.id)
    end
  end

  defp reconcile_blocked_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp reconcile_missing_running_issue_ids(%State{} = state, requested_issue_ids, issues)
       when is_list(requested_issue_ids) and is_list(issues) do
    visible_issue_ids =
      issues
      |> Enum.flat_map(fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _ -> []
      end)
      |> MapSet.new()

    Enum.reduce(requested_issue_ids, state, fn issue_id, state_acc ->
      if MapSet.member?(visible_issue_ids, issue_id) do
        state_acc
      else
        log_missing_running_issue(state_acc, issue_id)
        terminate_running_issue(state_acc, issue_id, false)
      end
    end)
  end

  defp reconcile_missing_running_issue_ids(state, _requested_issue_ids, _issues), do: state

  defp reconcile_missing_blocked_issue_ids(%State{} = state, requested_issue_ids, issues)
       when is_list(requested_issue_ids) and is_list(issues) do
    visible_issue_ids =
      issues
      |> Enum.flat_map(fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _ -> []
      end)
      |> MapSet.new()

    Enum.reduce(requested_issue_ids, state, fn issue_id, state_acc ->
      if MapSet.member?(visible_issue_ids, issue_id) do
        state_acc
      else
        Logger.info("Blocked issue no longer visible during state refresh: issue_id=#{issue_id}; releasing block")
        release_issue_claim(state_acc, issue_id)
      end
    end)
  end

  defp reconcile_missing_blocked_issue_ids(state, _requested_issue_ids, _issues), do: state

  defp log_missing_running_issue(%State{} = state, issue_id) when is_binary(issue_id) do
    case Map.get(state.running, issue_id) do
      %{identifier: identifier} ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id} issue_identifier=#{identifier}; stopping active agent")

      _ ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id}; stopping active agent")
    end
  end

  defp log_missing_running_issue(_state, _issue_id), do: :ok

  defp refresh_running_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.running, issue.id) do
      %{issue: _} = running_entry ->
        %{state | running: Map.put(state.running, issue.id, %{running_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp refresh_blocked_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.blocked, issue.id) do
      %{issue: _} = blocked_entry ->
        %{state | blocked: Map.put(state.blocked, issue.id, %{blocked_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp terminate_running_issue(%State{} = state, issue_id, cleanup_workspace) do
    case Map.get(state.running, issue_id) do
      nil ->
        release_issue_claim(state, issue_id)

      %{pid: pid, ref: ref, identifier: identifier} = running_entry ->
        state = record_session_completion_totals(state, running_entry)

        stop_running_task(pid, ref, state.task_supervisor)

        if cleanup_workspace do
          cleanup_issue_workspace(Map.get(running_entry, :issue, identifier), running_entry)
        end

        %{
          state
          | running: Map.delete(state.running, issue_id),
            claimed: MapSet.delete(state.claimed, issue_id),
            blocked: Map.delete(state.blocked, issue_id),
            retry_attempts: Map.delete(state.retry_attempts, issue_id)
        }

      _ ->
        release_issue_claim(state, issue_id)
    end
  end

  defp reconcile_stalled_running_issues(%State{} = state) do
    timeout_ms = Config.settings!().codex.stall_timeout_ms

    cond do
      timeout_ms <= 0 ->
        state

      map_size(state.running) == 0 ->
        state

      true ->
        now = DateTime.utc_now()

        Enum.reduce(state.running, state, fn {issue_id, running_entry}, state_acc ->
          maybe_restart_stalled_issue(state_acc, issue_id, running_entry, now, timeout_ms)
        end)
    end
  end

  defp maybe_restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms) do
    if Map.has_key?(state.blocked, issue_id) do
      state
    else
      restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms)
    end
  end

  defp restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms) do
    elapsed_ms = stall_elapsed_ms(running_entry, now)

    if is_integer(elapsed_ms) and elapsed_ms > timeout_ms do
      identifier = Map.get(running_entry, :identifier, issue_id)
      session_id = running_entry_session_id(running_entry)

      if input_required_blocker?(running_entry) do
        error = blocker_error(running_entry, "stalled for #{elapsed_ms}ms after Codex requested operator input")

        Logger.warning("Issue blocked: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; #{error}")

        state
        |> record_session_completion_totals(running_entry)
        |> stop_and_block_issue(issue_id, running_entry, error)
      else
        Logger.warning("Issue stalled: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; restarting with backoff")

        next_attempt = next_retry_attempt_from_running(running_entry)

        state
        |> terminate_running_issue(issue_id, false)
        |> schedule_issue_retry(issue_id, next_attempt, %{
          identifier: identifier,
          issue_url: running_entry.issue.url,
          error: "stalled for #{elapsed_ms}ms without codex activity"
        })
      end
    else
      state
    end
  end

  defp stall_elapsed_ms(running_entry, now) do
    running_entry
    |> last_activity_timestamp()
    |> case do
      %DateTime{} = timestamp ->
        max(0, DateTime.diff(now, timestamp, :millisecond))

      _ ->
        nil
    end
  end

  defp last_activity_timestamp(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :last_codex_timestamp) || Map.get(running_entry, :started_at)
  end

  defp last_activity_timestamp(_running_entry), do: nil

  defp input_required_blocker?(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :last_codex_event) in [:turn_input_required, :approval_required] or
      not is_nil(input_required_completion_outcome(Map.get(running_entry, :completion))) or
      codex_message_method(Map.get(running_entry, :last_codex_message)) ==
        "mcpServer/elicitation/request"
  end

  defp input_required_blocker?(_running_entry), do: false

  defp input_required_completion_outcome(completion) when is_map(completion) do
    outcome = Map.get(completion, :outcome) || Map.get(completion, "outcome")
    normalize_input_required_outcome(outcome)
  end

  defp input_required_completion_outcome(_completion), do: nil

  defp normalize_input_required_outcome(outcome)
       when outcome in [:input_required, :needs_input, :approval_required],
       do: outcome

  defp normalize_input_required_outcome(outcome) when is_binary(outcome) do
    case outcome do
      "input_required" -> :input_required
      "needs_input" -> :needs_input
      "approval_required" -> :approval_required
      _ -> nil
    end
  end

  defp normalize_input_required_outcome(_outcome), do: nil

  defp blocker_error(running_entry, fallback) when is_map(running_entry) do
    codex_event_blocker_error(Map.get(running_entry, :last_codex_event)) ||
      completion_blocker_error(Map.get(running_entry, :completion)) ||
      codex_message_blocker_error(Map.get(running_entry, :last_codex_message)) ||
      fallback
  end

  defp blocker_error(_running_entry, fallback), do: fallback

  defp codex_event_blocker_error(:turn_input_required), do: "codex turn requires operator input"
  defp codex_event_blocker_error(:approval_required), do: "codex turn requires approval"
  defp codex_event_blocker_error(_event), do: nil

  defp completion_blocker_error(completion) do
    case input_required_completion_outcome(completion) do
      outcome when outcome in [:input_required, :needs_input] -> "codex turn requires operator input"
      :approval_required -> "codex turn requires approval"
      nil -> nil
    end
  end

  defp codex_message_blocker_error(message) do
    if codex_message_method(message) == "mcpServer/elicitation/request" do
      "codex MCP elicitation requires operator input"
    end
  end

  defp codex_message_method(%{message: %{"method" => method}}) when is_binary(method), do: method
  defp codex_message_method(%{message: %{method: method}}) when is_binary(method), do: method
  defp codex_message_method(%{"method" => method}) when is_binary(method), do: method
  defp codex_message_method(%{method: method}) when is_binary(method), do: method
  defp codex_message_method(_message), do: nil

  defp terminate_task(pid, task_supervisor) when is_pid(pid) do
    case Task.Supervisor.terminate_child(task_supervisor, pid) do
      :ok ->
        :ok

      {:error, :not_found} ->
        Process.exit(pid, :shutdown)
    end
  end

  defp terminate_task(_pid, _task_supervisor), do: :ok

  defp stop_running_task(pid, ref, task_supervisor) do
    if is_pid(pid) do
      terminate_task(pid, task_supervisor)
    end

    if is_reference(ref) do
      Process.demonitor(ref, [:flush])
    end

    :ok
  end

  defp stop_and_block_issue(%State{} = state, issue_id, running_entry, error) do
    stop_running_task(
      Map.get(running_entry, :pid),
      Map.get(running_entry, :ref),
      state.task_supervisor
    )

    block_issue_from_entry(state, issue_id, running_entry, error)
  end

  defp block_issue_from_entry(%State{} = state, issue_id, running_entry, error) do
    blocked_entry = %{
      issue_id: issue_id,
      identifier: Map.get(running_entry, :identifier, issue_id),
      issue: Map.get(running_entry, :issue),
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      session_id: running_entry_session_id(running_entry),
      error: error,
      blocked_at: DateTime.utc_now(),
      last_codex_message: Map.get(running_entry, :last_codex_message),
      last_codex_event: Map.get(running_entry, :last_codex_event),
      last_codex_timestamp: Map.get(running_entry, :last_codex_timestamp)
    }

    %{
      state
      | running: Map.delete(state.running, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id),
        claimed: MapSet.put(state.claimed, issue_id),
        blocked: Map.put(state.blocked, issue_id, blocked_entry)
    }
  end

  defp choose_issues(issues, state) do
    active_states = active_state_set()
    terminal_states = terminal_state_set()

    issues
    |> sort_issues_for_dispatch()
    |> Enum.reduce(state, fn issue, state_acc ->
      if should_dispatch_issue?(issue, state_acc, active_states, terminal_states) do
        dispatch_issue(state_acc, issue)
      else
        state_acc
      end
    end)
  end

  defp sort_issues_for_dispatch(issues) when is_list(issues) do
    Enum.sort_by(issues, fn
      %Issue{} = issue ->
        {priority_rank(issue.priority), issue_created_at_sort_key(issue), issue.identifier || issue.id || ""}

      _ ->
        {priority_rank(nil), issue_created_at_sort_key(nil), ""}
    end)
  end

  defp priority_rank(priority) when is_integer(priority) and priority in 1..4, do: priority
  defp priority_rank(_priority), do: 5

  defp issue_created_at_sort_key(%Issue{created_at: %DateTime{} = created_at}) do
    DateTime.to_unix(created_at, :microsecond)
  end

  defp issue_created_at_sort_key(%Issue{}), do: 9_223_372_036_854_775_807
  defp issue_created_at_sort_key(_issue), do: 9_223_372_036_854_775_807

  defp should_dispatch_issue?(
         %Issue{} = issue,
         %State{running: running, claimed: claimed, blocked: blocked} = state,
         active_states,
         terminal_states
       ) do
    candidate_issue?(issue, active_states, terminal_states) and
      !MapSet.member?(claimed, issue.id) and
      !Map.has_key?(running, issue.id) and
      !Map.has_key?(blocked, issue.id) and
      available_slots(state) > 0 and
      state_slots_available?(issue, running) and
      worker_slots_available?(state)
  end

  defp should_dispatch_issue?(_issue, _state, _active_states, _terminal_states), do: false

  defp state_slots_available?(%Issue{state: issue_state}, running) when is_map(running) do
    limit = Config.max_concurrent_agents_for_state(issue_state)
    used = running_issue_count_for_state(running, issue_state)
    limit > used
  end

  defp state_slots_available?(_issue, _running), do: false

  defp running_issue_count_for_state(running, issue_state) when is_map(running) do
    normalized_state = normalize_issue_state(issue_state)

    Enum.count(running, fn
      {_id, %{issue: %Issue{state: state_name}}} ->
        normalize_issue_state(state_name) == normalized_state

      _ ->
        false
    end)
  end

  defp candidate_issue?(
         %Issue{
           id: id,
           identifier: identifier,
           title: title,
           state: state_name
         } = issue,
         active_states,
         terminal_states
       )
       when is_binary(id) and is_binary(identifier) and is_binary(title) and is_binary(state_name) do
    Enum.all?([id, identifier, title, state_name], &present_string?/1) and
      issue_routable?(issue) and
      active_issue_state?(state_name, active_states) and
      !terminal_issue_state?(state_name, terminal_states)
  end

  defp candidate_issue?(_issue, _active_states, _terminal_states), do: false

  defp issue_routable?(%Issue{} = issue) do
    Issue.routable?(issue, Config.settings!().tracker.required_labels)
  end

  defp terminal_issue_state?(state_name, terminal_states) when is_binary(state_name) do
    MapSet.member?(terminal_states, normalize_issue_state(state_name))
  end

  defp terminal_issue_state?(_state_name, _terminal_states), do: false

  defp present_string?(value) when is_binary(value), do: String.trim(value) != ""
  defp present_string?(_value), do: false

  defp active_issue_state?(state_name, active_states) when is_binary(state_name) do
    MapSet.member?(active_states, normalize_issue_state(state_name))
  end

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    String.downcase(String.trim(state_name))
  end

  defp terminal_state_set do
    Config.settings!().tracker.terminal_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp active_state_set do
    Config.settings!().tracker.active_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp dispatch_issue(%State{} = state, issue, attempt \\ nil, preferred_worker_host \\ nil) do
    case refresh_issue_for_dispatch(issue) do
      {:ok, %Issue{} = refreshed_issue} ->
        do_dispatch_issue(state, refreshed_issue, attempt, preferred_worker_host)

      {:skip, _reason} ->
        state

      {:error, _reason} ->
        state
    end
  end

  defp refresh_issue_for_dispatch(issue) do
    case revalidate_issue_for_dispatch(issue, &Tracker.fetch_issues_by_ids/1, terminal_state_set()) do
      {:ok, %Issue{} = refreshed_issue} ->
        {:ok, refreshed_issue}

      {:skip, :missing} ->
        Logger.info("Skipping dispatch; issue no longer active or visible: #{issue_context(issue)}")
        {:skip, :missing}

      {:skip, %Issue{} = refreshed_issue} ->
        Logger.info("Skipping stale dispatch after issue refresh: #{issue_context(refreshed_issue)} state=#{inspect(refreshed_issue.state)} blocked_by=#{length(refreshed_issue.blocked_by)}")

        {:skip, refreshed_issue}

      {:error, reason} ->
        Logger.warning("Skipping dispatch; issue refresh failed for #{issue_context(issue)}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp do_dispatch_issue(%State{} = state, issue, attempt, preferred_worker_host) do
    recipient = self()

    case select_worker_host(state, preferred_worker_host) do
      :no_worker_capacity ->
        Logger.debug("No SSH worker slots available for #{issue_context(issue)} preferred_worker_host=#{inspect(preferred_worker_host)}")
        state

      worker_host ->
        spawn_issue_on_worker_host(state, issue, attempt, recipient, worker_host)
    end
  end

  defp spawn_issue_on_worker_host(%State{} = state, issue, attempt, recipient, worker_host) do
    case Task.Supervisor.start_child(state.task_supervisor, fn ->
           AgentRunner.run(issue, recipient, attempt: attempt, worker_host: worker_host)
         end) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        Logger.info("Dispatching issue to agent: #{issue_context(issue)} pid=#{inspect(pid)} attempt=#{inspect(attempt)} worker_host=#{worker_host || "local"}")

        running =
          Map.put(state.running, issue.id, %{
            pid: pid,
            ref: ref,
            identifier: issue.identifier,
            issue: issue,
            worker_host: worker_host,
            workspace_path: nil,
            session_id: nil,
            last_codex_message: nil,
            last_codex_timestamp: nil,
            last_codex_event: nil,
            codex_app_server_pid: nil,
            codex_input_tokens: 0,
            codex_output_tokens: 0,
            codex_total_tokens: 0,
            codex_last_reported_input_tokens: 0,
            codex_last_reported_output_tokens: 0,
            codex_last_reported_total_tokens: 0,
            turn_count: 0,
            retry_attempt: normalize_retry_attempt(attempt),
            started_at: DateTime.utc_now()
          })

        %{
          state
          | running: running,
            claimed: MapSet.put(state.claimed, issue.id),
            retry_attempts: Map.delete(state.retry_attempts, issue.id)
        }

      {:error, reason} ->
        Logger.error("Unable to spawn agent for #{issue_context(issue)}: #{inspect(reason)}")
        next_attempt = if is_integer(attempt), do: attempt + 1, else: nil

        schedule_issue_retry(state, issue.id, next_attempt, %{
          identifier: issue.identifier,
          issue_url: issue.url,
          error: "failed to spawn agent: #{inspect(reason)}",
          worker_host: worker_host
        })
    end
  end

  defp revalidate_issue_for_dispatch(%Issue{id: issue_id}, issue_fetcher, terminal_states)
       when is_binary(issue_id) and is_function(issue_fetcher, 1) do
    case issue_fetcher.([issue_id]) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        if retry_candidate_issue?(refreshed_issue, terminal_states) do
          {:ok, refreshed_issue}
        else
          {:skip, refreshed_issue}
        end

      {:ok, []} ->
        {:skip, :missing}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp revalidate_issue_for_dispatch(issue, _issue_fetcher, _terminal_states), do: {:ok, issue}

  defp complete_issue(%State{} = state, issue_id) do
    %{
      state
      | completed: MapSet.put(state.completed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  defp schedule_issue_retry(%State{} = state, issue_id, attempt, metadata)
       when is_binary(issue_id) and is_map(metadata) do
    previous_retry = Map.get(state.retry_attempts, issue_id, %{attempt: 0})
    next_attempt = if is_integer(attempt), do: attempt, else: previous_retry.attempt + 1
    delay_ms = retry_delay(next_attempt, metadata)
    old_timer = Map.get(previous_retry, :timer_ref)
    retry_token = make_ref()
    due_at_ms = System.monotonic_time(:millisecond) + delay_ms
    identifier = pick_retry_identifier(issue_id, previous_retry, metadata)
    issue_url = pick_retry_issue_url(previous_retry, metadata)
    error = pick_retry_error(previous_retry, metadata)
    worker_host = pick_retry_worker_host(previous_retry, metadata)
    workspace_path = pick_retry_workspace_path(previous_retry, metadata)

    if is_reference(old_timer) do
      Process.cancel_timer(old_timer)
    end

    timer_ref = Process.send_after(self(), {:retry_issue, issue_id, retry_token}, delay_ms)

    error_suffix = if is_binary(error), do: " error=#{error}", else: ""

    Logger.warning("Retrying issue_id=#{issue_id} issue_identifier=#{identifier} in #{delay_ms}ms (attempt #{next_attempt})#{error_suffix}")

    %{
      state
      | retry_attempts:
          Map.put(state.retry_attempts, issue_id, %{
            attempt: next_attempt,
            timer_ref: timer_ref,
            retry_token: retry_token,
            due_at_ms: due_at_ms,
            identifier: identifier,
            issue_url: issue_url,
            error: error,
            worker_host: worker_host,
            workspace_path: workspace_path
          })
    }
  end

  defp pop_retry_attempt_state(%State{} = state, issue_id, retry_token) when is_reference(retry_token) do
    case Map.get(state.retry_attempts, issue_id) do
      %{attempt: attempt, retry_token: ^retry_token} = retry_entry ->
        metadata = %{
          identifier: Map.get(retry_entry, :identifier),
          issue_url: Map.get(retry_entry, :issue_url),
          error: Map.get(retry_entry, :error),
          worker_host: Map.get(retry_entry, :worker_host),
          workspace_path: Map.get(retry_entry, :workspace_path)
        }

        {:ok, attempt, metadata, %{state | retry_attempts: Map.delete(state.retry_attempts, issue_id)}}

      _ ->
        :missing
    end
  end

  defp handle_retry_issue(%State{} = state, issue_id, attempt, metadata) do
    case Tracker.fetch_issues_by_ids([issue_id]) do
      {:ok, issues} ->
        issues
        |> find_issue_by_id(issue_id)
        |> handle_retry_issue_lookup(state, issue_id, attempt, metadata)

      {:error, reason} ->
        Logger.warning("Retry poll failed for issue_id=#{issue_id} issue_identifier=#{metadata[:identifier] || issue_id}: #{inspect(reason)}")

        {:noreply,
         schedule_issue_retry(
           state,
           issue_id,
           attempt + 1,
           Map.merge(metadata, %{error: "retry poll failed: #{inspect(reason)}"})
         )}
    end
  end

  defp handle_retry_issue_lookup(%Issue{} = issue, state, issue_id, attempt, metadata) do
    terminal_states = terminal_state_set()

    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue state is terminal: issue_id=#{issue_id} issue_identifier=#{issue.identifier} state=#{issue.state}; removing associated workspace")

        cleanup_issue_workspace(issue, metadata)
        {:noreply, release_issue_claim(state, issue_id)}

      retry_candidate_issue?(issue, terminal_states) ->
        handle_active_retry(state, issue, attempt, metadata)

      true ->
        Logger.debug("Issue left active states, removing claim issue_id=#{issue_id} issue_identifier=#{issue.identifier}")

        {:noreply, release_issue_claim(state, issue_id)}
    end
  end

  defp handle_retry_issue_lookup(nil, state, issue_id, _attempt, _metadata) do
    Logger.debug("Issue no longer visible, removing claim issue_id=#{issue_id}")
    {:noreply, release_issue_claim(state, issue_id)}
  end

  defp cleanup_issue_workspace(identifier, worker_host \\ nil)

  defp cleanup_issue_workspace(issue_or_identifier, metadata) when is_map(metadata) do
    case Map.get(metadata, :workspace_path) do
      workspace_path when is_binary(workspace_path) and workspace_path != "" ->
        Workspace.remove_recorded(workspace_path, Map.get(metadata, :worker_host))

      _ ->
        cleanup_issue_workspace(issue_or_identifier, Map.get(metadata, :worker_host))
    end
  end

  defp cleanup_issue_workspace(%Issue{} = issue, worker_host) do
    Workspace.remove_issue_workspaces(issue, worker_host)
  end

  defp cleanup_issue_workspace(identifier, worker_host) when is_binary(identifier) do
    Workspace.remove_issue_workspaces(identifier, worker_host)
  end

  defp cleanup_issue_workspace(_issue_or_identifier, _worker_host), do: :ok

  defp run_terminal_workspace_cleanup do
    case Tracker.fetch_issues_by_states(Config.settings!().tracker.terminal_states) do
      {:ok, issues} ->
        issues
        |> Enum.each(fn
          %Issue{} = issue ->
            cleanup_issue_workspace(issue)

          _ ->
            :ok
        end)

      {:error, reason} ->
        Logger.warning("Skipping startup terminal workspace cleanup; failed to fetch terminal issues: #{inspect(reason)}")
    end
  end

  defp notify_dashboard do
    StatusDashboard.notify_update()
  end

  defp handle_active_retry(state, issue, attempt, metadata) do
    if retry_candidate_issue?(issue, terminal_state_set()) and
         dispatch_slots_available?(issue, state) and
         worker_slots_available?(state, metadata[:worker_host]) do
      case refresh_issue_for_dispatch(issue) do
        {:ok, %Issue{} = refreshed_issue} ->
          {:noreply, do_dispatch_issue(state, refreshed_issue, attempt, metadata[:worker_host])}

        {:skip, :missing} ->
          {:noreply, release_issue_claim(state, issue.id)}

        {:skip, %Issue{} = refreshed_issue} ->
          handle_retry_issue_lookup(refreshed_issue, state, issue.id, attempt, metadata)

        {:error, reason} ->
          {:noreply,
           schedule_issue_retry(
             state,
             issue.id,
             attempt + 1,
             Map.merge(metadata, %{
               identifier: issue.identifier,
               error: "retry dispatch refresh failed: #{inspect(reason)}"
             })
           )}
      end
    else
      Logger.debug("No available slots for retrying #{issue_context(issue)}; retrying again")

      {:noreply,
       schedule_issue_retry(
         state,
         issue.id,
         attempt + 1,
         Map.merge(metadata, %{
           identifier: issue.identifier,
           error: "no available orchestrator slots"
         })
       )}
    end
  end

  defp release_issue_claim(%State{} = state, issue_id) do
    %{
      state
      | claimed: MapSet.delete(state.claimed, issue_id),
        blocked: Map.delete(state.blocked, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  defp retry_delay(attempt, metadata) when is_integer(attempt) and attempt > 0 and is_map(metadata) do
    if metadata[:delay_type] == :continuation and attempt == 1 do
      @continuation_retry_delay_ms
    else
      failure_retry_delay(attempt)
    end
  end

  defp failure_retry_delay(attempt) do
    max_delay_power = min(attempt - 1, 10)
    min(@failure_retry_base_ms * (1 <<< max_delay_power), Config.settings!().agent.max_retry_backoff_ms)
  end

  defp normalize_retry_attempt(attempt) when is_integer(attempt) and attempt > 0, do: attempt
  defp normalize_retry_attempt(_attempt), do: 0

  defp next_retry_attempt_from_running(running_entry) do
    case Map.get(running_entry, :retry_attempt) do
      attempt when is_integer(attempt) and attempt > 0 -> attempt + 1
      _ -> nil
    end
  end

  defp pick_retry_identifier(issue_id, previous_retry, metadata) do
    metadata[:identifier] || Map.get(previous_retry, :identifier) || issue_id
  end

  defp pick_retry_issue_url(previous_retry, metadata) do
    metadata[:issue_url] || Map.get(previous_retry, :issue_url)
  end

  defp pick_retry_error(previous_retry, metadata) do
    metadata[:error] || Map.get(previous_retry, :error)
  end

  defp pick_retry_worker_host(previous_retry, metadata) do
    metadata[:worker_host] || Map.get(previous_retry, :worker_host)
  end

  defp pick_retry_workspace_path(previous_retry, metadata) do
    metadata[:workspace_path] || Map.get(previous_retry, :workspace_path)
  end

  defp maybe_put_runtime_value(running_entry, _key, nil), do: running_entry

  defp maybe_put_runtime_value(running_entry, key, value) when is_map(running_entry) do
    Map.put(running_entry, key, value)
  end

  defp select_worker_host(%State{} = state, preferred_worker_host) do
    case Config.settings!().worker.ssh_hosts do
      [] ->
        nil

      hosts ->
        available_hosts = Enum.filter(hosts, &worker_host_slots_available?(state, &1))

        cond do
          available_hosts == [] ->
            :no_worker_capacity

          preferred_worker_host_available?(preferred_worker_host, available_hosts) ->
            preferred_worker_host

          true ->
            least_loaded_worker_host(state, available_hosts)
        end
    end
  end

  defp preferred_worker_host_available?(preferred_worker_host, hosts)
       when is_binary(preferred_worker_host) and is_list(hosts) do
    preferred_worker_host != "" and preferred_worker_host in hosts
  end

  defp preferred_worker_host_available?(_preferred_worker_host, _hosts), do: false

  defp least_loaded_worker_host(%State{} = state, hosts) when is_list(hosts) do
    hosts
    |> Enum.with_index()
    |> Enum.min_by(fn {host, index} ->
      {running_worker_host_count(state.running, host), index}
    end)
    |> elem(0)
  end

  defp running_worker_host_count(running, worker_host) when is_map(running) and is_binary(worker_host) do
    Enum.count(running, fn
      {_issue_id, %{worker_host: ^worker_host}} -> true
      _ -> false
    end)
  end

  defp worker_slots_available?(%State{} = state) do
    select_worker_host(state, nil) != :no_worker_capacity
  end

  defp worker_slots_available?(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host) != :no_worker_capacity
  end

  defp worker_host_slots_available?(%State{} = state, worker_host) when is_binary(worker_host) do
    case Config.settings!().worker.max_concurrent_agents_per_host do
      limit when is_integer(limit) and limit > 0 ->
        running_worker_host_count(state.running, worker_host) < limit

      _ ->
        true
    end
  end

  defp find_issue_by_id(issues, issue_id) when is_binary(issue_id) do
    Enum.find(issues, fn
      %Issue{id: ^issue_id} ->
        true

      _ ->
        false
    end)
  end

  defp find_issue_id_for_ref(running, ref) do
    running
    |> Enum.find_value(fn {issue_id, %{ref: running_ref}} ->
      if running_ref == ref, do: issue_id
    end)
  end

  defp running_entry_session_id(%{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp running_entry_session_id(_running_entry), do: "n/a"

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp available_slots(%State{} = state) do
    max(
      (state.max_concurrent_agents || Config.settings!().agent.max_concurrent_agents) -
        map_size(state.running),
      0
    )
  end

  @spec request_refresh() :: map() | :unavailable
  def request_refresh do
    request_refresh(__MODULE__)
  end

  @spec request_refresh(GenServer.server()) :: map() | :unavailable
  def request_refresh(server) do
    if Process.whereis(server) do
      GenServer.call(server, :request_refresh)
    else
      :unavailable
    end
  end

  @spec snapshot() :: map() | :timeout | :unavailable
  def snapshot, do: snapshot(__MODULE__, 15_000)

  @spec snapshot(GenServer.server(), timeout()) :: map() | :timeout | :unavailable
  def snapshot(server, timeout) do
    if Process.whereis(server) do
      try do
        GenServer.call(server, :snapshot, timeout)
      catch
        :exit, {:timeout, _} -> :timeout
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc "Queue one local task. Duplicate events and task identities reuse the durable run."
  @spec queue_workstream(GenServer.server(), String.t(), String.t(), Path.t(), map(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def queue_workstream(server, event_id, task_id, path, inputs, opts),
    do: GenServer.call(server, {:queue_workstream, event_id, task_id, path, inputs, opts}, 30_000)

  @spec workstream_state(GenServer.server(), String.t()) :: {:ok, map()} | {:error, term()}
  def workstream_state(server, run_id), do: GenServer.call(server, {:workstream_state, run_id})

  @spec answer_workstream(GenServer.server(), String.t(), String.t(), String.t(), map()) :: :ok | {:error, term()}
  def answer_workstream(server, event_id, run_id, wait_id, answer),
    do: GenServer.call(server, {:answer_workstream, event_id, run_id, wait_id, answer})

  @doc "Advance ready local stages once; useful for inspecting a durable stage boundary."
  @spec step_workstreams(GenServer.server()) :: :ok
  def step_workstreams(server), do: GenServer.call(server, :step_workstreams)

  @spec reconcile_workstreams(GenServer.server()) :: :ok
  def reconcile_workstreams(server), do: GenServer.call(server, :reconcile_workstreams)

  @impl true
  def handle_call({:linear_event, event}, _from, state) do
    case LinearCoordinator.receive_event(state, event, &stop_linear_workstream/2) do
      {:ok, updated, reply} -> {:reply, reply, updated}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:workstream_question, _run_id, _attempt_id, _pid, _request}, _from, %{workstreams: nil} = state),
    do: {:reply, {:error, :durable_workstreams_disabled}, state}

  def handle_call({:workstream_question, run_id, attempt_id, pid, request}, {caller, _}, state) do
    case {durable_run(state, run_id), state.workstreams.workers[attempt_id]} do
      {%{status: :executing, current_attempt_id: ^attempt_id} = run, %{pid: ^pid}} when caller == pid ->
        with {:ok, prompt} <- SymphonyElixir.Linear.Text.safe(request[:prompt]),
             {:ok, updated, question} <- WorkstreamRun.ask_question(run, Map.put(request, :prompt, prompt)) do
          event = %{kind: :question_created, question_id: question.id}
          updated_state = commit_workstream(state, updated, question.id <> "/asked", event)
          {:reply, {:ok, question}, updated_state}
        else
          {:error, _} = error -> {:reply, error, state}
        end

      _ ->
        {:reply, {:error, :question_not_current_worker}, state}
    end
  end

  def handle_call({:workstream_wait, _run_id, _attempt_id, _pid, _id}, _from, %{workstreams: nil} = state),
    do: {:reply, {:error, :durable_workstreams_disabled}, state}

  def handle_call({:workstream_wait, run_id, attempt_id, pid, id}, {caller, _}, state) do
    case {durable_run(state, run_id), state.workstreams.workers[attempt_id]} do
      {%{status: :executing, current_attempt_id: ^attempt_id} = run, %{pid: ^pid}} when caller == pid ->
        case Map.get(Map.get(run, :questions, %{}), id) do
          %{stage_id: stage_id, active_attempt_id: question_attempt, artifact_ids: artifacts, reply: reply}
          when stage_id == run.stage_id and question_attempt == attempt_id and artifacts == run.artifacts ->
            answer =
              case reply do
                %{body: body} -> {:answered, %{body: body}}
                nil -> :wait
              end

            {:reply, answer, state}

          _ ->
            {:reply, {:error, :unknown_or_stale_question}, state}
        end

      _ ->
        {:reply, {:error, :question_not_current_worker}, state}
    end
  end

  def handle_call({:workstream_process, _run_id, _attempt_id, _pid, _identity}, _from, %{workstreams: nil} = state),
    do: {:reply, {:error, :durable_workstreams_disabled}, state}

  def handle_call({:workstream_process, run_id, attempt_id, pid, identity}, {caller, _}, state) do
    case {durable_run(state, run_id), state.workstreams.workers[attempt_id]} do
      {%{status: :executing, current_attempt_id: ^attempt_id} = run, %{pid: ^pid}} when caller == pid ->
        register_workstream_process(state, run, identity)

      _ ->
        {:reply, {:error, :process_registration_not_current}, state}
    end
  end

  def handle_call({:queue_workstream, event_id, task_id, path, inputs, opts}, _from, state) do
    case queue_durable_workstream(state, input_event_key(event_id), task_id, path, inputs, opts) do
      {:ok, run_id, updated} ->
        send(self(), :advance_workstreams)
        {:reply, {:ok, run_id}, updated}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:workstream_state, run_id}, _from, state) do
    result =
      case durable_run(state, run_id) do
        nil -> {:error, :unknown_workstream_run}
        run -> {:ok, WorkstreamRun.report(run)}
      end

    {:reply, result, state}
  end

  def handle_call({:answer_workstream, event_id, run_id, wait_id, answer}, _from, state) do
    event_id = input_event_key(event_id)
    event = %{kind: :human_answer, wait_id: wait_id, fingerprint: workstream_fingerprint({run_id, wait_id, answer})}

    case check_workstream_event(state, event_id, event) do
      {:duplicate, ^run_id} ->
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}

      :new ->
        with run when is_map(run) <- durable_run(state, run_id),
             {:ok, updated} <- WorkstreamRun.wait_answer(run, wait_id, answer) do
          updated_state = commit_workstream(state, updated, event_id, event)
          send(self(), :advance_workstreams)
          {:reply, :ok, updated_state}
        else
          nil -> {:reply, {:error, :unknown_workstream_run}, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call(:step_workstreams, _from, state), do: {:reply, :ok, advance_workstreams(state)}

  def handle_call(:reconcile_workstreams, _from, state) do
    state = state |> recover_workstreams() |> reconcile_stopped_workstreams()
    send(self(), :advance_workstreams)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    state = refresh_runtime_config(state)
    now = DateTime.utc_now()
    now_ms = System.monotonic_time(:millisecond)

    running =
      state.running
      |> Enum.map(fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: metadata.identifier,
          issue_url: metadata.issue.url,
          state: metadata.issue.state,
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: metadata.session_id,
          codex_app_server_pid: metadata.codex_app_server_pid,
          codex_input_tokens: metadata.codex_input_tokens,
          codex_output_tokens: metadata.codex_output_tokens,
          codex_total_tokens: metadata.codex_total_tokens,
          turn_count: Map.get(metadata, :turn_count, 0),
          started_at: metadata.started_at,
          last_codex_timestamp: metadata.last_codex_timestamp,
          last_codex_message: metadata.last_codex_message,
          last_codex_event: metadata.last_codex_event,
          runtime_seconds: running_seconds(metadata.started_at, now)
        }
      end)

    retrying =
      state.retry_attempts
      |> Enum.map(fn {issue_id, %{attempt: attempt, due_at_ms: due_at_ms} = retry} ->
        %{
          issue_id: issue_id,
          attempt: attempt,
          due_in_ms: max(0, due_at_ms - now_ms),
          identifier: Map.get(retry, :identifier),
          issue_url: Map.get(retry, :issue_url),
          error: Map.get(retry, :error),
          worker_host: Map.get(retry, :worker_host),
          workspace_path: Map.get(retry, :workspace_path)
        }
      end)

    blocked =
      state.blocked
      |> Enum.map(fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: Map.get(metadata, :identifier),
          issue_url: blocked_issue_url(metadata),
          state: blocked_issue_state(metadata),
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: Map.get(metadata, :session_id),
          error: Map.get(metadata, :error),
          blocked_at: Map.get(metadata, :blocked_at),
          last_codex_timestamp: Map.get(metadata, :last_codex_timestamp),
          last_codex_message: Map.get(metadata, :last_codex_message),
          last_codex_event: Map.get(metadata, :last_codex_event)
        }
      end)

    {:reply,
     %{
       running: running,
       retrying: retrying,
       blocked: blocked,
       linear: LinearCoordinator.report(state),
       codex_totals: state.codex_totals,
       rate_limits: Map.get(state, :codex_rate_limits),
       polling: %{
         checking?: state.poll_check_in_progress == true,
         next_poll_in_ms: next_poll_in_ms(state.next_poll_due_at_ms, now_ms),
         poll_interval_ms: state.poll_interval_ms
       }
     }, state}
  end

  def handle_call(:request_refresh, _from, state) do
    now_ms = System.monotonic_time(:millisecond)
    already_due? = is_integer(state.next_poll_due_at_ms) and state.next_poll_due_at_ms <= now_ms
    coalesced = state.poll_check_in_progress == true or already_due?
    state = if coalesced, do: state, else: schedule_tick(state, 0)

    {:reply,
     %{
       queued: true,
       coalesced: coalesced,
       requested_at: DateTime.utc_now(),
       operations: ["poll", "reconcile"]
     }, state}
  end

  defp blocked_issue_state(%{issue: %Issue{state: state}}), do: state
  defp blocked_issue_state(_metadata), do: nil

  defp blocked_issue_url(%{issue: %Issue{url: url}}), do: url
  defp blocked_issue_url(_metadata), do: nil

  defp integrate_codex_update(running_entry, %{event: event, timestamp: timestamp} = update) do
    token_delta = extract_token_delta(running_entry, update)
    codex_input_tokens = Map.get(running_entry, :codex_input_tokens, 0)
    codex_output_tokens = Map.get(running_entry, :codex_output_tokens, 0)
    codex_total_tokens = Map.get(running_entry, :codex_total_tokens, 0)
    codex_app_server_pid = Map.get(running_entry, :codex_app_server_pid)
    last_reported_input = Map.get(running_entry, :codex_last_reported_input_tokens, 0)
    last_reported_output = Map.get(running_entry, :codex_last_reported_output_tokens, 0)
    last_reported_total = Map.get(running_entry, :codex_last_reported_total_tokens, 0)
    turn_count = Map.get(running_entry, :turn_count, 0)

    {
      Map.merge(running_entry, %{
        last_codex_timestamp: timestamp,
        last_codex_message: summarize_codex_update(update),
        session_id: session_id_for_update(running_entry.session_id, update),
        last_codex_event: event,
        codex_app_server_pid: codex_app_server_pid_for_update(codex_app_server_pid, update),
        codex_input_tokens: codex_input_tokens + token_delta.input_tokens,
        codex_output_tokens: codex_output_tokens + token_delta.output_tokens,
        codex_total_tokens: codex_total_tokens + token_delta.total_tokens,
        codex_last_reported_input_tokens: max(last_reported_input, token_delta.input_reported),
        codex_last_reported_output_tokens: max(last_reported_output, token_delta.output_reported),
        codex_last_reported_total_tokens: max(last_reported_total, token_delta.total_reported),
        turn_count: turn_count_for_update(turn_count, running_entry.session_id, update)
      }),
      token_delta
    }
  end

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_binary(pid),
       do: pid

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_integer(pid),
       do: Integer.to_string(pid)

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid}) when is_list(pid),
    do: to_string(pid)

  defp codex_app_server_pid_for_update(existing, _update), do: existing

  defp session_id_for_update(_existing, %{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp session_id_for_update(existing, _update), do: existing

  defp turn_count_for_update(existing_count, existing_session_id, %{
         event: :session_started,
         session_id: session_id
       })
       when is_integer(existing_count) and is_binary(session_id) do
    if session_id == existing_session_id do
      existing_count
    else
      existing_count + 1
    end
  end

  defp turn_count_for_update(existing_count, _existing_session_id, _update)
       when is_integer(existing_count),
       do: existing_count

  defp turn_count_for_update(_existing_count, _existing_session_id, _update), do: 0

  defp summarize_codex_update(update) do
    %{
      event: update[:event],
      message: update[:payload] || update[:raw],
      timestamp: update[:timestamp]
    }
  end

  defp schedule_tick(%State{} = state, delay_ms) when is_integer(delay_ms) and delay_ms >= 0 do
    if is_reference(state.tick_timer_ref) do
      Process.cancel_timer(state.tick_timer_ref)
    end

    tick_token = make_ref()
    timer_ref = Process.send_after(self(), {:tick, tick_token}, delay_ms)

    %{
      state
      | tick_timer_ref: timer_ref,
        tick_token: tick_token,
        next_poll_due_at_ms: System.monotonic_time(:millisecond) + delay_ms
    }
  end

  defp schedule_poll_cycle_start do
    :timer.send_after(@poll_transition_render_delay_ms, self(), :run_poll_cycle)
    :ok
  end

  defp next_poll_in_ms(nil, _now_ms), do: nil

  defp next_poll_in_ms(next_poll_due_at_ms, now_ms) when is_integer(next_poll_due_at_ms) do
    max(0, next_poll_due_at_ms - now_ms)
  end

  defp pop_running_entry(state, issue_id) do
    {Map.get(state.running, issue_id), %{state | running: Map.delete(state.running, issue_id)}}
  end

  defp record_session_completion_totals(state, running_entry) when is_map(running_entry) do
    runtime_seconds = running_seconds(running_entry.started_at, DateTime.utc_now())

    codex_totals =
      apply_token_delta(
        state.codex_totals,
        %{
          input_tokens: 0,
          output_tokens: 0,
          total_tokens: 0,
          seconds_running: runtime_seconds
        }
      )

    %{state | codex_totals: codex_totals}
  end

  defp record_session_completion_totals(state, _running_entry), do: state

  defp refresh_runtime_config(%{polling_enabled: false} = state), do: state

  defp refresh_runtime_config(%State{} = state) do
    config = Config.settings!()

    %{
      state
      | poll_interval_ms: config.polling.interval_ms,
        max_concurrent_agents: config.agent.max_concurrent_agents
    }
  end

  defp retry_candidate_issue?(%Issue{} = issue, terminal_states) do
    candidate_issue?(issue, active_state_set(), terminal_states)
  end

  defp dispatch_slots_available?(%Issue{} = issue, %State{} = state) do
    available_slots(state) > 0 and state_slots_available?(issue, state.running)
  end

  defp apply_codex_token_delta(
         %{codex_totals: codex_totals} = state,
         %{input_tokens: input, output_tokens: output, total_tokens: total} = token_delta
       )
       when is_integer(input) and is_integer(output) and is_integer(total) do
    %{state | codex_totals: apply_token_delta(codex_totals, token_delta)}
  end

  defp apply_codex_token_delta(state, _token_delta), do: state

  defp apply_codex_rate_limits(%State{} = state, update) when is_map(update) do
    case extract_rate_limits(update) do
      %{} = rate_limits ->
        %{state | codex_rate_limits: rate_limits}

      _ ->
        state
    end
  end

  defp apply_codex_rate_limits(state, _update), do: state

  defp apply_token_delta(codex_totals, token_delta) do
    input_tokens = Map.get(codex_totals, :input_tokens, 0) + token_delta.input_tokens
    output_tokens = Map.get(codex_totals, :output_tokens, 0) + token_delta.output_tokens
    total_tokens = Map.get(codex_totals, :total_tokens, 0) + token_delta.total_tokens

    seconds_running =
      Map.get(codex_totals, :seconds_running, 0) + Map.get(token_delta, :seconds_running, 0)

    %{
      input_tokens: max(0, input_tokens),
      output_tokens: max(0, output_tokens),
      total_tokens: max(0, total_tokens),
      seconds_running: max(0, seconds_running)
    }
  end

  defp extract_token_delta(running_entry, %{event: _, timestamp: _} = update) do
    running_entry = running_entry || %{}
    usage = extract_token_usage(update)

    {
      compute_token_delta(
        running_entry,
        :input,
        usage,
        :codex_last_reported_input_tokens
      ),
      compute_token_delta(
        running_entry,
        :output,
        usage,
        :codex_last_reported_output_tokens
      ),
      compute_token_delta(
        running_entry,
        :total,
        usage,
        :codex_last_reported_total_tokens
      )
    }
    |> Tuple.to_list()
    |> then(fn [input, output, total] ->
      %{
        input_tokens: input.delta,
        output_tokens: output.delta,
        total_tokens: total.delta,
        input_reported: input.reported,
        output_reported: output.reported,
        total_reported: total.reported
      }
    end)
  end

  defp compute_token_delta(running_entry, token_key, usage, reported_key) do
    next_total = get_token_usage(usage, token_key)
    prev_reported = Map.get(running_entry, reported_key, 0)

    delta =
      if is_integer(next_total) and next_total >= prev_reported do
        next_total - prev_reported
      else
        0
      end

    %{
      delta: max(delta, 0),
      reported: if(is_integer(next_total), do: next_total, else: prev_reported)
    }
  end

  defp extract_token_usage(update) do
    payloads = [
      update[:usage],
      Map.get(update, "usage"),
      Map.get(update, :usage),
      update[:payload],
      Map.get(update, "payload"),
      update
    ]

    Enum.find_value(payloads, &absolute_token_usage_from_payload/1) ||
      Enum.find_value(payloads, &turn_completed_usage_from_payload/1) ||
      %{}
  end

  defp extract_rate_limits(update) do
    rate_limits_from_payload(update[:rate_limits]) ||
      rate_limits_from_payload(Map.get(update, "rate_limits")) ||
      rate_limits_from_payload(Map.get(update, :rate_limits)) ||
      rate_limits_from_payload(update[:payload]) ||
      rate_limits_from_payload(Map.get(update, "payload")) ||
      rate_limits_from_payload(update)
  end

  defp absolute_token_usage_from_payload(payload) when is_map(payload) do
    absolute_paths = [
      ["params", "msg", "payload", "info", "total_token_usage"],
      [:params, :msg, :payload, :info, :total_token_usage],
      ["params", "msg", "info", "total_token_usage"],
      [:params, :msg, :info, :total_token_usage],
      ["params", "tokenUsage", "total"],
      [:params, :tokenUsage, :total],
      ["tokenUsage", "total"],
      [:tokenUsage, :total]
    ]

    explicit_map_at_paths(payload, absolute_paths)
  end

  defp absolute_token_usage_from_payload(_payload), do: nil

  defp turn_completed_usage_from_payload(payload) when is_map(payload) do
    method = Map.get(payload, "method") || Map.get(payload, :method)

    if method in ["turn/completed", :turn_completed] do
      direct =
        Map.get(payload, "usage") ||
          Map.get(payload, :usage) ||
          map_at_path(payload, ["params", "usage"]) ||
          map_at_path(payload, [:params, :usage])

      if is_map(direct) and integer_token_map?(direct), do: direct
    end
  end

  defp turn_completed_usage_from_payload(_payload), do: nil

  defp rate_limits_from_payload(payload) when is_map(payload) do
    direct = Map.get(payload, "rate_limits") || Map.get(payload, :rate_limits)

    cond do
      rate_limits_map?(direct) ->
        direct

      rate_limits_map?(payload) ->
        payload

      true ->
        rate_limit_payloads(payload)
    end
  end

  defp rate_limits_from_payload(payload) when is_list(payload) do
    rate_limit_payloads(payload)
  end

  defp rate_limits_from_payload(_payload), do: nil

  defp rate_limit_payloads(payload) when is_map(payload) do
    Map.values(payload)
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limit_payloads(payload) when is_list(payload) do
    payload
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limits_map?(payload) when is_map(payload) do
    limit_id =
      Map.get(payload, "limit_id") ||
        Map.get(payload, :limit_id) ||
        Map.get(payload, "limit_name") ||
        Map.get(payload, :limit_name)

    has_buckets =
      Enum.any?(
        ["primary", :primary, "secondary", :secondary, "credits", :credits],
        &Map.has_key?(payload, &1)
      )

    !is_nil(limit_id) and has_buckets
  end

  defp rate_limits_map?(_payload), do: false

  defp explicit_map_at_paths(payload, paths) when is_map(payload) and is_list(paths) do
    Enum.find_value(paths, fn path ->
      value = map_at_path(payload, path)

      if is_map(value) and integer_token_map?(value), do: value
    end)
  end

  defp explicit_map_at_paths(_payload, _paths), do: nil

  defp map_at_path(payload, path) when is_map(payload) and is_list(path) do
    Enum.reduce_while(path, payload, fn key, acc ->
      if is_map(acc) and Map.has_key?(acc, key) do
        {:cont, Map.get(acc, key)}
      else
        {:halt, nil}
      end
    end)
  end

  defp map_at_path(_payload, _path), do: nil

  defp integer_token_map?(payload) do
    token_fields = [
      :input_tokens,
      :output_tokens,
      :total_tokens,
      :prompt_tokens,
      :completion_tokens,
      :inputTokens,
      :outputTokens,
      :totalTokens,
      :promptTokens,
      :completionTokens,
      "input_tokens",
      "output_tokens",
      "total_tokens",
      "prompt_tokens",
      "completion_tokens",
      "inputTokens",
      "outputTokens",
      "totalTokens",
      "promptTokens",
      "completionTokens"
    ]

    token_fields
    |> Enum.any?(fn field ->
      value = payload_get(payload, field)
      !is_nil(integer_like(value))
    end)
  end

  defp get_token_usage(usage, :input),
    do:
      payload_get(usage, [
        "input_tokens",
        "prompt_tokens",
        :input_tokens,
        :prompt_tokens,
        :input,
        "promptTokens",
        :promptTokens,
        "inputTokens",
        :inputTokens
      ])

  defp get_token_usage(usage, :output),
    do:
      payload_get(usage, [
        "output_tokens",
        "completion_tokens",
        :output_tokens,
        :completion_tokens,
        :output,
        :completion,
        "outputTokens",
        :outputTokens,
        "completionTokens",
        :completionTokens
      ])

  defp get_token_usage(usage, :total),
    do:
      payload_get(usage, [
        "total_tokens",
        "total",
        :total_tokens,
        :total,
        "totalTokens",
        :totalTokens
      ])

  defp payload_get(payload, fields) when is_list(fields) do
    Enum.find_value(fields, fn field -> map_integer_value(payload, field) end)
  end

  defp payload_get(payload, field), do: map_integer_value(payload, field)

  defp map_integer_value(payload, field) do
    if is_map(payload) do
      value = Map.get(payload, field)
      integer_like(value)
    else
      nil
    end
  end

  defp running_seconds(%DateTime{} = started_at, %DateTime{} = now) do
    max(0, DateTime.diff(now, started_at, :second))
  end

  defp running_seconds(_started_at, _now), do: 0

  defp integer_like(value) when is_integer(value) and value >= 0, do: value

  defp integer_like(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {num, _} when num >= 0 -> num
      _ -> nil
    end
  end

  defp integer_like(_value), do: nil

  defp json_data(value) do
    case Jason.encode(value) do
      {:ok, encoded} -> if Jason.decode!(encoded) == value, do: :ok, else: {:error, :inputs_must_be_json_data}
      {:error, _} -> {:error, :inputs_must_be_json_data}
    end
  end

  defp initial_orchestrator_settings(opts) do
    if Keyword.has_key?(opts, :workstream_store_path) do
      max = Keyword.get(opts, :max_concurrent_agents, 1)

      if is_integer(max) and max > 0,
        do: {:ok, %{polling: %{interval_ms: 30_000}, agent: %{max_concurrent_agents: max}}},
        else: {:error, :invalid_workstream_capacity}
    else
      Config.settings()
    end
  end

  defp init_workstreams(state, opts) do
    case Keyword.get(opts, :workstream_store_path) do
      nil ->
        {:ok, state}

      path ->
        if Keyword.get(opts, :polling, false) do
          {:error, :durable_workstreams_require_manual_mode}
        else
          Code.ensure_loaded!(SymphonyElixir.Workstream)
          Code.ensure_loaded!(WorkstreamRun)
          Code.ensure_loaded!(WorkstreamRunner)
          Code.ensure_loaded!(SymphonyElixir.ValidationPolicy)
          Code.ensure_loaded!(SymphonyElixir.Validation)
          Code.ensure_loaded!(SymphonyElixir.ValidationCommand)

          with {:ok, store} <- WorkstreamStore.start_link(path: path, owner: self()),
               {:ok, runs} <- WorkstreamStore.load(store) do
            {:ok, store_path} = SymphonyElixir.PathSafety.canonicalize(path)
            durable = %{store: store, store_path: store_path, runs: Map.new(runs, &{&1.id, &1}), workers: %{}, cancellations: %{}, opts: opts}
            {:ok, %{state | workstreams: durable, polling_enabled: false}}
          end
        end
    end
  end

  defp durable_run(%{workstreams: nil}, _id), do: nil
  defp durable_run(state, id), do: Map.get(state.workstreams.runs, id)

  defp queue_durable_workstream(%{workstreams: nil}, _event_id, _task_id, _path, _inputs, _opts), do: {:error, :durable_workstreams_disabled}

  defp queue_durable_workstream(state, event_id, task_id, path, inputs, opts) do
    event = %{kind: :queued, task_id: task_id, fingerprint: workstream_fingerprint({task_id, path, inputs, opts})}

    case check_workstream_event(state, event_id, event) do
      {:duplicate, id} -> {:ok, id, state}
      {:error, _} = error -> error
      :new -> queue_new_workstream_event(state, event_id, task_id, path, inputs, opts, event)
    end
  end

  defp queue_new_workstream_event(state, event_id, task_id, path, inputs, opts, event) do
    existing = Enum.find_value(state.workstreams.runs, fn {_id, run} -> if run.task_id == task_id, do: run end)

    with :ok <- json_data(inputs),
         true <- is_binary(task_id) and task_id != "",
         {:ok, run} <- existing_or_new_workstream(state, existing, task_id, path, inputs, opts) do
      case WorkstreamStore.commit(state.workstreams.store, run, event_id, event) do
        :ok -> {:ok, run.id, put_in(state.workstreams.runs[run.id], run)}
        {:duplicate, id} -> {:ok, id, state}
        {:error, reason} -> {:error, reason}
      end
    else
      false -> {:error, :invalid_event_or_task_identity}
      {:error, _} = error -> error
    end
  end

  defp check_workstream_event(%{workstreams: nil}, _id, _event), do: {:error, :durable_workstreams_disabled}

  defp check_workstream_event(state, id, event) do
    case WorkstreamStore.event(state.workstreams.store, id) do
      :not_found -> :new
      {:ok, %{event: ^event, run_id: run_id}} -> {:duplicate, run_id}
      {:ok, _} -> {:error, :event_identity_conflict}
      {:error, _} = error -> error
    end
  end

  # Caller IDs cannot occupy the coordinator's operation-event namespace.
  defp input_event_key(id) when is_binary(id) and id != "", do: "input/" <> id
  defp input_event_key(_id), do: nil

  defp workstream_fingerprint(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value)) |> Base.encode16(case: :lower)

  defp register_workstream_process(state, run, identity) do
    operation = run.operations[run.current_attempt_id]

    with {:ok, identity} <- validate_external_process(run, operation, identity) do
      case operation[:external_process] do
        ^identity ->
          {:reply, :ok, state}

        nil ->
          updated = put_in(run.operations[operation.id][:external_process], identity)
          state = commit_workstream(state, updated, operation.id <> "/external-process", %{kind: :external_process_registered})
          {:reply, :ok, state}

        _ ->
          {:reply, {:error, :process_identity_conflict}, state}
      end
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp validate_external_process(run, operation, %{"kind" => "systemd-unit"} = identity),
    do: SymphonyElixir.WorkerOperation.validate_registration_identity(identity, operation.id, run.execution[:worker_control])

  defp validate_external_process(_run, operation, identity),
    do: SymphonyElixir.WorkstreamCancellation.validate_identity(identity, operation.id)

  defp existing_or_new_workstream(_state, run, _task_id, _path, _inputs, _opts) when is_map(run), do: {:ok, run}

  defp existing_or_new_workstream(state, nil, task_id, path, inputs, opts) do
    with true <-
           Enum.all?(opts, fn {key, value} ->
             (key in [
                :workspace,
                :workspace_root,
                :codex_command,
                :branch,
                :issue_id,
                :definition_sha256,
                :authentication_reference,
                :validation_policy,
                :validation_archive,
                :validation_scratch,
                :validation_base
              ] and is_binary(value)) or (key == :worker_control and is_map(value)) or
               (key == :secret_environment_names and is_list(value) and Enum.all?(value, &is_binary/1))
           end),
         {:ok, definition, workspace} <- WorkstreamRunner.prepare(path, inputs, opts),
         :ok <- check_workstream_definition(definition, opts),
         :ok <- database_outside_workspace(state.workstreams.store_path, workspace),
         false <- Enum.any?(state.workstreams.runs, fn {_id, run} -> run.workspace == workspace end),
         {:ok, root} <- SymphonyElixir.PathSafety.canonicalize(opts[:workspace_root]),
         execution_opts = Keyword.put(opts, :workspace_root, root),
         {:ok, execution} <- WorkstreamRunner.pin_execution_context(execution_opts, workspace, definition) do
      {:ok, WorkstreamRun.new(task_id, definition, workspace, execution_opts, execution)}
    else
      true -> {:error, :workspace_already_owned}
      false -> {:error, :invalid_workstream_execution_options}
      {:error, _} = error -> error
    end
  end

  defp check_workstream_definition(definition, opts) do
    expected = Keyword.get(opts, :definition_sha256)
    if is_nil(expected) or expected == workstream_fingerprint(definition), do: :ok, else: {:error, :workstream_definition_changed_before_enqueue}
  end

  defp database_outside_workspace(database, workspace) do
    if database == workspace or String.starts_with?(database, workspace <> "/"), do: {:error, :database_overlaps_worker_workspace}, else: :ok
  end

  defp commit_workstream(state, run, event_id, event) do
    send(self(), :linear_process)

    case WorkstreamStore.commit(state.workstreams.store, run, event_id, event) do
      :ok ->
        put_in(state.workstreams.runs[run.id], run)

      {:duplicate, id} when id == run.id ->
        case {WorkstreamStore.event(state.workstreams.store, event_id), WorkstreamStore.fetch(state.workstreams.store, id)} do
          {{:ok, %{event: ^event}}, {:ok, stored}} -> put_in(state.workstreams.runs[id], stored)
          _ -> exit(:durable_workstream_event_conflict)
        end

      {:duplicate, _id} ->
        exit(:durable_workstream_event_conflict)

      {:error, reason} ->
        exit({:durable_workstream_commit_failed, reason})
    end
  end

  defp advance_workstreams(%{workstreams: nil} = state), do: state

  defp advance_workstreams(state) do
    Enum.reduce(state.workstreams.runs, state, fn {id, _}, acc ->
      run = durable_run(acc, id)

      cond do
        run.status != :ready -> acc
        not LinearCoordinator.ready?(acc, run) -> acc
        not WorkstreamRun.compatible_policy?(run) -> block_workstream_policy(acc, run)
        run.definition.stages[run.stage_id].type != :human_wait and workstream_capacity_used(acc) >= acc.max_concurrent_agents -> acc
        true -> start_workstream_stage(acc, run)
      end
    end)
  end

  defp workstream_capacity_used(state) do
    Enum.count(state.workstreams.runs, fn {_id, run} ->
      operation = run.operations[run.current_attempt_id]

      run.status in [:executing, :reconciling] or
        (run.status in [:policy_blocked, :stopped] and workstream_operation_unconfirmed?(run, operation))
    end) + map_size(state.running)
  end

  defp workstream_operation_unconfirmed?(_run, %{status: :executing}), do: true

  defp workstream_operation_unconfirmed?(run, %{status: :canceled}),
    do: not trusted_workstream_termination?(run)

  defp workstream_operation_unconfirmed?(_run, _operation), do: false

  defp start_workstream_stage(state, run) do
    run = WorkstreamRun.start_stage(run)
    state = commit_workstream(state, run, run.current_attempt_id <> "/start", %{kind: :stage_started})

    if run.status == :waiting_for_answer do
      state
    else
      owner = self()

      case Task.Supervisor.start_child(state.task_supervisor, fn -> AgentRunner.run_workstream(run, owner, state.workstreams.opts) end) do
        {:ok, pid} ->
          ref = Process.monitor(pid)
          identity = %{pid: inspect(pid), boot_id: workstream_boot_id(), node: to_string(node())}
          run = WorkstreamRun.record_worker(run, identity)
          state = commit_workstream(state, run, run.current_attempt_id <> "/worker", %{kind: :worker_registered})
          send(pid, :workstream_start)
          put_in(state.workstreams.workers[run.current_attempt_id], %{pid: pid, ref: ref, run_id: run.id})

        {:error, reason} ->
          # Even an unsuccessful spawn is reconciled explicitly before any replacement.
          updated = WorkstreamRun.uncertain(run, {:spawn_failed, reason})
          commit_workstream(state, updated, run.current_attempt_id <> "/spawn_failed", %{kind: :reconciliation_required})
      end
    end
  end

  defp accept_workstream_result(%{workstreams: nil} = state, _run, _attempt, _pid, _result), do: state

  defp accept_workstream_result(state, run_id, attempt_id, pid, result) do
    case {durable_run(state, run_id), Map.get(state.workstreams.workers, attempt_id)} do
      {%{current_attempt_id: ^attempt_id, status: status} = run, %{pid: ^pid, ref: ref}} when status in [:executing, :reconciling] ->
        {updated, event_id, event} =
          case result do
            {:uncertain, reason} -> {WorkstreamRun.uncertain(run, reason), attempt_id <> "/uncertain", %{kind: :reconciliation_required}}
            _ -> {WorkstreamRun.finish_stage(run, result), attempt_id <> "/complete", %{kind: :stage_completed}}
          end

        state = commit_workstream(state, updated, event_id, event)
        send(pid, {:workstream_ack, attempt_id})
        Process.demonitor(ref, [:flush])
        state = update_in(state.workstreams.workers, &Map.delete(&1, attempt_id))
        send(self(), :advance_workstreams)
        state

      _ ->
        state
    end
  end

  defp workstream_worker_down(%{workstreams: nil} = state, _ref, _reason), do: state

  defp workstream_worker_down(state, ref, reason) do
    case Enum.find(state.workstreams.workers, fn {_id, worker} -> worker.ref == ref end) do
      nil ->
        state

      {attempt_id, worker} ->
        run = durable_run(state, worker.run_id)
        state = update_in(state.workstreams.workers, &Map.delete(&1, attempt_id))

        if run.status == :stopped do
          state
        else
          updated = WorkstreamRun.uncertain(run, {:worker_down_without_receipt, reason})
          commit_workstream(state, updated, attempt_id <> "/down", %{kind: :reconciliation_required})
        end
    end
  end

  defp recover_workstreams(%{workstreams: nil} = state), do: state

  defp recover_workstreams(state) do
    state = reconcile_stopped_workstreams(state)
    acknowledge_committed_receipts(state)

    Enum.reduce(state.workstreams.runs, state, fn {_id, run}, acc ->
      cond do
        linear_stopped?(acc, run) -> stop_linear_workstream(acc, run)
        run.status in [:complete, :blocked, :policy_blocked, :stopped] -> acc
        not WorkstreamRun.compatible_policy?(run) -> block_workstream_policy(acc, run)
        run.status in [:executing, :reconciling] -> recover_workstream(acc, run)
        true -> acc
      end
    end)
  end

  defp linear_stopped?(%{linear: nil}, _run), do: false

  defp linear_stopped?(state, run) do
    Enum.any?(state.linear.tasks, fn {_id, task} -> task.run_id == run.id and task.status == :stopped end) or
      Enum.any?(state.linear.events, fn {_id, record} -> record.event.kind == :stop and record.event.issue_id == run.issue_id end)
  end

  defp stop_linear_workstream(state, %{status: :stopped} = run), do: request_workstream_cancellation(state, run, nil)

  defp stop_linear_workstream(state, run) do
    worker = state.workstreams.workers[run.current_attempt_id]
    operation = run.operations[run.current_attempt_id]
    pid = if worker, do: worker.pid, else: if(operation, do: find_workstream_worker(state.task_supervisor, run, operation))
    outcome = if workstream_operation_unconfirmed?(run, operation), do: :unknown, else: :terminated
    stopped = Map.merge(run, %{status: :stopped, phase: :stopped, cancellation: %{termination: outcome}})
    stopped = if operation && outcome == :terminated, do: WorkstreamRun.confirm_termination(stopped), else: stopped
    state = commit_workstream(state, stopped, run.id <> "/linear-stop", %{kind: :stopped})
    state = request_workstream_cancellation(state, stopped, pid)

    if pid do
      # A BEAM exit alone is not proof that a remote process tree has stopped.
      Task.Supervisor.terminate_child(state.task_supervisor, pid)
      if worker, do: Process.demonitor(worker.ref, [:flush])
      update_in(state.workstreams.workers, &Map.delete(&1, run.current_attempt_id))
    else
      state
    end
  end

  defp request_termination(canceller, run, operation, pid) do
    case canceller.(run, operation, pid) do
      :terminated -> :terminated
      _ -> :unknown
    end
  rescue
    _ -> :unknown
  catch
    _, _ -> :unknown
  end

  defp cancellation_down(%{workstreams: nil}, _ref), do: :unhandled

  defp cancellation_down(state, ref) do
    case Enum.find(state.workstreams.cancellations, fn {_id, task} -> task.ref == ref end) do
      {id, _task} -> {:handled, update_in(state.workstreams.cancellations, &Map.delete(&1, id))}
      nil -> :unhandled
    end
  end

  defp reconcile_stopped_workstreams(%{workstreams: nil} = state), do: state

  defp reconcile_stopped_workstreams(state) do
    Enum.reduce(state.workstreams.runs, state, fn {_id, run}, acc ->
      if run.status == :stopped, do: reconcile_stopped_workstream(acc, run), else: acc
    end)
  end

  defp reconcile_stopped_workstream(state, run) do
    if trusted_workstream_termination?(run) do
      updated = WorkstreamRun.confirm_termination(run)

      if updated == run do
        state
      else
        event_id = "#{run.current_attempt_id}/termination-normalized-v1"
        commit_workstream(state, updated, event_id, %{kind: :termination_bookkeeping_reconciled})
      end
    else
      request_workstream_cancellation(state, run, nil)
    end
  end

  defp request_workstream_cancellation(state, run, worker_pid) do
    operation = run.operations[run.current_attempt_id]

    if cancellation_confirmation_pending?(run, operation) and not Map.has_key?(state.workstreams.cancellations, operation.id) do
      canceller = Keyword.get(state.workstreams.opts, :workstream_canceller, &SymphonyElixir.WorkstreamCancellation.cancel/3)

      task =
        Task.Supervisor.async_nolink(state.task_supervisor, fn ->
          {:workstream_canceled, run.id, operation.id, request_termination(canceller, run, operation, worker_pid)}
        end)

      put_in(state.workstreams.cancellations[operation.id], task)
    else
      state
    end
  end

  defp accept_workstream_cancellation(%{workstreams: nil} = state, _ref, _run_id, _attempt_id, _outcome), do: state

  defp accept_workstream_cancellation(state, ref, run_id, attempt_id, outcome) do
    case state.workstreams.cancellations[attempt_id] do
      %Task{ref: ^ref} ->
        Process.demonitor(ref, [:flush])
        state = update_in(state.workstreams.cancellations, &Map.delete(&1, attempt_id))
        run = durable_run(state, run_id)

        if outcome == :terminated and not is_nil(run) and run.status == :stopped and run.current_attempt_id == attempt_id do
          updated = WorkstreamRun.confirm_termination(run)
          state = commit_workstream(state, updated, attempt_id <> "/termination-confirmed-v2", %{kind: :termination_confirmed})
          send(self(), :advance_workstreams)
          state
        else
          state
        end

      _ ->
        state
    end
  end

  defp cancellation_confirmation_pending?(%{status: :stopped} = run, %{id: id, status: status})
       when status in [:executing, :canceled] do
    id == run.current_attempt_id and not trusted_workstream_termination?(run)
  end

  defp cancellation_confirmation_pending?(_run, _operation), do: false

  defp trusted_workstream_termination?(%{cancellation: %{termination: :terminated}}), do: true
  defp trusted_workstream_termination?(_run), do: false

  defp block_workstream_policy(state, run) do
    updated = %{run | status: :policy_blocked, phase: :reconciling}
    commit_workstream(state, updated, "#{run.id}/policy-blocked/#{WorkstreamRun.policy().lifecycle_sha256}", %{kind: :incompatible_service_policy})
  end

  defp acknowledge_committed_receipts(state) do
    Enum.each(Task.Supervisor.children(state.task_supervisor), fn pid ->
      case Process.info(pid, :dictionary) do
        {:dictionary, dictionary} ->
          case Keyword.get(dictionary, :symphony_workstream) do
            %{run_id: run_id, attempt_id: id} ->
              run = durable_run(state, run_id)
              if run && match?(%{status: :completed}, run.operations[id]), do: send(pid, {:workstream_ack, id})

            _ ->
              :ok
          end

        _ ->
          :ok
      end
    end)
  end

  defp recover_workstream(state, run) do
    operation = Map.fetch!(run.operations, run.current_attempt_id)
    worker = find_workstream_worker(state.task_supervisor, run, operation)

    if is_pid(worker) do
      workers = state.workstreams.workers

      if Map.has_key?(workers, run.current_attempt_id) do
        send(worker, {:workstream_redeliver, self()})
        state
      else
        ref = Process.monitor(worker)
        send(worker, :workstream_start)
        send(worker, {:workstream_redeliver, self()})
        put_in(state.workstreams.workers[run.current_attempt_id], %{pid: worker, ref: ref, run_id: run.id})
      end
    else
      reconciler = Keyword.get(state.workstreams.opts, :workstream_reconciler, fn _run, _operation -> :unknown end)
      outcome = reconciler.(run, operation)

      updated =
        case outcome do
          {:completed, result} -> WorkstreamRun.finish_stage(run, result)
          :terminated when operation.type == :agent -> WorkstreamRun.retry_transport(run)
          :not_applied -> WorkstreamRun.retry_transport(run)
          other -> WorkstreamRun.uncertain(run, other)
        end

      event_id = "#{operation.id}/reconcile/#{:crypto.strong_rand_bytes(8) |> Base.encode16()}"
      commit_workstream(state, updated, event_id, %{kind: :reconciled, outcome: inspect(outcome)})
    end
  end

  defp find_workstream_worker(supervisor, run, operation) do
    if operation.worker && operation.worker.boot_id == workstream_boot_id() do
      Enum.find(Task.Supervisor.children(supervisor), fn pid ->
        case Process.info(pid, :dictionary) do
          {:dictionary, dictionary} ->
            identity = Keyword.get(dictionary, :symphony_workstream)
            identity == %{run_id: run.id, attempt_id: run.current_attempt_id, operation_id: operation.id} and operation.worker.pid == inspect(pid)

          nil ->
            false
        end
      end)
    end
  end

  defp workstream_boot_id do
    key = {__MODULE__, :workstream_boot_id}

    case :persistent_term.get(key, nil) do
      nil ->
        id = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
        :persistent_term.put(key, id)
        id

      id ->
        id
    end
  end
end
