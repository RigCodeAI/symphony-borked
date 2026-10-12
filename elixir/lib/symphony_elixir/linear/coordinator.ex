defmodule SymphonyElixir.Linear.Coordinator do
  @moduledoc "Durable native event intake owned by the existing coordinator."

  alias SymphonyElixir.Linear.{AgentMapping, Delegation}
  alias SymphonyElixir.{Config, WorkstreamRun, WorkstreamStore}

  @spec init(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def init(state, opts) do
    case Keyword.get(opts, :linear_delegation) do
      nil ->
        {:ok, state}

      config ->
        Code.ensure_loaded!(SymphonyElixir.Linear.Webhook)

        with {:ok, config} <- Delegation.validate(config),
             false <- is_nil(state.workstreams),
             {:ok, stored} <- WorkstreamStore.linear_load(state.workstreams.store) do
          tasks = Map.new(stored.tasks, fn {id, task} -> {id, Map.merge(task, %{authorized_stage: nil, authorized_until: 0, check_at: 0})} end)
          stored = Map.put(stored, :tasks, tasks)
          send(self(), :linear_process)
          {:ok, Map.put(state, :linear, Map.merge(stored, %{config: config, job: nil, opts: opts, timer: nil}))}
        else
          true -> {:error, :linear_requires_durable_coordinator}
          {:error, _} = error -> error
        end
    end
  end

  @spec receive_event(map(), map(), (map(), map() -> map())) :: {:ok, map(), term()} | {:error, atom()}
  def receive_event(%{linear: nil}, _event, _stop), do: {:error, :linear_disabled}

  def receive_event(state, event, stop) do
    config = state.linear.config

    with {:ok, route} <- event_route(event, config),
         true <- event.kind != :issue_updated or Map.has_key?(state.linear.tasks, event.issue_id) do
      case WorkstreamStore.linear_receive(state.workstreams.store, event.id, event) do
        :ok ->
          record = %{id: event.id, event: event, status: :pending}
          state = put_in(state.linear.events[event.id], record)

          state =
            cond do
              event.kind == :stop and not stop_matches_assignment?(state.linear.tasks[event.issue_id], route, event) ->
                commit(state, record, nil, :ignored_old_assignment)

              event.kind == :stop ->
                stop_task(state, event.issue_id, event, stop)

              event.kind == :created ->
                maybe_begin_reassignment(state, route, event, stop)

              event.kind in [:prompted, :issue_updated] and
                not current_assignment_event?(state.linear.tasks[event.issue_id], route, event) and
                  Map.has_key?(state.linear.tasks, event.issue_id) ->
                commit(state, record, nil, :ignored_old_assignment)

              true ->
                state
            end

          state =
            if event.kind == :issue_updated and current_assignment_event?(state.linear.tasks[event.issue_id], route, event) do
              task = state.linear.tasks[event.issue_id]
              save_task(state, Map.merge(task, %{authorized_stage: nil, authorized_until: 0, check_at: 0, scope_revision: (task[:scope_revision] || 0) + 1}))
            else
              state
            end

          send(self(), :linear_process)
          {:ok, state, :ok}

        {:duplicate, _record} = reply ->
          {:ok, state, reply}

        {:error, _} ->
          {:error, :linear_receipt_failed}
      end
    else
      false -> {:error, :unrelated_linear_event}
      {:error, _} -> {:error, :unrelated_linear_event}
    end
  end

  @spec process(map(), function()) :: map()
  def process(%{linear: nil} = state, _stop), do: state
  def process(%{linear: %{job: job}} = state, _stop) when not is_nil(job), do: state

  def process(state, stop) do
    state = schedule(state)

    pending = state.linear.events |> Enum.filter(fn {_id, record} -> record.status == :pending end) |> Enum.sort_by(fn {id, r} -> {r.event.timestamp, id} end)
    selected = Enum.find(pending, fn {_id, record} -> record.event.kind == :stop end) || List.first(pending)

    case pending_reassignment(state) do
      {issue_id, _task} ->
        process_reassignment(state, issue_id, stop)

      nil ->
        process_events_or_task(state, selected, stop)
    end
  end

  defp process_events_or_task(state, selected, stop) do
    case selected do
      {_id, %{event: %{kind: :stop}} = record} ->
        task = state.linear.tasks[record.event.issue_id]

        if stop_matches_assignment?(task, event_route_value(record.event, state.linear.config), record.event) do
          state |> stop_task(record.event.issue_id, record.event, stop) |> commit(record, nil, :handled) |> process(stop)
        else
          state |> commit(record, nil, :ignored_old_assignment) |> process(stop)
        end

      {_id, record} ->
        task = state.linear.tasks[record.event.issue_id]
        authenticated_route = event_route_value(record.event, state.linear.config)
        route = if record.event.kind == :issue_updated, do: task_route(task, state.linear.config), else: authenticated_route

        cond do
          task && task[:pending_reassignment] ->
            state

          task && (is_nil(authenticated_route) or not current_assignment_event?(task, authenticated_route, record.event)) ->
            state |> commit(record, nil, :ignored_old_assignment) |> process(stop)

          task && task.status == :stopped ->
            state |> commit(record, nil, :ignored_after_stop) |> process(stop)

          task && task.run_id && record.event.kind == :prompted && is_binary(record.event[:body]) ->
            start_job(state, {:reply, record.id, task[:scope_revision] || 0}, fn -> Delegation.inspect_event(record.event, route, state.linear.opts) end)

          task && task.run_id && record.event.kind in [:created, :prompted] ->
            state |> commit(record, nil, :duplicate_task) |> process(stop)

          record.event.kind == :prompted ->
            state |> commit(record, nil, :no_delegated_task) |> process(stop)

          true ->
            revision = if task, do: task[:scope_revision] || 0, else: nil
            start_job(state, {:event, record.id, revision}, fn -> inspect_for_run(state, record.event) end)
        end

      nil ->
        state |> sync_publications() |> process_task()
    end
  end

  defp inspect_for_run(state, event) do
    route = if event.kind == :issue_updated, do: task_route(state.linear.tasks[event.issue_id], state.linear.config), else: event_route_value(event, state.linear.config)

    with true <- not is_nil(route),
         {:ok, issue} <- Delegation.inspect_event(event, route, state.linear.opts),
         {:ok, prepared} <- prepare_run(state, event, issue, route) do
      {:ok, issue, prepared}
    else
      false -> {:error, :unknown_cloud_agent_identity}
      {:error, _} = error -> error
    end
  end

  defp prepare_run(_state, %{kind: :issue_updated}, _issue, _route), do: {:ok, nil}

  defp prepare_run(state, _event, issue, route) do
    case existing_run(state, issue.id) do
      nil -> Delegation.prepare(issue, route, state.linear.opts)
      existing -> qualify_existing(state, existing, route)
    end
  end

  defp qualify_existing(state, existing, route) do
    agent = existing.definition.stages[existing.definition.entry].agent

    with true <- route["agent_id"] == agent,
         :ok <- Delegation.execution_isolated(existing, route),
         :ok <- Delegation.qualify(existing.definition, agent, route, state.linear.opts) do
      {:ok, %{existing_run: existing.id, agent_id: agent}}
    else
      false -> {:error, :cloud_agent_run_mismatch}
      {:error, _} = error -> error
    end
  end

  defp event_route(event, config) do
    case event_route_value(event, config) do
      nil -> {:error, :unknown_cloud_agent_identity}
      route -> {:ok, route}
    end
  end

  defp event_route_value(event, config) do
    case AgentMapping.routes(config) do
      {:ok, routes} when event.kind == :issue_updated ->
        verified = event[:verified_app_user_id]

        case Enum.find(routes, fn route ->
               route["organization_id"] == event.organization_id and
                 (route["app_user_id"] == verified or (is_nil(verified) and length(routes) == 1)) and
                 (is_binary(verified) or length(routes) == 1)
             end) do
          nil -> nil
          route -> route
        end

      {:ok, routes} ->
        case Enum.find(routes, fn route ->
               route["organization_id"] == event.organization_id and
                 route["app_user_id"] == event[:app_user_id] and
                 route["oauth_client_id"] == event[:oauth_client_id] and
                 (event[:verified_app_user_id] == route["app_user_id"] or (is_nil(event[:verified_app_user_id]) and length(routes) == 1))
             end) do
          nil -> nil
          route -> route
        end

      _ ->
        nil
    end
  end

  defp route_identity(route) do
    Map.take(route, ["organization_id", "app_user_id", "oauth_client_id", "agent_id"])
  end

  defp task_route(nil, _config), do: nil

  defp task_route(task, config) do
    case AgentMapping.routes(config) do
      {:ok, routes} ->
        identity = task[:route] || %{}
        app_user_id = identity["app_user_id"] || task[:app_user_id] || task[:event][:app_user_id] || task[:event][:verified_app_user_id]
        oauth_client_id = identity["oauth_client_id"] || task[:oauth_client_id] || task[:event][:oauth_client_id]
        agent_id = identity["agent_id"] || task[:agent_id]

        Enum.find(routes, fn route ->
          route["organization_id"] == (identity["organization_id"] || task[:event][:organization_id] || config["organization_id"]) and
            route["app_user_id"] == app_user_id and route["oauth_client_id"] == oauth_client_id and
            (is_nil(agent_id) or route["agent_id"] == agent_id)
        end)

      _ ->
        nil
    end
  end

  defp current_assignment_event?(nil, _route, _event), do: false
  defp current_assignment_event?(%{status: :reassigning}, _route, _event), do: false

  defp current_assignment_event?(task, route, event) when is_map(route) do
    assigned_route =
      task[:route] ||
        %{
          "organization_id" => task[:event][:organization_id] || route["organization_id"],
          "app_user_id" => task[:app_user_id] || task[:event][:app_user_id] || task[:event][:verified_app_user_id],
          "oauth_client_id" => task[:oauth_client_id] || task[:event][:oauth_client_id],
          "agent_id" => task[:agent_id]
        }

    route_matches = route_identity(route) == route_identity(assigned_route)

    session_matches =
      event.kind == :issue_updated or
        (event[:reason] == :undelegated and is_nil(event[:session_id])) or
        event[:session_id] == task[:session_id]

    route_matches and session_matches and is_nil(task[:pending_reassignment])
  end

  defp current_assignment_event?(_task, _route, _event), do: false

  defp stop_matches_assignment?(nil, _route, _event), do: true

  defp stop_matches_assignment?(task, route, event),
    do: current_assignment_event?(task, route, event)

  defp maybe_begin_reassignment(state, route, event, stop) do
    task = state.linear.tasks[event.issue_id]

    cond do
      is_nil(task) or is_nil(task.run_id) ->
        state

      task[:pending_reassignment] ->
        state

      is_nil(task_route(task, state.linear.config)) ->
        state

      route["agent_id"] == task.agent_id ->
        state

      not newer_assignment?(task, event) ->
        record = state.linear.events[event.id]
        commit(state, record, nil, :stale_assignment_event)

      true ->
        run = state.workstreams.runs[task.run_id]

        if is_nil(run) or run.status in [:complete, :blocked, :policy_blocked] or not WorkstreamRun.compatible_policy?(run) do
          block_reassignment(state, event.id, task, :run_not_reassignable)
        else
          pending = %{
            id: event.id,
            route: route_identity(route),
            agent_id: route["agent_id"],
            session_id: event.session_id,
            timestamp: event.timestamp
          }

          task =
            Map.merge(task, %{
              status: :reassigning,
              pending_reassignment: pending,
              authorized_stage: nil,
              authorized_until: 0,
              check_at: 0,
              scope_revision: (task[:scope_revision] || 0) + 1
            })

          state = save_task(state, task)
          stop.(state, run)
        end
    end
  end

  defp newer_assignment?(task, event) do
    baseline = [task[:event] && task.event[:timestamp], task[:stop_timestamp]] |> Enum.reject(&is_nil/1) |> Enum.max(fn -> nil end)
    is_number(event[:timestamp]) and (is_nil(baseline) or event.timestamp > baseline)
  end

  defp pending_reassignment(state) do
    Enum.find(state.linear.tasks, fn {_issue_id, task} ->
      task[:pending_reassignment] && state.linear.events[task.pending_reassignment.id] &&
        state.linear.events[task.pending_reassignment.id].status == :pending
    end)
  end

  defp process_reassignment(state, issue_id, stop) do
    task = state.linear.tasks[issue_id]
    run = state.workstreams.runs[task.run_id]
    pending = task.pending_reassignment
    record = state.linear.events[pending.id]

    cond do
      is_nil(run) or run.status in [:complete, :blocked, :policy_blocked] or not WorkstreamRun.compatible_policy?(run) ->
        block_reassignment(state, record.id, task, :run_not_reassignable)

      run.status != :stopped ->
        stop.(state, run)

      not reassignment_quiescent?(run) ->
        stop.(state, run)

      (task[:reassignment_retry_at] || 0) > System.system_time(:millisecond) ->
        state

      true ->
        route = route_for_identity(pending.route, state.linear.config)

        if is_nil(route) do
          block_reassignment(state, record.id, task, :unknown_cloud_agent_identity)
        else
          revision = task[:scope_revision] || 0
          start_job(state, {:reassign, issue_id, record.id, revision}, fn -> prepare_reassignment(state, task, record.event, route, run) end)
        end
    end
  end

  defp prepare_reassignment(state, task, event, route, run) do
    with {:ok, issue} <- Delegation.inspect_event(event, route, state.linear.opts),
         {:ok, updated} <- AgentMapping.reassign(run, route["agent_id"], event.id),
         execution = Map.put(updated.execution, :secret_environment_names, Enum.uniq(Map.get(updated.execution, :secret_environment_names, []) ++ AgentMapping.credential_environment_names(route))),
         updated = Map.put(updated, :execution, execution),
         :ok <- Delegation.execution_isolated(updated, route),
         :ok <- Delegation.qualify(updated.definition, route["agent_id"], route, state.linear.opts) do
      {:ok, %{expected_run: run, updated_run: updated, issue: issue, route: route, task: task}}
    else
      {:error, _} = error -> error
      _ -> {:error, :reassignment_scope_check_failed}
    end
  end

  defp reassignment_quiescent?(run) do
    Enum.all?(run.operations, fn {_id, operation} ->
      case operation do
        %{status: status} when status in [:completed, :not_applied] -> not Map.has_key?(operation, :reconciliation)
        %{status: :canceled, cancellation: %{termination: :terminated}} -> not Map.has_key?(operation, :reconciliation)
        _ -> false
      end
    end)
  end

  defp route_for_identity(identity, config) do
    case AgentMapping.routes(config) do
      {:ok, routes} -> Enum.find(routes, &(route_identity(&1) == identity))
      _ -> nil
    end
  end

  @spec result(map(), reference(), term(), function(), function()) :: map()
  def result(%{linear: %{job: %{token: token, ref: ref, key: key}}} = state, token, result, queue, stop) do
    Process.demonitor(ref, [:flush])
    state = put_in(state.linear.job, nil)
    state = apply_result(state, key, result, queue, stop)
    send(self(), :linear_process)
    state
  end

  def result(state, _token, _result, _queue, _stop), do: state

  @spec down(map(), reference()) :: map() | :unhandled
  def down(%{linear: %{job: %{ref: ref}}} = state, ref) do
    state = put_in(state.linear.job, nil)
    # Durable events and activity IDs stay pending. Retry only after the bounded timer.
    schedule(state)
  end

  def down(_state, _ref), do: :unhandled

  @spec ready?(map(), map()) :: boolean()
  def ready?(%{linear: nil}, _run), do: true

  def ready?(state, run) do
    case Enum.find_value(state.linear.tasks, fn {_id, task} -> if task.run_id == run.id, do: task end) do
      nil -> not String.starts_with?(run.task_id, "linear/")
      task -> task.status == :acknowledged and task[:authorized_stage] == run.stage_id and task[:authorized_until] > System.system_time(:millisecond)
    end
  end

  @spec report(map()) :: map()
  def report(%{linear: nil}), do: %{enabled: false}

  def report(state) do
    tasks =
      Enum.map(state.linear.tasks, fn {_id, task} ->
        report = Map.take(task, [:issue_id, :run_id, :status, :session_id, :assignee_id, :agent_id, :activity_id, :error])

        case state.workstreams.runs[task.run_id] do
          nil -> report
          run -> Map.merge(report, Map.take(run, [:stage_id, :current_attempt_id, :phase, :cancellation]))
        end
      end)

    %{enabled: true, tasks: tasks}
  end

  @spec expire(map()) :: map()
  def expire(%{linear: nil} = state), do: state
  def expire(state), do: put_in(state.linear.timer, nil)

  defp process_task(state) do
    now = System.system_time(:millisecond)

    Enum.find_value(state.linear.tasks, state, fn {issue_id, task} ->
      route = task_route(task, state.linear.config)
      revision = task[:scope_revision] || 0
      activity = task |> Map.get(:publications, %{}) |> Map.values() |> Enum.filter(&(&1.status == :pending and (&1[:retry_at] || 0) <= now)) |> Enum.sort_by(& &1.sequence) |> List.first()

      cond do
        task[:pending_reassignment] || task.status == :reassigning ->
          nil

        activity && (route_for_identity(activity[:route], state.linear.config) || route) ->
          publication_route = route_for_identity(activity[:route], state.linear.config) || route
          publication_task = if activity[:session_id], do: Map.put(task, :session_id, activity.session_id), else: task
          start_job(state, {:publish, issue_id, activity.id, revision}, fn -> Delegation.publish(publication_task, activity, publication_route, state.linear.opts) end)

        task.status == :acknowledging and not is_nil(route) and (task[:retry_at] || 0) <= now ->
          start_job(state, {:ack, issue_id, revision}, fn -> Delegation.acknowledge(task, route, state.linear.opts) end)

        task.status == :acknowledged and not is_nil(route) and (task[:check_at] || 0) <= now ->
          event = task.event
          run = state.workstreams.runs[task.run_id]

          start_job(state, {:check, issue_id, revision}, fn ->
            with :ok <- Delegation.execution_isolated(run, route),
                 do: Delegation.inspect_event(event, route, state.linear.opts)
          end)

        true ->
          nil
      end
    end)
  end

  defp start_job(state, key, fun) do
    owner = self()
    token = make_ref()

    {:ok, pid} =
      Task.start(fn ->
        value =
          try do
            fun.()
          rescue
            _ -> {:error, :linear_operation_failed}
          catch
            _, _ -> {:error, :linear_operation_failed}
          end

        send(owner, {:linear_result, token, value})
      end)

    ref = Process.monitor(pid)
    put_in(state.linear.job, %{key: key, token: token, pid: pid, ref: ref})
  end

  defp apply_result(state, {:reassign, issue_id, event_id, revision}, result, _queue, _stop) do
    task = state.linear.tasks[issue_id]
    record = state.linear.events[event_id]

    cond do
      is_nil(task) or is_nil(record) ->
        state

      task.status != :reassigning or task[:pending_reassignment][:id] != event_id or (task[:scope_revision] || 0) != revision ->
        state

      match?({:ok, _}, result) ->
        {:ok, handoff} = result
        commit_reassignment(state, task, record, handoff)

      match?({:error, :previous_writer_not_reconciled}, result) ->
        save_task(state, Map.put(task, :reassignment_retry_at, System.system_time(:millisecond) + 5_000))

      true ->
        {:error, reason} = result
        block_reassignment(state, event_id, task, reason)
    end
  end

  defp apply_result(state, {:event, id, revision}, result, queue, stop) do
    record = state.linear.events[id]
    event = record.event
    current = state.linear.tasks[event.issue_id]
    route = event_route_value(event, state.linear.config)

    cond do
      not is_nil(current) and current.status == :stopped ->
        commit(state, record, nil, :ignored_after_stop)

      not is_nil(current) and not is_nil(revision) and (current[:scope_revision] || 0) != revision ->
        commit(state, record, nil, :stale_scope_check)

      not is_nil(current) and event.kind == :issue_updated and not current_assignment_event?(current, route, event) ->
        commit(state, record, nil, :ignored_old_assignment)

      event.kind == :issue_updated ->
        if match?({:ok, _, nil}, result),
          do: commit(state, record, nil, :handled),
          else: state |> stop_task(event.issue_id, event, stop) |> commit(record, nil, :handled)

      match?({:ok, _, _}, result) ->
        {:ok, issue, prepared} = result

        queued =
          if prepared[:existing_run],
            do: {:ok, prepared.existing_run, state},
            else: queue.(state, "linear-run/" <> event.issue_id, "linear/" <> event.issue_id, prepared.path, prepared.inputs, prepared.execution)

        case queued do
          {:ok, run_id, state} ->
            task = %{
              issue_id: issue.id,
              run_id: run_id,
              status: :acknowledging,
              session_id: event.session_id,
              activity_id: Delegation.new_activity_id(),
              assignee_id: issue.assignee_id,
              agent_id: prepared.agent_id,
              route: route_identity(route),
              app_user_id: route["app_user_id"],
              oauth_client_id: route["oauth_client_id"],
              event: event,
              authorized_stage: nil,
              authorized_until: 0,
              scope_revision: 0,
              publications: %{},
              assignment_history: []
            }

            task =
              case SymphonyElixir.Evidence.url(state.workstreams.runs[run_id]) do
                nil -> task
                url -> Map.put(task, :evidence_url, url)
              end

            commit(state, record, task, :handled)

          {:error, reason} ->
            commit(state, record, blocked_task(event, reason, route), :blocked)
        end

      true ->
        {:error, reason} = result
        commit(state, record, blocked_task(event, reason, route), :blocked)
    end
  end

  defp apply_result(state, {:ack, issue_id, revision}, result, _queue, _stop) do
    task = state.linear.tasks[issue_id]

    if task.status in [:stopped, :reassigning] or (task[:scope_revision] || 0) != revision do
      state
    else
      task =
        case result do
          {:ok, %{id: id}} when id == task.activity_id -> Map.merge(task, %{status: :acknowledged, check_at: 0})
          _ -> Map.merge(task, %{error: :linear_acknowledgement_pending, retry_at: System.system_time(:millisecond) + 5_000})
        end

      save_task(state, task)
    end
  end

  defp apply_result(state, {:check, issue_id, revision}, result, _queue, stop) do
    task = state.linear.tasks[issue_id]
    run = state.workstreams.runs[task.run_id]

    cond do
      task.status in [:stopped, :reassigning] ->
        state

      (task[:scope_revision] || 0) != revision ->
        state

      match?({:ok, _}, result) and run.status == :ready ->
        task = Map.merge(task, %{authorized_stage: run.stage_id, authorized_until: System.system_time(:millisecond) + 5_000, check_at: System.system_time(:millisecond) + 1_000})
        send(self(), :advance_workstreams)
        save_task(state, task)

      match?({:ok, _}, result) ->
        save_task(state, Map.put(task, :check_at, System.system_time(:millisecond) + 5_000))

      true ->
        stop_task(state, issue_id, task.event, stop)
    end
  end

  defp apply_result(state, {:reply, id, revision}, result, _queue, stop) do
    record = state.linear.events[id]
    event = record.event
    task = state.linear.tasks[event.issue_id]

    cond do
      task.status in [:stopped, :reassigning] ->
        commit(state, record, nil, :ignored_after_stop)

      (task[:scope_revision] || 0) != revision ->
        state

      not match?({:ok, _}, result) ->
        state |> stop_task(task.issue_id, task.event, stop) |> commit(record, nil, :not_delegated)

      true ->
        run = state.workstreams.runs[task.run_id]
        event_id = "linear-activity/" <> event.activity_id
        receipt = %{kind: :linear_reply, session_id: event.session_id, body_sha256: digest(event.body)}

        case WorkstreamStore.event(state.workstreams.store, event_id) do
          {:ok, %{run_id: run_id, event: ^receipt}} when run_id == run.id -> commit(state, record, nil, :duplicate_reply)
          {:ok, _} -> commit(state, record, nil, :reply_identity_conflict)
          :not_found -> apply_reply(state, record, run, event_id, receipt)
          other -> exit({:linear_reply_store_failed, other})
        end
    end
  end

  defp apply_result(state, {:publish, issue_id, id, revision}, result, _queue, _stop) do
    task = state.linear.tasks[issue_id]
    publication = task.publications[id]

    if task.status == :reassigning or (task[:scope_revision] || 0) != revision or is_nil(publication) do
      state
    else
      updated =
        case result do
          {:ok, %{id: ^id}} -> Map.put(publication, :status, :published)
          _ -> Map.put(publication, :retry_at, System.system_time(:millisecond) + 5_000)
        end

      save_task(state, put_in(task.publications[id], updated))
    end
  end

  defp commit_reassignment(state, task, record, %{expected_run: expected_run, updated_run: updated_run, issue: issue, route: route, task: expected_task}) do
    current_run = state.workstreams.runs[task.run_id]

    if task !== expected_task or current_run !== expected_run or task.pending_reassignment.id != record.id do
      state
    else
      previous_assignment = %{
        agent_id: task.agent_id,
        route: task[:route],
        app_user_id: task[:app_user_id],
        oauth_client_id: task[:oauth_client_id],
        session_id: task.session_id,
        activity_id: task[:activity_id],
        event: task.event,
        stop_timestamp: task[:stop_timestamp],
        publications: task[:publications] || %{}
      }

      updated_task =
        task
        |> Map.merge(%{
          status: :acknowledging,
          agent_id: route["agent_id"],
          route: route_identity(route),
          app_user_id: route["app_user_id"],
          oauth_client_id: route["oauth_client_id"],
          session_id: record.event.session_id,
          activity_id: Delegation.new_activity_id(),
          assignee_id: issue.assignee_id,
          event: record.event,
          authorized_stage: nil,
          authorized_until: 0,
          check_at: 0,
          retry_at: 0,
          scope_revision: (task[:scope_revision] || 0) + 1,
          pending_reassignment: nil,
          stop_timestamp: nil,
          stop_event_id: nil,
          publications: %{},
          assignment_history: (task[:assignment_history] || []) ++ [previous_assignment]
        })
        |> Map.delete(:reassignment_retry_at)
        |> Map.delete(:error)

      handled_record = Map.put(record, :status, :handled)
      transition_id = record.id
      run_event_id = "linear-agent-transition/" <> transition_id

      run_event = %{
        kind: :agent_reassigned,
        transition_id: transition_id,
        agent_id: route["agent_id"],
        from_agent_id: task.agent_id
      }

      run_event =
        case Config.evidence() do
          :disabled -> run_event
          {:ok, _} -> Map.put(run_event, :reassignment, %{previous: SymphonyElixir.Evidence.identity(expected_run), current: SymphonyElixir.Evidence.identity(updated_run)})
          _ -> exit(:private_evidence_config_unavailable)
        end

      case WorkstreamStore.linear_reassign(
             state.workstreams.store,
             expected_run,
             updated_run,
             task,
             run_event_id,
             run_event,
             handled_record,
             updated_task
           ) do
        :ok ->
          :ok = SymphonyElixir.Evidence.sync!(state.workstreams.store)
          state = put_in(state.workstreams.runs[updated_run.id], updated_run)
          state = put_in(state.linear.events[handled_record.id], handled_record)
          state = put_in(state.linear.tasks[updated_task.issue_id], updated_task)
          send(self(), :linear_process)
          if updated_run.status == :ready, do: send(self(), :advance_workstreams)
          state

        {:error, reason} ->
          exit({:linear_reassignment_commit_failed, reason})
      end
    end
  end

  defp block_reassignment(state, event_id, task, reason) do
    case state.linear.events[event_id] do
      nil ->
        state

      record ->
        pending = task[:pending_reassignment] || %{}
        requested_route = route_for_identity(pending[:route], state.linear.config) || event_route_value(record.event, state.linear.config)
        requested_route_identity = pending[:route] || (requested_route && route_identity(requested_route))
        requested_agent_id = pending[:agent_id] || (requested_route && requested_route["agent_id"])
        requested_session_id = pending[:session_id] || record.event[:session_id]

        task =
          task
          |> Map.merge(%{
            status: :blocked,
            error: inspect(reason),
            blocked_reassignment: %{
              id: event_id,
              agent_id: requested_agent_id,
              route: requested_route_identity,
              session_id: requested_session_id,
              error: inspect(reason)
            },
            pending_reassignment: nil,
            authorized_stage: nil,
            authorized_until: 0,
            scope_revision: (task[:scope_revision] || 0) + 1
          })
          |> Map.delete(:reassignment_retry_at)

        task =
          if requested_route && is_binary(requested_session_id) do
            add_routed_publication(
              task,
              "reassignment-blocked/" <> event_id,
              "error",
              "Agent reassignment is blocked: #{inspect(reason)}. No other agent will be selected automatically.",
              requested_route,
              requested_session_id
            )
          else
            task
          end

        commit(state, record, task, :blocked)
    end
  end

  defp apply_reply(state, record, run, event_id, receipt) do
    case WorkstreamRun.linear_reply(run, record.event.body, record.event.activity_id) do
      {:ok, updated} ->
        :ok = WorkstreamStore.commit(state.workstreams.store, updated, event_id, receipt)
        :ok = SymphonyElixir.Evidence.sync!(state.workstreams.store)
        state = put_in(state.workstreams.runs[run.id], updated)
        task = state.linear.tasks[record.event.issue_id] |> Map.merge(%{authorized_stage: nil, authorized_until: 0, check_at: 0})
        send(self(), :advance_workstreams)
        commit(state, record, task, :reply_delivered)

      {:error, reason} ->
        task = state.linear.tasks[record.event.issue_id]

        task =
          add_publication(
            task,
            "rejected-reply/" <> record.event.activity_id,
            "error",
            "Reply did not resume work: #{inspect(reason)}. Use the current question ID and approval revision when required."
          )

        commit(state, record, task, :reply_rejected)
    end
  end

  defp sync_publications(state) do
    Enum.reduce(state.linear.tasks, state, fn {_id, task}, acc ->
      if task[:pending_reassignment] || task.status == :reassigning do
        acc
      else
        run = acc.workstreams.runs[task.run_id]
        updated = if run, do: run_publications(task, run), else: task
        updated = if task.status == :stopped, do: add_publication(updated, "stopped", "response", "Work stopped. Further replies do not restart this task."), else: updated
        updated = if task.status == :blocked, do: add_publication(updated, "blocked", "error", "Task blocked before execution: #{task.error}"), else: updated
        if updated == task, do: acc, else: save_task(acc, updated)
      end
    end)
  end

  defp run_publications(task, run) do
    task = publication_link(task, run)

    task =
      case SymphonyElixir.Evidence.url(run) do
        nil -> task
        url -> add_publication(task, "evidence-url", "thought", "Private conversation and test evidence: #{url}")
      end

    task =
      Enum.reduce(Map.get(run, :questions, %{}), task, fn {_id, question}, acc ->
        add_publication(acc, "question/" <> question.id, "elicitation", question.prompt <> "\n\nReply: answer #{question.id}: <your answer>")
      end)

    key = "phase/#{run.phase}/#{run.stage_id}/#{run.current_attempt_id || length(run.attempts)}"

    case run.phase do
      :waiting_for_answer ->
        wait = run.pending_wait

        if wait do
          command = if wait[:approval_digest], do: "approve #{wait.id} #{wait.approval_digest}", else: "answer #{wait.id}: <your answer>"
          add_publication(task, key, "elicitation", wait.prompt <> "\n\nReply: #{command}\nExecution capacity is released.")
        else
          task
        end

      :blocked ->
        add_publication(task, key, "error", "Run #{run.id} failed or requires unsupported input. Inspect the durable stage receipt before retrying.")

      :policy_blocked ->
        add_publication(task, key, "error", "Run #{run.id} is blocked by a changed service policy; explicit migration is required.")

      :complete ->
        message =
          if published_pr(run),
            do: "Run #{run.id} completed its configured stages. Human GitHub merge remains pending.",
            else: "Run #{run.id} completed its configured stages. No PR has been published."

        add_publication(task, key, "response", message)

      :implementing ->
        add_publication(task, key, "thought", "Run #{run.id}: working on stage #{run.stage_id}.")

      :validating ->
        add_publication(task, key, "thought", "Run #{run.id}: checking stage #{run.stage_id}.")

      :publishing ->
        add_publication(task, key, "thought", "Run #{run.id}: verifying exact validation and permanent archival before publishing stage #{run.stage_id}.")

      _ ->
        task
    end
  end

  defp publication_link(task, run) do
    task = stopped_publication_links(task, run)

    case published_pr(run) do
      %{"number" => number, "url" => url} ->
        add_publication(task, "github-pr/#{number}", "thought", "Validated Rig PR: #{url}. The PR contains the tested commit and permanent evidence links. Human merge remains in GitHub.")

      _ ->
        task
    end
  end

  defp stopped_publication_links(task, run) do
    Enum.reduce(run.operations, task, fn {_id, operation}, acc ->
      case operation[:publication_observation] do
        %{status: :observed, pr: %{number: number, url: url}} ->
          add_publication(
            acc,
            "stopped-github-pr/#{number}",
            "thought",
            "GitHub PR observed after cancellation: #{url}. The run remains stopped; inspect the PR before taking further action. Human merge remains in GitHub."
          )

        %{status: :mismatched} ->
          add_publication(
            acc,
            "stopped-publication-mismatch/#{operation.id}",
            "error",
            "GitHub state differs from the stopped publication intent. The run remains stopped; inspect the deterministic task branch and PR."
          )

        _ ->
          acc
      end
    end)
  end

  defp published_pr(run) do
    run.attempts
    |> Enum.reverse()
    |> Enum.find_value(fn attempt ->
      if attempt.type == :publish and attempt.gate == :passed, do: get_in(attempt, [:result, :evidence, "pr"])
    end)
  end

  defp add_publication(task, key, type, body) do
    publications = Map.get(task, :publications, %{})
    id = activity_id("#{task.issue_id}/#{task.session_id}/#{key}")

    if Map.has_key?(publications, id) do
      task
    else
      {:ok, body} = SymphonyElixir.Linear.Text.safe(String.slice(body, 0, 4_000))
      publication = %{id: id, key: key, status: :pending, sequence: map_size(publications), content: %{"type" => type, "body" => body}}
      Map.put(task, :publications, Map.put(publications, id, publication))
    end
  end

  defp add_routed_publication(task, key, type, body, route, session_id) do
    publications = Map.get(task, :publications, %{})
    id = activity_id("#{task.issue_id}/#{session_id}/#{key}")

    if Map.has_key?(publications, id) do
      task
    else
      {:ok, body} = SymphonyElixir.Linear.Text.safe(String.slice(body, 0, 4_000))

      publication = %{
        id: id,
        key: key,
        status: :pending,
        sequence: map_size(publications),
        content: %{"type" => type, "body" => body},
        route: route_identity(route),
        session_id: session_id
      }

      Map.put(task, :publications, Map.put(publications, id, publication))
    end
  end

  defp activity_id(key) do
    <<a::32, b::16, c::12, d::14, e::48, _::bitstring>> = :crypto.hash(:sha256, key)
    Enum.join([hex(a, 8), hex(b, 4), hex(0x4000 + c, 4), hex(0x8000 + d, 4), hex(e, 12)], "-")
  end

  defp hex(n, size), do: n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(size, "0")
  defp digest(body), do: :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)

  defp blocked_task(event, reason, route) do
    %{
      issue_id: event.issue_id,
      run_id: nil,
      status: :blocked,
      session_id: event.session_id,
      activity_id: Delegation.new_activity_id(),
      agent_id: route["agent_id"],
      route: route_identity(route),
      app_user_id: route["app_user_id"],
      oauth_client_id: route["oauth_client_id"],
      error: inspect(reason),
      event: event,
      authorized_stage: nil,
      authorized_until: 0,
      scope_revision: 0,
      publications: %{},
      assignment_history: []
    }
  end

  defp stop_task(state, issue_id, event, stop) do
    route = event_route_value(event, state.linear.config)

    task =
      state.linear.tasks[issue_id] ||
        %{
          issue_id: issue_id,
          run_id: nil,
          agent_id: route && route["agent_id"],
          route: route && route_identity(route),
          app_user_id: route && route["app_user_id"],
          oauth_client_id: route && route["oauth_client_id"],
          session_id: event.session_id,
          event: event,
          publications: %{},
          assignment_history: []
        }

    same_stop = task[:stop_event_id] == event.id

    stop_timestamp =
      case {task[:stop_timestamp], event[:timestamp]} do
        {old, new} when is_number(old) and is_number(new) -> max(old, new)
        {_old, new} when is_number(new) -> new
        {old, _new} -> old
      end

    task =
      Map.merge(task, %{
        status: :stopped,
        authorized_stage: nil,
        authorized_until: 0,
        stop_event_id: if(same_stop, do: task[:stop_event_id], else: event.id),
        stop_timestamp: stop_timestamp,
        scope_revision: (task[:scope_revision] || 0) + if(same_stop or event.kind == :issue_updated, do: 0, else: 1)
      })

    # Persist the tombstone before requesting worker termination.
    state = save_task(state, task)
    if task.run_id, do: stop.(state, state.workstreams.runs[task.run_id]), else: state
  end

  defp commit(state, record, task, status) do
    record = Map.put(record, :status, status)
    :ok = WorkstreamStore.linear_commit(state.workstreams.store, record, task)
    state = put_in(state.linear.events[record.id], record)
    if task, do: put_in(state.linear.tasks[task.issue_id], task), else: state
  end

  defp save_task(state, task) do
    :ok = WorkstreamStore.linear_task(state.workstreams.store, task)
    put_in(state.linear.tasks[task.issue_id], task)
  end

  defp schedule(%{linear: %{timer: nil}} = state) do
    timer = Process.send_after(self(), :linear_tick, state.linear.config["reconcile_interval_ms"] || 5_000)
    put_in(state.linear.timer, timer)
  end

  defp schedule(state), do: state

  defp existing_run(state, issue_id), do: Enum.find_value(state.workstreams.runs, fn {_id, run} -> if run.task_id == "linear/" <> issue_id, do: run end)
end
