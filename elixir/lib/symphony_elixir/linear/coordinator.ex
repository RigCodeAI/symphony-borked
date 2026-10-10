defmodule SymphonyElixir.Linear.Coordinator do
  @moduledoc "Durable native event intake owned by the existing coordinator."

  alias SymphonyElixir.Linear.Delegation
  alias SymphonyElixir.{WorkstreamRun, WorkstreamStore}

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

    if Delegation.accepts?(event, config) and (event.kind != :issue_updated or Map.has_key?(state.linear.tasks, event.issue_id)) do
      case WorkstreamStore.linear_receive(state.workstreams.store, event.id, event) do
        :ok ->
          record = %{id: event.id, event: event, status: :pending}
          state = put_in(state.linear.events[event.id], record)
          state = if event.kind == :stop, do: stop_task(state, event.issue_id, event, stop), else: state

          state =
            if event.kind == :issue_updated do
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
      {:error, :unrelated_linear_event}
    end
  end

  @spec process(map(), function()) :: map()
  def process(%{linear: nil} = state, _stop), do: state
  def process(%{linear: %{job: job}} = state, _stop) when not is_nil(job), do: state

  def process(state, stop) do
    state = state |> sync_publications() |> schedule()

    pending = state.linear.events |> Enum.filter(fn {_id, record} -> record.status == :pending end) |> Enum.sort_by(fn {id, r} -> {r.event.timestamp, id} end)
    selected = Enum.find(pending, fn {_id, record} -> record.event.kind == :stop end) || List.first(pending)

    case selected do
      {_id, %{event: %{kind: :stop}} = record} ->
        state |> stop_task(record.event.issue_id, record.event, stop) |> commit(record, nil, :handled) |> process(stop)

      {_id, record} ->
        task = state.linear.tasks[record.event.issue_id]

        cond do
          task && task.status == :stopped ->
            state |> commit(record, nil, :ignored_after_stop) |> process(stop)

          task && task.run_id && record.event.kind == :prompted && is_binary(record.event[:body]) ->
            if task.session_id == record.event.session_id,
              do: start_job(state, {:reply, record.id, task[:scope_revision] || 0}, fn -> Delegation.inspect_event(record.event, state.linear.config, state.linear.opts) end),
              else: state |> commit(record, nil, :wrong_session) |> process(stop)

          task && task.run_id && record.event.kind in [:created, :prompted] ->
            state |> commit(record, nil, :duplicate_task) |> process(stop)

          record.event.kind == :prompted ->
            state |> commit(record, nil, :no_delegated_task) |> process(stop)

          true ->
            start_job(state, {:event, record.id}, fn -> inspect_for_run(state, record.event) end)
        end

      nil ->
        process_task(state)
    end
  end

  defp inspect_for_run(state, event) do
    with {:ok, issue} <- Delegation.inspect_event(event, state.linear.config, state.linear.opts),
         {:ok, prepared} <- prepare_run(state, event, issue) do
      {:ok, issue, prepared}
    end
  end

  defp prepare_run(_state, %{kind: :issue_updated}, _issue), do: {:ok, nil}

  defp prepare_run(state, _event, issue) do
    case existing_run(state, issue.id) do
      nil -> Delegation.prepare(issue, state.linear.config, state.linear.opts)
      existing -> qualify_existing(state, existing)
    end
  end

  defp qualify_existing(state, existing) do
    agent = existing.definition.stages[existing.definition.entry].agent

    with :ok <- Delegation.qualify(existing.definition, agent, state.linear.config, state.linear.opts) do
      {:ok, %{existing_run: existing.id, agent_id: agent}}
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
      activity = task |> Map.get(:publications, %{}) |> Map.values() |> Enum.filter(&(&1.status == :pending and (&1[:retry_at] || 0) <= now)) |> Enum.sort_by(& &1.sequence) |> List.first()

      cond do
        activity ->
          start_job(state, {:publish, issue_id, activity.id}, fn -> Delegation.publish(task, activity, state.linear.config, state.linear.opts) end)

        task.status == :acknowledging and (task[:retry_at] || 0) <= now ->
          start_job(state, {:ack, issue_id}, fn -> Delegation.acknowledge(task, state.linear.config, state.linear.opts) end)

        task.status == :acknowledged and (task[:check_at] || 0) <= now ->
          event = task.event
          start_job(state, {:check, issue_id, task[:scope_revision] || 0}, fn -> Delegation.inspect_event(event, state.linear.config, state.linear.opts) end)

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

  defp apply_result(state, {:event, id}, result, queue, stop) do
    record = state.linear.events[id]
    event = record.event
    current = state.linear.tasks[event.issue_id]

    cond do
      current && current.status == :stopped ->
        commit(state, record, nil, :ignored_after_stop)

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
              event: event,
              authorized_stage: nil,
              authorized_until: 0
            }

            commit(state, record, task, :handled)

          {:error, reason} ->
            commit(state, record, blocked_task(event, reason), :blocked)
        end

      true ->
        {:error, reason} = result
        commit(state, record, blocked_task(event, reason), :blocked)
    end
  end

  defp apply_result(state, {:ack, issue_id}, result, _queue, _stop) do
    task = state.linear.tasks[issue_id]

    if task.status == :stopped do
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
      task.status == :stopped ->
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
      task.status == :stopped ->
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

  defp apply_result(state, {:publish, issue_id, id}, result, _queue, _stop) do
    task = state.linear.tasks[issue_id]
    publication = task.publications[id]

    updated =
      case result do
        {:ok, %{id: ^id}} -> Map.put(publication, :status, :published)
        _ -> Map.put(publication, :retry_at, System.system_time(:millisecond) + 5_000)
      end

    save_task(state, put_in(task.publications[id], updated))
  end

  defp apply_reply(state, record, run, event_id, receipt) do
    case WorkstreamRun.linear_reply(run, record.event.body, record.event.activity_id) do
      {:ok, updated} ->
        :ok = WorkstreamStore.commit(state.workstreams.store, updated, event_id, receipt)
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
      run = acc.workstreams.runs[task.run_id]
      updated = if run, do: run_publications(task, run), else: task
      updated = if task.status == :stopped, do: add_publication(updated, "stopped", "response", "Work stopped. Further replies do not restart this task."), else: updated
      updated = if task.status == :blocked, do: add_publication(updated, "blocked", "error", "Task blocked before execution: #{task.error}"), else: updated
      if updated == task, do: acc, else: save_task(acc, updated)
    end)
  end

  defp run_publications(task, run) do
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
        add_publication(task, key, "response", "Run #{run.id} completed its configured stages. Candidate publication remains disabled.")

      :implementing ->
        add_publication(task, key, "thought", "Run #{run.id}: working on stage #{run.stage_id}.")

      :validating ->
        add_publication(task, key, "thought", "Run #{run.id}: checking stage #{run.stage_id}.")

      _ ->
        task
    end
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

  defp activity_id(key) do
    <<a::32, b::16, c::12, d::14, e::48, _::bitstring>> = :crypto.hash(:sha256, key)
    Enum.join([hex(a, 8), hex(b, 4), hex(0x4000 + c, 4), hex(0x8000 + d, 4), hex(e, 12)], "-")
  end

  defp hex(n, size), do: n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(size, "0")
  defp digest(body), do: :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)

  defp blocked_task(event, reason) do
    %{issue_id: event.issue_id, run_id: nil, status: :blocked, session_id: event.session_id, error: inspect(reason), event: event}
  end

  defp stop_task(state, issue_id, event, stop) do
    task = state.linear.tasks[issue_id] || %{issue_id: issue_id, run_id: nil, session_id: event.session_id, event: event}
    task = Map.merge(task, %{status: :stopped, authorized_stage: nil, authorized_until: 0})
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
