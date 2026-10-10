defmodule SymphonyElixir.LinearDelegationTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{AgentRuntimeSupervisor, Orchestrator, WorkstreamRunner, WorkstreamStore}
  alias SymphonyElixir.Tracker.Issue

  @issue_id "linear-issue-1"
  @session_id "linear-session-1"
  @app_user_id "linear-app-user-1"
  @oauth_client_id "linear-oauth-client-1"
  @organization_id "linear-org-1"

  setup do
    root = Path.join(System.tmp_dir!(), "linear-delegation-#{System.unique_integer([:positive])}")
    workspace = Path.join(root, "workspaces/rig")
    File.mkdir_p!(workspace)
    {_, 0} = System.cmd("git", ["init", "--quiet", workspace])

    File.write!(Path.join(root, "instructions.md"), "Use the controlled test executor.")
    File.write!(Path.join(root, "SKILL.md"), "Do not publish or merge.")

    File.write!(Path.join(root, "default-cloud.yaml"), """
    version: 1
    name: default-cloud
    model: gpt-6-luna
    reasoning_effort: medium
    daybreak: false
    approval_policy: never
    sandbox: workspace-write
    instructions: [instructions.md]
    skills: [SKILL.md]
    """)

    workstream_path = Path.join(root, "software-change.yaml")

    File.write!(workstream_path, """
    version: 1
    name: software-change
    inputs: [task]
    agents:
      default-cloud: default-cloud.yaml
    entry: implement
    stages:
      - id: implement
        type: agent
        inputs: [task]
        outputs: [candidate]
        agent: default-cloud
        prompt: Implement the delegated task.
        next: validate
      - id: validate
        type: check
        inputs: [candidate]
        outputs: [validation]
        gate:
          command: [sh, -c, "printf checked >> effects; test -f candidate"]
          timeout_ms: 1000
          success: wait
          failure: blocked
      - id: wait
        type: human_wait
        inputs: [candidate, validation]
        outputs: [answer]
        prompt: Review this candidate.
        next: complete
    """)

    context = %{
      root: root,
      issue_id: @issue_id,
      workspace: workspace,
      workspace_root: Path.join(root, "workspaces"),
      store_path: Path.join(root, "state/linear.sqlite"),
      workstream_path: workstream_path,
      config: %{
        "organization_id" => @organization_id,
        "team_id" => "linear-dev-team",
        "app_user_id" => @app_user_id,
        "oauth_client_id" => @oauth_client_id,
        "webhook_secret_env" => "LINEAR_API_TOKEN",
        "token_env" => "LINEAR_API_KEY",
        "store_path" => Path.join(root, "state/linear.sqlite"),
        "workspace_root" => Path.join(root, "workspaces"),
        "workstream_path" => workstream_path,
        "agent_id" => "default-cloud",
        "rig_label" => "factory:rig",
        "workspaces" => %{@issue_id => workspace},
        "reconcile_interval_ms" => 1_000
      }
    }

    on_exit(fn -> File.rm_rf!(root) end)
    context
  end

  test "delegation acknowledges before execution and resumes one active run after coordinator restart", c do
    parent = self()

    acknowledger = fn task, _config ->
      send(parent, {:ack_requested, task, self()})
      activity_id = task.activity_id

      receive do
        {:confirm_ack, ^activity_id} -> {:ok, %{id: activity_id}}
      after
        10_000 -> {:error, :acknowledgement_not_confirmed}
      end
    end

    executor = controlled_executor(parent)
    runtime = runtime(c, stage_executor: executor, linear_acknowledger: acknowledger)
    created = event(c, :created, "delivery-created")

    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, created)
    assert_receive {:ack_requested, task, ack_worker}
    assert task.issue_id == c.issue_id
    assert task.assignee_id == "human-owner-17"
    assert is_binary(task.activity_id)
    assert is_binary(task.run_id)

    refute_receive {:stage_started, "implement", _, _}, 100

    assert {:duplicate, %{id: "delivery-created"}} =
             Orchestrator.receive_linear_event(runtime.coordinator, created)

    prompted = event(c, :prompted, "delivery-prompted", %{activity_id: "activity-prompt-1"})
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, prompted)

    send(ack_worker, {:confirm_ack, task.activity_id})
    assert_receive {:stage_started, "implement", worker, run_id}, 5_000
    assert run_id == task.run_id
    assert Process.alive?(worker)
    assert %{status: :executing, stage_id: "implement"} = run_state(runtime, run_id)

    coordinator_before_restart = Process.whereis(runtime.coordinator)
    restarted = restart(runtime)
    assert is_pid(restarted)
    refute restarted == coordinator_before_restart
    assert Process.alive?(worker)
    assert Task.Supervisor.children(runtime.tasks) == [worker]

    assert {:duplicate, %{id: "delivery-created"}} =
             Orchestrator.receive_linear_event(runtime.coordinator, created)

    persisted_task = linear_task(runtime, c.issue_id)
    assert persisted_task.run_id == run_id
    assert persisted_task.assignee_id == "human-owner-17"
    assert persisted_task.status == :acknowledged
    refute_receive {:ack_requested, _, _}, 100

    send(worker, :finish_agent)
    assert_receive {:stage_started, "validate", _check_worker, ^run_id}, 10_000

    report = wait_run(runtime, run_id, &(&1.status == :waiting_for_answer))
    assert report.task_id == "linear/" <> c.issue_id

    assert [
             %{stage: "implement", status: :completed},
             %{stage: "validate", gate: :passed},
             %{stage: "wait", status: :waiting}
           ] = report.attempts

    assert File.read!(Path.join(c.workspace, "effects")) == "checked"

    snapshot = Orchestrator.snapshot(runtime.coordinator, 1_000)
    assert [%{run_id: ^run_id, assignee_id: "human-owner-17", status: :acknowledged}] = snapshot.linear.tasks
    assert {:ok, runs} = WorkstreamStore.load(:sys.get_state(runtime.coordinator).workstreams.store)
    assert Enum.count(runs, &(&1.id == run_id)) == 1
    refute_receive {:stage_started, "implement", _, _}, 100
  end

  test "stop received while delegated issue lookup is queued prevents run creation", c do
    parent = self()

    issue_fetcher = fn _issue_id, _config ->
      send(parent, {:issue_lookup, self()})

      receive do
        {:release_issue, issue} -> {:ok, issue}
      end
    end

    runtime = runtime(c, linear_issue_fetcher: issue_fetcher, stage_executor: controlled_executor(parent))

    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "queued-create"))
    assert_receive {:issue_lookup, lookup_worker}
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :stop, "queued-stop"))

    stopped = wait_linear_task(runtime, c.issue_id, &(&1.status == :stopped))
    assert stopped.run_id == nil
    send(lookup_worker, {:release_issue, issue(c)})

    await(fn ->
      state = :sys.get_state(runtime.coordinator)

      case state.linear.events["queued-create"] do
        %{status: :ignored_after_stop} -> true
        _ -> nil
      end
    end)

    assert Task.Supervisor.children(runtime.tasks) == []
    refute_receive {:stage_started, _, _, _}, 100
    assert map_size(:sys.get_state(runtime.coordinator).workstreams.runs) == 0
  end

  test "recovery applies a durable pending stop before an acknowledged run can advance", c do
    parent = self()
    runtime = runtime(c, auto_advance: false, stage_executor: controlled_executor(parent))

    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "pending-stop-create"))
    task = wait_linear_task(runtime, c.issue_id, &(&1.status == :acknowledged))
    assert %{status: :ready, attempts: []} = run_state(runtime, task.run_id)

    stop = event(c, :stop, "pending-stop-before-tombstone")

    :sys.replace_state(runtime.coordinator, fn state ->
      :ok = WorkstreamStore.linear_receive(state.workstreams.store, stop.id, stop)
      state
    end)

    stored = :sys.get_state(runtime.coordinator)
    refute Map.has_key?(stored.linear.events, stop.id)

    assert {:ok, %{events: events, tasks: tasks}} =
             WorkstreamStore.linear_load(stored.workstreams.store)

    assert %{status: :pending, event: ^stop} = events[stop.id]
    assert tasks[c.issue_id].status == :acknowledged

    restarted = restart(runtime)
    assert is_pid(restarted)
    assert %{status: :stopped, attempts: []} = wait_run(runtime, task.run_id, &(&1.status == :stopped))
    assert wait_linear_task(runtime, c.issue_id, &(&1.status == :stopped)).run_id == task.run_id
    assert :sys.get_state(runtime.coordinator).linear.events[stop.id].status == :handled
    assert Task.Supervisor.children(runtime.tasks) == []
    refute_receive {:stage_started, _, _, _}, 100
  end

  test "recovery links a pending Linear event to its existing pinned run after the workstream file is removed", c do
    parent = self()
    runtime = runtime(c, auto_advance: false, stage_executor: controlled_executor(parent))
    pinned_input = "Use the task that was pinned before the coordinator restart."

    assert {:ok, run_id} =
             Orchestrator.queue_workstream(
               runtime.coordinator,
               "linear-run/" <> c.issue_id,
               "linear/" <> c.issue_id,
               c.workstream_path,
               %{"task" => pinned_input},
               workspace: c.workspace,
               workspace_root: c.workspace_root,
               issue_id: c.issue_id
             )

    assert %{status: :ready} = run_state(runtime, run_id)
    pinned = :sys.get_state(runtime.coordinator).workstreams.runs[run_id]
    assert pinned.definition.name == "software-change"
    assert pinned.outputs["task"] == pinned_input
    refute_receive {:stage_started, _, _, _}, 100

    created = event(c, :created, "pending-created-before-link")

    :sys.replace_state(runtime.coordinator, fn state ->
      :ok = WorkstreamStore.linear_receive(state.workstreams.store, created.id, created)
      state
    end)

    state_before_restart = :sys.get_state(runtime.coordinator)
    refute Map.has_key?(state_before_restart.linear.events, created.id)
    refute Map.has_key?(state_before_restart.linear.tasks, c.issue_id)

    assert {:ok, %{events: events, tasks: tasks}} =
             WorkstreamStore.linear_load(state_before_restart.workstreams.store)

    assert %{status: :pending, event: ^created} = events[created.id]
    refute Map.has_key?(tasks, c.issue_id)
    File.rm!(c.workstream_path)

    assert is_pid(restart(runtime))
    task = wait_linear_task(runtime, c.issue_id, &(&1.status == :acknowledged))
    assert task.run_id == run_id
    assert task.assignee_id == "human-owner-17"
    assert task.agent_id == "default-cloud"
    assert %{status: :ready} = run_state(runtime, run_id)
    pinned = :sys.get_state(runtime.coordinator).workstreams.runs[run_id]
    assert pinned.definition.name == "software-change"
    assert pinned.outputs["task"] == pinned_input
    assert map_size(:sys.get_state(runtime.coordinator).workstreams.runs) == 1
    assert :sys.get_state(runtime.coordinator).linear.events[created.id].status == :handled

    assert {:duplicate, %{id: "pending-created-before-link"}} =
             Orchestrator.receive_linear_event(runtime.coordinator, created)

    assert Task.Supervisor.children(runtime.tasks) == []
    refute_receive {:stage_started, _, _, _}, 100
  end

  test "stop after execution starts cancels the worker and later events do not start another stage", c do
    parent = self()

    canceller = fn _run, operation, worker ->
      send(parent, {:cancel_requested, operation.id, worker})
      :terminated
    end

    runtime = runtime(c, stage_executor: controlled_executor(parent), workstream_canceller: canceller)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "running-create"))
    assert_receive {:stage_started, "implement", worker, run_id}, 5_000

    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :stop, "running-stop"))
    assert_receive {:cancel_requested, _operation_id, ^worker}
    refute Process.alive?(worker)

    stopped_run = wait_run(runtime, run_id, &(&1.status == :stopped))
    assert stopped_run.phase == :stopped
    assert length(stopped_run.attempts) == 1
    assert stopped_run.cancellation.termination == :terminated

    later = event(c, :prompted, "running-prompted", %{activity_id: "activity-after-stop"})
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, later)

    await(fn ->
      case :sys.get_state(runtime.coordinator).linear.events["running-prompted"] do
        %{status: :ignored_after_stop} -> true
        _ -> nil
      end
    end)

    assert wait_linear_task(runtime, c.issue_id, &(&1.status == :stopped)).run_id == run_id
    refute File.exists?(Path.join(c.workspace, "effects"))
    refute_receive {:stage_started, _, _, _}, 100
    assert [] == Task.Supervisor.children(runtime.tasks)
  end

  test "recovery cancels the executing delegated worker after a durable stop receipt", c do
    parent = self()

    canceller = fn _run, operation, worker ->
      send(parent, {:recovery_cancel_requested, operation.id, worker})
      :terminated
    end

    runtime = runtime(c, stage_executor: controlled_executor(parent), workstream_canceller: canceller)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "recovery-running-create"))
    assert_receive {:stage_started, "implement", worker, run_id}, 5_000

    stop = event(c, :stop, "recovery-running-stop")

    :sys.replace_state(runtime.coordinator, fn state ->
      :ok = WorkstreamStore.linear_receive(state.workstreams.store, stop.id, stop)
      state
    end)

    stored = :sys.get_state(runtime.coordinator)
    refute Map.has_key?(stored.linear.events, stop.id)
    assert {:ok, %{events: events}} = WorkstreamStore.linear_load(stored.workstreams.store)
    assert %{status: :pending, event: ^stop} = events[stop.id]
    assert Process.alive?(worker)

    assert is_pid(restart(runtime))
    assert_receive {:recovery_cancel_requested, _operation_id, ^worker}, 10_000
    await(fn -> if Process.alive?(worker), do: nil, else: true end)

    stopped = wait_run(runtime, run_id, &(&1.status == :stopped))
    assert stopped.phase == :stopped
    assert length(stopped.attempts) == 1
    assert stopped.cancellation.termination == :terminated
    assert wait_linear_task(runtime, c.issue_id, &(&1.status == :stopped)).run_id == run_id
    assert :sys.get_state(runtime.coordinator).linear.events[stop.id].status == :handled
    assert [] == Task.Supervisor.children(runtime.tasks)
    refute_receive {:stage_started, _, _, _}, 100
  end

  test "an issue update invalidates an in-flight successful ownership check", c do
    parent = self()
    gate = start_supervised!({Agent, fn -> %{armed: false, phase: 0} end})

    issue_fetcher = fn _issue_id, _config ->
      phase =
        Agent.get_and_update(gate, fn
          %{armed: false} = state -> {:normal, state}
          %{armed: true, phase: 0} = state -> {{:hold, :stale_check}, %{state | phase: 1}}
          %{armed: true, phase: 1} = state -> {{:hold, :updated_scope}, %{state | phase: 2}}
          state -> {:normal, state}
        end)

      case phase do
        :normal ->
          {:ok, issue(c)}

        {:hold, :stale_check} ->
          send(parent, {:ownership_fetch_held, :stale_check, self()})

          receive do
            :release_stale_check -> {:ok, issue(c)}
          end

        {:hold, :updated_scope} ->
          send(parent, {:ownership_fetch_held, :updated_scope, self()})

          receive do
            :release_updated_scope -> {:ok, %{issue(c) | delegate_id: "different-agent"}}
          end
      end
    end

    runtime =
      runtime(c,
        auto_advance: false,
        linear_issue_fetcher: issue_fetcher,
        stage_executor: controlled_executor(parent)
      )

    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "scope-create"))
    task = wait_linear_task(runtime, c.issue_id, &(&1.status == :acknowledged))

    authorized =
      await(fn ->
        current = :sys.get_state(runtime.coordinator).linear.tasks[c.issue_id]
        if current && current[:authorized_stage] == "implement", do: current
      end)

    Agent.update(gate, &%{&1 | armed: true})
    assert_receive {:ownership_fetch_held, :stale_check, old_check}, 10_000

    revision = authorized[:scope_revision] || 0
    update = event(c, :issue_updated, "scope-updated")
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, update)

    invalidated = :sys.get_state(runtime.coordinator).linear.tasks[c.issue_id]
    assert invalidated.scope_revision == revision + 1
    assert invalidated.authorized_stage == nil
    assert invalidated.authorized_until == 0

    send(old_check, :release_stale_check)
    assert_receive {:ownership_fetch_held, :updated_scope, update_check}, 10_000

    after_stale_result = :sys.get_state(runtime.coordinator).linear.tasks[c.issue_id]
    assert after_stale_result.scope_revision == revision + 1
    assert after_stale_result.authorized_stage == nil
    assert after_stale_result.authorized_until == 0
    assert %{status: :ready, attempts: []} = run_state(runtime, task.run_id)
    refute_receive {:stage_started, _, _, _}, 100

    send(update_check, :release_updated_scope)
    assert %{status: :stopped, attempts: []} = wait_run(runtime, task.run_id, &(&1.status == :stopped))
    wait_linear_task(runtime, c.issue_id, &(&1.status == :stopped))
    final_task = :sys.get_state(runtime.coordinator).linear.tasks[c.issue_id]
    assert :sys.get_state(runtime.coordinator).linear.tasks[c.issue_id].scope_revision == revision + 1
    assert final_task.authorized_stage == nil
    refute_receive {:stage_started, _, _, _}, 100
  end

  test "an unknown configured agent blocks with its routing diagnostic before dispatch", c do
    parent = self()
    config = Map.put(c.config, "agent_id", "missing-agent")
    runtime = runtime(c, linear_delegation: config, stage_executor: controlled_executor(parent))

    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "invalid-agent"))
    task = wait_linear_task(runtime, c.issue_id, &(&1.status == :blocked))

    assert task.run_id == nil
    assert task.error =~ "unknown_workstream_agent_or_workspace"
    assert Task.Supervisor.children(runtime.tasks) == []
    assert map_size(:sys.get_state(runtime.coordinator).workstreams.runs) == 0
    refute_receive {:stage_started, _, _, _}, 100
  end

  test "custom service credential references block before an agent can inherit them", c do
    parent = self()
    config = Map.put(c.config, "token_env", "FACTORY_APP_TOKEN")
    runtime = runtime(c, linear_delegation: config, stage_executor: controlled_executor(parent))

    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "unsafe-credential-name"))
    task = wait_linear_task(runtime, c.issue_id, &(&1.status == :blocked))
    assert task.error =~ "credential_environment_isolation_unavailable"
    assert task.run_id == nil
    assert Task.Supervisor.children(runtime.tasks) == []
    refute_receive {:stage_started, _, _, _}, 100
  end

  test "unqualified readiness blocks before acknowledgement and stage execution", c do
    parent = self()

    runtime =
      runtime(c,
        linear_readiness: fn _definition, _agent -> {:error, :worker_readiness_unavailable} end,
        linear_acknowledger: fn task, _config ->
          send(parent, {:unexpected_ack, task})
          {:ok, %{id: task.activity_id}}
        end
      )

    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "unqualified-agent"))
    task = wait_linear_task(runtime, c.issue_id, &(&1.status == :blocked))
    assert task.error =~ "worker_readiness_unavailable"
    assert task.run_id == nil
    refute_receive {:unexpected_ack, _}, 100
    refute_receive {:stage_started, _, _, _}, 100
  end

  test "pending-event recovery cannot bypass credential isolation by reusing a pinned run", c do
    config = Map.put(c.config, "token_env", "FACTORY_APP_TOKEN")
    runtime = runtime(c, linear_delegation: config, auto_advance: false)

    assert {:ok, run_id} =
             Orchestrator.queue_workstream(runtime.coordinator, "linear-run/" <> c.issue_id, "linear/" <> c.issue_id, c.workstream_path, %{"task" => "Pinned before task linking"},
               workspace: c.workspace,
               workspace_root: c.workspace_root,
               issue_id: c.issue_id
             )

    created = event(c, :created, "unsafe-orphan-created")

    :sys.replace_state(runtime.coordinator, fn state ->
      :ok = WorkstreamStore.linear_receive(state.workstreams.store, created.id, created)
      state
    end)

    File.rm!(c.workstream_path)
    assert is_pid(restart(runtime))
    task = wait_linear_task(runtime, c.issue_id, &(&1.status == :blocked))
    assert task.error =~ "credential_environment_isolation_unavailable"
    assert task.run_id == nil
    assert %{status: :ready, attempts: []} = run_state(runtime, run_id)
    assert map_size(:sys.get_state(runtime.coordinator).workstreams.runs) == 1
    assert [] == Task.Supervisor.children(runtime.tasks)
  end

  test "a definition changed after qualification blocks before enqueue", c do
    runtime =
      runtime(c,
        linear_readiness: fn _definition, _agent ->
          source = File.read!(c.workstream_path)
          File.write!(c.workstream_path, String.replace(source, "Implement the delegated task.", "Changed after qualification."))
          :ok
        end
      )

    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "changed-definition"))
    task = wait_linear_task(runtime, c.issue_id, &(&1.status == :blocked))
    assert task.error =~ "workstream_definition_changed_before_enqueue"
    assert task.run_id == nil
    assert map_size(:sys.get_state(runtime.coordinator).workstreams.runs) == 0
    assert [] == Task.Supervisor.children(runtime.tasks)
  end

  test "failed cancellation keeps unknown execution capacity reserved through restart", c do
    runtime = runtime(c, workstream_canceller: fn _run, _operation, _pid -> raise "controlled cancellation failure" end)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "uncertain-created"))
    assert_receive {:stage_started, "implement", worker, run_id}, 5_000
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :stop, "uncertain-stop"))
    refute Process.alive?(worker)
    stopped = wait_run(runtime, run_id, &(&1.status == :stopped))
    assert stopped.cancellation.termination == :unknown

    workspace = Path.join(c.workspace_root, "second-rig")
    File.mkdir_p!(workspace)
    {_, 0} = System.cmd("git", ["init", "--quiet", workspace])

    assert {:ok, waiting_id} =
             Orchestrator.queue_workstream(runtime.coordinator, "other-queue", "other-task", c.workstream_path, %{"task" => "Wait for a proven free slot"},
               workspace: workspace,
               workspace_root: c.workspace_root
             )

    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert %{status: :ready, attempts: []} = run_state(runtime, waiting_id)
    assert is_pid(restart(runtime))
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert %{status: :ready, attempts: []} = run_state(runtime, waiting_id)
    assert %{status: :stopped, cancellation: %{termination: :unknown}} = run_state(runtime, run_id)
    assert [] == Task.Supervisor.children(runtime.tasks)
    refute_receive {:stage_started, _, _, _}, 100
  end

  test "stop acknowledges while termination is pending and keeps the coordinator responsive", c do
    parent = self()

    canceller = fn _run, _operation, _pid ->
      send(parent, {:termination_pending, self()})

      receive do
        :confirm_termination -> :terminated
      end
    end

    runtime = runtime(c, workstream_canceller: canceller)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "async-created"))
    assert_receive {:stage_started, "implement", _worker, run_id}, 5_000
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :stop, "async-stop"))
    assert_receive {:termination_pending, cancellation}, 5_000
    assert %{status: :stopped, cancellation: %{termination: :unknown}} = run_state(runtime, run_id)
    stored = :sys.get_state(runtime.coordinator).workstreams.runs[run_id]
    assert stored.operations[stored.current_attempt_id].status == :executing
    assert {:duplicate, _} = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :stop, "async-stop"))
    assert :ok = Orchestrator.reconcile_workstreams(runtime.coordinator)
    refute_receive {:termination_pending, _}, 100
    send(cancellation, :confirm_termination)
    wait_run(runtime, run_id, &(&1.cancellation.termination == :terminated))
  end

  test "reconciliation releases a stopped slot only after trusted termination proof", c do
    proof = start_supervised!({Agent, fn -> :unknown end})
    runtime = runtime(c, workstream_canceller: fn _run, _operation, _pid -> Agent.get(proof, & &1) end)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "reconcile-created"))
    assert_receive {:stage_started, "implement", _worker, run_id}, 5_000
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :stop, "reconcile-stop"))
    assert %{cancellation: %{termination: :unknown}} = run_state(runtime, run_id)
    assert :ok = Orchestrator.reconcile_workstreams(runtime.coordinator)
    assert %{cancellation: %{termination: :unknown}} = run_state(runtime, run_id)
    Agent.update(proof, fn _ -> :terminated end)
    assert :ok = Orchestrator.reconcile_workstreams(runtime.coordinator)
    wait_run(runtime, run_id, &(&1.cancellation.termination == :terminated))
    assert %{status: :stopped, cancellation: %{termination: :terminated}} = run_state(runtime, run_id)
    stored = :sys.get_state(runtime.coordinator).workstreams.runs[run_id]
    assert stored.operations[stored.current_attempt_id].status == :canceled
    assert is_pid(restart(runtime))
    assert %{status: :stopped, cancellation: %{termination: :terminated}} = run_state(runtime, run_id)
    refute_receive {:stage_started, _, _, _}, 100
  end

  test "a lost external termination proof keeps the completed worker's operation reserved", c do
    parent = self()

    executor = fn _stage, _definition, run, _opts ->
      send(parent, {:uncertain_worker, self(), run.id})

      receive do
        :finish_without_proof -> {:uncertain, :external_termination_unknown}
      end
    end

    runtime = runtime(c, stage_executor: executor, max_concurrent_agents: 1)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "uncertain-result"))
    assert_receive {:uncertain_worker, worker, run_id}, 5_000
    send(worker, :finish_without_proof)
    await(fn -> if run_state(runtime, run_id).status == :reconciling, do: true end)
    stored = :sys.get_state(runtime.coordinator).workstreams.runs[run_id]
    assert stored.operations[stored.current_attempt_id].status == :executing
    assert List.last(stored.attempts).status == :executing
    assert is_pid(restart(runtime))
    assert run_state(runtime, run_id).status == :reconciling
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    refute_receive {:uncertain_worker, _, _}, 100
    refute File.exists?(Path.join(c.workspace, "effects"))
  end

  test "real dispatch stays blocked when execution control is unavailable", c do
    assert {:error, :worker_execution_control_unavailable} =
             SymphonyElixir.Linear.Delegation.prepare(
               issue(c),
               c.config,
               linear_readiness: fn _definition, _agent -> :ok end
             )
  end

  @tag skip: :os.type() != {:unix, :linux}
  test "a held local process registers once durably before release and retains its identity after restart", c do
    parent = self()

    executor = fn _stage, _definition, run, opts ->
      python = System.find_executable("python3")
      script = "import os,sys\nif os.getpgrp() != os.getpid(): os.setsid()\nprint(os.getpid(),flush=True)\nif sys.stdin.readline() == 'start\\n':\n open('registered-effect','w').write('once')\n"

      port =
        Port.open({:spawn_executable, String.to_charlist(python)}, [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: [~c"-u", ~c"-c", String.to_charlist(script)],
          line: 1024,
          cd: String.to_charlist(run.workspace)
        ])

      try do
        pid =
          receive do
            {^port, {:data, {:eol, value}}} -> String.to_integer(value)
          after
            2000 -> raise "controlled process did not identify itself"
          end

        {:ok, identity} = SymphonyElixir.WorkstreamCancellation.capture_identity(pid, run.current_attempt_id)
        register = Keyword.fetch!(opts, :on_process_start)
        :ok = register.(identity)
        :ok = register.(identity)
        send(parent, {:process_held, self(), run.id, run.current_attempt_id, identity})

        receive do
          :register_after_restart -> :ok
        end

        :ok = register.(identity)
        {:error, _} = register.(Map.put(identity, "machine_id", String.duplicate("a", 32)))
        send(parent, {:process_registration_rechecked, self()})

        receive do
          :release_process -> :ok
        end

        true = Port.command(port, "start\n")

        receive do
          {^port, {:exit_status, 0}} -> :ok
        after
          2000 -> raise "controlled process did not exit"
        end

        {:ok, %{session_id: "controlled-process-registration"}}
      after
        if Port.info(port), do: Port.close(port)
      end
    end

    runtime = runtime(c, stage_executor: executor)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "registered-process"))
    assert_receive {:process_held, worker, run_id, attempt_id, identity}, 5_000
    refute File.exists?(Path.join(c.workspace, "registered-effect"))
    assert :sys.get_state(runtime.coordinator).workstreams.runs[run_id].operations[attempt_id].external_process == identity
    assert {:error, :process_registration_not_current} = GenServer.call(runtime.coordinator, {:workstream_process, run_id, attempt_id, worker, identity})
    assert is_pid(restart(runtime))
    send(worker, :register_after_restart)
    assert_receive {:process_registration_rechecked, ^worker}, 5_000
    state = :sys.get_state(runtime.coordinator)
    assert {:ok, stored} = WorkstreamStore.fetch(state.workstreams.store, run_id)
    assert stored.operations[attempt_id].external_process == identity
    refute File.exists?(Path.join(c.workspace, "registered-effect"))
    send(worker, :release_process)
    await(fn -> if File.exists?(Path.join(c.workspace, "registered-effect")), do: true end)
    assert File.read!(Path.join(c.workspace, "registered-effect")) == "once"
  end

  test "client credentials replace the legacy token reference exclusively", c do
    config = c.config |> Map.delete("token_env") |> Map.put("client_secret_env", "LINEAR_API_KEY")
    assert {:ok, _} = SymphonyElixir.Linear.Delegation.validate(config)
    assert {:error, :invalid_linear_delegation_config} = SymphonyElixir.Linear.Delegation.validate(Map.put(config, "token_env", "OAUTH_TOKEN"))
    assert {:error, :invalid_linear_delegation_config} = SymphonyElixir.Linear.Delegation.validate(Map.delete(config, "client_secret_env"))
  end

  test "question releases the slot and a native reply resumes the same stage once after restart", c do
    parent = self()

    executor = fn stage, definition, run, opts ->
      if stage.type == :agent do
        if Map.get(run, :continuation) do
          send(parent, {:continued_question, run.id, run.continuation})
          File.write!(Path.join(run.workspace, "candidate"), "answered")
          {:ok, %{thread_id: "original-thread", session_id: "original-thread-next"}}
        else
          {:ok, q} = opts[:on_question].(%{prompt: "Which harmless word should be written?"})
          send(parent, {:question_saved, q.id, self(), run.id})

          receive do
            :release_wait -> :ok
          end

          :wait = opts[:on_wait].(q.id)
          {:waiting, %{wait_id: q.id, thread_id: "original-thread", session_id: "original-thread-question"}}
        end
      else
        WorkstreamRunner.execute_stage(stage, definition, run, opts)
      end
    end

    publisher = fn _task, activity, _config ->
      send(parent, {:native_activity, activity})
      {:ok, %{id: activity.id}}
    end

    runtime = runtime(c, stage_executor: executor, linear_publisher: publisher, max_concurrent_agents: 1)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "question-created"))
    assert_receive {:question_saved, id, worker, run_id}, 5_000
    refute_receive {:continued_question, _, _}, 100
    send(worker, :release_wait)
    waiting = wait_run(runtime, run_id, &(&1.status == :waiting_for_answer))
    assert waiting.thread_id == "original-thread"
    await(fn -> if Task.Supervisor.children(runtime.tasks) == [], do: true end)
    assert is_pid(restart(runtime))
    assert %{status: :waiting_for_answer, pending_wait: %{id: ^id}} = run_state(runtime, run_id)

    # With the one execution slot released, a second controlled task can execute.
    second = Path.join(c.workspace_root, "slot-proof")
    File.mkdir_p!(second)
    {_, 0} = System.cmd("git", ["init", "--quiet", second])

    assert {:ok, second_id} =
             Orchestrator.queue_workstream(runtime.coordinator, "slot-proof", "slot-proof", c.workstream_path, %{"task" => "Harmless slot proof"}, workspace: second, workspace_root: c.workspace_root)

    assert_receive {:question_saved, second_question, second_worker, ^second_id}, 5_000
    send(second_worker, :release_wait)
    wait_run(runtime, second_id, &(&1.status == :waiting_for_answer and &1.pending_wait.id == second_question))

    reply = event(c, :prompted, "question-reply", %{activity_id: "human-reply-1", body: "answer #{id}: blue"})
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, reply)
    assert_receive {:continued_question, ^run_id, context}, 5_000
    assert inspect(context) =~ "blue"
    assert inspect(context) =~ "original-thread"
    wait_run(runtime, run_id, &(&1.stage_id == "wait"))
    assert {:duplicate, _} = Orchestrator.receive_linear_event(runtime.coordinator, reply)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, %{reply | id: "second-delivery-same-activity"})
    await(fn -> if :sys.get_state(runtime.coordinator).linear.events["second-delivery-same-activity"].status == :duplicate_reply, do: true end)
    conflict = %{reply | id: "conflicting-activity-delivery", body: "answer #{id}: red"}
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, conflict)
    await(fn -> if :sys.get_state(runtime.coordinator).linear.events[conflict.id].status == :reply_identity_conflict, do: true end)
    assert is_pid(restart(runtime))
    refute_receive {:continued_question, ^run_id, _}, 150
    assert File.read!(Path.join(c.workspace, "effects")) == "checked"
    assert_receive {:native_activity, %{content: %{"type" => "elicitation"}}}, 5_000
  end

  test "reply arriving during independent work is stored and delivered once at the safe boundary", c do
    parent = self()

    executor = fn stage, definition, run, opts ->
      if stage.type == :agent do
        {:ok, q} = opts[:on_question].(%{prompt: "Choose the harmless label."})
        send(parent, {:midturn_question, q.id, self(), run.id})

        receive do
          :finish_independent_work -> :ok
        end

        {:answered, %{body: body}} = opts[:on_wait].(q.id)
        send(parent, {:midturn_answer, body})
        {:ok, %{thread_id: "same-live-thread"}}
      else
        WorkstreamRunner.execute_stage(stage, definition, run, opts)
      end
    end

    runtime = runtime(c, stage_executor: executor)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, event(c, :created, "midturn-created"))
    assert_receive {:midturn_question, id, worker, run_id}, 5_000
    reply = event(c, :prompted, "midturn-reply", %{activity_id: "midturn-activity", body: "answer #{id}: green"})
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, reply)
    await(fn -> if :sys.get_state(runtime.coordinator).linear.events[reply.id].status == :reply_delivered, do: true end)
    assert is_pid(restart(runtime))
    assert Process.alive?(worker)
    assert :ok = Orchestrator.receive_linear_event(runtime.coordinator, %{reply | id: "midturn-replay"})
    send(worker, :finish_independent_work)
    assert_receive {:midturn_answer, "green"}, 5_000
    refute_receive {:midturn_answer, _}, 100
    # The check deliberately fails without a candidate, surfacing failure as a native error.
    wait_run(runtime, run_id, &(&1.status == :blocked))
    task = :sys.get_state(runtime.coordinator).linear.tasks[c.issue_id]
    assert Enum.any?(Map.values(task.publications), &(&1.content["type"] == "error"))
  end

  defp runtime(context, extra) do
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "Runtime#{suffix}")
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    issue_fetcher = fn issue_id, _config ->
      if issue_id == context.issue_id, do: {:ok, issue(context)}, else: {:error, :unknown_issue}
    end

    session_fetcher = fn session_id, _config ->
      {:ok,
       %{
         "id" => session_id,
         "issueId" => context.issue_id,
         "appUser" => %{"id" => @app_user_id},
         "dismissedAt" => nil
       }}
    end

    defaults = [
      name: supervisor,
      orchestrator_name: coordinator,
      task_supervisor_name: tasks,
      workstream_store_path: context.store_path,
      linear_delegation: context.config,
      stage_executor: controlled_executor(self()),
      linear_issue_fetcher: issue_fetcher,
      linear_session_fetcher: session_fetcher,
      linear_readiness: fn _definition, _agent_id -> :ok end,
      linear_execution_control: fn _definition, _agent_id -> :ok end,
      linear_acknowledger: fn task, _config -> {:ok, %{id: task.activity_id}} end,
      linear_publisher: fn _task, activity, _config -> {:ok, %{id: activity.id}} end,
      workstream_canceller: fn _run, _operation, _worker -> :terminated end
    ]

    opts = Keyword.merge(defaults, extra)
    start_supervised!({AgentRuntimeSupervisor, opts}, id: supervisor)
    %{supervisor: supervisor, coordinator: coordinator, tasks: tasks}
  end

  defp issue(context) do
    %Issue{
      id: context.issue_id,
      identifier: "DEV-233",
      title: "Exercise native delegation lifecycle",
      description: "Run the controlled local lifecycle fixture.",
      state: "Backlog",
      state_type: "started",
      team_id: "linear-dev-team",
      delegate_id: @app_user_id,
      assignee_id: "human-owner-17",
      labels: ["factory:rig"],
      dispatchable: true
    }
  end

  defp event(context, kind, id, extra \\ %{}) do
    Map.merge(
      %{
        id: id,
        kind: kind,
        issue_id: context.issue_id,
        session_id: @session_id,
        app_user_id: @app_user_id,
        oauth_client_id: @oauth_client_id,
        organization_id: @organization_id,
        timestamp: 1_800_000_000_000,
        activity_id: nil
      },
      extra
    )
  end

  defp controlled_executor(parent) do
    fn stage, definition, run, opts ->
      send(parent, {:stage_started, stage.id, self(), run.id})

      case stage.type do
        :agent ->
          receive do
            :finish_agent ->
              File.write!(Path.join(run.workspace, "candidate"), "controlled candidate")

              {:ok,
               %{
                 session_id: "controlled-session",
                 thread_id: "controlled-thread",
                 model: definition.agents[stage.agent].model
               }}
          end

        :check ->
          WorkstreamRunner.execute_stage(stage, definition, run, opts)
      end
    end
  end

  defp linear_task(runtime, issue_id) do
    await(fn ->
      case Enum.find(Orchestrator.snapshot(runtime.coordinator, 1_000).linear.tasks, &(&1.issue_id == issue_id)) do
        nil -> nil
        task -> task
      end
    end)
  end

  defp wait_linear_task(runtime, issue_id, predicate) do
    await(fn ->
      case Enum.find(Orchestrator.snapshot(runtime.coordinator, 1_000).linear.tasks, &(&1.issue_id == issue_id)) do
        nil -> nil
        task -> if predicate.(task), do: task
      end
    end)
  end

  defp run_state(runtime, run_id) do
    case Orchestrator.workstream_state(runtime.coordinator, run_id) do
      {:ok, report} -> report
      _ -> nil
    end
  end

  defp wait_run(runtime, run_id, predicate) do
    await(fn ->
      case run_state(runtime, run_id) do
        nil -> nil
        report -> if predicate.(report), do: report
      end
    end)
  end

  defp restart(runtime) do
    old = Process.whereis(runtime.coordinator)
    reference = Process.monitor(old)
    Process.exit(old, :kill)
    assert_receive {:DOWN, ^reference, :process, ^old, :killed}

    await(fn ->
      case Process.whereis(runtime.coordinator) do
        pid when is_pid(pid) and pid != old ->
          try do
            if is_map(GenServer.call(pid, :snapshot, 1_000)), do: pid
          catch
            :exit, _ -> nil
          end

        _ ->
          nil
      end
    end)
  end

  defp await(fun, tries \\ 2_000)
  defp await(_fun, 0), do: flunk("observable Linear delegation state did not converge")

  defp await(fun, tries) do
    case fun.() do
      nil ->
        Process.sleep(10)
        await(fun, tries - 1)

      false ->
        Process.sleep(10)
        await(fun, tries - 1)

      value ->
        value
    end
  end
end
