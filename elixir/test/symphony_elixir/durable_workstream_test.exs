defmodule SymphonyElixir.DurableWorkstreamTest do
  use ExUnit.Case, async: false
  alias SymphonyElixir.{AgentRuntimeSupervisor, Orchestrator, WorkstreamRun, WorkstreamStore}

  setup do
    root = Path.join(System.tmp_dir!(), "durable-smoke-#{System.unique_integer([:positive])}")
    workspace = Path.join(root, "workspaces/rig")
    File.mkdir_p!(workspace)
    {_, 0} = System.cmd("git", ["init", "--quiet", workspace])
    File.write!(Path.join(root, "instructions.md"), "Pinned instructions")
    File.write!(Path.join(root, "SKILL.md"), "Pinned skill")

    File.write!(Path.join(root, "agent.yaml"), """
    version: 1
    name: controlled
    model: gpt-6-luna
    reasoning_effort: medium
    daybreak: false
    approval_policy: never
    sandbox: workspace-write
    instructions: [instructions.md]
    skills: [SKILL.md]
    """)

    path = Path.join(root, "flow.yaml")

    File.write!(path, """
    version: 1
    name: controlled-local
    inputs: [task]
    agents:
      worker: agent.yaml
    entry: implement
    stages:
      - id: implement
        type: agent
        inputs: [task]
        outputs: [candidate]
        agent: worker
        prompt: Create a candidate.
        next: validate
      - id: validate
        type: check
        inputs: [candidate]
        outputs: [validation]
        gate:
          command: [sh, -c, "printf checked >> effects; test -f candidate"]
          timeout_ms: 1000
          success: decision
          failure:
            repair: implement
            max_attempts: 1
      - id: decision
        type: human_wait
        inputs: [candidate, validation]
        outputs: [answer]
        prompt: Continue the local demo?
        next: complete
    """)

    context = %{root: root, workspace: workspace, path: path, db: Path.join(root, "state/runs.sqlite"), options: [workspace: workspace, workspace_root: Path.join(root, "workspaces")]}
    on_exit(fn -> File.rm_rf!(root) end)
    context
  end

  defp runtime(context, executor, extra \\ []) do
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "Runtime#{suffix}")
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")
    opts = [name: supervisor, orchestrator_name: coordinator, task_supervisor_name: tasks, workstream_store_path: context.db, stage_executor: executor] ++ extra
    pid = start_supervised!({AgentRuntimeSupervisor, opts}, id: supervisor)
    %{pid: pid, supervisor: supervisor, coordinator: coordinator, tasks: tasks}
  end

  defp restart(runtime) do
    old = Process.whereis(runtime.coordinator)
    ref = Process.monitor(old)
    Process.exit(old, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old, :killed}

    await(fn ->
      case Process.whereis(runtime.coordinator) do
        pid when is_pid(pid) and pid != old ->
          try do
            if is_map(GenServer.call(pid, :snapshot)), do: pid
          catch
            :exit, _ -> nil
          end

        _ ->
          nil
      end
    end)
  end

  defp await(fun, tries \\ 200)
  defp await(_fun, 0), do: flunk("observable state did not converge")

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

  defp run_state(runtime, id, status) do
    await(fn ->
      case Orchestrator.workstream_state(runtime.coordinator, id) do
        {:ok, %{status: ^status} = report} -> report
        _ -> nil
      end
    end)
  end

  defp queue(runtime, context, event \\ "queue", task \\ "task") do
    assert {:ok, id} = Orchestrator.queue_workstream(runtime.coordinator, event, task, context.path, %{"task" => "controlled smoke"}, context.options)
    id
  end

  defp fixture_run(context, task_id, path \\ nil) do
    inputs = %{"task" => "controlled smoke"}
    path = path || context.path
    assert {:ok, definition, workspace} = SymphonyElixir.WorkstreamRunner.prepare(path, inputs, context.options)
    assert {:ok, execution} = SymphonyElixir.WorkstreamRunner.pin_execution_context(context.options, workspace, definition)
    WorkstreamRun.new(task_id, definition, workspace, context.options, execution)
  end

  defp persist_pre_dev234_run(context, run, event_id, expected_policy \\ WorkstreamRun.policy()) do
    legacy = Map.drop(run, [:questions, :inbox, :activity_ids, :continuation])

    assert Map.has_key?(run, :questions)
    assert Map.has_key?(run, :inbox)
    assert Map.has_key?(run, :activity_ids)
    assert Map.has_key?(run, :continuation)
    assert run.policy == expected_policy

    assert {:ok, store} = WorkstreamStore.start_link(path: context.db, owner: self())
    assert :ok = WorkstreamStore.commit(store, legacy, event_id, %{kind: :pre_dev234_fixture})
    assert :ok = GenServer.stop(store)
    legacy
  end

  defp coordinator_run(runtime, id) do
    await(fn ->
      case Map.get(:sys.get_state(runtime.coordinator).workstreams.runs, id) do
        run when is_map(run) -> run
        _ -> nil
      end
    end)
  end

  defp assert_pre_dev234_defaults(loaded, legacy) do
    assert loaded.questions == %{}
    assert loaded.inbox == []
    assert loaded.activity_ids == %{}
    assert loaded.continuation == nil
    assert loaded.id == legacy.id
    assert loaded.task_id == legacy.task_id
    assert loaded.definition == legacy.definition
    assert loaded.execution == legacy.execution
    assert loaded.policy == legacy.policy
    assert loaded.artifacts == legacy.artifacts
    assert loaded.attempts == legacy.attempts
    assert loaded.operations == legacy.operations
  end

  defp persist_stopped_run(context, task_id, termination, operation_status) do
    inputs = %{"task" => "controlled smoke"}
    assert {:ok, definition, workspace} = SymphonyElixir.WorkstreamRunner.prepare(context.path, inputs, context.options)
    assert {:ok, execution} = SymphonyElixir.WorkstreamRunner.pin_execution_context(context.options, workspace, definition)
    run = WorkstreamRun.new(task_id, definition, workspace, context.options, execution) |> WorkstreamRun.start_stage()
    attempt_id = run.current_attempt_id

    attempts =
      Enum.map(run.attempts, fn attempt ->
        if attempt.id == attempt_id, do: Map.put(attempt, :status, :executing), else: attempt
      end)

    operation =
      run.operations[attempt_id]
      |> Map.put(:status, operation_status)
      |> Map.put(:reconciliation, ":unknown")

    operation =
      if termination == :terminated do
        Map.put(operation, :cancellation, %{termination: :terminated})
      else
        operation
      end

    stopped =
      run
      |> Map.put(:status, :stopped)
      |> Map.put(:phase, :stopped)
      |> Map.put(:cancellation, %{termination: termination})
      |> Map.put(:attempts, attempts)
      |> Map.put(:operations, Map.put(run.operations, attempt_id, operation))

    assert {:ok, store} = WorkstreamStore.start_link(path: context.db, owner: self())
    assert :ok = WorkstreamStore.commit(store, stopped, stopped.id <> "/fixture-stopped", %{kind: :stopped})
    assert :ok = GenServer.stop(store)
    stopped
  end

  defp await_cancellations(runtime) do
    await(fn ->
      if map_size(:sys.get_state(runtime.coordinator).workstreams.cancellations) == 0, do: true
    end)
  end

  defp normal_executor(stage, definition, run, opts) do
    if stage.type == :agent do
      File.write!(Path.join(run.workspace, "candidate"), "candidate")
      {:ok, %{session_id: "controlled-session", thread_id: "controlled-thread", model: definition.agents["worker"].model}}
    else
      SymphonyElixir.WorkstreamRunner.execute_stage(stage, definition, run, opts)
    end
  end

  test "a pre-DEV234 ready agent run dispatches once after the store is reopened", c do
    run = fixture_run(c, "legacy-ready")
    legacy = persist_pre_dev234_run(c, run, "legacy-ready-fixture")
    parent = self()
    effects = Path.join(c.root, "legacy-ready-effects")

    executor = fn stage, definition, run, opts ->
      if stage.type == :agent do
        send(parent, {:pre_dev234_agent_started, run.id, run.operations[run.current_attempt_id].side_effect_id})
        File.write!(effects, "agent\n", [:append])
      end

      normal_executor(stage, definition, run, opts)
    end

    runtime = runtime(c, executor, auto_advance: false)
    loaded = coordinator_run(runtime, legacy.id)
    assert_pre_dev234_defaults(loaded, legacy)
    assert loaded.status == :ready
    assert loaded.policy == WorkstreamRun.policy()

    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert_receive {:pre_dev234_agent_started, run_id, side_effect_id}
    assert run_id == legacy.id
    assert side_effect_id == "#{legacy.id}/implement/1"
    ready = run_state(runtime, legacy.id, :ready)
    assert ready.stage_id == "validate"
    assert File.read!(effects) == "agent\n"

    restart(runtime)
    assert run_state(runtime, legacy.id, :ready).stage_id == "validate"
    refute_receive {:pre_dev234_agent_started, _, _}, 100
    assert File.read!(effects) == "agent\n"
  end

  test "a pre-DEV234 human wait can be answered into an agent stage", c do
    path = Path.join(c.root, "legacy-wait-agent.yaml")

    File.write!(path, """
    version: 1
    name: legacy-wait-agent
    inputs: [task]
    agents:
      worker: agent.yaml
    entry: setup
    stages:
      - id: setup
        type: agent
        inputs: [task]
        outputs: [initial_candidate]
        agent: worker
        prompt: Prepare the initial candidate.
        next: setup_check
      - id: setup_check
        type: check
        inputs: [initial_candidate]
        outputs: [initial_validation]
        gate:
          command: [sh, -c, "test -f initial_candidate"]
          timeout_ms: 1000
          success: review
          failure:
            repair: setup
            max_attempts: 1
      - id: review
        type: human_wait
        inputs: [initial_candidate, initial_validation]
        outputs: [answer]
        prompt: Review the task.
        next: implement
      - id: implement
        type: agent
        inputs: [task, answer]
        outputs: [candidate]
        agent: worker
        prompt: Use the answer.
        next: validate
      - id: validate
        type: check
        inputs: [candidate]
        outputs: [validation]
        gate:
          command: [sh, -c, "printf checked >> effects; test -f candidate"]
          timeout_ms: 1000
          success: decision
          failure:
            repair: implement
            max_attempts: 1
      - id: decision
        type: human_wait
        inputs: [candidate, validation]
        outputs: [review]
        prompt: Approve the candidate.
        next: complete
    """)

    run =
      fixture_run(c, "legacy-wait-agent", path)
      |> WorkstreamRun.start_stage()
      |> WorkstreamRun.finish_stage({:ok, %{session_id: "initial-session", thread_id: "initial-thread", initial_candidate: "initial"}})
      |> WorkstreamRun.start_stage()
      |> WorkstreamRun.finish_stage({:ok, %{exit_status: 0, timed_out: false, output: "initial check passed"}})
      |> WorkstreamRun.start_stage()

    legacy = persist_pre_dev234_run(c, run, "legacy-wait-agent-fixture")
    parent = self()
    effects = Path.join(c.root, "legacy-wait-agent-effects")

    executor = fn stage, definition, run, opts ->
      if stage.type == :agent do
        send(parent, {:pre_dev234_wait_agent_started, run.id, run.outputs["answer"]})
        File.write!(effects, "agent\n", [:append])
      end

      normal_executor(stage, definition, run, opts)
    end

    runtime = runtime(c, executor, auto_advance: false)
    loaded = coordinator_run(runtime, legacy.id)
    assert_pre_dev234_defaults(loaded, legacy)
    assert loaded.status == :waiting_for_answer
    wait_id = loaded.pending_wait.id

    assert {:ok, native_reply} =
             WorkstreamRun.linear_reply(loaded, "answer #{wait_id}: {\"answer\":\"from Linear\"}", "legacy-linear-activity")

    assert native_reply.status == :ready
    assert native_reply.stage_id == "implement"
    assert native_reply.outputs["answer"] == "from Linear"
    assert Map.has_key?(native_reply.activity_ids, "legacy-linear-activity")

    assert :ok =
             Orchestrator.answer_workstream(
               runtime.coordinator,
               "legacy-answer",
               legacy.id,
               wait_id,
               %{"answer" => "from legacy API"}
             )

    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert_receive {:pre_dev234_wait_agent_started, run_id, "from legacy API"}
    assert run_id == legacy.id
    assert run_state(runtime, legacy.id, :ready).stage_id == "validate"
    assert File.read!(effects) == "agent\n"

    restart(runtime)
    assert run_state(runtime, legacy.id, :ready).stage_id == "validate"
    refute_receive {:pre_dev234_wait_agent_started, _, _}, 100
    assert File.read!(effects) == "agent\n"
  end

  test "an active pre-DEV234 operation reconciles to completion without replay", c do
    run =
      fixture_run(c, "legacy-active-completion")
      |> WorkstreamRun.start_stage()
      |> WorkstreamRun.finish_stage({:ok, %{session_id: "fixture-session", thread_id: "fixture-thread", candidate: "candidate"}})
      |> WorkstreamRun.start_stage()

    check_id = run.current_attempt_id
    side_effect_id = "legacy-check-effect-1"
    operation = Map.put(run.operations[check_id], :side_effect_id, side_effect_id)
    run = put_in(run.operations[check_id], operation)
    effects = Path.join(c.root, "legacy-check-effects")
    File.write!(effects, side_effect_id <> "\n")
    legacy = persist_pre_dev234_run(c, run, "legacy-active-completion-fixture")
    parent = self()

    reconciler = fn _run, operation ->
      send(parent, {:pre_dev234_completed, operation.id, operation.side_effect_id})
      {:completed, {:ok, %{exit_status: 0, timed_out: false, output: "already checked"}}}
    end

    executor = fn stage, _definition, _run, _opts ->
      send(parent, {:unexpected_reexecution, stage.id})
      {:error, :completed_operation_must_not_run_again}
    end

    runtime = runtime(c, executor, auto_advance: false, workstream_reconciler: reconciler)
    recovered = coordinator_run(runtime, legacy.id)
    assert recovered.questions == %{}
    assert recovered.inbox == []
    assert recovered.activity_ids == %{}
    assert recovered.continuation == nil
    assert recovered.id == legacy.id
    assert recovered.definition == legacy.definition
    assert recovered.execution == legacy.execution
    assert recovered.policy == legacy.policy

    for {artifact_id, artifact} <- legacy.artifacts do
      assert recovered.artifacts[artifact_id] == artifact
    end

    assert Enum.map(recovered.attempts, & &1.id) == Enum.map(legacy.attempts, & &1.id)
    assert Map.keys(recovered.operations) == Map.keys(legacy.operations)
    assert recovered.operations[check_id].status == :completed
    assert recovered.operations[check_id].side_effect_id == side_effect_id
    assert Enum.find(recovered.attempts, &(&1.id == check_id)).status == :completed
    assert recovered.stage_id == "decision"
    assert_receive {:pre_dev234_completed, ^check_id, ^side_effect_id}
    refute_receive {:unexpected_reexecution, _}
    assert File.read!(effects) == side_effect_id <> "\n"

    restart(runtime)
    assert run_state(runtime, legacy.id, :ready).stage_id == "decision"
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert run_state(runtime, legacy.id, :waiting_for_answer).stage_id == "decision"
    refute_receive {:pre_dev234_completed, _, _}
    refute_receive {:unexpected_reexecution, _}
    assert File.read!(effects) == side_effect_id <> "\n"
  end

  test "a terminated pre-DEV234 agent retry keeps its side effect ID", c do
    run = fixture_run(c, "legacy-terminated-retry") |> WorkstreamRun.start_stage()
    original_attempt_id = run.current_attempt_id
    side_effect_id = "legacy-agent-effect-1"
    run = put_in(run.operations[original_attempt_id].side_effect_id, side_effect_id)
    effects = Path.join(c.root, "legacy-agent-effects")
    File.write!(effects, side_effect_id <> "\n")
    legacy = persist_pre_dev234_run(c, run, "legacy-terminated-retry-fixture")
    parent = self()

    reconciler = fn _run, operation ->
      send(parent, {:pre_dev234_terminated, operation.id, operation.side_effect_id})
      :terminated
    end

    executor = fn stage, definition, run, opts ->
      if stage.type == :agent do
        retry_effect_id = run.operations[run.current_attempt_id].side_effect_id
        send(parent, {:pre_dev234_retry_started, run.current_attempt_id, retry_effect_id})

        effects_before = File.read!(effects) |> String.split("\n", trim: true)
        if retry_effect_id not in effects_before, do: File.write!(effects, retry_effect_id <> "\n", [:append])
      end

      normal_executor(stage, definition, run, opts)
    end

    runtime = runtime(c, executor, auto_advance: false, workstream_reconciler: reconciler)
    recovered = coordinator_run(runtime, legacy.id)
    assert recovered.questions == %{}
    assert recovered.inbox == []
    assert recovered.activity_ids == %{}
    assert recovered.continuation == nil
    assert recovered.id == legacy.id
    assert recovered.definition == legacy.definition
    assert recovered.execution == legacy.execution
    assert recovered.policy == legacy.policy
    assert recovered.artifacts == legacy.artifacts
    assert recovered.retry_operation_id == original_attempt_id
    assert recovered.operations[original_attempt_id].status == :not_applied
    assert recovered.operations[original_attempt_id].side_effect_id == side_effect_id
    assert Enum.map(recovered.attempts, & &1.id) == Enum.map(legacy.attempts, & &1.id)
    assert_receive {:pre_dev234_terminated, ^original_attempt_id, ^side_effect_id}

    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert_receive {:pre_dev234_retry_started, retry_attempt_id, ^side_effect_id}
    refute retry_attempt_id == original_attempt_id
    retried = run_state(runtime, legacy.id, :ready)
    assert retried.stage_id == "validate"
    assert retried.transport_retries == 1
    assert retried.operations[retry_attempt_id].side_effect_id == side_effect_id
    assert File.read!(effects) == side_effect_id <> "\n"

    restart(runtime)
    assert run_state(runtime, legacy.id, :ready).stage_id == "validate"
    refute_receive {:pre_dev234_retry_started, _, _}, 100
    assert File.read!(effects) == side_effect_id <> "\n"
  end

  test "restart retains pinned definitions, artifacts, workspace, waits and idempotent answers", c do
    runtime = runtime(c, &normal_executor/4)
    id = queue(runtime, c)
    before = run_state(runtime, id, :waiting_for_answer)
    assert before.workspace == c.workspace
    assert before.session_id == "controlled-session"
    assert [%{gate: nil}, %{gate: :passed}, %{status: :waiting}] = before.attempts
    assert File.read!(Path.join(c.workspace, "effects")) == "checked"
    File.write!(Path.join(c.root, "SKILL.md"), "Updated skill for future runs")
    restart(runtime)
    after_restart = run_state(runtime, id, :waiting_for_answer)
    assert after_restart == before
    assert {:ok, ^id} = Orchestrator.queue_workstream(runtime.coordinator, "queue", "task", c.path, %{"task" => "controlled smoke"}, c.options)
    assert queue(runtime, c, "redelivery", "task") == id
    assert {:error, :event_identity_conflict} = Orchestrator.queue_workstream(runtime.coordinator, "queue", "task", c.path, %{"task" => "changed"}, c.options)
    wait_id = before.pending_wait.id
    assert {:error, :invalid_human_wait_outputs} = Orchestrator.answer_workstream(runtime.coordinator, "bad", id, wait_id, %{})
    assert {:error, :invalid_human_wait_outputs} = Orchestrator.answer_workstream(runtime.coordinator, "bad", id, wait_id, %{"answer" => self()})
    assert :ok = Orchestrator.answer_workstream(runtime.coordinator, "answer", id, wait_id, %{"answer" => "yes"})
    assert :ok = Orchestrator.answer_workstream(runtime.coordinator, "answer", id, wait_id, %{"answer" => "yes"})
    assert {:error, :event_identity_conflict} = Orchestrator.answer_workstream(runtime.coordinator, "answer", id, wait_id, %{"answer" => "no"})
    complete = run_state(runtime, id, :complete)
    assert complete.outputs["answer"] == "yes"
    assert length(complete.attempts) == 3
    assert Task.Supervisor.children(runtime.tasks) == []
    other = Path.join(c.root, "workspaces/other")
    File.mkdir_p!(other)
    {_, 0} = System.cmd("git", ["init", "--quiet", other])
    assert {:ok, new_id} = Orchestrator.queue_workstream(runtime.coordinator, "new", "new-task", c.path, %{"task" => "new"}, Keyword.put(c.options, :workspace, other))
    newer = run_state(runtime, new_id, :waiting_for_answer)
    refute newer.definitions == before.definitions
    assert newer.id != id
  end

  test "restart at a real committed stage boundary runs only the pending next stage", c do
    runtime = runtime(c, &normal_executor/4, auto_advance: false)
    id = queue(runtime, c)
    assert run_state(runtime, id, :ready).phase == :queued
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    boundary = run_state(runtime, id, :ready)
    assert boundary.stage_id == "validate"
    assert length(boundary.attempts) == 1
    restart(runtime)
    assert run_state(runtime, id, :ready) == boundary
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    checked = run_state(runtime, id, :ready)
    assert checked.stage_id == "decision"
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    wait = run_state(runtime, id, :waiting_for_answer)
    assert length(wait.attempts) == 3
    assert File.read!(Path.join(c.workspace, "effects")) == "checked"
  end

  test "caller event IDs cannot shadow a future internal operation event", c do
    runtime = runtime(c, &normal_executor/4, auto_advance: false)
    id = queue(runtime, c)
    # This is exactly the next internal start-event identity before it exists.
    collision = "#{id}/implement/1/start"
    assert queue(runtime, c, collision, "task") == id
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    boundary = run_state(runtime, id, :ready)
    assert boundary.stage_id == "validate"
    restart(runtime)
    assert run_state(runtime, id, :ready) == boundary
    store = :sys.get_state(runtime.coordinator).workstreams.store
    assert {:ok, %{event: %{kind: :stage_started}}} = WorkstreamStore.event(store, collision)
    assert {:ok, %{event: %{kind: :queued}}} = WorkstreamStore.event(store, "input/" <> collision)
  end

  test "supervised coordinator restart reconciles original live worker and accepts its receipt once", c do
    parent = self()

    executor = fn stage, definition, run, opts ->
      if stage.type == :agent do
        send(parent, {:started, self(), run.current_attempt_id})

        receive do
          :finish -> normal_executor(stage, definition, run, opts)
        end
      else
        normal_executor(stage, definition, run, opts)
      end
    end

    runtime = runtime(c, executor)
    id = queue(runtime, c)
    assert_receive {:started, worker, attempt_id}
    first = run_state(runtime, id, :executing)
    restart(runtime)
    assert Process.alive?(worker)
    assert Task.Supervisor.children(runtime.tasks) == [worker]
    assert run_state(runtime, id, :executing).workspace == first.workspace
    send(worker, :finish)
    wait = run_state(runtime, id, :waiting_for_answer)
    refute_receive {:started, _, _}
    send(Process.whereis(runtime.coordinator), {:workstream_result, id, attempt_id, worker, {:ok, %{session_id: "replayed"}}})
    assert {:ok, ^wait} = Orchestrator.workstream_state(runtime.coordinator, id)
    assert File.read!(Path.join(c.workspace, "effects")) == "checked"
  end

  test "unknown worker liveness reserves capacity and blocks replacement across restart", c do
    parent = self()

    executor = fn _stage, _definition, _run, _opts ->
      send(parent, {:started, self()})

      receive do
        :never -> {:error, :not_expected}
      end
    end

    runtime = runtime(c, executor)
    id = queue(runtime, c)
    assert_receive {:started, worker}
    Process.exit(worker, :kill)
    pending = run_state(runtime, id, :reconciling)
    assert pending.phase == :reconciling
    restart(runtime)
    assert :ok = Orchestrator.reconcile_workstreams(runtime.coordinator)
    assert length(run_state(runtime, id, :reconciling).attempts) == 1
    assert Task.Supervisor.children(runtime.tasks) == []
    refute_receive {:started, _}
  end

  test "restart normalizes a legacy trusted stop without another worker-control call", c do
    parent = self()

    executor = fn _stage, _definition, _run, _opts ->
      send(parent, :unexpected_replacement)
      {:error, :stopped_run_must_not_launch}
    end

    stopped = persist_stopped_run(c, "legacy-task", :terminated, :canceled)
    id = stopped.id
    attempt_id = stopped.current_attempt_id

    canceller = fn _run, _operation, _pid ->
      send(parent, :unexpected_cancellation_request)
      :unknown
    end

    recovered = runtime(c, executor, workstream_canceller: canceller)

    normalized =
      await(fn ->
        case Orchestrator.workstream_state(recovered.coordinator, id) do
          {:ok, %{status: :stopped, attempts: attempts, operations: operations} = run} ->
            operation = operations[attempt_id]
            attempt = Enum.find(attempts, &(&1.id == attempt_id))

            if attempt.status == :canceled and operation.status == :canceled and not Map.has_key?(operation, :reconciliation), do: run

          _ ->
            nil
        end
      end)

    assert normalized.cancellation.termination == :terminated
    assert normalized.attempts |> Enum.find(&(&1.id == attempt_id)) |> Map.fetch!(:status) == :canceled
    assert normalized.operations[attempt_id].cancellation.termination == :terminated
    refute_receive :unexpected_cancellation_request
    refute_receive :unexpected_replacement

    assert :ok = Orchestrator.reconcile_workstreams(recovered.coordinator)
    assert {:ok, ^normalized} = Orchestrator.workstream_state(recovered.coordinator, id)
    refute_receive :unexpected_cancellation_request

    restart(recovered)
    assert {:ok, ^normalized} = Orchestrator.workstream_state(recovered.coordinator, id)
    refute_receive :unexpected_cancellation_request
    refute_receive :unexpected_replacement
  end

  test "unknown cancellation stays executing until the adapter returns trusted termination", c do
    parent = self()
    {:ok, proof} = Agent.start_link(fn -> %{"status" => "terminated"} end)
    on_exit(fn -> if Process.alive?(proof), do: Agent.stop(proof) end)

    stopped = persist_stopped_run(c, "unknown-task", :unknown, :canceled)
    id = stopped.id
    attempt_id = stopped.current_attempt_id

    canceller = fn _run, operation, _pid ->
      send(parent, {:cancellation_requested, operation.id})
      Agent.get(proof, & &1)
    end

    replacement_executor = fn _stage, _definition, run, _opts ->
      send(parent, {:replacement_started, run.task_id})

      receive do
        :never -> {:error, :not_expected}
      end
    end

    recovered = runtime(c, replacement_executor, workstream_canceller: canceller)
    assert_receive {:cancellation_requested, ^attempt_id}
    await_cancellations(recovered)

    assert {:ok, unconfirmed} = Orchestrator.workstream_state(recovered.coordinator, id)
    assert unconfirmed.cancellation.termination == :unknown
    assert Enum.find(unconfirmed.attempts, &(&1.id == attempt_id)).status == :executing
    assert unconfirmed.operations[attempt_id].status == :canceled
    assert unconfirmed.operations[attempt_id].reconciliation == ":unknown"

    other_workspace = Path.join(c.root, "workspaces/other")
    File.mkdir_p!(other_workspace)
    {_, 0} = System.cmd("git", ["init", "--quiet", other_workspace])
    other_options = Keyword.put(c.options, :workspace, other_workspace)

    assert {:ok, queued_id} =
             Orchestrator.queue_workstream(
               recovered.coordinator,
               "queue-second",
               "second-task",
               c.path,
               %{"task" => "wait behind unknown cancellation"},
               other_options
             )

    queued =
      await(fn ->
        case Orchestrator.workstream_state(recovered.coordinator, queued_id) do
          {:ok, %{status: status} = run} when status in [:ready, :executing] -> run
          _ -> nil
        end
      end)

    assert queued.status == :ready
    refute_receive {:replacement_started, _task_id}

    assert :ok = Orchestrator.reconcile_workstreams(recovered.coordinator)
    assert_receive {:cancellation_requested, ^attempt_id}
    await_cancellations(recovered)
    assert {:ok, still_unconfirmed} = Orchestrator.workstream_state(recovered.coordinator, id)
    assert still_unconfirmed == unconfirmed

    Agent.update(proof, fn _ -> :terminated end)
    assert :ok = Orchestrator.reconcile_workstreams(recovered.coordinator)
    assert_receive {:cancellation_requested, ^attempt_id}
    await_cancellations(recovered)

    confirmed =
      await(fn ->
        case Orchestrator.workstream_state(recovered.coordinator, id) do
          {:ok, %{status: :stopped, attempts: attempts, operations: operations} = run} ->
            operation = operations[attempt_id]
            attempt = Enum.find(attempts, &(&1.id == attempt_id))

            if attempt.status == :canceled and operation.status == :canceled and not Map.has_key?(operation, :reconciliation), do: run

          _ ->
            nil
        end
      end)

    assert confirmed.cancellation.termination == :terminated
    assert confirmed.operations[attempt_id].cancellation.termination == :terminated
    assert_receive {:replacement_started, "second-task"}
    assert %{status: :executing} = run_state(recovered, queued_id, :executing)

    assert :ok = Orchestrator.reconcile_workstreams(recovered.coordinator)
    assert {:ok, ^confirmed} = Orchestrator.workstream_state(recovered.coordinator, id)
    refute_receive {:cancellation_requested, ^attempt_id}

    restart(recovered)
    assert {:ok, ^confirmed} = Orchestrator.workstream_state(recovered.coordinator, id)
    refute_receive {:cancellation_requested, ^attempt_id}
    refute_receive {:replacement_started, "second-task"}
  end

  test "termination confirmation safely fills nil cancellation metadata", _c do
    attempt_id = "run/implement/1"

    run = %{
      status: :stopped,
      current_attempt_id: attempt_id,
      cancellation: nil,
      attempts: [%{id: attempt_id, status: :executing, result: nil}],
      operations: %{
        attempt_id => %{id: attempt_id, status: :executing, cancellation: nil, reconciliation: ":unknown"}
      }
    }

    confirmed = WorkstreamRun.confirm_termination(run)

    assert confirmed.cancellation == %{termination: :terminated}
    assert [%{status: :canceled, result: nil}] = confirmed.attempts
    assert confirmed.operations[attempt_id].status == :canceled
    assert confirmed.operations[attempt_id].cancellation == %{termination: :terminated}
    refute Map.has_key?(confirmed.operations[attempt_id], :reconciliation)
    assert WorkstreamRun.confirm_termination(confirmed) == confirmed
  end

  test "termination confirmation preserves a completed operation and attempt", _c do
    attempt_id = "run/implement/1"

    run = %{
      status: :stopped,
      current_attempt_id: attempt_id,
      cancellation: %{termination: :terminated},
      attempts: [%{id: attempt_id, status: :completed, result: %{candidate: "sha"}, gate: :passed}],
      operations: %{
        attempt_id => %{id: attempt_id, status: :completed, cancellation: %{termination: :terminated}, reconciliation: "historical"}
      }
    }

    assert WorkstreamRun.confirm_termination(run) == run
  end

  test "lost action receipt reconciles known side effect without replay", c do
    parent = self()

    executor = fn stage, definition, run, opts ->
      if stage.type == :check do
        File.write!(Path.join(run.workspace, "effects"), "once")
        send(parent, {:action_applied, self()})

        receive do
          :never -> {:error, :not_expected}
        end
      else
        normal_executor(stage, definition, run, opts)
      end
    end

    reconciler = fn _run, operation ->
      if operation.type == :check, do: {:completed, {:ok, %{exit_status: 0, timed_out: false, output: "observed existing effect"}}}, else: :unknown
    end

    runtime = runtime(c, executor, workstream_reconciler: reconciler)
    id = queue(runtime, c)
    assert_receive {:action_applied, worker}
    Process.exit(worker, :kill)
    pending = run_state(runtime, id, :reconciling)
    operation_id = pending.current_attempt_id
    restart(runtime)
    recovered = run_state(runtime, id, :waiting_for_answer)
    assert recovered.operations[operation_id].status == :completed
    assert File.read!(Path.join(c.workspace, "effects")) == "once"
    refute_receive {:action_applied, _}
    restart(runtime)
    assert length(run_state(runtime, id, :waiting_for_answer).attempts) == 3
  end

  test "repair and transport counters persist and a failing check stays blocked", c do
    parent = self()

    executor = fn stage, _definition, run, _opts ->
      if stage.type == :agent do
        send(parent, {:attempt, run.repairs, self()})

        receive do
          :finish -> {:ok, %{session_id: "controlled"}}
        end
      else
        {:ok, %{exit_status: 1, timed_out: false, output: "required assertion failed"}}
      end
    end

    runtime = runtime(c, executor, workstream_reconciler: fn _, _ -> :terminated end)
    id = queue(runtime, c)
    assert_receive {:attempt, repairs, first}
    assert repairs == %{}
    Process.exit(first, :kill)
    run_state(runtime, id, :reconciling)
    restart(runtime)
    assert_receive {:attempt, _, replacement}
    send(replacement, :finish)
    assert_receive {:attempt, %{"validate" => 1}, repaired}
    executing = run_state(runtime, id, :executing)
    assert executing.transport_retries == 1
    restart(runtime)
    assert run_state(runtime, id, :executing).repairs == %{"validate" => 1}
    send(repaired, :finish)
    blocked = run_state(runtime, id, :blocked)
    assert blocked.transport_retries == 1
    assert blocked.repairs == %{"validate" => 1}
    assert List.last(blocked.attempts).gate == :failed
  end

  test "a committed result is acknowledged after restart even when the previous ack was lost", c do
    runtime = runtime(c, &normal_executor/4)
    id = queue(runtime, c)
    run_state(runtime, id, :waiting_for_answer)
    state = :sys.get_state(runtime.coordinator)
    run = state.workstreams.runs[id]
    completed = Enum.find(run.attempts, &(&1.type == :check))
    parent = self()

    worker =
      Task.Supervisor.async_nolink(runtime.tasks, fn ->
        Process.put(:symphony_workstream, %{run_id: id, attempt_id: completed.id, operation_id: completed.id})
        send(parent, :receipt_ready)

        receive do
          {:workstream_ack, _} -> :acked
        end
      end)

    assert_receive :receipt_ready
    restart(runtime)
    assert Task.await(worker) == :acked
    assert length(run_state(runtime, id, :waiting_for_answer).attempts) == 3
  end

  test "a fresh BEAM process restores the actual run and executable evidence", c do
    executor = fn stage, definition, run, opts ->
      case normal_executor(stage, definition, run, opts) do
        {:ok, evidence} -> {:ok, Map.put(evidence, :custom_producer_metadata, %{opaque_custom_key: "retained"})}
        error -> error
      end
    end

    runtime = runtime(c, executor)
    id = queue(runtime, c)
    saved = run_state(runtime, id, :waiting_for_answer)
    store = :sys.get_state(runtime.coordinator).workstreams.store
    monitor = Process.monitor(store)
    assert :ok = stop_supervised(runtime.supervisor)
    assert_receive {:DOWN, ^monitor, :process, ^store, _}

    script = """
    for app <- [:yaml_elixir, :jason, :exqlite], do: {:ok, _} = Application.ensure_all_started(app)
    {:ok, runtime} = SymphonyElixir.AgentRuntimeSupervisor.start_link(name: FreshRuntime,
      orchestrator_name: FreshCoordinator, task_supervisor_name: FreshTasks,
      workstream_store_path: #{inspect(c.db)})
    {:ok, report} = SymphonyElixir.Orchestrator.workstream_state(FreshCoordinator, #{inspect(id)})
    IO.puts(Jason.encode!(report))
    Supervisor.stop(runtime)
    """

    paths = Enum.flat_map(:code.get_path(), fn path -> ["-pa", to_string(path)] end)
    {json, status} = System.cmd("elixir", paths ++ ["-e", script], stderr_to_stdout: true)
    assert status == 0, json
    assert Jason.decode!(json) == Jason.decode!(Jason.encode!(saved))
  end

  test "database inside a writable worker workspace rejects before dispatch", c do
    inside = %{c | db: Path.join(c.workspace, "state/runs.sqlite")}
    runtime = runtime(inside, &normal_executor/4)
    assert {:error, :database_overlaps_worker_workspace} = Orchestrator.queue_workstream(runtime.coordinator, "queue", "task", c.path, %{"task" => "controlled"}, c.options)
    assert Task.Supervisor.children(runtime.tasks) == []
  end

  test "changed service policy blocks a saved ready stage instead of silently changing its behavior", c do
    run = fixture_run(c, "legacy-policy")
    historical = put_in(run.policy.lifecycle_sha256, "previous-service-policy")
    legacy = persist_pre_dev234_run(c, historical, "previous-policy-fixture", historical.policy)
    runtime = runtime(c, &normal_executor/4, auto_advance: false)
    blocked = run_state(runtime, legacy.id, :policy_blocked)
    loaded = coordinator_run(runtime, legacy.id)
    assert_pre_dev234_defaults(loaded, legacy)
    assert loaded.policy == legacy.policy
    assert blocked.policy.lifecycle_sha256 == "previous-service-policy"
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert Task.Supervisor.children(runtime.tasks) == []
    assert blocked.attempts == []
  end

  test "manual mode avoids tracker startup and rejects malformed requests without dispatch", c do
    runtime = runtime(c, &normal_executor/4)
    assert {:error, :inputs_must_be_json_data} = Orchestrator.queue_workstream(runtime.coordinator, "bad", "task", c.path, %{"task" => self()}, c.options)
    assert {:error, :invalid_event_id} = Orchestrator.queue_workstream(runtime.coordinator, "", "task", c.path, %{"task" => "task"}, c.options)

    assert {:error, :invalid_workstream_execution_options} =
             Orchestrator.queue_workstream(runtime.coordinator, "opts", "task", c.path, %{"task" => "task"}, c.options ++ [agent_executor: fn -> :ok end])

    assert Task.Supervisor.children(runtime.tasks) == []
    id = queue(runtime, c)
    run_state(runtime, id, :waiting_for_answer)
    assert {:error, :workspace_already_owned} = Orchestrator.queue_workstream(runtime.coordinator, "other", "other", c.path, %{"task" => "task"}, c.options)
    assert {:error, :unknown_workstream_run} = Orchestrator.workstream_state(runtime.coordinator, "unknown")
    store = :sys.get_state(runtime.coordinator).workstreams.store
    assert {:ok, [stored]} = WorkstreamStore.load(store)
    assert stored.id == id
    assert stored.definition.definitions[Path.join(c.root, "SKILL.md")].text == "Pinned skill"
  end
end
