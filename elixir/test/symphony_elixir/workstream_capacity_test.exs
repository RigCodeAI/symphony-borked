defmodule SymphonyElixir.WorkstreamCapacityTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{AgentRuntimeSupervisor, Orchestrator, WorkstreamRunner}

  @fixtures Path.expand("../fixtures/workstream_capacity", __DIR__)
  @timeout 5_000

  # These are local admission tests, not worker resource or review/child qualification.
  setup do
    root = Path.join(System.tmp_dir!(), "workstream-capacity-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "workspaces"))
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root, workspace_root: Path.join(root, "workspaces"), store: Path.join(root, "state/runs.sqlite")}
  end

  test "two definitions share two slots and a human reply queues behind admitted work", c do
    runtime = runtime(c)
    waiting = queue(runtime, c, "with-wait", "waiting")
    busy = queue(runtime, c, "without-wait", "busy")
    step(runtime)
    {waiting_worker, _} = started(waiting, "prepare")
    {busy_worker, _} = started(busy, "execute")

    send(waiting_worker, :finish)
    wait = report(runtime, waiting, :waiting_for_answer)
    queued = queue(runtime, c, "without-wait", "queued")
    step(runtime)
    {queued_worker, _} = started(queued, "execute")

    # Only the human wait releases its slot; the other definition stays executing.
    assert report(runtime, busy, :executing).stage_id == "execute"

    assert Map.fetch!(wait.definitions, Path.join(@fixtures, "with-wait.yaml")) !=
             Map.fetch!(report(runtime, busy, :executing).definitions, Path.join(@fixtures, "without-wait.yaml"))

    answer = %{"answer" => "continue"}
    assert :ok = Orchestrator.answer_workstream(runtime.coordinator, "reply", waiting, wait.pending_wait.id, answer)
    step(runtime)
    resumed = report(runtime, waiting, :ready)
    assert resumed.stage_id == "finish"
    assert length(resumed.attempts) == 3
    refute_receive {:capacity_started, ^waiting, "finish", _, _}, 100

    send(busy_worker, :finish)
    {resumed_worker, _} = started(waiting, "finish")
    assert report(runtime, queued, :executing).stage_id == "execute"

    # Duplicate replies cannot create a third continuation, even after the wait ended.
    assert :ok = Orchestrator.answer_workstream(runtime.coordinator, "reply", waiting, wait.pending_wait.id, answer)
    step(runtime)
    refute_receive {:capacity_started, _, _, _, _}, 100
    send(resumed_worker, :finish)
    send(queued_worker, :finish)
    report(runtime, waiting, :complete)
    report(runtime, queued, :complete)
    report(runtime, busy, :complete)
    assert effects(c, "waiting") == ["prepare", "finish"]
    assert effects(c, "busy") == ["execute"]
    assert effects(c, "queued") == ["execute"]
  end

  test "a saturated pool survives restart and duplicate events or receipts cannot free a slot", c do
    runtime = runtime(c)
    waiting = queue(runtime, c, "with-wait", "waiting")
    busy = queue(runtime, c, "without-wait", "busy")
    step(runtime)
    {waiting_worker, waiting_attempt} = started(waiting, "prepare")
    {busy_worker, busy_attempt} = started(busy, "execute")
    queued = queue(runtime, c, "without-wait", "queued")
    before = report(runtime, waiting, :executing)

    restart(runtime)
    assert queue(runtime, c, "with-wait", "waiting") == waiting
    assert queue(runtime, c, "with-wait", "waiting", "another-delivery") == waiting
    step(runtime)
    assert_unstarted(runtime, queued)
    assert report(runtime, waiting, :executing).current_attempt_id == waiting_attempt
    assert report(runtime, busy, :executing).current_attempt_id == busy_attempt
    assert report(runtime, waiting, :executing).definitions == before.definitions
    assert Enum.sort(Task.Supervisor.children(runtime.tasks)) == Enum.sort([waiting_worker, busy_worker])
    refute_receive {:capacity_started, _, _, _, _}, 100

    send(busy_worker, :finish)
    send(waiting_worker, :finish)
    completed = report(runtime, busy, :complete)
    report(runtime, waiting, :waiting_for_answer)
    step(runtime)
    {queued_worker, queued_attempt} = started(queued, "execute")
    next = queue(runtime, c, "without-wait", "next")
    {next_worker, _} = started(next, "execute")
    pending = queue(runtime, c, "without-wait", "pending")

    # Replay a completed receipt, then attempt to associate its stale identity with a live run.
    coordinator = Process.whereis(runtime.coordinator)
    send(coordinator, {:workstream_result, busy, busy_attempt, busy_worker, {:ok, %{session_id: "replayed"}}})
    send(coordinator, {:workstream_result, queued, busy_attempt, busy_worker, {:ok, %{session_id: "misrouted"}}})
    step(runtime)
    assert report(runtime, busy, :complete) == completed
    assert report(runtime, queued, :executing).current_attempt_id == queued_attempt
    assert_unstarted(runtime, pending)
    assert effects(c, "busy") == ["execute"]
    refute_receive {:capacity_started, _, _, _, _}, 100

    send(queued_worker, :finish)
    send(next_worker, :finish)
    report(runtime, queued, :complete)
  end

  test "one uncertain attempt reserves one slot across restart while unrelated work advances", c do
    runtime = runtime(c)
    uncertain = queue(runtime, c, "with-wait", "uncertain")
    busy = queue(runtime, c, "without-wait", "busy")
    step(runtime)
    {uncertain_worker, uncertain_attempt} = started(uncertain, "prepare")
    {busy_worker, busy_attempt} = started(busy, "execute")
    queued_a = queue(runtime, c, "with-wait", "queued-a")
    queued_b = queue(runtime, c, "without-wait", "queued-b")

    # A dead local task is not proof its external operation terminated.
    Process.exit(uncertain_worker, :kill)
    uncertain_before = report(runtime, uncertain, :reconciling)
    restart(runtime)
    assert :ok = Orchestrator.reconcile_workstreams(runtime.coordinator)
    step(runtime)
    assert_unstarted(runtime, queued_a)
    assert_unstarted(runtime, queued_b)
    assert report(runtime, busy, :executing).current_attempt_id == busy_attempt

    send(busy_worker, :finish)
    assert_receive {:capacity_started, admitted, stage, admitted_worker, admitted_attempt}, @timeout
    assert admitted in [queued_a, queued_b]
    pending = if admitted == queued_a, do: queued_b, else: queued_a
    assert stage == if(admitted == queued_a, do: "prepare", else: "execute")
    assert_unstarted(runtime, pending)
    assert effects(c, "busy") == ["execute"]

    restart(runtime)
    assert :ok = Orchestrator.reconcile_workstreams(runtime.coordinator)
    step(runtime)
    unknown = report(runtime, uncertain, :reconciling)
    assert unknown.current_attempt_id == uncertain_attempt
    assert length(unknown.attempts) == 1
    assert unknown.definitions == uncertain_before.definitions
    assert unknown.operations[uncertain_attempt].status == :executing
    assert report(runtime, admitted, :executing).current_attempt_id == admitted_attempt
    assert_unstarted(runtime, pending)
    assert Task.Supervisor.children(runtime.tasks) == [admitted_worker]
    refute File.exists?(Path.join(c.workspace_root, "uncertain/effects"))
    refute_receive {:capacity_started, _, _, _, _}, 100
  end

  defp runtime(c) do
    parent = self()
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "Runtime#{suffix}")
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    executor = fn stage, definition, run, opts ->
      if stage.type == :agent do
        send(parent, {:capacity_started, run.id, stage.id, self(), run.current_attempt_id})

        receive do
          :finish ->
            File.write!(Path.join(run.workspace, "effects"), stage.id <> "\n", [:append])
            {:ok, %{session_id: run.current_attempt_id}}
        after
          10_000 -> {:error, :controlled_capacity_test_timeout}
        end
      else
        WorkstreamRunner.execute_stage(stage, definition, run, opts)
      end
    end

    start_supervised!(
      {AgentRuntimeSupervisor, name: supervisor, orchestrator_name: coordinator, task_supervisor_name: tasks, workstream_store_path: c.store, stage_executor: executor, max_concurrent_agents: 2},
      id: supervisor
    )

    %{coordinator: coordinator, tasks: tasks}
  end

  defp queue(runtime, c, definition, task, event \\ nil) do
    workspace = Path.join(c.workspace_root, task)

    unless File.dir?(workspace) do
      File.mkdir_p!(workspace)
      {_, 0} = System.cmd("git", ["init", "--quiet", workspace])
    end

    assert {:ok, id} =
             Orchestrator.queue_workstream(
               runtime.coordinator,
               event || "queue-" <> task,
               task,
               Path.join(@fixtures, definition <> ".yaml"),
               %{"task" => task},
               workspace: workspace,
               workspace_root: c.workspace_root
             )

    id
  end

  defp step(runtime), do: assert(:ok == Orchestrator.step_workstreams(runtime.coordinator))

  defp started(id, stage) do
    assert_receive {:capacity_started, ^id, ^stage, worker, attempt}, @timeout
    {worker, attempt}
  end

  defp report(runtime, id, status) do
    await(fn ->
      case Orchestrator.workstream_state(runtime.coordinator, id) do
        {:ok, %{status: ^status} = report} -> report
        {:ok, %{status: failed} = report} when failed in [:blocked, :policy_blocked] -> flunk("run failed instead of reaching #{status}: #{inspect(report)}")
        _ -> nil
      end
    end)
  end

  defp assert_unstarted(runtime, id) do
    assert %{attempts: [], current_attempt_id: nil} = report(runtime, id, :ready)
    refute_receive {:capacity_started, ^id, _, _, _}, 100
  end

  defp restart(runtime) do
    old = Process.whereis(runtime.coordinator)
    ref = Process.monitor(old)
    Process.exit(old, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old, :killed}, @timeout

    await(fn ->
      case Process.whereis(runtime.coordinator) do
        pid when is_pid(pid) and pid != old ->
          try do
            if is_map(Orchestrator.snapshot(runtime.coordinator, @timeout)), do: pid
          catch
            :exit, _ -> nil
          end

        _ ->
          nil
      end
    end)
  end

  defp await(fun, tries \\ 300)
  defp await(_fun, 0), do: flunk("capacity state did not converge")

  defp await(fun, tries) do
    case fun.() do
      nil ->
        Process.sleep(10)
        await(fun, tries - 1)

      value ->
        value
    end
  end

  defp effects(c, task) do
    c.workspace_root |> Path.join(task <> "/effects") |> File.read!() |> String.split("\n", trim: true)
  end
end
