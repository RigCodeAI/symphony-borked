defmodule SymphonyElixir.DurableChildHandoffTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{AgentRuntimeSupervisor, Orchestrator, WorkstreamRun, WorkstreamRunner, WorkstreamStore}

  @fixture_dir Path.expand("../fixtures/dev243_child", __DIR__)
  @fixture_files [
    "coverage-factory.yaml",
    "software-change.yaml",
    "researcher.yaml",
    "implementer.yaml",
    "researcher.md",
    "implementer.md",
    "skills/research/SKILL.md",
    "skills/implementation/SKILL.md"
  ]

  setup do
    root = Path.join(System.tmp_dir!(), "durable-child-handoff-#{System.unique_integer([:positive])}")
    workspace_root = Path.join(root, "workspaces")
    parent_workspace = Path.join(workspace_root, "parent")
    child_workspace = Path.join(workspace_root, "child")

    Enum.each(@fixture_files, fn relative_path ->
      source = Path.join(@fixture_dir, relative_path)
      destination = Path.join(root, relative_path)
      File.mkdir_p!(Path.dirname(destination))
      File.cp!(source, destination)
    end)

    init_workspace!(parent_workspace, "cycle/dev243-parent")
    init_workspace!(child_workspace, "cycle/dev243-child")

    on_exit(fn -> File.rm_rf!(root) end)

    %{
      root: root,
      workspace_root: workspace_root,
      parent_workspace: parent_workspace,
      child_workspace: child_workspace,
      parent_path: Path.join(root, "coverage-factory.yaml"),
      child_path: Path.join(root, "software-change.yaml"),
      implementer_path: Path.join(root, "implementer.md"),
      implementer_skill_path: Path.join(root, "skills/implementation/SKILL.md"),
      db: Path.join(root, "state/runs.sqlite3"),
      options: [
        workspace: parent_workspace,
        child_workspace: child_workspace,
        workspace_root: workspace_root,
        codex_command: "/fixture/no-codex-command"
      ]
    }
  end

  test "approval and restart create one linked child that cannot complete the parent stage", context do
    executor = stage_executor(self())
    runtime = runtime(context, executor)
    parent_id = queue_parent(runtime, context)

    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert_receive {:child_fixture_stage, ^parent_id, "research", :agent}
    assert %{status: :ready, stage_id: "qualify"} = await_run(runtime, parent_id, :ready, "qualify")

    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert_receive {:child_fixture_stage, ^parent_id, "qualify", :check}
    assert %{status: :ready, stage_id: "approval-before"} = await_run(runtime, parent_id, :ready, "approval-before")

    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    waiting = await_run(runtime, parent_id, :waiting_for_answer, "approval-before")
    assert waiting.status == :waiting_for_answer
    assert waiting.stage_id == "approval-before"
    assert waiting.pending_wait.kind == :approval
    assert {:ok, [^waiting]} = WorkstreamStore.load(store(runtime))
    refute child_run?(runtime, parent_id)

    child_stage = waiting.definition.stages["implement-gap"]
    pinned_instruction = hd(child_stage.child_definition.agents["implementer"].instructions).text
    pinned_skill = hd(child_stage.child_definition.agents["implementer"].skills).text

    # The child must use the definitions pinned with the parent even if their source files change.
    File.write!(context.child_path, "not valid YAML after the parent was loaded\n")
    File.write!(context.implementer_path, "Changed implementation instructions.\n")
    File.write!(context.implementer_skill_path, "Changed implementation skill.\n")

    approved = persist_fixture_owner_approval(context, runtime, parent_id, waiting)
    assert approved.status == :ready
    assert approved.stage_id == "implement-gap"

    recovered = runtime(context, executor)
    assert coordinator_run(recovered, parent_id) == approved
    assert {:ok, [^approved]} = WorkstreamStore.load(store(recovered))

    assert :ok = Orchestrator.step_workstreams(recovered.coordinator)
    parent_waiting = await_run(recovered, parent_id, :waiting_for_child, "implement-gap")
    assert parent_waiting.status == :waiting_for_child
    assert parent_waiting.stage_id == "implement-gap"
    assert parent_waiting.child_handoff.parent_stage_id == "implement-gap"
    assert length(parent_waiting.attempts) == 4
    refute Map.has_key?(parent_waiting.outputs, "child-result")
    refute_receive {:child_fixture_stage, _, "implement", _}

    child_id = parent_waiting.child_handoff.child_run_id
    child = coordinator_run(recovered, child_id)
    assert child.status == :ready
    assert child.definition.name == "software-change"
    assert child.workspace == context.child_workspace
    assert child.parent_run_id == parent_id
    assert child.parent_handoff.parent_run_id == parent_id
    assert child.definition.input_values["task"] == parent_waiting.outputs["qualified-gap"]
    assert hd(child.definition.agents["implementer"].instructions).text == pinned_instruction
    assert hd(child.definition.agents["implementer"].skills).text == pinned_skill
    assert {:ok, runs} = WorkstreamStore.load(store(recovered))
    assert length(runs) == 2

    handoff = parent_waiting.child_handoff

    assert {:ok, %{run_id: ^parent_id, event: %{kind: :child_handoff_started, intent: ^handoff}}} =
             WorkstreamStore.event(store(recovered), "child-handoff/#{handoff.handoff_id}/parent")

    assert :ok = Orchestrator.step_workstreams(recovered.coordinator)
    assert_receive {:child_fixture_stage, ^child_id, "implement", :agent}
    child_check = await_run(recovered, child_id, :ready, "inspect")
    assert child_check.status == :ready
    assert child_check.outputs["candidate"]["candidate"] == "fixture candidate"
    assert File.read!(Path.join(context.child_workspace, "candidate.txt")) == "fixture candidate\n"

    assert :ok = Orchestrator.step_workstreams(recovered.coordinator)
    assert_receive {:child_fixture_stage, ^child_id, "inspect", :check}
    assert %{status: :complete, stage_id: :complete} = await_run(recovered, child_id, :complete, :complete)

    restart(recovered)
    restart(recovered)

    parent_after_restarts = coordinator_run(recovered, parent_id)
    child_after_restarts = coordinator_run(recovered, child_id)
    assert parent_after_restarts.status == :waiting_for_child
    assert parent_after_restarts.stage_id == "implement-gap"
    assert parent_after_restarts.child_observation.status == :complete

    assert Enum.map(parent_after_restarts.attempts, & &1.stage) ==
             ["research", "qualify", "approval-before", "implement-gap"]

    refute Map.has_key?(parent_after_restarts.outputs, "child-result")
    assert child_after_restarts.status == :complete
    assert {:ok, runs} = WorkstreamStore.load(store(recovered))
    assert length(runs) == 2

    assert {:ok, %{run_id: ^child_id, event: %{kind: :child_handoff_linked, intent: ^handoff}}} =
             WorkstreamStore.event(store(recovered), "child-handoff/#{handoff.handoff_id}/child")

    refute_receive {:child_fixture_stage, _, _, _}
  end

  test "restart stops an owned child without redispatching it and retains its completed output", context do
    executor = stage_executor(self())
    runtime = runtime(context, executor)
    parent_id = queue_parent(runtime, context)
    wait_at_parent_approval(runtime, parent_id)
    waiting = coordinator_run(runtime, parent_id)

    approved = persist_fixture_owner_approval(context, runtime, parent_id, waiting)
    recovered = runtime(context, executor)
    assert coordinator_run(recovered, parent_id) == approved
    assert :ok = Orchestrator.step_workstreams(recovered.coordinator)

    parent_waiting = await_run(recovered, parent_id, :waiting_for_child, "implement-gap")
    child_id = parent_waiting.child_handoff.child_run_id
    assert :ok = Orchestrator.step_workstreams(recovered.coordinator)
    assert_receive {:child_fixture_stage, ^child_id, "implement", :agent}

    child_before_stop = await_run(recovered, child_id, :ready, "inspect")
    assert child_before_stop.parent_run_id == parent_id
    assert child_before_stop.status == :ready
    assert child_before_stop.outputs["candidate"]["candidate"] == "fixture candidate"
    candidate_artifact = child_before_stop.artifacts["candidate"]
    assert is_map(candidate_artifact)

    stop_runtime(recovered)
    assert {:ok, store} = WorkstreamStore.start_link(path: context.db, owner: self())
    assert {:ok, persisted_parent} = WorkstreamStore.fetch(store, parent_id)

    stopped_parent =
      Map.merge(persisted_parent, %{
        status: :stopped,
        phase: :stopped,
        cancellation: %{termination: :terminated}
      })

    assert :ok =
             WorkstreamStore.commit(
               store,
               stopped_parent,
               "test-fixture-owner-stop/#{parent_id}",
               %{kind: :test_fixture_owner_stop}
             )

    assert :ok = GenServer.stop(store)

    restarted = runtime(context, executor)
    assert %{status: :stopped, child_handoff: handoff} = await_run(restarted, parent_id, :stopped, "implement-gap")
    assert handoff.child_run_id == child_id

    stopped_child = await_run(restarted, child_id, :stopped, "inspect")
    assert stopped_child.parent_run_id == parent_id
    assert stopped_child.status == :stopped
    assert stopped_child.outputs["candidate"] == child_before_stop.outputs["candidate"]
    assert stopped_child.artifacts["candidate"] == candidate_artifact
    assert stopped_child.stage_id == "inspect"
    assert File.read!(Path.join(context.child_workspace, "candidate.txt")) == "fixture candidate\n"
    refute_receive {:child_fixture_stage, ^child_id, "inspect", :check}
    refute_receive {:child_fixture_stage, ^child_id, "implement", :agent}
  end

  test "capture sentinel holds a ready child stage without mutating its handoff state", context do
    executor = stage_executor(self())
    runtime = runtime(context, executor)
    parent_id = queue_parent(runtime, context)
    wait_at_parent_approval(runtime, parent_id)
    waiting = coordinator_run(runtime, parent_id)
    approved = persist_fixture_owner_approval(context, runtime, parent_id, waiting)

    recovered = runtime(context, executor)
    assert coordinator_run(recovered, parent_id) == approved
    before = coordinator_run(recovered, parent_id)

    :sys.replace_state(Process.whereis(recovered.coordinator), fn state ->
      Map.update!(state, :workstreams, &Map.put(&1, :capture, %{test_only: true}))
    end)

    assert {:error, :capture_in_progress} = Orchestrator.step_workstreams(recovered.coordinator)
    assert coordinator_run(recovered, parent_id) == before
    assert {:ok, [^before]} = WorkstreamStore.load(store(recovered))
    refute child_run?(recovered, parent_id)
    refute_receive {:child_fixture_stage, _, "implement", _}

    :sys.replace_state(Process.whereis(recovered.coordinator), fn state ->
      Map.update!(state, :workstreams, &Map.put(&1, :capture, nil))
    end)
  end

  test "a persisted child with mismatched lineage is not reconciled or dispatched after restart", context do
    executor = stage_executor(self())
    runtime = runtime(context, executor)
    parent_id = queue_parent(runtime, context)
    wait_at_parent_approval(runtime, parent_id)
    waiting = coordinator_run(runtime, parent_id)
    approved = persist_fixture_owner_approval(context, runtime, parent_id, waiting)

    recovered = runtime(context, executor)
    assert coordinator_run(recovered, parent_id) == approved
    assert :ok = Orchestrator.step_workstreams(recovered.coordinator)
    parent_waiting = await_run(recovered, parent_id, :waiting_for_child, "implement-gap")
    child_id = parent_waiting.child_handoff.child_run_id

    stop_runtime(recovered)
    assert {:ok, owner_store} = WorkstreamStore.start_link(path: context.db, owner: self())
    assert {:ok, child} = WorkstreamStore.fetch(owner_store, child_id)
    assert child.parent_run_id == parent_id

    active_child = WorkstreamRun.start_stage(child)
    mismatched_handoff = Map.update!(active_child.parent_handoff, :fingerprint, fn _ -> String.duplicate("f", 64) end)
    mismatched_lineage = Map.put(active_child, :parent_handoff, mismatched_handoff)

    refute mismatched_lineage.parent_handoff.fingerprint == parent_waiting.child_handoff.fingerprint

    assert :ok =
             WorkstreamStore.commit(
               owner_store,
               mismatched_lineage,
               "test-fixture-mismatched-lineage/#{child_id}",
               %{kind: :test_fixture_mismatched_lineage}
             )

    assert :ok = GenServer.stop(owner_store)

    parent = self()

    reconciler = fn run, operation ->
      send(parent, {:unexpected_child_reconciliation, run.id, operation.id})
      :not_applied
    end

    restarted = runtime(context, executor, workstream_reconciler: reconciler)
    assert %{status: :waiting_for_child} = await_run(restarted, parent_id, :waiting_for_child, "implement-gap")
    assert coordinator_run(restarted, child_id) == mismatched_lineage
    assert :ok = Orchestrator.step_workstreams(restarted.coordinator)
    assert coordinator_run(restarted, child_id) == mismatched_lineage
    refute_receive {:unexpected_child_reconciliation, ^child_id, _}
    refute_receive {:child_fixture_stage, ^child_id, _, _}
  end

  test "proof job ownership blocks capture and stale results cannot advance the waiting parent", context do
    executor = stage_executor(self())
    runtime = runtime(context, executor)
    parent_id = queue_parent(runtime, context)
    wait_at_parent_approval(runtime, parent_id)
    waiting = coordinator_run(runtime, parent_id)
    persist_fixture_owner_approval(context, runtime, parent_id, waiting)
    recovered = runtime(context, executor)
    assert :ok = Orchestrator.step_workstreams(recovered.coordinator)
    parent = await_run(recovered, parent_id, :waiting_for_child, "implement-gap")
    child_id = parent.child_handoff.child_run_id
    child = coordinator_run(recovered, child_id)
    ref = make_ref()
    key = parent.current_attempt_id

    # This owner-injected record checks callback guards, not real merge-proof production.
    job = %{ref: ref, pid: self(), parent_id: parent_id, child_id: child_id, parent_fingerprint: "stale-parent-snapshot", child_fingerprint: SymphonyElixir.WorkstreamCleanup.fingerprint(child)}

    :sys.replace_state(Process.whereis(recovered.coordinator), fn state ->
      put_in(state.workstreams.child_proofs[key], job)
    end)

    assert {:error, :capture_child_proof_active} = Orchestrator.capture_backup(recovered.coordinator, "held-proof-job")
    send(Process.whereis(recovered.coordinator), {make_ref(), {:child_merge_proof, key, {:ok, %{merged: true}}}})
    assert is_map(GenServer.call(recovered.coordinator, :snapshot))
    assert coordinator_run(recovered, parent_id) == parent

    send(Process.whereis(recovered.coordinator), {ref, {:child_merge_proof, key, {:ok, %{merged: true}}}})
    assert is_map(GenServer.call(recovered.coordinator, :snapshot))
    assert coordinator_run(recovered, parent_id) == parent
    state = :sys.get_state(recovered.coordinator)
    assert %{retry_at: _} = state.workstreams.child_proofs[key]
    timer = state.workstreams.child_proof_timer
    assert {_timer_ref, token} = timer
    assert is_reference(token)
    send(Process.whereis(recovered.coordinator), {:child_proofs_tick, make_ref()})
    assert is_map(GenServer.call(recovered.coordinator, :snapshot))
    assert :sys.get_state(recovered.coordinator).workstreams.child_proof_timer == timer
  end

  test "a queued parent reserves its prepared child workspace before approval", context do
    runtime = runtime(context, stage_executor(self()))
    parent_id = queue_parent(runtime, context)
    options = context.options |> Keyword.put(:workspace, context.child_workspace) |> Keyword.put(:child_workspace, context.parent_workspace)

    assert {:error, :workspace_already_owned} =
             Orchestrator.queue_workstream(runtime.coordinator, "conflicting-parent", "other-parent", context.parent_path, %{"request" => "isolated conflict"}, options)

    assert {:ok, [run]} = WorkstreamStore.load(store(runtime))
    assert run.id == parent_id
    refute_receive {:child_fixture_stage, _, _, _}
  end

  defp runtime(context, executor) do
    runtime(context, executor, [])
  end

  defp runtime(context, executor, extra) do
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "Runtime#{suffix}")
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    opts =
      [
        name: supervisor,
        orchestrator_name: coordinator,
        task_supervisor_name: tasks,
        workstream_store_path: context.db,
        stage_executor: executor,
        auto_advance: false
      ] ++ extra

    pid = start_supervised!({AgentRuntimeSupervisor, opts}, id: supervisor)
    %{pid: pid, supervisor: supervisor, coordinator: coordinator, tasks: tasks}
  end

  defp queue_parent(runtime, context) do
    assert {:ok, run_id} =
             Orchestrator.queue_workstream(
               runtime.coordinator,
               "queue-fixture",
               "fixture-parent-task",
               context.parent_path,
               %{"request" => "isolated contract fixture"},
               context.options
             )

    run_id
  end

  defp wait_at_parent_approval(runtime, parent_id) do
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert_receive {:child_fixture_stage, ^parent_id, "research", :agent}
    assert %{status: :ready, stage_id: "qualify"} = await_run(runtime, parent_id, :ready, "qualify")
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert_receive {:child_fixture_stage, ^parent_id, "qualify", :check}
    assert %{status: :ready, stage_id: "approval-before"} = await_run(runtime, parent_id, :ready, "approval-before")
    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)

    assert %{status: :waiting_for_answer, stage_id: "approval-before", pending_wait: %{kind: :approval}} =
             await_run(runtime, parent_id, :waiting_for_answer, "approval-before")
  end

  defp persist_fixture_owner_approval(context, runtime, parent_id, waiting) do
    wait = waiting.pending_wait

    assert {:error, :explicit_approval_required} =
             Orchestrator.answer_workstream(
               runtime.coordinator,
               "manual-api-cannot-approve/#{wait.id}",
               parent_id,
               wait.id,
               %{"approval" => true, "artifact_revision" => wait.approval_digest}
             )

    stop_runtime(runtime)

    assert {:ok, owner_store} = WorkstreamStore.start_link(path: context.db, owner: self())
    assert {:ok, persisted} = WorkstreamStore.fetch(owner_store, parent_id)
    activity_id = "test-fixture-owner/#{wait.id}"
    reply = "approve #{wait.id} #{wait.approval_digest}"
    assert {:ok, approved} = WorkstreamRun.linear_reply(persisted, reply, activity_id)
    assert approved.status == :ready
    assert approved.stage_id == "implement-gap"

    assert :ok =
             WorkstreamStore.commit(
               owner_store,
               approved,
               "test-fixture-owner-approval/#{wait.id}",
               %{kind: :test_fixture_owner_approval, wait_id: wait.id}
             )

    assert :ok = GenServer.stop(owner_store)
    approved
  end

  defp stage_executor(owner) do
    fn stage, definition, run, opts ->
      send(owner, {:child_fixture_stage, run.id, stage.id, stage.type})

      if stage.type == :agent do
        File.write!(Path.join(run.workspace, "candidate.txt"), "fixture candidate\n")

        {:ok,
         %{
           session_id: "fixture-session-#{run.id}",
           thread_id: "fixture-thread-#{run.id}",
           candidate: "fixture candidate"
         }}
      else
        WorkstreamRunner.execute_stage(stage, definition, run, opts)
      end
    end
  end

  defp stop_runtime(runtime) do
    store_pid = store(runtime)
    assert :ok = stop_supervised(runtime.supervisor)
    assert await(fn -> if not Process.alive?(store_pid), do: true end)
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

  defp await_run(runtime, run_id, status, stage_id) do
    await(fn ->
      case lookup_run(runtime, run_id) do
        %{status: ^status, stage_id: ^stage_id} = run -> run
        _ -> nil
      end
    end)
  end

  defp coordinator_run(runtime, run_id) do
    await(fn -> lookup_run(runtime, run_id) end)
  end

  defp lookup_run(runtime, run_id) do
    case Process.whereis(runtime.coordinator) do
      pid when is_pid(pid) ->
        try do
          case Map.get(:sys.get_state(pid).workstreams.runs, run_id) do
            run when is_map(run) -> run
            _ -> nil
          end
        catch
          :exit, _ -> nil
        end

      _ ->
        nil
    end
  end

  defp child_run?(runtime, parent_id) do
    runs = :sys.get_state(runtime.coordinator).workstreams.runs

    case Map.get(runs, parent_id) do
      %{child_handoff: %{child_run_id: child_id}} -> Map.has_key?(runs, child_id)
      _ -> false
    end
  end

  defp store(runtime), do: :sys.get_state(runtime.coordinator).workstreams.store

  defp await(fun, tries \\ 200)
  defp await(_fun, 0), do: flunk("durable child handoff state did not converge")

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

  defp init_workspace!(workspace, branch) do
    File.mkdir_p!(Path.dirname(workspace))
    {_output, 0} = System.cmd("git", ["init", "--quiet", "-b", branch, workspace])
  end
end
