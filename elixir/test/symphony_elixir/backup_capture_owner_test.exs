defmodule SymphonyElixir.BackupCaptureOwnerTest do
  use SymphonyElixir.TestSupport

  alias Exqlite.Sqlite3

  alias SymphonyElixir.{
    AgentRuntimeSupervisor,
    BackupCapture,
    Evidence,
    RecoveryInspection,
    WorkstreamChild,
    WorkstreamRun,
    WorkstreamRunner,
    WorkstreamStore
  }

  alias SymphonyElixirWeb.LinearWebhookController

  @child_fixture_dir Path.expand("../fixtures/dev243_child", __DIR__)
  @child_fixture_files [
    "coverage-factory.yaml",
    "software-change.yaml",
    "researcher.yaml",
    "implementer.yaml",
    "researcher.md",
    "implementer.md",
    "skills/research/SKILL.md",
    "skills/implementation/SKILL.md"
  ]

  setup test_context do
    root = Path.join(System.tmp_dir!(), "backup-capture-owner-#{System.unique_integer([:positive])}")
    workspace_root = Path.join(root, "workspaces")
    ready_workspace = Path.join(workspace_root, "rig-ready")
    waiting_workspace = Path.join(workspace_root, "rig-humanwait")
    capture_root = Path.join(root, "captures")
    evidence_root = Path.join(root, "private-evidence")
    File.mkdir_p!(ready_workspace)
    File.mkdir_p!(waiting_workspace)
    File.mkdir_p!(capture_root)
    File.chmod!(capture_root, 0o700)
    {_, 0} = System.cmd("git", ["init", "--quiet", ready_workspace])
    {_, 0} = System.cmd("git", ["init", "--quiet", waiting_workspace])
    coordinator = Module.concat(__MODULE__, "Coordinator#{System.unique_integer([:positive])}")

    File.write!(Path.join(root, "instructions.md"), "Use the captured test executor.")
    File.write!(Path.join(root, "SKILL.md"), "Keep this local fixture deterministic.")

    File.write!(Path.join(root, "agent.yaml"), """
    version: 1
    name: capture-agent
    model: gpt-6-luna
    reasoning_effort: medium
    daybreak: false
    approval_policy: never
    sandbox: workspace-write
    instructions: [instructions.md]
    skills: [SKILL.md]
    """)

    workstream_path = Path.join(root, "capture-workstream.yaml")

    File.write!(workstream_path, """
    version: 1
    name: capture-owner-fixture
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
        prompt: Prepare a candidate.
        next: validate
      - id: validate
        type: check
        inputs: [candidate]
        outputs: [validation]
        gate:
          command: ["true"]
          timeout_ms: 1000
          success: review
          failure: blocked
      - id: review
        type: human_wait
        inputs: [candidate, validation]
        outputs: [answer]
        prompt: Review the candidate.
        next: complete
    """)

    evidence_config = %{
      "root" => evidence_root,
      "base_url" => "https://evidence.example.com",
      "iap_audience" => "/projects/123456789/global/backendServices/123456789",
      "allowed_domains" => ["rig.ai"],
      "allowed_emails" => []
    }

    linear_config = %{
      "organization_id" => "capture-org",
      "team_id" => "capture-team",
      "app_user_id" => "capture-app",
      "oauth_client_id" => "capture-oauth",
      "webhook_secret_env" => "FACTORY_TEST_LINEAR_SECRET",
      "token_env" => "FACTORY_TEST_LINEAR_TOKEN",
      "store_path" => Path.join(root, "state/runs.sqlite"),
      "workspace_root" => workspace_root,
      "workstream_path" => workstream_path,
      "agent_id" => "default-cloud",
      "rig_label" => "factory:rig"
    }

    workflow_path = Workflow.workflow_file_path()
    original_workflow = File.read!(workflow_path)
    write_workflow_file!(workflow_path, workspace_root: workspace_root)
    workflow = File.read!(workflow_path)
    [front_matter, prompt] = String.split(workflow, "\n---\n", parts: 2)

    File.write!(
      workflow_path,
      front_matter <>
        "\nevidence: " <>
        Jason.encode!(evidence_config) <>
        "\nlinear_delegation: " <>
        Jason.encode!(linear_config) <>
        "\n---\n" <> prompt
    )

    assert :ok = WorkflowStore.force_reload()
    assert {:ok, ^evidence_config} = Config.evidence()
    assert {:ok, _} = Config.linear_delegation()
    assert Process.whereis(Evidence) == nil

    assert {:ok, evidence_pid} =
             Evidence.start_link(
               config: {:ok, evidence_config},
               receipt_uid: File.stat!(root).uid,
               capture_owner: coordinator
             )

    on_exit(fn ->
      if Process.alive?(evidence_pid), do: GenServer.stop(evidence_pid)
      File.write!(workflow_path, original_workflow)
      WorkflowStore.force_reload()
      File.rm_rf!(root)
    end)

    context = %{
      root: root,
      workspace_root: workspace_root,
      ready_workspace: ready_workspace,
      waiting_workspace: waiting_workspace,
      coordinator: coordinator,
      workstream_path: workstream_path,
      capture_root: capture_root,
      evidence_root: evidence_root,
      evidence_pid: evidence_pid,
      evidence_config: evidence_config,
      linear_config: linear_config,
      workflow_path: workflow_path,
      original_workflow: original_workflow,
      db: Path.join(root, "state/runs.sqlite"),
      options: [workspace: ready_workspace, workspace_root: workspace_root],
      ready_run: fixture_run(ready_workspace, workspace_root, workstream_path, "run-capture-ready", "task-ready")
    }

    waiting_run = human_wait_fixture(context)
    ready_run = %{context.ready_run | validation_repair_rounds: 2, review_repair_rounds: 1, transport_retries: 3}

    {ready_run, waiting_run} =
      cond do
        test_context[:stale_saved_policy] ->
          policy = Map.put(WorkstreamRun.policy(), :lifecycle_sha256, String.duplicate("0", 64))
          {Map.put(ready_run, :policy, policy), Map.put(waiting_run, :policy, policy)}

        test_context[:legacy_saved_policy] ->
          policy = Map.delete(WorkstreamRun.policy(), :child_sha256)
          {Map.put(ready_run, :policy, policy), Map.put(waiting_run, :policy, policy)}

        true ->
          {ready_run, waiting_run}
      end

    seed_store!(context.db, [ready_run, waiting_run])
    context = Map.merge(context, %{ready_run: ready_run, waiting_run: waiting_run})

    context
  end

  test "captures ready and human-wait runs through both live owners and preserves stored BLOB data", c do
    runtime = start_runtime(c)
    coordinator = runtime.coordinator
    store = :sys.get_state(coordinator).workstreams.store
    {initial_timer_ref, initial_timer_token} = :sys.get_state(coordinator).workstreams.validation_check_timer
    assert is_integer(Process.read_timer(initial_timer_ref))

    assert {:error, {:database_ownership_conflict, _, _}} = WorkstreamStore.start_link(path: c.db, owner: self())
    assert {:ok, %{status: :ready} = ready} = Orchestrator.workstream_state(coordinator, "run-capture-ready")
    assert {:ok, %{status: :waiting_for_answer} = waiting} = Orchestrator.workstream_state(coordinator, "run-capture-humanwait")
    assert waiting.pending_wait.artifact_ids == waiting.artifacts
    assert ready.validation_repair_rounds == 2
    assert ready.review_repair_rounds == 1
    assert ready.transport_retries == 3

    assert :ok = Evidence.artifact(waiting, "retained fixture", "captured evidence bytes", Evidence)
    assert {:ok, %{id: "quiet-capture", status: :capturing, restore_ready: false}} = Orchestrator.capture_backup(coordinator, "quiet-capture")
    {:ok, manifest} = await_capture(coordinator, "quiet-capture")

    {released_timer_ref, released_timer_token} = :sys.get_state(coordinator).workstreams.validation_check_timer
    refute released_timer_ref == initial_timer_ref
    refute released_timer_token == initial_timer_token
    assert Process.read_timer(initial_timer_ref) == false
    assert is_integer(Process.read_timer(released_timer_ref))

    for _ <- 1..10 do
      send(coordinator, {:validation_checks_tick, initial_timer_token})
      send(coordinator, :reconcile_validation_checks)
    end

    assert {:ok, ^manifest} = Orchestrator.backup_capture_status(coordinator, "quiet-capture")
    assert :sys.get_state(coordinator).workstreams.validation_check_timer == {released_timer_ref, released_timer_token}

    send(coordinator, {:validation_checks_tick, released_timer_token})
    assert {:ok, ^manifest} = Orchestrator.backup_capture_status(coordinator, "quiet-capture")
    {next_timer_ref, next_timer_token} = :sys.get_state(coordinator).workstreams.validation_check_timer
    refute next_timer_ref == released_timer_ref
    refute next_timer_token == released_timer_token
    assert Process.read_timer(released_timer_ref) == false
    assert is_integer(Process.read_timer(next_timer_ref))

    for _ <- 1..10 do
      send(coordinator, {:validation_checks_tick, released_timer_token})
      send(coordinator, :reconcile_validation_checks)
    end

    assert {:ok, ^manifest} = Orchestrator.backup_capture_status(coordinator, "quiet-capture")
    assert :sys.get_state(coordinator).workstreams.validation_check_timer == {next_timer_ref, next_timer_token}
    assert map_size(:sys.get_state(coordinator).workstreams.validation_checks) == 0

    assert manifest["restore_ready"] == false
    assert manifest["metadata"]["service_revision"] == String.duplicate("c", 40)
    assert manifest["metadata"]["run_ids"] == ["run-capture-humanwait", "run-capture-ready"]
    assert length(manifest["metadata"]["evidence_bundles"]) == 2
    assert {:ok, ^manifest} = BackupCapture.verify(c.capture_root, "quiet-capture", [c.workspace_root])

    assert {:ok, same_manifest} = Orchestrator.capture_backup(coordinator, "quiet-capture")
    assert same_manifest == manifest
    assert {:ok, ^manifest} = Orchestrator.backup_capture_status(coordinator, "quiet-capture")

    state_path = Path.join([c.capture_root, "quiet-capture", "state.sqlite"])
    evidence_path = Path.join([c.capture_root, "quiet-capture", "evidence.sqlite"])
    assert [[2]] = sqlite_rows(state_path, "SELECT COUNT(*) FROM runs")
    assert [[2]] = sqlite_rows(state_path, "SELECT COUNT(*) FROM events")
    assert [[3]] = sqlite_rows(state_path, "SELECT COUNT(*) FROM stage_attempts")
    assert [[2]] = sqlite_rows(state_path, "SELECT COUNT(*) FROM operations")
    assert [[0]] = sqlite_rows(state_path, "SELECT COUNT(*) FROM evidence_outbox")
    assert [["blob", _], ["blob", _]] = sqlite_rows(state_path, "SELECT typeof(payload), length(payload) FROM runs ORDER BY run_id")
    assert [[2]] = sqlite_rows(evidence_path, "SELECT COUNT(*) FROM evidence_runs")
    assert [[1]] = sqlite_rows(evidence_path, "SELECT COUNT(*) FROM evidence_artifacts")
    assert [["captured evidence bytes"]] = sqlite_rows(evidence_path, "SELECT payload FROM evidence_artifacts")

    assert {:ok, snapshot_store} = WorkstreamStore.start_link(path: state_path, owner: self())
    assert {:ok, captured_runs} = WorkstreamStore.load(snapshot_store)
    assert Map.new(captured_runs, &{&1.id, &1}) == Map.new([c.waiting_run, c.ready_run], &{&1.id, &1})
    assert :ok = GenServer.stop(snapshot_store)

    assert {:error, {:database_ownership_conflict, _, _}} = WorkstreamStore.start_link(path: c.db, owner: self())
    assert Process.alive?(store)
  end

  test "captures a linked child and keeps its wait and lineage held across recovery restarts", c do
    capture = create_quiet_child_capture(c)
    parent = Map.fetch!(capture.runs, capture.parent_id)
    child = Map.fetch!(capture.runs, capture.child_id)
    intent = parent.child_handoff

    assert parent.status == :waiting_for_child
    assert parent.phase == :waiting_for_child
    assert parent.stage_id == "implement-gap"
    assert parent.current_attempt_id == intent.parent_attempt_id
    assert intent == capture.intent
    assert %{type: :child, status: :waiting, stage: "implement-gap"} = Enum.find(parent.attempts, &(&1.id == intent.parent_attempt_id))
    assert %{type: :child, status: :waiting_for_child, child_handoff: ^intent} = parent.operations[intent.parent_attempt_id]
    assert child.id == intent.child_run_id
    assert child.status == :ready
    assert child.definition.name == "software-change"
    assert child.definition.input_values["task"] == parent.outputs["qualified-gap"]
    assert child.parent_run_id == parent.id
    assert child.parent_handoff == child_lineage(intent)

    config = recovery_capture_config(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    assert {:ok, %{runs: inspected_runs, report: inspection}} = RecoveryInspection.inspect(config)
    assert Map.new(inspected_runs, &{&1.id, &1}) == capture.runs
    assert inspection.status == :dispatch_held
    assert inspection.dispatch_allowed == false
    assert inspection.restore_ready == false

    runtime = start_recovery_runtime(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    state = :sys.get_state(runtime.coordinator)
    assert state.workstreams.runs == capture.runs
    assert state.workstreams.child_proofs == %{}
    assert state.workstreams.child_proof_timer == nil
    assert Task.Supervisor.children(runtime.tasks) == []
    assert {:ok, parent_report} = Orchestrator.workstream_state(runtime.coordinator, parent.id)
    assert parent_report == capture.reports[parent.id]
    assert {:ok, child_report} = Orchestrator.workstream_state(runtime.coordinator, child.id)
    assert child_report == capture.reports[child.id]

    stop_runtime(runtime)
    restarted = start_recovery_runtime(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    restarted_state = :sys.get_state(restarted.coordinator)

    assert recovery_report(restarted.coordinator).status == :dispatch_held
    assert restarted_state.workstreams.runs == capture.runs
    assert restarted_state.workstreams.child_proofs == %{}
    assert restarted_state.workstreams.child_proof_timer == nil
    assert Task.Supervisor.children(restarted.tasks) == []
    assert Map.fetch!(restarted_state.workstreams.runs, parent.id).child_handoff == intent
    assert Map.fetch!(restarted_state.workstreams.runs, child.id).parent_handoff == child_lineage(intent)
  end

  test "the linked-child inspector rejects a missing sibling and changed lineage", c do
    capture = create_quiet_child_capture(c)
    parent = Map.fetch!(capture.runs, capture.parent_id)
    child = Map.fetch!(capture.runs, capture.child_id)
    runs = Map.values(capture.runs)

    assert :ok = WorkstreamChild.inspect_recovery_links(runs)
    attempt_id = parent.child_handoff.parent_attempt_id
    attempt = Enum.find(parent.attempts, &(&1.id == attempt_id))
    assert Map.has_key?(attempt, :candidate_context_id)
    assert attempt.candidate_context_id == nil

    legacy = %{parent | attempts: Enum.map(parent.attempts, fn saved -> if saved.id == attempt_id, do: Map.delete(saved, :candidate_context_id), else: saved end)}
    assert :ok = WorkstreamChild.inspect_recovery_links([legacy, child])

    for value <- ["", String.duplicate("f", 64), false, %{}] do
      unsupported = %{parent | attempts: Enum.map(parent.attempts, fn saved -> if saved.id == attempt_id, do: Map.put(saved, :candidate_context_id, value), else: saved end)}
      assert {:error, :invalid_child_recovery_link} = WorkstreamChild.inspect_recovery_links([unsupported, child])
    end

    extra = %{parent | attempts: Enum.map(parent.attempts, fn saved -> if saved.id == attempt_id, do: Map.put(saved, "candidate_context_id", nil), else: saved end)}
    assert {:error, :invalid_child_recovery_link} = WorkstreamChild.inspect_recovery_links([extra, child])
    assert {:error, _reason} = WorkstreamChild.inspect_recovery_links(Enum.reject(runs, &(&1.id == child.id)))

    tampered = put_in(child.parent_handoff.parent_attempt_id, "different-attempt")
    assert {:error, _reason} = WorkstreamChild.inspect_recovery_links([parent, tampered])

    before_digest = capture_files_digest(c.capture_root, "quiet-capture")
    assert {:ok, images} = BackupCapture.read_images(c.capture_root, "quiet-capture", [c.workspace_root], capture.manifest_sha256)
    assert {:ok, _saved} = WorkstreamStore.inspect_recovery_image(images.images["state"])

    for sql <- [
          "DELETE FROM events WHERE event_id LIKE 'child-handoff/%/child'",
          "UPDATE events SET run_id=(SELECT run_id FROM runs WHERE run_id != events.run_id LIMIT 1) WHERE event_id LIKE 'child-handoff/%/child'",
          "UPDATE events SET payload=(SELECT payload FROM events WHERE event_id LIKE 'child-handoff/%/parent' LIMIT 1) WHERE event_id LIKE 'child-handoff/%/child'"
        ] do
      path = Path.join(c.root, "mutated-child-#{System.unique_integer([:positive])}.sqlite")
      File.write!(path, images.images["state"])
      assert {:ok, db} = Sqlite3.open(path)
      assert :ok = Sqlite3.execute(db, sql)
      assert :ok = Sqlite3.close(db)
      assert {:error, :invalid_recovery_image} = WorkstreamStore.inspect_recovery_image(File.read!(path))
    end

    assert capture_files_digest(c.capture_root, "quiet-capture") == before_digest
  end

  test "rejects unresolved publications, validation jobs, and cleanup-enabled coordinators", c do
    runtime = start_runtime(c)
    coordinator = runtime.coordinator

    intent = %{
      branch: "cycle/candidate",
      candidate_sha: String.duplicate("a", 40),
      publication_id: String.duplicate("b", 64),
      validation_attempt_id: "validation-1",
      evidence_revision: String.duplicate("d", 64)
    }

    operation_id = "run-capture-ready/publish/1"
    operation = %{id: operation_id, status: :completed, publication_intent: intent}
    blocked = %{c.ready_run | status: :blocked, phase: :blocked, operations: %{operation_id => operation}}

    for status <- [:blocked, :complete] do
      candidate = %{blocked | status: status, phase: status}

      :sys.replace_state(coordinator, fn state ->
        workstreams = Map.update!(state.workstreams, :runs, &Map.put(&1, candidate.id, candidate))
        %{state | workstreams: workstreams}
      end)

      assert {:error, :capture_operation_unresolved} =
               Orchestrator.capture_backup(coordinator, "unresolved-publish-#{status}")
    end

    :sys.replace_state(coordinator, fn state ->
      workstreams = Map.put(state.workstreams, :publications, %{"active-publication" => %{ref: make_ref()}})
      %{state | workstreams: workstreams}
    end)

    assert {:error, :capture_publication_active} = Orchestrator.capture_backup(coordinator, "active-publication")

    :sys.replace_state(coordinator, fn state ->
      workstreams = Map.update!(state.workstreams, :runs, &Map.delete(&1, blocked.id))
      workstreams = Map.put(workstreams, :publications, %{})
      workstreams = Map.put(workstreams, :validation_checks, %{"check-1" => %{status: :running}})
      %{state | workstreams: workstreams}
    end)

    assert {:error, :capture_validation_active} = Orchestrator.capture_backup(coordinator, "validation-active")

    :sys.replace_state(coordinator, fn state ->
      workstreams = Map.delete(state.workstreams, :validation_checks)
      workstreams = Map.update!(workstreams, :opts, &Keyword.put(&1, :merged_cleanup, true))
      %{state | workstreams: workstreams}
    end)

    assert {:error, :capture_not_configured} = Orchestrator.capture_backup(coordinator, "cleanup-mode-mismatch")

    :sys.replace_state(coordinator, fn state ->
      options = Keyword.update!(state.workstreams.opts, :backup_capture, &Map.put(&1, :cleanup, :enabled))
      state = put_in(state.workstreams.opts, options)
      put_in(state.workstreams.cleanup.jobs, %{"cleanup-active" => %{}})
    end)

    assert {:error, :capture_cleanup_active_or_uncertain} = Orchestrator.capture_backup(coordinator, "cleanup-active")

    :sys.replace_state(coordinator, fn state ->
      state = put_in(state.workstreams.cleanup.jobs, %{})

      put_in(
        state.workstreams.runs[c.waiting_run.id][:cleanup],
        %{status: :pending, delete_started: true, last_error: :cleanup_outcome_unknown}
      )
    end)

    assert {:error, :capture_cleanup_active_or_uncertain} = Orchestrator.capture_backup(coordinator, "cleanup-uncertain")
  end

  test "actual capture clears the periodic token before a held expiry can rearm", c do
    runtime = start_runtime(c)
    coordinator = runtime.coordinator
    test_pid = self()

    held =
      :sys.replace_state(coordinator, fn state ->
        {timer_ref, token} = state.workstreams.validation_check_timer

        {:reply, {:ok, %{status: :capturing}}, capturing} =
          Orchestrator.handle_call({:backup_capture, "timer-boundary"}, {test_pid, make_ref()}, state)

        assert Orchestrator.capture_held?(capturing)
        assert capturing.workstreams.validation_check_timer == nil
        assert Process.read_timer(timer_ref) == false
        assert {:noreply, ^capturing} = Orchestrator.handle_info({:validation_checks_tick, token}, capturing)
        assert {:noreply, ^capturing} = Orchestrator.handle_info(:reconcile_validation_checks, capturing)
        capturing
      end)

    assert Orchestrator.capture_held?(held)
    assert {:ok, %{"restore_ready" => false}} = await_capture(coordinator, "timer-boundary")
    released = :sys.get_state(coordinator)
    refute Orchestrator.capture_held?(released)
    {timer_ref, _token} = released.workstreams.validation_check_timer
    assert is_integer(Process.read_timer(timer_ref))
  end

  test "holds intake during capture, ignores stale completion, and waits for both owners after interruption", c do
    runtime = start_runtime(c)
    coordinator = runtime.coordinator
    store = :sys.get_state(coordinator).workstreams.store
    assert {:ok, plan} = BackupCapture.prepare(c.capture_root, "interrupted-capture", [c.workspace_root])
    test_pid = self()

    held_state =
      :sys.replace_state(coordinator, fn state ->
        token = make_ref()

        worker =
          spawn(fn ->
            receive do
              :snapshot -> WorkstreamStore.snapshot_capture(store, token)
            end
          end)

        ref = Process.monitor(worker)
        :ok = WorkstreamStore.begin_capture(store, token, worker, plan.state_path)
        :ok = Evidence.begin_capture(Evidence, token, worker, plan.evidence_path, Map.values(state.workstreams.runs))

        stale_token = make_ref()
        stale_store_end = WorkstreamStore.end_capture(store, stale_token)
        stale_evidence_end = Evidence.end_capture(Evidence, stale_token)
        send(test_pid, {:stale_capture_end, stale_store_end, stale_evidence_end})

        validation_timer = Map.get(state.workstreams, :validation_check_timer)
        if validation_timer, do: Process.cancel_timer(elem(validation_timer, 0))
        send(test_pid, {:validation_timer_paused, validation_timer})

        linear =
          case state.linear do
            nil ->
              nil

            current ->
              if current.timer, do: Process.cancel_timer(current.timer)
              %{current | timer: nil}
          end

        capture = %{id: plan.id, token: token, pid: worker, ref: ref, evidence_server: Evidence}

        workstreams =
          state.workstreams
          |> Map.put(:validation_check_timer, nil)
          |> Map.put(:capture, capture)

        %{state | workstreams: workstreams, linear: linear}
      end)

    capture = held_state.workstreams.capture
    token = capture.token
    worker = capture.pid
    assert_receive {:stale_capture_end, {:error, :capture_not_authorized}, {:error, :capture_not_authorized}}
    assert_receive {:validation_timer_paused, {paused_timer_ref, paused_timer_token}}
    assert Process.read_timer(paused_timer_ref) == false
    assert held_state.workstreams.validation_check_timer == nil
    assert Orchestrator.capture_held?(held_state)
    assert :sys.get_state(store).capture.token == token
    assert :sys.get_state(Evidence).capture.token == token

    assert {:error, :capture_in_progress} =
             GenServer.call(coordinator, {:validation_stage_verifier_owner, "run-capture-ready", "attempt", %{}})

    assert {:error, :capture_in_progress} =
             GenServer.call(coordinator, {:validation_monitor_owner, "run-capture-ready", %{}})

    send(coordinator, :advance_workstreams)
    send(coordinator, {make_ref(), {:validation_stage_verified, "run-capture-ready", "attempt", self(), %{}, :qualified}})
    send(coordinator, {make_ref(), {:validation_observation, "run-capture-ready", %{}, :qualified}})
    send(coordinator, {:validation_checks_tick, paused_timer_token})
    send(coordinator, {:validation_checks_tick, make_ref()})
    send(coordinator, :reconcile_validation_checks)
    assert {:ok, %{status: :capturing, restore_ready: false}} = Orchestrator.backup_capture_status(coordinator, "interrupted-capture")
    assert :sys.get_state(coordinator).workstreams.runs == held_state.workstreams.runs
    assert :sys.get_state(coordinator).workstreams.validation_check_timer == nil
    assert map_size(:sys.get_state(coordinator).workstreams.validation_checks) == 0

    assert :ok = :sys.suspend(store)

    on_exit(fn ->
      if Process.alive?(worker), do: Process.exit(worker, :kill)

      if Process.alive?(store) do
        try do
          :sys.resume(store)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    send(worker, :snapshot)
    assert :ok = await_queued_capture_snapshot(store, worker, token)

    evidence_write = Evidence.capture(c.waiting_run, %{payload: %{"method" => "item/completed", "params" => %{"item" => %{"type" => "agentMessage", "text" => "during hold"}}}}, Evidence)
    assert {:error, :capture_in_progress} = evidence_write
    assert {:error, :capture_in_progress} = Orchestrator.queue_workstream(coordinator, "held-queue", "task-held", c.workstream_path, %{"task" => "blocked"}, c.options)

    stale_result_token = make_ref()
    send(coordinator, {:backup_capture_result, stale_result_token, worker, {:ok, %{"restore_ready" => true}}})
    assert {:ok, %{status: :capturing, restore_ready: false}} = Orchestrator.backup_capture_status(coordinator, "interrupted-capture")

    event_conn = webhook_conn(coordinator)
    assert event_conn.status == 503

    pending_write = Task.async(fn -> WorkstreamStore.linear_receive(store, "held-delivery", %{kind: :created}) end)
    assert :ok = await_queued_store_write(store, pending_write.pid)
    worker_ref = Process.monitor(worker)
    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^worker_ref, :process, ^worker, :killed}, 5_000

    await(fn ->
      case :sys.get_state(coordinator).workstreams.capture do
        %{settled: settled} -> if MapSet.member?(settled, :evidence), do: :ok
        _ -> nil
      end
    end)

    held_after_settle = :sys.get_state(coordinator)
    assert Orchestrator.capture_held?(held_after_settle)
    refute MapSet.member?(held_after_settle.workstreams.capture.settled, :store)
    assert :sys.get_state(Evidence).capture != nil
    assert File.exists?(Path.join([c.capture_root, "interrupted-capture", "manifest.json"])) == false

    assert :ok = :sys.resume(store)
    assert {:error, :capture_in_progress} = Task.await(pending_write, 5_000)
    assert {:error, :capture_worker_failed} = await_capture(coordinator, "interrupted-capture")
    refute Orchestrator.capture_held?(:sys.get_state(coordinator))
    refute File.exists?(Path.join([c.capture_root, "interrupted-capture", "manifest.json"]))

    {released_timer_ref, released_timer_token} = :sys.get_state(coordinator).workstreams.validation_check_timer
    refute released_timer_ref == paused_timer_ref
    refute released_timer_token == paused_timer_token
    assert is_integer(Process.read_timer(released_timer_ref))

    for _ <- 1..10 do
      send(coordinator, {:validation_checks_tick, paused_timer_token})
      send(coordinator, :reconcile_validation_checks)
    end

    assert {:error, :capture_worker_failed} = await_capture(coordinator, "interrupted-capture")
    assert :sys.get_state(coordinator).workstreams.validation_check_timer == {released_timer_ref, released_timer_token}

    send(coordinator, {:validation_checks_tick, released_timer_token})
    assert {:error, :capture_worker_failed} = await_capture(coordinator, "interrupted-capture")
    {next_timer_ref, next_timer_token} = :sys.get_state(coordinator).workstreams.validation_check_timer
    refute next_timer_ref == released_timer_ref
    refute next_timer_token == released_timer_token
    assert Process.read_timer(released_timer_ref) == false
    assert is_integer(Process.read_timer(next_timer_ref))

    for _ <- 1..10 do
      send(coordinator, {:validation_checks_tick, released_timer_token})
      send(coordinator, :reconcile_validation_checks)
    end

    assert {:error, :capture_worker_failed} = await_capture(coordinator, "interrupted-capture")
    assert :sys.get_state(coordinator).workstreams.validation_check_timer == {next_timer_ref, next_timer_token}
    assert map_size(:sys.get_state(coordinator).workstreams.validation_checks) == 0

    assert :ok = WorkstreamStore.linear_receive(store, "after-capture", %{kind: :created})
    assert :ok = Evidence.capture(c.waiting_run, %{payload: %{"method" => "item/completed", "params" => %{"item" => %{"type" => "agentMessage", "text" => "after hold"}}}}, Evidence)
  end

  test "anchored recovery inspection preserves captured runs and remains dispatch-held across restart", c do
    capture = create_quiet_capture(c)
    state_db_digest = digest_file(c.db)
    capture_digest_before = capture_files_digest(c.capture_root, "quiet-capture")

    # A held inspection must use the captured definition even if today's source file changed.
    File.write!(c.workstream_path, "this is no longer valid workstream YAML\n")

    runtime = start_recovery_runtime(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    state = :sys.get_state(runtime.coordinator)
    recovery = recovery_report(runtime.coordinator)

    assert recovery.status == :dispatch_held
    assert recovery.restore_ready == false
    assert recovery.dispatch_allowed == false
    assert recovery.policy_compatible == true
    assert state.recovery == recovery
    assert state.polling_enabled == false
    assert state.tick_timer_ref == nil
    assert state.tick_token == nil
    assert state.linear == nil
    assert state.workstreams.store == nil
    assert state.workstreams.store_path == nil
    assert state.workstreams.validation_check_timer == nil
    assert state.workstreams.cleanup.timer == nil
    assert map_size(state.workstreams.workers) == 0
    assert map_size(state.workstreams.cancellations) == 0
    assert map_size(state.workstreams.publications) == 0
    assert map_size(state.workstreams.validation_checks) == 0
    assert Task.Supervisor.children(runtime.tasks) == []
    assert Process.whereis(Evidence) == c.evidence_pid

    for {run_id, captured_run} <- capture.runs do
      assert Map.fetch!(state.workstreams.runs, run_id) == captured_run
      assert {:ok, report} = Orchestrator.workstream_state(runtime.coordinator, run_id)
      assert report == Map.fetch!(capture.reports, run_id)
    end

    assert is_map(Orchestrator.snapshot(runtime.coordinator, 1_000))

    assert {:error, :recovery_dispatch_held} =
             Orchestrator.receive_linear_event(runtime.coordinator, %{kind: :created})

    assert {:error, :recovery_dispatch_held} =
             Orchestrator.queue_workstream(
               runtime.coordinator,
               "held-queue",
               "held-task",
               c.workstream_path,
               %{"task" => "must not start"},
               c.options
             )

    assert {:error, :recovery_dispatch_held} =
             Orchestrator.answer_workstream(runtime.coordinator, "held-answer", "run-capture-humanwait", "wait", %{"body" => "no"})

    assert {:error, :recovery_dispatch_held} = Orchestrator.step_workstreams(runtime.coordinator)
    assert {:error, :recovery_dispatch_held} = Orchestrator.reconcile_workstreams(runtime.coordinator)

    assert {:error, :recovery_dispatch_held} =
             GenServer.call(runtime.coordinator, {:validation_monitor_owner, "run-capture-ready", %{}})

    assert {:error, :recovery_dispatch_held} =
             GenServer.call(runtime.coordinator, {:workstream_publication_intent, "run-capture-ready", %{}})

    before_messages = :sys.get_state(runtime.coordinator)

    for message <- [
          :linear_process,
          :linear_tick,
          :reconcile_validation_checks,
          :advance_workstreams,
          :reconcile_merged_cleanup,
          :observe_stopped_publications,
          {:validation_checks_tick, make_ref()},
          {:linear_result, make_ref(), {:ok, :ignored}},
          {make_ref(), {:validation_observation, "run-capture-ready", %{}, :qualified}},
          {make_ref(), {:workstream_canceled, "run-capture-ready", "attempt", :terminated}}
        ] do
      send(runtime.coordinator, message)
    end

    assert :ok = GenServer.cast(runtime.coordinator, {:unexpected_mutating_cast, make_ref()})

    after_messages = :sys.get_state(runtime.coordinator)
    assert after_messages == before_messages
    assert Task.Supervisor.children(runtime.tasks) == []
    assert capture_files_digest(c.capture_root, "quiet-capture") == capture_digest_before
    assert digest_file(c.db) == state_db_digest
    assert evidence_tree_digest(c.evidence_root) == capture.evidence_tree_digest

    stop_runtime(runtime)
    restarted = start_recovery_runtime(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    restarted_state = :sys.get_state(restarted.coordinator)

    assert recovery_report(restarted.coordinator).status == :dispatch_held
    assert recovery_report(restarted.coordinator).dispatch_allowed == false
    assert restarted_state.workstreams.store == nil
    assert restarted_state.workstreams.validation_check_timer == nil
    assert restarted_state.workstreams.cleanup.timer == nil
    assert restarted_state.workstreams.runs == capture.runs
    assert restarted_state.tick_timer_ref == nil
    assert restarted_state.linear == nil
    assert Task.Supervisor.children(restarted.tasks) == []
    assert Process.whereis(Evidence) == c.evidence_pid
    assert capture_files_digest(c.capture_root, "quiet-capture") == capture_digest_before
    assert digest_file(c.db) == state_db_digest
    assert evidence_tree_digest(c.evidence_root) == capture.evidence_tree_digest
  end

  test "cold full application boot and restart keep a linked child capture held", c do
    capture = create_quiet_child_capture(c)
    plan_path = write_application_recovery_plan(c, capture)
    plan_bytes = File.read!(plan_path)
    capture_before = capture_files_digest(c.capture_root, "quiet-capture")
    state_before = digest_file(c.db)
    evidence_before = evidence_tree_digest(c.evidence_root)

    # The application must rely on the captured definition pins, not today's mutable file.
    File.write!(Path.join([c.root, "dev243-child-fixture", "coverage-factory.yaml"]), "this is no longer valid workstream YAML\n")

    {output, status} = run_cold_application_rehearsal(c, capture, plan_path)

    assert status == 0, output
    assert File.read!(plan_path) == plan_bytes
    assert capture_files_digest(c.capture_root, "quiet-capture") == capture_before
    assert digest_file(c.db) == state_before
    assert evidence_tree_digest(c.evidence_root) == evidence_before
  end

  @tag stale_saved_policy: true
  test "rejects changed or missing capture anchors and reports incompatible policy without writes", c do
    capture = create_quiet_capture(c)
    manifest_path = Path.join([c.capture_root, "quiet-capture", "manifest.json"])
    manifest_bytes = File.read!(manifest_path)
    state_db_digest = digest_file(c.db)
    capture_digest_before = capture_files_digest(c.capture_root, "quiet-capture")

    File.write!(manifest_path, manifest_bytes <> " ")
    assert_recovery_start_fails(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    File.write!(manifest_path, manifest_bytes)

    missing_manifest_path = manifest_path <> ".missing"
    File.rename!(manifest_path, missing_manifest_path)
    assert_recovery_start_fails(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    File.rename!(missing_manifest_path, manifest_path)

    runtime = start_recovery_runtime(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    state = :sys.get_state(runtime.coordinator)
    recovery = recovery_report(runtime.coordinator)

    assert recovery.status == :dispatch_held
    assert recovery.restore_ready == false
    assert recovery.dispatch_allowed == false
    assert recovery.policy_compatible == false
    assert recovery.incompatible_run_ids == Enum.sort(Map.keys(capture.runs))
    assert state.workstreams.store == nil
    assert state.workstreams.validation_check_timer == nil
    assert state.workstreams.cleanup.timer == nil
    assert state.workstreams.runs == capture.runs
    assert state.tick_timer_ref == nil
    assert state.linear == nil
    assert Task.Supervisor.children(runtime.tasks) == []
    assert Process.whereis(Evidence) == c.evidence_pid
    assert capture_files_digest(c.capture_root, "quiet-capture") == capture_digest_before
    assert digest_file(c.db) == state_db_digest
    assert evidence_tree_digest(c.evidence_root) == capture.evidence_tree_digest
  end

  test "recovery evidence inspection rejects changed identity, duplicate JSON and unsupported schema", c do
    capture = create_quiet_capture(c)
    original = capture_files_digest(c.capture_root, "quiet-capture")
    assert {:ok, images} = BackupCapture.read_images(c.capture_root, "quiet-capture", [c.workspace_root], capture.manifest_sha256)
    runs = Map.values(capture.runs)
    assert {:ok, _} = Evidence.inspect_recovery_image(images.images["evidence"], runs)

    for sql <- [
          "UPDATE evidence_runs SET task_key='wrong-task'",
          "UPDATE evidence_events SET run_id='missing-run'",
          "UPDATE evidence_runs SET snapshot='{\"id\":\"wrong\",' || substr(snapshot,2)",
          "UPDATE evidence_events SET payload='{\"kind\":\"one\",\"kind\":\"two\"}'",
          "CREATE TABLE unexpected (value TEXT)",
          "PRAGMA user_version=2"
        ] do
      path = Path.join(c.root, "mutated-evidence-#{System.unique_integer([:positive])}.sqlite")
      File.write!(path, images.images["evidence"])
      assert {:ok, db} = Sqlite3.open(path)
      assert :ok = Sqlite3.execute(db, sql)
      assert :ok = Sqlite3.close(db)
      assert {:error, :invalid_recovery_evidence} = Evidence.inspect_recovery_image(File.read!(path), runs)
    end

    assert capture_files_digest(c.capture_root, "quiet-capture") == original
  end

  @tag legacy_saved_policy: true
  test "retains a legacy saved policy in a real held capture without upgrading its pin", c do
    capture = create_quiet_capture(c)
    legacy_policy = Map.delete(WorkstreamRun.policy(), :child_sha256)

    assert Enum.all?(capture.runs, fn {_id, run} -> run.policy == legacy_policy end)

    config = recovery_capture_config(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    assert {:ok, %{runs: inspected_runs, report: inspection}} = RecoveryInspection.inspect(config)
    assert inspection.status == :dispatch_held
    assert inspection.policy_compatible == false
    assert inspection.incompatible_run_ids == Enum.sort(Map.keys(capture.runs))
    assert Enum.all?(inspected_runs, fn run -> run.policy == legacy_policy end)

    runtime = start_recovery_runtime(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    assert recovery_report(runtime.coordinator).policy_compatible == false
    assert :sys.get_state(runtime.coordinator).workstreams.runs == capture.runs
    assert Task.Supervisor.children(runtime.tasks) == []

    stop_runtime(runtime)
    restarted = start_recovery_runtime(c, "quiet-capture", capture.manifest_sha256, WorkstreamRun.policy())
    restarted_state = :sys.get_state(restarted.coordinator)
    assert recovery_report(restarted.coordinator).policy_compatible == false
    assert restarted_state.workstreams.runs == capture.runs
    assert Enum.all?(restarted_state.workstreams.runs, fn {_id, run} -> run.policy == legacy_policy end)
    assert Task.Supervisor.children(restarted.tasks) == []
  end

  defp create_quiet_capture(c) do
    runtime = start_capture_runtime(c)
    coordinator = runtime.coordinator

    pause_evidence_flush()
    assert :ok = Evidence.artifact(c.waiting_run, "held inspection fixture", "captured inspection evidence", Evidence)
    assert {:ok, %{id: "quiet-capture", status: :capturing, restore_ready: false}} = Orchestrator.capture_backup(coordinator, "quiet-capture")
    assert {:ok, manifest} = await_capture(coordinator, "quiet-capture")
    assert {:ok, ^manifest} = BackupCapture.verify(c.capture_root, "quiet-capture", [c.workspace_root])
    pause_evidence_flush()

    state = :sys.get_state(coordinator)
    runs = state.workstreams.runs

    reports =
      Map.new(runs, fn {run_id, _run} ->
        assert {:ok, report} = Orchestrator.workstream_state(coordinator, run_id)
        {run_id, report}
      end)

    manifest_sha256 = digest_file(Path.join([c.capture_root, "quiet-capture", "manifest.json"]))
    stop_runtime(runtime)

    %{manifest: manifest, manifest_sha256: manifest_sha256, runs: runs, reports: reports, evidence_tree_digest: evidence_tree_digest(c.evidence_root)}
  end

  defp child_lineage(intent), do: Map.take(intent, [:handoff_id, :parent_run_id, :parent_stage_id, :parent_attempt_id, :fingerprint])

  defp create_quiet_child_capture(c) do
    fixture_root = Path.join(c.root, "dev243-child-fixture")

    Enum.each(@child_fixture_files, fn relative_path ->
      source = Path.join(@child_fixture_dir, relative_path)
      destination = Path.join(fixture_root, relative_path)
      File.mkdir_p!(Path.dirname(destination))
      File.cp!(source, destination)
    end)

    parent_options = [
      workspace: c.ready_workspace,
      workspace_root: c.workspace_root,
      branch: "cycle/capture-parent",
      codex_command: "/fixture/no-codex-command"
    ]

    parent_path = Path.join(fixture_root, "coverage-factory.yaml")

    assert {:ok, parent_definition, parent_workspace} =
             WorkstreamRunner.prepare(parent_path, %{"request" => "qualify the child capture"}, parent_options)

    assert {:ok, parent_execution} = WorkstreamRunner.pin_execution_context(parent_options, parent_workspace, parent_definition)

    parent =
      WorkstreamRun.new("task-capture-parent", parent_definition, parent_workspace, parent_options, parent_execution)
      |> Map.put(:id, "run-capture-child-parent")
      |> Map.put(:stage_id, "implement-gap")
      |> Map.put(:outputs, %{
        "request" => "qualify the child capture",
        "capability-gap" => "capture a linked child safely",
        "qualified-gap" => "capture a linked child safely"
      })

    child_stage = parent.definition.stages[parent.stage_id]
    assert {:ok, child_inputs} = WorkstreamChild.inputs(child_stage, parent.outputs)
    child_definition = Map.put(child_stage.child_definition, :input_values, child_inputs)

    child_options = [
      workspace: c.waiting_workspace,
      workspace_root: c.workspace_root,
      branch: "cycle/capture-child",
      codex_command: "/fixture/no-codex-command"
    ]

    assert {:ok, child_execution} = WorkstreamRunner.pin_execution_context(child_options, c.waiting_workspace, child_definition)

    child =
      WorkstreamRun.new("task-capture-child", child_definition, c.waiting_workspace, child_options, child_execution)

    assert {:ok, waiting_parent, linked_child, intent} = WorkstreamRun.begin_child(parent, child)
    child_db = Path.join([c.root, "child-state", "runs.sqlite"])
    assert {:ok, store} = WorkstreamStore.start_link(path: child_db, owner: self())
    assert :ok = WorkstreamStore.commit(store, parent, "child-capture-parent-created", %{kind: :queued})

    assert {:ok, ^waiting_parent, ^linked_child} =
             WorkstreamStore.commit_child_handoff(store, parent, waiting_parent, linked_child, intent)

    assert :ok = GenServer.stop(store)

    child_context = Map.put(c, :db, child_db)
    runtime = start_capture_runtime(child_context)
    coordinator = runtime.coordinator

    pause_evidence_flush()

    assert {:ok, %{id: "quiet-capture", status: :capturing, restore_ready: false}} =
             Orchestrator.capture_backup(coordinator, "quiet-capture")

    assert {:ok, manifest} = await_capture(coordinator, "quiet-capture")
    assert {:ok, ^manifest} = BackupCapture.verify(c.capture_root, "quiet-capture", [c.workspace_root])

    state = :sys.get_state(coordinator)
    runs = state.workstreams.runs
    parent = Map.fetch!(runs, parent.id)
    child = Map.fetch!(runs, linked_child.id)
    assert parent.status == :waiting_for_child
    assert parent.child_handoff == intent
    assert child.status == :ready
    assert child.parent_handoff == child_lineage(intent)
    assert manifest["metadata"]["run_ids"] == Enum.sort([parent.id, child.id])

    reports =
      Map.new(runs, fn {run_id, _run} ->
        assert {:ok, report} = Orchestrator.workstream_state(coordinator, run_id)
        {run_id, report}
      end)

    manifest_sha256 = digest_file(Path.join([c.capture_root, "quiet-capture", "manifest.json"]))
    stop_runtime(runtime)

    %{
      manifest: manifest,
      manifest_sha256: manifest_sha256,
      runs: runs,
      reports: reports,
      parent_id: parent.id,
      child_id: child.id,
      intent: intent
    }
  end

  defp recovery_capture_config(c, capture_id, manifest_sha256, policy) do
    %{
      root: c.capture_root,
      id: capture_id,
      workspace_roots: [c.workspace_root],
      manifest_sha256: manifest_sha256,
      service_revision: String.duplicate("c", 40),
      policy: policy
    }
  end

  defp start_recovery_runtime(c, capture_id, manifest_sha256, policy) do
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "RecoveryRuntime#{suffix}")
    coordinator = Module.concat(__MODULE__, "RecoveryCoordinator#{suffix}")
    tasks = Module.concat(__MODULE__, "RecoveryTasks#{suffix}")

    opts = [
      name: supervisor,
      orchestrator_name: coordinator,
      task_supervisor_name: tasks,
      recovery_capture: %{
        root: c.capture_root,
        id: capture_id,
        workspace_roots: [c.workspace_root],
        manifest_sha256: manifest_sha256,
        service_revision: String.duplicate("c", 40),
        policy: policy
      }
    ]

    runtime = start_isolated_runtime(opts)
    assert {:ok, _report} = Orchestrator.recovery_status(coordinator)
    runtime
  end

  defp write_application_recovery_plan(c, capture) do
    {:ok, canonical_root} = SymphonyElixir.PathSafety.canonicalize(c.root)
    plan_dir = Path.join(canonical_root, "recovery-plan")
    File.mkdir!(plan_dir)
    File.chmod!(plan_dir, 0o700)

    policy =
      Map.new(WorkstreamRun.policy(), fn {key, value} ->
        {Atom.to_string(key), value}
      end)

    plan = %{
      "version" => 1,
      "mode" => "held_inspection",
      "root" => c.capture_root,
      "id" => "quiet-capture",
      "workspace_roots" => [c.workspace_root],
      "manifest_sha256" => capture.manifest_sha256,
      "service_revision" => String.duplicate("c", 40),
      "policy" => policy
    }

    plan_path = Path.join(plan_dir, "plan.json")
    File.write!(plan_path, Jason.encode!(plan))
    File.chmod!(plan_path, 0o600)
    plan_path
  end

  defp run_cold_application_rehearsal(c, capture, plan_path) do
    script = """
    watchdog = spawn_link(fn -> receive do :cancel -> :ok after 30_000 -> System.halt(124) end end)
    ack = "--i-understand-that-this-will-be-running-without-the-usual-guardrails"
    plan_path = System.fetch_env!("DEV242_PLAN")
    workflow_path = System.fetch_env!("DEV242_WORKFLOW")
    logs_root = System.fetch_env!("DEV242_LOGS")
    capture_root = System.fetch_env!("DEV242_CAPTURE_ROOT")
    expected_ids = Jason.decode!(System.fetch_env!("DEV242_EXPECTED_IDS"))
    expected_pins = Jason.decode!(System.fetch_env!("DEV242_EXPECTED_PINS"))
    expected_runs = Jason.decode!(System.fetch_env!("DEV242_EXPECTED_RUNS"))
    expected_capture_files = Jason.decode!(System.fetch_env!("DEV242_CAPTURE_FILES"))

    check_capture_files = fn ->
      actual =
        Map.new(["state.sqlite", "evidence.sqlite", "manifest.json"], fn name ->
          path = Path.join([capture_root, "quiet-capture", name])
          digest = path |> File.read!() |> then(fn bytes -> :crypto.hash(:sha256, bytes) end) |> Base.encode16(case: :lower)
          {name, digest}
        end)

      true = actual == expected_capture_files
    end

    check_tree = fn ->
      root_children = Supervisor.which_children(SymphonyElixir.Supervisor)
      true = Enum.map(root_children, &elem(&1, 0)) == [SymphonyElixir.AgentRuntimeSupervisor]

      runtime_children = Supervisor.which_children(SymphonyElixir.AgentRuntimeSupervisor)
      runtime_child_ids = runtime_children |> Enum.map(&elem(&1, 0)) |> Enum.sort()
      true = runtime_child_ids == Enum.sort([SymphonyElixir.TaskSupervisor, SymphonyElixir.Orchestrator])

      for name <- [
            SymphonyElixir.WorkflowStore,
            SymphonyElixir.Evidence,
            SymphonyElixir.Linear.OAuth,
            SymphonyElixir.HttpServer,
            SymphonyElixir.LinearWebhookServer,
            SymphonyElixir.StatusDashboard,
            SymphonyElixir.PubSub,
            SymphonyElixirWeb.Endpoint
          ] do
        true = Process.whereis(name) == nil
      end

      state = :sys.get_state(SymphonyElixir.Orchestrator)
      {:ok, report} = SymphonyElixir.Orchestrator.recovery_status(SymphonyElixir.Orchestrator)
      true = report.status == :dispatch_held
      true = report.dispatch_allowed == false
      true = report.restore_ready == false
      true = report.capture_id == "quiet-capture"
      true = state.recovery == report
      true = state.polling_enabled == false
      true = state.linear == nil
      true = state.workstreams.store == nil
      true = state.workstreams.validation_check_timer == nil
      true = state.workstreams.child_proofs == %{}
      true = state.workstreams.child_proof_timer == nil
      true = state.workstreams.cleanup.timer == nil
      true = Enum.sort(Map.keys(state.workstreams.runs)) == expected_ids

      actual_pins =
        Map.new(state.workstreams.runs, fn {run_id, run} ->
          pins = Map.new(run.definition.definitions, fn {path, resource} -> {path, resource.sha256} end)
          {run_id, pins}
        end)

      actual_runs =
        Map.new(state.workstreams.runs, fn {run_id, run} ->
          summary = %{
            "status" => Atom.to_string(run.status),
            "validation_repair_rounds" => run.validation_repair_rounds,
            "review_repair_rounds" => run.review_repair_rounds,
            "transport_retries" => run.transport_retries,
            "has_pending_wait" => not is_nil(run.pending_wait)
          }

          {run_id, summary}
        end)

      true = actual_pins == expected_pins
      true = actual_runs == expected_runs
      true = Task.Supervisor.children(SymphonyElixir.TaskSupervisor) == []
      check_capture_files.()
      {state, report}
    end

    :ok =
      SymphonyElixir.CLI.evaluate([
        ack,
        "--recovery-plan",
        plan_path,
        "--logs-root",
        logs_root,
        workflow_path
      ])

    {:ok, _started} = Application.ensure_all_started(:symphony_elixir)
    true = Application.get_env(:symphony_elixir, :recovery_plan_path) == plan_path
    {initial_state, initial_report} = check_tree.()
    Logger.flush()
    :ok = :logger_disk_log_h.filesync(:symphony_disk_log)
    log_text = Path.wildcard(Path.join([logs_root, "log", "symphony.log.*"])) |> Enum.map(&File.read!/1) |> IO.iodata_to_binary()
    true = String.contains?(log_text, "recovery_dispatch_held capture_id=quiet-capture")
    true = String.contains?(log_text, "dispatch_allowed=false restore_ready=false")
    snapshot = SymphonyElixir.Orchestrator.snapshot(SymphonyElixir.Orchestrator, 1_000)
    true = snapshot.recovery == initial_report
    true = snapshot.running == [] and snapshot.retrying == [] and snapshot.blocked == []

    {:error, running_cli_error} =
      SymphonyElixir.CLI.evaluate([
        ack,
        "--recovery-plan",
        plan_path,
        "--logs-root",
        logs_root,
        workflow_path
      ])

    true = String.contains?(running_cli_error, "recovery_requires_cold_start")
    true = :sys.get_state(SymphonyElixir.Orchestrator) == initial_state

    true =
      SymphonyElixir.Orchestrator.queue_workstream(
        SymphonyElixir.Orchestrator,
        "held-queue",
        "held-task",
        workflow_path,
        %{"task" => "must not start"},
        []
      ) == {:error, :recovery_dispatch_held}

    true = SymphonyElixir.Orchestrator.receive_linear_event(SymphonyElixir.Orchestrator, %{kind: :created}) == {:error, :recovery_dispatch_held}
    true = SymphonyElixir.Orchestrator.reconcile_workstreams(SymphonyElixir.Orchestrator) == {:error, :recovery_dispatch_held}

    for message <- [:linear_process, :advance_workstreams, :reconcile_validation_checks, {:validation_checks_tick, make_ref()}],
        do: send(SymphonyElixir.Orchestrator, message)

    :ok = GenServer.cast(SymphonyElixir.Orchestrator, {:unexpected_mutating_cast, make_ref()})
    true = :sys.get_state(SymphonyElixir.Orchestrator) == initial_state
    true = Task.Supervisor.children(SymphonyElixir.TaskSupervisor) == []
    check_capture_files.()

    :ok = Application.stop(:symphony_elixir)
    {:ok, _restarted} = Application.ensure_all_started(:symphony_elixir)
    {restarted_state, restarted_report} = check_tree.()
    true = restarted_report == initial_report
    true = restarted_state.workstreams.runs == initial_state.workstreams.runs

    :ok = Application.stop(:symphony_elixir)
    original_plan = File.read!(plan_path)
    File.write!(plan_path, "not-json")

    true = match?({:error, _reason}, Application.ensure_all_started(:symphony_elixir))
    true = Process.whereis(SymphonyElixir.Supervisor) == nil
    true = Process.whereis(SymphonyElixir.AgentRuntimeSupervisor) == nil

    for name <- [
          SymphonyElixir.WorkflowStore,
          SymphonyElixir.Evidence,
          SymphonyElixir.Linear.OAuth,
          SymphonyElixir.PubSub,
          SymphonyElixirWeb.Endpoint
        ] do
      true = Process.whereis(name) == nil
    end

    File.write!(plan_path, original_plan)
    File.chmod!(plan_path, 0o600)
    check_capture_files.()
    send(watchdog, :cancel)
    IO.puts(Jason.encode!(%{"status" => "passed", "capture_id" => initial_report.capture_id, "run_ids" => expected_ids}))
    """

    executable = System.find_executable("elixir") || raise "elixir executable not found for cold startup rehearsal"

    code_paths = Enum.map(:code.get_path(), &List.to_string/1)

    args =
      ["--erl", "+S 1:1"] ++
        Enum.flat_map(code_paths, fn path -> ["-pa", path] end) ++ ["-e", script]

    expected_pins =
      Map.new(capture.runs, fn {run_id, run} ->
        pins = Map.new(run.definition.definitions, fn {path, resource} -> {path, resource.sha256} end)
        {run_id, pins}
      end)

    expected_runs =
      Map.new(capture.runs, fn {run_id, run} ->
        {run_id,
         %{
           "status" => Atom.to_string(run.status),
           "validation_repair_rounds" => run.validation_repair_rounds,
           "review_repair_rounds" => run.review_repair_rounds,
           "transport_retries" => run.transport_retries,
           "has_pending_wait" => not is_nil(run.pending_wait)
         }}
      end)

    System.cmd(executable, args,
      cd: File.cwd!(),
      stderr_to_stdout: true,
      env: [
        {"__BURRITO", nil},
        {"DEV242_PLAN", plan_path},
        {"DEV242_WORKFLOW", c.workflow_path},
        {"DEV242_LOGS", Path.join(c.root, "cold-start-logs")},
        {"DEV242_CAPTURE_ROOT", c.capture_root},
        {"DEV242_EXPECTED_IDS", Jason.encode!(Enum.sort(Map.keys(capture.runs)))},
        {"DEV242_EXPECTED_PINS", Jason.encode!(expected_pins)},
        {"DEV242_EXPECTED_RUNS", Jason.encode!(expected_runs)},
        {"DEV242_CAPTURE_FILES", Jason.encode!(capture_files_digest(c.capture_root, "quiet-capture"))}
      ]
    )
  end

  defp start_capture_runtime(c) do
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "CaptureRuntime#{suffix}")
    coordinator = c.coordinator
    tasks = Module.concat(__MODULE__, "CaptureTasks#{suffix}")

    opts = [
      name: supervisor,
      orchestrator_name: coordinator,
      task_supervisor_name: tasks,
      workstream_store_path: c.db,
      auto_advance: false,
      backup_capture: %{
        root: c.capture_root,
        service_revision: String.duplicate("c", 40),
        workspace_roots: [c.workspace_root],
        cleanup: :disabled,
        evidence_server: Evidence
      }
    ]

    start_isolated_runtime(opts)
  end

  defp start_isolated_runtime(opts) do
    {:ok, pid} = AgentRuntimeSupervisor.start_link(opts)
    Process.unlink(pid)

    runtime = %{
      pid: pid,
      coordinator: Keyword.fetch!(opts, :orchestrator_name),
      tasks: Keyword.fetch!(opts, :task_supervisor_name)
    }

    on_exit(fn -> stop_runtime(runtime) end)
    runtime
  end

  defp assert_recovery_start_fails(c, capture_id, manifest_sha256, policy) do
    suffix = System.unique_integer([:positive])

    opts = [
      name: Module.concat(__MODULE__, "RejectedRecoveryRuntime#{suffix}"),
      orchestrator_name: Module.concat(__MODULE__, "RejectedRecoveryCoordinator#{suffix}"),
      task_supervisor_name: Module.concat(__MODULE__, "RejectedRecoveryTasks#{suffix}"),
      recovery_capture: %{
        root: c.capture_root,
        id: capture_id,
        workspace_roots: [c.workspace_root],
        manifest_sha256: manifest_sha256,
        service_revision: String.duplicate("c", 40),
        policy: policy
      }
    ]

    task =
      Task.async(fn ->
        Process.flag(:trap_exit, true)

        case AgentRuntimeSupervisor.start_link(opts) do
          {:error, _reason} = error ->
            error

          {:ok, pid} ->
            Supervisor.stop(pid)
            :unexpected_recovery_start
        end
      end)

    assert {:error, _reason} = Task.await(task, 5_000)
  end

  defp recovery_report(coordinator) do
    assert {:ok, report} = Orchestrator.recovery_status(coordinator)
    report
  end

  defp capture_files_digest(root, id) do
    directory = Path.join(root, id)

    Map.new(["state.sqlite", "evidence.sqlite", "manifest.json"], fn filename ->
      {filename, digest_file(Path.join(directory, filename))}
    end)
  end

  defp evidence_tree_digest(root) do
    root
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.filter(&File.regular?/1)
    |> Enum.sort()
    |> Map.new(fn path -> {Path.relative_to(path, root), digest_file(path)} end)
  end

  defp pause_evidence_flush do
    :sys.replace_state(Evidence, fn state ->
      if is_reference(state.flush_timer), do: Process.cancel_timer(state.flush_timer)
      %{state | flush_timer: nil}
    end)
  end

  defp digest_file(path) do
    path
    |> File.read!()
    |> then(fn bytes -> :crypto.hash(:sha256, bytes) end)
    |> Base.encode16(case: :lower)
  end

  defp start_runtime(c, extra \\ []) do
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "Runtime#{suffix}")
    coordinator = c.coordinator
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    opts =
      [
        name: supervisor,
        orchestrator_name: coordinator,
        task_supervisor_name: tasks,
        workstream_store_path: c.db,
        auto_advance: false,
        backup_capture: %{
          root: c.capture_root,
          service_revision: String.duplicate("c", 40),
          workspace_roots: [c.workspace_root],
          cleanup: :disabled,
          evidence_server: Evidence
        }
      ] ++ extra

    pid = start_supervised!({AgentRuntimeSupervisor, opts}, id: supervisor)
    runtime = %{pid: pid, coordinator: coordinator, tasks: tasks}
    on_exit(fn -> stop_runtime(runtime) end)
    await(fn -> if is_map(Orchestrator.snapshot(coordinator, 1_000)), do: true end)
    runtime
  end

  defp stop_runtime(%{pid: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: Supervisor.stop(pid)
  catch
    :exit, _ -> :ok
  end

  defp stop_runtime(_), do: :ok

  defp fixture_run(workspace, workspace_root, workstream_path, id, task_id) do
    options = [workspace: workspace, workspace_root: workspace_root]
    assert {:ok, definition, prepared_workspace} = WorkstreamRunner.prepare(workstream_path, %{"task" => task_id}, options)
    assert {:ok, execution} = WorkstreamRunner.pin_execution_context(options, prepared_workspace, definition)

    WorkstreamRun.new(task_id, definition, prepared_workspace, options, execution)
    |> Map.put(:id, id)
  end

  defp human_wait_fixture(c) do
    fixture_run(c.waiting_workspace, c.workspace_root, c.workstream_path, "run-capture-humanwait", "task-humanwait")
    |> WorkstreamRun.start_stage()
    |> WorkstreamRun.finish_stage({:ok, %{session_id: "capture-session", thread_id: "capture-thread", candidate: %{"sha256" => String.duplicate("e", 64)}}})
    |> WorkstreamRun.start_stage()
    |> WorkstreamRun.finish_stage({:ok, %{exit_status: 0, timed_out: false, output: "fixture check passed"}})
    |> WorkstreamRun.start_stage()
  end

  defp seed_store!(path, runs) do
    assert {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())

    Enum.each(runs, fn run ->
      assert :ok = WorkstreamStore.commit(store, run, "fixture-" <> run.id, %{kind: :capture_fixture})
    end)

    assert :ok = GenServer.stop(store)
  end

  defp await_capture(coordinator, id) do
    await(fn ->
      case Orchestrator.backup_capture_status(coordinator, id) do
        {:ok, %{status: :capturing}} -> nil
        {:error, :capture_worker_failed_held} -> nil
        {:error, :capture_not_found} -> if Orchestrator.capture_held?(:sys.get_state(coordinator)), do: nil, else: {:error, :capture_not_found}
        result -> result
      end
    end)
  end

  defp webhook_conn(coordinator) do
    secret = "capture-webhook-secret"
    previous = System.get_env("FACTORY_TEST_LINEAR_SECRET")
    System.put_env("FACTORY_TEST_LINEAR_SECRET", secret)

    on_exit(fn -> restore_env("FACTORY_TEST_LINEAR_SECRET", previous) end)

    payload = %{
      "type" => "AppUserNotification",
      "action" => "issueUnassignedFromYou",
      "organizationId" => "capture-org",
      "appUserId" => "capture-app",
      "oauthClientId" => "capture-oauth",
      "webhookTimestamp" => System.system_time(:millisecond),
      "notification" => %{"type" => "issueUnassignedFromYou", "issueId" => "capture-issue"}
    }

    raw = Jason.encode!(payload)
    signature = :crypto.mac(:hmac, :sha256, secret, raw) |> Base.encode16(case: :lower)

    conn =
      Plug.Test.conn(:post, "/hooks/linear", raw)
      |> Plug.Conn.put_req_header("linear-event", "AppUserNotification")
      |> Plug.Conn.put_req_header("linear-delivery", "00000000-0000-4000-8000-000000000242")
      |> Plug.Conn.put_req_header("linear-signature", signature)
      |> Plug.Conn.assign(:linear_raw_body, raw)
      |> Plug.Conn.assign(:linear_orchestrator, coordinator)

    LinearWebhookController.create(conn, %{})
  end

  defp await_queued_capture_snapshot(store, worker, token) do
    await(fn ->
      case Process.info(store, :messages) do
        {:messages, messages} ->
          if Enum.any?(messages, fn
               {:"$gen_call", {^worker, _tag}, {:capture_snapshot, ^token}} -> true
               _ -> false
             end),
             do: :ok

        _ ->
          nil
      end
    end)
  end

  defp await_queued_store_write(store, caller) do
    await(fn ->
      case Process.info(store, :messages) do
        {:messages, messages} ->
          if Enum.any?(messages, fn
               {:"$gen_call", {^caller, _tag}, {:linear_receive, "held-delivery", _event}} -> true
               _ -> false
             end),
             do: :ok

        _ ->
          nil
      end
    end)
  end

  defp sqlite_rows(path, sql) do
    assert {:ok, db} = Sqlite3.open(path, mode: :readonly)
    assert {:ok, statement} = Sqlite3.prepare(db, sql)
    result = Sqlite3.fetch_all(db, statement)
    assert :ok = Sqlite3.release(db, statement)
    assert :ok = Sqlite3.close(db)
    assert {:ok, rows} = result
    rows
  end

  defp await(fun, tries \\ 500)
  defp await(_fun, 0), do: flunk("expected backup capture state did not appear")

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
