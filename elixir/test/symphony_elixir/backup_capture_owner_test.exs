defmodule SymphonyElixir.BackupCaptureOwnerTest do
  use SymphonyElixir.TestSupport

  alias Exqlite.Sqlite3

  alias SymphonyElixir.{
    AgentRuntimeSupervisor,
    BackupCapture,
    Evidence,
    WorkstreamRun,
    WorkstreamRunner,
    WorkstreamStore
  }

  alias SymphonyElixirWeb.LinearWebhookController

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
      if test_context[:stale_saved_policy] do
        policy = Map.put(WorkstreamRun.policy(), :lifecycle_sha256, String.duplicate("0", 64))
        {Map.put(ready_run, :policy, policy), Map.put(waiting_run, :policy, policy)}
      else
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
