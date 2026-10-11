defmodule SymphonyElixir.EvidenceRuntimeTest do
  use SymphonyElixir.TestSupport
  alias SymphonyElixir.{AgentRuntimeSupervisor, Evidence, WorkstreamRun, WorkstreamStore}
  alias SymphonyElixir.Linear.Coordinator, as: LinearCoordinator
  alias SymphonyElixirWeb.EvidenceController

  setup do
    root = Path.join(System.tmp_dir!(), "evidence-runtime-#{System.unique_integer([:positive])}")
    workspace = Path.join(root, "workspaces/task")
    evidence_root = Path.join(root, "evidence")
    File.mkdir_p!(workspace)
    {_, 0} = System.cmd("git", ["init", "--quiet", workspace])
    path = Workflow.workflow_file_path()
    write_workflow_file!(path, workspace_root: Path.join(root, "workspaces"))
    config = "\nevidence:\n  root: #{Jason.encode!(evidence_root)}\n  base_url: https://factory.rig.ai\n  iap_audience: /projects/1/global/backendServices/2\n  allowed_domains: [rig.ai]\n"
    File.write!(path, String.replace(File.read!(path), "\n---\n", config <> "\n---\n", global: false))
    assert :ok = WorkflowStore.force_reload()
    start_supervised!({Evidence, [receipt_uid: File.stat!(root).uid]})

    on_exit(fn ->
      write_workflow_file!(path)
      File.rm_rf!(root)
    end)

    %{root: root, workspace: workspace, evidence_root: evidence_root, db: Path.join(root, "state/runs.sqlite")}
  end

  test "real durable commits, retry, wait and restart retain completed evidence", c do
    flow = write_flow(c)
    coordinator = Module.concat(__MODULE__, "Coordinator#{System.unique_integer([:positive])}")
    supervisor = Module.concat(__MODULE__, "Runtime#{System.unique_integer([:positive])}")
    tasks = Module.concat(__MODULE__, "Tasks#{System.unique_integer([:positive])}")

    executor = fn stage, definition, run, opts ->
      if stage.type == :agent do
        marker = Path.join(run.workspace, "attempted")
        if File.exists?(marker), do: File.write!(Path.join(run.workspace, "candidate"), "candidate"), else: File.write!(marker, "attempted")
        event = %{payload: %{"method" => "item/completed", "params" => %{"item" => %{"type" => "agentMessage", "text" => "Observable stage #{run.current_attempt_id}"}}}}
        :ok = Evidence.capture!(run, event)
        {:ok, %{session_id: "fixture-session", thread_id: "fixture-thread"}}
      else
        SymphonyElixir.WorkstreamRunner.execute_stage(stage, definition, run, opts)
      end
    end

    start_supervised!({AgentRuntimeSupervisor, [name: supervisor, orchestrator_name: coordinator, task_supervisor_name: tasks, workstream_store_path: c.db, stage_executor: executor]}, id: supervisor)
    opts = [workspace: c.workspace, workspace_root: Path.join(c.root, "workspaces")]
    assert {:ok, id} = Orchestrator.queue_workstream(coordinator, "fixture-queue", "fixture-task", flow, %{"task" => "fixture"}, opts)

    report =
      await(fn ->
        case Orchestrator.workstream_state(coordinator, id) do
          {:ok, %{status: :waiting_for_answer} = r} -> r
          _ -> nil
        end
      end)

    assert Enum.map(report.attempts, & &1.stage) == ["implement", "check", "implement", "check", "wait"]
    assert Enum.at(report.attempts, 1).gate == :failed
    assert Enum.at(report.attempts, 3).gate == :passed
    assert {:ok, [view]} = Evidence.task(Evidence.task_key("fixture-task"))
    html = EvidenceController.render_task([view])
    assert html =~ "Stage implement · Attempt 3"
    assert html =~ "Inspect fixture?"
    assert length(Enum.filter(view.events, &(&1["kind"] == "item/completed"))) == 2
    parent = self()

    :sys.replace_state(coordinator, fn state ->
      send(parent, {:pending_evidence, WorkstreamStore.pending_evidence(state.workstreams.store)})
      state
    end)

    assert_receive {:pending_evidence, {:ok, []}}

    old = Process.whereis(coordinator)
    Process.exit(old, :kill)

    await(fn ->
      pid = Process.whereis(coordinator)
      if is_pid(pid) and pid != old, do: pid
    end)

    assert {:ok, restored} = Orchestrator.workstream_state(coordinator, id)
    assert restored.pending_wait == report.pending_wait
    assert {:ok, [after_restart]} = Evidence.task(Evidence.task_key("fixture-task"))
    assert after_restart.revision == view.revision
    assert :ok = Orchestrator.answer_workstream(coordinator, "fixture-answer", id, restored.pending_wait.id, %{"answer" => "yes"})

    await(fn ->
      case Orchestrator.workstream_state(coordinator, id) do
        {:ok, %{status: :complete}} -> true
        _ -> nil
      end
    end)

    assert {:ok, [completed]} = Evidence.task(Evidence.task_key("fixture-task"))
    refute Evidence.cleanup_ready?(id)
    write_receipt(c, completed)
    assert Evidence.cleanup_ready?(id)
    File.rm_rf!(c.workspace)
    assert {:ok, transcript} = Evidence.read_artifact(Evidence.task_key("fixture-task"), completed.revision, "transcript.txt")
    assert transcript =~ "Observable stage"
  end

  test "committed reassignment outbox replays both original and current pins", c do
    run = fixture_run(c, "reassignment")
    assert {:ok, store} = WorkstreamStore.start_link(path: c.db, owner: self())
    assert :ok = WorkstreamStore.commit(store, run, "created", %{kind: :queued})
    changed = %{run | definition: %{run.definition | definitions: %{"flow.yaml" => %{sha256: "replacement", text: "replacement instructions"}}}}
    event = %{kind: :agent_reassigned, transition_id: "authorized-fixture", reassignment: %{previous: Evidence.identity(run), current: Evidence.identity(changed)}}
    assert :ok = WorkstreamStore.commit(store, changed, "reassigned", event)
    assert :ok = GenServer.stop(store)
    assert {:ok, reopened} = WorkstreamStore.start_link(path: c.db, owner: self())
    assert {:ok, [saved]} = WorkstreamStore.load(reopened)
    assert :ok = Evidence.recover!(reopened, [saved])
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id))
    assert hd(view.events)["snapshot"]["definitions"]["flow.yaml"] == "original"
    assert view.snapshot["definitions"]["flow.yaml"] == "replacement"
    assert :ok = Evidence.recover!(reopened, [saved])
    assert {:ok, [same]} = Evidence.task(Evidence.task_key(run.task_id))
    assert same.revision == view.revision
    assert {:ok, []} = WorkstreamStore.pending_evidence(reopened)
    assert :ok = GenServer.stop(reopened)
  end

  test "managed Linear publication retains one stable task URL", c do
    run = fixture_run(c, "linear-url")
    assert {:ok, store} = WorkstreamStore.start_link(path: c.db, owner: self())
    parent = self()
    route = %{"organization_id" => "organization-fixture", "app_user_id" => "app-fixture", "oauth_client_id" => "client-fixture", "agent_id" => "default-cloud"}
    task = %{issue_id: "issue-fixture", run_id: run.id, session_id: "session-fixture", status: :acknowledged, route: route, check_at: System.system_time(:millisecond) + 60_000}

    publisher = fn _task, activity, _config ->
      send(parent, {:published, activity})
      {:ok, %{id: activity.id}}
    end

    state = %{
      workstreams: %{store: store, runs: %{run.id => run}},
      linear: %{
        tasks: %{task.issue_id => task},
        events: %{},
        job: nil,
        timer: nil,
        config: Map.merge(route, %{"reconcile_interval_ms" => 1_000, "webhook_secret_env" => "EVIDENCE_TEST_SIGNING", "token_env" => "EVIDENCE_TEST_TOKEN"}),
        opts: [linear_publisher: publisher]
      }
    }

    state = LinearCoordinator.process(state, fn s, _ -> s end)
    assert_receive {:published, activity}
    assert activity.content["body"] =~ Evidence.url(run)
    assert activity.key == "evidence-url"
    assert {:ok, saved} = WorkstreamStore.linear_load(store)
    assert map_size(saved.tasks[task.issue_id].publications) == 1
    Process.cancel_timer(state.linear.timer)
    assert :ok = GenServer.stop(store)
  end

  test "authenticated validation bytes are retained per attempt without exposing producer key", c do
    run = fixture_run(c, "validation")
    archive = Path.join(c.root, "validation")
    key = :crypto.strong_rand_bytes(32)
    receipt_id = digest("contract receipt")
    directory = Path.join(archive, receipt_id)
    File.mkdir_p!(directory)
    output = "check passed; client_secret=fixture-credential"
    artifact = %{"file" => "check.log", "sha256" => digest(output), "bytes" => byte_size(output)}
    manifest = %{"id" => receipt_id, "producer" => digest(key), "check_ids" => ["check"], "checks" => [%{"id" => "check", "status" => "completed", "artifact" => artifact}], "artifacts" => [artifact]}
    bytes = Jason.encode!(manifest)
    File.write!(Path.join(archive, "producer.key"), key)
    File.write!(Path.join(directory, "manifest.json"), bytes)
    File.write!(Path.join(directory, "manifest.mac"), :crypto.mac(:hmac, :sha256, key, bytes) |> Base.encode16(case: :lower))
    File.write!(Path.join(directory, "check.log"), output)
    receipt = %{"id" => receipt_id, "manifest_sha256" => digest(bytes)}
    attempt = %{id: "validation/check/1", stage: "check", result: %{evidence: %{"receipt" => receipt}}}
    run = %{run | execution: %{validation_archive: archive}, attempts: [attempt]}
    assert :ok = Evidence.snapshot(run, "fixture", %{kind: :queued})
    assert :ok = Evidence.retain_validation!(run)
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id))
    log = Enum.find(view.events, &(&1["label"] == "validation/validation/check/1/check.log"))
    assert log["attempt_id"] == attempt.id
    assert log["source_sha256"] == digest(output)
    assert {:ok, stored} = Evidence.read_artifact(Evidence.task_key(run.task_id), view.revision, log["name"])
    assert stored =~ "check passed"
    refute stored =~ "fixture-credential"
    refute Jason.encode!(view) =~ Base.encode64(key)
    File.rm_rf!(archive)
    assert :ok = Evidence.retain_validation!(run)
  end

  defp fixture_run(c, id) do
    definition = %{inputs: [], input_values: %{}, entry: "implement", agents: %{}, stages: %{}, definitions: %{"flow.yaml" => %{sha256: "original", text: "original instructions"}}}
    WorkstreamRun.new(id, definition, c.workspace, [branch: "cycle/fixture"], %{}) |> Map.put(:id, id)
  end

  defp write_receipt(c, view) do
    directory = Path.join(c.evidence_root, "receipts")
    File.mkdir_p!(directory)
    payload = %{"status" => "verified", "revision" => view.revision, "run_id" => view.snapshot["id"], "manifest_sha256" => view.revision, "artifacts" => view.artifacts}
    path = Path.join(directory, view.revision <> ".json")
    File.write!(path, Jason.encode!(payload))
    File.chmod!(path, 0o640)
  end

  defp await(fun, tries \\ 200)
  defp await(_fun, 0), do: flunk("observable state did not converge")

  defp await(fun, tries) do
    case fun.() do
      value when value not in [nil, false] ->
        value

      _ ->
        Process.sleep(10)
        await(fun, tries - 1)
    end
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp write_flow(c) do
    File.write!(Path.join(c.root, "instructions.md"), "Fixture instructions")

    File.write!(
      Path.join(c.root, "agent.yaml"),
      "version: 1\nname: fixture\nmodel: gpt-6-luna\nreasoning_effort: medium\ndaybreak: false\napproval_policy: never\nsandbox: workspace-write\ninstructions: [instructions.md]\nskills: []\n"
    )

    path = Path.join(c.root, "flow.yaml")

    File.write!(path, """
    version: 1
    name: evidence-fixture
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
        next: check
      - id: check
        type: check
        inputs: [candidate]
        outputs: [check]
        gate:
          command: [sh, -c, "test -f candidate"]
          timeout_ms: 1000
          success: wait
          failure:
            repair: implement
            max_attempts: 1
      - id: wait
        type: human_wait
        inputs: [candidate, check]
        outputs: [answer]
        prompt: Inspect fixture?
        next: complete
    """)

    path
  end
end
