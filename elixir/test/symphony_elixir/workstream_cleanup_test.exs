defmodule SymphonyElixir.WorkstreamCleanupTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{AgentRuntimeSupervisor, Evidence, Orchestrator, Publisher, ServiceCheckCompletion, WorkstreamCleanup, WorkstreamOutputs, WorkstreamRun, WorkstreamStore}
  alias SymphonyElixir.WorkstreamCleanupTest.Transport

  @remote __MODULE__.Remote

  # GitHub, containment and root-upload receipts here are contract fixtures.
  # Filesystem deletion, immutable evidence, SQLite and coordinator restarts are real.
  setup do
    base = Path.join(System.tmp_dir!(), "merged-cleanup-#{System.unique_integer([:positive])}")
    root = Path.join(base, "workspaces")
    workspace = Path.join(root, "owned-task")
    evidence_root = Path.join(base, "evidence")
    File.mkdir_p!(workspace)
    File.write!(Path.join(workspace, "work.txt"), "owned work")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: root)

    config = %{"root" => evidence_root, "base_url" => "https://factory.example.test", "iap_audience" => "/projects/1/global/backendServices/2", "allowed_domains" => ["example.test"]}
    {:ok, server} = Evidence.start_link(config: {:ok, config}, receipt_uid: File.stat!(base).uid)
    {:ok, remote} = Agent.start_link(fn -> %{pr: nil, reads: []} end, name: @remote)
    c = %{base: base, root: root, workspace: workspace, evidence_root: evidence_root, server: server, db: Path.join(base, "state/runs.sqlite3")}
    enable_evidence(c)
    run = published_run(c)
    Agent.update(remote, &%{&1 | pr: merged_pr(run)})
    archive(c, run, "published")

    on_exit(fn ->
      if Process.alive?(server), do: GenServer.stop(server)
      if Process.alive?(remote), do: Agent.stop(remote)
      File.rm_rf!(base)
    end)

    Map.merge(c, %{run: run, options: [cleanup_github: Transport, cleanup_worker_reconciler: fn _, _ -> :terminated end, on_cleanup_check: fn _ -> :ok end]})
  end

  test "merge proof is read-only and includes the exact published revision and retained bytes", c do
    assert {:ok, proof} = WorkstreamCleanup.merge_proof(c.run, c.options)
    assert proof.published_head == String.duplicate("a", 40)
    assert proof.merge.commit == String.duplicate("b", 40)
    assert proof.pr.number == 73
    assert proof.archive.run_id == c.run.id
    assert File.read!(Path.join(c.workspace, "work.txt")) == "owned work"
    assert Agent.get(@remote, & &1.reads) == [73]
  end

  test "a deletion intent needs a new exact archive, rather than the publication receipt", c do
    assert {:ok, intent} = WorkstreamCleanup.prepare(c.run, c.options)
    planned = WorkstreamCleanup.plan(c.run, intent)
    :ok = Evidence.snapshot(planned, "intent", %{kind: :cleanup_intent}, c.server)
    assert {:error, :current_evidence_not_permanently_archived} = WorkstreamCleanup.delete(planned, c.options)
    assert File.dir?(c.workspace)
    view = archive(c, planned, "intent-upload")
    assert view.snapshot["cleanup"]["intent"]["resources"] != []
    assert view.snapshot["cleanup"]["intent"]["operations"] != %{}
    assert {:ok, %{archive_revision: revision}} = WorkstreamCleanup.delete(planned, c.options)
    assert revision == view.revision
    refute File.exists?(c.workspace)
    assert {:ok, bytes} = Evidence.read_artifact(Evidence.task_key(c.run.task_id), revision, "snapshot.json", c.server)
    assert bytes =~ "\"intent\""
    assert {:ok, _} = WorkstreamCleanup.delete(planned, c.options)
  end

  test "changed or corrupt current evidence retains the workspace", c do
    planned = plan(c)
    view = archive(c, planned, "intent-upload")
    assert {:error, :current_evidence_not_permanently_archived} = Evidence.retained(Map.put(planned, :extra, true))
    path = Path.join([c.evidence_root, "bundles", view.revision, "snapshot.json"])
    File.write!(path, "corrupt bytes")
    assert {:error, :current_evidence_not_permanently_archived} = WorkstreamCleanup.delete(planned, c.options)
    assert File.dir?(c.workspace)
  end

  test "open, closed-unmerged, bot and wrong-head PRs never authorize deletion", c do
    pr = merged_pr(c.run)

    for changed <- [
          %{pr | state: "open", merged: false},
          %{pr | merged: false},
          %{pr | head_sha: String.duplicate("c", 40)},
          %{pr | number: 74},
          %{pr | merged_by: %{"id" => 2, "login" => "automation[bot]", "type" => "User"}},
          %{pr | merged_by: %{"id" => 2, "login" => "app", "type" => "Bot"}},
          %{pr | merge_commit_sha: nil},
          %{pr | merged_at: nil}
        ] do
      Agent.update(@remote, &%{&1 | pr: changed})
      assert {:error, :github_merge_not_verified} = WorkstreamCleanup.prepare(c.run, c.options)
      assert File.dir?(c.workspace)
    end
  end

  test "stopped, active, arbitrary human waits and changed publication records retain data", c do
    wait = %{id: "wait", inputs: %{}, artifact_ids: %{}, prompt: "Unrelated question"}
    arbitrary = %{c.run | status: :waiting_for_answer, stage_id: "question", pending_wait: wait}
    arbitrary = put_in(arbitrary.definition.stages["question"], %{type: :human_wait, inputs: [], next: :complete})
    changed = %{c.run | attempts: Enum.map(c.run.attempts, fn a -> if a.type == :publish, do: %{a | gate: :failed}, else: a end)}

    for run <- [%{c.run | status: :stopped}, %{c.run | status: :executing}, arbitrary, changed, Map.delete(c.run, :managed_resources), %{c.run | policy: %{version: 0}}] do
      assert {:error, _} = WorkstreamCleanup.prepare(run, c.options)
      assert File.dir?(c.workspace)
    end
  end

  test "every recorded worker needs fresh containment proof, including older completed attempts", c do
    id = hd(c.run.attempts).id
    assert {:error, :worker_termination_unknown} = WorkstreamCleanup.prepare(c.run, Keyword.put(c.options, :cleanup_worker_reconciler, fn _, _ -> :unknown end))
    assert {:error, :worker_termination_unknown} = WorkstreamCleanup.prepare(c.run, Keyword.put(c.options, :cleanup_worker_reconciler, fn _, _ -> :running end))
    local = put_in(c.run, [:operations, id, :external_process], %{"kind" => "linux-process-group", "operation_id" => id})
    missing = update_in(c.run, [:operations, id], &Map.delete(&1, :external_process))
    absent = %{c.run | operations: Map.delete(c.run.operations, id)}
    for run <- [local, missing, absent], do: assert({:error, :worker_termination_unknown} = WorkstreamCleanup.prepare(run, c.options))
    assert File.dir?(c.workspace)
  end

  test "only an explicit publication wait ending the workstream can resolve through GitHub merge", c do
    wait_id = c.run.id <> "/merge-wait/3"
    wait = %{id: wait_id <> "/wait", prompt: "Human merge stays in GitHub", inputs: %{}, artifact_ids: c.run.artifacts}
    run = %{c.run | status: :waiting_for_answer, phase: :waiting_for_answer, stage_id: "merge-wait", current_attempt_id: wait_id, pending_wait: wait}
    run = put_in(run.definition.stages["merge-wait"], %{id: "merge-wait", type: :human_wait, inputs: ["publication"], next: :complete})
    run = %{run | attempts: run.attempts ++ [%{id: wait_id, stage: "merge-wait", type: :human_wait, status: :waiting}]}
    assert {:ok, intent} = WorkstreamCleanup.prepare(run, c.options)
    planned = WorkstreamCleanup.plan(run, intent)
    assert planned.status == :complete
    assert planned.pending_wait == nil
    assert List.last(planned.attempts).result == %{resolved_by: :github_merge}
    approval = put_in(run.pending_wait[:approval_digest], "candidate-approval")
    assert {:error, :cleanup_run_not_eligible} = WorkstreamCleanup.prepare(approval, c.options)
  end

  test "authenticated service completion permits cleanup but unknown agents and executables retain", c do
    run = service_completed(c)
    id = hd(run.attempts).id
    assert ServiceCheckCompletion.verified?(run, id)
    assert {:ok, _} = WorkstreamCleanup.prepare(run, c.options)

    for changed <- [
          update_in(run, [:operations, id], &Map.delete(&1, :service_completion)),
          put_in(run, [:operations, id, :service_completion, :mac], "forged"),
          put_in(run, [:definition, :stages, "validate", :gate], %{command: ["true"]}),
          put_in(run, [:operations, id, :type], :agent),
          put_in(run, [:execution, :validation_base], String.duplicate("f", 40))
        ],
        do: assert({:error, :worker_termination_unknown} = WorkstreamCleanup.prepare(changed, c.options))

    assert File.dir?(c.workspace)
  end

  test "typed metadata and command text require actual archived bytes and survive directory removal", c do
    agent = %{id: "implement", type: :agent, outputs: ["candidate"]}
    command = %{id: "inspect", type: :check, outputs: ["log"], gate: %{command: ["true"]}}
    run = c.run |> Map.put(:parent_run_id, "parent-run")
    run = add_output(run, agent, %{"thread_id" => "thread-1", "model" => "gpt-6-luna", "workspace" => c.workspace})
    text = "candidate bundle locator: .git/factory-candidates/input.bundle\n"
    # WorkerOperation's registered remote check adds the terminal exit-code enum.
    run = add_output(run, command, %{"exit_status" => 0, "output" => text, "truncated" => false, "timed_out" => false, "exit_code" => "exited"})
    archive(c, run, "typed-output-inputs")
    assert {:error, :retained_output_archive_unavailable} = WorkstreamCleanup.prepare(run, c.options)
    assert File.dir?(c.workspace)
    archive(c, run, "typed-output-upload")
    assert {:ok, intent} = WorkstreamCleanup.prepare(run, c.options)
    planned = WorkstreamCleanup.plan(run, intent)
    assert planned.retained_output_artifacts["candidate"].kind == "agent_metadata"
    archive(c, planned, "typed-output-intent")
    assert {:ok, _} = WorkstreamCleanup.delete(planned, c.options)
    refute File.exists?(c.workspace)
    assert {:ok, %{"kind" => "agent_metadata", "value" => metadata}} = WorkstreamCleanup.retained_output(planned, "candidate", c.options)
    assert metadata == %{"thread_id" => "thread-1", "model" => "gpt-6-luna"}
    assert {:ok, %{"kind" => "command_log", "value" => %{"output" => ^text}}} = WorkstreamCleanup.retained_output(planned, "log", c.options)
    forged = put_in(planned.retained_output_artifacts["candidate"].kind, "file")
    assert {:error, :parent_outputs_not_retained} = WorkstreamCleanup.retained_output(forged, "candidate", c.options)
    assert {:ok, _} = WorkstreamCleanup.merge_proof(planned, c.options)
  end

  test "a service seal never waives fresh external-worker termination", c do
    run = service_completed(c)
    id = hd(run.attempts).id
    run = put_in(run.operations[id][:external_process], %{"kind" => "systemd-unit", "operation_id" => id})
    run = ServiceCheckCompletion.record(%{run | status: :executing, current_attempt_id: id})
    run = %{run | status: :complete, current_attempt_id: nil}
    assert ServiceCheckCompletion.verified?(run, id)

    for state <- [:unknown, :running] do
      opts = Keyword.put(c.options, :cleanup_worker_reconciler, fn _, _ -> state end)
      assert {:error, :worker_termination_unknown} = WorkstreamCleanup.prepare(run, opts)
    end

    assert {:ok, _} = WorkstreamCleanup.prepare(run, c.options)
    assert File.dir?(c.workspace)
  end

  test "terminal snapshots cannot mint a service completion retrospectively", c do
    run = service_completed(c)
    id = hd(run.attempts).id
    run = update_in(run.operations[id], &Map.delete(&1, :service_completion))
    terminal = %{run | current_attempt_id: id}
    assert ServiceCheckCompletion.record(terminal) == terminal
    refute ServiceCheckCompletion.verified?(terminal, id)
  end

  test "publication and archived validation must belong to the current context", c do
    validation = hd(c.run.attempts)
    publication = List.last(c.run.attempts)

    for changed <- [
          %{c.run | current_candidate_context_id: "unknown-context"},
          %{c.run | attempts: Enum.map(c.run.attempts, fn a -> if a.id == publication.id, do: Map.put(a, :candidate_context_id, "unknown-context"), else: a end)},
          %{c.run | attempts: Enum.map(c.run.attempts, fn a -> if a.id == validation.id, do: Map.put(a, :candidate_context_id, "unknown-context"), else: a end)}
        ] do
      assert {:error, _} = WorkstreamCleanup.merge_proof(changed, c.options)
      assert {:error, _} = WorkstreamCleanup.prepare(changed, c.options)
    end

    assert File.dir?(c.workspace)
  end

  test "ordinary Cloud Agent metadata accepts only its pinned validation base", c do
    stage = %{id: "implement", type: :agent, outputs: ["candidate"]}
    base = String.duplicate("b", 40)
    run = c.run
    run = put_in(run.execution[:validation_base], base)
    value = %{"thread_id" => "thread-1", "workspace" => c.workspace, "validation_base" => base}
    valid = add_output(run, stage, value)
    assert {:ok, %{"kind" => "agent_metadata", "value" => %{"validation_base" => ^base}}} = WorkstreamOutputs.projection(valid, "candidate")

    for changed <- [Map.put(value, "validation_base", String.duplicate("f", 40)), Map.put(value, "repair_explanation", "unbound repair")] do
      assert {:error, :workspace_outputs_not_retained} = WorkstreamOutputs.projection(add_output(run, stage, changed), "candidate")
    end
  end

  test "untyped files and producer identity changes cannot be preserved as metadata", c do
    stage = %{id: "implement", type: :agent, outputs: ["candidate"]}

    for value <- [%{"thread_id" => "thread-1", "workspace" => c.workspace, "report" => "report.json"}, %{"thread_id" => "path/to/report", "workspace" => c.workspace}] do
      run = add_output(c.run, stage, value)
      assert {:error, :workspace_outputs_not_retained} = WorkstreamCleanup.prepare(run, c.options)
    end

    run = add_output(c.run, stage, %{"thread_id" => "thread-1", "workspace" => c.workspace})
    changed = put_in(run.artifacts["candidate"].sha256, String.duplicate("0", 64))
    assert {:error, :workspace_outputs_not_retained} = WorkstreamCleanup.prepare(changed, c.options)
    assert File.dir?(c.workspace)
  end

  test "local and remote command-log shapes are typed without accepting arbitrary file fields", c do
    stage = %{id: "inspect", type: :check, outputs: ["log"], gate: %{command: ["true"]}}
    value = %{"exit_status" => 0, "output" => "report.json\n", "truncated" => false, "timed_out" => false}

    for output <- [value, Map.put(value, "exit_code", "exited")] do
      run = add_output(c.run, stage, output)
      assert {:ok, %{"kind" => "command_log", "value" => ^output}} = WorkstreamOutputs.projection(run, "log")
    end

    for output <- [Map.put(value, "exit_code", "report.json"), Map.put(value, "file", "report.json")] do
      run = add_output(c.run, stage, output)
      assert {:error, :workspace_outputs_not_retained} = WorkstreamOutputs.projection(run, "log")
    end
  end

  test "delete failure remains pending and can retry without losing the owned directory", c do
    planned = plan(c)
    archive(c, planned, "delete-failure-intent")
    moved = c.workspace <> "-moved"

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: c.root,
      hook_before_remove: "mv '#{c.workspace}' '#{moved}'; mkdir '#{c.workspace}'"
    )

    enable_evidence(c)
    outcome = WorkstreamCleanup.delete(planned, c.options)
    assert outcome == {:error, :owned_resource_removal_failed}
    pending = WorkstreamCleanup.result(planned, outcome)
    assert pending.cleanup.status == :pending
    assert pending.cleanup.last_error == :owned_resource_removal_failed
    assert File.read!(Path.join(moved, "work.txt")) == "owned work"
    File.rmdir!(c.workspace)
    File.rename!(moved, c.workspace)
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: c.root)
    enable_evidence(c)
    archive(c, pending, "delete-retry-intent")
    assert {:ok, _} = WorkstreamCleanup.delete(pending, c.options)
    refute File.exists?(c.workspace)
  end

  test "a recreated path, changed root or missing live owner cannot use an archived intent", c do
    planned = plan(c)
    archive(c, planned, "intent-upload")
    assert {:error, :cleanup_requires_live_owner} = WorkstreamCleanup.delete(planned, Keyword.delete(c.options, :on_cleanup_check))
    moved = c.workspace <> "-moved"
    File.rename!(c.workspace, moved)
    File.mkdir!(c.workspace)
    File.write!(Path.join(c.workspace, "new.txt"), "different owner's work")
    assert {:error, :owned_resource_removal_failed} = WorkstreamCleanup.delete(planned, c.options)
    assert File.read!(Path.join(moved, "work.txt")) == "owned work"
    assert File.read!(Path.join(c.workspace, "new.txt")) == "different owner's work"
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: Path.join(c.base, "new-root"))
    enable_evidence(c)
    assert {:error, :owned_resource_removal_failed} = WorkstreamCleanup.delete(planned, c.options)
    assert File.dir?(c.workspace)
  end

  test "birth identity rejects a replacement even when its inode tuple is presented as matching", c do
    [resource] = c.run.managed_resources
    moved = c.workspace <> "-moved"
    File.rename!(c.workspace, moved)
    Process.sleep(2)
    File.mkdir!(c.workspace)
    File.write!(Path.join(c.workspace, "new.txt"), "unrelated work")
    assert {:ok, replacement} = Workspace.recorded_resource(c.workspace, nil)
    assert replacement.identity["target_birth"] != resource.identity["target_birth"]
    simulated_reused_inode = put_in(resource.identity["target"], replacement.identity["target"])
    assert {:error, _, ""} = Workspace.remove_owned(simulated_reused_inode)
    assert File.read!(Path.join(c.workspace, "new.txt")) == "unrelated work"
    assert File.read!(Path.join(moved, "work.txt")) == "owned work"
  end

  test "parent outputs remain readable after child cleanup and need verified immutable references", c do
    child = Map.put(c.run, :parent_run_id, "parent-run")
    assert {:error, :retained_output_archive_unavailable} = WorkstreamCleanup.prepare(child, c.options)
    child = retain_outputs(c, child)
    assert {:ok, proof} = WorkstreamCleanup.merge_proof(child, c.options)
    assert proof.retained_outputs != %{}
    forged = put_in(child.retained_output_artifacts["publication"][:output_id], %{sha256: "wrong output"})
    assert {:error, :parent_outputs_not_retained} = WorkstreamCleanup.merge_proof(forged, c.options)
    assert {:ok, intent} = WorkstreamCleanup.prepare(child, c.options)
    planned = WorkstreamCleanup.plan(child, intent)
    archive(c, planned, "child-intent")
    assert {:ok, _} = WorkstreamCleanup.delete(planned, c.options)
    refute File.exists?(c.workspace)

    for {_name, ref} <- child.retained_output_artifacts do
      assert {:ok, bytes} = Evidence.retained_artifact(ref)
      assert {:ok, value} = Jason.decode(bytes)
      assert value in Map.values(child.outputs)
    end
  end

  test "a parent output that still references a workspace file blocks cleanup", c do
    child = c.run |> Map.put(:parent_run_id, "parent-run") |> put_in([:outputs, "publication"], %{"path" => "result.log"})
    child = retain_outputs(c, child)
    assert {:error, :workspace_outputs_not_retained} = WorkstreamCleanup.prepare(child, c.options)
    assert File.dir?(c.workspace)
  end

  test "read-only merge proof explicitly distinguishes partial retained outputs", c do
    run = retain_outputs(c, Map.put(c.run, :parent_run_id, "parent-run"))
    assert {:ok, %{retained_outputs_complete: true}} = WorkstreamCleanup.merge_proof(run, c.options)
    run = %{run | artifacts: Map.put(run.artifacts, "counter", %{sha256: "fixture"}), outputs: Map.put(run.outputs, "counter", true)}
    archive(c, run, "partial-retained-output-map")
    assert {:ok, %{retained_outputs_complete: false, retained_outputs: refs}} = WorkstreamCleanup.merge_proof(run, c.options)
    assert Map.keys(refs) == ["publication"]
    assert {:error, :parent_outputs_not_retained} = WorkstreamCleanup.prepare(run, c.options)
    assert File.dir?(c.workspace)
  end

  test "top-level and unknown-key relative output paths retain data even if the JSON is archived", c do
    for value <- [%{"report" => "build/report.json"}, "report", %{"file" => "report.json"}] do
      run = %{c.run | outputs: Map.put(c.run.outputs, "report", value), artifacts: Map.put(c.run.artifacts, "report", %{sha256: "fixture"})}
      archive(c, run, "opaque-output-" <> WorkstreamCleanup.fingerprint(value))
      assert {:error, :workspace_outputs_not_retained} = WorkstreamCleanup.prepare(run, c.options)
      assert {:ok, _} = WorkstreamCleanup.merge_proof(run, c.options)
      assert File.dir?(c.workspace)
    end
  end

  test "idle periodic reconciliation detects merge, survives restart and preserves the PR", c do
    persist(c, c.run)
    runtime = runtime(c)

    pending =
      await_value(fn ->
        run = coordinator_run(runtime, c.run.id)
        if run[:cleanup][:intent], do: run
      end)

    assert pending.cleanup.intent.publication.pr.number == 73
    assert File.dir?(c.workspace)
    restart(runtime)

    cleaned =
      await_value(fn ->
        run = coordinator_run(runtime, c.run.id)
        archive(c, run, "fixture-uploader-" <> WorkstreamCleanup.fingerprint(run))
        if run[:cleanup][:status] == :cleaned, do: run
      end)

    refute File.exists?(c.workspace)
    assert cleaned.cleanup.proof.intent_id == pending.cleanup.intent.id
    assert Agent.get(@remote, & &1.pr) == merged_pr(c.run)
    assert :ok = Orchestrator.reconcile_workstreams(runtime.coordinator)
    assert coordinator_run(runtime, c.run.id) == cleaned
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(c.run.task_id))
    assert view.events != []
    assert view.snapshot["cleanup"]["status"] == "cleaned"
  end

  test "capture hold prevents cleanup scheduling and ownership overlap prevents admission", c do
    persist(c, c.run)
    runtime = runtime(c, merged_cleanup: false)

    :sys.replace_state(runtime.coordinator, fn state ->
      state = put_in(state.workstreams.opts, Keyword.put(state.workstreams.opts, :merged_cleanup, true))
      put_in(state.workstreams[:capture], %{id: "capture-held"})
    end)

    assert {:error, :capture_in_progress} = Orchestrator.reconcile_workstreams(runtime.coordinator)
    state = :sys.get_state(runtime.coordinator)
    assert state.workstreams.cleanup.jobs == %{}
    refute Orchestrator.cleanup_active_or_uncertain?(state)
    assert File.dir?(c.workspace)
    assert WorkstreamCleanup.overlaps?(c.workspace, Path.join(c.workspace, "nested"))
    assert WorkstreamCleanup.overlaps?(c.root, c.workspace)
    refute WorkstreamCleanup.overlaps?(c.workspace, c.workspace <> "-other")
  end

  test "a cleanup helper surviving coordinator restart prevents duplicate hook execution", c do
    marker = Path.join(c.base, "hook-started")
    count = Path.join(c.base, "hook-count")

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: c.root,
      hook_before_remove: "printf hook >> '#{count}'; touch '#{marker}'; sleep 0.4"
    )

    enable_evidence(c)
    persist(c, c.run)
    runtime = runtime(c)

    await_value(fn ->
      run = coordinator_run(runtime, c.run.id)
      archive(c, run, "fixture-uploader-" <> WorkstreamCleanup.fingerprint(run))
      if File.exists?(marker), do: true
    end)

    restart(runtime)
    assert Orchestrator.cleanup_active_or_uncertain?(:sys.get_state(runtime.coordinator))

    await_value(fn ->
      run = coordinator_run(runtime, c.run.id)
      archive(c, run, "fixture-uploader-" <> WorkstreamCleanup.fingerprint(run))
      if run[:cleanup][:status] == :cleaned, do: run
    end)

    assert File.read!(count) == "hook"
    refute File.exists?(c.workspace)
  end

  defmodule Transport do
    def get_pr(_config, number) do
      Agent.get_and_update(SymphonyElixir.WorkstreamCleanupTest.Remote, fn state ->
        {{:ok, state.pr}, %{state | reads: state.reads ++ [number]}}
      end)
    end
  end

  defp published_run(c) do
    definition = %{
      inputs: [],
      input_values: %{},
      entry: "publish",
      agents: %{},
      definitions: %{"flow.yaml" => %{sha256: digest("flow"), text: "contract fixture"}},
      stages: %{"validate" => %{id: "validate", type: :check}, "publish" => %{id: "publish", type: :publish, outputs: ["publication"]}}
    }

    execution = %{workspace_root: c.root, codex_command: "pinned", publisher: %{app_id: 1, installation_id: 1, private_key_path: Path.join(c.base, "publisher.pem")}}
    run = WorkstreamRun.new("cleanup-task", definition, c.workspace, [branch: "cycle/fixture"], execution)
    validation_id = run.id <> "/validate/1"
    payload = Jason.encode!(%{"synthetic_contract_fixture" => true})
    receipt = %{"id" => digest("validation id"), "manifest_sha256" => digest(payload)}
    validation = %{id: validation_id, stage: "validate", type: :check, ordinal: 1, status: :completed, gate: :passed, result: %{status: :ok, evidence: %{"receipt" => receipt}}}
    run = %{run | stage_id: "validate", current_attempt_id: validation_id, attempts: [validation]}
    import_id = "validation/" <> digest(Jason.encode!([run.id, validation_id, receipt["manifest_sha256"]]))
    :ok = Evidence.snapshot(run, import_id, %{kind: :validation_imported, attempt_id: validation_id, manifest_sha256: receipt["manifest_sha256"]})
    :ok = Evidence.artifact(run, "validation/" <> validation_id <> "/manifest.json", payload)
    view = archive(c, run, "validation-upload")
    publication_id = run.id <> "/publish/2"

    value = %{
      "candidate_sha" => String.duplicate("a", 40),
      "branch" => Publisher.branch(run),
      "publication_id" => digest("publication"),
      "evidence_revision" => view.revision,
      "validation_attempt_id" => validation_id,
      "pr" => %{"number" => 73, "url" => "https://github.com/RigCodeAI/rig/pull/73"}
    }

    intent = Map.new([:branch, :candidate_sha, :publication_id, :evidence_revision, :validation_attempt_id], &{&1, value[Atom.to_string(&1)]})
    publication = %{id: publication_id, stage: "publish", type: :publish, ordinal: 2, status: :completed, gate: :passed, result: %{status: :ok, evidence: value}}

    operations = %{
      validation_id => %{id: validation_id, stage: "validate", type: :check, status: :completed, external_process: %{"kind" => "systemd-unit", "operation_id" => validation_id}},
      publication_id => %{id: publication_id, stage: "publish", type: :publish, status: :completed, publication_intent: intent}
    }

    run = %{
      run
      | status: :complete,
        phase: :complete,
        stage_id: :complete,
        current_attempt_id: nil,
        attempts: [validation, publication],
        operations: operations,
        outputs: %{"publication" => value},
        artifacts: %{"publication" => %{run_id: run.id, attempt_id: publication_id, sha256: WorkstreamCleanup.fingerprint(value)}}
    }

    WorkstreamCleanup.enroll(run)
  end

  defp merged_pr(run),
    do: %{
      number: 73,
      url: "https://github.com/RigCodeAI/rig/pull/73",
      head_sha: String.duplicate("a", 40),
      head_branch: Publisher.branch(run),
      base_branch: "main",
      state: "closed",
      body: "Human text",
      merged: true,
      merge_commit_sha: String.duplicate("b", 40),
      merged_at: "2026-10-11T22:00:00Z",
      merged_by: %{"id" => 1, "login" => "human-owner", "type" => "User"}
    }

  defp plan(c) do
    {:ok, intent} = WorkstreamCleanup.prepare(c.run, c.options)
    WorkstreamCleanup.plan(c.run, intent)
  end

  # The coordinator-to-cleanup seam uses a sealed synthetic completion here.
  # ValidationCheckTest exercises the actual authenticated finish_stage path.
  defp service_completed(c) do
    run = c.run
    id = hd(run.attempts).id
    archive = Path.join(c.base, "validation-archive")
    File.mkdir_p!(archive)
    File.write!(Path.join(archive, "producer.key"), :crypto.strong_rand_bytes(32))
    File.chmod!(Path.join(archive, "producer.key"), 0o600)
    run = put_in(run.execution[:validation_archive], archive)
    run = put_in(run.definition.stages["validate"][:gate], %{evaluator: :candidate_validation})
    run = update_in(run.operations[id], &Map.delete(&1, :external_process))
    run = %{run | status: :executing, attempts: Enum.map(run.attempts, fn a -> if a.id == id, do: Map.put(a, :gate_detail, %{verified: true}), else: a end), current_attempt_id: id}
    run = ServiceCheckCompletion.record(run)
    %{run | status: :complete, current_attempt_id: nil}
  end

  defp add_output(run, stage, value) do
    id = run.id <> "/" <> stage.id <> "/" <> Integer.to_string(length(run.attempts) + 1)
    name = hd(stage.outputs)
    attempt = %{id: id, stage: stage.id, type: stage.type, ordinal: length(run.attempts) + 1, status: :completed, gate: :passed, result: %{status: :ok, evidence: value}}
    operation = %{id: id, stage: stage.id, type: stage.type, status: :completed, external_process: %{"kind" => "systemd-unit", "operation_id" => id}}

    run = %{
      run
      | attempts: run.attempts ++ [attempt],
        operations: Map.put(run.operations, id, operation),
        artifacts: Map.put(run.artifacts, name, %{run_id: run.id, attempt_id: id, sha256: WorkstreamCleanup.fingerprint(value)}),
        outputs: Map.put(run.outputs, name, value)
    }

    put_in(run.definition.stages[stage.id], stage)
  end

  defp archive(c, run, id) do
    :ok = Evidence.snapshot(run, id, %{kind: :contract_fixture_snapshot})
    {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id))
    directory = Path.join(c.evidence_root, "receipts")
    File.mkdir_p!(directory)
    receipt = %{"version" => 1, "status" => "verified", "run_id" => run.id, "revision" => view.revision, "manifest_sha256" => view.revision, "artifacts" => view.artifacts}
    path = Path.join(directory, view.revision <> ".json")
    File.write!(path, Jason.encode!(receipt))
    File.chmod!(path, 0o640)
    view
  end

  defp retain_outputs(c, run) do
    refs =
      Map.new(run.artifacts, fn {name, id} ->
        bytes = Jason.encode!(run.outputs[name])
        :ok = Evidence.artifact(run.id, "parent-output/" <> name, bytes)
        view = archive(c, run, "retained-output-" <> name)
        artifact = Enum.find(view.artifacts, &(&1["sha256"] == digest(bytes)))
        {name, %{run_id: run.id, task_key: Evidence.task_key(run.task_id), revision: view.revision, name: artifact["name"], sha256: artifact["sha256"], output_id: id}}
      end)

    run = Map.put(run, :retained_output_artifacts, refs)
    archive(c, run, "retained-output-refs")
    run
  end

  defp enable_evidence(c) do
    path = Workflow.workflow_file_path()
    [front, prompt] = String.split(File.read!(path), "\n---\n", parts: 2)

    evidence =
      "evidence:\n  root: #{Jason.encode!(c.evidence_root)}\n  base_url: https://factory.example.test\n  iap_audience: /projects/1/global/backendServices/2\n  allowed_domains: [example.test]\n"

    File.write!(path, front <> "\n" <> evidence <> "---\n" <> prompt)
    :ok = WorkflowStore.force_reload()
  end

  defp persist(c, run) do
    {:ok, store} = WorkstreamStore.start_link(path: c.db, owner: self())
    :ok = WorkstreamStore.commit(store, run, "fixture-run", %{kind: :queued})
    :ok = GenServer.stop(store)
  end

  defp runtime(c, extra \\ []) do
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "Runtime#{suffix}")
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    opts = [
      name: supervisor,
      orchestrator_name: coordinator,
      task_supervisor_name: tasks,
      workstream_store_path: c.db,
      auto_advance: false,
      merged_cleanup: true,
      cleanup_interval_ms: 50,
      cleanup_retry_ms: 50,
      cleanup_github: Transport,
      cleanup_worker_reconciler: fn _, _ -> :terminated end
    ]

    start_supervised!({AgentRuntimeSupervisor, Keyword.merge(opts, extra)}, id: supervisor)
    %{coordinator: coordinator}
  end

  defp coordinator_run(runtime, id), do: :sys.get_state(runtime.coordinator).workstreams.runs[id]

  defp restart(runtime) do
    old = Process.whereis(runtime.coordinator)
    ref = Process.monitor(old)
    Process.exit(old, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old, :killed}

    await_value(fn ->
      case Process.whereis(runtime.coordinator) do
        pid when is_pid(pid) and pid != old -> if GenServer.call(pid, :snapshot), do: pid
        _ -> nil
      end
    end)
  end

  defp await_value(fun, remaining \\ 250) do
    case fun.() do
      nil when remaining > 0 ->
        Process.sleep(20)
        await_value(fun, remaining - 1)

      nil ->
        flunk("cleanup did not reach the expected durable state")

      value ->
        value
    end
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
