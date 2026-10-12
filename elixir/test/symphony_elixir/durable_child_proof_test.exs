defmodule SymphonyElixir.DurableChildProofTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{AgentRuntimeSupervisor, Evidence, Orchestrator, Publisher, WorkflowStore, WorkstreamCleanup, WorkstreamRun, WorkstreamStore}

  @remote __MODULE__.Remote

  # GitHub and archive-upload receipts are synthetic contract fixtures. The
  # coordinator, SQLite store, Evidence index, archived bytes and local check are real.
  setup do
    base = Path.join(System.tmp_dir!(), "durable-child-proof-#{System.unique_integer([:positive])}")
    workspace_root = Path.join(base, "workspaces")
    parent_workspace = Path.join(workspace_root, "parent")
    child_workspace = Path.join(workspace_root, "child")
    evidence_root = Path.join(base, "evidence")
    File.mkdir_p!(parent_workspace)
    File.mkdir_p!(child_workspace)
    git_init!(parent_workspace, "cycle/fixture-parent")
    git_init!(child_workspace, "cycle/fixture-child")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

    config = %{
      "root" => evidence_root,
      "base_url" => "https://factory.example.test",
      "iap_audience" => "/projects/1/global/backendServices/2",
      "allowed_domains" => ["example.test"]
    }

    {:ok, evidence} = Evidence.start_link(config: {:ok, config}, receipt_uid: File.stat!(base).uid)
    {:ok, remote} = Agent.start_link(fn -> %{pr: nil, reads: []} end, name: @remote)

    context = %{
      base: base,
      workspace_root: workspace_root,
      parent_workspace: parent_workspace,
      child_workspace: child_workspace,
      evidence_root: evidence_root,
      db: Path.join(base, "state/runs.sqlite3"),
      evidence: evidence,
      remote: remote,
      options: [
        workspace: parent_workspace,
        child_workspace: child_workspace,
        workspace_root: workspace_root,
        codex_command: "/fixture/no-codex-command",
        cleanup_github: __MODULE__.Transport,
        child_proof_retry_ms: 50
      ]
    }

    enable_evidence(context)

    on_exit(fn ->
      if Process.alive?(evidence), do: GenServer.stop(evidence)
      if Process.alive?(remote), do: Agent.stop(remote)
      File.rm_rf!(base)
    end)

    context
  end

  test "missing child archive holds the parent; verified retained bytes resume it once", context do
    {parent, waiting_parent, linked_child, intent, child} = prepared_handoff(context)
    persist_handoff(context, parent, waiting_parent, linked_child, intent, child)
    runtime = runtime(context)

    parent_waiting =
      await_value(fn ->
        current = coordinator_run(runtime, parent.id)
        if current.status == :waiting_for_child, do: current
      end)

    child_unretained =
      await_value(fn ->
        state = :sys.get_state(runtime.coordinator)
        job = state.workstreams.child_proofs[parent_waiting.current_attempt_id]
        current = state.workstreams.runs[child.id]
        if match?(%{retry_at: _}, job) and (current[:retained_output_artifacts] || %{}) == %{}, do: current
      end)

    assert parent_waiting.stage_id == "child-change"
    assert child_unretained.status == :complete
    assert (child_unretained[:retained_output_artifacts] || %{}) == %{}
    refute Map.has_key?(parent_waiting.outputs, "child-result")
    refute Enum.any?(parent_waiting.attempts, &(&1.stage == "re-evaluate"))

    restart_coordinator(runtime)
    recovered_waiting = coordinator_run(runtime, parent.id)
    assert recovered_waiting.status == :waiting_for_child
    assert recovered_waiting.current_attempt_id == parent_waiting.current_attempt_id
    assert coordinator_run(runtime, child.id).status == :complete

    uploader = start_fixture_uploader(context.evidence_root, Evidence.task_key(child.task_id))

    try do
      retained_child =
        await_value(fn ->
          current = coordinator_run(runtime, child.id)
          if map_size(current[:retained_output_artifacts] || %{}) == map_size(current.artifacts), do: current
        end)

      assert retained_child.retained_output_artifacts["publication"].version == 2
      assert {:ok, retained_value} = WorkstreamCleanup.retained_output(retained_child, "publication")
      assert retained_value["kind"] == "publication"
      assert retained_value["value"]["pr"]["number"] == 73

      resumed =
        await_value(fn ->
          current = coordinator_run(runtime, parent.id)
          if current.status == :ready and current.stage_id == "re-evaluate", do: current
        end)

      assert resumed.current_attempt_id == nil
      assert Enum.count(resumed.attempts, &(&1.stage == "re-evaluate")) == 0
      envelope = resumed.outputs["child-result"]
      assert envelope["child_run_id"] == child.id
      assert envelope["outputs"]["publication"] == retained_value
      assert envelope["merge"]["commit"] == String.duplicate("b", 40)
      assert envelope["archive"]["run_id"] == child.id

      completion = resumed.operations[waiting_parent.current_attempt_id].completion_proof
      assert completion.retained_outputs_complete
      assert completion.outputs["publication"] == retained_value

      # Recovery must not replay the child or its publication, and auto_advance
      # stays off until the explicit local re-evaluation step below.
      restart_coordinator(runtime)
      recovered = coordinator_run(runtime, parent.id)
      assert recovered.status == :ready
      assert recovered.stage_id == "re-evaluate"
      assert recovered.attempts == resumed.attempts
      assert coordinator_run(runtime, child.id).status == :complete

      assert :ok = Orchestrator.step_workstreams(runtime.coordinator)

      completed =
        await_value(fn ->
          current = coordinator_run(runtime, parent.id)
          if current.status == :complete, do: current
        end)

      assert Enum.count(completed.attempts, &(&1.stage == "re-evaluate")) == 1
      assert completed.outputs["evaluation"]["exit_status"] == 0

      restart_coordinator(runtime)
      final = coordinator_run(runtime, parent.id)
      assert final.status == :complete
      assert Enum.count(final.attempts, &(&1.stage == "re-evaluate")) == 1
      assert coordinator_run(runtime, child.id).attempts == child.attempts
      assert Agent.get(context.remote, & &1.reads) != []
      assert {:ok, [%{archive: %{"status" => "verified"}}]} = Evidence.task(Evidence.task_key(child.task_id))
    after
      stop_fixture_uploader(uploader)
    end
  end

  defmodule Transport do
    def get_pr(_config, number) do
      Agent.get_and_update(SymphonyElixir.DurableChildProofTest.Remote, fn state ->
        {{:ok, state.pr}, %{state | reads: state.reads ++ [number]}}
      end)
    end
  end

  defp prepared_handoff(context) do
    parent = WorkstreamRun.new("fixture-parent-task", parent_definition(), context.parent_workspace, [branch: "cycle/fixture-parent"], execution(context))

    child = WorkstreamRun.new("fixture-child-task", child_definition(), context.child_workspace, [branch: "cycle/fixture-child"], execution(context))
    assert {:ok, waiting, linked, intent} = WorkstreamRun.begin_child(parent, child)
    child = published_child(context, linked)
    Agent.update(context.remote, &%{&1 | pr: merged_pr(child)})
    {parent, waiting, linked, intent, child}
  end

  defp parent_definition do
    %{
      name: "test-only-parent",
      entry: "child-change",
      inputs: ["request"],
      input_values: %{"request" => "synthetic proof contract"},
      agents: %{},
      definitions: %{},
      stages: %{
        "child-change" => %{
          id: "child-change",
          type: :child,
          inputs: ["request"],
          outputs: ["child-result"],
          input_mapping: %{"task" => "request"},
          child_definition: %{inputs: ["task"]},
          next: "re-evaluate"
        },
        "re-evaluate" => %{
          id: "re-evaluate",
          type: :check,
          inputs: ["child-result"],
          outputs: ["evaluation"],
          gate: %{command: ["true"], timeout_ms: 2_000, success: :complete, failure: :blocked},
          next: :complete
        }
      }
    }
  end

  defp child_definition do
    %{
      name: "software-change",
      entry: "validate",
      inputs: ["task"],
      input_values: %{"task" => "synthetic proof contract"},
      agents: %{},
      definitions: %{"flow.yaml" => %{sha256: digest("child-flow"), text: "synthetic test-only child"}},
      stages: %{
        "validate" => %{id: "validate", type: :check},
        "publish" => %{id: "publish", type: :publish, outputs: ["publication"]}
      }
    }
  end

  defp published_child(context, linked) do
    validation_id = linked.id <> "/validate/1"
    payload = Jason.encode!(%{"synthetic_contract_fixture" => true})
    receipt = %{"id" => digest("synthetic-validation"), "manifest_sha256" => digest(payload)}

    validation = %{
      id: validation_id,
      stage: "validate",
      type: :check,
      ordinal: 1,
      status: :completed,
      gate: :passed,
      result: %{status: :ok, evidence: %{"receipt" => receipt}}
    }

    validation_state = %{linked | status: :executing, phase: :executing, stage_id: "validate", current_attempt_id: validation_id, attempts: [validation]}
    import_id = "validation/" <> digest(Jason.encode!([linked.id, validation_id, receipt["manifest_sha256"]]))
    assert :ok = Evidence.snapshot(validation_state, import_id, %{kind: :validation_imported, attempt_id: validation_id, manifest_sha256: receipt["manifest_sha256"]})
    assert :ok = Evidence.artifact(%{id: linked.id, stage_id: "validate", current_attempt_id: validation_id}, "validation/" <> validation_id <> "/manifest.json", payload)
    validation_archive = upload_current(context.evidence_root, linked.task_id)

    publication_id = linked.id <> "/publish/2"

    publication = %{
      "candidate_sha" => String.duplicate("a", 40),
      "branch" => Publisher.branch(linked),
      "publication_id" => digest("synthetic-publication-" <> linked.id),
      "evidence_revision" => validation_archive.revision,
      "validation_attempt_id" => validation_id,
      "pr" => %{"number" => 73, "url" => "https://github.com/RigCodeAI/rig/pull/73"}
    }

    publish_attempt = %{
      id: publication_id,
      stage: "publish",
      type: :publish,
      ordinal: 2,
      status: :completed,
      result: %{status: :ok, evidence: publication},
      gate: :passed
    }

    publication_intent = Map.new([:branch, :candidate_sha, :publication_id, :evidence_revision, :validation_attempt_id], &{&1, publication[Atom.to_string(&1)]})

    %{
      linked
      | status: :complete,
        phase: :complete,
        stage_id: :complete,
        current_attempt_id: nil,
        attempts: [validation, publish_attempt],
        operations: %{
          publication_id => %{id: publication_id, stage: "publish", type: :publish, status: :completed, publication_intent: publication_intent}
        },
        outputs: %{"task" => linked.outputs["task"], "publication" => publication},
        artifacts: %{"publication" => %{run_id: linked.id, attempt_id: publication_id, sha256: WorkstreamCleanup.fingerprint(publication)}}
    }
  end

  defp persist_handoff(context, parent, waiting, linked, intent, child) do
    assert {:ok, store} = WorkstreamStore.start_link(path: context.db, owner: self())
    assert :ok = WorkstreamStore.commit(store, parent, "fixture-parent-created", %{kind: :test_fixture_parent})
    assert {:ok, ^waiting, ^linked} = WorkstreamStore.commit_child_handoff(store, parent, waiting, linked, intent)
    assert :ok = WorkstreamStore.commit(store, child, "fixture-child-published", %{kind: :test_fixture_child_published})
    assert :ok = Evidence.sync!(store)
    assert :ok = GenServer.stop(store)
  end

  defp runtime(context) do
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "Runtime#{suffix}")
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    opts = [
      name: supervisor,
      orchestrator_name: coordinator,
      task_supervisor_name: tasks,
      workstream_store_path: context.db,
      auto_advance: false,
      child_proof_retry_ms: 50,
      cleanup_github: Transport
    ]

    start_supervised!({AgentRuntimeSupervisor, opts}, id: supervisor)
    %{coordinator: coordinator, supervisor: supervisor}
  end

  defp restart_coordinator(runtime) do
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

  defp coordinator_run(runtime, id), do: :sys.get_state(runtime.coordinator).workstreams.runs[id]

  defp await_value(fun, remaining \\ 500) do
    case fun.() do
      nil when remaining > 0 ->
        Process.sleep(20)
        await_value(fun, remaining - 1)

      nil ->
        flunk("child proof runtime did not reach the expected durable state")

      value ->
        value
    end
  end

  defp start_fixture_uploader(root, task_key) do
    parent = self()
    spawn(fn -> fixture_uploader(parent, root, task_key) end)
  end

  defp fixture_uploader(parent, root, task_key) do
    receive do
      :stop -> send(parent, {:fixture_uploader_stopped, self()})
    after
      20 ->
        case Evidence.task(task_key) do
          {:ok, views} -> Enum.each(views, &upload_view(root, &1))
          _ -> :ok
        end

        fixture_uploader(parent, root, task_key)
    end
  end

  defp stop_fixture_uploader(pid) do
    monitor = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:fixture_uploader_stopped, ^pid}
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
  end

  defp upload_current(root, task_id) do
    {:ok, views} = Evidence.task(Evidence.task_key(task_id))
    [view] = views
    upload_view(root, view)
    view
  end

  defp upload_view(root, view) do
    if view.archive["status"] != "verified" do
      receipts = Path.join(root, "receipts")
      File.mkdir_p!(receipts)
      destination = Path.join(receipts, view.revision <> ".json")
      temporary = destination <> ".tmp-#{System.unique_integer([:positive])}"

      receipt = %{
        "version" => 1,
        "status" => "verified",
        "run_id" => view.snapshot["id"],
        "revision" => view.revision,
        "manifest_sha256" => view.revision,
        "artifacts" => view.artifacts
      }

      File.write!(temporary, Jason.encode!(receipt))
      File.chmod!(temporary, 0o640)
      File.rename!(temporary, destination)
    end

    :ok
  end

  defp merged_pr(run) do
    %{
      number: 73,
      url: "https://github.com/RigCodeAI/rig/pull/73",
      head_sha: String.duplicate("a", 40),
      head_branch: Publisher.branch(run),
      base_branch: "main",
      state: "closed",
      merged: true,
      merge_commit_sha: String.duplicate("b", 40),
      merged_at: "2026-10-12T12:00:00Z",
      merged_by: %{"id" => 42, "login" => "fixture-owner", "type" => "User"}
    }
  end

  defp execution(context) do
    %{
      workspace_root: context.workspace_root,
      codex_command: "/fixture/no-codex-command",
      publisher: %{app_id: 1, installation_id: 1, private_key_path: Path.join(context.base, "publisher.pem")}
    }
  end

  defp enable_evidence(context) do
    path = Workflow.workflow_file_path()
    [front, prompt] = String.split(File.read!(path), "\n---\n", parts: 2)

    config =
      "evidence:\n  root: #{Jason.encode!(context.evidence_root)}\n  base_url: https://factory.example.test\n  iap_audience: /projects/1/global/backendServices/2\n  allowed_domains: [example.test]\n"

    File.write!(path, front <> "\n" <> config <> "---\n" <> prompt)
    :ok = WorkflowStore.force_reload()
  end

  defp digest(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)

  defp git_init!(workspace, branch) do
    {_output, 0} = System.cmd("git", ["init", "-b", branch, workspace], stderr_to_stdout: true)
    :ok
  end
end
