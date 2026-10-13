Code.require_file(Path.expand("../../../factory/deploy/reconcile_stopped_local_publisher.exs", __DIR__))

defmodule SymphonyElixir.StoppedLocalPublisherTest do
  use SymphonyElixir.TestSupport
  alias SymphonyElixir.{AgentRuntimeSupervisor, WorkstreamRun, WorkstreamRunner, WorkstreamStore}
  alias SymphonyElixir.Factory.ReconcileStoppedLocalPublisher, as: Maintenance

  @old_executor "ec8ae546aeb408243529f96a1a7048c92d4a61bbe268eee6decbccfb169f4c22"
  @current_executor "30c942265ee1c5b9e40f6ff36ee6dab2bbdb11b398c11bf11aae2ae27f5177c1"
  @legacy_executor "505e0686567846995558b576a12f5e63a1edf2cfdd0e2a285e890683a9d15179"
  @definition Path.expand("../fixtures/workstream_capacity/without-wait.yaml", __DIR__)

  # Real store/coordinator admission, with synthetic host proof. These tests do
  # not qualify systemd or authorize applying maintenance on a deployed host.
  setup do
    root = Path.join(System.tmp_dir!(), "stopped-publisher-#{System.unique_integer([:positive])}")
    workspace = Path.join(root, "workspaces/task")
    File.mkdir_p!(workspace)
    {_, 0} = System.cmd("git", ["init", "--quiet", workspace])
    workflow = Path.join(root, "WORKFLOW.md")
    write_workflow_file!(workflow)
    [front, prompt] = String.split(File.read!(workflow), "\n---\n", parts: 2)

    File.write!(
      workflow,
      front <>
        """

        evidence:
          root: #{Jason.encode!(Path.join(root, "evidence"))}
          base_url: https://evidence.example.test
          iap_audience: /projects/1/global/backendServices/2
          allowed_domains: [example.test]
        ---
        #{prompt}
        """
    )

    Workflow.set_workflow_file_path(workflow)
    WorkflowStore.force_reload()
    start_supervised!({SymphonyElixir.Evidence, []})
    opts = [workspace: workspace, workspace_root: Path.join(root, "workspaces")]
    {:ok, definition, ^workspace} = WorkstreamRunner.prepare(@definition, %{"task" => "frozen task"}, opts)
    {:ok, execution} = WorkstreamRunner.pin_execution_context(opts, workspace, definition)
    queued = WorkstreamRun.new("DEV-259", definition, workspace, opts, execution)
    definition = put_in(definition.stages["publish"], %{id: "publish", type: :publish})

    original_worker =
      spawn(fn ->
        receive do
          :finish -> :ok
        end
      end)

    on_exit(fn ->
      Process.exit(original_worker, :kill)
      File.rm_rf!(root)
    end)

    stopped = WorkstreamRun.new("DEV-258", definition, workspace, opts, execution)
    stopped = %{stopped | stage_id: "publish"} |> WorkstreamRun.start_stage()
    worker_identity = %{pid: inspect(original_worker), node: "nonode@nohost", boot_id: String.duplicate("1", 32)}
    stopped = WorkstreamRun.record_worker(stopped, worker_identity)
    stopped = stopped |> WorkstreamRun.uncertain(:validation_not_permanently_archived) |> Map.merge(%{status: :stopped, phase: :stopped, cancellation: %{termination: :unknown}})
    stopped = put_in(stopped.policy.executor_sha256, @legacy_executor)

    expected = %{
      run_id: stopped.id,
      attempt_id: stopped.current_attempt_id,
      worker_boot_id: String.duplicate("1", 32),
      worker_pid: inspect(original_worker),
      machine_id: String.duplicate("a", 32),
      operator: "test"
    }

    db = Path.join(root, "runs.sqlite")

    with_store(db, fn store ->
      :ok = WorkstreamStore.commit(store, stopped, stopped.current_attempt_id <> "/uncertain", %{kind: :reconciliation_required})
      :ok = WorkstreamStore.commit(store, queued, "queue-dev259", %{kind: :queued})
    end)

    proof = fn machine ->
      if Process.alive?(original_worker), do: {:error, :original_worker_alive}, else: {:ok, %{machine_id: machine, unit: "factory-coordinator.service", cgroup_empty: true, other_uid_processes: []}}
    end

    %{root: root, db: db, stopped: stopped, queued: queued, expected: expected, worker: original_worker, proof: proof}
  end

  test "a live original publisher reserves the only slot; confirmed termination admits the same queued run", c do
    assert WorkstreamRun.policy().executor_sha256 == @current_executor
    assert Maintenance.supported_source_policy?(WorkstreamRun.policy())
    runtime = runtime(c)
    assert {:ok, %{status: :ready, attempts: []}} = Orchestrator.workstream_state(runtime.coordinator, c.queued.id)
    refute_receive {:admitted, _, _, _}, 150
    assert Process.alive?(c.worker)
    stop_supervised!(runtime.supervisor)

    with_store(c.db, fn store ->
      assert {:error, :original_worker_alive} = Maintenance.reconcile(store, c.expected, probe: c.proof, apply: true)
      assert {:ok, saved} = WorkstreamStore.fetch(store, c.stopped.id)
      assert saved.operations[c.expected.attempt_id].status == :executing
    end)

    stop_original(c.worker)

    with_store(c.db, fn store ->
      assert {:ok, %{outcome: :committed}} = Maintenance.reconcile(store, c.expected, probe: c.proof, apply: true)
      assert {:ok, saved} = WorkstreamStore.fetch(store, c.stopped.id)
      assert saved == WorkstreamRun.confirm_termination(c.stopped)
      assert saved.status == :stopped
      assert saved.policy == c.stopped.policy
      assert saved.artifacts == c.stopped.artifacts
      assert {:ok, queued} = WorkstreamStore.fetch(store, c.queued.id)
      assert queued == c.queued
    end)

    runtime = runtime(c)
    assert_receive {:admitted, run_id, "DEV-259", policy}, 2_000
    assert run_id == c.queued.id
    assert policy == c.queued.policy
    assert {:ok, %{status: :stopped, cancellation: %{termination: :terminated}}} = Orchestrator.workstream_state(runtime.coordinator, c.stopped.id)
    refute_receive {:publisher_write, _}, 100
    refute_receive {:admitted, _, _, _}, 100
  end

  test "runtime policy compatibility admits only the two exact reviewed source maps" do
    current = WorkstreamRun.policy()
    old = Map.put(current, :executor_sha256, @old_executor)

    assert Maintenance.supported_source_policy?(old)
    assert Maintenance.supported_source_policy?(current)
    refute Maintenance.supported_source_policy?(Map.put(current, :executor_sha256, String.duplicate("0", 64)))
    refute Maintenance.supported_source_policy?(Map.put(current, :lifecycle_sha256, String.duplicate("1", 64)))
    refute Maintenance.supported_source_policy?(Map.put(current, :unexpected, true))
  end

  test "duplicate offline confirmation survives store restart with one event and outbox record", c do
    stop_original(c.worker)

    first =
      with_store(c.db, fn store ->
        assert {:ok, result} = Maintenance.reconcile(store, c.expected, probe: c.proof, apply: true)
        assert {:ok, pending} = WorkstreamStore.pending_evidence(store)
        assert Enum.count(pending, &(&1.event_id == result.event_id)) == 1
        result
      end)

    with_store(c.db, fn store ->
      duplicate = Maintenance.reconcile(store, c.expected, probe: c.proof, apply: true)
      assert {:ok, %{outcome: :duplicate, event_id: event_id}} = duplicate
      assert event_id == first.event_id
      assert {:ok, pending} = WorkstreamStore.pending_evidence(store)
      assert Enum.count(pending, &(&1.event_id == event_id)) == 1
      assert {:ok, saved} = WorkstreamStore.fetch(store, c.stopped.id)
      assert saved == WorkstreamRun.confirm_termination(c.stopped)
      assert {:ok, %{event: event}} = WorkstreamStore.event(store, event_id)
      assert event["original_reconciliation"] == ":validation_not_permanently_archived"
      assert event["worker"] == Jason.decode!(Jason.encode!(c.stopped.operations[c.expected.attempt_id].worker))
    end)

    runtime = runtime(c)
    assert_receive {:admitted, id, "DEV-259", _}, 2_000
    assert id == c.queued.id
    assert :ok = Orchestrator.reconcile_workstreams(runtime.coordinator)
    refute_receive {:admitted, _, _, _}, 100
    refute_receive {:publisher_write, _}
  end

  test "remote, ambiguous, post-intent and mismatched operations remain untouched", c do
    stop_original(c.worker)

    mutations = [
      fn run -> put_in(run.operations[c.expected.attempt_id][:external_process], %{"kind" => "systemd-unit"}) end,
      fn run -> put_in(run.operations[c.expected.attempt_id][:publication_intent], %{branch: "existing"}) end,
      fn run -> put_in(run.operations[c.expected.attempt_id][:reconciliation], ":publication_outcome_unknown") end,
      fn run -> put_in(run.operations[c.expected.attempt_id].type, :agent) end,
      fn run -> put_in(run.operations[c.expected.attempt_id].worker.node, "remote@worker") end,
      fn run -> put_in(run.policy.executor_sha256, String.duplicate("0", 64)) end,
      fn run -> %{run | status: :reconciling} end,
      fn run ->
        prior = run.operations[c.expected.attempt_id] |> Map.put(:publication_intent, %{branch: "existing"})
        put_in(run.operations["prior-publish"], prior)
      end
    ]

    for {mutate, index} <- Enum.with_index(mutations) do
      with_store(c.db, fn store ->
        run = mutate.(c.stopped)
        :ok = WorkstreamStore.commit(store, run, "mutate-#{index}", %{kind: :fixture})
        {:ok, before} = WorkstreamStore.pending_evidence(store)
        assert {:error, _} = Maintenance.reconcile(store, c.expected, probe: c.proof, apply: true)
        assert {:ok, ^run} = WorkstreamStore.fetch(store, run.id)
        assert {:ok, ^before} = WorkstreamStore.pending_evidence(store)
        assert :not_found = WorkstreamStore.event(store, c.expected.attempt_id <> "/stopped-local-publisher-terminated-v1")
      end)
    end
  end

  test "dry run is read-only and a cold VM reads the event without loading this helper", c do
    stop_original(c.worker)

    result =
      with_store(c.db, fn store ->
        assert {:ok, before} = WorkstreamStore.pending_evidence(store)
        assert {:ok, %{outcome: :qualified}} = Maintenance.reconcile(store, c.expected, probe: c.proof)
        assert {:ok, saved} = WorkstreamStore.fetch(store, c.stopped.id)
        assert saved == c.stopped
        assert {:ok, ^before} = WorkstreamStore.pending_evidence(store)
        assert {:ok, result} = Maintenance.reconcile(store, c.expected, probe: c.proof, apply: true)
        result
      end)

    code = """
    {:ok, _} = Application.ensure_all_started(:exqlite)
    [db, id, event_id] = System.argv()
    {:ok, store} = SymphonyElixir.WorkstreamStore.start_link(path: db, owner: self())
    {:ok, run} = SymphonyElixir.WorkstreamStore.fetch(store, id)
    {:ok, %{event: event}} = SymphonyElixir.WorkstreamStore.event(store, event_id)
    {:ok, outbox} = SymphonyElixir.WorkstreamStore.pending_evidence(store)
    false = Code.loaded?(SymphonyElixir.Factory.ReconcileStoppedLocalPublisher)
    :stopped = run.status
    :terminated = run.cancellation.termination
    "stopped_local_publisher_termination_confirmed" = event["kind"]
    true = Enum.any?(outbox, &(&1.event_id == event_id))
    IO.puts("COLD_REPLAY_OK")
    GenServer.stop(store)
    """

    {output, status} = System.cmd("mix", ["run", "--no-start", "--no-compile", "-e", code, "--", c.db, c.stopped.id, result.event_id], env: [{"MIX_ENV", "test"}], stderr_to_stdout: true)
    assert status == 0, output
    assert output =~ "COLD_REPLAY_OK"
  end

  test "remote unknown termination continues to reserve capacity across restart", c do
    stop_original(c.worker)

    with_store(c.db, fn store ->
      original = c.stopped
      remote = put_in(original.operations[c.expected.attempt_id][:external_process], %{"kind" => "unknown-remote"})
      :ok = WorkstreamStore.commit(store, remote, "remote-fixture", %{kind: :fixture})
      assert {:error, _} = Maintenance.reconcile(store, c.expected, probe: c.proof, apply: true)
    end)

    for _ <- 1..2 do
      runtime = runtime(c)
      assert :ok = Orchestrator.reconcile_workstreams(runtime.coordinator)
      assert {:ok, %{status: :ready, attempts: []}} = Orchestrator.workstream_state(runtime.coordinator, c.queued.id)
      refute_receive {:admitted, _, _, _}, 100
      refute_receive {:publisher_write, _}
      stop_supervised!(runtime.supervisor)
    end
  end

  test "a changed identity or a failed last proof cannot confirm termination", c do
    stop_original(c.worker)

    with_store(c.db, fn store ->
      for expected <- [Map.put(c.expected, :worker_boot_id, "wrong"), Map.put(c.expected, :worker_pid, "wrong"), Map.put(c.expected, :attempt_id, "wrong")] do
        assert {:error, _} = Maintenance.reconcile(store, expected, probe: c.proof, apply: true)
      end

      Process.put(:probe_calls, 0)

      changing_proof = fn machine ->
        count = Process.get(:probe_calls) + 1
        Process.put(:probe_calls, count)
        if count == 1, do: c.proof.(machine), else: {:error, :owner_reappeared}
      end

      assert {:error, _} = Maintenance.reconcile(store, c.expected, probe: changing_proof, apply: true)
      assert {:ok, saved} = WorkstreamStore.fetch(store, c.stopped.id)
      assert saved == c.stopped
    end)
  end

  test "systemd and UID proof rejects an active service, wrong unit or any surviving old owner" do
    stopped = %{
      "Id" => "factory-coordinator.service",
      "LoadState" => "loaded",
      "ActiveState" => "inactive",
      "SubState" => "dead",
      "MainPID" => "0",
      "ControlGroup" => "",
      "User" => "factory-coordinator",
      "KillMode" => "control-group"
    }

    assert :ok = Maintenance.stopped_unit(stopped)

    for {key, value} <- [
          {"Id", "unrelated.service"},
          {"ActiveState", "active"},
          {"MainPID", "100"},
          {"ControlGroup", "/system.slice/factory-coordinator.service"},
          {"User", "root"},
          {"KillMode", "process"}
        ] do
      assert {:error, _} = Maintenance.stopped_unit(Map.put(stopped, key, value))
    end

    processes = %{1 => %{uid: 0, ppid: 0}, 100 => %{uid: 42, ppid: 1}, 101 => %{uid: 42, ppid: 100}}
    assert :ok = Maintenance.no_survivors(processes, 100, 42)
    assert {:error, :coordinator_uid_process_survives} = Maintenance.no_survivors(Map.put(processes, 99, %{uid: 42, ppid: 1}), 100, 42)
    assert {:error, _} = Maintenance.probe_host("not-this-machine")
    assert {:error, :offline_preflight_failed} = Maintenance.run_cli([])
  end

  defp with_store(db, fun) do
    {:ok, store} = WorkstreamStore.start_link(path: db, owner: self())

    try do
      fun.(store)
    after
      GenServer.stop(store)
    end
  end

  defp stop_original(worker) do
    ref = Process.monitor(worker)
    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^ref, :process, ^worker, :killed}
  end

  defp runtime(c) do
    parent = self()
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "Runtime#{suffix}")
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    executor = fn stage, _definition, run, _opts ->
      if stage.type == :publish, do: send(parent, {:publisher_write, run.id})
      send(parent, {:admitted, run.id, run.task_id, run.policy})

      receive do
        :finish -> {:ok, %{session_id: "fixture"}}
      end
    end

    names = [name: supervisor, orchestrator_name: coordinator, task_supervisor_name: tasks]
    opts = names ++ [workstream_store_path: c.db, stage_executor: executor, max_concurrent_agents: 1]
    start_supervised!({AgentRuntimeSupervisor, opts}, id: supervisor)
    %{coordinator: coordinator, supervisor: supervisor}
  end
end
