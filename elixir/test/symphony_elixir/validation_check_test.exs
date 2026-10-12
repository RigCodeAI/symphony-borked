defmodule SymphonyElixir.ValidationCheckTest do
  use SymphonyElixir.TestSupport
  alias SymphonyElixir.{Evidence, ServiceCheckCompletion, Validation, ValidationPolicy, ValidationCheck, WorkstreamCleanup, WorkstreamOutputs, WorkstreamRun, WorkstreamStore}

  # Authenticated synthetic contract receipts and simulated GitHub only.
  # No check, archive upload, live GitHub mutation or merge is performed here.
  setup do
    c = fixture()

    stage = %{
      id: "github-validation",
      type: :check,
      inputs: ["validation"],
      outputs: ["github-validation"],
      gate: %{evaluator: :github_validation, validation_stage: "validate", required: [%{check: "contract", assertion: "exit_status", equals: 0}], success: "merge-wait", failure: :blocked}
    }

    wait = %{id: "merge-wait", type: :human_wait, inputs: ["github-validation"], outputs: ["answer"], prompt: "Human merge", next: :complete}
    definition = %{c.run.definition | stages: Map.merge(c.run.definition.stages, %{stage.id => stage, wait.id => wait})}

    execution =
      Map.merge(c.run.execution, %{
        workspace_root: c.root,
        codex_command: "pinned",
        validation_check: %{app_id: 10, installation_id: 20, private_key_path: Path.join(c.root, "validation.pem")},
        validation_pr: 41,
        validation_check_transport: __MODULE__.Transport
      })

    run = %{c.run | definition: definition, execution: execution, status: :ready, stage_id: stage.id} |> WorkstreamRun.start_stage()

    pr = %{
      number: 41,
      url: "https://github.com/RigCodeAI/rig/pull/41",
      state: "open",
      draft: false,
      head_sha: c.candidate_sha,
      base_sha: c.base_sha,
      head_branch: "feature/manual",
      base_branch: "main",
      head_repository_full_name: "RigCodeAI/rig",
      base_repository_full_name: "RigCodeAI/rig"
    }

    {:ok, remote} = Agent.start_link(fn -> %{pr: pr, base_sha: c.base_sha, checks: [], creates: 0, updates: 0, lose_create: false, mutate_on_write: false} end)
    :persistent_term.put({__MODULE__, :remote}, remote)

    on_exit(fn ->
      :persistent_term.erase({__MODULE__, :remote})
      if Process.alive?(remote), do: Agent.stop(remote)
    end)

    {:ok, Map.merge(c, %{run: run, stage: stage, remote: remote})}
  end

  test "passing archived exact-commit evidence qualifies an ordinary PR without a publisher action", c do
    assert {:ok, %{qualified: true} = result} = ValidationCheck.reconcile(c.run, c.stage)
    assert :ok = ValidationCheck.verify_completion(c.run, c.stage, {:ok, result})
    assert {:ok, ready} = ValidationCheck.verify_ready(c.run, c.stage)
    assert ValidationCheck.same_observation?(result, ready)
    assert %{creates: 1, updates: 0} = Agent.get(c.remote, & &1)
    [check] = Agent.get(c.remote, & &1.checks)
    assert check.conclusion == "success"
    assert check.details_url =~ "/artifacts/"
    assert check.output.summary =~ c.candidate_sha
    assert WorkstreamRun.finish_stage(c.run, {:ok, result}).stage_id == "merge-wait"
  end

  test "missing, forged, failed, skipped, stale and policy-weakened evidence report failure even with high scores", c do
    for failure <- [:missing, :forged, :failed, :skipped, :stale, :policy] do
      Agent.update(c.remote, &%{&1 | checks: [], creates: 0, updates: 0})
      run = mutate_evidence(c, failure)
      assert {:ok, %{qualified: false}} = ValidationCheck.reconcile(run, c.stage)
      assert {:error, _} = ValidationCheck.verify_ready(run, c.stage)
      assert [check] = Agent.get(c.remote, & &1.checks)
      assert check.conclusion == "failure"
    end
  end

  test "an unavailable permanent archive cannot become success", c do
    File.rm_rf!(Path.join(c.root, "evidence/receipts"))
    assert {:ok, %{qualified: false}} = ValidationCheck.reconcile(c.run, c.stage)
    assert [check] = Agent.get(c.remote, & &1.checks)
    assert check.conclusion == "failure"
  end

  test "a lost accepted create response is reused on retry and after store restart", c do
    Agent.update(c.remote, &%{&1 | lose_create: true})
    assert {:uncertain, :validation_check_outcome_unknown} = ValidationCheck.reconcile(c.run, c.stage)
    path = Path.join(c.root, "checks.sqlite")
    {:ok, store} = WorkstreamStore.start_link(path: path, name: nil, owner: self())
    assert :ok = WorkstreamStore.commit(store, c.run, "check-start", %{kind: :stage_started})
    GenServer.stop(store)
    {:ok, store} = WorkstreamStore.start_link(path: path, name: nil, owner: self())
    assert {:ok, restored} = WorkstreamStore.fetch(store, c.run.id)
    assert {:ok, %{qualified: true}} = ValidationCheck.reconcile(restored, c.stage)
    GenServer.stop(store)
    assert %{creates: 1, updates: 0} = Agent.get(c.remote, & &1)
  end

  test "an out-of-band head invalidates readiness and cannot reuse the prior passing check", c do
    assert {:ok, %{qualified: true} = result} = ValidationCheck.reconcile(c.run, c.stage)
    Agent.update(c.remote, &put_in(&1.pr.head_sha, String.duplicate("a", 40)))
    assert {:error, _} = ValidationCheck.verify_ready(c.run, c.stage)
    assert {:ok, %{qualified: false, head_sha: head}} = ValidationCheck.reconcile(c.run, c.stage)
    assert head == String.duplicate("a", 40)
    assert {:error, _} = ValidationCheck.verify_completion(c.run, c.stage, {:ok, result})
  end

  test "base advancement demotes the existing check instead of creating another same-name success", c do
    assert {:ok, %{qualified: true}} = ValidationCheck.reconcile(c.run, c.stage)
    Agent.update(c.remote, fn state -> state |> Map.put(:base_sha, String.duplicate("b", 40)) |> put_in([:pr, :base_sha], String.duplicate("b", 40)) end)
    assert {:ok, %{qualified: false}} = ValidationCheck.reconcile(c.run, c.stage)
    assert %{creates: 1, updates: 1, checks: [%{conclusion: "failure"}]} = Agent.get(c.remote, & &1)
  end

  test "neutral, skipped, wrong App and changed contract checks never qualify", c do
    for change <- [:neutral, :skipped, :wrong_app, :wrong_contract] do
      Agent.update(c.remote, &%{&1 | checks: []})
      assert {:ok, _} = ValidationCheck.reconcile(c.run, c.stage)

      Agent.update(c.remote, fn state ->
        [check] = state.checks

        updated =
          case change do
            :neutral -> %{check | conclusion: "neutral"}
            :skipped -> %{check | conclusion: "skipped"}
            :wrong_app -> %{check | app_id: 999}
            :wrong_contract -> %{check | external_id: "untrusted-contract"}
          end

        %{state | checks: [updated]}
      end)

      assert {:error, _} = ValidationCheck.verify_ready(c.run, c.stage)
    end
  end

  test "remote changes during a check write cannot qualify stage completion", c do
    Agent.update(c.remote, &%{&1 | mutate_on_write: true})
    assert {:error, :remote_validation_identity_changed} = ValidationCheck.reconcile(c.run, c.stage)
    assert {:error, _} = ValidationCheck.verify_ready(c.run, c.stage)
  end

  test "revoked coordinator ownership stops writes and stopped runs cannot write", c do
    assert {:error, :revoked} = ValidationCheck.reconcile(c.run, c.stage, on_validation_check: fn -> {:error, :revoked} end)
    assert {:error, _} = ValidationCheck.reconcile(%{c.run | status: :stopped}, c.stage)
    assert %{creates: 0, updates: 0} = Agent.get(c.remote, & &1)
  end

  test "all human reply entry points require fresh validation", c do
    assert {:ok, result} = ValidationCheck.reconcile(c.run, c.stage)
    wait = WorkstreamRun.finish_stage(c.run, {:ok, result}) |> WorkstreamRun.start_stage()
    Agent.update(c.remote, &put_in(&1.pr.head_sha, String.duplicate("a", 40)))
    assert {:ok, queued} = WorkstreamRun.wait_answer(wait, wait.pending_wait.id, %{"answer" => "done"})
    assert queued.status == :waiting_for_answer
    assert queued.phase == :waiting_for_validation
    assert {:ok, native} = WorkstreamRun.linear_reply(wait, "answer #{wait.pending_wait.id}: {\"answer\":\"done\"}", "native-reply")
    assert {:ok, blocked} = ValidationCheck.reconcile(native, c.stage)
    refute blocked.qualified
    assert {:error, :current_validation_required} = WorkstreamRun.complete_validation_reply(queued, blocked)
  end

  test "App credentials cannot reuse publisher identity or key path", c do
    run = put_in(c.run, [:execution, :validation_check, :app_id], 1)
    assert {:error, :validation_credentials_not_separate} = ValidationCheck.reconcile(run, c.stage)
    run = put_in(c.run, [:execution, :validation_check, :private_key_path], c.run.execution.publisher.private_key_path)
    assert {:error, :validation_credentials_not_separate} = ValidationCheck.reconcile(run, c.stage)
  end

  test "a policy edit on the candidate branch cannot weaken the pinned contract", c do
    File.mkdir_p!(Path.join(c.workspace, "factory/policies"))
    File.write!(Path.join(c.workspace, "factory/policies/weak.yaml"), "required: []\nquality_score: 100\n")
    git!(c.workspace, ["add", "."])
    git!(c.workspace, ["commit", "--quiet", "-m", "candidate attempts policy waiver"])
    head = git!(c.workspace, ["rev-parse", "HEAD"])
    Agent.update(c.remote, &put_in(&1.pr.head_sha, head))
    assert {:ok, %{qualified: false}} = ValidationCheck.reconcile(c.run, c.stage)
    assert [check] = Agent.get(c.remote, & &1.checks)
    assert check.conclusion == "failure"
  end

  test "the coordinator persists changed-head failure during a human wait and reconciles after restart", c do
    assert {:ok, result} = ValidationCheck.reconcile(c.run, c.stage)
    waiting = WorkstreamRun.finish_stage(c.run, {:ok, result}) |> WorkstreamRun.start_stage()
    path = Path.join(c.root, "monitor.sqlite")
    {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    :ok = WorkstreamStore.commit(store, waiting, "waiting-fixture", %{kind: :fixture})
    GenServer.stop(store)
    suffix = System.unique_integer([:positive])
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    supervisor = Module.concat(__MODULE__, "Supervisor#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    pid =
      start_supervised!({SymphonyElixir.AgentRuntimeSupervisor, [name: supervisor, orchestrator_name: coordinator, task_supervisor_name: tasks, workstream_store_path: path, auto_advance: false]},
        id: supervisor
      )

    assert Process.alive?(pid)
    assert await_observation(coordinator, waiting.id, true).validation_observation.qualified
    Agent.update(c.remote, &put_in(&1.pr.head_sha, String.duplicate("a", 40)))
    send(coordinator, :reconcile_validation_checks)
    failed = await_observation(coordinator, waiting.id, false)
    assert failed.status == :waiting_for_answer
    assert failed.validation_observation.head_sha == String.duplicate("a", 40)
    assert failed.validation_observation_version == 1
    old = Process.whereis(coordinator)
    Process.exit(old, :kill)
    restored = await_observation(coordinator, waiting.id, false)
    assert restored.validation_observation_version == 1
    assert {:ok, queued} = WorkstreamRun.wait_answer(restored, restored.pending_wait.id, %{"answer" => "done"})
    assert queued.status == :waiting_for_answer
    assert %{creates: 2} = Agent.get(c.remote, & &1)
  end

  test "repeated reply reconciliation wakes retain one periodic timer", c do
    assert {:ok, result} = ValidationCheck.reconcile(c.run, c.stage)
    wait = WorkstreamRun.finish_stage(c.run, {:ok, result}) |> WorkstreamRun.start_stage()
    assert {:ok, queued} = WorkstreamRun.wait_answer(wait, wait.pending_wait.id, %{"answer" => "done"})
    Agent.update(c.remote, &Map.put(&1, :fail_reads, true))
    coordinator = start_validation_runtime(c, queued)
    assert await_observation(coordinator, queued.id, false).pending_validation_reply
    {ref, token} = timer = :sys.get_state(coordinator).workstreams.validation_check_timer
    assert is_integer(Process.read_timer(ref))

    for _ <- 1..20, do: send(coordinator, :reconcile_validation_checks)
    assert :sys.get_state(coordinator).workstreams.validation_check_timer == timer
    assert is_integer(Process.read_timer(ref))

    send(coordinator, {:validation_checks_tick, token})
    {next_ref, next_token} = next_timer = :sys.get_state(coordinator).workstreams.validation_check_timer
    refute next_ref == ref
    refute next_token == token
    assert Process.read_timer(ref) == false
    assert is_integer(Process.read_timer(next_ref))

    for _ <- 1..20, do: send(coordinator, {:validation_checks_tick, token})
    assert :sys.get_state(coordinator).workstreams.validation_check_timer == next_timer
    assert is_integer(Process.read_timer(next_ref))
  end

  defp await_observation(coordinator, id, qualified, retries \\ 200)

  defp await_observation(coordinator, id, _qualified, 0) do
    state = :sys.get_state(coordinator)
    run = state.workstreams.runs[id]
    flunk("validation observation did not converge: #{inspect(Map.take(run, [:status, :phase, :validation_observation]))} monitors=#{inspect(Map.keys(state.workstreams.validation_checks))}")
  end

  defp await_observation(coordinator, id, qualified, retries) do
    run =
      try do
        :sys.get_state(coordinator).workstreams.runs[id]
      catch
        :exit, _ -> nil
      end

    if run && get_in(run, [:validation_observation, :qualified]) == qualified do
      run
    else
      Process.sleep(10)
      await_observation(coordinator, id, qualified, retries - 1)
    end
  end

  test "the validation-only definition loads without a coding agent or publisher stage" do
    path = Path.expand("../../../factory/workstreams/pr-validation.yaml", __DIR__)
    assert {:ok, definition} = SymphonyElixir.Workstream.load(path, %{"task" => "Enrolled PR"})
    assert Enum.all?(definition.stages, fn {_id, stage} -> stage.type != :agent and stage.type != :publish end)
    assert definition.stages["github-validation"].gate.validation_stage == "validate"
  end

  test "a base change during a success write demotes the same-head check", c do
    Agent.update(c.remote, &Map.put(&1, :mutate_base_on_write, true))
    assert {:error, :remote_validation_identity_changed} = ValidationCheck.reconcile(c.run, c.stage)
    assert %{creates: 1, updates: 1, checks: [%{head_sha: head, conclusion: "failure"}]} = Agent.get(c.remote, & &1)
    assert head == c.candidate_sha
  end

  test "approval waits queue and retain their original approval identity when head or base becomes stale", c do
    for change <- [:head, :base] do
      Agent.update(c.remote, fn state -> state |> put_in([:pr, :head_sha], c.candidate_sha) |> put_in([:pr, :base_sha], c.base_sha) |> Map.put(:base_sha, c.base_sha) end)
      assert {:ok, result} = ValidationCheck.reconcile(c.run, c.stage)
      run = put_in(c.run, [:definition, :stages, "merge-wait", :approval], true)
      wait = WorkstreamRun.finish_stage(run, {:ok, result}) |> WorkstreamRun.start_stage()
      body = "approve #{wait.pending_wait.id} #{wait.pending_wait.approval_digest}"
      assert {:ok, queued} = WorkstreamRun.linear_reply(wait, body, "approval-#{change}")
      assert queued.pending_validation_reply.digest == wait.pending_wait.approval_digest

      Agent.update(c.remote, fn state ->
        if change == :head,
          do: put_in(state.pr.head_sha, String.duplicate("a", 40)),
          else: state |> Map.put(:base_sha, String.duplicate("b", 40)) |> put_in([:pr, :base_sha], String.duplicate("b", 40))
      end)

      assert {:ok, failed} = ValidationCheck.reconcile(queued, c.stage)
      refute failed.qualified
      assert {:error, :current_validation_required} = WorkstreamRun.complete_validation_reply(queued, failed)
      assert queued.status == :waiting_for_answer
    end
  end

  test "queued replies survive restart, tolerate monitor failure, and are consumed once without blocking coordinator calls", c do
    assert {:ok, result} = ValidationCheck.reconcile(c.run, c.stage)
    wait = WorkstreamRun.finish_stage(c.run, {:ok, result}) |> WorkstreamRun.start_stage()
    body = "answer #{wait.pending_wait.id}: {\"answer\":\"done\"}"
    assert {:ok, queued} = WorkstreamRun.linear_reply(wait, body, "human-reply")
    assert {:ok, ^queued} = WorkstreamRun.linear_reply(queued, body, "human-reply")
    Agent.update(c.remote, &Map.put(&1, :fail_reads, true))
    coordinator = start_validation_runtime(c, queued)
    blocked = await_observation(coordinator, queued.id, false)
    assert blocked.pending_validation_reply == queued.pending_validation_reply
    assert blocked.status == :waiting_for_answer
    Agent.update(c.remote, fn state -> state |> Map.put(:fail_reads, false) |> Map.put(:pause_reads, self()) end)
    # Agent callbacks run inside Agent; capture the test process explicitly.
    parent = self()
    Agent.update(c.remote, &Map.put(&1, :pause_reads, parent))
    assert :ok = SymphonyElixir.Orchestrator.step_workstreams(coordinator)
    assert_receive {:validation_read_blocked, reader}, 2_000
    assert is_map(GenServer.call(coordinator, :snapshot, 250))
    send(reader, :continue_validation_read)
    completed = await_run(coordinator, queued.id, &(&1.status == :complete))
    refute Map.has_key?(completed, :pending_validation_reply)
    assert completed.outputs["answer"] == "done"
    assert {:ok, ^completed} = WorkstreamRun.linear_reply(completed, body, "human-reply")
    assert %{creates: 1} = Agent.get(c.remote, & &1)
  end

  test "cancellation or changed artifacts cannot consume an already queued reply", c do
    assert {:ok, result} = ValidationCheck.reconcile(c.run, c.stage)
    wait = WorkstreamRun.finish_stage(c.run, {:ok, result}) |> WorkstreamRun.start_stage()
    assert {:ok, queued} = WorkstreamRun.wait_answer(wait, wait.pending_wait.id, %{"answer" => "done"})
    assert {:ok, current} = ValidationCheck.reconcile(queued, c.stage)
    assert {:error, _} = WorkstreamRun.complete_validation_reply(%{queued | status: :stopped}, current)
    altered = %{queued | artifacts: Map.put(queued.artifacts, "changed", %{sha256: "changed"})}
    assert {:error, _} = WorkstreamRun.complete_validation_reply(altered, current)
    assert queued.pending_validation_reply.reply == %{"answer" => "done"}
  end

  test "a registered validation operation recovers a lost create through coordinator process loss", c do
    Agent.update(c.remote, &%{&1 | lose_create: true})
    assert {:uncertain, _} = ValidationCheck.reconcile(c.run, c.stage)
    uncertain = WorkstreamRun.uncertain(c.run, :lost_check_response)
    assert :retry_safe = SymphonyElixir.Publisher.reconcile(uncertain, uncertain.operations[uncertain.current_attempt_id])
    coordinator = start_validation_runtime(c, uncertain)
    restored = await_run(coordinator, uncertain.id, &(&1.status == :ready))
    assert restored.retry_operation_id == uncertain.current_attempt_id
    assert :ok = SymphonyElixir.Orchestrator.step_workstreams(coordinator)
    finished = await_run(coordinator, uncertain.id, &(&1.status == :ready and &1.stage_id == "merge-wait"))
    assert List.last(finished.attempts).gate == :passed
    assert %{creates: 1, updates: 0} = Agent.get(c.remote, & &1)
  end

  test "a validation-only flow rejects mixed executable stages", c do
    source = Path.expand("../../../factory/workstreams/pr-validation.yaml", __DIR__)
    path = Path.join(c.root, "mixed.yaml")

    yaml =
      File.read!(source)
      |> String.replace("      evaluator: candidate_validation", "      command: [true]\n      timeout_ms: 1000")
      |> String.replace(
        "      required:\n        - check: architecture-tests\n          assertion: exit_status\n          equals: 0\n        - check: architecture-tests\n          assertion: test_count\n          equals: 1",
        ""
      )

    File.write!(path, yaml)
    assert {:error, _} = SymphonyElixir.Workstream.load(path, %{"task" => "Untrusted mixed flow"})
  end

  test "a stopped owner cannot finish remaining reads or consume a queued reply", c do
    assert {:ok, result} = ValidationCheck.reconcile(c.run, c.stage)
    wait = WorkstreamRun.finish_stage(c.run, {:ok, result}) |> WorkstreamRun.start_stage()
    assert {:ok, queued} = WorkstreamRun.wait_answer(wait, wait.pending_wait.id, %{"answer" => "done"})
    parent = self()
    Agent.update(c.remote, &Map.put(&1, :pause_reads, parent))
    coordinator = start_validation_runtime(c, queued)
    assert_receive {:validation_read_blocked, reader}, 2_000

    :sys.replace_state(coordinator, fn state ->
      stored = state.workstreams.runs[queued.id]
      put_in(state.workstreams.runs[queued.id], %{stored | status: :stopped, phase: :stopped})
    end)

    calls = Agent.get(c.remote, &Map.get(&1, :pr_reads, 0))
    send(reader, :continue_validation_read)
    stopped = await_run(coordinator, queued.id, fn run -> run.status == :stopped and map_size(:sys.get_state(coordinator).workstreams.validation_checks) == 0 end)
    assert stopped.pending_validation_reply == queued.pending_validation_reply
    assert Agent.get(c.remote, &Map.get(&1, :pr_reads, 0)) == calls
    assert %{creates: 1, updates: 0} = Agent.get(c.remote, & &1)
  end

  test "unsealed, altered and expired worker observations cannot qualify completion", c do
    assert {:ok, result} = ValidationCheck.reconcile(c.run, c.stage)

    for claim <- [Map.delete(result, :attestation), Map.put(result, :check_id, 999), Map.put(result, :observed_at, System.system_time(:second) - 31)] do
      assert {:error, _} = ValidationCheck.verify_attested_completion(c.run, c.stage, {:ok, claim})
      assert :blocked == WorkstreamRun.finish_stage(c.run, {:ok, claim}).status
      refute ServiceCheckCompletion.verified?(WorkstreamRun.finish_stage(c.run, {:ok, claim}), c.run.current_attempt_id)
    end
  end

  test "verified GitHub service completion is durable and bound to its exact attempt", c do
    assert {:ok, result} = ValidationCheck.reconcile(c.run, c.stage)
    finished = WorkstreamRun.finish_stage(c.run, {:ok, result})
    id = c.run.current_attempt_id
    assert ServiceCheckCompletion.verified?(finished, id)
    path = Path.join(c.root, "completed-check.sqlite")
    {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    :ok = WorkstreamStore.commit(store, finished, "completed-check", %{kind: :stage_completed})
    GenServer.stop(store)
    {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    assert {:ok, restored} = WorkstreamStore.fetch(store, finished.id)
    GenServer.stop(store)
    options = [cleanup_evidence_server: c.evidence_server]
    :ok = Evidence.snapshot(restored, "completed-service-output", %{kind: :stage_completed}, c.evidence_server)
    assert {:error, :retained_output_archive_unavailable} = WorkstreamCleanup.retain_outputs(restored, options)
    {:ok, [view]} = Evidence.task(Evidence.task_key(restored.task_id), c.evidence_server)
    write_upload_receipt(Path.join(c.root, "evidence"), view)
    assert {:ok, refs} = WorkstreamCleanup.retain_outputs(restored, options)
    restored = Map.put(restored, :retained_output_artifacts, refs)
    # Completion proof remains usable after merge/checkout removal, with no GitHub reads.
    Agent.update(c.remote, &Map.put(&1, :fail_reads, true))
    File.rm_rf!(c.workspace)
    assert ServiceCheckCompletion.verified?(restored, id)
    assert {:ok, %{"kind" => "github_validation", "value" => %{"qualified" => true}}} = WorkstreamCleanup.retained_output(restored, "github-validation", options)

    for altered <- [
          %{restored | task_id: "different-task"},
          put_in(restored, [:operations, id, :stage], "different-stage"),
          put_in(restored, [:operations, id, :service_completion, :mac], String.duplicate("0", 64)),
          put_in(restored, [:operations, id, :external_process], %{"kind" => "systemd-unit"}),
          %{restored | attempts: Enum.map(restored.attempts, fn a -> if a.id == id, do: %{a | gate: :failed}, else: a end)}
        ],
        do: refute(ServiceCheckCompletion.verified?(altered, id))

    File.rm!(Path.join(c.run.execution.validation_archive, "producer.key"))
    refute ServiceCheckCompletion.verified?(restored, id)
  end

  test "candidate service completion is sealed only after authentic receipt verification", c do
    receipt = hd(c.run.attempts).result.evidence["receipt"]
    stage = c.run.definition.stages["validate"]
    run = %{c.run | status: :ready, stage_id: stage.id, current_attempt_id: nil, attempts: [], operations: %{}}
    run = put_in(run.definition.stages[stage.id].gate, Map.merge(stage.gate, %{success: :complete, failure: :blocked}))
    started = WorkstreamRun.start_stage(run)
    finished = WorkstreamRun.finish_stage(started, {:ok, %{receipt: receipt, gate: %{verdict: :failed}}})
    assert finished.status == :complete
    assert ServiceCheckCompletion.verified?(finished, started.current_attempt_id)
    assert {:ok, %{"kind" => "candidate_validation"}} = WorkstreamOutputs.projection(finished, "validation")
    forged = %{receipt | "manifest_sha256" => String.duplicate("0", 64)}
    denied = WorkstreamRun.finish_stage(started, {:ok, %{receipt: forged, gate: %{verdict: :passed}}})
    refute ServiceCheckCompletion.verified?(denied, started.current_attempt_id)
    context = %{task_id: started.task_id, run_id: started.id, attempt_id: started.current_attempt_id, base_sha: c.base_sha}
    {failed_receipt, _} = write_signed_contract(c.workspace, c.run.execution.validation_archive, c.run.execution.validation_policy, context, c.base_sha, exit_status: 1)
    failed = WorkstreamRun.finish_stage(started, {:ok, %{receipt: failed_receipt}})
    assert failed.status == :blocked
    assert List.last(failed.attempts).gate == :failed
    assert ServiceCheckCompletion.verified?(failed, started.current_attempt_id)
  end

  defp start_validation_runtime(c, run) do
    suffix = System.unique_integer([:positive])
    path = Path.join(c.root, "runtime-#{suffix}.sqlite")
    {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    :ok = WorkstreamStore.commit(store, run, "runtime-fixture", %{kind: :fixture})
    GenServer.stop(store)
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    supervisor = Module.concat(__MODULE__, "Supervisor#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    start_supervised!({SymphonyElixir.AgentRuntimeSupervisor, [name: supervisor, orchestrator_name: coordinator, task_supervisor_name: tasks, workstream_store_path: path, auto_advance: false]},
      id: supervisor
    )

    coordinator
  end

  defp await_run(coordinator, id, predicate, retries \\ 200)
  defp await_run(_coordinator, _id, _predicate, 0), do: flunk("run did not converge")

  defp await_run(coordinator, id, predicate, retries) do
    run =
      try do
        :sys.get_state(coordinator).workstreams.runs[id]
      catch
        :exit, _ -> nil
      end

    if run && predicate.(run),
      do: run,
      else:
        (
          Process.sleep(10)
          await_run(coordinator, id, predicate, retries - 1)
        )
  end

  defp mutate_evidence(c, failure) do
    attempt = hd(c.run.attempts)

    case failure do
      :missing ->
        %{c.run | attempts: [%{attempt | result: %{}}]}

      :forged ->
        %{c.run | attempts: [%{attempt | result: %{evidence: %{"receipt" => %{"id" => String.duplicate("0", 64), "manifest_sha256" => String.duplicate("1", 64)}}}}]}

      :stale ->
        %{c.run | attempts: [%{attempt | id: "different-attempt"}]}

      :policy ->
        put_in(c.run, [:execution, :validation_policy, :digest], String.duplicate("0", 64))

      kind ->
        context = %{task_id: c.run.task_id, run_id: c.run.id, attempt_id: attempt.id, base_sha: c.base_sha}
        options = if kind == :failed, do: [exit_status: 1], else: [status: "incomplete"]
        {receipt, _} = write_signed_contract(c.workspace, c.run.execution.validation_archive, c.run.execution.validation_policy, context, c.base_sha, options)
        %{c.run | attempts: [%{attempt | result: %{evidence: %{"receipt" => receipt}}}]}
    end
  end

  defp fixture(opts \\ []) do
    root = Path.join(System.tmp_dir!(), "publisher-contract-#{System.unique_integer([:positive])}")
    seed = Path.join(root, "source")
    workspace = Path.join(root, "managed-clone")
    File.mkdir_p!(seed)
    git!(seed, ["init", "--quiet"])
    git!(seed, ["config", "user.name", "Candidate"])
    git!(seed, ["config", "user.email", "candidate@example.test"])
    File.mkdir_p!(Path.join(seed, ".github"))
    File.write!(Path.join(seed, ".github/pull_request_template.md"), "Review notes\n")
    File.write!(Path.join(seed, "README.md"), "base\n")
    git!(seed, ["add", "."])
    git!(seed, ["commit", "--quiet", "-m", "base"])
    base_sha = git!(seed, ["rev-parse", "HEAD"])

    git!(root, ["clone", "--quiet", "--local", seed, workspace])
    git!(workspace, ["config", "user.name", "Candidate"])
    git!(workspace, ["config", "user.email", "candidate@example.test"])
    File.write!(Path.join(workspace, "README.md"), "candidate\n")
    git!(workspace, ["add", "README.md"])
    git!(workspace, ["commit", "--quiet", "-m", "candidate"])
    candidate_sha = git!(workspace, ["rev-parse", "HEAD"])

    policy_path = Path.join(root, "validation-policy.yaml")
    File.write!(policy_path, policy_yaml())
    assert {:ok, policy} = ValidationPolicy.load(policy_path, workspace)
    validation_archive = Path.join(root, "validation-archive")
    validation_scratch = Path.join(root, "validation-scratch")
    context = %{task_id: "DEV-238", run_id: "run-publisher-fixture", attempt_id: "run-publisher-fixture/validate/1", base_sha: base_sha}
    {receipt, manifest_bytes} = write_signed_contract(workspace, validation_archive, policy, context, base_sha)

    validation_attempt = %{
      id: context.attempt_id,
      stage: "validate",
      type: :check,
      ordinal: 1,
      status: :completed,
      gate: :passed,
      gate_detail: nil,
      result: %{evidence: %{"receipt" => receipt}}
    }

    definition = %{
      name: "publisher-contract-fixture",
      inputs: ["task"],
      input_values: %{"task" => "DEV-238 publication fixture"},
      entry: "validate",
      agents: %{"worker" => %{model: "fixture", reasoning_effort: "medium", daybreak: false}},
      definitions: %{"flow.yaml" => %{sha256: digest("flow-v1"), text: "contract fixture"}},
      stages: %{
        "validate" => %{
          id: "validate",
          type: :check,
          outputs: ["validation"],
          gate: %{
            evaluator: :candidate_validation,
            required: [%{check: "contract", assertion: "exit_status", equals: 0}]
          }
        },
        "publish" => %{
          id: "publish",
          type: :publish,
          inputs: ["validation"],
          outputs: ["publication"],
          gate: %{
            validation_stage: "validate",
            required: [%{check: "contract", assertion: "exit_status", equals: 0}],
            success: :complete,
            failure: :blocked
          }
        }
      }
    }

    evidence_root = Path.join(root, "evidence")

    evidence_config = %{
      "root" => evidence_root,
      "base_url" => "https://evidence.example.test",
      "iap_audience" => "/projects/1/global/backendServices/2",
      "allowed_domains" => ["example.test"],
      "allowed_emails" => []
    }

    {:ok, evidence_server} = Evidence.start_link(config: {:ok, evidence_config}, name: nil, receipt_uid: File.stat!(root).uid)

    on_exit(fn ->
      if Process.alive?(evidence_server), do: GenServer.stop(evidence_server)
      File.rm_rf(root)
    end)

    execution = %{
      validation_policy: policy,
      validation_archive: validation_archive,
      validation_scratch: validation_scratch,
      validation_base: base_sha,
      publisher: %{app_id: 1, installation_id: 2, private_key_path: Path.join(root, "publisher.pem")},
      publisher_transport: SymphonyElixir.PublisherTest.Transport,
      evidence_server: evidence_server,
      evidence_url: "https://evidence.example.test/tasks/fixture"
    }

    run =
      WorkstreamRun.new("DEV-238", definition, workspace, [], execution)
      |> Map.merge(%{
        id: "run-publisher-fixture",
        status: :ready,
        stage_id: "publish",
        current_attempt_id: nil,
        attempts: [validation_attempt]
      })

    evidence_run = %{run | stage_id: "validate", current_attempt_id: validation_attempt.id}
    :ok = Evidence.snapshot(evidence_run, "validation-imported", %{"kind" => "validation_imported", "manifest_sha256" => receipt["manifest_sha256"]}, evidence_server)

    :ok =
      Evidence.artifact(
        %{id: run.id, stage_id: validation_attempt.stage, current_attempt_id: validation_attempt.id},
        "validation/" <> validation_attempt.id <> "/manifest.json",
        manifest_bytes,
        evidence_server
      )

    if Keyword.get(opts, :verified_upload, true) do
      {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id), evidence_server)
      write_upload_receipt(evidence_root, view)
    end

    publish_stage = definition.stages["publish"]

    %{
      root: root,
      workspace: workspace,
      base_sha: base_sha,
      candidate_sha: candidate_sha,
      run: run,
      publish_stage: publish_stage,
      validation_attempt: validation_attempt,
      evidence_server: evidence_server
    }
  end

  defp write_signed_contract(workspace, archive, policy, context, base_sha, opts \\ []) do
    File.mkdir_p!(archive)
    paths = git!(workspace, ["ls-tree", "-rz", "--name-only", "HEAD"]) |> String.split("\0", trim: true)
    changed_paths = git!(workspace, ["diff", "--no-ext-diff", "--no-renames", "--ignore-submodules=all", "--name-only", "-z", base_sha, "HEAD"]) |> String.split("\0", trim: true)
    {:ok, source_digest} = Validation.source_digest(workspace, paths)
    {:ok, checks} = ValidationPolicy.resolve(policy, changed_paths)
    candidate_sha = git!(workspace, ["rev-parse", "HEAD"])
    source_tree = git!(workspace, ["rev-parse", "HEAD^{tree}"])

    request = %{
      "candidate_sha" => candidate_sha,
      "base_sha" => base_sha,
      "source_tree" => source_tree,
      "source_digest" => source_digest,
      "changed_paths" => changed_paths,
      "version" => "candidate-validation-v1",
      "service_digest" => validation_service_digest(),
      "mode" => "final",
      "policy_digest" => policy.digest,
      "policy_revision" => policy.revision,
      "environment" => Jason.decode!(Jason.encode!(policy.environment)),
      "context" => Jason.decode!(Jason.encode!(context)),
      "check_ids" => Enum.map(checks, & &1.id)
    }

    id = digest(Jason.encode!(request))
    key_path = Path.join(archive, "producer.key")

    key =
      case File.read(key_path) do
        {:ok, existing} ->
          existing

        {:error, :enoent} ->
          generated = :crypto.strong_rand_bytes(32)
          File.write!(key_path, generated)
          File.chmod!(key_path, 0o600)
          generated
      end

    exit_status = Keyword.get(opts, :exit_status, 0)
    output = "synthetic contract check; not executed in this test\n"
    artifact = %{"file" => "check-" <> digest("contract") <> ".log", "sha256" => digest(output), "bytes" => byte_size(output)}

    check = %{
      "id" => "contract",
      "command" => ["python3", "-c", "pass"],
      "adapter" => "command-v1",
      "timeout_ms" => 5_000,
      "duration_ms" => 0,
      "status" => Keyword.get(opts, :status, "completed"),
      "reason" => "exited",
      "exit_status" => exit_status,
      "source_after" => source_digest,
      "build_digest" => nil,
      "limits" => %{},
      "assertions" => %{"exit_status" => exit_status, "completed" => true, "quality_score" => 100},
      "artifact" => artifact
    }

    manifest =
      Map.merge(request, %{
        "id" => id,
        "producer" => digest(key),
        "started_at" => "2026-10-11T00:00:00Z",
        "finished_at" => "2026-10-11T00:00:00Z",
        "complete" => true,
        "checks" => [check],
        "artifacts" => [artifact]
      })

    bytes = Jason.encode!(manifest, pretty: true)
    directory = Path.join(archive, id)
    File.mkdir_p!(directory)
    File.write!(Path.join(directory, "manifest.json"), bytes)
    File.write!(Path.join(directory, "manifest.mac"), :crypto.mac(:hmac, :sha256, key, bytes) |> Base.encode16(case: :lower))
    File.write!(Path.join(directory, artifact["file"]), output)

    {%{"id" => id, "manifest_sha256" => digest(bytes)}, bytes}
  end

  defp validation_service_digest do
    lib = Path.expand("../../lib/symphony_elixir", __DIR__)

    ["validation.ex", "validation_policy.ex", "validation_command.ex", "candidate_git.ex"]
    |> Enum.map(&File.read!(Path.join(lib, &1)))
    |> IO.iodata_to_binary()
    |> digest()
  end

  defp policy_yaml do
    """
    version: 1
    revision: synthetic-publisher-contract-v1
    environment:
      image: python@sha256:#{String.duplicate("a", 64)}
      runner_version: docker-v1
    checks:
      - id: contract
        adapter: command
        command: [python3, -c, "pass"]
        timeout_ms: 5000
        paths: [README.md]
        result_format: exit_status
    """
  end

  defp write_upload_receipt(root, view) do
    receipts = Path.join(root, "receipts")
    File.mkdir_p!(receipts)

    receipt = %{
      "status" => "verified",
      "revision" => view.revision,
      "run_id" => view.snapshot["id"],
      "manifest_sha256" => view.revision,
      "artifacts" => view.artifacts
    }

    path = Path.join(receipts, view.revision <> ".json")
    File.write!(path, Jason.encode!(receipt))
    File.chmod!(path, 0o640)
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp git!(directory, args) do
    {output, 0} = System.cmd("git", ["-C", directory | args], env: [{"GIT_CONFIG_GLOBAL", "/dev/null"}, {"GIT_CONFIG_NOSYSTEM", "1"}], stderr_to_stdout: true)
    String.trim(output)
  end
end

defmodule SymphonyElixir.ValidationCheckTest.Transport do
  def get_pr(_config, _number) do
    Agent.get_and_update(remote(), fn state -> {{:ok, state.pr}, Map.update(state, :pr_reads, 1, &(&1 + 1))} end)
  end

  def base_sha(_config), do: {:ok, Agent.get(remote(), & &1.base_sha)}

  def checks(_config, sha) do
    state = Agent.get(remote(), & &1)

    if state[:pause_reads] do
      Agent.update(remote(), &Map.delete(&1, :pause_reads))
      send(state.pause_reads, {:validation_read_blocked, self()})

      receive do
        :continue_validation_read -> :ok
      after
        5_000 -> raise "delayed fixture"
      end
    end

    if state[:fail_reads], do: {:error, {:github_request_failed, :transport}}, else: {:ok, Enum.filter(state.checks, &(&1.head_sha == sha))}
  end

  def create_check(config, payload) do
    Agent.get_and_update(remote(), fn state ->
      check = Map.merge(payload, %{id: state.creates + 1, app_id: config.app_id, details_url: payload[:details_url]})
      updated = %{state | checks: state.checks ++ [check], creates: state.creates + 1, lose_create: false}
      updated = if state.mutate_on_write, do: put_in(updated.pr.head_sha, String.duplicate("a", 40)), else: updated
      updated = if state[:mutate_base_on_write], do: updated |> Map.put(:base_sha, String.duplicate("b", 40)) |> put_in([:pr, :base_sha], String.duplicate("b", 40)), else: updated
      result = if state.lose_create, do: {:error, {:github_request_failed, :transport}}, else: {:ok, check}
      {result, updated}
    end)
  end

  def update_check(config, id, payload) do
    Agent.get_and_update(remote(), fn state ->
      check = Map.merge(payload, %{id: id, app_id: config.app_id, details_url: payload[:details_url]})
      {{:ok, check}, %{state | updates: state.updates + 1, checks: Enum.map(state.checks, fn old -> if old.id == id, do: check, else: old end)}}
    end)
  end

  defp remote, do: :persistent_term.get({SymphonyElixir.ValidationCheckTest, :remote})
end
