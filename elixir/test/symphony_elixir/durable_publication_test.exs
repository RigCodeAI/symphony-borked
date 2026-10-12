defmodule SymphonyElixir.DurablePublicationTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{AgentRuntimeSupervisor, Orchestrator, Publisher, WorkstreamRun, WorkstreamStore}

  @remote :symphony_durable_publication_test_remote

  setup do
    root = Path.join(System.tmp_dir!(), "durable-publication-#{System.unique_integer([:positive])}")
    workspace = Path.join(root, "workspaces/candidate")
    File.mkdir_p!(workspace)

    context = %{
      root: root,
      workspace: workspace,
      db: Path.join(root, "state/runs.sqlite3"),
      options: [workspace: workspace, workspace_root: Path.join(root, "workspaces"), branch: "cycle/durable-publication"]
    }

    on_exit(fn -> File.rm_rf!(root) end)
    context
  end

  test "publication intent is durable before the executor proceeds to its write step", c do
    parent = self()

    executor = fn stage, _definition, run, opts ->
      if stage.type == :publish do
        check = opts[:on_publication_check].()
        intent = publication_intent(run)
        intent_result = opts[:on_publication_intent].(intent)

        send(parent, {:intent_persisted, self(), run.id, run.current_attempt_id, check, intent_result, intent})

        receive do
          :permit_synthetic_write ->
            send(parent, {:synthetic_remote_write, run.current_attempt_id})
            {:error, :test_finished_after_intent}
        after
          5_000 ->
            {:error, :test_executor_timeout}
        end
      else
        {:error, :unexpected_stage}
      end
    end

    run = publication_ready_run(c, "durable-intent-task")
    persist(c, run, "durable-intent-fixture")
    runtime = runtime(c, executor)

    assert :ok = Orchestrator.step_workstreams(runtime.coordinator)
    assert_receive {:intent_persisted, worker, run_id, attempt_id, :ok, :ok, intent}
    assert run_id == run.id

    state_run = coordinator_run(runtime, run.id)
    assert state_run.operations[attempt_id].publication_intent == intent

    store = :sys.get_state(runtime.coordinator).workstreams.store
    assert {:ok, stored} = WorkstreamStore.fetch(store, run.id)
    assert stored.operations[attempt_id].publication_intent == intent

    assert {:ok, %{run_id: ^run_id, event: %{kind: :publication_intent, intent: ^intent}}} =
             WorkstreamStore.event(store, attempt_id <> "/publication-intent-v1")

    refute_receive {:synthetic_remote_write, ^attempt_id}, 25
    send(worker, :permit_synthetic_write)
    assert_receive {:synthetic_remote_write, ^attempt_id}
  end

  test "a stopped publication owner cannot pass the check or replace a saved intent", c do
    run = stopped_publication_run(c, "stopped-owner-task", %{})
    saved_intent = publication_intent(run)
    run = put_in(run.operations[run.current_attempt_id][:publication_intent], saved_intent)
    persist(c, run, "stopped-owner-fixture")
    runtime = runtime(c, fn _stage, _definition, _run, _opts -> {:error, :unexpected_dispatch} end)

    worker = self()

    :sys.replace_state(runtime.coordinator, fn state ->
      put_in(state.workstreams.workers[run.current_attempt_id], %{pid: worker, run_id: run.id})
    end)

    replacement_intent = %{saved_intent | candidate_sha: String.duplicate("b", 40)}

    assert {:error, :publication_no_longer_owned} =
             GenServer.call(
               runtime.coordinator,
               {:workstream_publication_check, run.id, run.current_attempt_id, worker, run.artifacts}
             )

    assert {:error, :publication_no_longer_owned} =
             GenServer.call(
               runtime.coordinator,
               {:workstream_publication_intent, run.id, run.current_attempt_id, worker, run.artifacts, replacement_intent}
             )

    assert coordinator_run(runtime, run.id).operations[run.current_attempt_id].publication_intent == saved_intent
    store = :sys.get_state(runtime.coordinator).workstreams.store
    assert {:ok, stored} = WorkstreamStore.fetch(store, run.id)
    assert stored.operations[run.current_attempt_id].publication_intent == saved_intent
  end

  test "stopped observation records an existing pull request read-only and survives restart", c do
    {:ok, remote} = Agent.start_link(fn -> %{branch_sha: nil, pull_request: nil, reads: [], writes: []} end, name: @remote)
    on_exit(fn -> if Process.alive?(remote), do: Agent.stop(remote) end)

    execution = %{
      workspace_root: Path.join(c.root, "workspaces"),
      codex_command: "pinned",
      publisher: %{app_id: 1, installation_id: 1, private_key_path: Path.join(c.root, "publisher-key.pem")},
      publisher_transport: SymphonyElixir.DurablePublicationTest.Transport
    }

    run = stopped_publication_run(c, "stopped-observation-task", execution)
    intent = publication_intent(run)
    run = put_in(run.operations[run.current_attempt_id][:publication_intent], intent)

    pull_request = %{
      number: 73,
      url: "https://github.com/RigCodeAI/rig/pull/73",
      state: "open",
      head_sha: intent.candidate_sha,
      head_branch: intent.branch,
      base_branch: "main",
      body: "Human-authored PR text"
    }

    Agent.update(remote, &%{&1 | branch_sha: intent.candidate_sha, pull_request: pull_request})
    persist(c, run, "stopped-observation-fixture")
    runtime = runtime(c, fn _stage, _definition, _run, _opts -> {:error, :unexpected_dispatch} end)

    observed = await_observation(runtime, run.id, run.current_attempt_id)
    observation = observed.operations[run.current_attempt_id].publication_observation

    assert observed.status == :stopped
    assert observation.status == :observed
    assert observation.pr == Map.take(pull_request, [:number, :url, :head_sha, :head_branch, :base_branch, :state])
    assert Agent.get(remote, & &1.writes) == []
    reads_before_restart = Agent.get(remote, & &1.reads)
    assert reads_before_restart == [{:branch_sha, intent.branch}, {:find_pr, intent.branch}]

    restart(runtime)

    recovered = coordinator_run(runtime, run.id)
    assert recovered.status == :stopped
    assert recovered.operations[run.current_attempt_id].publication_observation == observation
    assert Agent.get(remote, & &1.reads) == reads_before_restart
    assert Agent.get(remote, & &1.writes) == []
    assert Agent.get(remote, & &1.pull_request) == pull_request
  end

  defp runtime(context, executor) do
    suffix = System.unique_integer([:positive])
    supervisor = Module.concat(__MODULE__, "Runtime#{suffix}")
    coordinator = Module.concat(__MODULE__, "Coordinator#{suffix}")
    tasks = Module.concat(__MODULE__, "Tasks#{suffix}")

    opts = [
      name: supervisor,
      orchestrator_name: coordinator,
      task_supervisor_name: tasks,
      workstream_store_path: context.db,
      stage_executor: executor,
      auto_advance: false
    ]

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

  defp publication_ready_run(context, task_id) do
    run = new_run(context, task_id, %{})
    validation_attempt = validation_attempt(run)
    artifact = %{run_id: run.id, attempt_id: validation_attempt.id, sha256: digest("validated candidate")}

    %{
      run
      | attempts: [validation_attempt],
        outputs: Map.put(run.outputs, "validation", %{"verified" => true}),
        artifacts: %{"validation" => artifact}
    }
  end

  defp stopped_publication_run(context, task_id, execution) do
    run = new_run(context, task_id, execution)
    run = %{run | attempts: [validation_attempt(run)]} |> WorkstreamRun.start_stage()

    Map.merge(run, %{status: :stopped, phase: :stopped, cancellation: %{termination: :terminated}})
    |> WorkstreamRun.confirm_termination()
  end

  defp new_run(context, task_id, execution) do
    opts = Keyword.put(context.options, :branch, "cycle/durable-publication")

    WorkstreamRun.new(
      task_id,
      publication_definition(),
      context.workspace,
      opts,
      Map.merge(
        %{
          workspace_root: Path.join(context.root, "workspaces"),
          codex_command: "pinned"
        },
        execution
      )
    )
  end

  defp validation_attempt(run) do
    %{
      id: "#{run.id}/validate/1",
      stage: "validate",
      type: :check,
      ordinal: 1,
      status: :completed,
      result: %{status: :ok, evidence: %{}},
      gate: :passed,
      gate_detail: %{"verdict" => "passed", "verified" => true}
    }
  end

  defp publication_definition do
    required = [%{check: "unit-tests", assertion: "exit_status", equals: 0}]

    %{
      name: "synthetic-publication",
      inputs: ["task"],
      input_values: %{"task" => "synthetic fixture"},
      entry: "publish",
      agents: %{},
      definitions: %{"synthetic-workstream" => %{sha256: digest("synthetic definition")}},
      stages: %{
        "validate" => %{
          id: "validate",
          type: :check,
          inputs: [],
          outputs: ["validation"],
          gate: %{evaluator: :candidate_validation, required: required, success: "publish", failure: :blocked}
        },
        "publish" => %{
          id: "publish",
          type: :publish,
          inputs: ["validation"],
          outputs: ["publication"],
          gate: %{validation_stage: "validate", required: required, success: :complete, failure: :blocked}
        }
      }
    }
  end

  defp publication_intent(run) do
    validation_attempt = Enum.find(run.attempts, &(&1.stage == "validate"))

    %{
      branch: Publisher.branch(run),
      candidate_sha: String.duplicate("a", 40),
      publication_id: digest("synthetic publication id"),
      validation_attempt_id: validation_attempt.id,
      evidence_revision: digest("synthetic evidence revision")
    }
  end

  defp persist(context, run, event_id) do
    {:ok, store} = WorkstreamStore.start_link(path: context.db, owner: self())
    assert :ok = WorkstreamStore.commit(store, run, event_id, %{kind: :synthetic_fixture})
    assert :ok = GenServer.stop(store)
  end

  defp coordinator_run(runtime, run_id) do
    await(fn ->
      case Map.get(:sys.get_state(runtime.coordinator).workstreams.runs, run_id) do
        run when is_map(run) -> run
        _ -> nil
      end
    end)
  end

  defp await_observation(runtime, run_id, attempt_id) do
    await(fn ->
      case coordinator_run(runtime, run_id) do
        %{status: :stopped} = run ->
          if match?(%{status: :observed}, run.operations[attempt_id][:publication_observation]), do: run

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

  defp digest(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end

defmodule SymphonyElixir.DurablePublicationTest.Transport do
  @remote :symphony_durable_publication_test_remote

  def branch_sha(_config, branch) do
    Agent.get_and_update(@remote, fn state ->
      {{:ok, state.branch_sha}, %{state | reads: state.reads ++ [{:branch_sha, branch}]}}
    end)
  end

  def find_pr(_config, branch) do
    Agent.get_and_update(@remote, fn state ->
      pr = if state.pull_request && state.pull_request.head_branch == branch, do: state.pull_request, else: nil
      {{:ok, pr}, %{state | reads: state.reads ++ [{:find_pr, branch}]}}
    end)
  end

  def push(_config, _workspace, _sha, _branch, _expected_sha), do: record_write(:push)
  def create_pr(_config, _branch, _title, _body), do: record_write(:create_pr)
  def update_pr(_config, _number, _body, _expected_body), do: record_write(:update_pr)

  defp record_write(operation) do
    Agent.update(@remote, &%{&1 | writes: &1.writes ++ [operation]})
    {:error, :unexpected_publication_write}
  end
end
