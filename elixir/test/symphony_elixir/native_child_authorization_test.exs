defmodule SymphonyElixir.NativeChildAuthorizationTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Linear.{AgentMapping, Coordinator}
  alias SymphonyElixir.{WorkstreamChild, WorkstreamRun, WorkstreamStore}

  setup do
    root = Path.join(System.tmp_dir!(), "native-child-authorization-#{System.unique_integer([:positive])}")
    workspace_root = Path.join(root, "workspaces")
    parent_workspace = Path.join(workspace_root, "parent")
    child_workspace = Path.join(workspace_root, "child")
    File.mkdir_p!(workspace_root)
    init_repo!(parent_workspace)
    init_repo!(child_workspace)

    store_path = Path.join([root, "private", "state.sqlite3"])
    File.mkdir_p!(Path.dirname(store_path))
    assert {:ok, store} = WorkstreamStore.start_link(path: store_path, owner: self())

    config = config(root, store_path, workspace_root, parent_workspace, child_workspace)
    {parent, child_definition, child_execution} = parent_run(parent_workspace, child_workspace, workspace_root, config)
    assert :ok = WorkstreamStore.commit(store, parent, "native-child-parent-created", %{kind: :queued})

    event = event(:created, "native-child-created", route(config), 100)
    task = native_task(event, parent, config)
    assert :ok = WorkstreamStore.linear_task(store, task)

    opts = linear_opts(self(), "human-owner")
    state = state(store, config, opts, task, parent)

    on_exit(fn ->
      if Process.alive?(store), do: GenServer.stop(store)
      File.rm_rf!(root)
    end)

    %{
      root: root,
      workspace_root: workspace_root,
      parent_workspace: parent_workspace,
      child_workspace: child_workspace,
      store: store,
      store_path: store_path,
      config: config,
      parent: parent,
      child_definition: child_definition,
      child_execution: child_execution,
      task: task,
      event: event,
      opts: opts,
      state: state
    }
  end

  test "a fresh native check authorizes only the exact prepared child", c do
    state = refresh(c.state)
    {_, linked_child, intent} = planned_handoff(c.parent)

    assert Coordinator.child_authorized?(state, c.parent, linked_child)
    assert Coordinator.ready?(state, linked_child)
    assert intent.child_run_id == linked_child.id

    refute Coordinator.ready?(state, %{linked_child | id: "run-unmatched"})
    refute Coordinator.ready?(state, %{linked_child | parent_run_id: "run-unmatched"})

    mismatched_lineage = put_in(linked_child.parent_handoff.fingerprint, String.duplicate("0", 64))
    refute Coordinator.child_authorized?(state, c.parent, mismatched_lineage)

    orphan = linked_child |> Map.drop([:parent_handoff, :parent_run_id]) |> Map.put(:task_id, "child/unmatched")
    refute Coordinator.ready?(state, orphan)

    forged_linear_child = orphan |> Map.put(:task_id, "linear/issue-1") |> Map.put(:id, "run-unmatched")
    refute Coordinator.ready?(state, forged_linear_child)

    {waiting, child, _} = planned_handoff(c.parent)
    linked_state = put_in(state.workstreams.runs, %{waiting.id => waiting, child.id => child})
    assert Coordinator.ready?(linked_state, child)
    changed_context = %{child | current_candidate_context_id: "unqualified-context"}
    refute Coordinator.ready?(linked_state, changed_context)
    later_attempt = %{child | attempts: [%{id: "previous-attempt"}]}
    refute Coordinator.ready?(linked_state, later_attempt)
  end

  test "the owner callback commits the parent wait and child before child dispatch", c do
    state = refresh(c.state)
    {_, expected_child, _intent} = planned_handoff(c.parent)
    owner = self()

    owner_state =
      owner_state(state, c.store, c.opts, fn stage, _definition, run, _opts ->
        send(owner, {:native_child_stage, run.id, stage.id})

        receive do
          :finish_native_child -> {:ok, %{session_id: "child-session", thread_id: "child-thread", result: "child stage ran"}}
        end
      end)

    {:reply, :ok, handed_off} = Orchestrator.handle_call(:step_workstreams, {self(), make_ref()}, owner_state)
    waiting_parent = handed_off.workstreams.runs[c.parent.id]
    child = handed_off.workstreams.runs[waiting_parent.child_handoff.child_run_id]

    assert waiting_parent.status == :waiting_for_child
    assert child.status == :ready
    assert child.id == expected_child.id
    assert {:ok, ^waiting_parent} = WorkstreamStore.fetch(c.store, waiting_parent.id)
    assert {:ok, ^child} = WorkstreamStore.fetch(c.store, child.id)
    assert Coordinator.child_authorized?(handed_off, waiting_parent, child)

    handed_off = refresh(force_check(handed_off, c.store))
    waiting_parent = handed_off.workstreams.runs[c.parent.id]
    child = handed_off.workstreams.runs[child.id]
    assert Coordinator.child_authorized?(handed_off, waiting_parent, child)
    assert Coordinator.ready?(handed_off, child)

    changed_child = %{child | stage_id: "followup"}
    changed_state = put_in(handed_off.workstreams.runs[child.id], changed_child)
    refute Coordinator.ready?(changed_state, changed_child)

    refreshed = refresh(force_check(changed_state, c.store))
    changed_child = refreshed.workstreams.runs[child.id]
    assert Coordinator.ready?(refreshed, changed_child)

    {:reply, :ok, dispatched} = Orchestrator.handle_call(:step_workstreams, {self(), make_ref()}, refreshed)
    assert_receive {:native_child_stage, child_id, "followup"}, 5_000
    assert child_id == child.id
    assert dispatched.workstreams.runs[c.parent.id].status == :waiting_for_child
    assert dispatched.workstreams.runs[child.id].status == :executing
    assert map_size(dispatched.workstreams.runs) == 2

    attempt_id = dispatched.workstreams.runs[child.id].current_attempt_id
    worker = dispatched.workstreams.workers[attempt_id].pid
    {:reply, :ok, stepped_again} = Orchestrator.handle_call(:step_workstreams, {self(), make_ref()}, dispatched)
    refute_receive {:native_child_stage, _, _}, 50
    assert map_size(stepped_again.workstreams.runs) == 2
    assert map_size(stepped_again.workstreams.workers) == 1
    assert stepped_again.workstreams.workers[attempt_id].pid == worker
    send(worker, :finish_native_child)
  end

  test "expiry, restart, issue update, and reassignment revoke the child grant", c do
    authorized = refresh(c.state)
    {_, child, _intent} = planned_handoff(c.parent)
    assert Coordinator.child_authorized?(authorized, c.parent, child)

    expired_task = Map.update!(authorized.linear.tasks[c.task.issue_id], :owned_child_grant, &Map.put(&1, :until, 0))
    expired = put_in(authorized.linear.tasks[c.task.issue_id], expired_task)
    refute Coordinator.child_authorized?(expired, c.parent, child)

    assert {:ok, restarted} = Coordinator.init(Map.put(authorized, :linear, nil), [linear_delegation: c.config] ++ c.opts)
    assert restarted.linear.tasks[c.task.issue_id].owned_child_grant == nil
    refute Coordinator.child_authorized?(restarted, c.parent, child)

    update = event(:issue_updated, "native-child-update", route(c.config), 200)
    assert {:ok, updated, :ok} = Coordinator.receive_event(authorized, update, fn state, _run -> state end)
    assert updated.linear.tasks[c.task.issue_id].owned_child_grant == nil
    refute Coordinator.child_authorized?(updated, c.parent, child)

    high_route = route(c.config, "high-effort-cloud")
    reassignment = event(:created, "native-child-reassigned", high_route, 300)
    assert {:ok, reassigning, :ok} = Coordinator.receive_event(authorized, reassignment, fn state, _run -> state end)
    assert reassigning.linear.tasks[c.task.issue_id].status == :reassigning
    assert reassigning.linear.tasks[c.task.issue_id].owned_child_grant == nil
    refute Coordinator.child_authorized?(reassigning, c.parent, child)
  end

  test "a stale async check and a fresh assignee mismatch cannot grant child work", c do
    pending = Coordinator.process(c.state, fn state, _run -> state end)
    %{token: token} = pending.linear.job
    assert_receive {:linear_result, ^token, result}, 5_000

    changed_parent = %{c.parent | outputs: %{"approved_gap" => "changed after the check began"}}
    changed_state = put_in(pending.workstreams.runs[c.parent.id], changed_parent)
    stale = finish_job(changed_state, token, result, fn state, _run -> state end)
    refute stale.linear.tasks[c.task.issue_id].owned_child_grant
    {_, linked_child, _intent} = planned_handoff(changed_parent)
    refute Coordinator.child_authorized?(stale, changed_parent, linked_child)

    authorized = refresh(c.state)
    due = force_check(authorized, c.store)

    wrong_assignee_opts =
      Keyword.put(c.opts, :linear_issue_fetcher, fn issue_id, _route ->
        {:ok, issue(issue_id, "someone-else")}
      end)

    wrong_assignee = put_in(due.linear.opts, wrong_assignee_opts)
    parent = self()

    stopped =
      finish_process(wrong_assignee, fn state, _run ->
        send(parent, :native_assignee_mismatch_stopped)
        state
      end)

    assert_receive :native_assignee_mismatch_stopped
    assert stopped.linear.tasks[c.task.issue_id].status == :stopped
    assert stopped.linear.tasks[c.task.issue_id].owned_child_grant == nil
  end

  test "fresh undelegation and stop revoke the owned child", c do
    authorized = refresh(c.state)
    {_, child, _} = planned_handoff(c.parent)
    stopped_event = event(:stop, "native-child-stop", route(c.config), 200)
    assert {:ok, stopped, :ok} = Coordinator.receive_event(authorized, stopped_event, fn state, _run -> state end)
    assert stopped.linear.tasks[c.task.issue_id].owned_child_grant == nil
    refute Coordinator.child_authorized?(stopped, c.parent, child)

    opts =
      Keyword.put(c.opts, :linear_issue_fetcher, fn issue_id, _route ->
        {:ok, %{issue(issue_id, "human-owner") | delegate_id: nil}}
      end)

    undelegated = force_check(authorized, c.store) |> put_in([Access.key(:linear), :opts], opts)
    revoked = finish_process(undelegated, fn state, _run -> state end)
    assert revoked.linear.tasks[c.task.issue_id].status == :stopped
    assert revoked.linear.tasks[c.task.issue_id].owned_child_grant == nil
    refute Coordinator.child_authorized?(revoked, c.parent, child)
  end

  test "revoked leases and capture holds reject a tracked child proof callback", c do
    state = refresh(c.state) |> owner_state(c.store, c.opts, fn _, _, _, _ -> {:error, :unused} end)
    {:reply, :ok, state} = Orchestrator.handle_call(:step_workstreams, {self(), make_ref()}, state)
    parent = state.workstreams.runs[c.parent.id]
    child = state.workstreams.runs[parent.child_handoff.child_run_id]
    key = parent.current_attempt_id
    ref = make_ref()

    job = %{
      ref: ref,
      pid: self(),
      parent_id: parent.id,
      child_id: child.id,
      kind: :retain,
      parent_fingerprint: SymphonyElixir.WorkstreamCleanup.fingerprint(parent),
      child_fingerprint: SymphonyElixir.WorkstreamCleanup.fingerprint(child)
    }

    state = put_in(state.workstreams.child_proofs[key], job)
    # Owner-injected producer result checks only callback authorization. These
    # synthetic bytes are not merge/archive evidence and cannot finish the parent.
    refs = %{"fixture" => %{version: 2, output_id: "fixture-only"}}
    message = {ref, {:child_merge_proof, key, {:outputs_retained, refs}}}
    expired = put_in(state.linear.tasks[c.task.issue_id].owned_child_grant.until, 0)
    {:noreply, rejected} = Orchestrator.handle_info(message, expired)
    refute Map.has_key?(rejected.workstreams.runs[child.id], :retained_output_artifacts)
    assert rejected.workstreams.runs[parent.id].status == :waiting_for_child

    held = put_in(state.workstreams.capture, %{token: make_ref()})
    assert {:noreply, ^held} = Orchestrator.handle_info(message, held)

    {:noreply, accepted} = Orchestrator.handle_info(message, state)
    assert accepted.workstreams.runs[child.id].retained_output_artifacts == refs
    assert accepted.workstreams.runs[parent.id].status == :waiting_for_child
  end

  test "a recovered pending worker stays unstarted without a fresh grant", c do
    {waiting_parent, linked_child, _intent} = planned_handoff(c.parent)

    assert {:ok, waiting_parent, linked_child} =
             WorkstreamStore.commit_child_handoff(c.store, c.parent, waiting_parent, linked_child, waiting_parent.child_handoff)

    executing = WorkstreamRun.start_stage(linked_child)
    attempt_id = executing.current_attempt_id

    {:ok, task_supervisor} = Task.Supervisor.start_link()
    previous_boot_id = :persistent_term.get({Orchestrator, :workstream_boot_id}, :missing)

    boot_id =
      case previous_boot_id do
        :missing -> "test-boot-#{System.unique_integer([:positive])}"
        previous -> previous
      end

    :persistent_term.put({Orchestrator, :workstream_boot_id}, boot_id)

    on_exit(fn ->
      if Process.alive?(task_supervisor), do: GenServer.stop(task_supervisor)
      restore_boot_id(previous_boot_id)
    end)

    owner = self()

    stage_executor = fn stage, _definition, run, _opts ->
      send(owner, {:recovered_child_started, run.id, stage.id})

      receive do
        :finish_recovered_child -> {:ok, %{session_id: "recovered-session", thread_id: "recovered-thread"}}
      end
    end

    worker_opts = Keyword.put(c.opts, :stage_executor, stage_executor)
    {:ok, worker} = Task.Supervisor.start_child(task_supervisor, fn -> AgentRunner.run_workstream(executing, owner, worker_opts) end)
    assert await_worker_identity(worker, executing)

    executing = WorkstreamRun.record_worker(executing, %{pid: inspect(worker), boot_id: boot_id, node: to_string(node())})
    assert :ok = WorkstreamStore.commit(c.store, executing, attempt_id <> "/registered", %{kind: :worker_registered})

    state = state(c.store, c.config, c.opts, c.task, waiting_parent, %{waiting_parent.id => waiting_parent, executing.id => executing})
    assert {:ok, restarted} = Coordinator.init(Map.put(state, :linear, nil), [linear_delegation: c.config] ++ c.opts)
    restart_workers = owner_state(restarted, c.store, c.opts, stage_executor, task_supervisor)
    {:reply, :ok, attached} = Orchestrator.handle_call(:reconcile_workstreams, {self(), make_ref()}, restart_workers)

    assert attached.workstreams.workers[attempt_id].pid == worker
    assert attached.workstreams.workers[attempt_id].launch_pending
    refute_receive {:recovered_child_started, _, _}, 50

    granted = refresh(attached)
    held = put_in(granted.workstreams.capture, %{token: make_ref()})
    assert {:noreply, ^held} = Orchestrator.handle_info(:recover_authorized_children, held)
    refute_receive {:recovered_child_started, _, _}, 50
    {:noreply, launched} = Orchestrator.handle_info(:recover_authorized_children, granted)
    assert_receive {:recovered_child_started, child_id, "implement"}, 5_000
    assert child_id == executing.id
    assert launched.workstreams.workers[attempt_id].pid == worker
    refute Map.has_key?(launched.workstreams.workers[attempt_id], :launch_pending)

    # A restart after launch drops the volatile grant. The worker result remains an observation,
    # but it cannot make the linked child dispatchable or start the parent merge proof.
    assert {:ok, lease_expired} = Coordinator.init(Map.put(launched, :linear, nil), [linear_delegation: c.config] ++ c.opts)
    send(worker, :finish_recovered_child)
    assert_receive {:workstream_result, child_id, ^attempt_id, ^worker, result}, 5_000
    {:noreply, observed} = Orchestrator.handle_info({:workstream_result, child_id, attempt_id, worker, result}, lease_expired)
    parent_after = observed.workstreams.runs[waiting_parent.id]
    child_after = observed.workstreams.runs[child_id]

    assert parent_after.status == :waiting_for_child
    assert child_after.status == :ready
    refute Coordinator.ready?(observed, child_after)
    assert observed.workstreams.child_proofs == %{}
  end

  defp state(store, config, opts, task, parent, runs \\ nil) do
    %{
      workstreams: %{store: store, runs: runs || %{parent.id => parent}},
      linear: %{config: config, tasks: %{task.issue_id => task}, events: %{}, opts: opts, timer: nil, job: nil}
    }
  end

  defp owner_state(state, store, opts, stage_executor, task_supervisor \\ nil) do
    task_supervisor = task_supervisor || start_task_supervisor!()

    workstreams =
      Map.merge(
        %{
          store: store,
          runs: state.workstreams.runs,
          workers: %{},
          cancellations: %{},
          publications: %{},
          validation_checks: %{},
          child_proofs: %{},
          child_proof_timer: nil,
          cleanup: %{jobs: %{}, timer: nil},
          capture: nil,
          capture_results: %{},
          opts: Keyword.merge(opts, auto_advance: false, stage_executor: stage_executor)
        },
        Map.take(state.workstreams, [:workers, :validation_checks, :child_proofs, :child_proof_timer])
      )

    %Orchestrator.State{
      max_concurrent_agents: 1,
      task_supervisor: task_supervisor,
      workstreams: workstreams,
      linear: state.linear
    }
  end

  defp start_task_supervisor! do
    {:ok, supervisor} = Task.Supervisor.start_link()
    on_exit(fn -> if Process.alive?(supervisor), do: GenServer.stop(supervisor) end)
    supervisor
  end

  defp refresh(state, stop \\ fn state, _run -> state end) do
    processing = Coordinator.process(state, stop)
    %{token: token} = processing.linear.job
    assert_receive {:linear_result, ^token, result}, 5_000
    finish_job(processing, token, result, stop)
  end

  defp finish_process(state, stop) do
    processing = Coordinator.process(state, stop)
    %{token: token} = processing.linear.job
    assert_receive {:linear_result, ^token, result}, 5_000
    finish_job(processing, token, result, stop)
  end

  defp finish_job(state, token, result, stop) do
    Coordinator.result(
      state,
      token,
      result,
      fn _state, _task_key, _task_id, _path, _inputs, _execution -> {:error, :unused} end,
      stop
    )
  end

  defp force_check(state, store) do
    task = Map.put(state.linear.tasks["issue-1"], :check_at, 0)
    assert :ok = WorkstreamStore.linear_task(store, task)
    put_in(state.linear.tasks[task.issue_id], task)
  end

  defp planned_handoff(parent) do
    %{stage_id: stage_id} = prepared = parent.execution.child_preparation
    stage = parent.definition.stages[stage_id]
    {:ok, inputs} = WorkstreamChild.inputs(stage, parent.outputs)
    definition = Map.put(prepared.definition, :input_values, inputs)
    child = WorkstreamRun.new("child/" <> parent.id <> "/" <> stage.id, definition, prepared.workspace, [], prepared.execution)
    {:ok, waiting, linked, intent} = WorkstreamRun.begin_child(parent, child)
    {waiting, linked, intent}
  end

  defp parent_run(parent_workspace, child_workspace, workspace_root, config) do
    agent_definitions = %{
      "default-cloud" => agent("DefaultCloud"),
      "high-effort-cloud" => agent("HighEffortCloud")
    }

    child_definition = %{
      name: "software-change",
      entry: "implement",
      inputs: ["task"],
      input_values: %{"task" => "pending parent input"},
      definitions: %{},
      agents: agent_definitions,
      stages: %{
        "implement" => %{id: "implement", type: :agent, agent: "default-cloud", next: "followup", outputs: []},
        "followup" => %{id: "followup", type: :agent, agent: "default-cloud", next: :complete, outputs: []}
      }
    }

    {:ok, child_definition} = AgentMapping.bind(child_definition, "default-cloud")
    secret_names = AgentMapping.credential_environment_names(config) ++ ["FACTORY_LINEAR_SECRET_NAMES_JSON"]
    child_execution = %{workspace: child_workspace, workspace_root: workspace_root, secret_environment_names: secret_names}

    child_stage = %{
      id: "child",
      type: :child,
      input_mapping: %{"task" => "approved_gap"},
      child_definition: child_definition,
      outputs: ["child_result"],
      next: :complete
    }

    parent_definition = %{
      name: "software-change",
      entry: "implement",
      inputs: ["approved_gap"],
      input_values: %{"approved_gap" => "approved child task"},
      definitions: %{},
      agents: agent_definitions,
      stages: %{
        "implement" => %{id: "implement", type: :agent, agent: "default-cloud", next: "child", outputs: []},
        "child" => child_stage
      }
    }

    {:ok, parent_definition} = AgentMapping.bind(parent_definition, "default-cloud")
    child_preparation = %{stage_id: "child", definition: child_definition, execution: child_execution, workspace: child_workspace}

    parent_execution = %{
      workspace: parent_workspace,
      workspace_root: workspace_root,
      secret_environment_names: secret_names,
      child_preparation: child_preparation
    }

    parent = WorkstreamRun.new("linear/issue-1", parent_definition, parent_workspace, [issue_id: "issue-1", branch: "linear/issue-1"], parent_execution)
    parent = %{parent | stage_id: "child", outputs: %{"approved_gap" => "approved child task"}}
    {parent, child_definition, child_execution}
  end

  defp native_task(event, parent, config) do
    selected = route(config)

    %{
      issue_id: event.issue_id,
      run_id: parent.id,
      status: :acknowledged,
      session_id: event.session_id,
      assignee_id: "human-owner",
      agent_id: selected["agent_id"],
      route: Map.take(selected, ["organization_id", "app_user_id", "oauth_client_id", "agent_id"]),
      app_user_id: selected["app_user_id"],
      oauth_client_id: selected["oauth_client_id"],
      activity_id: "native-child-activity",
      event: event,
      authorized_stage: nil,
      authorized_until: 0,
      owned_child_grant: nil,
      scope_revision: 1,
      check_at: 0,
      publications: %{},
      assignment_history: []
    }
  end

  defp config(root, store_path, workspace_root, parent_workspace, child_workspace) do
    %{
      "organization_id" => "rig",
      "team_id" => "dev",
      "app_user_id" => "app-default",
      "oauth_client_id" => "client-default",
      "webhook_secret_env" => "HOOK_DEFAULT",
      "client_secret_env" => "CLIENT_DEFAULT",
      "agent_id" => "default-cloud",
      "cloud_agents" => [
        %{"app_user_id" => "app-high", "oauth_client_id" => "client-high", "webhook_secret_env" => "HOOK_HIGH", "client_secret_env" => "CLIENT_HIGH", "agent_id" => "high-effort-cloud"}
      ],
      "store_path" => store_path,
      "workspace_root" => workspace_root,
      "workstream_path" => Path.join(root, "workstream.yaml"),
      "rig_label" => "factory:rig",
      "workspaces" => %{"issue-1" => parent_workspace},
      "child_workspaces" => %{"issue-1" => child_workspace}
    }
  end

  defp linear_opts(owner, assignee_id) do
    [
      linear_issue_fetcher: fn issue_id, _route ->
        send(owner, {:native_issue_checked, issue_id})
        {:ok, issue(issue_id, assignee_id)}
      end,
      linear_session_fetcher: fn session_id, route ->
        {:ok, %{"id" => session_id, "issueId" => "issue-1", "appUser" => %{"id" => route["app_user_id"]}, "dismissedAt" => nil}}
      end,
      linear_readiness: fn _definition, _agent_id -> :ok end,
      linear_execution_control: fn _definition, _agent_id -> :ok end
    ]
  end

  defp issue(issue_id, assignee_id) do
    %Issue{
      id: issue_id,
      identifier: "DEV-243",
      title: "Authorize an owned child",
      description: "Check native child scope before dispatch.",
      state: "In Progress",
      state_type: "started",
      team_id: "dev",
      delegate_id: "app-default",
      assignee_id: assignee_id,
      labels: ["factory:rig"],
      dispatchable: true
    }
  end

  defp event(kind, id, route, timestamp) do
    %{
      id: id,
      kind: kind,
      issue_id: "issue-1",
      session_id: if(route["agent_id"] == "default-cloud", do: "session-default", else: "session-high"),
      app_user_id: route["app_user_id"],
      verified_app_user_id: route["app_user_id"],
      oauth_client_id: route["oauth_client_id"],
      organization_id: "rig",
      timestamp: timestamp,
      activity_id: nil
    }
  end

  defp route(config, agent_id \\ "default-cloud") do
    {:ok, routes} = AgentMapping.routes(config)
    Enum.find(routes, &(&1["agent_id"] == agent_id))
  end

  defp agent(name), do: %{name: name, revision: "revision-1", model: "gpt-6-luna", reasoning_effort: "medium", daybreak: false, instructions: [], skills: []}

  defp init_repo!(workspace) do
    File.mkdir_p!(workspace)
    {_output, 0} = System.cmd("git", ["init", "--initial-branch", "main", workspace], stderr_to_stdout: true)
  end

  defp await_worker_identity(worker, run, retries \\ 100)
  defp await_worker_identity(_worker, _run, 0), do: false

  defp await_worker_identity(worker, run, retries) do
    expected = %{run_id: run.id, attempt_id: run.current_attempt_id, operation_id: run.current_attempt_id}

    case Process.info(worker, :dictionary) do
      {:dictionary, dictionary} ->
        if Keyword.get(dictionary, :symphony_workstream) == expected do
          true
        else
          Process.sleep(5)
          await_worker_identity(worker, run, retries - 1)
        end

      _ ->
        Process.sleep(5)
        await_worker_identity(worker, run, retries - 1)
    end
  end

  defp restore_boot_id(:missing), do: :persistent_term.erase({Orchestrator, :workstream_boot_id})
  defp restore_boot_id(value), do: :persistent_term.put({Orchestrator, :workstream_boot_id}, value)
end
