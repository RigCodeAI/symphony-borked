defmodule SymphonyElixir.LinearAgentRoutingTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.Linear.{AgentMapping, Coordinator}
  alias SymphonyElixir.{Config, Evidence, WorkflowStore, WorkstreamRun, WorkstreamStore}
  alias SymphonyElixir.Tracker.Issue

  setup do
    root = Path.join(System.tmp_dir!(), "linear-routing-#{System.unique_integer([:positive])}")
    path = Path.join([root, "private", "state.sqlite3"])
    File.mkdir_p!(Path.dirname(path))
    assert {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())

    on_exit(fn ->
      if Process.alive?(store), do: GenServer.stop(store)
      File.rm_rf!(root)
    end)

    %{root: root, store: store, path: path, config: config(), opts: []}
  end

  test "a stop before task creation leaves a route-pinned tombstone", context do
    state = state(context, %{}, %{})
    stop_event = event(:stop, "early-stop", "default-cloud", "session-default", 100)

    assert {:ok, updated, :ok} = Coordinator.receive_event(state, stop_event, fn state, _run -> state end)
    task = updated.linear.tasks[stop_event.issue_id]

    assert task.status == :stopped
    assert task.agent_id == "default-cloud"
    assert task.session_id == "session-default"
    assert task.stop_timestamp == 100
    assert task.route["app_user_id"] == "app-default"
  end

  test "an authenticated different-agent create revokes the lease before handoff", context do
    run = %{
      id: "run-1",
      task_id: "linear/issue-1",
      status: :ready,
      phase: :queued,
      stage_id: "implement",
      operations: %{},
      policy: WorkstreamRun.policy()
    }

    old_event = event(:created, "old-create", "default-cloud", "session-default", 100)
    task = task(old_event, run.id, "default-cloud", "session-default", 1)
    state = state(context, %{task.issue_id => task}, %{run.id => run})

    stop = fn state, stopped_run ->
      Map.update!(state, :workstreams, &Map.update!(&1, :runs, fn runs -> Map.put(runs, stopped_run.id, %{stopped_run | status: :stopped, phase: :stopped}) end))
    end

    candidate = event(:created, "new-create", "high-effort-cloud", "session-high", 101)
    assert {:ok, updated, :ok} = Coordinator.receive_event(state, candidate, stop)

    reassignment = updated.linear.tasks[task.issue_id]
    assert reassignment.status == :reassigning
    assert reassignment.pending_reassignment.id == candidate.id
    assert reassignment.pending_reassignment.agent_id == "high-effort-cloud"
    assert reassignment.authorized_stage == nil
    assert reassignment.authorized_until == 0
    assert reassignment.agent_id == "default-cloud"
    assert updated.workstreams.runs[run.id].status == :stopped
    refute Coordinator.ready?(updated, updated.workstreams.runs[run.id])
  end

  test "events from the old app and session cannot stop or prompt the new assignment", context do
    new_event = event(:created, "new-create", "high-effort-cloud", "session-high", 200)
    run = %{id: "run-1", task_id: "linear/issue-1", status: :waiting_for_answer, phase: :waiting_for_answer, stage_id: "wait", operations: %{}, policy: WorkstreamRun.policy()}
    task = task(new_event, run.id, "high-effort-cloud", "session-high", 4)
    state = state(context, %{task.issue_id => task}, %{run.id => run})
    parent = self()

    stop = fn state, _run ->
      send(parent, :unexpected_stop)
      state
    end

    old_stop = event(:stop, "old-stop", "default-cloud", "session-default", 201)
    assert {:ok, after_stop, :ok} = Coordinator.receive_event(state, old_stop, stop)
    assert after_stop.linear.events[old_stop.id].status == :ignored_old_assignment
    assert after_stop.linear.tasks[task.issue_id].status == :acknowledged

    old_update = event(:issue_updated, "old-update", "default-cloud", nil, 202)
    assert {:ok, after_update, :ok} = Coordinator.receive_event(after_stop, old_update, stop)
    assert after_update.linear.events[old_update.id].status == :ignored_old_assignment
    assert after_update.linear.tasks[task.issue_id].scope_revision == task.scope_revision

    old_prompt = event(:prompted, "old-prompt", "default-cloud", "session-default", 203) |> Map.merge(%{activity_id: "old-activity", body: "answer question: no"})
    assert {:ok, after_prompt, :ok} = Coordinator.receive_event(after_update, old_prompt, stop)
    assert after_prompt.linear.events[old_prompt.id].status == :ignored_old_assignment
    assert after_prompt.linear.tasks[task.issue_id].session_id == "session-high"
    refute_receive :unexpected_stop
  end

  test "multi-app routing requires the webhook-authenticated app identity", context do
    state = state(context, %{}, %{})
    forged = event(:stop, "forged-route", "high-effort-cloud", "session-high", 100) |> Map.delete(:verified_app_user_id)

    assert {:error, :unrelated_linear_event} = Coordinator.receive_event(state, forged, fn state, _run -> state end)
    assert state.linear.tasks == %{}
  end

  test "reassignment preserves the same run and questions and survives store reopen", context do
    {state, run, task, stop} = handoff_fixture(context, "high-effort-cloud", %{})
    candidate = event(:created, "handoff-create", "high-effort-cloud", "session-high", 101)
    assert {:ok, received, :ok} = Coordinator.receive_event(state, candidate, stop)

    processing = Coordinator.process(received, stop)
    assert {:reassign, issue_id, delivery_id, _revision} = processing.linear.job.key
    assert issue_id == task.issue_id
    assert delivery_id == candidate.id
    completed = finish_job(processing, stop)
    updated_run = completed.workstreams.runs[run.id]
    updated_task = completed.linear.tasks[task.issue_id]

    assert updated_run.id == run.id
    assert updated_run.branch == run.branch
    assert updated_run.status == :waiting_for_answer
    assert updated_run.questions == run.questions
    assert updated_run.pending_wait == run.pending_wait
    assert updated_run.inbox == run.inbox
    assert updated_run.continuation == run.continuation
    assert updated_run.definition.stages["implement"].agent == "high-effort-cloud"
    assert [transition] = updated_run.agent_transitions
    assert transition.id == candidate.id
    assert transition.to.agent_id == "high-effort-cloud"
    assert transition.to.name == "HighEffortCloud"
    assert "CLIENT_DAYBREAK" in updated_run.execution.secret_environment_names

    assert updated_task.run_id == task.run_id
    assert updated_task.status == :acknowledging
    assert updated_task.session_id == "session-high"
    assert updated_task.agent_id == "high-effort-cloud"
    assert updated_task.publications == %{}
    assert [previous] = updated_task.assignment_history
    assert previous.session_id == "session-default"
    assert previous.publications == task.publications
    assert completed.linear.events[candidate.id].status == :handled

    assert :ok = GenServer.stop(context.store)
    assert {:ok, reopened} = WorkstreamStore.start_link(path: context.path, owner: self())
    assert {:ok, ^updated_run} = WorkstreamStore.fetch(reopened, run.id)
    assert {:ok, %{tasks: tasks, events: events}} = WorkstreamStore.linear_load(reopened)
    assert tasks[task.issue_id] == updated_task
    assert events[candidate.id].status == :handled
    assert :ok = GenServer.stop(reopened)
  end

  test "unknown prior-writer termination remains undispatchable after restart", context do
    operation = %{status: :canceled, cancellation: %{termination: :unknown}}
    {state, run, task, stop} = handoff_fixture(context, "high-effort-cloud", %{"attempt-1" => operation})
    candidate = event(:created, "uncertain-handoff", "high-effort-cloud", "session-high", 101)
    assert {:ok, received, :ok} = Coordinator.receive_event(state, candidate, stop)
    pending = Coordinator.process(received, stop)

    assert is_nil(pending.linear.job)
    assert pending.linear.tasks[task.issue_id].status == :reassigning
    refute Coordinator.ready?(pending, pending.workstreams.runs[run.id])

    assert :ok = GenServer.stop(context.store)
    assert {:ok, reopened} = WorkstreamStore.start_link(path: context.path, owner: self())
    assert {:ok, stored_run} = WorkstreamStore.fetch(reopened, run.id)
    assert {:ok, %{tasks: tasks}} = WorkstreamStore.linear_load(reopened)
    restarted = state(%{context | store: reopened}, tasks, %{run.id => stored_run})
    recovered = Coordinator.process(restarted, stop)

    assert is_nil(recovered.linear.job)
    assert recovered.linear.tasks[task.issue_id].status == :reassigning
    refute Coordinator.ready?(recovered, stored_run)
    assert :ok = GenServer.stop(reopened)
  end

  test "unsupported Daybreak reassignment reports to the requested app and session", context do
    {state, run, task, stop} = handoff_fixture(context, "daybreak-cloud", %{})
    candidate = event(:created, "daybreak-handoff", "daybreak-cloud", "session-daybreak", 101)
    parent = self()

    publisher = fn task, activity, route ->
      send(parent, {:routed_publication, task.session_id, route["agent_id"], activity.content})
      {:ok, %{id: activity.id}}
    end

    linear = Map.put(state.linear, :opts, Keyword.put(state.linear.opts, :linear_publisher, publisher))
    state = Map.put(state, :linear, linear)
    assert {:ok, received, :ok} = Coordinator.receive_event(state, candidate, stop)
    blocked = received |> Coordinator.process(stop) |> finish_job(stop)
    blocked_task = blocked.linear.tasks[task.issue_id]

    assert blocked_task.status == :blocked
    assert blocked_task.blocked_reassignment.agent_id == "daybreak-cloud"
    assert blocked.workstreams.runs[run.id].definition.stages["implement"].agent == "default-cloud"
    refute Map.has_key?(blocked.workstreams.runs[run.id], :agent_transitions)

    publishing = Coordinator.process(blocked, stop)
    assert_receive {:routed_publication, "session-daybreak", "daybreak-cloud", %{"type" => "error", "body" => body}}, 5_000
    assert body =~ "daybreak_execution_unverified"
    assert {:publish, _issue_id, id, _revision} = publishing.linear.job.key
    assert blocked_task.publications[id].route["agent_id"] == "daybreak-cloud"
  end

  test "a terminal run reports a rejected reassignment through the requesting session", context do
    {state, run, task, stop} = handoff_fixture(context, "daybreak-cloud", %{})
    completed_run = %{run | status: :complete, phase: :complete}
    assert :ok = WorkstreamStore.commit(context.store, completed_run, "completed-run", %{kind: :completed})
    state = put_in(state.workstreams.runs[run.id], completed_run)
    candidate = event(:created, "terminal-daybreak-handoff", "daybreak-cloud", "session-daybreak", 101)
    parent = self()

    publisher = fn publication_task, activity, route ->
      send(parent, {:terminal_routed_publication, publication_task.session_id, route["agent_id"], activity.content})
      {:ok, %{id: activity.id}}
    end

    linear = Map.put(state.linear, :opts, Keyword.put(state.linear.opts, :linear_publisher, publisher))
    state = Map.put(state, :linear, linear)
    assert {:ok, blocked, :ok} = Coordinator.receive_event(state, candidate, stop)
    blocked_task = blocked.linear.tasks[task.issue_id]

    assert blocked_task.status == :blocked
    assert blocked_task.blocked_reassignment.agent_id == "daybreak-cloud"
    assert blocked_task.blocked_reassignment.session_id == "session-daybreak"
    assert blocked_task.agent_id == "default-cloud"
    assert blocked.workstreams.runs[run.id].definition.stages["implement"].agent == "default-cloud"

    _publishing = Coordinator.process(blocked, fn current, _run -> current end)
    assert_receive {:terminal_routed_publication, "session-daybreak", "daybreak-cloud", %{"type" => "error", "body" => body}}, 5_000
    assert body =~ "run_not_reassignable"
  end

  test "restart pins expanded service secrets before checks and preserves an acknowledged wait", context do
    run = waiting_run("run-252", ["LINEAR_API_KEY", "LINEAR_API_TOKEN", "FACTORY_LINEAR_SECRET_NAMES_JSON"])
    created = event(:created, "created-252", "default-cloud", "session-default", 100)
    task = task(created, run.id, "default-cloud", "session-default", 1)
    assert :ok = WorkstreamStore.commit(context.store, run, "queued-252", %{kind: :queued})
    assert :ok = WorkstreamStore.linear_task(context.store, task)

    assert :ok = GenServer.stop(context.store)
    assert {:ok, reopened} = WorkstreamStore.start_link(path: context.path, owner: self())
    on_exit(fn -> if Process.alive?(reopened), do: GenServer.stop(reopened) end)
    context = %{context | store: reopened}

    assert {:ok, [stored_run]} = WorkstreamStore.load(reopened)
    restarted = %{workstreams: %{store: reopened, runs: %{run.id => stored_run}}, linear: nil}
    opts = startup_options(context)
    assert {:ok, restarted} = Coordinator.init(restarted, opts)
    restored_task = restarted.linear.tasks[task.issue_id]
    assert restored_task.status == :acknowledged
    assert restored_task.authorized_stage == nil
    refute Coordinator.ready?(restarted, stored_run)

    stop = fn state, stopped_run -> put_in(state.workstreams.runs[stopped_run.id], stopped_run) end

    state = restarted |> Coordinator.process(stop) |> finish_job(stop) |> Coordinator.process(stop)
    assert {:check, issue_id, _revision} = state.linear.job.key
    assert issue_id == task.issue_id
    refute Coordinator.ready?(state, state.workstreams.runs[run.id])

    state = finish_job(state, stop)
    updated_run = state.workstreams.runs[run.id]

    expected_names = [
      "LINEAR_API_KEY",
      "LINEAR_API_TOKEN",
      "FACTORY_LINEAR_SECRET_NAMES_JSON",
      "FACTORY_HIGH_WEBHOOK_SECRET",
      "FACTORY_HIGH_CLIENT_SECRET",
      "FACTORY_DAYBREAK_WEBHOOK_SECRET",
      "FACTORY_DAYBREAK_CLIENT_SECRET"
    ]

    assert updated_run.status == :waiting_for_answer
    assert updated_run.stage_id == "candidate-ready"
    assert updated_run.current_attempt_id == run.current_attempt_id
    assert updated_run.pending_wait == run.pending_wait
    assert Enum.all?(expected_names, &(&1 in updated_run.execution.secret_environment_names))
    assert {:ok, persisted_run} = WorkstreamStore.fetch(reopened, run.id)
    assert persisted_run.execution.secret_environment_names == updated_run.execution.secret_environment_names
    refute Coordinator.ready?(state, updated_run)

    wait_id = run.pending_wait.id

    reply =
      event(:prompted, "reply-252", "default-cloud", "session-default", 101)
      |> Map.merge(%{activity_id: "reply-activity-252", body: "answer #{wait_id}: {\"answer\":\"continue\"}"})

    assert {:ok, received, :ok} = Coordinator.receive_event(state, reply, stop)
    state = received |> Coordinator.process(stop) |> finish_job(stop)
    assert state.workstreams.runs[run.id].status == :ready
    refute Coordinator.ready?(state, state.workstreams.runs[run.id])

    state = Coordinator.process(state, stop)
    assert {:check, ^issue_id, _revision} = state.linear.job.key
    refute Coordinator.ready?(state, state.workstreams.runs[run.id])

    state = finish_job(state, stop)
    assert state.linear.tasks[task.issue_id].authorized_stage == "implement"
    assert Coordinator.ready?(state, state.workstreams.runs[run.id])
  end

  test "expanded secrets defer authorization while a run has an active operation", context do
    run = waiting_run("run-active", ["LINEAR_API_KEY", "LINEAR_API_TOKEN", "FACTORY_LINEAR_SECRET_NAMES_JSON"])
    run = put_in(run.operations["active-operation"], %{status: :executing})
    created = event(:created, "created-active", "default-cloud", "session-default", 100)
    task = task(created, run.id, "default-cloud", "session-default", 1)
    assert :ok = WorkstreamStore.commit(context.store, run, "queued-active", %{kind: :queued})
    assert :ok = WorkstreamStore.linear_task(context.store, task)

    assert {:ok, [stored_run]} = WorkstreamStore.load(context.store)
    restarted = %{workstreams: %{store: context.store, runs: %{run.id => stored_run}}, linear: nil}
    assert {:ok, restarted} = Coordinator.init(restarted, startup_options(context))
    stop = fn state, _run -> state end
    authorized = %{task | authorized_stage: run.stage_id, authorized_until: System.system_time(:millisecond) + 60_000}
    restarted = put_in(restarted.linear.tasks[task.issue_id], authorized)
    assert Coordinator.ready?(restarted, run)

    state = restarted |> Coordinator.process(stop) |> finish_job(stop) |> Coordinator.process(stop)
    refute state.linear.job
    assert state.workstreams.runs[run.id].execution.secret_environment_names == run.execution.secret_environment_names
    assert state.workstreams.runs[run.id].status == :waiting_for_answer
    assert state.workstreams.runs[run.id].operations["active-operation"].status == :executing
    assert state.linear.tasks[task.issue_id].status == :acknowledged
    assert state.linear.tasks[task.issue_id].authorized_stage == nil
    assert state.linear.tasks[task.issue_id].authorized_until == 0
    assert state.linear.tasks[task.issue_id].check_at > System.system_time(:millisecond)
    assert state.workstreams.runs[run.id].execution.secret_environment_names == run.execution.secret_environment_names
    refute Coordinator.ready?(state, state.workstreams.runs[run.id])
  end

  test "expanded secrets defer checks when persisting exclusions fails", context do
    run = waiting_run("run-persistence-failure", ["LINEAR_API_KEY", "LINEAR_API_TOKEN", "FACTORY_LINEAR_SECRET_NAMES_JSON"])
    created = event(:created, "created-persistence-failure", "default-cloud", "session-default", 100)
    task = task(created, run.id, "default-cloud", "session-default", 1)
    assert :ok = WorkstreamStore.commit(context.store, run, "queued-persistence-failure", %{kind: :queued})
    assert :ok = WorkstreamStore.linear_task(context.store, task)

    restarted = %{workstreams: %{store: context.store, runs: %{run.id => run}}, linear: nil}
    assert {:ok, restarted} = Coordinator.init(restarted, startup_options(context))
    failing_store = failing_commit_store()
    authorized = %{task | authorized_stage: run.stage_id, authorized_until: System.system_time(:millisecond) + 60_000}
    stop = fn state, _run -> state end
    state = put_in(restarted.linear.tasks[task.issue_id], authorized)

    assert Coordinator.ready?(state, run)
    state = state |> Coordinator.process(stop) |> finish_job(stop)
    assert Coordinator.ready?(state, run)

    state = put_in(state.workstreams.store, failing_store)
    state = Coordinator.process(state, stop)
    refute state.linear.job
    assert state.workstreams.runs[run.id].execution.secret_environment_names == run.execution.secret_environment_names
    assert state.linear.tasks[task.issue_id].authorized_stage == nil
    assert state.linear.tasks[task.issue_id].authorized_until == 0
    assert state.linear.tasks[task.issue_id].check_at > System.system_time(:millisecond)
    refute Coordinator.ready?(state, state.workstreams.runs[run.id])

    Process.exit(failing_store, :kill)
  end

  test "duplicate secret pin event does not replace a divergent in-memory wait snapshot", context do
    run = waiting_run("run-duplicate-pin", ["LINEAR_API_KEY", "LINEAR_API_TOKEN", "FACTORY_LINEAR_SECRET_NAMES_JSON"])
    created = event(:created, "created-duplicate-pin", "default-cloud", "session-default", 100)
    task = task(created, run.id, "default-cloud", "session-default", 1)
    assert :ok = WorkstreamStore.commit(context.store, run, "queued-duplicate-pin", %{kind: :queued})
    assert :ok = WorkstreamStore.linear_task(context.store, task)

    restarted = %{workstreams: %{store: context.store, runs: %{run.id => run}}, linear: nil}
    assert {:ok, restarted} = Coordinator.init(restarted, startup_options(context))

    names =
      restarted.linear.config
      |> AgentMapping.credential_environment_names()
      |> Kernel.++(["FACTORY_LINEAR_SECRET_NAMES_JSON"])
      |> Kernel.++(run.execution.secret_environment_names)
      |> Enum.uniq()
      |> Enum.sort()

    pinned = put_in(run.execution.secret_environment_names, names)
    event = %{kind: :service_secret_environment_names_pinned, environment_names: names}
    digest = :crypto.hash(:sha256, Jason.encode!([run.id, names])) |> Base.encode16(case: :lower)
    event_id = "linear-secret-environment-names/" <> digest
    assert :ok = WorkstreamStore.commit(context.store, pinned, event_id, event)

    divergent = put_in(run.pending_wait.prompt, "newer in-memory prompt")
    restarted = put_in(restarted.workstreams.runs[run.id], divergent)
    stop = fn state, _run -> state end
    state = restarted |> Coordinator.process(stop) |> finish_job(stop) |> Coordinator.process(stop)

    refute state.linear.job
    assert state.workstreams.runs[run.id].pending_wait.prompt == "newer in-memory prompt"
    assert state.workstreams.runs[run.id].execution.secret_environment_names == run.execution.secret_environment_names
    assert state.linear.tasks[task.issue_id].authorized_stage == nil
    refute Coordinator.ready?(state, state.workstreams.runs[run.id])
    assert {:ok, stored} = WorkstreamStore.fetch(context.store, run.id)
    assert stored.pending_wait == run.pending_wait
    assert stored.execution.secret_environment_names == names
  end

  test "secret pin evidence keeps the wait and pinned definition identity", context do
    workflow_store = Process.whereis(WorkflowStore)
    assert is_pid(workflow_store)
    original_state = :sys.get_state(workflow_store)

    evidence_config = %{
      "root" => Path.join(context.root, "private-evidence"),
      "base_url" => "https://evidence.example.com",
      "iap_audience" => "/projects/123456789/global/backendServices/123456789",
      "allowed_domains" => ["rig.ai"]
    }

    workflow =
      Map.put(
        original_state.workflow,
        :config,
        Map.put(original_state.workflow.config, "evidence", evidence_config)
      )

    :sys.replace_state(workflow_store, fn state -> Map.put(state, :workflow, workflow) end)

    try do
      assert {:ok, ^evidence_config} = Config.evidence()
      assert {:ok, _pid} = Evidence.start_link(config: {:ok, evidence_config})

      run = waiting_run("run-evidence-pin", ["LINEAR_API_KEY", "LINEAR_API_TOKEN", "FACTORY_LINEAR_SECRET_NAMES_JSON"])
      created = event(:created, "created-evidence-pin", "default-cloud", "session-default", 100)
      task = task(created, run.id, "default-cloud", "session-default", 1)
      assert :ok = WorkstreamStore.commit(context.store, run, "queued-evidence-pin", %{kind: :queued})
      assert :ok = WorkstreamStore.linear_task(context.store, task)

      restarted = %{workstreams: %{store: context.store, runs: %{run.id => run}}, linear: nil}
      assert {:ok, restarted} = Coordinator.init(restarted, startup_options(context))
      stop = fn state, _run -> state end
      state = process_until_check(restarted, stop)
      assert {:check, _issue_id, _revision} = state.linear.job.key
      state = finish_job(state, stop)

      assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id))
      assert view.snapshot["stage_id"] == "candidate-ready"
      assert view.snapshot["current_attempt_id"] == run.current_attempt_id
      assert view.snapshot["definitions"]["workstream.yaml"] == "definition-sha256"
      assert view.snapshot["pending_wait"]["id"] == run.pending_wait.id
      assert {:ok, []} = WorkstreamStore.pending_evidence(context.store)
      assert state.workstreams.runs[run.id].pending_wait == run.pending_wait
    after
      if pid = Process.whereis(Evidence), do: GenServer.stop(pid)
      :sys.replace_state(workflow_store, fn _state -> original_state end)
    end
  end

  defp state(context, tasks, runs) do
    %{
      workstreams: %{store: context.store, runs: runs},
      linear: %{config: context.config, tasks: tasks, events: %{}, opts: context[:opts] || [], timer: nil, job: nil}
    }
  end

  defp handoff_fixture(context, target_agent, operations) do
    run = %{
      id: "run-1",
      task_id: "linear/issue-1",
      issue_id: "issue-1",
      branch: "linear/issue-1",
      session_id: "codex-old-session",
      thread_id: "thread-old",
      status: :stopped,
      phase: :stopped,
      stage_id: "implement",
      current_attempt_id: "attempt-1",
      attempts: [],
      operations: operations,
      policy: WorkstreamRun.policy(),
      definition: definition(),
      execution: %{secret_environment_names: ["HOOK_DEFAULT", "CLIENT_DEFAULT"]},
      questions: %{"question-1" => %{id: "question-1", prompt: "Which behavior?", status: :pending}},
      pending_wait: %{id: "wait-1", prompt: "Approve this candidate?", artifact_ids: %{"candidate" => "sha256:candidate"}},
      inbox: [%{body: "keep this reply", activity_id: "reply-1", delivered_attempt_id: nil}],
      activity_ids: %{"reply-1" => "reply-digest"},
      continuation: %{thread_id: "thread-old", questions: [%{id: "question-1"}]},
      cancellation: %{termination: :terminated}
    }

    old_event = event(:created, "previous-create", "default-cloud", "session-default", 100)
    old_publication = %{id: "old-activity", key: "phase/implement", status: :published, content: %{"type" => "thought", "body" => "Previous session evidence"}}
    task = task(old_event, run.id, "default-cloud", "session-default", 1) |> Map.put(:publications, %{old_publication.id => old_publication})

    issue_fetcher = fn issue_id, route ->
      {:ok,
       %Issue{
         id: issue_id,
         identifier: "DEV-235",
         title: "Route assignment safely",
         description: "Keep the same workstream run.",
         state: "In Progress",
         state_type: "started",
         team_id: "dev",
         delegate_id: route["app_user_id"],
         assignee_id: "human-owner",
         labels: ["factory:rig"],
         dispatchable: true
       }}
    end

    session_fetcher = fn session_id, route ->
      {:ok, %{"id" => session_id, "issueId" => "issue-1", "appUser" => %{"id" => route["app_user_id"]}, "dismissedAt" => nil}}
    end

    context = %{
      context
      | opts: [
          linear_issue_fetcher: issue_fetcher,
          linear_session_fetcher: session_fetcher,
          linear_readiness: fn _definition, _agent -> :ok end,
          linear_execution_control: fn _definition, _agent -> :ok end
        ]
    }

    assert :ok = WorkstreamStore.commit(context.store, run, "initial-run", %{kind: :queued})
    assert :ok = WorkstreamStore.linear_task(context.store, task)

    stop = fn state, stopped_run ->
      send(self(), {:stop_requested, stopped_run.id})
      stopped_run = %{stopped_run | status: :stopped, phase: :stopped, cancellation: %{termination: :terminated}}

      case WorkstreamStore.commit(context.store, stopped_run, "stop/" <> target_agent, %{kind: :stopped}) do
        :ok -> :ok
        {:duplicate, _run_id} -> :ok
      end

      Map.update!(state, :workstreams, &Map.update!(&1, :runs, fn runs -> Map.put(runs, stopped_run.id, stopped_run) end))
    end

    {state(context, %{task.issue_id => task}, %{run.id => run}), run, task, stop}
  end

  defp waiting_run(id, secret_environment_names) do
    attempt_id = id <> "/candidate-ready/3"

    %{
      id: id,
      task_id: "linear/issue-1",
      issue_id: "issue-1",
      branch: "linear/issue-1",
      workspace: "/tmp/" <> id,
      status: :waiting_for_answer,
      phase: :waiting_for_answer,
      stage_id: "candidate-ready",
      current_attempt_id: attempt_id,
      attempts: [
        %{id: id <> "/implement/1", stage: "implement", status: :completed},
        %{id: id <> "/validate/2", stage: "validate", status: :completed},
        %{id: attempt_id, stage: "candidate-ready", type: :human_wait, status: :waiting}
      ],
      operations: %{},
      policy: WorkstreamRun.policy(),
      definition: %{
        inputs: [],
        entry: "implement",
        definitions: %{"workstream.yaml" => %{sha256: "definition-sha256", text: "pinned workstream definition"}},
        stages: %{
          "candidate-ready" => %{
            type: :human_wait,
            inputs: ["candidate"],
            outputs: ["answer"],
            prompt: "Review this candidate.",
            next: "implement"
          },
          "implement" => %{type: :agent, agent: "default-cloud", inputs: ["answer"], outputs: ["candidate"]}
        },
        agents: %{"default-cloud" => agent("DefaultCloud", false)}
      },
      execution: %{secret_environment_names: secret_environment_names},
      outputs: %{"candidate" => "candidate content"},
      artifacts: %{"candidate" => "sha256:candidate"},
      pending_wait: %{
        id: attempt_id <> "/wait",
        prompt: "Review this candidate.",
        inputs: %{"candidate" => "candidate content"},
        artifact_ids: %{"candidate" => "sha256:candidate"}
      },
      activity_ids: %{},
      questions: %{},
      inbox: []
    }
  end

  defp startup_options(context) do
    config =
      config()
      |> Map.put("webhook_secret_env", "LINEAR_API_TOKEN")
      |> Map.put("token_env", "LINEAR_API_KEY")
      |> Map.delete("client_secret_env")
      |> Map.put("cloud_agents", [
        %{
          "app_user_id" => "app-high",
          "oauth_client_id" => "client-high",
          "webhook_secret_env" => "FACTORY_HIGH_WEBHOOK_SECRET",
          "client_secret_env" => "FACTORY_HIGH_CLIENT_SECRET",
          "agent_id" => "high-effort-cloud"
        },
        %{
          "app_user_id" => "app-daybreak",
          "oauth_client_id" => "client-daybreak",
          "webhook_secret_env" => "FACTORY_DAYBREAK_WEBHOOK_SECRET",
          "client_secret_env" => "FACTORY_DAYBREAK_CLIENT_SECRET",
          "agent_id" => "daybreak-cloud"
        }
      ])
      |> Map.merge(%{
        "store_path" => context.path,
        "workspace_root" => "/tmp/linear-routing-workspaces",
        "workstream_path" => "/tmp/software-change.yaml",
        "workspaces" => %{"issue-1" => "/tmp/linear-routing-workspaces/issue-1"}
      })

    issue_fetcher = fn issue_id, route ->
      {:ok,
       %Issue{
         id: issue_id,
         identifier: "DEV-252",
         title: "Preserve a human wait after secret expansion",
         description: "",
         state: "In Progress",
         state_type: "started",
         team_id: "dev",
         delegate_id: route["app_user_id"],
         assignee_id: "human-owner",
         labels: ["factory:rig"],
         dispatchable: true
       }}
    end

    session_fetcher = fn session_id, route ->
      {:ok,
       %{
         "id" => session_id,
         "issueId" => "issue-1",
         "appUser" => %{"id" => route["app_user_id"]},
         "dismissedAt" => nil
       }}
    end

    [
      linear_delegation: config,
      linear_issue_fetcher: issue_fetcher,
      linear_session_fetcher: session_fetcher,
      linear_publisher: fn _task, activity, _route -> {:ok, %{id: activity.id}} end
    ]
  end

  defp finish_job(state, stop) do
    %{token: token} = state.linear.job
    assert_receive {:linear_result, ^token, result}, 5_000
    Coordinator.result(state, token, result, fn _state, _task_key, _task_id, _path, _inputs, _execution -> {:error, :unused} end, stop)
  end

  defp process_until_check(state, stop, remaining \\ 5)
  defp process_until_check(state, _stop, 0), do: state

  defp process_until_check(state, stop, remaining) do
    state = Coordinator.process(state, stop)

    case state.linear.job && state.linear.job.key do
      {:publish, _issue_id, _activity_id, _revision} -> state |> finish_job(stop) |> process_until_check(stop, remaining - 1)
      _ -> state
    end
  end

  defp failing_commit_store do
    spawn_link(fn -> failing_commit_store_loop() end)
  end

  defp failing_commit_store_loop do
    receive do
      {:"$gen_call", from, {:commit, _run, _event_id, _event}} ->
        GenServer.reply(from, {:error, :injected_commit_failure})
        failing_commit_store_loop()

      {:"$gen_call", from, {:linear_task, _task}} ->
        GenServer.reply(from, :ok)
        failing_commit_store_loop()
    end
  end

  defp definition do
    agents = %{
      "default-cloud" => agent("DefaultCloud", false),
      "high-effort-cloud" => agent("HighEffortCloud", false),
      "daybreak-cloud" => agent("DaybreakCloud", true)
    }

    %{entry: "implement", stages: %{"implement" => %{type: :agent, agent: "default-cloud"}}, agents: agents}
  end

  defp agent(name, daybreak), do: %{name: name, revision: "revision-1", model: "gpt-6-luna", reasoning_effort: "medium", daybreak: daybreak, instructions: [], skills: []}

  defp task(event, run_id, agent_id, session_id, revision) do
    route = route(agent_id)

    %{
      issue_id: event.issue_id,
      run_id: run_id,
      status: :acknowledged,
      agent_id: agent_id,
      route: Map.take(route, ["organization_id", "app_user_id", "oauth_client_id", "agent_id"]),
      app_user_id: route["app_user_id"],
      oauth_client_id: route["oauth_client_id"],
      session_id: session_id,
      activity_id: "activity-#{agent_id}",
      event: event,
      authorized_stage: "implement",
      authorized_until: System.system_time(:millisecond) + 60_000,
      scope_revision: revision,
      publications: %{},
      assignment_history: []
    }
  end

  defp event(kind, id, agent_id, session_id, timestamp) do
    route = route(agent_id)

    %{
      id: id,
      kind: kind,
      issue_id: "issue-1",
      session_id: session_id,
      app_user_id: route["app_user_id"],
      verified_app_user_id: route["app_user_id"],
      oauth_client_id: route["oauth_client_id"],
      organization_id: "rig",
      timestamp: timestamp,
      activity_id: nil
    }
  end

  defp route(agent_id) do
    {:ok, routes} = AgentMapping.routes(config())
    Enum.find(routes, &(&1["agent_id"] == agent_id))
  end

  defp config do
    %{
      "organization_id" => "rig",
      "team_id" => "dev",
      "app_user_id" => "app-default",
      "oauth_client_id" => "client-default",
      "webhook_secret_env" => "HOOK_DEFAULT",
      "client_secret_env" => "CLIENT_DEFAULT",
      "agent_id" => "default-cloud",
      "cloud_agents" => [
        %{"app_user_id" => "app-high", "oauth_client_id" => "client-high", "webhook_secret_env" => "HOOK_HIGH", "client_secret_env" => "CLIENT_HIGH", "agent_id" => "high-effort-cloud"},
        %{"app_user_id" => "app-daybreak", "oauth_client_id" => "client-daybreak", "webhook_secret_env" => "HOOK_DAYBREAK", "client_secret_env" => "CLIENT_DAYBREAK", "agent_id" => "daybreak-cloud"}
      ],
      "store_path" => "/tmp/linear-routing.sqlite",
      "workspace_root" => "/tmp/workspaces",
      "workstream_path" => "/tmp/software-change.yaml",
      "rig_label" => "factory:rig"
    }
  end
end
