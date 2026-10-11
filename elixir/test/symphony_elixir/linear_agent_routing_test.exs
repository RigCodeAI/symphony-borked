defmodule SymphonyElixir.LinearAgentRoutingTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.Linear.{AgentMapping, Coordinator}
  alias SymphonyElixir.WorkstreamRun
  alias SymphonyElixir.WorkstreamStore
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

    %{store: store, path: path, config: config(), opts: []}
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

  defp finish_job(state, stop) do
    %{token: token} = state.linear.job
    assert_receive {:linear_result, ^token, result}, 5_000
    Coordinator.result(state, token, result, fn _state, _task_key, _task_id, _path, _inputs, _execution -> {:error, :unused} end, stop)
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
