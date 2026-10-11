defmodule SymphonyElixir.WorkstreamStoreReassignmentTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkstreamStore

  setup do
    root = Path.join(System.tmp_dir!(), "workstream-reassignment-#{System.unique_integer([:positive])}")
    path = Path.join([root, "private", "state.sqlite3"])

    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    %{path: path, store: store}
  end

  test "commits the run transition, Linear ownership and event receipt together", context do
    {expected_run, updated_run, expected_task, updated_task, event_record, run_event_id, run_event} = fixture()
    assert :ok = WorkstreamStore.commit(context.store, expected_run, "initial-run", %{kind: :queued})
    assert :ok = WorkstreamStore.linear_receive(context.store, event_record.id, event_record.event)
    assert :ok = WorkstreamStore.linear_task(context.store, expected_task)

    pending_task = %{expected_task | pending_reassignment: %{id: event_record.id, agent_id: "high-effort-cloud"}}
    assert :ok = WorkstreamStore.linear_task(context.store, pending_task)
    handled = %{event_record | status: :handled}

    assert :ok =
             WorkstreamStore.linear_reassign(
               context.store,
               expected_run,
               updated_run,
               pending_task,
               run_event_id,
               run_event,
               handled,
               updated_task
             )

    assert {:ok, ^updated_run} = WorkstreamStore.fetch(context.store, expected_run.id)
    assert {:ok, %{events: events, tasks: tasks}} = WorkstreamStore.linear_load(context.store)
    assert events[event_record.id] == handled
    assert tasks[expected_task.issue_id] == updated_task

    assert :ok = GenServer.stop(context.store)
    assert {:ok, reopened} = WorkstreamStore.start_link(path: context.path, owner: self())
    assert {:ok, ^updated_run} = WorkstreamStore.fetch(reopened, expected_run.id)
    assert {:ok, %{events: events, tasks: tasks}} = WorkstreamStore.linear_load(reopened)
    assert events[event_record.id] == handled
    assert tasks[expected_task.issue_id] == updated_task
    assert :ok = GenServer.stop(reopened)
  end

  test "rejects stale task ownership without changing either record", context do
    {expected_run, updated_run, expected_task, updated_task, event_record, run_event_id, run_event} = fixture()
    assert :ok = WorkstreamStore.commit(context.store, expected_run, "initial-run", %{kind: :queued})
    assert :ok = WorkstreamStore.linear_receive(context.store, event_record.id, event_record.event)
    pending_task = %{expected_task | pending_reassignment: %{id: event_record.id, agent_id: "high-effort-cloud"}}
    assert :ok = WorkstreamStore.linear_task(context.store, pending_task)

    stale_task = Map.put(pending_task, :scope_revision, 99)

    assert {:error, :linear_task_assignment_conflict} =
             WorkstreamStore.linear_reassign(
               context.store,
               expected_run,
               updated_run,
               stale_task,
               run_event_id,
               run_event,
               %{event_record | status: :handled},
               updated_task
             )

    assert {:ok, ^expected_run} = WorkstreamStore.fetch(context.store, expected_run.id)
    assert {:ok, %{events: events, tasks: tasks}} = WorkstreamStore.linear_load(context.store)
    assert events[event_record.id].status == :pending
    assert tasks[expected_task.issue_id] == pending_task
  end

  test "a run event collision rolls back the reassignment transaction", context do
    {expected_run, updated_run, expected_task, updated_task, event_record, run_event_id, run_event} = fixture()
    assert :ok = WorkstreamStore.commit(context.store, expected_run, "initial-run", %{kind: :queued})
    assert :ok = WorkstreamStore.commit(context.store, %{expected_run | id: "other-run", task_id: "other-task"}, run_event_id, %{kind: :unrelated})
    assert :ok = WorkstreamStore.linear_receive(context.store, event_record.id, event_record.event)
    pending_task = %{expected_task | pending_reassignment: %{id: event_record.id, agent_id: "high-effort-cloud"}}
    assert :ok = WorkstreamStore.linear_task(context.store, pending_task)

    assert {:error, {:run_event_already_committed, "other-run"}} =
             WorkstreamStore.linear_reassign(
               context.store,
               expected_run,
               updated_run,
               pending_task,
               run_event_id,
               run_event,
               %{event_record | status: :handled},
               updated_task
             )

    assert {:ok, ^expected_run} = WorkstreamStore.fetch(context.store, expected_run.id)
    assert {:ok, %{events: events, tasks: tasks}} = WorkstreamStore.linear_load(context.store)
    assert events[event_record.id].status == :pending
    assert tasks[expected_task.issue_id] == pending_task
  end

  defp fixture do
    event = %{
      id: "created-high-effort",
      kind: :created,
      issue_id: "issue-1",
      session_id: "session-high-effort",
      app_user_id: "app-high-effort",
      oauth_client_id: "client-high-effort",
      timestamp: 1_800_000_000_001
    }

    question = %{id: "question-1", prompt: "Which behavior?", status: :pending}
    wait = %{id: "wait-1", prompt: "Approve this candidate?", artifact_ids: %{"candidate" => "sha256"}}
    continuation = %{thread_id: "old-thread", questions: [%{id: question.id, prompt: question.prompt}]}

    expected_run = %{
      id: "run-1",
      task_id: "linear/issue-1",
      issue_id: "issue-1",
      status: :stopped,
      phase: :stopped,
      stage_id: "implement",
      current_attempt_id: nil,
      attempts: [],
      operations: %{},
      execution: %{secret_environment_names: ["DEFAULT_SECRET"]},
      definition: %{stages: %{}, agents: %{}},
      questions: %{question.id => question},
      inbox: [%{body: "saved reply", activity_id: "activity-1", delivered_attempt_id: nil}],
      activity_ids: %{"activity-1" => "reply-digest"},
      continuation: continuation,
      pending_wait: wait,
      agent_transitions: []
    }

    transition = %{
      id: event.id,
      from: %{agent_id: "default-cloud"},
      to: %{agent_id: "high-effort-cloud"},
      previous_session_id: "old-codex-session",
      previous_thread_id: "old-thread"
    }

    updated_run = %{expected_run | status: :waiting_for_answer, phase: :waiting_for_answer, agent_transitions: [transition]}
    old_publication = %{id: "old-publication", status: :published, content: %{"type" => "thought", "body" => "Progress"}}

    expected_task = %{
      issue_id: event.issue_id,
      run_id: expected_run.id,
      status: :reassigning,
      agent_id: "default-cloud",
      app_user_id: "app-default",
      oauth_client_id: "client-default",
      session_id: "session-default",
      activity_id: "old-ack-activity",
      event: %{kind: :created, timestamp: 1_800_000_000_000},
      authorized_stage: nil,
      authorized_until: 0,
      scope_revision: 2,
      pending_reassignment: %{id: event.id, agent_id: "high-effort-cloud"},
      publications: %{old_publication.id => old_publication},
      assignment_history: []
    }

    assignment = %{
      agent_id: expected_task.agent_id,
      app_user_id: expected_task.app_user_id,
      oauth_client_id: expected_task.oauth_client_id,
      session_id: expected_task.session_id,
      event: expected_task.event,
      publications: expected_task.publications
    }

    updated_task = %{
      expected_task
      | status: :acknowledging,
        agent_id: "high-effort-cloud",
        app_user_id: event.app_user_id,
        oauth_client_id: event.oauth_client_id,
        session_id: event.session_id,
        event: event,
        activity_id: "new-ack-activity",
        authorized_stage: nil,
        authorized_until: 0,
        pending_reassignment: nil,
        publications: %{},
        assignment_history: [assignment]
    }

    event_record = %{id: event.id, event: event, status: :pending}
    run_event_id = "linear-agent-transition/" <> event.id
    run_event = %{kind: :agent_reassigned, transition_id: event.id, agent_id: "high-effort-cloud"}

    {expected_run, updated_run, expected_task, updated_task, event_record, run_event_id, run_event}
  end
end
