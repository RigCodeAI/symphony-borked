defmodule SymphonyElixir.WorkstreamChildStoreTest do
  use ExUnit.Case, async: true

  alias Exqlite.Sqlite3
  alias SymphonyElixir.{WorkstreamRun, WorkstreamStore}

  setup do
    root = Path.join(System.tmp_dir!(), "workstream-child-store-#{System.unique_integer([:positive])}")
    path = Path.join([root, "private", "state.sqlite3"])

    on_exit(fn -> File.rm_rf!(root) end)

    %{root: root, path: path}
  end

  test "commits the waiting parent, linked child, attempts, operations, events, and evidence outbox as one handoff", context do
    parent = parent(context.root)
    child = child(context.root)
    {waiting_parent, linked_child, intent} = planned_handoff(parent, child)

    assert {:ok, store} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, parent, "parent-created", %{kind: :task_queued})

    assert {:ok, ^waiting_parent, ^linked_child} =
             WorkstreamStore.commit_child_handoff(store, parent, waiting_parent, linked_child, intent)

    assert {:ok, ^waiting_parent} = WorkstreamStore.fetch(store, parent.id)
    assert {:ok, ^linked_child} = WorkstreamStore.fetch(store, linked_child.id)

    assert {:ok, %{run_id: parent_id, event: %{kind: :child_handoff_started, intent: ^intent}}} =
             WorkstreamStore.event(store, parent_event_id(intent))

    assert parent_id == parent.id

    assert {:ok, %{run_id: child_id, event: %{kind: :child_handoff_linked, intent: ^intent}}} =
             WorkstreamStore.event(store, child_event_id(intent))

    assert child_id == linked_child.id
    assert :ok = GenServer.stop(store)

    parent_attempt_id = intent.parent_attempt_id
    parent_id = parent.id

    assert [[^parent_attempt_id, ^parent_id]] =
             database_rows(context.path, "SELECT attempt_id, run_id FROM stage_attempts WHERE run_id = '#{parent.id}'")

    assert [[^parent_attempt_id, ^parent_id]] =
             database_rows(context.path, "SELECT operation_id, run_id FROM operations WHERE run_id = '#{parent.id}'")

    assert [[2]] = database_rows(context.path, "SELECT COUNT(*) FROM events WHERE event_id LIKE 'child-handoff/%'")
  end

  test "exact replay after restart returns the stored snapshots", context do
    parent = parent(context.root)
    child = child(context.root)
    {waiting_parent, linked_child, intent} = planned_handoff(parent, child)

    assert {:ok, store} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, parent, "parent-created", %{kind: :task_queued})

    assert {:ok, ^waiting_parent, ^linked_child} =
             WorkstreamStore.commit_child_handoff(store, parent, waiting_parent, linked_child, intent)

    persisted_parent = Map.put(waiting_parent, :replay_probe, "saved")
    assert :ok = WorkstreamStore.commit(store, persisted_parent, "parent-later", %{kind: :parent_updated})
    assert :ok = GenServer.stop(store)

    assert {:ok, restarted_store} = start_store(context.path)

    assert {:duplicate, ^persisted_parent, ^linked_child} =
             WorkstreamStore.commit_child_handoff(restarted_store, parent, waiting_parent, linked_child, intent)

    assert {:ok, ^persisted_parent} = WorkstreamStore.fetch(restarted_store, parent.id)
    assert {:ok, ^linked_child} = WorkstreamStore.fetch(restarted_store, linked_child.id)
    assert :ok = GenServer.stop(restarted_store)
    assert [[2]] = database_rows(context.path, "SELECT COUNT(*) FROM events WHERE event_id LIKE 'child-handoff/%'")
  end

  test "rejects a stale parent and a replay with changed child input pins", context do
    parent = parent(context.root)
    {waiting_parent, linked_child, intent} = planned_handoff(parent, child(context.root))

    assert {:ok, store} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, parent, "parent-created", %{kind: :task_queued})

    stale_parent = Map.put(parent, :unpersisted_change, true)
    {stale_waiting, stale_child, stale_intent} = planned_handoff(stale_parent, child(context.root))

    assert {:error, :parent_state_conflict} =
             WorkstreamStore.commit_child_handoff(store, stale_parent, stale_waiting, stale_child, stale_intent)

    assert {:ok, ^waiting_parent, ^linked_child} =
             WorkstreamStore.commit_child_handoff(store, parent, waiting_parent, linked_child, intent)

    changed_child = child(context.root, "changed-child-task")
    {changed_waiting, changed_linked, changed_intent} = planned_handoff(parent, changed_child)

    assert changed_intent.handoff_id == intent.handoff_id
    refute changed_intent.fingerprint == intent.fingerprint

    assert {:error, :child_handoff_inputs_changed} =
             WorkstreamStore.commit_child_handoff(store, parent, changed_waiting, changed_linked, changed_intent)

    assert {:ok, ^waiting_parent} = WorkstreamStore.fetch(store, parent.id)
    assert {:ok, ^linked_child} = WorkstreamStore.fetch(store, linked_child.id)
    assert :ok = GenServer.stop(store)
  end

  test "rejects a child run that already exists without a matching handoff", context do
    parent = parent(context.root)
    {waiting_parent, linked_child, intent} = planned_handoff(parent, child(context.root))

    assert {:ok, store} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, parent, "parent-created", %{kind: :task_queued})
    assert :ok = WorkstreamStore.commit(store, linked_child, "unrelated-child-event", %{kind: :queued})

    assert {:error, :child_run_conflict} =
             WorkstreamStore.commit_child_handoff(store, parent, waiting_parent, linked_child, intent)

    assert {:ok, ^parent} = WorkstreamStore.fetch(store, parent.id)
    assert {:ok, ^linked_child} = WorkstreamStore.fetch(store, linked_child.id)
    assert :ok = GenServer.stop(store)
  end

  test "rolls back the parent update and child insert when persisting the child event fails", context do
    parent = parent(context.root)
    {waiting_parent, linked_child, intent} = planned_handoff(parent, child(context.root))

    assert {:ok, store} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, parent, "parent-created", %{kind: :task_queued})
    assert :ok = GenServer.stop(store)

    install_child_event_failure(context.path, linked_child.id)

    assert {:ok, store} = start_store(context.path)

    assert {:error, _reason} =
             WorkstreamStore.commit_child_handoff(store, parent, waiting_parent, linked_child, intent)

    assert {:ok, ^parent} = WorkstreamStore.fetch(store, parent.id)
    assert :not_found = WorkstreamStore.fetch(store, linked_child.id)
    assert :ok = GenServer.stop(store)

    assert [["parent-created"]] = database_rows(context.path, "SELECT event_id FROM events ORDER BY event_id")
    assert [[1]] = database_rows(context.path, "SELECT COUNT(*) FROM runs")
    assert [[0]] = database_rows(context.path, "SELECT COUNT(*) FROM stage_attempts")
    assert [[0]] = database_rows(context.path, "SELECT COUNT(*) FROM operations")
    assert [[0]] = database_rows(context.path, "SELECT COUNT(*) FROM evidence_outbox")
  end

  test "requires the store owner and honors the common capture write hold", context do
    parent = parent(context.root)
    {waiting_parent, linked_child, intent} = planned_handoff(parent, child(context.root))

    assert {:ok, store} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, parent, "parent-created", %{kind: :task_queued})

    caller = self()

    spawn(fn ->
      send(caller, {:child_handoff_from_non_owner, WorkstreamStore.commit_child_handoff(store, parent, waiting_parent, linked_child, intent)})
    end)

    assert_receive {:child_handoff_from_non_owner, {:error, :not_owner}}, 2_000
    assert :not_found = WorkstreamStore.fetch(store, linked_child.id)

    token = make_ref()
    assert :ok = WorkstreamStore.begin_capture(store, token, self(), Path.join(context.root, "capture.sqlite3"))
    assert WorkstreamStore.capture_write_allowed?(:sys.get_state(store)) == false

    assert {:error, :capture_in_progress} =
             WorkstreamStore.commit_child_handoff(store, parent, waiting_parent, linked_child, intent)

    assert :ok = WorkstreamStore.end_capture(store, token)
    assert WorkstreamStore.capture_write_allowed?(:sys.get_state(store)) == true
    assert :ok = GenServer.stop(store)
  end

  defp planned_handoff(parent, child) do
    assert {:ok, waiting_parent, linked_child, intent} = WorkstreamRun.begin_child(parent, child)
    {waiting_parent, linked_child, intent}
  end

  defp parent(root) do
    definition = %{
      name: "test-child-parent",
      entry: "implement",
      inputs: ["approved-gap"],
      input_values: %{"approved-gap" => "isolated fixture"},
      definitions: %{},
      agents: %{},
      stages: %{
        "implement" => %{
          id: "implement",
          type: :child,
          inputs: ["approved-gap"],
          outputs: ["child-result"],
          input_mapping: %{"task" => "approved-gap"},
          child_definition: %{inputs: ["task"]},
          next: "check"
        },
        "check" => %{id: "check", type: :check, inputs: ["child-result"], outputs: ["result"]}
      }
    }

    WorkstreamRun.new("test-parent-task", definition, Path.join(root, "parent"),
      workspace_root: root,
      branch: "cycle/test-parent",
      codex_command: "never-executed"
    )
  end

  defp child(root, task_id \\ "test-child-task") do
    definition = %{
      name: "software-change",
      entry: "implement",
      inputs: ["task"],
      input_values: %{"task" => "isolated fixture"},
      definitions: %{},
      agents: %{},
      stages: %{"implement" => %{id: "implement", type: :agent}}
    }

    WorkstreamRun.new(task_id, definition, Path.join(root, "child"),
      workspace_root: root,
      branch: "cycle/test-child",
      codex_command: "never-executed"
    )
  end

  defp parent_event_id(intent), do: "child-handoff/#{intent.handoff_id}/parent"
  defp child_event_id(intent), do: "child-handoff/#{intent.handoff_id}/child"

  defp start_store(path), do: WorkstreamStore.start_link(path: path, owner: self())

  defp install_child_event_failure(path, child_id) do
    {:ok, db} = Sqlite3.open(path)

    assert :ok =
             Sqlite3.execute(
               db,
               "CREATE TRIGGER fail_child_handoff_event BEFORE INSERT ON events " <>
                 "WHEN NEW.run_id = '#{child_id}' BEGIN SELECT RAISE(ABORT, 'injected child event failure'); END;"
             )

    assert :ok = Sqlite3.close(db)
  end

  defp database_rows(path, sql) do
    {:ok, db} = Sqlite3.open(path, mode: [:readonly])
    {:ok, statement} = Sqlite3.prepare(db, sql)
    :ok = Sqlite3.bind(statement, [])
    {:ok, rows} = Sqlite3.fetch_all(db, statement)
    :ok = Sqlite3.release(db, statement)
    :ok = Sqlite3.close(db)
    rows
  end
end
