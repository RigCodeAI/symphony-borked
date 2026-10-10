defmodule SymphonyElixir.LinearStoreTest do
  use ExUnit.Case, async: true

  alias Exqlite.Sqlite3
  alias SymphonyElixir.WorkstreamStore

  setup do
    root = Path.join(System.tmp_dir!(), "linear-store-#{System.unique_integer([:positive])}")
    path = Path.join([root, "private", "state.sqlite3"])

    on_exit(fn -> File.rm_rf!(root) end)

    %{root: root, path: path}
  end

  test "stores, commits and reloads Linear events and tasks after restart", context do
    assert {:ok, store} = start_store(context.path)
    assert {:ok, %{events: %{}, tasks: %{}}} = WorkstreamStore.linear_load(store)

    event = %{kind: :issue_updated, issue_id: "issue-1", data: %{"state" => "In Progress"}}
    task = %{issue_id: "issue-1", status: :delegated, metadata: %{"attempt" => 1}}

    assert :ok = WorkstreamStore.linear_receive(store, "delivery-1", event)

    event_record = %{id: "delivery-1", event: event, status: :processed}
    assert :ok = WorkstreamStore.linear_commit(store, event_record, task)

    assert {:ok, %{events: %{"delivery-1" => ^event_record}, tasks: %{"issue-1" => ^task}}} =
             WorkstreamStore.linear_load(store)

    assert :ok = GenServer.stop(store)

    assert {:ok, reopened} = start_store(context.path)

    assert {:ok, %{events: %{"delivery-1" => ^event_record}, tasks: %{"issue-1" => ^task}}} =
             WorkstreamStore.linear_load(reopened)

    assert :ok = GenServer.stop(reopened)
  end

  test "replayed processed events compare content and reject identity conflicts", context do
    assert {:ok, store} = start_store(context.path)
    event = %{kind: :issue_updated, issue_id: "issue-1"}

    assert :ok = WorkstreamStore.linear_receive(store, "delivery-1", event)
    processed = %{id: "delivery-1", event: event, status: :processed}
    assert :ok = WorkstreamStore.linear_commit(store, processed, nil)
    assert {:duplicate, ^processed} = WorkstreamStore.linear_receive(store, "delivery-1", event)

    assert {:error, :event_identity_conflict} =
             WorkstreamStore.linear_receive(store, "delivery-1", %{kind: :issue_removed, issue_id: "issue-1"})

    assert {:ok, %{events: %{"delivery-1" => ^processed}, tasks: %{}}} = WorkstreamStore.linear_load(store)
    assert :ok = GenServer.stop(store)
  end

  test "rolls back the event update when the task upsert fails", context do
    assert {:ok, store} = start_store(context.path)
    event = %{kind: :issue_updated, issue_id: "issue-1"}
    original_task = %{issue_id: "issue-1", status: :delegated}
    assert :ok = WorkstreamStore.linear_receive(store, "delivery-1", event)
    assert :ok = WorkstreamStore.linear_task(store, original_task)
    assert :ok = GenServer.stop(store)

    execute_sql(
      context.path,
      "CREATE TRIGGER reject_linear_task_update BEFORE UPDATE ON linear_tasks " <>
        "BEGIN SELECT RAISE(ABORT, 'blocked task update'); END;"
    )

    assert {:ok, store} = start_store(context.path)
    updated_event = %{id: "delivery-1", event: event, status: :processed}
    updated_task = %{issue_id: "issue-1", status: :stopped}

    assert {:error, _reason} = WorkstreamStore.linear_commit(store, updated_event, updated_task)

    assert {:ok, %{events: %{"delivery-1" => %{status: :pending}}, tasks: %{"issue-1" => ^original_task}}} =
             WorkstreamStore.linear_load(store)

    assert :ok = GenServer.stop(store)
  end

  test "only the owner can commit events or upsert tasks", context do
    assert {:ok, store} = start_store(context.path)
    event = %{kind: :issue_updated, issue_id: "issue-1"}
    assert :ok = WorkstreamStore.linear_receive(store, "delivery-1", event)

    parent = self()

    spawn(fn ->
      send(parent, {:commit, WorkstreamStore.linear_commit(store, %{id: "delivery-1", event: event, status: :processed}, nil)})
      send(parent, {:task, WorkstreamStore.linear_task(store, %{issue_id: "issue-1", status: :delegated})})
    end)

    assert_receive {:commit, {:error, :not_owner}}, 2_000
    assert_receive {:task, {:error, :not_owner}}, 2_000
    assert {:ok, %{events: %{"delivery-1" => %{status: :pending}}, tasks: %{}}} = WorkstreamStore.linear_load(store)

    assert {:error, :invalid_linear_event} =
             WorkstreamStore.linear_receive(store, "unsafe", %{pid: self()})

    assert {:error, :invalid_linear_task} =
             WorkstreamStore.linear_task(store, %{issue_id: "issue-1", callback: fn -> :ok end})

    spawn(fn ->
      send(parent, {:receive, WorkstreamStore.linear_receive(store, "delivery-2", event)})
    end)

    assert_receive {:receive, :ok}, 2_000

    assert {:ok, %{events: %{"delivery-1" => %{status: :pending}, "delivery-2" => %{status: :pending}}, tasks: %{}}} =
             WorkstreamStore.linear_load(store)

    assert :ok = GenServer.stop(store)
  end

  test "a failed migration 2 rolls back its schema and preserves migration 1 data", context do
    assert {:ok, store} = start_store(context.path, migrations: [{1, migration_v1()}])

    run = %{
      id: "run-1",
      task_id: "task-1",
      status: :running,
      attempts: [],
      operations: %{}
    }

    assert :ok = WorkstreamStore.commit(store, run, "event-1", %{kind: :queued})
    assert :ok = GenServer.stop(store)

    failing_migrations = [
      {1, migration_v1()},
      {2,
       "CREATE TABLE partial_linear_migration (value TEXT); " <>
         "INSERT INTO partial_linear_migration VALUES ('x'); INVALID SQL;"}
    ]

    assert {:error, {:migration_failed, 2, _reason}} =
             start_store(context.path, migrations: failing_migrations)

    assert [] = database_rows(context.path, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'partial_linear_migration'")
    assert [[1]] = database_rows(context.path, "PRAGMA user_version")

    assert {:ok, reopened} = start_store(context.path)
    assert {:ok, loaded} = WorkstreamStore.fetch(reopened, "run-1")
    assert Map.drop(loaded, [:questions, :inbox, :activity_ids, :continuation]) == run
    assert loaded.questions == %{}
    assert loaded.inbox == []
    assert loaded.activity_ids == %{}
    assert loaded.continuation == nil
    assert {:ok, %{events: %{}, tasks: %{}}} = WorkstreamStore.linear_load(reopened)
    assert :ok = GenServer.stop(reopened)
  end

  defp start_store(path, opts \\ []) do
    WorkstreamStore.start_link(Keyword.merge([path: path, owner: self()], opts))
  end

  defp migration_v1 do
    """
    CREATE TABLE runs (
      run_id TEXT PRIMARY KEY,
      task_id TEXT NOT NULL,
      payload BLOB NOT NULL
    );
    CREATE TABLE events (
      event_id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      payload BLOB NOT NULL
    );
    CREATE TABLE stage_attempts (
      run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      payload BLOB NOT NULL,
      PRIMARY KEY (run_id, attempt_id)
    );
    CREATE TABLE operations (
      operation_id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      payload BLOB NOT NULL
    );
    """
  end

  defp execute_sql(path, sql) do
    {:ok, db} = Sqlite3.open(path)
    :ok = Sqlite3.execute(db, sql)
    :ok = Sqlite3.close(db)
  end

  defp database_rows(path, sql) do
    {:ok, db} = Sqlite3.open(path, mode: :readonly)
    {:ok, statement} = Sqlite3.prepare(db, sql)
    :ok = Sqlite3.bind(statement, [])
    {:ok, rows} = Sqlite3.fetch_all(db, statement)
    :ok = Sqlite3.release(db, statement)
    :ok = Sqlite3.close(db)
    rows
  end
end
