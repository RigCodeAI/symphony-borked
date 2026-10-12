defmodule SymphonyElixir.WorkstreamStoreTest do
  use ExUnit.Case, async: true
  import Bitwise

  alias Exqlite.Sqlite3
  alias SymphonyElixir.WorkstreamStore

  setup do
    root = Path.join(System.tmp_dir!(), "workstream-store-#{System.unique_integer([:positive])}")
    path = Path.join([root, "private", "state.sqlite3"])

    on_exit(fn -> File.rm_rf!(root) end)

    %{root: root, path: path}
  end

  test "creates private storage and atomically persists a run, attempts, operations, and event", context do
    assert {:ok, pid} = start_store(context.path)
    assert {:ok, []} = WorkstreamStore.load(pid)
    assert :not_found = WorkstreamStore.fetch(pid, "run-1")

    run = run("run-1", "task-1")
    event = %{kind: :task_queued, source: "test", fingerprint: String.duplicate("a", 64)}
    assert :ok = WorkstreamStore.commit(pid, run, "event-1", event)
    assert {:ok, ^run} = WorkstreamStore.fetch(pid, "run-1")
    assert {:ok, [^run]} = WorkstreamStore.load(pid)
    assert {:ok, %{run_id: "run-1", event: ^event}} = WorkstreamStore.event(pid, "event-1")
    assert :not_found = WorkstreamStore.event(pid, "missing-event")

    assert :ok = GenServer.stop(pid)
    assert {:error, {:store_unavailable, _reason}} = WorkstreamStore.load(pid)
    assert (File.stat!(context.path).mode &&& 0o777) == 0o600
    assert (File.stat!(Path.dirname(context.path)).mode &&& 0o777) == 0o700
    assert (File.stat!(Path.dirname(Path.dirname(context.path))).mode &&& 0o777) == 0o700

    assert [["attempt-1"]] = database_rows(context.path, "SELECT attempt_id FROM stage_attempts")
    assert [["op-1", "run-1"]] = database_rows(context.path, "SELECT operation_id, run_id FROM operations")
    assert [["event-1", "run-1"]] = database_rows(context.path, "SELECT event_id, run_id FROM events")
    assert [[3]] = database_rows(context.path, "PRAGMA user_version")
    assert [["delete"]] = database_rows(context.path, "PRAGMA journal_mode")
  end

  test "replaying an event returns its original run and writes no second run", context do
    assert {:ok, pid} = start_store(context.path)
    first_run = run("run-1", "task-1")
    second_run = run("run-2", "task-2")

    assert :ok = WorkstreamStore.commit(pid, first_run, "same-event", %{kind: :queued})
    assert {:duplicate, "run-1"} = WorkstreamStore.commit(pid, second_run, "same-event", %{kind: :different})
    assert :not_found = WorkstreamStore.fetch(pid, "run-2")
    assert {:ok, [^first_run]} = WorkstreamStore.load(pid)
    assert :ok = GenServer.stop(pid)
    assert [["same-event", "run-1"]] = database_rows(context.path, "SELECT event_id, run_id FROM events")
  end

  test "owner capture holds every write, preserves rows and keeps exclusive ownership", context do
    assert {:ok, store} = start_store(context.path)
    snapshot_dir = Path.join(context.root, "capture")
    File.mkdir!(snapshot_dir)
    File.chmod!(snapshot_dir, 0o700)
    target = Path.join(snapshot_dir, "state.sqlite")
    original = run("run-1", "task-1")
    assert :ok = WorkstreamStore.commit(store, original, "initial", %{kind: :queued})
    token = make_ref()
    assert :ok = WorkstreamStore.begin_capture(store, token, self(), target)
    refute WorkstreamStore.capture_write_allowed?(:sys.get_state(store))
    assert {:error, :capture_in_progress} = WorkstreamStore.commit(store, original, "later", %{kind: :queued})
    assert {:error, :capture_in_progress} = WorkstreamStore.linear_receive(store, "delivery", %{kind: :created})
    assert {:error, :capture_in_progress} = WorkstreamStore.linear_task(store, %{issue_id: "issue"})
    assert {:error, :capture_in_progress} = WorkstreamStore.acknowledge_evidence(store, "initial")
    assert {:error, :capture_not_authorized} = WorkstreamStore.end_capture(store, make_ref())
    assert {:ok, %{schema: %{"user_version" => 3}}} = WorkstreamStore.snapshot_capture(store, token)
    assert [["initial"]] = database_rows(target, "SELECT event_id FROM events")
    assert {:error, {:database_ownership_conflict, _, _}} = start_store(context.path)
    assert :ok = WorkstreamStore.end_capture(store, token)
    assert WorkstreamStore.capture_write_allowed?(:sys.get_state(store))
    assert :ok = WorkstreamStore.commit(store, original, "later", %{kind: :queued})
    assert [["initial"]] = database_rows(target, "SELECT event_id FROM events")
    assert :ok = GenServer.stop(store)
  end

  test "inspects a captured current image without changing its bytes", context do
    expected = run("run-1", "task-1")
    {source_path, bytes} = captured_store_image(context, expected)
    digest = digest(bytes)

    assert {:ok, inspected} = WorkstreamStore.inspect_recovery_image(bytes)
    assert inspected.runs == [expected]
    assert inspected.linear == %{events: %{}, tasks: %{}}
    assert inspected.run_ids == ["run-1"]
    assert inspected.operation_ids == ["op-1"]
    assert inspected.linear_delivery_ids == []
    assert File.read!(source_path) == bytes
    assert digest(File.read!(source_path)) == digest
  end

  test "rejects invalid recovery image payloads without changing the captured source", context do
    run = run("run-1", "task-1")
    {source_path, original_bytes} = captured_store_image(context, run)
    source_digest = digest(original_bytes)

    compressed_run = :erlang.term_to_binary(run, compressed: 1)
    pid_run = :erlang.term_to_binary(Map.put(run, :unsafe, self()))
    reference_run = :erlang.term_to_binary(Map.put(run, :unsafe, make_ref()))
    function_run = :erlang.term_to_binary(Map.put(run, :unsafe, &String.length/1))
    deep_run = :erlang.term_to_binary(Map.put(run, :unsafe, nested_term(:value, 65)))

    oversized_run =
      :erlang.term_to_binary(Map.put(run, :unsafe, :binary.copy("x", 4 * 1024 * 1024 + 1)))

    cases = [
      {
        "SQL run ID mismatch",
        {:sql,
         "UPDATE runs SET run_id = 'other-run' WHERE run_id = 'run-1'; " <>
           "UPDATE events SET run_id = 'other-run'; " <>
           "UPDATE stage_attempts SET run_id = 'other-run'; " <>
           "UPDATE operations SET run_id = 'other-run'"},
        :invalid_recovery_image
      },
      {
        "SQL task ID mismatch",
        {:sql, "UPDATE runs SET task_id = 'other-task' WHERE run_id = 'run-1'"},
        :invalid_recovery_image
      },
      {
        "orphan event",
        {:sql, "UPDATE events SET run_id = 'missing-run' WHERE event_id = 'initial'"},
        :invalid_recovery_image
      },
      {
        "changed attempt row",
        {:table_payload, "stage_attempts", "attempt_id", "attempt-1", :erlang.term_to_binary(%{id: "different-attempt"})},
        :invalid_recovery_image
      },
      {
        "changed operation row",
        {:table_payload, "operations", "operation_id", "op-1", :erlang.term_to_binary(%{kind: :different})},
        :invalid_recovery_image
      },
      {"extra table", {:sql, "CREATE TABLE recovery_extra (value TEXT)"}, :invalid_recovery_image},
      {
        "extra trigger",
        {:sql, "CREATE TRIGGER recovery_extra AFTER INSERT ON runs BEGIN SELECT 1; END"},
        :invalid_recovery_image
      },
      {
        "unsupported schema version",
        {:sql, "PRAGMA user_version = 4"},
        :unsupported_recovery_schema
      },
      {
        "nonempty evidence outbox",
        {:sql, "INSERT INTO evidence_outbox (event_id,snapshot,event) VALUES ('pending',x'01',x'01')"},
        :invalid_recovery_image
      },
      {"compressed ETF", {:run_payload, compressed_run}, :invalid_recovery_image},
      {"pid ETF value", {:run_payload, pid_run}, :invalid_recovery_image},
      {"reference ETF value", {:run_payload, reference_run}, :invalid_recovery_image},
      {"function ETF value", {:run_payload, function_run}, :invalid_recovery_image},
      {"deep ETF value", {:run_payload, deep_run}, :invalid_recovery_image},
      {"oversized ETF value", {:run_payload, oversized_run}, :invalid_recovery_image}
    ]

    cases
    |> Enum.with_index()
    |> Enum.each(fn {{name, mutation, expected_error}, index} ->
      copy_path = Path.join(context.root, "mutated-#{index}.sqlite")
      :ok = File.cp(source_path, copy_path)
      apply_recovery_mutation(copy_path, mutation)
      bytes = File.read!(copy_path)

      assert {:error, ^expected_error} = WorkstreamStore.inspect_recovery_image(bytes), name
      assert File.read!(source_path) == original_bytes
      assert digest(File.read!(source_path)) == source_digest
    end)
  end

  test "loading a legacy run adds empty reply state without replacing pinned state", context do
    expected =
      run("run-1", "task-1")
      |> Map.merge(%{
        policy: %{version: 1, lifecycle_sha256: "historical-policy"},
        definition: %{definitions: %{"agent" => %{sha256: "historical-agent"}}},
        execution: %{codex_command: "pinned-command"},
        artifacts: %{"candidate" => %{sha256: "existing-candidate"}},
        pending_wait: %{id: "attempt-1/wait", artifact_ids: %{"candidate" => "existing-candidate"}},
        current_attempt_id: "attempt-1",
        thread_id: "existing-thread",
        session_id: "existing-session",
        future_extension: %{value: "preserved"}
      })

    legacy = Map.drop(expected, [:questions, :inbox, :activity_ids, :continuation])
    assert {:ok, store} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, legacy, "legacy-snapshot", %{kind: :waiting})
    assert :ok = GenServer.stop(store)

    assert {:ok, reopened} = start_store(context.path)
    assert {:ok, ^expected} = WorkstreamStore.fetch(reopened, "run-1")
    assert {:ok, [^expected]} = WorkstreamStore.load(reopened)
    assert {:ok, %{run_id: "run-1", event: %{kind: :waiting}}} = WorkstreamStore.event(reopened, "legacy-snapshot")
    assert :ok = WorkstreamStore.commit(reopened, expected, "normalized-snapshot", %{kind: :loaded})
    assert :ok = GenServer.stop(reopened)

    assert {:ok, reopened} = start_store(context.path)
    assert {:ok, ^expected} = WorkstreamStore.fetch(reopened, "run-1")
    assert :ok = GenServer.stop(reopened)
    assert [["attempt-1"]] = database_rows(context.path, "SELECT attempt_id FROM stage_attempts")
    assert [["op-1", "run-1"]] = database_rows(context.path, "SELECT operation_id, run_id FROM operations")
  end

  test "loading current reply state preserves saved questions, messages and deduplication", context do
    expected =
      run("run-1", "task-1")
      |> Map.merge(%{
        questions: %{"question-1" => %{status: :answered, reply: %{body: "blue", activity_id: "activity-1"}}},
        inbox: [%{body: "saved message", activity_id: "activity-2", delivered_attempt_id: nil}],
        activity_ids: %{"activity-1" => "reply-fingerprint"},
        continuation: %{thread_id: "prior-thread", question_id: "question-1"}
      })

    assert {:ok, store} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, expected, "current-snapshot", %{kind: :replied})
    assert :ok = GenServer.stop(store)
    assert {:ok, reopened} = start_store(context.path)
    assert {:ok, ^expected} = WorkstreamStore.fetch(reopened, "run-1")
    assert {:ok, [^expected]} = WorkstreamStore.load(reopened)
    assert :ok = GenServer.stop(reopened)
  end

  test "only the declared owner can commit while other processes can read", context do
    assert {:ok, pid} = start_store(context.path)
    first_run = run("run-1", "task-1")
    assert :ok = WorkstreamStore.commit(pid, first_run, "event-1", %{kind: :queued})
    parent = self()

    spawn(fn ->
      send(parent, {:external_load, WorkstreamStore.load(pid)})

      send(
        parent,
        {:external_commit, WorkstreamStore.commit(pid, run("run-2", "task-2"), "event-2", %{kind: :queued})}
      )
    end)

    assert_receive {:external_load, {:ok, [^first_run]}}, 2_000
    assert_receive {:external_commit, {:error, :not_owner}}, 2_000
    assert :not_found = WorkstreamStore.event(pid, "event-2")
    assert :ok = GenServer.stop(pid)
  end

  test "failed migration rolls back its DDL and keeps previously committed data", context do
    assert {:ok, pid} = start_store(context.path)
    run = run("run-1", "task-1")
    assert :ok = WorkstreamStore.commit(pid, run, "event-1", %{kind: :queued})
    assert :ok = GenServer.stop(pid)

    migrations = [
      {1, "SELECT 1"},
      {2, "SELECT 2"},
      {3, "SELECT 3"},
      {4, "CREATE TABLE partial_migration (value TEXT); INSERT INTO partial_migration VALUES ('x'); INVALID SQL;"}
    ]

    assert {:error, {:migration_failed, 4, _reason}} =
             start_store(context.path, migrations: migrations)

    assert [] = database_rows(context.path, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'partial_migration'")
    assert {:ok, reopened} = start_store(context.path)
    assert {:ok, ^run} = WorkstreamStore.fetch(reopened, "run-1")
    assert :ok = GenServer.stop(reopened)
  end

  test "rejects a database created by a newer migration list", context do
    migrations = [{1, "SELECT 1"}, {2, "SELECT 2"}, {3, "SELECT 3"}, {4, "SELECT 4"}]
    assert {:ok, pid} = start_store(context.path, migrations: migrations)
    assert :ok = GenServer.stop(pid)

    assert {:error, {:unsupported_schema_version, 4, 3}} = start_store(context.path)
  end

  test "exclusive locking prevents a second coordinator from opening the same database", context do
    assert {:ok, first} = start_store(context.path)

    assert {:error, {:database_ownership_conflict, path, _reason}} = start_store(context.path)
    assert path == context.path
    assert Process.alive?(first)
    assert :ok = GenServer.stop(first)
  end

  test "owner death closes the connection so a replacement can take ownership", context do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    owner =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    assert {:ok, first} = start_store(context.path, owner: owner)
    ref = Process.monitor(first)
    send(owner, :stop)

    assert_receive {:DOWN, ^ref, :process, ^first, {:owner_down, :normal}}, 2_000

    assert {:ok, replacement} = start_store(context.path)
    assert {:ok, []} = WorkstreamStore.load(replacement)
    assert :ok = GenServer.stop(replacement)
  end

  test "a killed owner releases the database for a replacement", context do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    owner = spawn(fn -> Process.sleep(:infinity) end)
    assert {:ok, store} = start_store(context.path, owner: owner)
    ref = Process.monitor(store)
    Process.exit(owner, :kill)

    assert_receive {:DOWN, ^ref, :process, ^store, :killed}, 2_000
    assert {:ok, replacement} = start_store(context.path)
    assert :ok = GenServer.stop(replacement)
  end

  test "a store crash exits its linked owner", context do
    parent = self()

    owner =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        {:ok, store} = start_store(context.path, owner: self())
        send(parent, {:ready, self(), store})

        receive do
          {:EXIT, ^store, reason} -> send(parent, {:store_exit, self(), reason})
        end
      end)

    assert_receive {:ready, ^owner, store}, 2_000
    Process.exit(store, :kill)
    assert_receive {:store_exit, ^owner, :killed}, 2_000
    Process.exit(owner, :normal)

    assert {:ok, replacement} = start_store(context.path)
    assert :ok = GenServer.stop(replacement)
  end

  test "leaves permissions on an existing database and its parent untouched", context do
    File.mkdir_p!(Path.dirname(context.path))
    File.write!(context.path, "")
    File.chmod!(context.path, 0o640)
    File.chmod!(Path.dirname(context.path), 0o750)

    assert {:ok, pid} = start_store(context.path)
    assert (File.stat!(context.path).mode &&& 0o777) == 0o640
    assert (File.stat!(Path.dirname(context.path)).mode &&& 0o777) == 0o750
    assert :ok = GenServer.stop(pid)
  end

  test "reports corrupt and non-map run payloads", context do
    assert {:ok, pid} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(pid, run("run-1", "task-1"), "event-1", %{kind: :queued})
    assert :ok = GenServer.stop(pid)

    replace_run_payload(context.path, :erlang.term_to_binary(:not_a_map))
    assert {:ok, pid} = start_store(context.path)
    assert {:error, :invalid_run_payload} = WorkstreamStore.fetch(pid, "run-1")
    assert {:error, :invalid_run_payload} = WorkstreamStore.load(pid)
    assert :ok = GenServer.stop(pid)

    replace_run_payload(context.path, <<255>>)
    assert {:ok, pid} = start_store(context.path)
    assert {:error, {:term_decode_failed, %ArgumentError{}}} = WorkstreamStore.fetch(pid, "run-1")
    assert {:error, {:term_decode_failed, %ArgumentError{}}} = WorkstreamStore.load(pid)
    assert :ok = GenServer.stop(pid)
  end

  test "validates start and commit inputs", context do
    dead_owner = spawn(fn -> :ok end)
    owner_ref = Process.monitor(dead_owner)
    assert_receive {:DOWN, ^owner_ref, :process, ^dead_owner, :normal}, 2_000

    assert {:error, :database_path_must_be_absolute} = WorkstreamStore.start_link(path: "relative.db", owner: self())
    assert {:error, :owner_must_be_a_pid} = WorkstreamStore.start_link(path: context.path, owner: :not_a_pid)
    assert {:error, :owner_not_alive} = WorkstreamStore.start_link(path: context.path, owner: dead_owner)
    assert {:error, {:invalid_start_options, _keys}} = WorkstreamStore.start_link(path: context.path, owner: self(), other: true)
    assert {:error, :invalid_start_options} = WorkstreamStore.start_link(:not_a_keyword_list)
    assert {:error, :invalid_start_options} = WorkstreamStore.start_link([:bad])
    assert {:error, {:invalid_start_options, _keys}} = WorkstreamStore.start_link(path: context.path, owner: self(), owner: self())

    assert {:error, {:invalid_migrations, []}} = WorkstreamStore.start_link(path: context.path, owner: self(), migrations: [])
    assert {:error, {:invalid_migrations, _}} = WorkstreamStore.start_link(path: context.path, owner: self(), migrations: [{2, "SELECT 1"}])
    assert {:error, {:invalid_migrations, _}} = WorkstreamStore.start_link(path: context.path, owner: self(), migrations: [:bad])

    assert {:ok, pid} = start_store(context.path)
    assert {:error, :invalid_commit} = WorkstreamStore.commit(pid, :not_a_run, "event", %{})
    assert {:error, :invalid_run_id} = WorkstreamStore.commit(pid, Map.put(run("", "task"), :id, ""), "event", %{})
    assert {:error, :invalid_task_id} = WorkstreamStore.commit(pid, Map.put(run("run", ""), :task_id, ""), "event", %{})
    assert {:error, :invalid_attempts} = WorkstreamStore.commit(pid, Map.put(run("run", "task"), :attempts, [%{}]), "event", %{})
    assert {:error, :invalid_operations} = WorkstreamStore.commit(pid, Map.put(run("run", "task"), :operations, %{bad: :not_a_map}), "event", %{})
    assert {:error, :invalid_event_id} = WorkstreamStore.commit(pid, run("run", "task"), "", %{})
    assert {:error, :invalid_commit} = WorkstreamStore.commit(pid, run("run", "task"), "event", :not_a_map)
    assert {:error, :invalid_run_id} = WorkstreamStore.fetch(pid, :not_a_string)
    assert {:error, :invalid_event_id} = WorkstreamStore.event(pid, "")
    assert :ok = GenServer.stop(pid)
  end

  test "rejects a database path whose parent is a regular file", context do
    File.mkdir_p!(context.root)
    File.write!(Path.join(context.root, "file"), "data")

    assert {:error, {:invalid_database_directory, path}} =
             start_store(Path.join([context.root, "file", "state.sqlite3"]))

    assert path == Path.join(context.root, "file")
    assert File.read!(Path.join(context.root, "file")) == "data"
  end

  defp start_store(path, opts \\ []) do
    WorkstreamStore.start_link(Keyword.merge([path: path, owner: self()], opts))
  end

  defp run(run_id, task_id) do
    %{
      id: run_id,
      task_id: task_id,
      status: :running,
      questions: %{},
      inbox: [],
      activity_ids: %{},
      continuation: nil,
      attempts: [%{id: "attempt-1", stage: "implement", status: :complete}],
      operations: %{"op-1" => %{kind: :publish, status: :pending}}
    }
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

  defp replace_run_payload(path, payload) do
    {:ok, db} = Sqlite3.open(path)
    {:ok, statement} = Sqlite3.prepare(db, "UPDATE runs SET payload = ?1 WHERE run_id = 'run-1'")
    :ok = Sqlite3.bind(statement, [{:blob, payload}])
    :done = Sqlite3.step(db, statement)
    :ok = Sqlite3.release(db, statement)
    :ok = Sqlite3.close(db)
  end

  defp captured_store_image(context, expected_run) do
    assert {:ok, store} = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, expected_run, "initial", %{kind: :queued})

    directory = Path.join(context.root, "inspection-capture")
    File.mkdir!(directory)
    File.chmod!(directory, 0o700)
    target = Path.join(directory, "state.sqlite")
    token = make_ref()

    assert :ok = WorkstreamStore.begin_capture(store, token, self(), target)
    assert {:ok, %{schema: %{"user_version" => 3}}} = WorkstreamStore.snapshot_capture(store, token)
    assert :ok = WorkstreamStore.end_capture(store, token)
    assert :ok = GenServer.stop(store)

    {target, File.read!(target)}
  end

  defp mutate_recovery_copy(path, sql) do
    {:ok, db} = Sqlite3.open(path)
    :ok = Sqlite3.execute(db, sql)
    :ok = Sqlite3.close(db)
  end

  defp apply_recovery_mutation(path, {:sql, sql}), do: mutate_recovery_copy(path, sql)

  defp apply_recovery_mutation(path, {:run_payload, payload}),
    do: replace_recovery_run_payload(path, payload)

  defp apply_recovery_mutation(path, {:table_payload, table, column, id, payload}),
    do: replace_recovery_payload(path, table, column, id, payload)

  defp replace_recovery_run_payload(path, payload),
    do: replace_recovery_payload(path, "runs", "run_id", "run-1", payload)

  defp replace_recovery_payload(path, table, identity_column, identity, payload) do
    {:ok, db} = Sqlite3.open(path)
    {:ok, statement} = Sqlite3.prepare(db, "UPDATE #{table} SET payload = ?1 WHERE #{identity_column} = ?2")
    :ok = Sqlite3.bind(statement, [{:blob, payload}, identity])
    :done = Sqlite3.step(db, statement)
    :ok = Sqlite3.release(db, statement)
    :ok = Sqlite3.close(db)
  end

  defp nested_term(term, depth) when depth > 0, do: nested_term([term], depth - 1)
  defp nested_term(term, _depth), do: term

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
