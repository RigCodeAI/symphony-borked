defmodule SymphonyElixir.WorkstreamStore do
  @moduledoc """
  Durable SQLite storage for workstream runs, stage attempts, input events, and outgoing operations.

  One process owns the database connection. It keeps SQLite's exclusive lock for its lifetime,
  closes the connection when its owner exits, and is linked to that owner so a store failure also
  stops the coordinator.
  """

  use GenServer

  alias Exqlite.Sqlite3

  @busy_timeout_ms 100
  @directory_mode 0o700
  @file_mode 0o600

  @migration_v1 """
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

  @migration_v2 """
  CREATE TABLE linear_events (
    delivery_id TEXT PRIMARY KEY,
    payload BLOB NOT NULL
  );
  CREATE TABLE linear_tasks (
    issue_id TEXT PRIMARY KEY,
    payload BLOB NOT NULL
  );
  """

  @migrations [{1, @migration_v1}, {2, @migration_v2}]

  defmodule State do
    @moduledoc false
    defstruct [:path, :db, :owner, :owner_ref]
  end

  @type run_map :: map()

  @spec start_link(keyword()) :: GenServer.on_start() | {:error, term()}
  def start_link(opts) when is_list(opts) do
    with :ok <- validate_start_options(opts) do
      case Keyword.get(opts, :name) do
        nil -> GenServer.start(__MODULE__, opts)
        name -> GenServer.start(__MODULE__, opts, name: name)
      end
    end
  end

  def start_link(_opts), do: {:error, :invalid_start_options}

  @doc "Returns all stored run maps, ordered by run ID."
  @spec load(GenServer.server()) :: {:ok, [run_map()]} | {:error, term()}
  def load(server), do: safe_call(server, :load)

  @doc "Returns one stored run map or `:not_found`."
  @spec fetch(GenServer.server(), String.t()) :: {:ok, run_map()} | :not_found | {:error, term()}
  def fetch(server, run_id) when is_binary(run_id), do: safe_call(server, {:fetch, run_id})

  def fetch(_server, _run_id), do: {:error, :invalid_run_id}

  @doc "Returns the stored run and payload for one input event or `:not_found`."
  @spec event(GenServer.server(), String.t()) :: {:ok, %{run_id: String.t(), event: map()}} | :not_found | {:error, term()}
  def event(server, event_id) when is_binary(event_id) and event_id != "", do: safe_call(server, {:event, event_id})

  def event(_server, _event_id), do: {:error, :invalid_event_id}

  @doc "Atomically records a run snapshot, an input event, stage attempts, and outgoing operations."
  @spec commit(GenServer.server(), run_map(), String.t(), map()) ::
          :ok | {:duplicate, String.t()} | {:error, term()}
  def commit(server, run_map, event_id, event_map) do
    case validate_commit(run_map, event_id, event_map) do
      :ok -> safe_call(server, {:commit, run_map, event_id, event_map})
      {:error, _reason} = error -> error
    end
  end

  @doc "Stores a normalized Linear event once, returning its existing record on replay."
  @spec linear_receive(GenServer.server(), String.t(), map()) ::
          :ok | {:duplicate, map()} | {:error, term()}
  def linear_receive(server, delivery_id, normalized_event)
      when is_binary(delivery_id) and delivery_id != "" and is_map(normalized_event) do
    if safe_persistent_term?(normalized_event) do
      safe_call(server, {:linear_receive, delivery_id, normalized_event})
    else
      {:error, :invalid_linear_event}
    end
  end

  def linear_receive(_server, _delivery_id, _normalized_event), do: {:error, :invalid_linear_event}

  @doc "Loads all persisted Linear event and task maps keyed by their IDs."
  @spec linear_load(GenServer.server()) ::
          {:ok, %{events: %{optional(String.t()) => map()}, tasks: %{optional(String.t()) => map()}}}
          | {:error, term()}
  def linear_load(server), do: safe_call(server, :linear_load)

  @doc "Atomically updates a known Linear event and optionally upserts its task."
  @spec linear_commit(GenServer.server(), map(), map() | nil) :: :ok | {:error, term()}
  def linear_commit(server, event_record, task_record_or_nil) do
    case validate_linear_commit(event_record, task_record_or_nil) do
      :ok -> safe_call(server, {:linear_commit, event_record, task_record_or_nil})
      {:error, _reason} = error -> error
    end
  end

  @doc "Upserts one Linear task map. Only the store owner may write."
  @spec linear_task(GenServer.server(), map()) :: :ok | {:error, term()}
  def linear_task(server, task_map) do
    case validate_linear_task(task_map) do
      :ok -> safe_call(server, {:linear_task, task_map})
      {:error, _reason} = error -> error
    end
  end

  @impl true
  def init(opts) do
    with {:ok, path} <- normalized_path(Keyword.fetch!(opts, :path)),
         {:ok, migrations} <- normalize_migrations(Keyword.get(opts, :migrations, @migrations)),
         :ok <- ensure_parent_directory(Path.dirname(path)),
         :ok <- ensure_database_file(path) do
      case open_database(path) do
        {:ok, db} -> initialize_database(db, path, migrations, Keyword.fetch!(opts, :owner))
        {:error, reason} -> {:stop, reason}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:load, _from, %State{db: db} = state) do
    reply =
      with {:ok, rows} <- query(db, "SELECT payload FROM runs ORDER BY run_id"),
           {:ok, runs} <- decode_run_rows(rows, []) do
        {:ok, runs}
      end

    {:reply, reply, state}
  end

  def handle_call({:fetch, run_id}, _from, %State{db: db} = state) do
    reply =
      case query(db, "SELECT payload FROM runs WHERE run_id = ?1", [run_id]) do
        {:ok, [[payload]]} -> decode_run(payload)
        {:ok, []} -> :not_found
        {:ok, _rows} -> {:error, :invalid_run_row}
        {:error, _reason} = error -> error
      end

    {:reply, reply, state}
  end

  def handle_call({:event, event_id}, _from, %State{db: db} = state) do
    reply =
      case query(db, "SELECT run_id, payload FROM events WHERE event_id = ?1", [event_id]) do
        {:ok, [[run_id, payload]]} ->
          case decode_term(payload) do
            {:ok, event_map} when is_map(event_map) -> {:ok, %{run_id: run_id, event: event_map}}
            {:ok, _event_map} -> {:error, :invalid_event_payload}
            {:error, _reason} = error -> error
          end

        {:ok, []} ->
          :not_found

        {:ok, _rows} ->
          {:error, :invalid_event_row}

        {:error, _reason} = error ->
          error
      end

    {:reply, reply, state}
  end

  def handle_call(:linear_load, _from, %State{db: db} = state) do
    reply =
      with {:ok, event_rows} <- query(db, "SELECT delivery_id, payload FROM linear_events ORDER BY delivery_id"),
           {:ok, events} <- decode_linear_event_rows(event_rows, %{}),
           {:ok, task_rows} <- query(db, "SELECT issue_id, payload FROM linear_tasks ORDER BY issue_id"),
           {:ok, tasks} <- decode_linear_task_rows(task_rows, %{}) do
        {:ok, %{events: events, tasks: tasks}}
      end

    {:reply, reply, state}
  end

  def handle_call({:linear_receive, delivery_id, normalized_event}, _from, %State{db: db} = state) do
    record = %{id: delivery_id, event: normalized_event, status: :pending}

    reply =
      case transaction(db, "BEGIN IMMEDIATE", fn -> receive_linear_event(db, delivery_id, normalized_event, record) end) do
        {:committed, :ok} -> :ok
        {:rolled_back, {:duplicate, existing}} -> {:duplicate, existing}
        {:rolled_back, {:error, reason}} -> {:error, reason}
        {:error, reason} -> {:error, reason}
      end

    {:reply, reply, state}
  end

  def handle_call({:linear_commit, event_record, task_record_or_nil}, {caller, _tag}, %State{db: db, owner: owner} = state) do
    reply =
      if caller == owner do
        commit_linear_transaction(db, event_record, task_record_or_nil)
      else
        {:error, :not_owner}
      end

    {:reply, reply, state}
  end

  def handle_call({:linear_task, task_map}, {caller, _tag}, %State{db: db, owner: owner} = state) do
    reply =
      if caller == owner do
        case encode_term(task_map) do
          {:ok, payload} -> upsert_linear_task(db, field(task_map, :issue_id), payload)
          {:error, _reason} = error -> error
        end
      else
        {:error, :not_owner}
      end

    {:reply, reply, state}
  end

  def handle_call({:commit, run_map, event_id, event_map}, {caller, _tag}, %State{db: db, owner: owner} = state) do
    reply =
      if caller == owner do
        commit_transaction(db, run_map, event_id, event_map)
      else
        {:error, :not_owner}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:DOWN, owner_ref, :process, owner, reason}, %State{owner_ref: owner_ref, owner: owner} = state) do
    {:stop, {:owner_down, reason}, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %State{db: db}) do
    _ = Sqlite3.close(db)
    :ok
  end

  defp validate_start_options(opts) do
    if Keyword.keyword?(opts) do
      allowed = [:path, :owner, :name, :migrations]
      keys = Keyword.keys(opts)

      cond do
        Enum.any?(keys, &(&1 not in allowed)) or length(keys) != length(Enum.uniq(keys)) ->
          {:error, {:invalid_start_options, keys}}

        not is_binary(Keyword.get(opts, :path)) or Path.type(Keyword.get(opts, :path)) != :absolute ->
          {:error, :database_path_must_be_absolute}

        not is_pid(Keyword.get(opts, :owner)) ->
          {:error, :owner_must_be_a_pid}

        not Process.alive?(Keyword.get(opts, :owner)) ->
          {:error, :owner_not_alive}

        true ->
          :ok
      end
    else
      {:error, :invalid_start_options}
    end
  end

  defp normalized_path(path) do
    if Path.type(path) == :absolute do
      {:ok, Path.expand(path)}
    else
      {:error, :database_path_must_be_absolute}
    end
  end

  defp normalize_migrations(migrations) when is_list(migrations) and migrations != [] do
    valid? =
      migrations
      |> Enum.with_index(1)
      |> Enum.all?(fn {migration, expected_version} ->
        case migration do
          {version, sql} ->
            version == expected_version and is_binary(sql) and String.trim(sql) != ""

          _other ->
            false
        end
      end)

    if valid?, do: {:ok, migrations}, else: {:error, {:invalid_migrations, migrations}}
  end

  defp normalize_migrations(migrations), do: {:error, {:invalid_migrations, migrations}}

  defp ensure_parent_directory(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory}} ->
        :ok

      {:ok, %File.Stat{type: :symlink}} ->
        if File.dir?(path), do: :ok, else: {:error, {:invalid_database_directory, path}}

      {:ok, _stat} ->
        {:error, {:invalid_database_directory, path}}

      {:error, :enoent} ->
        parent = Path.dirname(path)

        if parent == path do
          {:error, {:invalid_database_directory, path}}
        else
          with :ok <- ensure_parent_directory(parent),
               :ok <- create_parent_directory(path) do
            :ok
          end
        end

      {:error, reason} ->
        {:error, {:database_directory_error, path, reason}}
    end
  end

  defp create_parent_directory(path) do
    case File.mkdir(path) do
      :ok ->
        case File.chmod(path, @directory_mode) do
          :ok -> :ok
          {:error, reason} -> {:error, {:database_directory_error, path, reason}}
        end

      {:error, :eexist} ->
        if File.dir?(path), do: :ok, else: {:error, {:invalid_database_directory, path}}

      {:error, reason} ->
        {:error, {:database_directory_error, path, reason}}
    end
  end

  defp ensure_database_file(path) do
    case File.lstat(path) do
      {:ok, _stat} ->
        :ok

      {:error, :enoent} ->
        case File.open(path, [:write, :exclusive]) do
          {:ok, file} ->
            with :ok <- File.close(file),
                 :ok <- File.chmod(path, @file_mode) do
              :ok
            else
              {:error, reason} -> {:error, {:database_file_error, path, reason}}
            end

          {:error, :eexist} ->
            :ok

          {:error, reason} ->
            {:error, {:database_file_error, path, reason}}
        end

      {:error, reason} ->
        {:error, {:database_file_error, path, reason}}
    end
  end

  defp open_database(path) do
    case Sqlite3.open(path, mode: [:readwrite, :create]) do
      {:ok, db} -> {:ok, db}
      {:error, reason} -> {:error, {:open_database_failed, path, reason}}
    end
  end

  defp initialize_database(db, path, migrations, owner) do
    with :ok <- configure_database(db, path),
         :ok <- migrate(db, migrations),
         {:ok, owner_ref} <- link_and_monitor_owner(owner) do
      {:ok, %State{path: path, db: db, owner: owner, owner_ref: owner_ref}}
    else
      {:error, reason} ->
        _ = Sqlite3.close(db)
        {:stop, reason}
    end
  end

  defp configure_database(db, path) do
    with :ok <- Sqlite3.set_busy_timeout(db, @busy_timeout_ms),
         :ok <- Sqlite3.execute(db, "PRAGMA journal_mode = DELETE"),
         :ok <- verify_pragma(db, "PRAGMA journal_mode", "delete"),
         :ok <- Sqlite3.execute(db, "PRAGMA synchronous = FULL"),
         :ok <- Sqlite3.execute(db, "PRAGMA locking_mode = EXCLUSIVE"),
         :ok <- verify_pragma(db, "PRAGMA locking_mode", "exclusive"),
         :ok <- acquire_exclusive_lock(db) do
      :ok
    else
      {:error, reason} -> {:error, {:database_ownership_conflict, path, reason}}
    end
  end

  defp verify_pragma(db, sql, expected) do
    case query(db, sql) do
      {:ok, [[value]]} when is_binary(value) ->
        if String.downcase(value) == expected, do: :ok, else: {:error, {:unexpected_pragma_value, sql, value}}

      {:ok, _rows} ->
        {:error, {:invalid_pragma_result, sql}}

      {:error, _reason} = error ->
        error
    end
  end

  defp acquire_exclusive_lock(db) do
    case Sqlite3.execute(db, "BEGIN EXCLUSIVE") do
      :ok ->
        case Sqlite3.execute(db, "COMMIT") do
          :ok ->
            :ok

          {:error, reason} ->
            _ = Sqlite3.execute(db, "ROLLBACK")
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp migrate(db, migrations) do
    supported_version = length(migrations)

    case schema_version(db) do
      {:ok, current_version} when current_version <= supported_version ->
        migrations
        |> Enum.drop(current_version)
        |> Enum.reduce_while(:ok, fn {version, sql}, :ok ->
          case run_migration(db, version, sql) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)

      {:ok, current_version} ->
        {:error, {:unsupported_schema_version, current_version, supported_version}}

      {:error, reason} ->
        {:error, {:migration_failed, 1, reason}}
    end
  end

  defp schema_version(db) do
    case query(db, "PRAGMA user_version") do
      {:ok, [[version]]} when is_integer(version) and version >= 0 -> {:ok, version}
      {:ok, _rows} -> {:error, :invalid_schema_version}
      {:error, _reason} = error -> error
    end
  end

  defp run_migration(db, version, sql) do
    case transaction(db, "BEGIN EXCLUSIVE", fn ->
           with :ok <- Sqlite3.execute(db, sql),
                :ok <- Sqlite3.execute(db, "PRAGMA user_version = #{version}") do
             {:commit, :ok}
           else
             {:error, reason} -> {:error, reason}
           end
         end) do
      {:committed, :ok} -> :ok
      {:error, reason} -> {:error, {:migration_failed, version, reason}}
      {:rolled_back, reason} -> {:error, {:migration_failed, version, reason}}
    end
  end

  defp commit_transaction(db, run_map, event_id, event_map) do
    case transaction(db, "BEGIN IMMEDIATE", fn ->
           case existing_event_run(db, event_id) do
             {:ok, nil} ->
               case persist_commit(db, run_map, event_id, event_map) do
                 :ok -> {:commit, :ok}
                 {:error, reason} -> {:error, reason}
               end

             {:ok, existing_run_id} ->
               {:rollback, {:duplicate, existing_run_id}}

             {:error, reason} ->
               {:error, reason}
           end
         end) do
      {:committed, :ok} -> :ok
      {:rolled_back, {:duplicate, existing_run_id}} -> {:duplicate, existing_run_id}
      {:error, reason} -> {:error, reason}
    end
  end

  defp receive_linear_event(db, delivery_id, normalized_event, record) do
    case query(db, "SELECT payload FROM linear_events WHERE delivery_id = ?1", [delivery_id]) do
      {:ok, []} ->
        with {:ok, payload} <- encode_term(record),
             :ok <- insert_linear_event(db, delivery_id, payload) do
          {:commit, :ok}
        else
          {:error, reason} -> {:error, reason}
        end

      {:ok, [[payload]]} ->
        case decode_linear_event(payload, delivery_id) do
          {:ok, existing} ->
            if field(existing, :event) === normalized_event do
              {:rollback, {:duplicate, existing}}
            else
              {:rollback, {:error, :event_identity_conflict}}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:ok, _rows} ->
        {:error, :invalid_linear_event_row}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp commit_linear_transaction(db, event_record, task_record_or_nil) do
    case transaction(db, "BEGIN IMMEDIATE", fn ->
           case persist_linear_commit(db, event_record, task_record_or_nil) do
             :ok -> {:commit, :ok}
             {:error, reason} -> {:error, reason}
           end
         end) do
      {:committed, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_linear_commit(db, event_record, task_record_or_nil) do
    delivery_id = field(event_record, :id)

    with {:ok, existing} <- fetch_linear_event(db, delivery_id),
         :ok <- ensure_linear_event_identity(existing, event_record),
         {:ok, event_payload} <- encode_term(event_record),
         :ok <- upsert_linear_event(db, delivery_id, event_payload),
         :ok <- maybe_upsert_linear_task(db, task_record_or_nil) do
      :ok
    end
  end

  defp fetch_linear_event(db, delivery_id) do
    case query(db, "SELECT payload FROM linear_events WHERE delivery_id = ?1", [delivery_id]) do
      {:ok, [[payload]]} -> decode_linear_event(payload, delivery_id)
      {:ok, []} -> {:error, :linear_event_not_found}
      {:ok, _rows} -> {:error, :invalid_linear_event_row}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_linear_event_identity(existing, incoming) do
    cond do
      field(existing, :id) != field(incoming, :id) -> {:error, :event_identity_conflict}
      field(existing, :event) !== field(incoming, :event) -> {:error, :event_identity_conflict}
      true -> :ok
    end
  end

  defp maybe_upsert_linear_task(_db, nil), do: :ok

  defp maybe_upsert_linear_task(db, task_map) do
    with {:ok, payload} <- encode_term(task_map) do
      upsert_linear_task(db, field(task_map, :issue_id), payload)
    end
  end

  defp insert_linear_event(db, delivery_id, payload) do
    execute_prepared(
      db,
      "INSERT INTO linear_events (delivery_id, payload) VALUES (?1, ?2)",
      [delivery_id, {:blob, payload}]
    )
  end

  defp upsert_linear_event(db, delivery_id, payload) do
    execute_prepared(
      db,
      "INSERT INTO linear_events (delivery_id, payload) VALUES (?1, ?2) " <>
        "ON CONFLICT(delivery_id) DO UPDATE SET payload = excluded.payload",
      [delivery_id, {:blob, payload}]
    )
  end

  defp upsert_linear_task(db, issue_id, payload) do
    execute_prepared(
      db,
      "INSERT INTO linear_tasks (issue_id, payload) VALUES (?1, ?2) " <>
        "ON CONFLICT(issue_id) DO UPDATE SET payload = excluded.payload",
      [issue_id, {:blob, payload}]
    )
  end

  defp validate_linear_commit(event_record, task_record_or_nil)
       when is_map(event_record) and (is_map(task_record_or_nil) or is_nil(task_record_or_nil)) do
    delivery_id = field(event_record, :id)
    event = field(event_record, :event)

    cond do
      not (is_binary(delivery_id) and delivery_id != "") -> {:error, :invalid_linear_event_record}
      not is_map(event) -> {:error, :invalid_linear_event_record}
      not has_field?(event_record, :status) -> {:error, :invalid_linear_event_record}
      not safe_persistent_term?(event_record) -> {:error, :invalid_linear_event_record}
      is_map(task_record_or_nil) -> validate_linear_task(task_record_or_nil)
      true -> :ok
    end
  end

  defp validate_linear_commit(_event_record, _task_record_or_nil), do: {:error, :invalid_linear_commit}

  defp validate_linear_task(task_map) when is_map(task_map) do
    issue_id = field(task_map, :issue_id)

    cond do
      not (is_binary(issue_id) and issue_id != "") -> {:error, :invalid_linear_task}
      not safe_persistent_term?(task_map) -> {:error, :invalid_linear_task}
      true -> :ok
    end
  end

  defp validate_linear_task(_task_map), do: {:error, :invalid_linear_task}

  defp has_field?(map, key), do: Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))

  defp safe_persistent_term?(term) when is_atom(term) or is_binary(term) or is_number(term), do: true

  defp safe_persistent_term?(term) when is_list(term), do: Enum.all?(term, &safe_persistent_term?/1)

  defp safe_persistent_term?(term) when is_map(term) do
    not Map.has_key?(term, :__struct__) and
      not Map.has_key?(term, "__struct__") and
      Enum.all?(term, fn {key, value} ->
        (is_atom(key) or is_binary(key)) and safe_persistent_term?(value)
      end)
  end

  defp safe_persistent_term?(_term), do: false

  defp decode_linear_event_rows([], events), do: {:ok, events}

  defp decode_linear_event_rows([[delivery_id, payload] | rest], events) when is_binary(delivery_id) do
    case decode_linear_event(payload, delivery_id) do
      {:ok, record} -> decode_linear_event_rows(rest, Map.put(events, delivery_id, record))
      {:error, _reason} = error -> error
    end
  end

  defp decode_linear_event_rows(_rows, _events), do: {:error, :invalid_linear_event_row}

  defp decode_linear_event(payload, delivery_id) when is_binary(payload) do
    case decode_term(payload) do
      {:ok, record} when is_map(record) ->
        cond do
          field(record, :id) != delivery_id -> {:error, :invalid_linear_event_payload}
          not is_map(field(record, :event)) -> {:error, :invalid_linear_event_payload}
          not has_field?(record, :status) -> {:error, :invalid_linear_event_payload}
          not safe_persistent_term?(record) -> {:error, :invalid_linear_event_payload}
          true -> {:ok, record}
        end

      {:ok, _record} ->
        {:error, :invalid_linear_event_payload}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_linear_event(_payload, _delivery_id), do: {:error, :invalid_linear_event_payload}

  defp decode_linear_task_rows([], tasks), do: {:ok, tasks}

  defp decode_linear_task_rows([[issue_id, payload] | rest], tasks) when is_binary(issue_id) do
    case decode_linear_task(payload, issue_id) do
      {:ok, task} -> decode_linear_task_rows(rest, Map.put(tasks, issue_id, task))
      {:error, _reason} = error -> error
    end
  end

  defp decode_linear_task_rows(_rows, _tasks), do: {:error, :invalid_linear_task_row}

  defp decode_linear_task(payload, issue_id) when is_binary(payload) do
    case decode_term(payload) do
      {:ok, task} when is_map(task) ->
        if field(task, :issue_id) == issue_id and safe_persistent_term?(task),
          do: {:ok, task},
          else: {:error, :invalid_linear_task_payload}

      {:ok, _task} ->
        {:error, :invalid_linear_task_payload}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_linear_task(_payload, _issue_id), do: {:error, :invalid_linear_task_payload}

  defp existing_event_run(db, event_id) do
    case query(db, "SELECT run_id FROM events WHERE event_id = ?1", [event_id]) do
      {:ok, [[run_id]]} -> {:ok, run_id}
      {:ok, []} -> {:ok, nil}
      {:ok, _rows} -> {:error, :invalid_event_row}
      {:error, _reason} = error -> error
    end
  end

  defp persist_commit(db, run_map, event_id, event_map) do
    run_id = field(run_map, :id)
    task_id = field(run_map, :task_id)
    attempts = field(run_map, :attempts)
    operations = field(run_map, :operations)

    with {:ok, run_blob} <- encode_term(run_map),
         {:ok, event_blob} <- encode_term(event_map),
         :ok <- upsert_run(db, run_id, task_id, run_blob),
         :ok <- upsert_attempts(db, run_id, attempts),
         :ok <- upsert_operations(db, run_id, operations),
         :ok <- insert_event(db, event_id, run_id, event_blob) do
      :ok
    end
  end

  defp upsert_run(db, run_id, task_id, payload) do
    execute_prepared(
      db,
      "INSERT INTO runs (run_id, task_id, payload) VALUES (?1, ?2, ?3) " <>
        "ON CONFLICT(run_id) DO UPDATE SET task_id = excluded.task_id, payload = excluded.payload",
      [run_id, task_id, {:blob, payload}]
    )
  end

  defp upsert_attempts(db, run_id, attempts) do
    Enum.reduce_while(attempts, :ok, fn attempt, :ok ->
      attempt_id = field(attempt, :id)

      case encode_term(attempt) do
        {:ok, payload} ->
          case execute_prepared(
                 db,
                 "INSERT INTO stage_attempts (run_id, attempt_id, payload) VALUES (?1, ?2, ?3) " <>
                   "ON CONFLICT(run_id, attempt_id) DO UPDATE SET payload = excluded.payload",
                 [run_id, attempt_id, {:blob, payload}]
               ) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp upsert_operations(db, run_id, operations) do
    Enum.reduce_while(operations, :ok, fn {operation_id, operation}, :ok ->
      case encode_term(operation) do
        {:ok, payload} ->
          case execute_prepared(
                 db,
                 "INSERT INTO operations (operation_id, run_id, payload) VALUES (?1, ?2, ?3) " <>
                   "ON CONFLICT(operation_id) DO UPDATE SET run_id = excluded.run_id, payload = excluded.payload",
                 [operation_id, run_id, {:blob, payload}]
               ) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp insert_event(db, event_id, run_id, payload) do
    execute_prepared(
      db,
      "INSERT INTO events (event_id, run_id, payload) VALUES (?1, ?2, ?3)",
      [event_id, run_id, {:blob, payload}]
    )
  end

  defp validate_commit(run_map, event_id, event_map) when is_map(run_map) and is_binary(event_id) and is_map(event_map) do
    run_id = field(run_map, :id)
    task_id = field(run_map, :task_id)
    attempts = field(run_map, :attempts)
    operations = field(run_map, :operations)

    cond do
      not (is_binary(run_id) and run_id != "") -> {:error, :invalid_run_id}
      not (is_binary(task_id) and task_id != "") -> {:error, :invalid_task_id}
      not (is_list(attempts) and Enum.all?(attempts, &valid_attempt?/1)) -> {:error, :invalid_attempts}
      not (is_map(operations) and Enum.all?(operations, &valid_operation?/1)) -> {:error, :invalid_operations}
      event_id == "" -> {:error, :invalid_event_id}
      true -> :ok
    end
  end

  defp validate_commit(_run_map, _event_id, _event_map), do: {:error, :invalid_commit}

  defp valid_attempt?(attempt) when is_map(attempt) do
    attempt_id = field(attempt, :id)
    is_binary(attempt_id) and attempt_id != ""
  end

  defp valid_attempt?(_attempt), do: false

  defp valid_operation?({operation_id, operation}) do
    is_binary(operation_id) and operation_id != "" and is_map(operation)
  end

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp safe_call(server, message) do
    GenServer.call(server, message)
  catch
    :exit, reason -> {:error, {:store_unavailable, reason}}
  end

  defp transaction(db, begin_sql, fun) do
    case Sqlite3.execute(db, begin_sql) do
      :ok ->
        case fun.() do
          {:commit, value} ->
            case Sqlite3.execute(db, "COMMIT") do
              :ok -> {:committed, value}
              {:error, reason} -> rollback_transaction(db, reason)
            end

          {:rollback, value} ->
            case Sqlite3.execute(db, "ROLLBACK") do
              :ok -> {:rolled_back, value}
              {:error, reason} -> {:error, {:rollback_failed, value, reason}}
            end

          {:error, reason} ->
            rollback_transaction(db, reason)

          other ->
            rollback_transaction(db, {:invalid_transaction_result, other})
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp rollback_transaction(db, reason) do
    case Sqlite3.execute(db, "ROLLBACK") do
      :ok -> {:error, reason}
      {:error, rollback_reason} -> {:error, {:rollback_failed, reason, rollback_reason}}
    end
  end

  defp execute_prepared(db, sql, params) do
    case Sqlite3.prepare(db, sql) do
      {:ok, statement} ->
        try do
          with :ok <- Sqlite3.bind(statement, params),
               :done <- Sqlite3.step(db, statement) do
            :ok
          else
            {:error, reason} -> {:error, reason}
            {:row, row} -> {:error, {:unexpected_sql_row, row}}
            other -> {:error, {:unexpected_sql_result, other}}
          end
        after
          _ = Sqlite3.release(db, statement)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp query(db, sql, params \\ []) do
    case Sqlite3.prepare(db, sql) do
      {:ok, statement} ->
        try do
          with :ok <- Sqlite3.bind(statement, params),
               {:ok, rows} <- Sqlite3.fetch_all(db, statement) do
            {:ok, rows}
          end
        after
          _ = Sqlite3.release(db, statement)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_run_rows([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_run_rows([[payload] | rest], acc) do
    case decode_run(payload) do
      {:ok, run} when is_map(run) -> decode_run_rows(rest, [run | acc])
      {:ok, _run} -> {:error, :invalid_run_payload}
      {:error, _reason} = error -> error
    end
  end

  defp decode_run_rows(_rows, _acc), do: {:error, :invalid_run_row}

  defp decode_run(payload) when is_binary(payload) do
    case decode_term(payload) do
      {:ok, run} when is_map(run) ->
        # Older snapshots predate question/reply state. Preserve every saved value,
        # including policy pins, and add only fields absent from those snapshots.
        {:ok, Map.merge(%{questions: %{}, inbox: [], activity_ids: %{}, continuation: nil}, run)}

      {:ok, _run} ->
        {:error, :invalid_run_payload}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_run(_payload), do: {:error, :invalid_run_payload}

  defp encode_term(term) do
    binary = :erlang.term_to_binary(term)

    case decode_term(binary) do
      {:ok, decoded} when decoded === term -> {:ok, binary}
      {:ok, _decoded} -> {:error, :term_round_trip_mismatch}
      {:error, reason} -> {:error, {:unsafe_term, reason}}
    end
  rescue
    error -> {:error, {:term_encode_failed, error}}
  end

  defp decode_term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    error -> {:error, {:term_decode_failed, error}}
  end

  defp link_and_monitor_owner(owner) do
    owner_ref = Process.monitor(owner)

    try do
      Process.link(owner)
      {:ok, owner_ref}
    catch
      :exit, reason ->
        Process.demonitor(owner_ref, [:flush])
        {:error, {:owner_unavailable, reason}}
    end
  end
end
