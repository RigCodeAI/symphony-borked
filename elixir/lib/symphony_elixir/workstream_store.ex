defmodule SymphonyElixir.WorkstreamStore do
  @moduledoc """
  Durable SQLite storage for workstream runs, stage attempts, input events, and outgoing operations.

  One process owns the database connection. It keeps SQLite's exclusive lock for its lifetime,
  closes the connection when its owner exits, and is linked to that owner so a store failure also
  stops the coordinator.

  Before reading retained terms, load this application's trusted modules so their schema atoms
  exist on cold startup. Payload decoding stays in safe mode and never creates atoms from data.
  """

  use GenServer

  alias Exqlite.Sqlite3
  alias SymphonyElixir.{BackupCapture, Config, ControlledReviewQualification, Evidence, WorkstreamChild, WorkstreamRun}

  @busy_timeout_ms 100
  @directory_mode 0o700
  @file_mode 0o600
  @evidence_payload_limit 10 * 1024 * 1024
  @evidence_entry_limit 32 * 1024 * 1024
  @evidence_event_id_limit 1_024
  @recovery_image_bytes_limit 256 * 1024 * 1024
  @recovery_payload_bytes_limit 128 * 1024 * 1024
  @recovery_payload_limit 4 * 1024 * 1024
  @recovery_term_depth_limit 64
  @recovery_term_nodes_limit 100_000
  @recovery_run_limit 128
  @recovery_row_limit 10_000
  @publisher_port_old_executor_sha256 "ec8ae546aeb408243529f96a1a7048c92d4a61bbe268eee6decbccfb169f4c22"

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

  @migration_v3 """
  CREATE TABLE evidence_outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    snapshot BLOB NOT NULL,
    event BLOB NOT NULL
  );
  """

  @migrations [{1, @migration_v1}, {2, @migration_v2}, {3, @migration_v3}]

  defmodule State do
    @moduledoc false
    defstruct [:path, :db, :owner, :owner_ref, capture: nil]
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

  @doc """
  Returns redacted evidence awaiting durable evidence-index persistence, in commit order.
  Only the store owner may read it.
  """
  @spec pending_evidence(GenServer.server()) ::
          {:ok, [%{event_id: String.t(), snapshot: map(), event: map()}]} | {:error, term()}
  def pending_evidence(server), do: safe_call(server, :pending_evidence)

  @doc """
  Acknowledges one evidence item after the evidence index has durably accepted it.
  Only the store owner may acknowledge it.
  """
  @spec acknowledge_evidence(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def acknowledge_evidence(server, event_id)
      when is_binary(event_id) and event_id != "" and byte_size(event_id) <= @evidence_event_id_limit do
    safe_call(server, {:acknowledge_evidence, event_id})
  end

  def acknowledge_evidence(_server, _event_id), do: {:error, :invalid_evidence_event_id}

  @doc "Atomically records a run snapshot, an input event, stage attempts, and outgoing operations."
  @spec commit(GenServer.server(), run_map(), String.t(), map()) ::
          :ok | {:duplicate, String.t()} | {:error, term()}
  def commit(server, run_map, event_id, event_map) do
    case validate_commit(run_map, event_id, event_map) do
      :ok -> safe_call(server, {:commit, run_map, event_id, event_map})
      {:error, _reason} = error -> error
    end
  end

  @doc "Atomically links a blocked publication run to a fresh same-task successor."
  @spec commit_publisher_port_recovery_successor(GenServer.server(), map()) ::
          {:ok, %{stopped_run: run_map(), successor_run: run_map()}}
          | {:duplicate, %{stopped_run: run_map(), successor_run: run_map()}}
          | {:error, term()}
  def commit_publisher_port_recovery_successor(server, recovery) when is_map(recovery) do
    case validate_publisher_port_recovery(recovery) do
      :ok -> safe_call(server, {:publisher_port_recovery_successor, recovery})
      {:error, _reason} = error -> error
    end
  end

  def commit_publisher_port_recovery_successor(_server, _recovery), do: {:error, :invalid_publisher_port_recovery}

  @doc "Atomically stores a waiting parent and its linked child before either is dispatched."
  @spec commit_child_handoff(GenServer.server(), run_map(), run_map(), run_map(), map()) ::
          {:ok, run_map(), run_map()} | {:duplicate, run_map(), run_map()} | {:error, term()}
  def commit_child_handoff(server, expected_parent, waiting_parent, linked_child, intent) do
    case validate_child_handoff(expected_parent, waiting_parent, linked_child, intent) do
      :ok -> safe_call(server, {:commit_child_handoff, expected_parent, waiting_parent, linked_child, intent})
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

  @doc "Inspects a captured Store image without opening or migrating a filesystem database."
  @spec inspect_recovery_image(binary()) ::
          {:ok,
           %{
             runs: [run_map()],
             linear: %{events: map(), tasks: map()},
             run_ids: [String.t()],
             operation_ids: [String.t()],
             linear_delivery_ids: [String.t()]
           }}
          | {:error, :invalid_recovery_image | :snapshot_timeout | :unsupported_recovery_schema}
  def inspect_recovery_image(bytes) when is_binary(bytes) do
    with true <- byte_size(bytes) <= @recovery_image_bytes_limit,
         true <- standalone_recovery_image?(bytes),
         :ok <- load_persisted_term_modules(),
         {:ok, result} <- BackupCapture.inspect_image(bytes, &inspect_recovery_database/1) do
      {:ok, result}
    else
      {:error, :unsupported_recovery_schema} = error -> error
      {:error, :snapshot_timeout} = error -> error
      _other -> {:error, :invalid_recovery_image}
    end
  rescue
    _error -> {:error, :invalid_recovery_image}
  catch
    _kind, _reason -> {:error, :invalid_recovery_image}
  end

  def inspect_recovery_image(_bytes), do: {:error, :invalid_recovery_image}

  @doc "Atomically updates a known Linear event and optionally upserts its task."
  @spec linear_commit(GenServer.server(), map(), map() | nil) :: :ok | {:error, term()}
  def linear_commit(server, event_record, task_record_or_nil) do
    case validate_linear_commit(event_record, task_record_or_nil) do
      :ok -> safe_call(server, {:linear_commit, event_record, task_record_or_nil})
      {:error, _reason} = error -> error
    end
  end

  @doc "Atomically commits a safe Linear agent reassignment across the run, task, and event records."
  @spec linear_reassign(
          GenServer.server(),
          run_map(),
          run_map(),
          run_map(),
          String.t(),
          map(),
          map(),
          map()
        ) :: :ok | {:error, term()}
  def linear_reassign(server, expected_run, run_map, expected_task, run_event_id, run_event, event_record, task_record) do
    case validate_linear_reassignment(expected_run, run_map, expected_task, run_event_id, run_event, event_record, task_record) do
      :ok ->
        safe_call(server, {:linear_reassign, expected_run, run_map, expected_task, run_event_id, run_event, event_record, task_record})

      {:error, _reason} = error ->
        error
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

  @doc "Common write predicate; check inside the same store callback as every mutation."
  @spec capture_write_allowed?(map()) :: boolean()
  def capture_write_allowed?(%State{capture: nil}), do: true
  def capture_write_allowed?(_state), do: false

  @doc "Holds all writes for an owner-authorized local capture worker."
  @spec begin_capture(GenServer.server(), reference(), pid(), Path.t()) :: :ok | {:error, term()}
  def begin_capture(server, token, worker, destination),
    do: safe_call(server, {:capture_begin, token, worker, destination})

  @spec snapshot_capture(GenServer.server(), reference()) :: {:ok, map()} | {:error, term()}
  def snapshot_capture(server, token), do: GenServer.call(server, {:capture_snapshot, token}, :infinity)

  @spec capture_inventory(GenServer.server(), reference()) :: {:ok, map()} | {:error, term()}
  def capture_inventory(server, token), do: safe_call(server, {:capture_inventory, token})

  @spec end_capture(GenServer.server(), reference()) :: :ok | {:error, term()}
  def end_capture(server, token), do: GenServer.call(server, {:capture_end, token}, :infinity)

  @impl true
  def init(opts) do
    with :ok <- load_persisted_term_modules(),
         {:ok, path} <- normalized_path(Keyword.fetch!(opts, :path)),
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
  def handle_call({:capture_begin, token, worker, destination}, {caller, _}, %State{owner: caller, capture: nil} = state)
      when is_reference(token) and is_pid(worker) and is_binary(destination) do
    case query(state.db, "SELECT COUNT(*) FROM evidence_outbox") do
      {:ok, [[0]]} -> {:reply, :ok, %{state | capture: %{token: token, worker: worker, destination: destination}}}
      {:ok, _} -> {:reply, {:error, :capture_outbox_pending}, state}
      _ -> {:reply, {:error, :capture_store_unavailable}, state}
    end
  end

  def handle_call({:capture_begin, _token, _worker, _destination}, _from, state),
    do: {:reply, {:error, :capture_not_authorized}, state}

  def handle_call({:capture_snapshot, token}, {caller, _}, %State{capture: %{token: token, worker: caller, destination: path}} = state),
    do: {:reply, BackupCapture.snapshot(state.db, path), state}

  def handle_call({:capture_snapshot, _token}, _from, state), do: {:reply, {:error, :capture_not_authorized}, state}

  def handle_call({:capture_inventory, token}, {caller, _}, %State{owner: caller, capture: %{token: token}} = state) do
    reply =
      with {:ok, runs} <- capture_ids(state.db, "SELECT run_id FROM runs ORDER BY run_id LIMIT 10001"),
           {:ok, operations} <- capture_ids(state.db, "SELECT operation_id FROM operations ORDER BY operation_id LIMIT 10001"),
           {:ok, deliveries} <- capture_ids(state.db, "SELECT delivery_id FROM linear_events ORDER BY delivery_id LIMIT 10001") do
        {:ok, %{run_ids: runs, operation_ids: operations, linear_delivery_ids: deliveries}}
      end

    {:reply, reply, state}
  end

  def handle_call({:capture_inventory, _token}, _from, state), do: {:reply, {:error, :capture_not_authorized}, state}

  def handle_call({:capture_end, token}, {caller, _}, %State{owner: caller, capture: %{token: token}} = state),
    do: {:reply, :ok, %{state | capture: nil}}

  def handle_call({:capture_end, _token}, _from, state), do: {:reply, {:error, :capture_not_authorized}, state}

  # This guard is also required on future child-run commits. A separate client-side
  # capture check cannot fence a write queued behind begin_capture.
  def handle_call(message, _from, %State{capture: capture} = state)
      when not is_nil(capture) and is_tuple(message) and
             elem(message, 0) in [
               :commit,
               :publisher_port_recovery_successor,
               :linear_receive,
               :linear_commit,
               :linear_reassign,
               :linear_task,
               :acknowledge_evidence
             ],
      do: {:reply, {:error, :capture_in_progress}, state}

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

  def handle_call(:pending_evidence, {caller, _tag}, %State{db: db, owner: owner} = state) do
    reply =
      if caller == owner do
        with {:ok, rows} <-
               query(db, "SELECT event_id, snapshot, event FROM evidence_outbox ORDER BY seq"),
             {:ok, entries} <- decode_evidence_rows(rows, []) do
          {:ok, entries}
        end
      else
        {:error, :not_owner}
      end

    {:reply, reply, state}
  end

  def handle_call(
        {:acknowledge_evidence, event_id},
        {caller, _tag},
        %State{db: db, owner: owner} = state
      ) do
    reply =
      if caller == owner do
        execute_prepared(db, "DELETE FROM evidence_outbox WHERE event_id = ?1", [event_id])
      else
        {:error, :not_owner}
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

  def handle_call(
        {:linear_reassign, expected_run, run_map, expected_task, run_event_id, run_event, event_record, task_record},
        {caller, _tag},
        %State{db: db, owner: owner} = state
      ) do
    reply =
      if caller == owner do
        commit_linear_reassignment(
          db,
          expected_run,
          run_map,
          expected_task,
          run_event_id,
          run_event,
          event_record,
          task_record
        )
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

  def handle_call({:publisher_port_recovery_successor, recovery}, {caller, _tag}, %State{db: db, owner: owner} = state) do
    reply =
      cond do
        caller != owner ->
          {:error, :not_owner}

        not __MODULE__.capture_write_allowed?(state) ->
          {:error, :capture_in_progress}

        true ->
          commit_publisher_port_recovery_successor_transaction(db, recovery)
      end

    {:reply, reply, state}
  end

  def handle_call(
        {:commit_child_handoff, expected_parent, waiting_parent, linked_child, intent},
        {caller, _tag},
        %State{db: db, owner: owner} = state
      ) do
    reply =
      cond do
        caller != owner ->
          {:error, :not_owner}

        not __MODULE__.capture_write_allowed?(state) ->
          {:error, :capture_in_progress}

        true ->
          with :ok <- validate_child_handoff(expected_parent, waiting_parent, linked_child, intent) do
            commit_child_handoff_transaction(db, expected_parent, waiting_parent, linked_child, intent)
          end
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_cast({:capture_settle, token}, %State{capture: %{token: token}, owner: owner} = state) do
    send(owner, {:backup_capture_settled, token, :store, self()})
    {:noreply, state}
  end

  def handle_cast(_message, state), do: {:noreply, state}

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

  defp capture_ids(db, sql) do
    case query(db, sql) do
      {:ok, rows} when length(rows) <= 10_000 -> {:ok, Enum.map(rows, fn [id] -> id end)}
      {:ok, _} -> {:error, :capture_inventory_limit}
      _ -> {:error, :capture_store_unavailable}
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

  defp commit_publisher_port_recovery_successor_transaction(db, recovery) do
    case transaction(db, "BEGIN IMMEDIATE", fn ->
           case publisher_port_recovery_replay(db, recovery) do
             {:ok, stopped_run, successor_run} ->
               {:rollback, {:duplicate, %{stopped_run: stopped_run, successor_run: successor_run}}}

             :not_a_replay ->
               commit_new_publisher_port_recovery_successor(db, recovery)

             {:error, reason} ->
               {:error, reason}
           end
         end) do
      {:committed, result} -> {:ok, result}
      {:rolled_back, {:duplicate, result}} -> {:duplicate, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp publisher_port_recovery_replay(db, recovery) do
    origin_id = field(recovery, :origin_event_id)
    successor_id = field(recovery, :successor_event_id)

    with {:ok, origin} <- fetch_stored_event(db, origin_id),
         {:ok, successor} <- fetch_stored_event(db, successor_id) do
      expected_origin = %{run_id: field(recovery, :stopped_run).id, event: field(recovery, :origin_event)}
      expected_successor = %{run_id: field(recovery, :successor_run).id, event: field(recovery, :successor_event)}

      if origin === expected_origin and successor === expected_successor do
        with {:ok, stopped_run} <- fetch_stored_run(db, field(recovery, :stopped_run).id),
             {:ok, successor_run} <- fetch_stored_run(db, field(recovery, :successor_run).id),
             {:ok, task} <- fetch_linear_task(db, field(recovery, :successor_task).issue_id),
             {:ok, reply} <- fetch_linear_event(db, field(recovery, :reply_record).id),
             true <- stopped_run === field(recovery, :stopped_run),
             true <- successor_run === field(recovery, :successor_run),
             true <- task === field(recovery, :successor_task),
             true <- reply === field(recovery, :reply_record) do
          {:ok, stopped_run, successor_run}
        else
          false -> {:error, :publisher_port_recovery_conflict}
          :not_found -> {:error, :publisher_port_recovery_conflict}
          {:error, reason} -> {:error, reason}
        end
      else
        {:error, :publisher_port_recovery_conflict}
      end
    else
      :not_found -> :not_a_replay
      {:error, reason} -> {:error, reason}
    end
  end

  defp commit_new_publisher_port_recovery_successor(db, recovery) do
    expected_run = field(recovery, :expected_run)
    stopped_run = field(recovery, :stopped_run)
    successor_run = field(recovery, :successor_run)
    expected_task = field(recovery, :expected_task)
    successor_task = field(recovery, :successor_task)
    reply_record = field(recovery, :reply_record)
    origin_event_id = field(recovery, :origin_event_id)
    successor_event_id = field(recovery, :successor_event_id)

    with :ok <- ensure_expected_run(db, expected_run),
         :ok <- ensure_expected_linear_task(db, expected_task),
         :ok <- ensure_single_active_task_run(db, expected_run),
         :not_found <- fetch_stored_run(db, successor_run.id),
         :ok <- ensure_recovery_events_absent(db, origin_event_id, successor_event_id),
         :ok <- ensure_rejected_reply_record(db, reply_record),
         :ok <- persist_commit(db, stopped_run, origin_event_id, field(recovery, :origin_event)),
         :ok <- persist_commit(db, successor_run, successor_event_id, field(recovery, :successor_event)),
         :ok <- persist_linear_commit(db, reply_record, successor_task) do
      {:commit, %{stopped_run: stopped_run, successor_run: successor_run}}
    else
      {:ok, _run} -> {:error, :publisher_port_recovery_conflict}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :publisher_port_recovery_conflict}
    end
  end

  defp ensure_single_active_task_run(db, expected_run) do
    task_id = field(expected_run, :task_id)

    case query(db, "SELECT run_id, payload FROM runs WHERE task_id = ?1 ORDER BY run_id", [task_id]) do
      {:ok, [[run_id, payload]]} when run_id == expected_run.id ->
        case decode_run(payload) do
          {:ok, ^expected_run} -> :ok
          {:ok, _run} -> {:error, :run_assignment_conflict}
          {:error, reason} -> {:error, reason}
        end

      {:ok, []} ->
        {:error, :run_not_found}

      {:ok, _rows} ->
        {:error, :active_task_run_conflict}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_recovery_events_absent(db, origin_event_id, successor_event_id) do
    with {:ok, nil} <- existing_event_run(db, origin_event_id),
         {:ok, nil} <- existing_event_run(db, successor_event_id),
         :ok <- ensure_evidence_event_absent(db, origin_event_id),
         :ok <- ensure_evidence_event_absent(db, successor_event_id) do
      :ok
    else
      {:ok, _run_id} -> {:error, :publisher_port_recovery_conflict}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_rejected_reply_record(db, expected_record) do
    with {:ok, record} <- fetch_linear_event(db, field(expected_record, :id)),
         true <- record === expected_record,
         true <- field(record, :status) == :reply_rejected do
      :ok
    else
      false -> {:error, :linear_event_assignment_conflict}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :linear_event_assignment_conflict}
    end
  end

  defp commit_child_handoff_transaction(db, expected_parent, waiting_parent, linked_child, intent) do
    case transaction(db, "BEGIN IMMEDIATE", fn ->
           with {:ok, stored_parent} <- fetch_stored_run(db, field(expected_parent, :id)) do
             case child_handoff_replay(db, stored_parent, linked_child, intent) do
               {:ok, parent, child} ->
                 {:rollback, {:duplicate, parent, child}}

               :not_a_replay ->
                 commit_new_child_handoff(db, stored_parent, expected_parent, waiting_parent, linked_child, intent)

               {:error, reason} ->
                 {:error, reason}
             end
           else
             :not_found -> {:error, :parent_not_found}
             {:error, reason} -> {:error, reason}
           end
         end) do
      {:committed, {:ok, parent, child}} -> {:ok, parent, child}
      {:rolled_back, {:duplicate, parent, child}} -> {:duplicate, parent, child}
      {:error, reason} -> {:error, reason}
    end
  end

  defp commit_new_child_handoff(db, stored_parent, expected_parent, waiting_parent, linked_child, intent) do
    {parent_event_id, parent_event, child_event_id, child_event} = child_handoff_events(intent, linked_child)

    with true <- stored_parent === expected_parent,
         :ok <- ensure_no_child_alias(db, linked_child),
         :ok <- ensure_child_handoff_events_absent(db, parent_event_id, child_event_id),
         :ok <- persist_commit(db, waiting_parent, parent_event_id, parent_event),
         :ok <- persist_commit(db, linked_child, child_event_id, child_event) do
      {:commit, {:ok, waiting_parent, linked_child}}
    else
      false -> {:error, :parent_state_conflict}
      {:error, reason} -> {:error, reason}
    end
  end

  defp child_handoff_replay(db, parent, linked_child, intent) do
    saved_intent = field(parent, :child_handoff)

    if is_map(saved_intent) and field(saved_intent, :handoff_id) == field(intent, :handoff_id) do
      with :ok <- SymphonyElixir.WorkstreamChild.replay(saved_intent, intent),
           {:ok, child} <- fetch_stored_run(db, field(linked_child, :id)),
           true <- field(child, :parent_run_id) == field(intent, :parent_run_id),
           true <- field(child, :parent_handoff) == child_handoff_lineage(intent),
           :ok <- ensure_child_handoff_events(db, parent, child, intent) do
        {:ok, parent, child}
      else
        false -> {:error, :child_handoff_conflict}
        :not_found -> {:error, :child_handoff_conflict}
        {:error, reason} -> {:error, reason}
      end
    else
      :not_a_replay
    end
  end

  defp ensure_child_handoff_events(db, parent, child, intent) do
    {parent_event_id, parent_event, child_event_id, child_event} = child_handoff_events(intent, child)

    with {:ok, %{run_id: parent_id, event: ^parent_event}} <- fetch_stored_event(db, parent_event_id),
         true <- parent_id == field(parent, :id),
         {:ok, %{run_id: child_id, event: ^child_event}} <- fetch_stored_event(db, child_event_id),
         true <- child_id == field(child, :id) do
      :ok
    else
      false -> {:error, :child_handoff_conflict}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :child_handoff_conflict}
    end
  end

  defp child_handoff_events(intent, child) do
    handoff_id = field(intent, :handoff_id)
    parent_id = field(intent, :parent_run_id)
    child_id = field(child, :id)
    parent_event_id = "child-handoff/#{handoff_id}/parent"
    child_event_id = "child-handoff/#{handoff_id}/child"

    parent_event = %{
      kind: :child_handoff_started,
      handoff_id: handoff_id,
      parent_run_id: parent_id,
      child_run_id: child_id,
      intent: intent
    }

    child_event = %{
      kind: :child_handoff_linked,
      handoff_id: handoff_id,
      parent_run_id: parent_id,
      child_run_id: child_id,
      intent: intent
    }

    {parent_event_id, parent_event, child_event_id, child_event}
  end

  defp child_handoff_lineage(intent),
    do: Map.take(intent, ~w(handoff_id parent_run_id parent_stage_id parent_attempt_id fingerprint)a)

  defp ensure_child_handoff_events_absent(db, parent_event_id, child_event_id) do
    with {:ok, nil} <- existing_event_run(db, parent_event_id),
         {:ok, nil} <- existing_event_run(db, child_event_id),
         :ok <- ensure_evidence_event_absent(db, parent_event_id),
         :ok <- ensure_evidence_event_absent(db, child_event_id) do
      :ok
    else
      {:ok, _run_id} -> {:error, :child_handoff_event_conflict}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_evidence_event_absent(db, event_id) do
    case query(db, "SELECT event_id FROM evidence_outbox WHERE event_id = ?1", [event_id]) do
      {:ok, []} -> :ok
      {:ok, _rows} -> {:error, :child_handoff_event_conflict}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_stored_run(db, run_id) do
    case query(db, "SELECT payload FROM runs WHERE run_id = ?1", [run_id]) do
      {:ok, [[payload]]} -> decode_run(payload)
      {:ok, []} -> :not_found
      {:ok, _rows} -> {:error, :invalid_run_row}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_stored_event(db, event_id) do
    case query(db, "SELECT run_id, payload FROM events WHERE event_id = ?1", [event_id]) do
      {:ok, [[run_id, payload]]} ->
        case decode_term(payload) do
          {:ok, event} when is_map(event) -> {:ok, %{run_id: run_id, event: event}}
          {:ok, _event} -> {:error, :invalid_event_payload}
          {:error, reason} -> {:error, reason}
        end

      {:ok, []} ->
        :not_found

      {:ok, _rows} ->
        {:error, :invalid_event_row}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_no_child_alias(db, child) do
    case query(db, "SELECT run_id, task_id, payload FROM runs ORDER BY run_id") do
      {:ok, rows} -> ensure_no_child_alias_rows(rows, child)
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_no_child_alias_rows([], _child), do: :ok

  defp ensure_no_child_alias_rows([[run_id, task_id, payload] | rest], child)
       when is_binary(run_id) and is_binary(task_id) and is_binary(payload) do
    with {:ok, run} <- decode_run(payload),
         true <- field(run, :id) == run_id and field(run, :task_id) == task_id do
      workspace = field(run, :workspace)

      cond do
        run_id == field(child, :id) -> {:error, :child_run_conflict}
        task_id == field(child, :task_id) -> {:error, :child_task_conflict}
        not (is_binary(workspace) and workspace != "") -> {:error, :child_workspace_unverified}
        workspaces_overlap?(workspace, field(child, :workspace)) -> {:error, :child_workspace_conflict}
        true -> ensure_no_child_alias_rows(rest, child)
      end
    else
      false -> {:error, :invalid_run_row}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_no_child_alias_rows(_rows, _child), do: {:error, :invalid_run_row}

  defp workspaces_overlap?(left, right) when is_binary(left) and is_binary(right) do
    left = Path.expand(left)
    right = Path.expand(right)
    left == right or String.starts_with?(left, right <> "/") or String.starts_with?(right, left <> "/")
  end

  defp workspaces_overlap?(_left, _right), do: false

  defp validate_child_handoff(expected_parent, waiting_parent, linked_child, intent)
       when is_map(expected_parent) and is_map(waiting_parent) and is_map(linked_child) and is_map(intent) do
    prepared_child = Map.drop(linked_child, [:parent_handoff, :parent_run_id])

    case SymphonyElixir.WorkstreamRun.begin_child(expected_parent, prepared_child) do
      {:ok, ^waiting_parent, ^linked_child, ^intent} ->
        parent_event_id = "child-handoff/#{field(intent, :handoff_id)}/parent"
        child_event_id = "child-handoff/#{field(intent, :handoff_id)}/child"

        with :ok <- validate_commit(waiting_parent, parent_event_id, %{}),
             :ok <- validate_commit(linked_child, child_event_id, %{}) do
          :ok
        end

      {:ok, _waiting, _child, _intent} ->
        {:error, :child_handoff_state_mismatch}

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    _error -> {:error, :invalid_child_handoff}
  end

  defp validate_child_handoff(_expected_parent, _waiting_parent, _linked_child, _intent),
    do: {:error, :invalid_child_handoff}

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

  defp commit_linear_reassignment(db, expected_run, run_map, expected_task, run_event_id, run_event, event_record, task_record) do
    case transaction(db, "BEGIN IMMEDIATE", fn ->
           with :ok <- ensure_expected_run(db, expected_run),
                :ok <- ensure_expected_linear_task(db, expected_task),
                :ok <- ensure_pending_reassignment(db, expected_task, run_map, run_event, event_record, task_record),
                {:ok, nil} <- existing_event_run(db, run_event_id),
                :ok <- persist_commit(db, run_map, run_event_id, run_event),
                :ok <- persist_linear_commit(db, event_record, task_record) do
             {:commit, :ok}
           else
             {:ok, existing_run_id} -> {:error, {:run_event_already_committed, existing_run_id}}
             {:error, reason} -> {:error, reason}
           end
         end) do
      {:committed, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_expected_run(db, expected_run) do
    run_id = field(expected_run, :id)

    case query(db, "SELECT payload FROM runs WHERE run_id = ?1", [run_id]) do
      {:ok, [[payload]]} ->
        case decode_run(payload) do
          {:ok, ^expected_run} -> :ok
          {:ok, _run} -> {:error, :run_assignment_conflict}
          {:error, _reason} = error -> error
        end

      {:ok, []} ->
        {:error, :run_not_found}

      {:ok, _rows} ->
        {:error, :invalid_run_row}

      {:error, _reason} = error ->
        error
    end
  end

  defp ensure_expected_linear_task(db, expected_task) do
    issue_id = field(expected_task, :issue_id)

    case query(db, "SELECT payload FROM linear_tasks WHERE issue_id = ?1", [issue_id]) do
      {:ok, [[payload]]} ->
        case decode_linear_task(payload, issue_id) do
          {:ok, ^expected_task} -> :ok
          {:ok, _task} -> {:error, :linear_task_assignment_conflict}
          {:error, _reason} = error -> error
        end

      {:ok, []} ->
        {:error, :linear_task_not_found}

      {:ok, _rows} ->
        {:error, :invalid_linear_task_row}

      {:error, _reason} = error ->
        error
    end
  end

  defp ensure_pending_reassignment(db, expected_task, run_map, run_event, event_record, task_record) do
    delivery_id = field(event_record, :id)

    with {:ok, existing_event} <- fetch_linear_event(db, delivery_id),
         :ok <- ensure_linear_event_identity(existing_event, event_record),
         true <- field(existing_event, :status) == :pending,
         true <- get_in(expected_task, [:pending_reassignment, :id]) == delivery_id,
         true <- field(expected_task, :status) == :reassigning,
         true <- field(expected_task, :authorized_stage) == nil,
         true <- field(expected_task, :authorized_until) in [nil, 0],
         true <- field(event_record, :status) == :handled,
         true <- field(field(event_record, :event), :kind) == :created,
         true <- field(task_record, :issue_id) == field(expected_task, :issue_id),
         true <- field(task_record, :run_id) == field(expected_task, :run_id),
         true <- field(task_record, :agent_id) == field(run_event, :agent_id),
         true <- field(run_event, :transition_id) == delivery_id,
         true <- reassignment_transition?(run_map, delivery_id, field(task_record, :agent_id)) do
      :ok
    else
      false -> {:error, :invalid_linear_reassignment_state}
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_linear_reassignment_state}
    end
  end

  defp reassignment_transition?(run, transition_id, agent_id) do
    Enum.any?(Map.get(run, :agent_transitions, []), fn
      %{id: ^transition_id, to: %{agent_id: ^agent_id}} -> true
      _ -> false
    end)
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

  defp fetch_linear_task(db, issue_id) do
    case query(db, "SELECT payload FROM linear_tasks WHERE issue_id = ?1", [issue_id]) do
      {:ok, [[payload]]} -> decode_linear_task(payload, issue_id)
      {:ok, []} -> :not_found
      {:ok, _rows} -> {:error, :invalid_linear_task_row}
      {:error, reason} -> {:error, reason}
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

  defp validate_linear_reassignment(expected_run, run_map, expected_task, run_event_id, run_event, event_record, task_record)
       when is_map(expected_run) and is_map(run_map) and is_map(expected_task) and is_map(run_event) and
              is_map(event_record) and is_map(task_record) do
    cond do
      field(expected_run, :id) != field(run_map, :id) ->
        {:error, :invalid_run_id}

      field(expected_run, :task_id) != field(run_map, :task_id) ->
        {:error, :invalid_task_id}

      field(expected_task, :run_id) != field(expected_run, :id) ->
        {:error, :invalid_linear_reassignment_state}

      not (is_binary(run_event_id) and run_event_id != "") ->
        {:error, :invalid_event_id}

      field(run_event, :kind) != :agent_reassigned ->
        {:error, :invalid_run_event}

      field(event_record, :id) != get_in(expected_task, [:pending_reassignment, :id]) ->
        {:error, :invalid_linear_event_record}

      field(task_record, :run_id) != field(run_map, :id) ->
        {:error, :invalid_linear_task}

      field(run_map, :status) not in [:ready, :waiting_for_answer] ->
        {:error, :run_not_reassignable}

      not reassignment_transition?(run_map, field(event_record, :id), field(task_record, :agent_id)) ->
        {:error, :invalid_agent_transition}

      validate_commit(run_map, run_event_id, run_event) != :ok ->
        validate_commit(run_map, run_event_id, run_event)

      validate_linear_commit(event_record, task_record) != :ok ->
        validate_linear_commit(event_record, task_record)

      validate_linear_task(expected_task) != :ok ->
        validate_linear_task(expected_task)

      true ->
        :ok
    end
  end

  defp validate_linear_reassignment(_expected_run, _run_map, _expected_task, _run_event_id, _run_event, _event_record, _task_record),
    do: {:error, :invalid_linear_reassignment}

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

    with {:ok, evidence_payload} <- journal_evidence(run_map, event_map, event_id),
         {:ok, run_blob} <- encode_term(run_map),
         {:ok, event_blob} <- encode_term(event_map),
         :ok <- upsert_run(db, run_id, task_id, run_blob),
         :ok <- upsert_attempts(db, run_id, attempts),
         :ok <- upsert_operations(db, run_id, operations),
         :ok <- insert_event(db, event_id, run_id, event_blob),
         :ok <- insert_evidence_outbox(db, event_id, evidence_payload) do
      :ok
    end
  end

  defp journal_evidence(run_map, event_map, event_id) do
    case Config.evidence() do
      :disabled ->
        {:ok, :disabled}

      {:ok, _config} when byte_size(event_id) <= @evidence_event_id_limit ->
        try do
          case Evidence.journal(run_map, event_map) do
            %{snapshot: snapshot, event: event} -> encode_evidence_payload(snapshot, event)
            _ -> {:error, :invalid_evidence_payload}
          end
        rescue
          _error -> {:error, :evidence_journal_failed}
        catch
          _kind, _reason -> {:error, :evidence_journal_failed}
        end

      {:ok, _config} ->
        {:error, :invalid_evidence_payload}

      {:error, _reason} ->
        {:error, :evidence_configuration_unavailable}
    end
  rescue
    _error -> {:error, :evidence_configuration_unavailable}
  catch
    _kind, _reason -> {:error, :evidence_configuration_unavailable}
  end

  defp encode_evidence_payload(snapshot, event) when is_map(snapshot) and is_map(event) do
    if safe_persistent_term?(snapshot) and safe_persistent_term?(event) do
      with {:ok, snapshot_blob} <- encode_term(snapshot),
           {:ok, event_blob} <- encode_term(event),
           true <- byte_size(snapshot_blob) <= @evidence_payload_limit,
           true <- byte_size(event_blob) <= @evidence_payload_limit,
           true <- byte_size(snapshot_blob) + byte_size(event_blob) <= @evidence_entry_limit do
        {:ok, {:enabled, snapshot_blob, event_blob}}
      else
        _ -> {:error, :invalid_evidence_payload}
      end
    else
      {:error, :invalid_evidence_payload}
    end
  end

  defp encode_evidence_payload(_snapshot, _event), do: {:error, :invalid_evidence_payload}

  defp insert_evidence_outbox(_db, _event_id, :disabled), do: :ok

  defp insert_evidence_outbox(db, event_id, {:enabled, snapshot_blob, event_blob}) do
    execute_prepared(
      db,
      "INSERT INTO evidence_outbox (event_id, snapshot, event) VALUES (?1, ?2, ?3)",
      [event_id, {:blob, snapshot_blob}, {:blob, event_blob}]
    )
  end

  defp decode_evidence_rows([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_evidence_rows([[event_id, snapshot_blob, event_blob] | rest], acc)
       when is_binary(event_id) and byte_size(event_id) in 1..@evidence_event_id_limit and
              is_binary(snapshot_blob) and is_binary(event_blob) do
    with true <- byte_size(snapshot_blob) <= @evidence_payload_limit,
         true <- byte_size(event_blob) <= @evidence_payload_limit,
         true <- byte_size(snapshot_blob) + byte_size(event_blob) <= @evidence_entry_limit,
         {:ok, snapshot} when is_map(snapshot) <- decode_term(snapshot_blob),
         {:ok, event} when is_map(event) <- decode_term(event_blob),
         true <- safe_persistent_term?(snapshot) and safe_persistent_term?(event) do
      decode_evidence_rows(rest, [%{event_id: event_id, snapshot: snapshot, event: event} | acc])
    else
      _ -> {:error, :invalid_evidence_outbox_row}
    end
  end

  defp decode_evidence_rows(_rows, _acc), do: {:error, :invalid_evidence_outbox_row}

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

  defp validate_publisher_port_recovery(recovery) do
    expected_run = field(recovery, :expected_run)
    stopped_run = field(recovery, :stopped_run)
    successor_run = field(recovery, :successor_run)
    expected_task = field(recovery, :expected_task)
    successor_task = field(recovery, :successor_task)
    reply_record = field(recovery, :reply_record)
    origin_event_id = field(recovery, :origin_event_id)
    successor_event_id = field(recovery, :successor_event_id)
    origin_event = field(recovery, :origin_event)
    successor_event = field(recovery, :successor_event)

    with true <- Enum.all?([expected_run, stopped_run, successor_run, expected_task, successor_task, reply_record, origin_event, successor_event], &is_map/1),
         true <- valid_event_id?(origin_event_id) and valid_event_id?(successor_event_id) and origin_event_id != successor_event_id,
         :ok <- validate_commit(stopped_run, origin_event_id, origin_event),
         :ok <- validate_commit(successor_run, successor_event_id, successor_event),
         :ok <- validate_linear_commit(reply_record, successor_task),
         :ok <- validate_recovery_run_pair(expected_run, stopped_run, successor_run, field(origin_event, :controlled_review_provenance)),
         :ok <- validate_recovery_task_pair(expected_run, successor_run, expected_task, successor_task),
         :ok <- validate_recovery_events(expected_run, successor_run, origin_event_id, successor_event_id, origin_event, successor_event),
         true <- field(reply_record, :status) == :reply_rejected do
      :ok
    else
      _ -> {:error, :invalid_publisher_port_recovery}
    end
  rescue
    _ -> {:error, :invalid_publisher_port_recovery}
  end

  defp validate_recovery_run_pair(expected_run, stopped_run, successor_run, qualification_provenance) do
    old_id = field(expected_run, :id)
    new_id = field(successor_run, :id)
    current_policy = WorkstreamRun.policy()
    expected_old_policy = Map.put(current_policy, :executor_sha256, @publisher_port_old_executor_sha256)

    stopped_expected =
      expected_run
      |> Map.merge(%{
        status: :stopped,
        phase: :stopped,
        recovery_successor_run_id: new_id
      })

    valid_successor =
      field(successor_run, :task_id) == field(expected_run, :task_id) and
        field(successor_run, :issue_id) == field(expected_run, :issue_id) and
        field(successor_run, :branch) == field(expected_run, :branch) and
        field(successor_run, :workspace) == field(expected_run, :workspace) and
        get_in(successor_run, [:outputs, "candidate"]) == get_in(expected_run, [:outputs, "candidate"]) and
        ControlledReviewQualification.valid_transition?(
          field(expected_run, :definition),
          field(successor_run, :definition),
          qualification_provenance
        ) and
        field(successor_run, :execution) == field(expected_run, :execution) and
        field(successor_run, :thread_id) == field(expected_run, :thread_id) and
        field(successor_run, :session_id) == field(expected_run, :session_id) and
        field(expected_run, :policy) == expected_old_policy and
        field(successor_run, :policy) == current_policy and
        field(successor_run, :recovery_parent_run_id) == old_id and
        is_nil(field(successor_run, :parent_run_id)) and
        field(successor_run, :status) == :ready and field(successor_run, :phase) == :queued and
        field(successor_run, :current_attempt_id) == nil and field(successor_run, :attempts) == [] and
        field(successor_run, :operations) == %{} and
        field(successor_run, :stage_id) == get_in(successor_run, [:execution, :candidate_producer_stage])

    if is_binary(old_id) and is_binary(new_id) and old_id != new_id and
         field(expected_run, :status) == :blocked and field(expected_run, :phase) == :blocked and
         field(expected_run, :stage_id) == :blocked and stopped_run === stopped_expected and valid_successor do
      :ok
    else
      {:error, :invalid_publisher_port_recovery_runs}
    end
  end

  defp validate_recovery_task_pair(expected_run, successor_run, expected_task, successor_task) do
    revision = field(expected_task, :scope_revision) || 0

    expected_successor_task =
      expected_task
      |> Map.merge(%{
        run_id: field(successor_run, :id),
        authorized_stage: nil,
        authorized_until: 0,
        owned_child_grant: nil,
        check_at: 0,
        scope_revision: revision + 1
      })

    if field(expected_task, :run_id) == field(expected_run, :id) and
         field(expected_task, :issue_id) == field(expected_run, :issue_id) and
         field(expected_task, :status) == :acknowledged and
         is_integer(revision) and revision >= 0 and successor_task === expected_successor_task do
      :ok
    else
      {:error, :invalid_publisher_port_recovery_task}
    end
  end

  defp validate_recovery_events(old_run, new_run, origin_id, successor_id, origin_event, successor_event) do
    if field(origin_event, :kind) == :publisher_port_recovery_origin_stopped and
         field(origin_event, :event_id) == origin_id and
         field(origin_event, :old_run_id) == field(old_run, :id) and
         field(origin_event, :successor_run_id) == field(new_run, :id) and
         field(successor_event, :kind) == :publisher_port_recovery_successor_queued and
         field(successor_event, :event_id) == successor_id and
         field(successor_event, :old_run_id) == field(old_run, :id) and
         field(successor_event, :successor_run_id) == field(new_run, :id) and
         field(successor_event, :controlled_review_provenance) == field(origin_event, :controlled_review_provenance) and
         field(successor_event, :candidate_handoff) == field(origin_event, :candidate_handoff) and
         valid_candidate_handoff?(old_run, field(origin_event, :candidate_handoff)) do
      :ok
    else
      {:error, :invalid_publisher_port_recovery_events}
    end
  end

  defp valid_candidate_handoff?(old_run, handoff) when is_map(handoff) do
    attempt_id = field(old_run, :id) <> "/implement/2"
    attempt = Enum.find(field(old_run, :attempts), &(field(&1, :id) == attempt_id))
    operation = get_in(old_run, [:operations, attempt_id])
    output = get_in(old_run, [:outputs, "candidate"])
    artifact = get_in(old_run, [:artifacts, "candidate"])
    intent = get_in(old_run, [:operations, field(old_run, :id) <> "/publish/16", :publication_intent])
    output_sha256 = :crypto.hash(:sha256, :erlang.term_to_binary(output)) |> Base.encode16(case: :lower)

    expected = %{
      version: 1,
      parent_run_id: field(old_run, :id),
      producer_attempt_id: attempt_id,
      candidate_output_id: artifact,
      candidate_output_sha256: output_sha256,
      candidate_sha: field(intent, :candidate_sha)
    }

    is_map(output) and is_map(artifact) and artifact[:run_id] == field(old_run, :id) and
      artifact[:attempt_id] == attempt_id and artifact[:sha256] == output_sha256 and
      match?(%{stage: "implement", type: :agent, status: :completed, result: %{status: :ok, evidence: ^output}}, attempt) and
      is_map(operation) and operation[:type] == :agent and operation[:status] == :completed and
      is_map(intent) and handoff === expected
  rescue
    _ -> false
  end

  defp valid_candidate_handoff?(_old_run, _handoff), do: false

  defp valid_event_id?(value),
    do: is_binary(value) and byte_size(value) in 1..@evidence_event_id_limit

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

  defp load_persisted_term_modules do
    # The module list comes from the installed application, never from a retained payload.
    # Loading WorkstreamRun alone misses atoms owned by Linear, worker and review modules.
    with result when result in [:ok, {:error, {:already_loaded, :symphony_elixir}}] <- Application.load(:symphony_elixir),
         modules when is_list(modules) <- Application.spec(:symphony_elixir, :modules),
         :ok <- :code.ensure_modules_loaded(modules) do
      :ok
    else
      reason -> {:error, {:persisted_term_modules_unavailable, reason}}
    end
  end

  defp standalone_recovery_image?(bytes) when byte_size(bytes) >= 100 do
    case bytes do
      <<"SQLite format 3", 0, page_size::unsigned-big-16, 1, 1, _rest::binary>> ->
        page_size in [1, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768]

      _other ->
        false
    end
  end

  defp standalone_recovery_image?(_bytes), do: false

  defp inspect_recovery_database(db) do
    with {:ok, [[1]]} <- query(db, "PRAGMA query_only"),
         {:ok, [["ok"]]} <- query(db, "PRAGMA integrity_check"),
         {:ok, version} <- schema_version(db),
         :ok <- supported_recovery_schema_version(version),
         :ok <- verify_recovery_schema_inventory(db),
         :ok <- validate_recovery_payload_budget(db),
         {:ok, result} <- inspect_recovery_rows(db) do
      {:ok, result}
    else
      {:error, :unsupported_recovery_schema} = error -> error
      _other -> {:error, :invalid_recovery_image}
    end
  rescue
    _error -> {:error, :invalid_recovery_image}
  catch
    _kind, _reason -> {:error, :invalid_recovery_image}
  end

  defp supported_recovery_schema_version(version) when version == length(@migrations), do: :ok
  defp supported_recovery_schema_version(_version), do: {:error, :unsupported_recovery_schema}

  defp verify_recovery_schema_inventory(db) do
    with {:ok, actual} <- recovery_schema_inventory(db),
         {:ok, expected} <- expected_recovery_schema_inventory(),
         true <- actual === expected do
      :ok
    else
      _other -> {:error, :invalid_recovery_image}
    end
  end

  defp recovery_schema_inventory(db),
    do: query(db, "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name")

  defp expected_recovery_schema_inventory do
    case Sqlite3.open(":memory:") do
      {:ok, db} ->
        try do
          with :ok <- apply_recovery_schema(db),
               {:ok, inventory} <- recovery_schema_inventory(db) do
            {:ok, inventory}
          else
            _other -> {:error, :invalid_recovery_image}
          end
        after
          _ = Sqlite3.close(db)
        end

      {:error, _reason} ->
        {:error, :invalid_recovery_image}
    end
  rescue
    _error -> {:error, :invalid_recovery_image}
  catch
    _kind, _reason -> {:error, :invalid_recovery_image}
  end

  defp apply_recovery_schema(db) do
    Enum.reduce_while(@migrations, :ok, fn {version, sql}, :ok ->
      case Sqlite3.execute(db, sql) do
        :ok ->
          case Sqlite3.execute(db, "PRAGMA user_version = #{version}") do
            :ok -> {:cont, :ok}
            _other -> {:halt, {:error, :invalid_recovery_image}}
          end

        _other ->
          {:halt, {:error, :invalid_recovery_image}}
      end
    end)
  end

  defp validate_recovery_payload_budget(db) do
    limits = [
      {"runs", @recovery_run_limit},
      {"events", @recovery_row_limit},
      {"stage_attempts", @recovery_row_limit},
      {"operations", @recovery_row_limit},
      {"linear_events", @recovery_row_limit},
      {"linear_tasks", @recovery_row_limit}
    ]

    Enum.reduce_while(limits, {:ok, 0}, fn {table, row_limit}, {:ok, total_bytes} ->
      sql = "SELECT COUNT(*),COALESCE(SUM(length(payload)),0),COALESCE(MAX(length(payload)),0) FROM #{table}"

      case query(db, sql) do
        {:ok, [[rows, bytes, max_payload]]}
        when is_integer(rows) and is_integer(bytes) and is_integer(max_payload) and
               rows <= row_limit and bytes >= 0 and max_payload <= @recovery_payload_limit ->
          next_bytes = total_bytes + bytes

          if next_bytes <= @recovery_payload_bytes_limit,
            do: {:cont, {:ok, next_bytes}},
            else: {:halt, {:error, :invalid_recovery_image}}

        _other ->
          {:halt, {:error, :invalid_recovery_image}}
      end
    end)
    |> case do
      {:ok, _bytes} ->
        case query(db, "SELECT COUNT(*) FROM evidence_outbox") do
          {:ok, [[0]]} -> :ok
          _other -> {:error, :invalid_recovery_image}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp inspect_recovery_rows(db) do
    with {:ok, run_rows} <- query(db, "SELECT run_id,task_id,payload FROM runs ORDER BY run_id"),
         {:ok, runs} <- decode_recovery_run_rows(run_rows),
         run_ids <- Enum.map(runs, fn {id, _run} -> id end),
         {:ok, attempt_rows} <-
           query(db, "SELECT run_id,attempt_id,payload FROM stage_attempts ORDER BY run_id,attempt_id"),
         :ok <- recovery_attempts_match?(attempt_rows, runs),
         {:ok, operation_rows} <- query(db, "SELECT operation_id,run_id,payload FROM operations ORDER BY operation_id"),
         :ok <- recovery_operations_match?(operation_rows, runs),
         run_maps <- Enum.map(runs, fn {_id, run} -> run end),
         :ok <- WorkstreamChild.inspect_recovery_links(run_maps),
         {:ok, event_rows} <- query(db, "SELECT event_id,run_id,payload FROM events ORDER BY event_id"),
         :ok <- validate_recovery_events(event_rows, MapSet.new(run_ids)),
         :ok <- validate_recovery_child_events(event_rows, run_maps),
         {:ok, linear_event_rows} <- query(db, "SELECT delivery_id,payload FROM linear_events ORDER BY delivery_id"),
         {:ok, linear_events} <- decode_recovery_linear_event_rows(linear_event_rows),
         {:ok, linear_task_rows} <- query(db, "SELECT issue_id,payload FROM linear_tasks ORDER BY issue_id"),
         {:ok, linear_tasks} <- decode_recovery_linear_task_rows(linear_task_rows) do
      {:ok,
       %{
         runs: run_maps,
         linear: %{events: linear_events, tasks: linear_tasks},
         run_ids: run_ids,
         operation_ids: Enum.map(operation_rows, fn [id, _run_id, _payload] -> id end),
         linear_delivery_ids: Map.keys(linear_events) |> Enum.sort()
       }}
    else
      _other -> {:error, :invalid_recovery_image}
    end
  end

  defp decode_recovery_run_rows(rows) when is_list(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      [run_id, task_id, payload], {:ok, runs} ->
        with true <- valid_recovery_id?(run_id) and valid_recovery_id?(task_id),
             {:ok, run} <- decode_recovery_run(payload),
             true <- field(run, :id) == run_id and field(run, :task_id) == task_id,
             true <- valid_attempts_for_recovery?(field(run, :attempts)),
             true <- valid_operations_for_recovery?(field(run, :operations)) do
          {:cont, {:ok, [{run_id, run} | runs]}}
        else
          _other -> {:halt, {:error, :invalid_recovery_image}}
        end

      _row, _acc ->
        {:halt, {:error, :invalid_recovery_image}}
    end)
    |> case do
      {:ok, runs} -> {:ok, Enum.reverse(runs)}
      error -> error
    end
  end

  defp decode_recovery_run(payload) when is_binary(payload) do
    with {:ok, run} when is_map(run) <- decode_recovery_term(payload) do
      {:ok, Map.merge(%{questions: %{}, inbox: [], activity_ids: %{}, continuation: nil}, run)}
    else
      _other -> {:error, :invalid_recovery_image}
    end
  end

  defp decode_recovery_run(_payload), do: {:error, :invalid_recovery_image}

  defp valid_attempts_for_recovery?(attempts) when is_list(attempts),
    do: length(attempts) <= @recovery_row_limit and Enum.all?(attempts, &valid_attempt?/1)

  defp valid_attempts_for_recovery?(_attempts), do: false

  defp valid_operations_for_recovery?(operations) when is_map(operations),
    do: map_size(operations) <= @recovery_row_limit and Enum.all?(operations, &valid_operation?/1)

  defp valid_operations_for_recovery?(_operations), do: false

  defp recovery_attempts_match?(rows, runs) do
    expected =
      Enum.flat_map(runs, fn {run_id, run} ->
        Enum.map(run.attempts, fn attempt -> {run_id, field(attempt, :id), attempt} end)
      end)

    with true <- length(expected) == length(rows),
         true <- unique_recovery_keys?(expected, fn {run_id, attempt_id, _attempt} -> {run_id, attempt_id} end),
         {:ok, actual} <- decode_recovery_attempt_rows(rows),
         true <- MapSet.new(expected) == MapSet.new(actual) do
      :ok
    else
      _other -> {:error, :invalid_recovery_image}
    end
  end

  defp decode_recovery_attempt_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      [run_id, attempt_id, payload], {:ok, attempts} ->
        with true <- valid_recovery_id?(run_id) and valid_recovery_id?(attempt_id),
             {:ok, attempt} when is_map(attempt) <- decode_recovery_term(payload),
             true <- field(attempt, :id) == attempt_id do
          {:cont, {:ok, [{run_id, attempt_id, attempt} | attempts]}}
        else
          _other -> {:halt, {:error, :invalid_recovery_image}}
        end

      _row, _acc ->
        {:halt, {:error, :invalid_recovery_image}}
    end)
    |> reverse_recovery_rows()
  end

  defp recovery_operations_match?(rows, runs) do
    expected =
      Enum.flat_map(runs, fn {run_id, run} ->
        Enum.map(run.operations, fn {operation_id, operation} -> {operation_id, run_id, operation} end)
      end)

    with true <- length(expected) == length(rows),
         true <- unique_recovery_keys?(expected, fn {operation_id, _run_id, _operation} -> operation_id end),
         {:ok, actual} <- decode_recovery_operation_rows(rows),
         true <- MapSet.new(expected) == MapSet.new(actual) do
      :ok
    else
      _other -> {:error, :invalid_recovery_image}
    end
  end

  defp decode_recovery_operation_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      [operation_id, run_id, payload], {:ok, operations} ->
        with true <- valid_recovery_id?(operation_id) and valid_recovery_id?(run_id),
             {:ok, operation} when is_map(operation) <- decode_recovery_term(payload) do
          {:cont, {:ok, [{operation_id, run_id, operation} | operations]}}
        else
          _other -> {:halt, {:error, :invalid_recovery_image}}
        end

      _row, _acc ->
        {:halt, {:error, :invalid_recovery_image}}
    end)
    |> reverse_recovery_rows()
  end

  defp unique_recovery_keys?(rows, key_fun) do
    MapSet.size(MapSet.new(Enum.map(rows, key_fun))) == length(rows)
  end

  defp validate_recovery_events(rows, run_ids) do
    Enum.reduce_while(rows, :ok, fn
      [event_id, run_id, payload], :ok ->
        case decode_recovery_term(payload) do
          {:ok, event} when is_map(event) and is_binary(run_id) ->
            if valid_recovery_id?(event_id) and MapSet.member?(run_ids, run_id),
              do: {:cont, :ok},
              else: {:halt, {:error, :invalid_recovery_image}}

          _other ->
            {:halt, {:error, :invalid_recovery_image}}
        end

      _row, _acc ->
        {:halt, {:error, :invalid_recovery_image}}
    end)
  end

  defp validate_recovery_child_events(rows, runs) do
    with {:ok, expected} <- expected_recovery_child_events(runs),
         child_rows <-
           Enum.filter(rows, fn
             [event_id, _run_id, _payload] when is_binary(event_id) -> String.starts_with?(event_id, "child-handoff/")
             _row -> false
           end),
         true <- length(child_rows) == map_size(expected),
         {:ok, actual} <- decode_recovery_child_event_rows(child_rows, expected),
         true <- MapSet.new(actual) == MapSet.new(Map.to_list(expected)) do
      :ok
    else
      _other -> {:error, :invalid_recovery_image}
    end
  end

  defp expected_recovery_child_events(runs) do
    Enum.reduce_while(runs, {:ok, %{}}, fn parent, {:ok, expected} ->
      operations = parent[:operations]

      child_operations =
        if is_map(operations), do: Enum.filter(operations, fn {_id, operation} -> is_map(operation) and operation[:type] == :child end), else: []

      Enum.reduce_while(child_operations, {:ok, expected}, fn {_attempt_id, operation}, {:ok, current} ->
        intent = operation[:child_handoff]
        child = if is_map(intent), do: Enum.find(runs, &(&1[:id] == intent[:child_run_id])), else: nil

        if is_map(child) do
          {parent_id, parent_event, child_id, child_event} = child_handoff_events(intent, child)
          events = [{parent_id, parent[:id], parent_event}, {child_id, child[:id], child_event}]

          if Enum.any?(events, fn {event_id, _run_id, _event} -> Map.has_key?(current, event_id) end) do
            {:halt, {:error, :invalid_recovery_image}}
          else
            updated = Enum.reduce(events, current, fn {event_id, run_id, event}, acc -> Map.put(acc, event_id, {run_id, event}) end)
            {:cont, {:ok, updated}}
          end
        else
          {:halt, {:error, :invalid_recovery_image}}
        end
      end)
      |> case do
        {:ok, updated} -> {:cont, {:ok, updated}}
        error -> {:halt, error}
      end
    end)
  rescue
    _error -> {:error, :invalid_recovery_image}
  end

  defp decode_recovery_child_event_rows(rows, expected) do
    Enum.reduce_while(rows, {:ok, []}, fn
      [event_id, run_id, payload], {:ok, events} ->
        case {Map.fetch(expected, event_id), decode_recovery_term(payload)} do
          {{:ok, {^run_id, expected_event}}, {:ok, event}} when event === expected_event ->
            {:cont, {:ok, [{event_id, {run_id, event}} | events]}}

          _other ->
            {:halt, {:error, :invalid_recovery_image}}
        end

      _row, _acc ->
        {:halt, {:error, :invalid_recovery_image}}
    end)
    |> case do
      {:ok, events} -> {:ok, Enum.reverse(events)}
      error -> error
    end
  end

  defp decode_recovery_linear_event_rows(rows) do
    Enum.reduce_while(rows, {:ok, %{}}, fn
      [delivery_id, payload], {:ok, events} ->
        with true <- valid_recovery_id?(delivery_id),
             {:ok, record} when is_map(record) <- decode_recovery_term(payload),
             true <- field(record, :id) == delivery_id,
             true <- is_map(field(record, :event)),
             true <- has_field?(record, :status),
             true <- safe_persistent_term?(record) do
          {:cont, {:ok, Map.put(events, delivery_id, record)}}
        else
          _other -> {:halt, {:error, :invalid_recovery_image}}
        end

      _row, _acc ->
        {:halt, {:error, :invalid_recovery_image}}
    end)
  end

  defp decode_recovery_linear_task_rows(rows) do
    Enum.reduce_while(rows, {:ok, %{}}, fn
      [issue_id, payload], {:ok, tasks} ->
        with true <- valid_recovery_id?(issue_id),
             {:ok, task} when is_map(task) <- decode_recovery_term(payload),
             true <- field(task, :issue_id) == issue_id,
             true <- safe_persistent_term?(task) do
          {:cont, {:ok, Map.put(tasks, issue_id, task)}}
        else
          _other -> {:halt, {:error, :invalid_recovery_image}}
        end

      _row, _acc ->
        {:halt, {:error, :invalid_recovery_image}}
    end)
  end

  defp decode_recovery_term(payload)
       when is_binary(payload) and byte_size(payload) > 0 and byte_size(payload) <= @recovery_payload_limit do
    if match?(<<131, 80, _rest::binary>>, payload) do
      {:error, :invalid_recovery_image}
    else
      case decode_term(payload) do
        {:ok, term} ->
          if bounded_recovery_term?(term), do: {:ok, term}, else: {:error, :invalid_recovery_image}

        _other ->
          {:error, :invalid_recovery_image}
      end
    end
  end

  defp decode_recovery_term(_payload), do: {:error, :invalid_recovery_image}

  defp bounded_recovery_term?(term) do
    case visit_recovery_term(term, 0, 0, 0) do
      {:ok, _nodes, _bytes} -> true
      :error -> false
    end
  rescue
    _error -> false
  catch
    _kind, _reason -> false
  end

  defp visit_recovery_term(_term, depth, _nodes, _bytes) when depth > @recovery_term_depth_limit, do: :error

  defp visit_recovery_term(term, depth, nodes, bytes) do
    nodes = nodes + 1

    cond do
      nodes > @recovery_term_nodes_limit ->
        :error

      is_pid(term) or is_port(term) or is_reference(term) or is_function(term) ->
        :error

      is_binary(term) ->
        next_bytes = bytes + byte_size(term)
        if next_bytes <= @recovery_payload_limit, do: {:ok, nodes, next_bytes}, else: :error

      is_number(term) or is_atom(term) ->
        {:ok, nodes, bytes}

      is_list(term) ->
        visit_recovery_list(term, depth + 1, nodes, bytes)

      is_tuple(term) ->
        visit_recovery_tuple(term, 0, depth + 1, nodes, bytes)

      is_map(term) ->
        visit_recovery_map(:maps.to_list(term), depth + 1, nodes, bytes)

      true ->
        :error
    end
  end

  defp visit_recovery_list([], _depth, nodes, bytes), do: {:ok, nodes, bytes}

  defp visit_recovery_list([head | tail], depth, nodes, bytes) do
    with {:ok, nodes, bytes} <- visit_recovery_term(head, depth, nodes, bytes) do
      visit_recovery_list(tail, depth, nodes, bytes)
    end
  end

  defp visit_recovery_list(_improper_list, _depth, _nodes, _bytes), do: :error

  defp visit_recovery_tuple(tuple, index, depth, nodes, bytes) when index < tuple_size(tuple) do
    with {:ok, nodes, bytes} <- visit_recovery_term(elem(tuple, index), depth, nodes, bytes) do
      visit_recovery_tuple(tuple, index + 1, depth, nodes, bytes)
    end
  end

  defp visit_recovery_tuple(_tuple, _index, _depth, nodes, bytes), do: {:ok, nodes, bytes}

  defp visit_recovery_map([], _depth, nodes, bytes), do: {:ok, nodes, bytes}

  defp visit_recovery_map([{key, value} | rest], depth, nodes, bytes) do
    with {:ok, nodes, bytes} <- visit_recovery_term(key, depth, nodes, bytes),
         {:ok, nodes, bytes} <- visit_recovery_term(value, depth, nodes, bytes) do
      visit_recovery_map(rest, depth, nodes, bytes)
    end
  end

  defp reverse_recovery_rows({:ok, rows}), do: {:ok, Enum.reverse(rows)}
  defp reverse_recovery_rows({:error, _reason} = error), do: error

  defp valid_recovery_id?(value),
    do: is_binary(value) and byte_size(value) in 1..4_096 and String.valid?(value)

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
