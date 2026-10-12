defmodule SymphonyElixir.BackupCapture do
  @moduledoc """
  Filesystem and SQLite snapshot helpers for a private local owner capture.

  This module does not pause database owners or establish a capture boundary. The
  coordinator and database owners must hold their leases while these helpers run.

  Filesystem checks use `lstat` preflight and post-read identity checks. They do not
  provide an atomic no-follow file-descriptor open against concurrent filesystem
  changes. Database validation runs against the exact bytes read and hashed.
  """

  import Bitwise

  alias Exqlite.Sqlite3

  @capture_id ~r/\A[A-Za-z0-9][A-Za-z0-9._-]{0,63}\z/
  @database_names ["state.sqlite", "evidence.sqlite"]
  @max_database_bytes 256 * 1024 * 1024
  @max_manifest_bytes 1024 * 1024
  @snapshot_timeout_ms 30_000
  @progress_handler_steps 1_000

  @type plan :: %{
          id: String.t(),
          root: String.t(),
          directory: String.t(),
          state_path: String.t(),
          evidence_path: String.t(),
          manifest_path: String.t()
        }

  @type database_snapshot :: %{sha256: String.t(), bytes: non_neg_integer(), schema: map()}

  @doc "Creates an exclusive capture directory or returns a previously verified completion."
  @spec prepare(Path.t(), String.t(), [Path.t()] | Path.t()) ::
          {:ok, plan()} | {:completed, map()} | {:error, atom()}
  def prepare(root, capture_id, workspace_roots) do
    with {:ok, root} <- validate_root(root),
         :ok <- validate_capture_id(capture_id),
         :ok <- validate_workspace_roots(root, workspace_roots) do
      directory = Path.join(root, capture_id)

      case File.lstat(directory) do
        {:error, :enoent} -> create_capture_directory(root, capture_id, directory)
        {:ok, _stat} -> existing_capture(root, capture_id)
        {:error, _reason} -> {:error, :invalid_capture_directory}
      end
    end
  end

  @doc "Writes a local completion manifest last, after validating both fixed SQLite copies."
  @spec complete(plan(), map()) :: {:ok, map()} | {:error, atom()}
  def complete(plan, metadata) when is_map(metadata) do
    with {:ok, plan} <- validate_plan(plan),
         :ok <- validate_capture_directory(plan.root, plan.id),
         {:ok, state_database} <- inspect_database(plan.state_path),
         {:ok, evidence_database} <- inspect_database(plan.evidence_path),
         {:ok, metadata} <- normalize_metadata(metadata),
         :ok <- require_absent(plan.manifest_path),
         {:ok, root_sync} <- sync_directory(plan.root),
         {:ok, directory_sync} <- sync_directory(plan.directory),
         directory_synced <- root_sync == :synced and directory_sync == :synced,
         manifest <-
           completion_manifest(
             plan,
             metadata,
             state_database,
             evidence_database,
             directory_synced
           ),
         {:ok, encoded} <- encode_manifest(manifest) do
      case write_exclusive(plan.manifest_path, encoded) do
        :ok ->
          case sync_directory(plan.directory) do
            {:ok, :synced} ->
              case verify_from_root(plan.root, plan.id) do
                {:ok, verified} ->
                  {:ok, verified}

                {:error, _reason} = error ->
                  remove_created_file(plan.manifest_path)
                  error

                :not_found ->
                  remove_created_file(plan.manifest_path)
                  {:error, :invalid_capture}
              end

            {:ok, :unavailable} when not directory_synced ->
              case verify_from_root(plan.root, plan.id) do
                {:ok, verified} ->
                  {:ok, verified}

                {:error, _reason} = error ->
                  remove_created_file(plan.manifest_path)
                  error

                :not_found ->
                  remove_created_file(plan.manifest_path)
                  {:error, :invalid_capture}
              end

            {:ok, :unavailable} ->
              remove_created_file(plan.manifest_path)
              {:error, :directory_sync_failed}

            {:error, _reason} = error ->
              remove_created_file(plan.manifest_path)
              error
          end

        {:error, _reason} = error ->
          error
      end
    else
      {:error, _reason} = error -> error
      _other -> {:error, :invalid_capture}
    end
  end

  def complete(_plan, _metadata), do: {:error, :invalid_metadata}

  @doc "Verifies a completed capture without trusting manifest-supplied paths."
  @spec verify(Path.t(), String.t(), [Path.t()] | Path.t()) ::
          {:ok, map()} | :not_found | {:error, atom()}
  def verify(root, capture_id, workspace_roots) do
    with {:ok, root} <- validate_root(root),
         :ok <- validate_capture_id(capture_id),
         :ok <- validate_workspace_roots(root, workspace_roots) do
      verify_from_root(root, capture_id)
    end
  end

  @doc "Creates a standalone SQLite copy from the supplied owner connection."
  @spec snapshot(Sqlite3.db(), Path.t()) :: {:ok, database_snapshot()} | {:error, atom()}
  def snapshot(db, target) do
    case validate_snapshot_target(target) do
      {:ok, target} ->
        case do_snapshot(db, target) do
          {:ok, result} ->
            {:ok, result}

          {:error, reason} ->
            remove_partial_snapshot(target)
            {:error, reason}
        end

      {:error, _reason} = error ->
        error
    end
  rescue
    _error ->
      {:error, :snapshot_failed}
  catch
    _kind, _reason ->
      {:error, :snapshot_failed}
  end

  defp create_capture_directory(root, capture_id, directory) do
    case File.mkdir(directory) do
      :ok ->
        with :ok <- File.chmod(directory, 0o700),
             :ok <- validate_capture_directory(root, capture_id),
             {:ok, _sync_status} <- sync_directory(root) do
          {:ok, capture_plan(root, capture_id)}
        else
          _other -> {:error, :capture_directory_failed}
        end

      {:error, :eexist} ->
        existing_capture(root, capture_id)

      {:error, _reason} ->
        {:error, :capture_directory_failed}
    end
  end

  defp existing_capture(root, capture_id) do
    case verify_from_root(root, capture_id) do
      {:ok, manifest} -> {:completed, manifest}
      :not_found -> {:error, :incomplete_capture}
      {:error, _reason} = error -> error
    end
  end

  defp capture_plan(root, capture_id) do
    directory = Path.join(root, capture_id)

    %{
      id: capture_id,
      root: root,
      directory: directory,
      state_path: Path.join(directory, "state.sqlite"),
      evidence_path: Path.join(directory, "evidence.sqlite"),
      manifest_path: Path.join(directory, "manifest.json")
    }
  end

  defp validate_plan(%{
         id: capture_id,
         root: root,
         directory: directory,
         state_path: state_path,
         evidence_path: evidence_path,
         manifest_path: manifest_path
       }) do
    with {:ok, root} <- validate_root(root),
         :ok <- validate_capture_id(capture_id),
         expected <- capture_plan(root, capture_id),
         true <- directory == expected.directory,
         true <- state_path == expected.state_path,
         true <- evidence_path == expected.evidence_path,
         true <- manifest_path == expected.manifest_path do
      {:ok, expected}
    else
      _other -> {:error, :invalid_capture_plan}
    end
  end

  defp validate_plan(_plan), do: {:error, :invalid_capture_plan}

  defp absolute_clean_path?(path) when is_binary(path) do
    Path.type(path) == :absolute and not String.contains?(path, <<0>>) and
      ".." not in Path.split(path)
  end

  defp absolute_clean_path?(_path), do: false

  defp validate_root(root) when is_binary(root) do
    if absolute_clean_path?(root) do
      root = Path.expand(root)

      with :ok <- no_symlink_ancestry(root),
           {:ok, stat} <- File.lstat(root),
           :ok <- private_owned_directory(stat) do
        {:ok, root}
      else
        _other -> {:error, :invalid_capture_root}
      end
    else
      {:error, :invalid_capture_root}
    end
  end

  defp validate_root(_root), do: {:error, :invalid_capture_root}

  defp validate_capture_id(capture_id) when is_binary(capture_id) do
    if Regex.match?(@capture_id, capture_id), do: :ok, else: {:error, :invalid_capture_id}
  end

  defp validate_capture_id(_capture_id), do: {:error, :invalid_capture_id}

  defp validate_workspace_roots(root, workspace_roots) do
    with {:ok, roots} <- normalize_workspace_roots(workspace_roots) do
      if Enum.any?(roots, &paths_overlap?(root, &1)),
        do: {:error, :capture_root_overlaps_workspace},
        else: :ok
    end
  end

  defp normalize_workspace_roots(roots) when is_binary(roots),
    do: normalize_workspace_roots([roots])

  defp normalize_workspace_roots(roots) when is_list(roots) do
    Enum.reduce_while(roots, {:ok, []}, fn root, {:ok, acc} ->
      if absolute_clean_path?(root) do
        {:cont, {:ok, [Path.expand(root) | acc]}}
      else
        {:halt, {:error, :invalid_workspace_roots}}
      end
    end)
    |> case do
      {:ok, roots} -> {:ok, Enum.reverse(roots)}
      error -> error
    end
  end

  defp normalize_workspace_roots(_roots), do: {:error, :invalid_workspace_roots}

  defp paths_overlap?(left, right), do: path_within?(left, right) or path_within?(right, left)

  defp path_within?(path, parent) do
    parent = String.trim_trailing(parent, "/")

    path == parent or
      if(parent == "", do: String.starts_with?(path, "/"), else: String.starts_with?(path, parent <> "/"))
  end

  defp verify_from_root(root, capture_id) do
    directory = Path.join(root, capture_id)

    case File.lstat(directory) do
      {:error, :enoent} ->
        :not_found

      {:ok, %{type: :directory} = stat} ->
        with :ok <- private_owned_directory(stat),
             :ok <- no_symlink_ancestry(directory),
             :ok <- exact_capture_files(directory),
             {:ok, manifest} <- read_manifest(Path.join(directory, "manifest.json"), capture_id),
             :ok <- verify_manifest_databases(directory, manifest) do
          {:ok, manifest}
        else
          {:error, _reason} = error -> error
          _other -> {:error, :invalid_capture}
        end

      {:ok, _stat} ->
        {:error, :invalid_capture_directory}

      {:error, _reason} ->
        {:error, :invalid_capture_directory}
    end
  end

  defp validate_capture_directory(root, capture_id) do
    directory = Path.join(root, capture_id)

    with :ok <- no_symlink_ancestry(directory),
         {:ok, %{type: :directory} = stat} <- File.lstat(directory),
         :ok <- private_owned_directory(stat) do
      :ok
    else
      _other -> {:error, :invalid_capture_directory}
    end
  end

  defp exact_capture_files(directory) do
    case File.ls(directory) do
      {:ok, names} ->
        if Enum.sort(names) == Enum.sort(["state.sqlite", "evidence.sqlite", "manifest.json"]),
          do: :ok,
          else: {:error, :incomplete_capture}

      {:error, _reason} ->
        {:error, :invalid_capture_directory}
    end
  end

  defp read_manifest(path, capture_id) do
    with {:ok, stat} <- private_file_stat(path),
         true <- stat.size > 0 and stat.size <= @max_manifest_bytes,
         {:ok, bytes} <- read_bounded_file(path, @max_manifest_bytes),
         true <- byte_size(bytes) == stat.size,
         :ok <- unchanged_private_file(path, stat),
         {:ok, decoded} <- Jason.decode(bytes, objects: :ordered_objects, strings: :copy),
         {:ok, manifest} <- decode_json_without_duplicate_keys(decoded),
         :ok <- validate_manifest(manifest, capture_id) do
      {:ok, manifest}
    else
      {:error, :enoent} -> {:error, :incomplete_capture}
      {:error, _reason} -> {:error, :invalid_manifest}
      _other -> {:error, :invalid_manifest}
    end
  end

  defp validate_manifest(manifest, capture_id) when is_map(manifest) do
    with true <-
           exact_keys?(manifest, [
             "version",
             "kind",
             "capture_id",
             "created_at",
             "restore_ready",
             "databases",
             "metadata",
             "durability"
           ]),
         true <- manifest["version"] == 1,
         true <- manifest["kind"] == "local_owner_capture",
         true <- manifest["capture_id"] == capture_id,
         true <- manifest["restore_ready"] == false,
         true <- is_binary(manifest["created_at"]),
         true <- is_map(manifest["metadata"]),
         durability when is_map(durability) <- manifest["durability"],
         true <- exact_keys?(durability, ["files_synced", "durable_directory_synced"]),
         true <- durability["files_synced"] == true,
         true <- is_boolean(durability["durable_directory_synced"]),
         databases when is_map(databases) <- manifest["databases"],
         true <- exact_keys?(databases, ["state", "evidence"]),
         :ok <- validate_database_record(databases["state"], "state.sqlite", "workstream_store"),
         :ok <- validate_database_record(databases["evidence"], "evidence.sqlite", "evidence_index") do
      :ok
    else
      _other -> {:error, :invalid_manifest}
    end
  end

  defp validate_manifest(_manifest, _capture_id), do: {:error, :invalid_manifest}

  defp validate_database_record(record, filename, identity) when is_map(record) do
    with true <- exact_keys?(record, ["file", "identity", "bytes", "sha256", "schema"]),
         true <- record["file"] == filename,
         true <- record["identity"] == identity,
         true <- is_integer(record["bytes"]) and record["bytes"] > 0,
         true <-
           is_binary(record["sha256"]) and
             Regex.match?(~r/\A[0-9a-f]{64}\z/, record["sha256"]),
         schema when is_map(schema) <- record["schema"],
         true <- exact_keys?(schema, ["user_version"]),
         true <- is_integer(schema["user_version"]) and schema["user_version"] >= 0 do
      :ok
    else
      _other -> {:error, :invalid_manifest}
    end
  end

  defp validate_database_record(_record, _filename, _identity), do: {:error, :invalid_manifest}

  defp verify_manifest_databases(directory, manifest) do
    checks = [
      {"state", "state.sqlite"},
      {"evidence", "evidence.sqlite"}
    ]

    Enum.reduce_while(checks, :ok, fn {key, filename}, :ok ->
      record = manifest["databases"][key]

      case inspect_database(Path.join(directory, filename)) do
        {:ok, actual} ->
          expected = %{
            "file" => filename,
            "identity" => record["identity"],
            "bytes" => actual.bytes,
            "sha256" => actual.sha256,
            "schema" => actual.schema
          }

          if Map.take(record, Map.keys(expected)) == expected,
            do: {:cont, :ok},
            else: {:halt, {:error, :capture_integrity_failed}}

        {:error, _reason} ->
          {:halt, {:error, :capture_integrity_failed}}
      end
    end)
  end

  defp completion_manifest(plan, metadata, state_database, evidence_database, directory_synced) do
    %{
      "version" => 1,
      "kind" => "local_owner_capture",
      "capture_id" => plan.id,
      "created_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "restore_ready" => false,
      "durability" => %{
        "files_synced" => true,
        "durable_directory_synced" => directory_synced
      },
      "databases" => %{
        "state" => database_record("state.sqlite", "workstream_store", state_database),
        "evidence" => database_record("evidence.sqlite", "evidence_index", evidence_database)
      },
      "metadata" => metadata
    }
  end

  defp database_record(filename, identity, database) do
    %{
      "file" => filename,
      "identity" => identity,
      "bytes" => database.bytes,
      "sha256" => database.sha256,
      "schema" => database.schema
    }
  end

  defp normalize_metadata(metadata) do
    case normalize_json(metadata) do
      {:ok, normalized} when is_map(normalized) -> {:ok, normalized}
      _other -> {:error, :invalid_metadata}
    end
  end

  defp normalize_json(value) when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
    do: {:ok, value}

  defp normalize_json(value) when is_atom(value), do: {:ok, Atom.to_string(value)}

  defp normalize_json(value) when is_list(value) do
    Enum.reduce_while(value, {:ok, []}, fn item, {:ok, acc} ->
      case normalize_json(item) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        _other -> {:halt, {:error, :invalid_metadata}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp normalize_json(value) when is_map(value) do
    Enum.reduce_while(value, {:ok, %{}}, fn {key, item}, {:ok, acc} ->
      key = if is_atom(key), do: Atom.to_string(key), else: key

      if is_binary(key) do
        case normalize_json(item) do
          {:ok, normalized} when not is_map_key(acc, key) ->
            {:cont, {:ok, Map.put(acc, key, normalized)}}

          _other ->
            {:halt, {:error, :invalid_metadata}}
        end
      else
        {:halt, {:error, :invalid_metadata}}
      end
    end)
  end

  defp normalize_json(_value), do: {:error, :invalid_metadata}

  defp encode_manifest(manifest) do
    case Jason.encode(manifest) do
      {:ok, bytes} when byte_size(bytes) <= @max_manifest_bytes -> {:ok, bytes}
      {:ok, _bytes} -> {:error, :manifest_too_large}
      {:error, _reason} -> {:error, :invalid_metadata}
    end
  end

  defp exact_keys?(map, expected) when is_map(map),
    do: map_size(map) == length(expected) and Enum.all?(expected, &Map.has_key?(map, &1))

  defp exact_keys?(_map, _expected), do: false

  defp decode_json_without_duplicate_keys(%Jason.OrderedObject{values: pairs}) do
    Enum.reduce_while(pairs, {:ok, %{}, MapSet.new()}, fn
      {key, value}, {:ok, acc, seen} when is_binary(key) ->
        if MapSet.member?(seen, key) do
          {:halt, {:error, :duplicate_json_key}}
        else
          case decode_json_without_duplicate_keys(value) do
            {:ok, decoded} -> {:cont, {:ok, Map.put(acc, key, decoded), MapSet.put(seen, key)}}
            error -> {:halt, error}
          end
        end

      _pair, _acc ->
        {:halt, {:error, :invalid_json_object}}
    end)
    |> case do
      {:ok, map, _seen} -> {:ok, map}
      error -> error
    end
  end

  defp decode_json_without_duplicate_keys(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case decode_json_without_duplicate_keys(value) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      error -> error
    end
  end

  defp decode_json_without_duplicate_keys(value) when is_map(value), do: {:ok, value}
  defp decode_json_without_duplicate_keys(value), do: {:ok, value}

  defp write_exclusive(path, bytes) do
    case :file.open(String.to_charlist(path), [:write, :binary, :exclusive, :raw]) do
      {:ok, file} ->
        result =
          with :ok <- :file.write(file, bytes),
               :ok <- File.chmod(path, 0o600),
               :ok <- :file.sync(file) do
            :ok
          else
            _other -> {:error, :manifest_write_failed}
          end

        close_result = :file.close(file)
        final_result = if result == :ok and close_result == :ok, do: :ok, else: {:error, :manifest_write_failed}

        if final_result == :ok do
          :ok
        else
          remove_created_file(path)
          final_result
        end

      {:error, :eexist} ->
        {:error, :manifest_exists}

      {:error, _reason} ->
        {:error, :manifest_write_failed}
    end
  end

  defp validate_snapshot_target(target) when is_binary(target) do
    if absolute_clean_path?(target) do
      target = Path.expand(target)
      parent = Path.dirname(target)

      with true <- Path.basename(target) in @database_names,
           :ok <- no_symlink_ancestry(parent),
           {:ok, stat} <- File.lstat(parent),
           :ok <- private_owned_directory(stat),
           :ok <- require_absent(target),
           :ok <- require_absent(target <> "-wal"),
           :ok <- require_absent(target <> "-shm"),
           :ok <- require_absent(target <> "-journal") do
        {:ok, target}
      else
        _other -> {:error, :unsafe_snapshot_target}
      end
    else
      {:error, :unsafe_snapshot_target}
    end
  end

  defp validate_snapshot_target(_target), do: {:error, :unsafe_snapshot_target}

  defp do_snapshot(db, target) do
    with {:ok, source_schema} <-
           run_database_with_deadline(db, fn -> prepare_and_vacuum(db, target) end),
         :ok <- secure_snapshot(target),
         {:ok, result} <- inspect_database(target),
         true <- result.schema == source_schema do
      {:ok, result}
    else
      {:error, _reason} = error -> error
      _other -> {:error, :snapshot_failed}
    end
  rescue
    _error -> {:error, :snapshot_failed}
  catch
    _kind, _reason -> {:error, :snapshot_failed}
  end

  defp prepare_and_vacuum(db, target) do
    with {:ok, schema} <- schema_version(db),
         {:ok, source_bytes} <- database_size(db),
         true <- source_bytes <= @max_database_bytes,
         :ok <- vacuum_into(db, target) do
      {:ok, schema}
    else
      _other -> {:error, :source_database_too_large_or_invalid}
    end
  end

  defp database_size(db) do
    with {:ok, [[page_count]]} <- query_rows(db, "PRAGMA page_count"),
         {:ok, [[page_size]]} <- query_rows(db, "PRAGMA page_size"),
         true <- is_integer(page_count) and page_count >= 0,
         true <- is_integer(page_size) and page_size > 0 do
      {:ok, page_count * page_size}
    else
      _other -> {:error, :invalid_capture_database}
    end
  end

  defp run_database_with_deadline(db, operation) do
    with :ok <- Sqlite3.set_progress_handler_steps(db, @progress_handler_steps) do
      token = make_ref()
      caller = self()

      {watchdog, monitor} =
        spawn_monitor(fn ->
          receive do
            {:finish, ^token} ->
              send(caller, {:backup_capture_watchdog, token, :finished})
          after
            @snapshot_timeout_ms ->
              _ = cancel_database(db)
              send(caller, {:backup_capture_watchdog, token, :timed_out})
          end
        end)

      started_at = System.monotonic_time(:millisecond)
      result = safe_database_operation(operation)
      elapsed = System.monotonic_time(:millisecond) - started_at
      send(watchdog, {:finish, token})

      guard_result =
        receive do
          {:backup_capture_watchdog, ^token, :finished} -> :finished
          {:backup_capture_watchdog, ^token, :timed_out} -> :timed_out
          {:DOWN, ^monitor, :process, ^watchdog, _reason} -> :watchdog_failed
        end

      _ = Process.demonitor(monitor, [:flush])

      if guard_result == :timed_out or guard_result == :watchdog_failed or
           elapsed > @snapshot_timeout_ms do
        {:error, :snapshot_timeout}
      else
        result
      end
    else
      _other -> {:error, :snapshot_failed}
    end
  rescue
    _error -> {:error, :snapshot_failed}
  catch
    _kind, _reason -> {:error, :snapshot_failed}
  end

  defp safe_database_operation(operation) do
    operation.()
  rescue
    _error -> {:error, :snapshot_failed}
  catch
    _kind, _reason -> {:error, :snapshot_failed}
  end

  defp vacuum_into(db, target) do
    case Sqlite3.prepare(db, "VACUUM main INTO ?1") do
      {:ok, statement} ->
        try do
          with :ok <- Sqlite3.bind(statement, [target]),
               :done <- Sqlite3.step(db, statement) do
            :ok
          else
            _other -> {:error, :snapshot_failed}
          end
        rescue
          _error -> {:error, :snapshot_failed}
        catch
          _kind, _reason -> {:error, :snapshot_failed}
        after
          _ = safe_release(db, statement)
        end

      {:error, _reason} ->
        {:error, :snapshot_failed}
    end
  end

  defp inspect_database(path) do
    with {:ok, stat} <- private_file_stat(path),
         true <- stat.size > 0 and stat.size <= @max_database_bytes,
         {:ok, bytes} <- read_bounded_file(path, @max_database_bytes),
         true <- byte_size(bytes) == stat.size,
         :ok <- standalone_database_header(bytes),
         {:ok, schema} <- verify_sqlite_bytes(bytes),
         :ok <- unchanged_private_file(path, stat),
         :ok <- no_sqlite_sidecars(path) do
      {:ok,
       %{
         bytes: byte_size(bytes),
         sha256: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower),
         schema: schema
       }}
    else
      {:error, _reason} = error -> error
      _other -> {:error, :invalid_capture_database}
    end
  rescue
    _error -> {:error, :invalid_capture_database}
  catch
    _kind, _reason -> {:error, :invalid_capture_database}
  end

  defp standalone_database_header(<<"SQLite format 3", 0, _page_size::binary-size(2), 1, 1, _rest::binary>>),
    do: :ok

  defp standalone_database_header(_bytes), do: {:error, :invalid_capture_database}

  defp verify_sqlite_bytes(bytes) do
    case Sqlite3.open(":memory:") do
      {:ok, db} ->
        result =
          with :ok <- Sqlite3.deserialize(db, "main", bytes),
               {:ok, schema} <-
                 run_database_with_deadline(db, fn -> verify_integrity_and_schema(db) end) do
            {:ok, schema}
          else
            _other -> {:error, :invalid_capture_database}
          end

        _ = safe_close(db)
        result

      {:error, _reason} ->
        {:error, :invalid_capture_database}
    end
  rescue
    _error -> {:error, :invalid_capture_database}
  catch
    _kind, _reason -> {:error, :invalid_capture_database}
  end

  defp verify_integrity_and_schema(db) do
    with {:ok, [["ok"]]} <- query_rows(db, "PRAGMA integrity_check"),
         {:ok, schema} <- schema_version(db) do
      {:ok, schema}
    else
      _other -> {:error, :invalid_capture_database}
    end
  end

  defp schema_version(db) do
    case query_rows(db, "PRAGMA user_version") do
      {:ok, [[version]]} when is_integer(version) and version >= 0 ->
        {:ok, %{"user_version" => version}}

      _other ->
        {:error, :snapshot_failed}
    end
  end

  defp query_rows(db, sql) do
    with {:ok, statement} <- Sqlite3.prepare(db, sql) do
      result = Sqlite3.fetch_all(db, statement)
      _ = safe_release(db, statement)
      result
    else
      _other -> {:error, :sqlite_query_failed}
    end
  rescue
    _error -> {:error, :sqlite_query_failed}
  catch
    _kind, _reason -> {:error, :sqlite_query_failed}
  end

  defp private_file_stat(path) do
    with :ok <- no_symlink_ancestry(Path.dirname(path)),
         {:ok, %{type: :regular} = stat} <- File.lstat(path),
         {:ok, uid} <- current_uid(),
         true <- stat.uid == uid,
         true <- stat.links == 1,
         true <- band(stat.mode, 0o077) == 0 do
      {:ok, stat}
    else
      _other -> {:error, :invalid_capture_file}
    end
  end

  defp read_bounded_file(path, max_bytes) do
    case :file.open(String.to_charlist(path), [:read, :binary, :raw]) do
      {:ok, file} ->
        try do
          read_bounded_chunks(file, max_bytes, 0, [])
        after
          _ = :file.close(file)
        end

      {:error, _reason} ->
        {:error, :file_read_failed}
    end
  rescue
    _error -> {:error, :file_read_failed}
  catch
    _kind, _reason -> {:error, :file_read_failed}
  end

  defp read_bounded_chunks(file, max_bytes, bytes_read, chunks) do
    requested = min(64 * 1024, max_bytes + 1 - bytes_read)

    case :file.read(file, requested) do
      :eof ->
        {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}

      {:ok, <<>>} ->
        {:error, :file_read_failed}

      {:ok, chunk} ->
        next_size = bytes_read + byte_size(chunk)

        if next_size > max_bytes do
          {:error, :file_too_large}
        else
          read_bounded_chunks(file, max_bytes, next_size, [chunk | chunks])
        end

      {:error, _reason} ->
        {:error, :file_read_failed}
    end
  end

  defp unchanged_private_file(path, initial_stat) do
    with {:ok, final_stat} <- private_file_stat(path),
         true <- file_identity(initial_stat) == file_identity(final_stat) do
      :ok
    else
      _other -> {:error, :invalid_capture_file}
    end
  end

  defp file_identity(stat) do
    {
      stat.type,
      stat.links,
      stat.mode,
      stat.uid,
      stat.size,
      stat.inode,
      stat.major_device,
      stat.minor_device,
      stat.mtime,
      stat.ctime
    }
  end

  defp secure_snapshot(path) do
    with {:ok, %{type: :regular} = stat} <- File.lstat(path),
         {:ok, uid} <- current_uid(),
         true <- stat.uid == uid,
         true <- stat.links == 1,
         :ok <- File.chmod(path, 0o600),
         {:ok, file} <- :file.open(String.to_charlist(path), [:read, :write, :raw, :binary]) do
      try do
        case :file.sync(file) do
          :ok -> :ok
          _other -> {:error, :snapshot_failed}
        end
      after
        _ = :file.close(file)
      end
    else
      _other -> {:error, :snapshot_failed}
    end
  rescue
    _error -> {:error, :snapshot_failed}
  catch
    _kind, _reason -> {:error, :snapshot_failed}
  end

  defp no_sqlite_sidecars(path) do
    if Enum.all?(["-wal", "-shm", "-journal"], fn suffix -> missing?(path <> suffix) end),
      do: :ok,
      else: {:error, :invalid_capture_database}
  end

  defp missing?(path), do: match?({:error, :enoent}, File.lstat(path))

  defp remove_partial_snapshot(path) do
    for suffix <- ["", "-wal", "-shm", "-journal"] do
      candidate = path <> suffix

      case File.lstat(candidate) do
        {:ok, %{type: :regular, links: 1} = stat} ->
          with {:ok, uid} <- current_uid(), true <- stat.uid == uid do
            _ = File.rm(candidate)
          end

        _other ->
          :ok
      end
    end

    :ok
  rescue
    _error -> :ok
  end

  defp remove_created_file(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular, links: 1} = stat} ->
        with {:ok, uid} <- current_uid(), true <- stat.uid == uid do
          _ = File.rm(path)
        end

      _other ->
        :ok
    end

    :ok
  rescue
    _error -> :ok
  end

  defp private_owned_directory(stat) do
    with {:ok, uid} <- current_uid(),
         true <- stat.type == :directory,
         true <- stat.uid == uid,
         true <- band(stat.mode, 0o077) == 0 do
      :ok
    else
      _other -> {:error, :unsafe_directory}
    end
  end

  defp no_symlink_ancestry(path) do
    path
    |> path_prefixes()
    |> Enum.reduce_while(:ok, fn prefix, :ok ->
      case File.lstat(prefix) do
        {:ok, %{type: :directory}} -> {:cont, :ok}
        {:error, :enoent} -> {:halt, {:error, :missing_path_component}}
        _other -> {:halt, {:error, :symlink_or_non_directory_component}}
      end
    end)
  end

  defp path_prefixes(path) do
    path
    |> Path.split()
    |> Enum.reduce({[], ""}, fn component, {prefixes, parent} ->
      current = if component == "/", do: "/", else: Path.join(parent, component)
      {[current | prefixes], current}
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp current_uid do
    case System.cmd("id", ["-u"], stderr_to_stdout: true) do
      {value, 0} ->
        case Integer.parse(String.trim(value)) do
          {uid, ""} -> {:ok, uid}
          _other -> {:error, :uid_unavailable}
        end

      _other ->
        {:error, :uid_unavailable}
    end
  rescue
    _error -> {:error, :uid_unavailable}
  end

  defp require_absent(path) do
    case File.lstat(path) do
      {:error, :enoent} -> :ok
      {:ok, _stat} -> {:error, :path_exists}
      {:error, _reason} -> {:error, :unsafe_path}
    end
  end

  defp sync_directory(path) do
    case :file.open(String.to_charlist(path), [:read, :raw]) do
      {:ok, directory} ->
        sync_result = :file.sync(directory)
        close_result = :file.close(directory)

        case {sync_result, close_result} do
          {:ok, :ok} ->
            {:ok, :synced}

          {{:error, reason}, :ok}
          when reason in [:einval, :enotsup, :eopnotsupp, :enosys] ->
            {:ok, :unavailable}

          _other ->
            {:error, :directory_sync_failed}
        end

      {:error, reason}
      when reason in [:eisdir, :enotsup, :eopnotsupp, :einval, :enosys] ->
        {:ok, :unavailable}

      _other ->
        {:error, :directory_sync_failed}
    end
  rescue
    _error -> {:error, :directory_sync_failed}
  end

  defp safe_release(db, statement) do
    Sqlite3.release(db, statement)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp safe_close(db) do
    Sqlite3.close(db)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp cancel_database(db) do
    Sqlite3.cancel(db)
  rescue
    _error -> {:error, :cancel_failed}
  catch
    _kind, _reason -> {:error, :cancel_failed}
  end
end
