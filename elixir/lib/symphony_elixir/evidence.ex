defmodule SymphonyElixir.Evidence do
  @moduledoc """
  Private evidence index and immutable upload outbox, outside disposable workspaces.

  All persisted inputs are redacted. The root uploader reads every GCS object back
  before issuing a receipt. A local bundle alone never qualifies for cleanup.
  """
  use GenServer
  require Logger

  alias Exqlite.Sqlite3
  alias SymphonyElixir.{Config, EvidenceRedaction, PathSafety, WorkstreamRun, WorkstreamStore}

  @limit 10 * 1024 * 1024
  @identity_keys ~w(task_id definitions pinned_definitions agents policy agent_selection stage_agents)
  @schema """
  CREATE TABLE IF NOT EXISTS evidence_runs (run_id TEXT PRIMARY KEY, task_key TEXT NOT NULL, snapshot TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS evidence_events (seq INTEGER PRIMARY KEY, run_id TEXT NOT NULL, event_id TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(run_id,event_id));
  CREATE TABLE IF NOT EXISTS evidence_artifacts (run_id TEXT NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(run_id,name));
  PRAGMA user_version=1;
  """

  @spec validate_config(map()) :: {:ok, map()} | {:error, atom()}
  def validate_config(config) do
    valid =
      is_binary(config["root"]) and Path.type(config["root"]) == :absolute and
        is_binary(config["base_url"]) and URI.parse(config["base_url"]).scheme == "https" and
        URI.parse(config["base_url"]).host not in [nil, ""] and
        is_binary(config["iap_audience"]) and Regex.match?(~r|^/projects/[0-9]+/global/backendServices/[0-9]+$|, config["iap_audience"]) and
        valid_allowlist?(config["allowed_domains"] || []) and valid_allowlist?(config["allowed_emails"] || []) and
        (config["allowed_domains"] || []) ++ (config["allowed_emails"] || []) != []

    if valid, do: {:ok, config}, else: {:error, :invalid_evidence_config}
  end

  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts \\ []) do
    case Keyword.get_lazy(opts, :config, &Config.evidence/0) do
      :disabled ->
        :ignore

      {:ok, config} ->
        names =
          case Keyword.get(opts, :name, __MODULE__) do
            nil -> []
            name -> [name: name]
          end

        GenServer.start_link(__MODULE__, Keyword.put(opts, :config, config), names)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec task_key(String.t()) :: String.t()
  def task_key(task_id), do: digest(task_id)

  @spec url(map()) :: String.t() | nil
  def url(run) do
    case Config.evidence() do
      {:ok, config} -> String.trim_trailing(config["base_url"], "/") <> "/tasks/" <> task_key(run.task_id)
      _ -> nil
    end
  end

  @spec snapshot(map(), String.t(), map(), GenServer.server()) :: :ok | {:error, atom()}
  def snapshot(run, event_id, event, server \\ __MODULE__) do
    call(server, {:snapshot, report(run), event_id, EvidenceRedaction.safe(event)})
  end

  @doc "Redacted evidence journal stored atomically with a durable run commit."
  @spec journal(map(), map()) :: map() | :invalid
  def journal(run, event) do
    snapshot = report(run)
    event = EvidenceRedaction.safe(event)
    wrapped = Map.merge(event, %{"run_id" => snapshot["id"], "stage_id" => snapshot["stage_id"], "attempt_id" => snapshot["current_attempt_id"], "snapshot" => snapshot})

    if valid_run?(snapshot["id"]) and valid_task?(snapshot["task_id"]) and byte_size(Jason.encode!(wrapped)) <= @limit,
      do: %{snapshot: snapshot, event: event},
      else: :invalid
  end

  @doc "Drains committed evidence before dispatch or durable acknowledgement."
  @spec sync!(GenServer.server()) :: :ok
  def sync!(store) do
    case Config.evidence() do
      :disabled -> :ok
      {:ok, _} -> sync_pending!(store)
      _ -> exit(:private_evidence_config_unavailable)
    end
  end

  defp sync_pending!(store) do
    with {:ok, pending} <- WorkstreamStore.pending_evidence(store) do
      Enum.each(pending, fn entry ->
        with :ok <- call(__MODULE__, {:snapshot, entry.snapshot, entry.event_id, entry.event}),
             :ok <- WorkstreamStore.acknowledge_evidence(store, entry.event_id) do
          :ok
        else
          _ -> exit(:private_evidence_sync_failed)
        end
      end)
    else
      _ -> exit(:private_evidence_sync_failed)
    end

    :ok
  end

  @doc "Reconciles retained pre-evidence runs after draining committed journals."
  @spec recover!(GenServer.server(), [map()]) :: :ok
  def recover!(store, runs) do
    sync!(store)

    case Config.evidence() do
      :disabled ->
        :ok

      {:ok, _} ->
        Enum.each(runs, fn run ->
          recover_run!(run)
          retain_validation!(run)
        end)

      _ ->
        exit(:private_evidence_config_unavailable)
    end

    :ok
  end

  defp recover_run!(run) do
    snapshot = report(run)

    case call(__MODULE__, {:current_snapshot, run.id}) do
      {:ok, ^snapshot} ->
        :ok

      :not_found ->
        id = "recovery/" <> digest(Jason.encode!(snapshot))
        if call(__MODULE__, {:snapshot, snapshot, id, %{"kind" => "recovered_baseline"}}) != :ok, do: exit(:private_evidence_recovery_failed)

      _ ->
        exit(:private_evidence_recovery_conflict)
    end
  end

  @doc "Fails the attempt visibly if observable evidence cannot be persisted."
  @spec capture!(map(), map()) :: :ok
  def capture!(run, event) do
    case capture(run, event) do
      :ok -> :ok
      _ -> raise RuntimeError, "private_evidence_capture_failed"
    end
  end

  @doc "Redacted pins used by a durable owner's explicit agent reassignment event."
  @spec identity(map()) :: map()
  def identity(run), do: Map.take(report(run), @identity_keys)

  defp report(run) do
    report = run |> WorkstreamRun.report() |> Map.drop([:workspace, :operations, :continuation])
    definitions = Map.new(run.definition.definitions, fn {path, resource} -> {path, Map.take(resource, [:sha256, :text])} end)
    stage_agents = stage_agent_bindings(Map.get(run.definition, :stages, %{}))

    report =
      Map.merge(report, %{
        policy: run.policy,
        pinned_definitions: definitions,
        agent_selection: Map.get(run.definition, :agent_selection),
        stage_agents: stage_agents,
        parent_run_id: run[:parent_run_id],
        child_run_ids: run[:child_run_ids] || []
      })

    EvidenceRedaction.safe(report)
  end

  defp stage_agent_bindings(stages) when is_map(stages) do
    Enum.reduce(stages, %{}, fn {id, stage}, bindings ->
      type = if is_map(stage), do: Map.get(stage, :type, Map.get(stage, "type")), else: nil
      agent = if is_map(stage), do: Map.get(stage, :agent, Map.get(stage, "agent")), else: nil

      if is_binary(id) and type in [:agent, "agent"] and is_binary(agent),
        do: Map.put(bindings, id, agent),
        else: bindings
    end)
  end

  defp stage_agent_bindings(_stages), do: %{}

  @spec capture(map(), map(), GenServer.server()) :: :ok | {:error, atom()}
  def capture(run, event, server \\ __MODULE__) do
    case EvidenceRedaction.observable(event) do
      :ignore ->
        :ok

      observed ->
        observed = Map.merge(observed, %{"run_id" => run.id, "stage_id" => run.stage_id, "attempt_id" => run.current_attempt_id})
        call(server, {:event, run.id, digest(Jason.encode!(observed)), observed})
    end
  end

  @spec artifact(String.t() | map(), String.t(), binary()) :: :ok | {:error, atom()}
  def artifact(run_id, label, payload), do: artifact(run_id, label, payload, __MODULE__)

  @spec artifact(String.t() | map(), String.t(), binary(), GenServer.server()) :: :ok | {:error, atom()}

  def artifact(run_id, label, payload, server) when is_binary(payload) and byte_size(payload) <= @limit do
    redacted =
      case Jason.decode(payload) do
        {:ok, json} -> Jason.encode!(EvidenceRedaction.safe(json))
        _ -> EvidenceRedaction.text(payload)
      end

    name = "artifact-" <> digest(redacted) <> ".txt"

    event = %{
      "kind" => "artifact",
      "label" => EvidenceRedaction.text(label),
      "name" => name,
      "source_sha256" => digest(payload),
      "stored_sha256" => digest(redacted),
      "redacted" => redacted != payload
    }

    {run_id, event} =
      case run_id do
        %{id: id, stage_id: stage, current_attempt_id: attempt} -> {id, Map.merge(event, %{"stage_id" => stage, "attempt_id" => attempt})}
        id -> {id, event}
      end

    call(server, {:artifact, run_id, name, redacted, event})
  end

  def artifact(_run_id, _label, _payload, _server), do: {:error, :artifact_too_large}

  @doc "Copies authenticated validation bytes for each saved attempt; never grants gate eligibility."
  @spec retain_validation!(map()) :: :ok
  def retain_validation!(run) do
    case Config.evidence() do
      :disabled -> :ok
      {:ok, _} -> Enum.each(run.attempts, &retain_attempt_validation!(run, &1))
      _ -> exit(:private_evidence_config_unavailable)
    end

    :ok
  end

  defp retain_attempt_validation!(run, attempt) do
    result = attempt[:result] || %{}
    evidence = result[:evidence] || result["evidence"] || %{}

    case evidence["receipt"] do
      nil ->
        :ok

      receipt ->
        event_id = "validation/" <> digest(Jason.encode!([run.id, attempt.id, receipt["manifest_sha256"]]))

        if call(__MODULE__, {:has_event, run.id, event_id}) != true do
          import_validation!(run, attempt, receipt, event_id)
        end
    end
  end

  defp import_validation!(run, attempt, receipt, event_id) do
    archive = run.execution[:validation_archive]
    context = %{id: run.id, stage_id: attempt.stage, current_attempt_id: attempt.id}

    with true <- is_binary(archive),
         {:ok, manifest} <- SymphonyElixir.Validation.read_receipt(receipt, archive),
         {:ok, _} <- SymphonyElixir.Validation.feedback(receipt, archive),
         {:ok, bytes} <- bounded_read(Path.join([archive, manifest["id"], "manifest.json"])),
         true <- digest(bytes) == receipt["manifest_sha256"],
         :ok <- artifact(context, "validation/" <> attempt.id <> "/manifest.json", bytes) do
      Enum.each(manifest["artifacts"], fn item ->
        with true <- Path.basename(item["file"]) == item["file"],
             {:ok, payload} <- bounded_read(Path.join([archive, manifest["id"], item["file"]])),
             true <- digest(payload) == item["sha256"] and byte_size(payload) == item["bytes"],
             :ok <- artifact(context, "validation/" <> attempt.id <> "/" <> item["file"], payload) do
          :ok
        else
          _ -> exit(:private_validation_evidence_import_failed)
        end
      end)

      event = %{"kind" => "validation_imported", "stage_id" => attempt.stage, "attempt_id" => attempt.id, "manifest_sha256" => receipt["manifest_sha256"]}
      if call(__MODULE__, {:event, run.id, event_id, event}) != :ok, do: exit(:private_validation_evidence_import_failed)
    else
      _ -> exit(:private_validation_evidence_import_failed)
    end
  end

  @spec task(String.t(), GenServer.server()) :: {:ok, [map()]} | {:error, atom()}
  def task(key, server \\ __MODULE__), do: call(server, {:task, key})

  @spec list(GenServer.server()) :: {:ok, [map()]} | {:error, atom()}
  def list(server \\ __MODULE__), do: call(server, :list)

  @spec read_artifact(String.t(), String.t(), String.t(), GenServer.server()) :: {:ok, binary()} | {:error, atom()}
  def read_artifact(key, revision, name, server \\ __MODULE__), do: call(server, {:read_artifact, key, revision, name})

  @doc "Finds an immutable, verified upload containing an exact retained validation attempt. It does not grant validation eligibility."
  @spec publication(map(), String.t(), map(), GenServer.server()) :: {:ok, map()} | {:error, atom()}
  def publication(run, attempt_id, validation_receipt, server \\ __MODULE__) do
    call(server, {:publication, report(run), attempt_id, validation_receipt})
  end

  @spec publication_revision(map(), String.t(), map(), String.t(), GenServer.server()) :: {:ok, map()} | {:error, atom()}
  def publication_revision(run, attempt_id, validation_receipt, revision, server \\ __MODULE__) do
    call(server, {:publication_revision, report(run), attempt_id, validation_receipt, revision})
  end

  @spec cleanup_ready?(String.t(), GenServer.server()) :: boolean()
  def cleanup_ready?(run_id, server \\ __MODULE__) do
    case call(server, {:run, run_id}) do
      {:ok, %{snapshot: %{"status" => status}, archive: %{"status" => "verified"}}} when status in ["complete", "completed", "failed", "stopped"] -> true
      _ -> false
    end
  end

  @doc "Verifies the current archive bytes and exact expected sanitized run snapshot."
  @spec retained(map(), GenServer.server()) :: {:ok, map()} | {:error, atom()}
  def retained(run, server \\ __MODULE__), do: call(server, {:retained, report(run)})

  @doc "Reads an immutable output only after verifying its exact permanent archive receipt."
  @spec retained_artifact(map(), GenServer.server()) :: {:ok, binary()} | {:error, atom()}
  def retained_artifact(reference, server \\ __MODULE__), do: call(server, {:retained_artifact, reference})

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    root = Path.expand(config["root"])

    with :ok <- directory(root),
         :ok <- directory(Path.join(root, "bundles")),
         :ok <- regular_or_missing(Path.join(root, "index.sqlite")),
         :ok <- regular_or_missing(Path.join(root, "index.sqlite-wal")),
         :ok <- regular_or_missing(Path.join(root, "index.sqlite-shm")),
         {:ok, db} <- Sqlite3.open(Path.join(root, "index.sqlite")),
         :ok <- Sqlite3.execute(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"),
         {:ok, [[version]]} <- query(db, "PRAGMA user_version", []),
         true <- version in [0, 1],
         :ok <- Sqlite3.execute(db, @schema) do
      File.chmod!(Path.join(root, "index.sqlite"), 0o600)
      Process.send_after(self(), :flush, 10_000)
      {:ok, %{root: root, db: db, receipt_uid: Keyword.get(opts, :receipt_uid, 0)}}
    else
      _ -> {:stop, :evidence_startup_failed}
    end
  end

  @impl true
  def handle_call({:snapshot, snapshot, event_id, event}, _from, state) do
    id = snapshot["id"]
    event = Map.merge(event, %{"run_id" => id, "stage_id" => snapshot["stage_id"], "attempt_id" => snapshot["current_attempt_id"], "snapshot" => snapshot})

    result =
      transact(state.db, fn ->
        with true <- valid_run?(id) and valid_task?(snapshot["task_id"]),
             {:ok, status} <- event_status(state.db, id, event_id, Jason.encode!(event)) do
          case status do
            :duplicate ->
              :ok

            :new ->
              with :ok <- pinned_identity(state.db, id, snapshot, event),
                   :ok <-
                     execute(state.db, "INSERT INTO evidence_runs VALUES (?1,?2,?3) ON CONFLICT(run_id) DO UPDATE SET snapshot=excluded.snapshot", [
                       id,
                       task_key(snapshot["task_id"]),
                       Jason.encode!(snapshot)
                     ]),
                   :ok <- add_event(state.db, id, event_id, event),
                   do: :ok
          end
        else
          _ -> {:error, :evidence_write_failed}
        end
      end)

    {:reply, export_result(result, state, id), state}
  end

  def handle_call({:event, id, event_id, event}, _from, state) do
    result = with {:ok, [_]} <- query(state.db, "SELECT run_id FROM evidence_runs WHERE run_id=?1", [id]), do: add_event(state.db, id, event_id, event)
    {:reply, if(result == :ok, do: :ok, else: {:error, :evidence_write_failed}), state}
  end

  def handle_call({:artifact, id, name, payload, event}, _from, state) do
    result =
      transact(state.db, fn ->
        with {:ok, [_]} <- query(state.db, "SELECT run_id FROM evidence_runs WHERE run_id=?1", [id]),
             :ok <- execute(state.db, "INSERT OR IGNORE INTO evidence_artifacts VALUES (?1,?2,?3)", [id, name, payload]),
             :ok <- add_event(state.db, id, "artifact/" <> digest(Jason.encode!(event)), event),
             do: :ok
      end)

    {:reply, export_result(result, state, id), state}
  end

  def handle_call({:task, key}, _from, state) do
    result =
      with true <- Regex.match?(~r/^[a-f0-9]{64}$/, key),
           {:ok, rows} <- query(state.db, "SELECT run_id FROM evidence_runs WHERE task_key=?1 ORDER BY run_id", [key]) do
        Enum.reduce_while(rows, {:ok, []}, fn [id], {:ok, acc} ->
          case view(state, id) do
            {:ok, value} -> {:cont, {:ok, acc ++ [value]}}
            _ -> {:halt, {:error, :evidence_read_failed}}
          end
        end)
      else
        _ -> {:error, :invalid_task_key}
      end

    {:reply, result, state}
  end

  def handle_call({:run, id}, _from, state), do: {:reply, view(state, id), state}

  def handle_call({:retained, expected}, _from, state) do
    result =
      with {:ok, data} <- bundle(state, expected["id"]),
           true <- data.snapshot == expected,
           {:ok, revision} <- export(state, data),
           %{"status" => "verified"} <- receipt(state, revision, data.manifest),
           {:ok, files} <- publication_files(Path.join([state.root, "bundles", revision]), data.manifest["artifacts"]),
           {:ok, ^expected} <- Jason.decode(files["snapshot.json"]) do
        {:ok, %{run_id: expected["id"], task_key: task_key(expected["task_id"]), revision: revision, manifest_sha256: revision, artifacts: data.manifest["artifacts"]}}
      else
        _ -> {:error, :current_evidence_not_permanently_archived}
      end

    {:reply, result, state}
  end

  def handle_call({:retained_artifact, reference}, _from, state) do
    result =
      with %{run_id: id, task_key: key, revision: revision, name: name, sha256: sha} <- reference,
           true <- is_binary(revision) and Regex.match?(~r/^[a-f0-9]{64}$/, revision),
           directory = Path.join([state.root, "bundles", revision]),
           {:ok, bytes} <- publication_read(Path.join(directory, "manifest.json")),
           true <- digest(bytes) == revision,
           {:ok, manifest} <- Jason.decode(bytes),
           true <- manifest["run_id"] == id and task_key(manifest["task_id"]) == key,
           %{"status" => "verified"} <- receipt(state, revision, manifest),
           {:ok, files} <- publication_files(directory, manifest["artifacts"]),
           payload when is_binary(payload) <- files[name],
           true <- digest(payload) == sha do
        {:ok, payload}
      else
        _ -> {:error, :retained_artifact_unavailable}
      end

    {:reply, result, state}
  end

  def handle_call({:current_snapshot, id}, _from, state) do
    result =
      case query(state.db, "SELECT snapshot FROM evidence_runs WHERE run_id=?1", [id]) do
        {:ok, [[payload]]} -> {:ok, Jason.decode!(payload)}
        {:ok, []} -> :not_found
        _ -> {:error, :evidence_read_failed}
      end

    {:reply, result, state}
  end

  def handle_call({:publication, snapshot, attempt_id, validation_receipt}, _from, state) do
    result = publication_bundle(state, snapshot, attempt_id, validation_receipt)
    {:reply, result, state}
  end

  def handle_call({:publication_revision, snapshot, attempt_id, %{"id" => id, "manifest_sha256" => sha}, revision}, _from, state)
      when is_binary(revision) and is_binary(attempt_id) do
    result =
      if Regex.match?(~r/^[a-f0-9]{64}$/, revision),
        do: verified_publication_bundle(state, revision, snapshot, attempt_id, id, sha),
        else: {:error, :validation_not_permanently_archived}

    {:reply, result, state}
  end

  def handle_call({:publication_revision, _snapshot, _attempt_id, _receipt, _revision}, _from, state),
    do: {:reply, {:error, :validation_not_permanently_archived}, state}

  def handle_call({:has_event, id, event_id}, _from, state) do
    result =
      case query(state.db, "SELECT event_id FROM evidence_events WHERE run_id=?1 AND event_id=?2", [id, event_id]) do
        {:ok, [[^event_id]]} -> true
        {:ok, []} -> false
        _ -> {:error, :evidence_read_failed}
      end

    {:reply, result, state}
  end

  def handle_call(:list, _from, state) do
    result =
      with {:ok, rows} <- query(state.db, "SELECT task_key,snapshot FROM evidence_runs ORDER BY run_id LIMIT 100", []) do
        {:ok,
         Enum.map(rows, fn [key, payload] ->
           snapshot = Jason.decode!(payload)
           %{key: key, task_id: snapshot["task_id"], run_id: snapshot["id"], status: snapshot["status"]}
         end)}
      end

    {:reply, result, state}
  end

  def handle_call({:read_artifact, key, revision, name}, _from, state) do
    result =
      with true <- is_binary(revision) and Regex.match?(~r/^[a-f0-9]{64}$/, revision),
           true <- name in ["snapshot.json", "events.jsonl", "transcript.txt"] or Regex.match?(~r/^artifact-[a-f0-9]{64}\.txt$/, name),
           directory = Path.join([state.root, "bundles", revision]),
           {:ok, manifest_payload} <- bounded_read(Path.join(directory, "manifest.json")),
           true <- digest(manifest_payload) == revision,
           {:ok, manifest} <- Jason.decode(manifest_payload),
           true <- task_key(manifest["task_id"]) == key,
           artifact when is_map(artifact) <- Enum.find(manifest["artifacts"], &(&1["name"] == name)),
           {:ok, payload} <- bounded_read(Path.join(directory, name)),
           true <- digest(payload) == artifact["sha256"] and byte_size(payload) == artifact["bytes"] do
        {:ok, payload}
      else
        _ -> {:error, :evidence_artifact_unavailable}
      end

    {:reply, result, state}
  end

  @impl true
  def terminate(_reason, state), do: Sqlite3.close(state.db)

  @impl true
  def handle_info(:flush, state) do
    case query(state.db, "SELECT run_id FROM evidence_runs", []) do
      {:ok, rows} ->
        Enum.each(rows, fn [id] ->
          if export_result(:ok, state, id) != :ok, do: Logger.error("Private evidence bundle export failed run_id=#{id}")
        end)

      _ ->
        Logger.error("Private evidence index flush failed")
    end

    Process.send_after(self(), :flush, 10_000)
    {:noreply, state}
  end

  defp view(state, id) do
    with {:ok, data} <- bundle(state, id), {:ok, revision} <- export(state, data) do
      {:ok,
       %{
         snapshot: data.snapshot,
         events: data.events,
         artifacts: data.manifest["artifacts"],
         revision: revision,
         archive: receipt(state, revision, data.manifest),
         parent_key: linked_key(state, data.snapshot["parent_run_id"]),
         child_keys: Enum.map(data.snapshot["child_run_ids"] || [], &linked_key(state, &1)) |> Enum.reject(&is_nil/1)
       }}
    else
      _ -> {:error, :evidence_read_failed}
    end
  end

  defp linked_key(state, id) when is_binary(id) do
    case query(state.db, "SELECT task_key FROM evidence_runs WHERE run_id=?1", [id]) do
      {:ok, [[key]]} -> key
      _ -> nil
    end
  end

  defp linked_key(_state, _id), do: nil

  defp bundle(state, id) do
    with {:ok, [[snapshot]]} <- query(state.db, "SELECT snapshot FROM evidence_runs WHERE run_id=?1", [id]),
         {:ok, rows} <- query(state.db, "SELECT payload FROM evidence_events WHERE run_id=?1 ORDER BY seq", [id]),
         {:ok, artifacts} <- query(state.db, "SELECT name,payload FROM evidence_artifacts WHERE run_id=?1 ORDER BY name", [id]) do
      events = Enum.map(rows, fn [json] -> Jason.decode!(json) end)

      files =
        [{"snapshot.json", snapshot}, {"events.jsonl", Enum.map_join(rows, "\n", fn [json] -> json end) <> "\n"}, {"transcript.txt", Enum.map_join(events, "\n\n", &Jason.encode!(&1, pretty: true))}] ++
          Enum.map(artifacts, fn [name, payload] -> {name, payload} end)

      snapshot = Jason.decode!(snapshot)

      manifest = %{
        "version" => 1,
        "task_id" => snapshot["task_id"],
        "run_id" => id,
        "artifacts" => Enum.map(files, fn {name, payload} -> %{"name" => name, "sha256" => digest(payload), "bytes" => byte_size(payload)} end)
      }

      if Enum.all?(files, fn {_, data} -> byte_size(data) <= @limit end) and Enum.sum(Enum.map(files, fn {_, data} -> byte_size(data) end)) <= 32 * 1024 * 1024,
        do: {:ok, %{snapshot: snapshot, events: events, files: files, manifest: manifest}},
        else: {:error, :evidence_limit_exceeded}
    else
      _ -> {:error, :missing_evidence_run}
    end
  end

  defp export_result(:ok, state, id) do
    with {:ok, data} <- bundle(state, id), {:ok, _revision} <- export(state, data), do: :ok
  end

  defp export_result(_result, _state, _id), do: {:error, :evidence_write_failed}

  defp export(state, data) do
    manifest = Jason.encode!(data.manifest)
    revision = digest(manifest)
    directory = Path.join([state.root, "bundles", revision])

    with :ok <- directory(directory),
         :ok <- write_files(directory, data.files),
         :ok <- remove_orphaned_temporaries(directory),
         :ok <- immutable(Path.join(directory, "manifest.json"), manifest),
         do: {:ok, revision}
  end

  defp remove_orphaned_temporaries(directory) do
    with {:ok, names} <- File.ls(directory) do
      names
      |> Enum.filter(&Regex.match?(~r/^(?:manifest\.json|snapshot\.json|events\.jsonl|transcript\.txt|artifact-[a-f0-9]{64}\.txt)\.[a-f0-9]{16}$/, &1))
      |> Enum.reduce_while(:ok, fn name, :ok ->
        path = Path.join(directory, name)

        case File.lstat(path) do
          {:ok, %{type: :regular, links: 1}} ->
            case File.rm(path) do
              :ok -> {:cont, :ok}
              error -> {:halt, error}
            end

          _ ->
            {:halt, {:error, :unsafe_evidence_temporary}}
        end
      end)
    end
  end

  defp receipt(state, revision, manifest) do
    path = Path.join([state.root, "receipts", revision <> ".json"])

    with {:ok, ^path} <- PathSafety.canonicalize(path),
         {:ok, %{type: :regular, uid: uid, size: size, links: 1, mode: mode}} <- File.lstat(path),
         true <- uid == state.receipt_uid and size <= 65_536 and Bitwise.band(mode, 0o022) == 0,
         {:ok, payload} <- File.read(path),
         {:ok, receipt} <- Jason.decode(payload),
         true <- receipt["revision"] == revision and receipt["run_id"] == manifest["run_id"] do
      if receipt["status"] == "verified" and receipt["manifest_sha256"] == revision and receipt["artifacts"] == manifest["artifacts"], do: receipt, else: %{"status" => "failed"}
    else
      _ -> %{"status" => "pending"}
    end
  end

  defp publication_bundle(state, snapshot, attempt_id, %{"id" => validation_id, "manifest_sha256" => manifest_sha256}) do
    with true <- is_binary(attempt_id) and is_binary(validation_id) and is_binary(manifest_sha256),
         {:ok, revisions} <- File.ls(Path.join(state.root, "bundles")) do
      revisions
      |> Enum.filter(&Regex.match?(~r/^[a-f0-9]{64}$/, &1))
      |> Enum.sort()
      |> Enum.find_value({:error, :validation_not_permanently_archived}, fn revision ->
        case verified_publication_bundle(state, revision, snapshot, attempt_id, validation_id, manifest_sha256) do
          {:ok, _} = found -> found
          _ -> nil
        end
      end)
    else
      _ -> {:error, :validation_not_permanently_archived}
    end
  rescue
    _ -> {:error, :validation_not_permanently_archived}
  end

  defp publication_bundle(_state, _snapshot, _attempt_id, _receipt), do: {:error, :validation_not_permanently_archived}

  defp verified_publication_bundle(state, revision, expected, attempt_id, validation_id, manifest_sha256) do
    directory = Path.join([state.root, "bundles", revision])

    with {:ok, ^directory} <- PathSafety.canonicalize(directory),
         {:ok, bytes} <- publication_read(Path.join(directory, "manifest.json")),
         true <- digest(bytes) == revision,
         {:ok, manifest} <- Jason.decode(bytes),
         true <- manifest["run_id"] == expected["id"] and manifest["task_id"] == expected["task_id"],
         %{"status" => "verified"} = upload <- receipt(state, revision, manifest),
         artifacts when is_list(artifacts) <- manifest["artifacts"],
         {:ok, files} <- publication_files(directory, artifacts),
         true <- Enum.all?(["snapshot.json", "events.jsonl", "transcript.txt"], &Map.has_key?(files, &1)),
         {:ok, snapshot} <- Jason.decode(files["snapshot.json"] || ""),
         true <- Map.take(snapshot, @identity_keys) == Map.take(expected, @identity_keys),
         attempt when is_map(attempt) <- Enum.find(snapshot["attempts"] || [], &(&1["id"] == attempt_id)),
         true <- attempt["gate"] == "passed",
         true <- get_in(attempt, ["result", "evidence", "receipt"]) == %{"id" => validation_id, "manifest_sha256" => manifest_sha256},
         {:ok, events} <- publication_events(files["events.jsonl"] || ""),
         true <- Enum.any?(events, &(&1["kind"] == "validation_imported" and &1["attempt_id"] == attempt_id and &1["manifest_sha256"] == manifest_sha256)),
         imported when is_map(imported) <- Enum.find(events, &(&1["kind"] == "artifact" and &1["attempt_id"] == attempt_id and &1["label"] == "validation/" <> attempt_id <> "/manifest.json")),
         true <- imported["source_sha256"] == manifest_sha256 and is_binary(files[imported["name"]]) do
      {:ok, %{revision: revision, archive: upload, validation_attempt_id: attempt_id, manifest_sha256: manifest_sha256, validation_manifest: imported["name"]}}
    else
      _ -> {:error, :validation_not_permanently_archived}
    end
  rescue
    _ -> {:error, :validation_not_permanently_archived}
  end

  defp publication_files(directory, artifacts) do
    Enum.reduce_while(artifacts, {:ok, %{}}, fn artifact, {:ok, files} ->
      name = artifact["name"]

      with true <- is_binary(name) and (name in ["snapshot.json", "events.jsonl", "transcript.txt"] or Regex.match?(~r/^artifact-[a-f0-9]{64}\.txt$/, name)),
           false <- Map.has_key?(files, name),
           {:ok, bytes} <- publication_read(Path.join(directory, name)),
           true <- byte_size(bytes) == artifact["bytes"] and digest(bytes) == artifact["sha256"] do
        {:cont, {:ok, Map.put(files, name, bytes)}}
      else
        _ -> {:halt, {:error, :invalid_archived_artifact}}
      end
    end)
  end

  defp publication_read(path) do
    with {:ok, ^path} <- PathSafety.canonicalize(path),
         {:ok, %{type: :regular, links: 1, size: size, mode: mode}} <- File.lstat(path),
         true <- size <= @limit and Bitwise.band(mode, 0o022) == 0,
         {:ok, bytes} <- bounded_read(path),
         do: {:ok, bytes},
         else: (_ -> {:error, :unsafe_archived_artifact})
  end

  defp publication_events(bytes) do
    Enum.reduce_while(String.split(bytes, "\n", trim: true), {:ok, []}, fn line, {:ok, events} ->
      case Jason.decode(line) do
        {:ok, event} when is_map(event) -> {:cont, {:ok, [event | events]}}
        _ -> {:halt, {:error, :invalid_archived_events}}
      end
    end)
  end

  defp write_files(directory, files) do
    Enum.reduce_while(files, :ok, fn {name, data}, :ok ->
      case immutable(Path.join(directory, name), data) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp immutable(path, data) do
    case File.lstat(path) do
      {:ok, %{type: :regular, links: 1, size: size}} when size <= @limit ->
        if File.read(path) == {:ok, data}, do: :ok, else: {:error, :immutable_evidence_conflict}

      {:error, :enoent} ->
        temporary = path <> "." <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

        with {:ok, file} <- File.open(temporary, [:write, :binary, :exclusive]), :ok <- File.chmod(temporary, 0o600) do
          try do
            with :ok <- IO.binwrite(file, data), :ok <- :file.sync(file), do: File.rename(temporary, path)
          after
            File.close(file)
            File.rm(temporary)
          end
        end

      _ ->
        {:error, :unsafe_evidence_file}
    end
  end

  defp bounded_read(path) do
    with {:ok, ^path} <- PathSafety.canonicalize(path),
         {:ok, %{type: :regular, links: 1, size: size}} <- File.lstat(path),
         true <- size <= @limit,
         {:ok, payload} <- File.read(path),
         true <- byte_size(payload) <= @limit,
         do: {:ok, payload}
  end

  defp directory(path) do
    with {:ok, ^path} <- PathSafety.canonicalize(path), :ok <- File.mkdir_p(path), {:ok, %{type: :directory}} <- File.lstat(path), :ok <- File.chmod(path, 0o700), do: :ok
  end

  defp regular_or_missing(path) do
    case File.lstat(path) do
      {:error, :enoent} -> :ok
      {:ok, %{type: :regular, links: 1}} -> :ok
      _ -> {:error, :unsafe_evidence_database}
    end
  end

  defp add_event(db, id, event_id, event) do
    payload = Jason.encode!(event)

    case event_status(db, id, event_id, payload) do
      {:ok, :duplicate} -> :ok
      {:ok, :new} -> execute(db, "INSERT INTO evidence_events(run_id,event_id,payload) VALUES (?1,?2,?3)", [id, event_id, payload])
      _ -> {:error, :evidence_event_conflict}
    end
  end

  defp event_status(db, id, event_id, payload) do
    if is_binary(event_id) and byte_size(event_id) in 1..1_024 and byte_size(payload) <= @limit do
      case query(db, "SELECT payload FROM evidence_events WHERE run_id=?1 AND event_id=?2", [id, event_id]) do
        {:ok, [[^payload]]} -> {:ok, :duplicate}
        {:ok, []} -> {:ok, :new}
        _ -> {:error, :evidence_event_conflict}
      end
    else
      {:error, :invalid_evidence_event}
    end
  end

  defp pinned_identity(db, id, incoming, event) do
    case query(db, "SELECT snapshot FROM evidence_runs WHERE run_id=?1", [id]) do
      {:ok, []} ->
        :ok

      {:ok, [[payload]]} ->
        stored = Jason.decode!(payload)
        previous = Map.take(stored, @identity_keys)
        current = Map.take(incoming, @identity_keys)

        reassigned =
          event["kind"] == "agent_reassigned" and
            event["reassignment"] == %{"previous" => previous, "current" => current} and
            Map.take(previous, ~w(task_id policy)) == Map.take(current, ~w(task_id policy))

        if previous == current or reassigned, do: :ok, else: {:error, :changed_evidence_run_identity}

      _ ->
        {:error, :evidence_read_failed}
    end
  end

  defp transact(db, fun) do
    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE") do
      case fun.() do
        :ok ->
          Sqlite3.execute(db, "COMMIT")

        _ ->
          Sqlite3.execute(db, "ROLLBACK")
          {:error, :evidence_write_failed}
      end
    end
  end

  defp execute(db, sql, params) do
    with {:ok, statement} <- Sqlite3.prepare(db, sql) do
      try do
        with :ok <- Sqlite3.bind(statement, params), :done <- Sqlite3.step(db, statement), do: :ok
      after
        Sqlite3.release(db, statement)
      end
    end
  end

  defp query(db, sql, params) do
    with {:ok, statement} <- Sqlite3.prepare(db, sql) do
      try do
        with :ok <- Sqlite3.bind(statement, params), do: Sqlite3.fetch_all(db, statement)
      after
        Sqlite3.release(db, statement)
      end
    end
  end

  defp call(server, request) do
    if server == __MODULE__ and Config.evidence() == :disabled do
      {:error, :evidence_disabled}
    else
      GenServer.call(server, request, 30_000)
    end
  catch
    :exit, _ -> {:error, :evidence_unavailable}
  end

  defp valid_allowlist?(list), do: is_list(list) and Enum.all?(list, &(is_binary(&1) and byte_size(&1) in 1..254))
  defp valid_run?(id), do: is_binary(id) and Regex.match?(~r/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/, id)
  defp valid_task?(id), do: is_binary(id) and String.valid?(id) and byte_size(id) in 1..512 and not Regex.match?(~r/\p{Cc}/u, id)
  defp digest(data), do: :crypto.hash(:sha256, data) |> Base.encode16(case: :lower)
end
