defmodule SymphonyElixir.EvidenceOutboxTest do
  use SymphonyElixir.TestSupport

  alias Exqlite.Sqlite3
  alias SymphonyElixir.{WorkstreamRun, WorkstreamStore}

  setup do
    root = Path.join(System.tmp_dir!(), "evidence-outbox-#{System.unique_integer([:positive])}")
    path = Path.join(root, "state.sqlite3")
    File.mkdir_p!(root)

    workflow_path = Workflow.workflow_file_path()

    on_exit(fn ->
      write_workflow_file!(workflow_path)
      File.rm_rf!(root)
    end)

    %{root: root, path: path}
  end

  test "disabled evidence preserves commits without creating outbox rows", context do
    assert :disabled = Config.evidence()
    store = start_store(context.path)

    run = run("outbox-disabled", "linear/task-disabled")
    assert :ok = WorkstreamStore.commit(store, run, "disabled-event", %{kind: :queued})
    assert {:ok, ^run} = WorkstreamStore.fetch(store, run.id)
    assert {:ok, []} = WorkstreamStore.pending_evidence(store)
    assert :ok = GenServer.stop(store)

    assert [[3]] = database_rows(context.path, "PRAGMA user_version")
    assert [[0]] = database_rows(context.path, "SELECT count(*) FROM evidence_outbox")
  end

  test "enabled evidence is redacted, atomic, ordered, deduplicated, and restartable", context do
    enable_evidence!(context.root)
    System.put_env("FACTORY_OUTBOX_SECRET", "fixture-outbox-secret")
    on_exit(fn -> System.delete_env("FACTORY_OUTBOX_SECRET") end)

    store = start_store(context.path)
    first_run = run("outbox-first", "linear/11111111-1111-4111-8111-111111111111")
    first_event = %{kind: :queued, password: "fixture-password", text: "fixture-outbox-secret"}
    second_run = run("outbox-second", "linear/22222222-2222-4222-8222-222222222222")

    assert :ok = WorkstreamStore.commit(store, first_run, "first-event", first_event)
    assert :ok = WorkstreamStore.commit(store, second_run, "second-event", %{kind: :started})

    first_id = first_run.id

    assert {:duplicate, ^first_id} =
             WorkstreamStore.commit(store, run("outbox-ignored", first_run.task_id), "first-event", %{kind: :different})

    assert {:ok, [first, second]} = WorkstreamStore.pending_evidence(store)
    assert first.event_id == "first-event"
    assert second.event_id == "second-event"
    refute Map.has_key?(first.snapshot, "workspace")
    refute Map.has_key?(first.snapshot, "definition")
    refute inspect(first.event) =~ "fixture-password"
    refute inspect(first.event) =~ "fixture-outbox-secret"
    assert first.event["password"] == "[REDACTED]"
    assert first.snapshot["id"] == first_run.id

    assert :ok = GenServer.stop(store)

    assert [["first-event", "outbox-first"]] =
             database_rows(context.path, "SELECT event_id, run_id FROM events WHERE event_id = 'first-event'")

    assert [[2]] = database_rows(context.path, "SELECT count(*) FROM evidence_outbox")
    assert [[snapshot_blob, event_blob]] = database_rows(context.path, "SELECT snapshot, event FROM evidence_outbox WHERE event_id = 'first-event'")
    assert :nomatch = :binary.match(snapshot_blob, "fixture-outbox-secret")
    assert :nomatch = :binary.match(event_blob, "fixture-outbox-secret")
    assert :nomatch = :binary.match(event_blob, "fixture-password")

    store = start_store(context.path)
    assert {:ok, [^first, ^second]} = WorkstreamStore.pending_evidence(store)

    assert :ok = WorkstreamStore.acknowledge_evidence(store, first.event_id)
    assert {:ok, [^second]} = WorkstreamStore.pending_evidence(store)
    assert :ok = GenServer.stop(store)

    store = start_store(context.path)
    assert {:ok, [^second]} = WorkstreamStore.pending_evidence(store)
    assert :ok = GenServer.stop(store)
  end

  test "only the store owner can inspect or acknowledge pending evidence", context do
    enable_evidence!(context.root)
    store = start_store(context.path)
    assert :ok = WorkstreamStore.commit(store, run("outbox-owner", "linear/owner"), "owner-event", %{kind: :queued})

    caller = self()

    spawn(fn ->
      send(caller, {:pending, WorkstreamStore.pending_evidence(store)})
      send(caller, {:acknowledge, WorkstreamStore.acknowledge_evidence(store, "owner-event")})
    end)

    assert_receive {:pending, {:error, :not_owner}}, 2_000
    assert_receive {:acknowledge, {:error, :not_owner}}, 2_000
    assert {:ok, [%{event_id: "owner-event"}]} = WorkstreamStore.pending_evidence(store)
    assert :ok = GenServer.stop(store)
  end

  test "oversized evidence rolls back the run, event, and outbox together", context do
    enable_evidence!(context.root)
    store = start_store(context.path)
    run = run("outbox-too-large", "linear/too-large")
    event = %{kind: :large, text: String.duplicate("x", 10 * 1024 * 1024 + 1)}

    assert {:error, :invalid_evidence_payload} = WorkstreamStore.commit(store, run, "large-event", event)
    assert :not_found = WorkstreamStore.fetch(store, run.id)
    assert :not_found = WorkstreamStore.event(store, "large-event")
    assert {:ok, []} = WorkstreamStore.pending_evidence(store)
    assert :ok = GenServer.stop(store)

    assert [[0]] = database_rows(context.path, "SELECT count(*) FROM events")
    assert [[0]] = database_rows(context.path, "SELECT count(*) FROM evidence_outbox")
  end

  defp start_store(path) do
    assert {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    on_exit(fn -> if Process.alive?(store), do: GenServer.stop(store) end)
    store
  end

  defp enable_evidence!(root) do
    path = Workflow.workflow_file_path()
    content = File.read!(path)
    [front_matter, prompt] = String.split(content, "\n---\n", parts: 2)

    evidence_config =
      [
        "evidence:",
        "  root: #{Jason.encode!(Path.join(root, "private-evidence"))}",
        "  base_url: \"https://evidence.example.com\"",
        "  iap_audience: \"/projects/123456789/global/backendServices/123456789\"",
        "  allowed_domains: [\"rig.ai\"]"
      ]
      |> Enum.join("\n")

    File.write!(path, front_matter <> "\n" <> evidence_config <> "\n---\n" <> prompt)
    assert :ok = WorkflowStore.force_reload()
    assert {:ok, _config} = Config.evidence()
  end

  defp run(run_id, task_id) do
    definition = %{
      inputs: [],
      input_values: %{},
      entry: "implement",
      agents: %{},
      stages: %{},
      definitions: %{"workstream.yaml" => %{sha256: String.duplicate("a", 64), text: "redacted fixture instructions"}}
    }

    WorkstreamRun.new(task_id, definition, "/disposable-fixture", [branch: "cycle/evidence"], %{})
    |> Map.put(:id, run_id)
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
