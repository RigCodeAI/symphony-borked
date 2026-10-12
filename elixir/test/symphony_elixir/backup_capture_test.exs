defmodule SymphonyElixir.BackupCaptureTest do
  use ExUnit.Case, async: true

  alias Exqlite.Sqlite3
  alias SymphonyElixir.BackupCapture

  setup do
    base = if File.dir?("/private/tmp"), do: "/private/tmp", else: Path.expand(System.tmp_dir!())

    parent =
      Path.join(base, "backup-capture-test-#{System.unique_integer([:positive, :monotonic])}")

    root = Path.join(parent, "captures")
    workspace_root = Path.join(parent, "workspaces")

    File.mkdir_p!(root)
    File.mkdir_p!(workspace_root)
    :ok = File.chmod(parent, 0o700)
    :ok = File.chmod(root, 0o700)
    :ok = File.chmod(workspace_root, 0o700)

    on_exit(fn -> File.rm_rf!(parent) end)

    %{parent: parent, root: root, workspace_root: workspace_root}
  end

  test "rejects capture roots that overlap workspaces in either direction", context do
    assert {:error, :capture_root_overlaps_workspace} =
             BackupCapture.prepare(context.root, "capture-a", [context.parent])

    nested_workspace = Path.join(context.root, "issue-workspaces")

    assert {:error, :capture_root_overlaps_workspace} =
             BackupCapture.prepare(context.root, "capture-b", [nested_workspace])
  end

  test "rejects symlink roots and unsafe capture IDs", context do
    symlink = Path.join(context.parent, "capture-root-link")
    :ok = File.ln_s(context.root, symlink)

    assert {:error, :invalid_capture_root} =
             BackupCapture.prepare(symlink, "capture-a", [context.workspace_root])

    assert {:error, :invalid_capture_id} =
             BackupCapture.prepare(context.root, "../capture-a", [context.workspace_root])

    ambiguous_root = Path.join(context.parent, "captures/../captures")

    assert {:error, :invalid_capture_root} =
             BackupCapture.prepare(ambiguous_root, "capture-b", [context.workspace_root])

    ambiguous_target = Path.join(context.parent, "captures/../captures/state.sqlite")
    assert {:error, :unsafe_snapshot_target} = BackupCapture.snapshot(nil, ambiguous_target)
  end

  test "does not reuse an incomplete capture directory", context do
    assert {:ok, plan} =
             BackupCapture.prepare(context.root, "capture-partial", [context.workspace_root])

    :ok = File.write(plan.state_path, "partial database")
    :ok = File.chmod(plan.state_path, 0o600)

    assert {:error, :incomplete_capture} =
             BackupCapture.prepare(context.root, "capture-partial", [context.workspace_root])
  end

  test "snapshots live owner connections, verifies hashes, and returns the same completion", context do
    assert {:ok, plan} =
             BackupCapture.prepare(context.root, "capture-complete", [context.workspace_root])

    sources = Path.join(context.parent, "sources")
    File.mkdir_p!(sources)
    :ok = File.chmod(sources, 0o700)

    {:ok, state_db} = Sqlite3.open(Path.join(sources, "state-source.sqlite"))
    {:ok, evidence_db} = Sqlite3.open(Path.join(sources, "evidence-source.sqlite"))

    try do
      :ok =
        Sqlite3.execute(
          state_db,
          "CREATE TABLE runs (id TEXT PRIMARY KEY); INSERT INTO runs VALUES ('run-1'); PRAGMA user_version = 3"
        )

      :ok =
        Sqlite3.execute(
          evidence_db,
          "PRAGMA journal_mode = WAL; CREATE TABLE evidence_events (id TEXT PRIMARY KEY); INSERT INTO evidence_events VALUES ('event-1'); PRAGMA user_version = 1"
        )

      assert {:ok, %{bytes: state_bytes, sha256: state_sha, schema: %{"user_version" => 3}}} =
               BackupCapture.snapshot(state_db, plan.state_path)

      assert state_bytes > 0
      assert byte_size(state_sha) == 64

      assert {:ok, %{bytes: evidence_bytes, sha256: evidence_sha, schema: %{"user_version" => 1}}} =
               BackupCapture.snapshot(evidence_db, plan.evidence_path)

      assert evidence_bytes > 0
      assert byte_size(evidence_sha) == 64

      metadata = %{
        service_revision: "test-revision",
        run_ids: ["run-1"],
        operation_ids: [],
        delivery_ids: [],
        pending_outbox: 0,
        evidence_event_watermark: 1,
        evidence_bundle_revisions: []
      }

      assert {:ok, manifest} = BackupCapture.complete(plan, metadata)
      assert manifest["kind"] == "local_owner_capture"
      assert manifest["restore_ready"] == false
      assert manifest["metadata"]["pending_outbox"] == 0
      assert manifest["durability"]["files_synced"] == true
      assert is_boolean(manifest["durability"]["durable_directory_synced"])

      assert {:completed, ^manifest} =
               BackupCapture.prepare(context.root, "capture-complete", [context.workspace_root])

      assert {:ok, ^manifest} =
               BackupCapture.verify(context.root, "capture-complete", [context.workspace_root])

      assert Bitwise.band(File.stat!(plan.state_path).mode, 0o077) == 0
      assert Bitwise.band(File.stat!(plan.evidence_path).mode, 0o077) == 0
    after
      :ok = Sqlite3.close(state_db)
      :ok = Sqlite3.close(evidence_db)
    end
  end

  test "detects changed snapshot bytes and hard-linked database files", context do
    assert {:ok, plan} =
             BackupCapture.prepare(context.root, "capture-integrity", [context.workspace_root])

    create_completed_capture(plan)

    hard_link = Path.join(context.parent, "state-hard-link.sqlite")
    :ok = File.ln(plan.state_path, hard_link)

    assert {:error, :capture_integrity_failed} =
             BackupCapture.verify(context.root, "capture-integrity", [context.workspace_root])

    :ok = File.rm(hard_link)
    {:ok, altered_db} = Sqlite3.open(plan.state_path)
    :ok = Sqlite3.execute(altered_db, "INSERT INTO sample VALUES ('changed')")
    :ok = Sqlite3.close(altered_db)

    assert {:error, :capture_integrity_failed} =
             BackupCapture.verify(context.root, "capture-integrity", [context.workspace_root])
  end

  test "rejects duplicate manifest keys", context do
    assert {:ok, plan} =
             BackupCapture.prepare(context.root, "capture-duplicate-key", [context.workspace_root])

    create_completed_capture(plan)
    {:ok, original} = File.read(plan.manifest_path)
    duplicate_version = String.replace_prefix(original, "{", "{\"version\":1,")
    :ok = File.write(plan.manifest_path, duplicate_version)
    :ok = File.chmod(plan.manifest_path, 0o600)

    assert {:error, :invalid_manifest} =
             BackupCapture.verify(context.root, "capture-duplicate-key", [context.workspace_root])
  end

  defp create_completed_capture(plan) do
    {:ok, source} = Sqlite3.open(":memory:")

    try do
      :ok = Sqlite3.execute(source, "CREATE TABLE sample (value TEXT); PRAGMA user_version = 1")
      assert {:ok, _state} = BackupCapture.snapshot(source, plan.state_path)
      assert {:ok, _evidence} = BackupCapture.snapshot(source, plan.evidence_path)
      assert {:ok, _manifest} = BackupCapture.complete(plan, %{})
    after
      :ok = Sqlite3.close(source)
    end
  end
end
