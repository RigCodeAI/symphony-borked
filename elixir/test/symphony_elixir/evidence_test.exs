defmodule SymphonyElixir.EvidenceTest do
  use ExUnit.Case, async: false
  alias SymphonyElixir.{Evidence, WorkstreamRun}
  alias SymphonyElixirWeb.EvidenceController

  setup do
    root = Path.join(System.tmp_dir!(), "evidence-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    uid = File.stat!(root).uid
    config = %{"root" => root, "base_url" => "https://factory.rig.ai", "iap_audience" => "/projects/292978199748/global/backendServices/123", "allowed_domains" => ["rig.ai"]}
    opts = [config: {:ok, config}, name: nil, receipt_uid: uid]
    assert {:ok, pid} = Evidence.start_link(opts)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root, store: pid, opts: opts}
  end

  test "redacts credentials and excludes hidden reasoning before database and bundle persistence", c do
    System.put_env("FACTORY_TEST_SECRET", "fixture-secret-12345")
    on_exit(fn -> System.delete_env("FACTORY_TEST_SECRET") end)
    attempt_id = "run-1/implement/1"

    run =
      run("run-1", "linear/task-1")
      |> Map.merge(%{
        stage_id: "implement",
        current_attempt_id: attempt_id,
        attempts: [
          %{id: attempt_id, stage: "implement", ordinal: 1, type: :agent, status: :completed, gate: :passed, gate_detail: "checks passed", result: %{status: :ok}}
        ]
      })

    assert :ok = Evidence.snapshot(run, "created", %{password: "unpatterned-password", client_secret: "unpatterned-client-credential"}, c.store)

    event = %{
      payload: %{
        "method" => "item/completed",
        "params" => %{"item" => %{"id" => "msg-1", "type" => "agentMessage", "text" => "fixture-secret-12345 ghp_fixturetoken password=fixture-pass <script>bad</script>"}}
      }
    }

    assert :ok = Evidence.capture(run, event, c.store)
    assert :ok = Evidence.capture(run, event, c.store)
    assert :ok = Evidence.capture(run, put_in(event.payload["params"]["item"], %{"type" => "reasoning", "text" => "hidden-model-thought"}), c.store)
    assert :ok = Evidence.capture(run, put_in(event.payload["params"]["item"]["phase"], "analysis"), c.store)

    user =
      put_in(event.payload["params"]["item"], %{
        "type" => "userMessage",
        "content" => [%{"type" => "text", "text" => "Visible question; client_secret=fixture-client"}, %{"type" => "image", "url" => "data:image/png;base64,private-image-fixture"}]
      })

    assert :ok = Evidence.capture(run, user, c.store)
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id), c.store)
    assert length(view.events) == 3
    json = Jason.encode!(view)

    for secret <- [
          "fixture-secret-12345",
          "ghp_fixturetoken",
          "fixture-pass",
          "unpatterned-password",
          "unpatterned-client-credential",
          "fixture-client",
          "private-image-fixture",
          "hidden-model-thought"
        ],
        do: refute(String.contains?(json, secret))

    assert json =~ "Visible question"
    html = EvidenceController.render_task([view])
    assert html =~ "<h3>Conversation</h3>"
    assert html =~ "<strong>Agent</strong> · Stage implement · Attempt 1"
    assert html =~ "<strong>Task input</strong> · Stage implement · Attempt 1"
    assert html =~ "<details><summary><strong>Task input</strong>"
    assert html =~ "Latest gate/check:</strong> Passed"
    assert Regex.match?(~r/<blockquote[^>]*>.*&lt;script&gt;bad&lt;\/script&gt;.*<\/blockquote>/s, html)
    assert html =~ "Visible question; [REDACTED]"
    assert html =~ "<details><summary>Run details, waits and policy</summary>"
    assert html =~ "<details><summary>Observable event audit details (3 events)</summary>"
    {conversation_position, _} = :binary.match(html, "<h3>Conversation</h3>")
    {details_position, _} = :binary.match(html, "<details><summary>Run details, waits and policy</summary>")
    assert conversation_position < details_position
    assert html =~ "&lt;script&gt;bad&lt;/script&gt;"
    refute html =~ "<script>"
    assert :ok = GenServer.stop(c.store)

    for path <- Path.wildcard(Path.join(c.root, "**/*")), File.regular?(path) do
      data = File.read!(path)
      refute data =~ "fixture-secret-12345"
      refute data =~ "hidden-model-thought"
      refute data =~ "unpatterned-password"
    end
  end

  test "conversation omits unknown and non-message events while retaining them in audit details", c do
    run = run("audit-run", "audit-task")
    assert :ok = Evidence.snapshot(run, "created", %{kind: :queued}, c.store)
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id), c.store)

    audit_events = [
      %{"kind" => "item/completed", "item" => %{"type" => "commandExecution", "command" => "check command"}},
      %{"kind" => "future_event", "message" => "future event payload"}
    ]

    html = EvidenceController.render_task([%{view | events: audit_events}])
    [_, after_conversation_heading] = String.split(html, "<h3>Conversation</h3>", parts: 2)

    [conversation, _audit_details] =
      String.split(after_conversation_heading, "<details><summary>Run details, waits and policy</summary>", parts: 2)

    assert conversation =~ "No agent messages or task input were recorded."
    refute conversation =~ "check command"
    refute conversation =~ "future event payload"
    assert html =~ "commandExecution"
    assert html =~ "future event payload"
  end

  test "restart and workspace deletion retain transcript; latest exact upload receipt gates cleanup", c do
    workspace = Path.join(c.root, "disposable-workspace")
    File.mkdir!(workspace)
    run = %{run("run-2", "task-2") | workspace: workspace, status: :completed}
    assert :ok = Evidence.snapshot(run, "completed", %{kind: :stage_completed}, c.store)
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id), c.store)
    refute Evidence.cleanup_ready?(run.id, c.store)
    write_receipt(c, view, "failed")
    assert {:ok, [%{archive: %{"status" => "failed"}}]} = Evidence.task(Evidence.task_key(run.task_id), c.store)
    refute Evidence.cleanup_ready?(run.id, c.store)
    write_receipt(c, view, "verified")
    assert Evidence.cleanup_ready?(run.id, c.store)
    orphan = Path.join([c.root, "bundles", view.revision, "transcript.txt.0123456789abcdef"])
    File.write!(orphan, "redacted partial write from an interrupted export")
    File.rm_rf!(workspace)
    assert :ok = GenServer.stop(c.store)
    assert {:ok, restarted} = Evidence.start_link(c.opts)
    assert {:ok, [restored]} = Evidence.task(Evidence.task_key(run.task_id), restarted)
    assert restored.revision == view.revision
    refute File.exists?(orphan)
    assert restored.archive["status"] == "verified"
    assert :ok = Evidence.artifact(run.id, "test.log", "test success; secret=fixture-credential", restarted)
    refute Evidence.cleanup_ready?(run.id, restarted)
    assert {:ok, [latest]} = Evidence.task(Evidence.task_key(run.task_id), restarted)
    artifact = Enum.find(latest.artifacts, &String.starts_with?(&1["name"], "artifact-"))
    assert {:ok, data} = Evidence.read_artifact(Evidence.task_key(run.task_id), latest.revision, artifact["name"], restarted)
    assert data =~ "test success"
    refute data =~ "fixture-credential"
    assert {:error, _} = Evidence.read_artifact(Evidence.task_key("other-task"), latest.revision, artifact["name"], restarted)
    assert :ok = GenServer.stop(restarted)
  end

  test "retry, waiting gate, parent and child links keep original definition pins", c do
    parent = run("parent-run", "parent-task") |> Map.put(:child_run_ids, ["child-run"])
    child = run("child-run", "child-task") |> Map.put(:parent_run_id, parent.id)

    child = %{
      child
      | status: :waiting_for_answer,
        pending_wait: %{id: "wait-1", prompt: "Approve candidate?"},
        attempts: [
          %{id: "child-run/implement/1", stage: "implement", ordinal: 1, type: :agent, status: :failed, gate: :failed, gate_detail: "check failed", result: %{exit_status: 1}},
          %{id: "child-run/implement/2", stage: "implement", ordinal: 2, type: :agent, status: :completed, gate: :passed, gate_detail: "check passed", result: %{exit_status: 0}},
          %{id: "child-run/approval/3", stage: "approval", ordinal: 3, type: :human_wait, status: :waiting, gate: nil, gate_detail: nil, result: nil}
        ]
    }

    assert :ok = Evidence.snapshot(parent, "parent", %{kind: :queued}, c.store)
    assert :ok = Evidence.snapshot(child, "child", %{kind: :waiting}, c.store)
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(child.task_id), c.store)
    assert view.parent_key == Evidence.task_key(parent.task_id)
    assert {:ok, [parent_view]} = Evidence.task(Evidence.task_key(parent.task_id), c.store)
    assert parent_view.child_keys == [Evidence.task_key(child.task_id)]
    html = EvidenceController.render_task([view])
    assert html =~ "Stage implement · Attempt 1"
    assert html =~ "Stage implement · Attempt 2"
    assert html =~ "Stage approval · Attempt 3"
    assert html =~ "Latest gate/check:</strong> Passed"
    assert html =~ "original-definition"
    assert html =~ "Approve candidate?"
    changed = %{run("new-run", child.task_id) | definition: %{child.definition | definitions: %{"workstream.yaml" => %{sha256: "new-definition", text: "new instructions"}}}}
    assert :ok = Evidence.snapshot(changed, "new", %{kind: :queued}, c.store)
    assert {:ok, views} = Evidence.task(Evidence.task_key(child.task_id), c.store)
    assert Enum.find(views, &(&1.snapshot["id"] == child.id)).snapshot["definitions"]["workstream.yaml"] == "original-definition"
  end

  test "conflicting event identity, spool corruption and forged receipt fail closed", c do
    run = run("run-3", "task-3")
    assert :ok = Evidence.snapshot(run, "created", %{kind: :queued}, c.store)
    assert {:error, _} = Evidence.snapshot(run, "created", %{kind: :different}, c.store)
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id), c.store)
    write_receipt(c, view, "verified")
    path = Path.join([c.root, "receipts", view.revision <> ".json"])
    receipt = Jason.decode!(File.read!(path)) |> Map.put("artifacts", [])
    File.write!(path, Jason.encode!(receipt))
    refute Evidence.cleanup_ready?(run.id, c.store)
    snapshot = Path.join([c.root, "bundles", view.revision, "snapshot.json"])
    File.write!(snapshot, "corrupt")
    assert {:error, _} = Evidence.task(Evidence.task_key(run.task_id), c.store)
    assert {:error, _} = Evidence.read_artifact(Evidence.task_key(run.task_id), view.revision, "snapshot.json", c.store)
  end

  test "explicit durable reassignment retains old pins and rejects silent mutation", c do
    original = run("reassigned-run", "reassigned-task")
    assert :ok = Evidence.snapshot(original, "created", %{kind: :queued}, c.store)
    assert {:ok, [before]} = Evidence.task(Evidence.task_key(original.task_id), c.store)
    replacement = %{original | definition: %{original.definition | definitions: %{"workstream.yaml" => %{sha256: "replacement-definition", text: "replacement instructions"}}}}
    assert {:error, _} = Evidence.snapshot(replacement, "silent-change", %{kind: :queued}, c.store)
    assert {:ok, [unchanged]} = Evidence.task(Evidence.task_key(original.task_id), c.store)
    assert unchanged.revision == before.revision
    transition = %{kind: :agent_reassigned, reassignment: %{previous: Evidence.identity(original), current: Evidence.identity(replacement)}}
    assert :ok = Evidence.snapshot(replacement, "reassign-1", transition, c.store)
    assert :ok = Evidence.snapshot(replacement, "reassign-1", transition, c.store)
    assert {:ok, [after_view]} = Evidence.task(Evidence.task_key(original.task_id), c.store)
    assert length(after_view.events) == 2
    assert after_view.snapshot["definitions"]["workstream.yaml"] == "replacement-definition"
    assert hd(after_view.events)["snapshot"]["definitions"]["workstream.yaml"] == "original-definition"
    assert :ok = Evidence.snapshot(original, "created", %{kind: :queued}, c.store)
    assert {:ok, [replayed]} = Evidence.task(Evidence.task_key(original.task_id), c.store)
    assert replayed.revision == after_view.revision
    assert {:ok, old_json} = Evidence.read_artifact(Evidence.task_key(original.task_id), before.revision, "snapshot.json", c.store)
    assert old_json =~ "original-definition"
    refute old_json =~ "replacement-definition"
    assert :ok = GenServer.stop(c.store)
    assert {:ok, restarted} = Evidence.start_link(c.opts)
    assert {:ok, [restored]} = Evidence.task(Evidence.task_key(original.task_id), restarted)
    assert restored.events == after_view.events
    assert :ok = GenServer.stop(restarted)
  end

  test "symlinked storage is rejected and evidence config is explicit", c do
    assert {:ok, _} = Evidence.validate_config(elem(c.opts[:config], 1))
    assert {:error, _} = Evidence.validate_config(%{})
    assert :ok = GenServer.stop(c.store)
    link = c.root <> "-link"
    File.ln_s!(c.root, link)
    config = elem(c.opts[:config], 1) |> Map.put("root", link)
    Process.flag(:trap_exit, true)
    assert {:error, :evidence_startup_failed} = Evidence.start_link(config: {:ok, config}, name: nil)
    File.rm!(link)
    File.ln_s!(Path.join(c.root, "index.sqlite"), Path.join(c.root, "index.sqlite-wal"))
    assert {:error, :evidence_startup_failed} = Evidence.start_link(c.opts)
    File.rm!(Path.join(c.root, "index.sqlite-wal"))
  end

  defp write_receipt(c, view, status) do
    directory = Path.join(c.root, "receipts")
    File.mkdir_p!(directory)
    receipt = %{"version" => 1, "status" => status, "run_id" => view.snapshot["id"], "revision" => view.revision, "manifest_sha256" => view.revision, "artifacts" => view.artifacts}
    path = Path.join(directory, view.revision <> ".json")
    File.write!(path, Jason.encode!(receipt))
    File.chmod!(path, 0o640)
  end

  defp run(id, task_id) do
    definition = %{inputs: [], input_values: %{}, entry: "implement", agents: %{}, stages: %{}, definitions: %{"workstream.yaml" => %{sha256: "original-definition", text: "original instructions"}}}
    WorkstreamRun.new(task_id, definition, "/disposable-fixture", [branch: "cycle/fixture"], %{}) |> Map.put(:id, id)
  end
end
