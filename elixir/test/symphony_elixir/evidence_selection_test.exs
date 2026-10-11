defmodule SymphonyElixir.EvidenceSelectionTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{Evidence, Workstream, WorkstreamRun}
  alias SymphonyElixir.Linear.AgentMapping

  @root Path.expand("../../..", __DIR__)

  setup do
    root = Path.join(System.tmp_dir!(), "evidence-selection-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    config = %{
      "root" => root,
      "base_url" => "https://factory.rig.ai",
      "iap_audience" => "/projects/292978199748/global/backendServices/123",
      "allowed_domains" => ["rig.ai"]
    }

    opts = [config: {:ok, config}, name: nil, receipt_uid: File.stat!(root).uid]
    assert {:ok, store} = Evidence.start_link(opts)

    on_exit(fn ->
      if Process.alive?(store), do: GenServer.stop(store)
      File.rm_rf!(root)
    end)

    %{root: root, store: store, opts: opts}
  end

  test "records actual agent reassignment without changing old snapshot bundles", c do
    assert {:ok, definition} =
             Workstream.load(Path.join(@root, "factory/workstreams/software-change.yaml"), %{"task" => "selection test"})

    original =
      WorkstreamRun.new("linear/selection-task", definition, "/disposable/selection-workspace", [branch: "cycle/selection-test"], %{})

    assert Map.get(original.definition, :agent_selection) == nil
    assert :ok = Evidence.snapshot(original, "created", %{kind: :queued}, c.store)
    assert {:ok, [before]} = Evidence.task(Evidence.task_key(original.task_id), c.store)
    assert before.snapshot["stage_agents"]["implement"] == "default-cloud"
    assert before.snapshot["agent_selection"] == nil
    assert {:ok, before_manifest} = Evidence.read_artifact(Evidence.task_key(original.task_id), before.revision, "snapshot.json", c.store)

    assert {:ok, reassigned} = AgentMapping.reassign(original, "high-effort-cloud", "linear-transition-1")
    assert reassigned.definition.agent_selection.agent_id == "high-effort-cloud"
    assert reassigned.definition.definitions == original.definition.definitions
    assert reassigned.definition.agents == original.definition.agents
    assert reassigned.definition.stages["implement"].agent == "high-effort-cloud"

    assert {:error, _} = Evidence.snapshot(reassigned, "silent-reassignment", %{kind: :queued}, c.store)

    event = %{
      kind: :agent_reassigned,
      reassignment: %{previous: Evidence.identity(original), current: Evidence.identity(reassigned)}
    }

    assert :ok = Evidence.snapshot(reassigned, "reassign-1", event, c.store)
    assert {:ok, [after_view]} = Evidence.task(Evidence.task_key(original.task_id), c.store)
    assert after_view.snapshot["agent_selection"]["agent_id"] == "high-effort-cloud"
    assert after_view.snapshot["stage_agents"]["implement"] == "high-effort-cloud"

    [created_event, reassignment_event] = after_view.events
    assert created_event["snapshot"]["agent_selection"] == nil
    assert created_event["snapshot"]["stage_agents"]["implement"] == "default-cloud"
    assert reassignment_event["reassignment"]["previous"]["stage_agents"]["implement"] == "default-cloud"
    assert reassignment_event["reassignment"]["current"]["stage_agents"]["implement"] == "high-effort-cloud"

    assert {:ok, after_manifest} = Evidence.read_artifact(Evidence.task_key(original.task_id), before.revision, "snapshot.json", c.store)
    assert after_manifest == before_manifest
    assert Jason.decode!(after_manifest)["stage_agents"]["implement"] == "default-cloud"

    assert :ok = GenServer.stop(c.store)
    assert {:ok, restarted} = Evidence.start_link(c.opts)
    assert {:ok, [restored]} = Evidence.task(Evidence.task_key(original.task_id), restarted)
    assert restored.snapshot["agent_selection"]["agent_id"] == "high-effort-cloud"
    assert [restored_created, _restored_reassignment] = restored.events
    assert restored_created["snapshot"]["stage_agents"]["implement"] == "default-cloud"
    assert {:ok, restored_manifest} = Evidence.read_artifact(Evidence.task_key(original.task_id), before.revision, "snapshot.json", restarted)
    assert restored_manifest == before_manifest
    assert :ok = GenServer.stop(restarted)
  end
end
