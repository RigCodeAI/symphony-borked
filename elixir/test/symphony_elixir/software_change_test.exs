defmodule SymphonyElixir.SoftwareChangeTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{Workstream, WorkstreamRun, WorkstreamRunner}

  test "the shipped entry pins Default Cloud and pauses the local candidate without publication" do
    root = Path.join(System.tmp_dir!(), "software-change-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "workstreams"))
    source = Path.expand("../../../factory/workstreams/software-change.yaml", __DIR__)
    path = Path.join(root, "workstreams/software-change.yaml")
    File.cp!(source, path)
    on_exit(fn -> File.rm_rf!(root) end)

    # Pin all shipped definitions and their shared resources. The controlled
    # stage result below is not a production qualification receipt.
    factory_root = source |> Path.dirname() |> Path.dirname()
    Enum.each(~w(agents instructions skills), &File.cp_r!(Path.join(factory_root, &1), Path.join(root, &1)))

    assert {:ok, definition} = Workstream.load(path, %{"task" => "DEV pilot task"})
    assert definition.stages[definition.entry].agent == "default-cloud"

    assert definition.definitions[path].sha256 ==
             :crypto.hash(:sha256, File.read!(source)) |> Base.encode16(case: :lower)

    workspace = Path.join(root, "clone")
    File.mkdir_p!(workspace)
    {_, 0} = System.cmd("git", ["init", "--quiet", workspace])
    File.write!(Path.join(workspace, "candidate"), "local candidate")
    {_, 0} = System.cmd("git", ["add", "candidate"], cd: workspace)
    {_, 0} = System.cmd("git", ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "candidate"], cd: workspace)
    assert {:ok, high, ^workspace} = WorkstreamRunner.prepare(path, %{"task" => "same workstream, high effort"}, workspace: workspace, workspace_root: root, cloud_agent_id: "high-effort-cloud")
    assert high.name == definition.name
    assert high.stages[high.entry].agent == "high-effort-cloud"
    assert high.agent_selection.requested.reasoning_effort == "high"
    assert high.agent_selection.resources == Enum.map(definition.agents["default-cloud"].instructions ++ definition.agents["default-cloud"].skills, &Map.take(&1, [:path, :sha256]))
    assert {:error, :unknown_cloud_agent_definition} = WorkstreamRunner.prepare(path, %{"task" => "reviewer forbidden"}, workspace: workspace, workspace_root: root, cloud_agent_id: "default-review")
    run = WorkstreamRun.new("linear/pilot", definition, workspace, workspace_root: root)
    File.write!(path, "changed after enqueue")
    run = run |> WorkstreamRun.start_stage() |> WorkstreamRun.finish_stage({:ok, %{commit: "local-candidate"}}) |> WorkstreamRun.start_stage()
    assert run.stage_id == "inspect-candidate"
    assert {:ok, %{exit_status: 0} = checked} = WorkstreamRunner.execute_stage(run.stage_id, definition, run, workspace_root: root)
    File.write!(Path.join(workspace, "untracked"), "dirty candidate")
    assert {:ok, %{exit_status: status}} = WorkstreamRunner.execute_stage(run.stage_id, definition, run, workspace_root: root)
    assert status != 0
    run = run |> WorkstreamRun.finish_stage({:ok, checked}) |> WorkstreamRun.start_stage()
    assert run.status == :waiting_for_answer
    assert run.stage_id == "candidate-ready"
    assert run.pending_wait.prompt =~ "Publication is disabled"
    assert run.outputs["candidate"] == %{"commit" => "local-candidate"}
    assert Enum.map(run.attempts, & &1.stage) == ["implement", "inspect-candidate", "candidate-ready"]
  end
end
