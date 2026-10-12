defmodule SymphonyElixir.SoftwareChangeTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{Workstream, WorkstreamRun, WorkstreamRunner}

  test "the shipped entry requires trusted validation and publication before a human wait" do
    root = Path.join(System.tmp_dir!(), "software-change-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "workstreams"))
    source = Path.expand("../../../factory/workstreams/software-change.yaml", __DIR__)
    path = Path.join(root, "workstreams/software-change.yaml")
    File.cp!(source, path)
    on_exit(fn -> File.rm_rf!(root) end)

    # Pin all shipped definitions and their shared resources. The controlled
    # stage result below is not a production qualification receipt.
    factory_root = source |> Path.dirname() |> Path.dirname()
    Enum.each(~w(agents instructions skills policies), &File.cp_r!(Path.join(factory_root, &1), Path.join(root, &1)))

    assert {:ok, definition} = Workstream.load(path, %{"task" => "DEV pilot task"})
    assert definition.stages[definition.entry].agent == "default-cloud"

    assert definition.definitions[path].sha256 ==
             :crypto.hash(:sha256, File.read!(source)) |> Base.encode16(case: :lower)

    commit_command = definition.stages["local-commit"].gate.command
    commit_script = List.last(commit_command)

    assert Base.encode16(:crypto.hash(:sha256, commit_script), case: :lower) ==
             "9b7f2d4ea6e70c3ca71642c13ba0a1cb88b16d1a2b35a27cb5317c28777072c5"

    assert Base.encode16(:crypto.hash(:sha256, Jason.encode!(commit_command)), case: :lower) ==
             "aeae2623255e9c74ec80718023f5bda386d87d5943ca3dd2e171967b15483da8"

    workspace = Path.join(root, "clone")
    File.mkdir_p!(workspace)
    {_, 0} = System.cmd("git", ["init", "--quiet", workspace])
    File.write!(Path.join(workspace, "candidate"), "local candidate")
    {_, 0} = System.cmd("git", ["add", "candidate"], cd: workspace)

    {_, 0} =
      System.cmd(
        "git",
        ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "candidate"],
        cd: workspace
      )

    {base, 0} = System.cmd("git", ["rev-parse", "HEAD"], cd: workspace)

    opts = [
      workspace: workspace,
      workspace_root: root,
      validation_policy: Path.join(root, "policies/rig-local.yaml"),
      validation_archive: Path.join(root, "validation-archive"),
      validation_scratch: Path.join(root, "validation-scratch"),
      validation_base: String.trim(base),
      publisher: %{app_id: "1", installation_id: "2", private_key_path: Path.join(root, "publisher.pem")},
      validation_check: %{app_id: "3", installation_id: "4", private_key_path: Path.join(root, "validation.pem")}
    ]

    assert {:error, {:missing_validation_option, :validation_policy}} =
             WorkstreamRunner.prepare(
               path,
               %{"task" => "missing trusted setup"},
               workspace: workspace,
               workspace_root: root
             )

    assert {:ok, high, ^workspace} =
             WorkstreamRunner.prepare(
               path,
               %{"task" => "same workstream, high effort"},
               Keyword.put(opts, :cloud_agent_id, "high-effort-cloud")
             )

    assert high.name == definition.name
    assert high.stages[high.entry].agent == "high-effort-cloud"
    assert high.agent_selection.requested.reasoning_effort == "high"

    assert high.agent_selection.resources ==
             Enum.map(
               definition.agents["default-cloud"].instructions ++ definition.agents["default-cloud"].skills,
               &Map.take(&1, [:path, :sha256])
             )

    assert {:error, :unknown_cloud_agent_definition} =
             WorkstreamRunner.prepare(
               path,
               %{"task" => "reviewer forbidden"},
               workspace: workspace,
               workspace_root: root,
               cloud_agent_id: "default-review"
             )

    run = WorkstreamRun.new("linear/pilot", definition, workspace, workspace_root: root)
    File.write!(path, "changed after enqueue")
    candidate = %{thread_id: "controlled-contract-conversation"}
    run = run |> WorkstreamRun.start_stage() |> WorkstreamRun.finish_stage({:ok, candidate}) |> WorkstreamRun.start_stage()
    assert run.stage_id == "local-commit"
    assert run.phase == :validating

    File.write!(Path.join(workspace, "candidate"), "uncommitted local candidate")

    assert {:ok, %{exit_status: 0, output: commit_output} = committed} =
             WorkstreamRunner.execute_stage(run.stage_id, definition, run, workspace_root: root)

    metadata = commit_output |> String.trim() |> Jason.decode!()
    bundle = Path.join(workspace, metadata["candidate_bundle"])
    {head, 0} = System.cmd("git", ["rev-parse", "HEAD"], cd: workspace)
    assert metadata["candidate_commit"] == String.trim(head)
    assert metadata["base_commit"] == String.trim(base)

    assert metadata["candidate_input_sha256"] ==
             Base.encode16(
               :crypto.hash(:sha256, Jason.encode!(%{"thread_id" => "controlled-contract-conversation"})),
               case: :lower
             )

    assert File.regular?(bundle)
    assert metadata["bundle_sha256"] == Base.encode16(:crypto.hash(:sha256, File.read!(bundle)), case: :lower)
    assert metadata["bundle_bytes"] == byte_size(File.read!(bundle))

    run = run |> WorkstreamRun.finish_stage({:ok, committed}) |> WorkstreamRun.start_stage()
    assert run.stage_id == "inspect-candidate"
    assert run.artifacts["candidate-commit"].attempt_id == "#{run.id}/local-commit/2"

    assert {:ok, %{exit_status: 0} = checked} =
             WorkstreamRunner.execute_stage(run.stage_id, definition, run, workspace_root: root)

    File.write!(Path.join(workspace, "untracked"), "dirty candidate")

    assert {:ok, %{exit_status: status}} =
             WorkstreamRunner.execute_stage(run.stage_id, definition, run, workspace_root: root)

    assert status != 0
    run = run |> WorkstreamRun.finish_stage({:ok, checked}) |> WorkstreamRun.start_stage()
    assert run.stage_id == "validate"
    assert run.status == :executing
    assert definition.stages["validate"].inputs == ["candidate", "candidate-inspection", "candidate-commit"]
    assert definition.stages["publish"].type == :publish
    assert definition.stages["publish"].gate.validation_stage == "validate"
    assert definition.stages["publish"].gate.required == definition.stages["validate"].gate.required
    assert definition.stages["candidate-ready"].inputs == ["publication", "github-validation"]
    assert definition.stages["candidate-ready"].prompt =~ "Human merge remains in GitHub"
    # A caller's prose or verdict cannot stand in for authenticated final validation.
    blocked = WorkstreamRun.finish_stage(run, {:ok, %{gate: %{verdict: :passed}, exit_status: 0}})
    assert blocked.status == :blocked
    refute Enum.any?(blocked.attempts, &(&1.stage == "publish"))
    assert run.outputs["candidate"] == %{"thread_id" => "controlled-contract-conversation"}
    assert run.artifacts["candidate-commit"].attempt_id == "#{run.id}/local-commit/2"
    assert Enum.map(blocked.attempts, & &1.stage) == ["implement", "local-commit", "inspect-candidate", "validate"]
  end
end
