defmodule SymphonyElixir.LocalCandidateCommitTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{Workstream, WorkstreamRun, WorkstreamRunner}

  setup do
    root = Path.join(System.tmp_dir!(), "software-change-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "workstreams"))
    source = Path.expand("../../../factory/workstreams/software-change.yaml", __DIR__)
    path = Path.join(root, "workstreams/software-change.yaml")
    File.cp!(source, path)
    on_exit(fn -> File.rm_rf!(root) end)
    factory_root = source |> Path.dirname() |> Path.dirname()
    Enum.each(~w(agents instructions skills), &File.cp_r!(Path.join(factory_root, &1), Path.join(root, &1)))
    assert {:ok, definition} = Workstream.load(path, %{"task" => "DEV pilot task"})
    workspace = Path.join(root, "clone")
    File.mkdir_p!(workspace)
    git(workspace, ["init", "--quiet"])
    git(workspace, ["switch", "--quiet", "-c", "cycle/disposable"])
    File.write!(Path.join(workspace, "candidate"), "base\n")
    git(workspace, ["add", "candidate"])
    git(workspace, ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "seed"])
    git(workspace, ["remote", "add", "origin", "https://example.invalid/private.git"])
    git(workspace, ["remote", "set-url", "--push", "origin", "DISABLED"])
    candidate = %{thread_id: "controlled-contract-conversation"}
    run = WorkstreamRun.new("linear/pilot", definition, workspace, workspace_root: root)
    run = run |> WorkstreamRun.start_stage() |> WorkstreamRun.finish_stage({:ok, candidate}) |> WorkstreamRun.start_stage()
    %{root: root, source: source, path: path, definition: definition, workspace: workspace, run: run, candidate: candidate}
  end

  test "real tracked and untracked changes commit once with a verifiable transport bundle", c do
    base = git(c.workspace, ["rev-parse", "HEAD"])
    config = File.read!(Path.join(c.workspace, ".git/config"))
    receiver = Path.join(c.root, "receiver")
    git(c.root, ["clone", "--quiet", "--no-hardlinks", c.workspace, receiver])
    File.write!(Path.join(c.workspace, "candidate"), "changed\n")
    File.write!(Path.join(c.workspace, "new-file"), "new candidate\n")
    assert {:ok, %{exit_status: 0, output: output}} = execute(c)
    result = Jason.decode!(output)
    head = git(c.workspace, ["rev-parse", "HEAD"])
    assert result["candidate_commit"] == head
    assert result["base_commit"] == base
    assert head != base
    assert git(c.workspace, ["status", "--porcelain"]) == ""
    assert git(c.workspace, ["rev-list", "--count", "HEAD"]) == "2"
    assert git(c.workspace, ["branch", "--show-current"]) == "cycle/disposable"
    assert git(c.workspace, ["remote", "get-url", "--push", "origin"]) == "DISABLED"
    assert File.read!(Path.join(c.workspace, ".git/config")) == config
    assert {:ok, %{exit_status: 0, output: replay}} = execute(c, nil)
    assert Jason.decode!(replay) == result
    assert git(c.workspace, ["rev-list", "--count", "HEAD"]) == "2"
    bundle = Path.join(c.workspace, result["candidate_bundle"])
    assert result["bundle_sha256"] == :crypto.hash(:sha256, File.read!(bundle)) |> Base.encode16(case: :lower)
    assert result["bundle_bytes"] == File.stat!(bundle).size
    assert Bitwise.band(File.stat!(bundle).mode, 0o777) == 0o640
    assert Bitwise.band(File.stat!(Path.dirname(bundle)).mode, 0o777) == 0o750
    assert Bitwise.band(File.stat!(c.workspace).mode, 0o777) == 0o710
    assert Bitwise.band(File.stat!(Path.join(c.workspace, ".git")).mode, 0o777) == 0o710
    assert git(c.workspace, ["bundle", "list-heads", bundle]) == head <> " HEAD"
    {_output, missing} = System.cmd("git", ["cat-file", "-e", head <> "^{commit}"], cd: receiver, stderr_to_stdout: true)
    assert missing != 0
    git(receiver, ["fetch", "--quiet", bundle, "HEAD"])
    git(receiver, ["reset", "--hard", head])
    assert File.read!(Path.join(receiver, "new-file")) == "new candidate\n"
    assert git(receiver, ["status", "--porcelain"]) == ""
    File.write!(Path.join(c.workspace, "candidate"), "changed after preparation\n")
    assert {:ok, %{exit_status: changed_status, output: changed_output}} = execute(c)
    assert changed_status != 0
    assert changed_output =~ "prepared candidate changed"
    assert git(c.workspace, ["rev-parse", "HEAD"]) == head
    File.write!(Path.join(c.workspace, "candidate"), "changed\n")
    assert {:ok, %{exit_status: status}} = execute(%{c | run: %{c.run | outputs: %{"candidate" => %{thread_id: "another-conversation"}}}})
    assert status != 0
    assert git(c.workspace, ["rev-parse", "HEAD"]) == head
  end

  test "remote runner grants the larger bound only to its saved producer stage and digest", c do
    stage = c.definition.stages["local-commit"]
    pin = :crypto.hash(:sha256, Jason.encode!(stage.gate.command)) |> Base.encode16(case: :lower)

    control = %{
      "host" => "worker-a",
      "machine_id" => String.duplicate("a", 32),
      "service_revision" => String.duplicate("a", 40),
      "release_sha256" => String.duplicate("b", 64),
      "argv" => ["/opt/rig/bin/codex", "app-server"]
    }

    execution = Map.merge(c.run.execution, %{candidate_producer_stage: stage.id, candidate_producer_command_sha256: pin})
    run = %{c.run | id: "local", execution: execution}
    opts = [workspace_root: c.root, worker_control: control]

    # No callback: stop after both command and request validation, before any RPC.
    result = WorkstreamRunner.execute_stage(stage, c.definition, run, opts)
    assert {:error, :worker_registration_callback_required} = result

    for {check, execution} <- [
          {%{stage | id: "ordinary-check"}, execution},
          {stage, Map.delete(execution, :candidate_producer_command_sha256)},
          {stage, Map.put(execution, :candidate_producer_command_sha256, String.duplicate("a", 64))}
        ] do
      assert {:error, :invalid_worker_check_command} =
               WorkstreamRunner.execute_stage(check, c.definition, %{run | execution: execution}, opts)
    end
  end

  test "an authorized repair commits on its exact parent and replays the same marker", c do
    parent = git(c.workspace, ["rev-parse", "HEAD"])
    context = candidate_context(parent, parent)
    File.write!(Path.join(c.workspace, "candidate"), "repaired candidate\n")

    assert {:ok, %{exit_status: 0, output: output}} = execute(c, context)
    result = Jason.decode!(output)
    head = git(c.workspace, ["rev-parse", "HEAD"])

    assert result["candidate_commit"] == head
    assert result["base_commit"] == parent
    assert result["candidate_context_id"] == context["id"]
    assert result["repair_parent_sha"] == parent
    assert result["validation_base"] == parent
    assert result["candidate_input_sha256"] == candidate_context_digest(c.candidate, context)
    assert git(c.workspace, ["rev-list", "--parents", "-n", "1", head]) == "#{head} #{parent}"
    assert git(c.workspace, ["status", "--porcelain"]) == ""

    assert {:ok, %{exit_status: 0, output: replay}} = execute(c, context)
    assert Jason.decode!(replay) == result
    assert git(c.workspace, ["rev-list", "--count", "HEAD"]) == "2"
  end

  test "missing and forged repair claims fail before creating a candidate commit", c do
    parent = git(c.workspace, ["rev-parse", "HEAD"])
    File.write!(Path.join(c.workspace, "candidate"), "untrusted candidate changes\n")

    missing = %{"id" => String.duplicate("a", 64), "repair_parent_sha" => parent}
    assert {:ok, %{exit_status: missing_status, output: missing_output}} = execute(c, missing)
    assert missing_status != 0
    assert missing_output =~ "invalid candidate context claims"
    assert git(c.workspace, ["rev-parse", "HEAD"]) == parent
    assert git(c.workspace, ["diff", "--cached", "--name-only"]) == ""

    tree = git(c.workspace, ["rev-parse", "HEAD^{tree}"])

    {claimed_parent, 0} =
      System.cmd(
        "git",
        ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit-tree", tree, "-p", parent, "-m", "forged repair parent"],
        cd: c.workspace,
        stderr_to_stdout: true
      )

    claimed_parent = String.trim(claimed_parent)
    forged = candidate_context(claimed_parent, parent)

    assert {:ok, %{exit_status: forged_status, output: forged_output}} = execute(c, forged)
    assert forged_status != 0
    assert forged_output =~ "repair parent does not match branch head"
    assert git(c.workspace, ["rev-parse", "HEAD"]) == parent
    assert git(c.workspace, ["diff", "--cached", "--name-only"]) == ""
    assert File.read!(Path.join(c.workspace, "candidate")) == "untrusted candidate changes\n"
  end

  test "a validation base outside the repair parent's ancestry fails before staging", c do
    parent = git(c.workspace, ["rev-parse", "HEAD"])
    tree = git(c.workspace, ["rev-parse", "HEAD^{tree}"])

    {unrelated_base, 0} =
      System.cmd(
        "git",
        ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit-tree", tree, "-m", "unrelated main"],
        cd: c.workspace,
        stderr_to_stdout: true
      )

    unrelated_base = String.trim(unrelated_base)
    File.write!(Path.join(c.workspace, "candidate"), "candidate against unrelated main\n")

    assert {:ok, %{exit_status: status, output: output}} = execute(c, candidate_context(parent, unrelated_base))
    assert status != 0
    assert output =~ "validation base is not an ancestor of repair parent"
    assert git(c.workspace, ["rev-parse", "HEAD"]) == parent
    assert git(c.workspace, ["diff", "--cached", "--name-only"]) == ""
    assert File.read!(Path.join(c.workspace, "candidate")) == "candidate against unrelated main\n"
  end

  test "a clean base does not become a falsely successful candidate", c do
    head = git(c.workspace, ["rev-parse", "HEAD"])
    assert {:ok, %{exit_status: status, output: output}} = execute(c)
    assert status != 0
    assert output =~ "no candidate changes"
    run = WorkstreamRun.finish_stage(c.run, {:ok, %{exit_status: status, output: output}})
    assert run.status == :blocked
    assert git(c.workspace, ["rev-parse", "HEAD"]) == head
  end

  test "the fixed check does not execute commit hooks or signing programs", c do
    sentinel = Path.join(c.root, "unsafe-executed")
    script = Path.join(c.root, "unsafe-program")
    File.write!(script, "#!/bin/sh\nprintf unsafe > #{sentinel}\nexit 1\n")
    File.chmod!(script, 0o755)
    File.cp!(script, Path.join(c.workspace, ".git/hooks/pre-commit"))
    git(c.workspace, ["config", "core.hooksPath", Path.join(c.workspace, ".git/hooks")])
    git(c.workspace, ["config", "commit.gpgSign", "true"])
    File.write!(Path.join(c.workspace, "candidate"), "changed\n")
    assert {:ok, %{exit_status: 0}} = execute(c)
    refute File.exists?(sentinel)
    assert git(c.workspace, ["status", "--porcelain"]) == ""
  end

  test "untrusted filter configuration blocks before staging or running candidate code", c do
    sentinel = Path.join(c.root, "filter-executed")
    git(c.workspace, ["config", "filter.unsafe.clean", "touch #{sentinel}"])
    File.write!(Path.join(c.workspace, ".gitattributes"), "candidate filter=unsafe\n")
    File.write!(Path.join(c.workspace, "candidate"), "changed\n")
    assert {:ok, %{exit_status: status, output: output}} = execute(c)
    assert status != 0
    assert output =~ "unsupported Git configuration"
    refute File.exists?(sentinel)
    assert git(c.workspace, ["diff", "--cached", "--name-only"]) == ""
  end

  test "detached HEAD and shared metadata cannot receive service commits", c do
    head = git(c.workspace, ["rev-parse", "HEAD"])
    git(c.workspace, ["checkout", "--quiet", "--detach"])
    File.write!(Path.join(c.workspace, "candidate"), "changed\n")
    assert {:ok, %{exit_status: status}} = execute(c)
    assert status != 0
    assert git(c.workspace, ["rev-parse", "HEAD"]) == head
    git(c.workspace, ["checkout", "--quiet", "cycle/disposable"])
    File.write!(Path.join(c.workspace, ".git/commondir"), "../other-git")
    refute match?({:ok, %{exit_status: 0}}, execute(c))
    assert File.read!(Path.join(c.workspace, "candidate")) == "changed\n"
  end

  test "model results cannot substitute missing conversation evidence", c do
    File.write!(Path.join(c.workspace, "candidate"), "changed\n")
    assert {:ok, %{exit_status: status, output: output}} = execute(%{c | run: %{c.run | outputs: %{"candidate" => %{}}}})
    assert status != 0
    assert output =~ "missing candidate input"
    assert git(c.workspace, ["diff", "--cached", "--name-only"]) == ""
  end

  defp execute(c), do: WorkstreamRunner.execute_stage("local-commit", c.definition, c.run, workspace_root: c.root)

  defp execute(c, context) do
    SymphonyElixir.WorkstreamCommand.run(
      c.definition.stages["local-commit"].gate.command,
      c.workspace,
      c.definition.stages["local-commit"].gate.timeout_ms,
      %{"candidate" => c.candidate, "candidate_context" => context}
    )
  end

  defp candidate_context(repair_parent_sha, validation_base) do
    %{
      "id" => String.duplicate("b", 64),
      "repair_parent_sha" => repair_parent_sha,
      "validation_base" => validation_base
    }
  end

  defp candidate_context_digest(candidate, context) do
    candidate_json = Jason.encode!(candidate)

    context_json =
      "{" <>
        Enum.map_join(
          ["id", "repair_parent_sha", "validation_base"],
          ",",
          fn key ->
            Jason.encode!(key) <> ":" <> Jason.encode!(context[key])
          end
        ) <> "}"

    input_json = ~s({"candidate":#{candidate_json},"candidate_context":#{context_json}})
    :crypto.hash(:sha256, input_json) |> Base.encode16(case: :lower)
  end

  defp git(workspace, args) do
    {output, 0} = System.cmd("git", args, cd: workspace, stderr_to_stdout: true)
    String.trim(output)
  end
end
