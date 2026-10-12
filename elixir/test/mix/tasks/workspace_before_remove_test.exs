defmodule Mix.Tasks.Workspace.BeforeRemoveTest do
  use SymphonyElixir.TestSupport

  import ExUnit.CaptureIO

  setup do
    Mix.Task.reenable("workspace.before_remove")
    root = Path.join(System.tmp_dir!(), "workspace-before-remove-test-#{System.unique_integer([:positive])}")
    bin = Path.join(root, "bin")
    log = Path.join(root, "commands.log")
    pr_state = Path.join(root, "pull-request-state")
    File.mkdir_p!(bin)
    File.write!(log, "")
    File.write!(pr_state, "open\n")

    scripts = %{
      "gh" => """
      #!/bin/sh
      printf 'gh %s\\n' "$*" >> "$SYMPHONY_HOOK_COMMAND_LOG"
      case "$1 $2" in
        'auth status') exit 0 ;;
        'pr list') printf '101\\n'; exit 0 ;;
        'pr close') printf 'closed\\n' > "$SYMPHONY_HOOK_PR_STATE"; exit 0 ;;
      esac
      exit 99
      """,
      "git" => """
      #!/bin/sh
      printf 'git %s\\n' "$*" >> "$SYMPHONY_HOOK_COMMAND_LOG"
      printf 'feature/workpad\\n'
      """
    }

    Enum.each(scripts, fn {name, script} ->
      path = Path.join(bin, name)
      File.write!(path, script)
      File.chmod!(path, 0o755)
    end)

    overrides = %{
      "PATH" => bin <> ":" <> (System.get_env("PATH") || ""),
      "SYMPHONY_HOOK_COMMAND_LOG" => log,
      "SYMPHONY_HOOK_PR_STATE" => pr_state
    }

    previous = Map.new(overrides, fn {key, _} -> {key, System.get_env(key)} end)
    Enum.each(overrides, fn {key, value} -> System.put_env(key, value) end)

    on_exit(fn ->
      Enum.each(previous, fn {key, value} -> restore_env(key, value) end)
      File.rm_rf!(root)
    end)

    %{root: root, log: log, pr_state: pr_state}
  end

  test "legacy invocations leave an open PR and the workspace unchanged without GitHub or Git calls", c do
    note = Path.join(c.root, "uncommitted.txt")
    File.write!(note, "recoverable work\n")

    for args <- [[], ["--branch", "feature/workpad"], ["--repo", "openai/symphony"], ["--branch", "feature/workpad", "--repo", "RigCodeAI/rig"], ["legacy-positional"]] do
      Mix.Task.reenable("workspace.before_remove")
      output = capture_io(fn -> assert :ok = Mix.Task.run("workspace.before_remove", args) end)
      assert File.read!(c.pr_state) == "open\n"
      assert File.read!(note) == "recoverable work\n"
      assert File.read!(c.log) == ""
      assert output == ""
    end
  end

  test "succeeds without Git or GitHub tools or a repository", c do
    System.put_env("PATH", "")

    File.cd!(c.root, fn ->
      assert :ok = Mix.Task.run("workspace.before_remove", ["--branch", "feature/no-tools", "--repo", "RigCodeAI/rig"])
    end)

    assert File.read!(c.log) == ""
    assert File.read!(c.pr_state) == "open\n"
  end

  test "help describes the compatibility options without invoking external commands", c do
    for args <- [["--help"], ["-h"]] do
      Mix.Task.reenable("workspace.before_remove")
      output = capture_io(fn -> Mix.Task.run("workspace.before_remove", args) end)
      assert output =~ "mix workspace.before_remove"
      assert output =~ "--branch"
      assert output =~ "--repo"
      assert output =~ "have no effect"
    end

    assert File.read!(c.log) == ""
    assert File.read!(c.pr_state) == "open\n"
  end

  test "invalid and missing options still fail without touching GitHub", c do
    for args <- [["--wat"], ["--branch"], ["--repo"]] do
      Mix.Task.reenable("workspace.before_remove")
      assert_raise Mix.Error, ~r/Invalid option/, fn -> Mix.Task.run("workspace.before_remove", args) end
    end

    assert File.read!(c.log) == ""
    assert File.read!(c.pr_state) == "open\n"
  end

  test "workspace deletion invokes the compatibility task in a shell and preserves the open PR", c do
    source = Path.expand("../../../lib/mix/tasks/workspace.before_remove.ex", __DIR__)
    code = "Mix.start(); Code.require_file(#{inspect(source)}); Mix.Task.run(\"workspace.before_remove\", System.argv())"
    elixir = System.find_executable("elixir") || flunk("Elixir executable is required for the shell hook test")
    argv = Enum.map_join([elixir, "-e", code, "--", "--branch", "feature/workpad", "--repo", "RigCodeAI/rig"], " ", &shell_quote/1)
    command = "PATH=" <> shell_quote(System.fetch_env!("PATH")) <> " " <> argv
    marker = Path.join(c.root, "hook-completed")
    workspace_root = Path.join(c.root, "workspaces")
    workspace = Path.join(workspace_root, "DEV-DISPOSABLE")
    File.mkdir_p!(workspace)
    File.write!(Path.join(workspace, "temporary.txt"), "disposable test data")

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: workspace_root,
      hook_before_remove: command <> " && printf completed > " <> shell_quote(marker)
    )

    assert {:ok, _} = Workspace.remove(workspace)
    assert File.read!(marker) == "completed"
    refute File.exists?(workspace)
    assert File.read!(c.pr_state) == "open\n"
    assert File.read!(c.log) == ""
  end

  defp shell_quote(value), do: "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
end
