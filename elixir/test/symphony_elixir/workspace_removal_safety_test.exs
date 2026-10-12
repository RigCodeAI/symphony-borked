defmodule SymphonyElixir.WorkspaceRemovalSafetyTest do
  use SymphonyElixir.TestSupport

  setup do
    base = Path.join(System.tmp_dir!(), "workspace-removal-safety-#{System.unique_integer([:positive])}")
    root = Path.join(base, "workspaces")
    outside = Path.join(base, "outside")
    bin = Path.join(base, "bin")
    log = Path.join(base, "ssh.log")
    File.mkdir_p!(root)
    File.mkdir_p!(outside)
    File.mkdir_p!(bin)
    File.write!(Path.join(outside, "keep.txt"), "outside evidence")
    File.write!(log, "")
    python = System.find_executable("python3") || flunk("Python3.11+ is required for removal tests")
    ssh = Path.join(bin, "ssh")

    File.write!(ssh, """
    #!#{python}
    import json, os, shlex, sys
    with open(os.environ['SYMPHONY_REMOVE_TEST_SSH_LOG'], 'a') as log:
        log.write(sys.argv[-2] + '\\n')
    script = shlex.split(sys.argv[-1])[2]
    nonce = next(line.split('=', 1)[1] for line in script.splitlines() if line.startswith('removal_nonce='))
    mode = os.environ.get('SYMPHONY_REMOVE_TEST_MODE', '')
    if mode == 'failure':
        print('__SYMPHONY_REMOVE__' + json.dumps({'status':'error','reason':'filesystem_error','errno':13,'nonce':nonce}))
        sys.exit(1)
    if mode == 'startup-spoof':
        print('__SYMPHONY_REMOVE__' + json.dumps({'status':'ok','hook':None}))
        sys.exit(0)
    if mode == 'missing':
        script = 'PATH=/nonexistent\\n' + script
    if mode == 'output-limit':
        sys.stdout.write('x' * 70000)
        sys.stdout.flush()
        sys.exit(0)
    os.execv('/bin/bash', ['bash', '-lc', script])
    """)

    File.chmod!(ssh, 0o755)
    overrides = %{"PATH" => bin <> ":" <> (System.get_env("PATH") || ""), "SYMPHONY_REMOVE_TEST_SSH_LOG" => log, "SYMPHONY_REMOVE_TEST_MODE" => ""}
    previous = Map.new(overrides, fn {key, _} -> {key, System.get_env(key)} end)
    Enum.each(overrides, fn {key, value} -> System.put_env(key, value) end)

    on_exit(fn ->
      Enum.each(previous, fn {key, value} -> restore_env(key, value) end)
      File.rm_rf!(base)
    end)

    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: root)
    %{base: base, root: root, outside: outside, bin: bin, log: log, python: python}
  end

  test "local and executable SSH removal reject outside/root paths before hooks", c do
    marker = Path.join(c.base, "hook")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: c.root, hook_before_remove: "touch " <> shell_quote(marker))

    for host <- [nil, "fixture-worker"] do
      assert {:error, {:workspace_outside_root, _, _}, ""} = Workspace.remove(c.outside, host)
      assert {:error, {:workspace_equals_root, _, _}, ""} = Workspace.remove(c.root, host)
      refute File.exists?(marker)
      assert File.read!(Path.join(c.outside, "keep.txt")) == "outside evidence"
    end
  end

  test "final, dangling and ancestor links cannot authorize deletion", c do
    for {name, target} <- [{"escape", c.outside}, {"dangling", Path.join(c.base, "missing")}], host <- [nil, "fixture-worker"] do
      path = Path.join(c.root, name <> if(is_nil(host), do: "-local", else: "-ssh"))
      File.ln_s!(target, path)
      assert {:error, _, ""} = Workspace.remove(path, host)
      assert {:ok, %{type: :symlink}} = File.lstat(path)
    end

    ancestor = Path.join(c.root, "ancestor")
    File.ln_s!(c.outside, ancestor)

    for host <- [nil, "fixture-worker"] do
      assert {:error, _, ""} = Workspace.remove(Path.join(ancestor, "keep.txt"), host)
    end

    assert File.read!(Path.join(c.outside, "keep.txt")) == "outside evidence"
  end

  test "hook-induced target, ancestor and root replacements retain the work and outside evidence", c do
    for host <- [nil, "fixture-worker"], kind <- [:target, :ancestor, :root] do
      suffix = "#{kind}-#{if(is_nil(host), do: "local", else: "ssh")}"
      root = Path.join(c.base, "root-" <> suffix)
      workspace = Path.join([root, "ancestor", "task"])
      File.mkdir_p!(workspace)
      File.write!(Path.join(workspace, "work.txt"), "retain")

      changed =
        case kind do
          :target -> workspace
          :ancestor -> Path.dirname(workspace)
          :root -> root
        end

      saved = changed <> "-saved"
      hook = "mv " <> shell_quote(changed) <> " " <> shell_quote(saved) <> "; ln -s " <> shell_quote(c.outside) <> " " <> shell_quote(changed)
      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: root, hook_before_remove: hook)
      assert {:error, _, ""} = Workspace.remove(workspace, host)

      retained =
        case kind do
          :target -> Path.join(saved, "work.txt")
          :ancestor -> Path.join([saved, "task", "work.txt"])
          :root -> Path.join([saved, "ancestor", "task", "work.txt"])
        end

      assert File.read!(retained) == "retain"
      assert File.read!(Path.join(c.outside, "keep.txt")) == "outside evidence"
    end
  end

  test "recorded paths require the current configured root", c do
    old = Path.join(c.outside, "old-task")
    File.mkdir!(old)

    for host <- [nil, "fixture-worker"] do
      assert {:error, {:workspace_outside_root, _, _}, ""} = Workspace.remove_recorded(old, host)
      assert File.dir?(old)
      assert {:error, {:workspace_outside_root, _, _}, ""} = Workspace.remove_recorded(Path.join(c.outside, "absent"), host)
    end
  end

  test "a fresh caller recovering a retained path uses the current root after configuration reload", c do
    workspace = Path.join(c.root, "retained-task")
    record = Path.join(c.base, "retained-resource.json")
    new_root = Path.join(c.base, "new-root")
    File.mkdir!(workspace)
    File.mkdir!(new_root)
    File.write!(Path.join(workspace, "work.txt"), "recoverable work")
    File.write!(record, Jason.encode!(%{workspace: workspace}))

    recover = fn ->
      retained = record |> File.read!() |> Jason.decode!()
      Workspace.remove_recorded(retained["workspace"], nil)
    end

    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: new_root)
    assert {:error, {:workspace_outside_root, _, _}, ""} = Task.async(recover) |> Task.await()
    assert File.read!(Path.join(workspace, "work.txt")) == "recoverable work"

    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: c.root)
    assert {:ok, []} = Task.async(recover) |> Task.await()
    refute File.exists?(workspace)
  end

  test "an ancestor replacement after the last check cannot redirect descriptor-relative deletion", c do
    workspace = Path.join([c.root, "ancestor", "task"])
    outside_task = Path.join(c.outside, "task")
    File.mkdir_p!(workspace)
    File.mkdir!(outside_task)
    File.write!(Path.join(outside_task, "keep.txt"), "outside evidence")
    fake = Path.join(c.bin, "python3")

    File.write!(fake, """
    #!#{c.python}
    import os, shutil, sys
    original = shutil.rmtree
    def replaced(name, *, dir_fd):
        os.rename(sys.argv[2] + '/ancestor', sys.argv[2] + '/ancestor-saved')
        os.symlink(#{inspect(c.outside)}, sys.argv[2] + '/ancestor')
        return original(name, dir_fd=dir_fd)
    replaced.avoids_symlink_attacks = original.avoids_symlink_attacks
    shutil.rmtree = replaced
    source = sys.argv[2]
    sys.argv = ['-c'] + sys.argv[3:]
    exec(source)
    """)

    File.chmod!(fake, 0o755)
    assert {:ok, []} = Workspace.remove(workspace)
    assert File.read!(Path.join(outside_task, "keep.txt")) == "outside evidence"
    refute File.exists?(Path.join([c.root, "ancestor-saved", "task"]))
  end

  test "truly absent direct paths are no-ops without hooks; ordinary successful cleanup stays compatible", c do
    marker = Path.join(c.base, "hook")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: c.root, hook_before_remove: "touch " <> shell_quote(marker))

    for host <- [nil, "fixture-worker"] do
      assert {:ok, []} = Workspace.remove(Path.join(c.outside, "absent"), host)
      refute File.exists?(marker)
      assert :ok = Workspace.remove_issue_workspaces("missing", host)
      refute File.exists?(marker)
      path = Path.join(c.root, "task")
      File.mkdir!(path)
      File.write!(Path.join(path, "data"), "disposable")
      assert :ok = Workspace.remove_issue_workspaces("task", host)
      assert File.read!(marker) == ""
      refute File.exists?(path)
      File.rm!(marker)
    end
  end

  test "hook failure and timeout are logged and ignored without leaking hook output", c do
    for {hook, log_text} <- [{"printf fixture-secret; exit 17", "Workspace hook failed"}, {"sleep 1", "Workspace hook timed out"}], host <- [nil, "fixture-worker"] do
      path = Path.join(c.root, "task")
      File.mkdir!(path)
      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: c.root, hook_before_remove: hook, hook_timeout_ms: 100)
      logs = capture_log(fn -> assert {:ok, []} = Workspace.remove(path, host) end)
      assert logs =~ log_text
      refute logs =~ "fixture-secret"
      refute File.exists?(path)
    end
  end

  test "missing local and SSH interpreters fail closed with actionable errors", c do
    path = Path.join(c.root, "task")
    File.mkdir!(path)
    previous = System.get_env("PATH")
    System.put_env("PATH", "")
    assert {:error, {:workspace_remove_unavailable, :python3_not_found}, ""} = Workspace.remove(path)
    System.put_env("PATH", previous)
    System.put_env("SYMPHONY_REMOVE_TEST_MODE", "missing")
    assert {:error, {:workspace_remove_unavailable, :python3_not_found}, ""} = Workspace.remove(path, "fixture-worker")
    assert File.dir?(path)
  end

  test "hook timeout reaps the hook and stops ordinary descendants", c do
    for host <- [nil, "fixture-worker"] do
      path = Path.join(c.root, "task")
      marker = Path.join(c.base, "survived-#{if(is_nil(host), do: "local", else: "ssh")}")
      File.mkdir!(path)
      hook = "(sleep 0.3; touch " <> shell_quote(marker) <> ") & sleep 2"
      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: c.root, hook_before_remove: hook, hook_timeout_ms: 100)
      assert {:ok, []} = Workspace.remove(path, host)
      Process.sleep(400)
      refute File.exists?(marker)
    end
  end

  test "an interpreter without symlink-resistant rmtree never runs hooks or deletes", c do
    path = Path.join(c.root, "task")
    marker = Path.join(c.base, "hook")
    File.mkdir!(path)
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: c.root, hook_before_remove: "touch " <> shell_quote(marker))
    fake = Path.join(c.bin, "python3")

    File.write!(fake, """
    #!#{c.python}
    import shutil, sys
    shutil.rmtree.avoids_symlink_attacks = False
    source = sys.argv[2]
    sys.argv = ['-c'] + sys.argv[3:]
    exec(source)
    """)

    File.chmod!(fake, 0o755)
    assert {:error, {:workspace_remove_unavailable, :python3_11_descriptor_support_required}, ""} = Workspace.remove(path)
    refute File.exists?(marker)
    assert File.dir?(path)
  end

  test "issue cleanup propagates failures and stops before the next host", c do
    path = Path.join(c.root, "task")
    File.mkdir!(path)
    System.put_env("SYMPHONY_REMOVE_TEST_MODE", "failure")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: c.root, worker_ssh_hosts: ["first-worker", "second-worker"])
    assert {:error, {:workspace_remove_failed, "first-worker", "filesystem_error", 13, _}, ""} = Workspace.remove_issue_workspaces("task")
    assert File.read!(c.log) == "first-worker\n"
    assert File.dir?(path)
  end

  test "excessive SSH output is stopped without invoking the deletion helper", c do
    path = Path.join(c.root, "task")
    File.mkdir!(path)
    System.put_env("SYMPHONY_REMOVE_TEST_MODE", "output-limit")
    assert {:error, {:workspace_remove_failed, :output_limit}, ""} = Workspace.remove(path, "fixture-worker")
    assert File.dir?(path)
  end

  test "a static SSH startup success marker cannot report cleanup success", c do
    path = Path.join(c.root, "task")
    File.mkdir!(path)
    System.put_env("SYMPHONY_REMOVE_TEST_MODE", "startup-spoof")
    assert {:error, {:workspace_remove_failed, "fixture-worker", 0, :invalid_result}, ""} = Workspace.remove(path, "fixture-worker")
    assert File.dir?(path)
  end

  test "an unusable discovered interpreter returns an error rather than crashing", c do
    path = Path.join(c.root, "task")
    File.mkdir!(path)
    fake = Path.join(c.bin, "python3")
    File.write!(fake, "not an executable interpreter")
    File.chmod!(fake, 0o755)
    assert {:error, _, ""} = Workspace.remove(path)
    assert File.dir?(path)
  end

  test "mount identity differences retain the subtree before hooks or deletion", c do
    path = Path.join(c.root, "task")
    nested = Path.join(path, "mounted")
    marker = Path.join(c.base, "hook")
    File.mkdir_p!(nested)
    File.write!(Path.join(nested, "keep.txt"), "mounted evidence")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: c.root, hook_before_remove: "touch " <> shell_quote(marker))
    fake = Path.join(c.bin, "python3")

    File.write!(fake, """
    #!#{c.python}
    import os, sys
    source = sys.argv[2]
    sys.argv = ['-c'] + sys.argv[3:]
    source = source.replace('result = {"status": "error"}', '''
    original_mount_identity = mount_identity
    def mounted(fd):
        if os.fstat(fd).st_ino == os.stat(#{inspect(nested)}).st_ino:
            return 'foreign-mounted-subtree'
        return original_mount_identity(fd)
    mount_identity = mounted
    result = {"status": "error"}
    ''')
    exec(source)
    """)

    File.chmod!(fake, 0o755)
    assert {:error, {:workspace_remove_failed, nil, "mount_boundary", nil, _}, ""} = Workspace.remove(path)
    assert File.read!(Path.join(nested, "keep.txt")) == "mounted evidence"
    refute File.exists?(marker)
  end

  test "remote tilde roots still expand on the host", c do
    name = ".workspace-removal-tilde-#{System.unique_integer([:positive])}"
    root = Path.join(System.user_home!(), name)
    path = Path.join(root, "task")
    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf(root) end)
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: "~/" <> name)
    assert {:ok, []} = Workspace.remove("~/" <> name <> "/task", "fixture-worker")
    refute File.exists?(path)
    assert File.dir?(root)
    assert File.read!(c.log) == "fixture-worker\n"
  end

  defp shell_quote(value), do: "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
end
