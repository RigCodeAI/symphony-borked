defmodule SymphonyElixir.PublisherGitTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.PublisherGit

  setup do
    root = Path.join(System.tmp_dir!(), "publisher-git-test-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false))
    workspace = Path.join(root, "repo")
    File.mkdir_p!(workspace)
    git!(workspace, ["init", "--quiet"])
    git!(workspace, ["config", "user.name", "Candidate"])
    git!(workspace, ["config", "user.email", "candidate@example.test"])
    File.write!(Path.join(workspace, "file.txt"), "candidate\n")
    git!(workspace, ["add", "file.txt"])
    git!(workspace, ["commit", "--quiet", "-m", "candidate"])
    sha = git!(workspace, ["rev-parse", "HEAD"]) |> String.trim()

    on_exit(fn -> File.rm_rf(root) end)
    {:ok, workspace: workspace, sha: sha}
  end

  test "push uses exact commit, fixed HTTPS remote, lease, and environment-only credentials", %{workspace: workspace, sha: sha} do
    secret = "installation-token-never-argv"
    expected_remote_sha = String.duplicate("b", 40)
    parent = self()

    command_fun = fn executable, args, cwd, env ->
      send(parent, {:command, executable, args, cwd, env})

      if "rev-parse" in args do
        {:ok, sha}
      else
        {:ok, "pushed"}
      end
    end

    assert :ok = PublisherGit.push_for_test(workspace, sha, "feature/publish", secret, expected_remote_sha, command_fun)

    assert_receive {:command, git, verify_args, ^workspace, verify_env}
    assert is_binary(git)
    assert "rev-parse" in verify_args
    refute Enum.any?(verify_args, &String.contains?(&1, secret))
    refute Map.has_key?(Map.new(verify_env), "GIT_DIR")

    assert_receive {:command, ^git, head_args, ^workspace, _head_env}
    assert "HEAD^{commit}" in head_args

    assert_receive {:command, ^git, push_args, ^workspace, push_env}
    assert Enum.member?(push_args, "--force-with-lease=refs/heads/feature/publish:" <> expected_remote_sha)
    assert Enum.member?(push_args, "https://github.com/RigCodeAI/rig.git")
    assert Enum.member?(push_args, sha <> ":refs/heads/feature/publish")
    assert Enum.member?(push_args, "--no-verify")
    assert Enum.member?(push_args, "protocol.https.allow=always")
    assert Enum.member?(push_args, "http.followRedirects=false")
    refute Enum.any?(push_args, &String.contains?(&1, secret))

    push_env = Map.new(push_env)
    auth_value = Map.fetch!(push_env, "GIT_CONFIG_VALUE_0")
    assert auth_value == "AUTHORIZATION: basic " <> Base.encode64("x-access-token:" <> secret)
    assert Map.fetch!(push_env, "GIT_CONFIG_GLOBAL") == "/dev/null"
    assert Map.fetch!(push_env, "GIT_CONFIG_SYSTEM") == "/dev/null"
    refute File.read!(Path.join(workspace, "file.txt")) =~ secret
  end

  test "nil expected remote SHA produces a creates-only lease", %{workspace: workspace, sha: sha} do
    parent = self()

    command_fun = fn _git, args, _cwd, _env ->
      send(parent, {:args, args})
      if "rev-parse" in args, do: {:ok, sha}, else: {:ok, "pushed"}
    end

    assert :ok = PublisherGit.push_for_test(workspace, sha, "feature/new", "token", nil, command_fun)
    assert_receive {:args, _verify_args}
    assert_receive {:args, _head_args}
    assert_receive {:args, push_args}
    assert Enum.member?(push_args, "--force-with-lease=refs/heads/feature/new:")
  end

  test "a different resolved commit blocks push", %{workspace: workspace, sha: sha} do
    parent = self()

    command_fun = fn _git, args, _cwd, _env ->
      send(parent, {:args, args})
      {:ok, String.duplicate("c", byte_size(sha))}
    end

    assert {:error, :publisher_commit_mismatch} =
             PublisherGit.push_for_test(workspace, sha, "feature/publish", "token", nil, command_fun)

    assert_receive {:args, _verify_args}
    refute_receive {:args, _push_args}
  end

  test "a candidate HEAD change between the last trusted check and push blocks publication", %{workspace: workspace, sha: sha} do
    parent = self()

    command_fun = fn _git, args, _cwd, _env ->
      send(parent, {:args, args})
      if "HEAD^{commit}" in args, do: {:ok, String.duplicate("d", byte_size(sha))}, else: {:ok, sha}
    end

    assert {:error, :candidate_head_changed} =
             PublisherGit.push_for_test(workspace, sha, "feature/publish", "token", nil, command_fun)

    assert_receive {:args, _verify_args}
    assert_receive {:args, _head_args}
    refute_receive {:args, _push_args}
  end

  test "production Git runner clears inherited secrets and stops before push on a changed HEAD", %{
    workspace: workspace,
    sha: sha
  } do
    File.write!(Path.join(workspace, "file.txt"), "changed head\n")
    git!(workspace, ["add", "file.txt"])
    git!(workspace, ["commit", "--quiet", "-m", "new HEAD"])

    real_git = System.find_executable("git") || flunk("git executable is required")
    wrapper_dir = Path.join(Path.dirname(workspace), "git-wrapper")
    File.mkdir_p!(wrapper_dir)
    File.write!(Path.join(wrapper_dir, "real-git"), real_git)

    wrapper = Path.join(wrapper_dir, "git")

    File.write!(
      wrapper,
      """
      #!/bin/sh
      if [ "${SYMPHONY_PUBLISHER_TEST_SECRET+x}" = x ]; then
        exit 91
      fi
      printf '%s\\n' "$*" >> "$0.calls"
      real_git=$(cat "$(dirname "$0")/real-git")
      exec "$real_git" "$@"
      """
    )

    File.chmod!(wrapper, 0o755)

    old_path = System.get_env("PATH")
    old_secret = System.get_env("SYMPHONY_PUBLISHER_TEST_SECRET")
    on_exit(fn -> restore_env("PATH", old_path) end)
    on_exit(fn -> restore_env("SYMPHONY_PUBLISHER_TEST_SECRET", old_secret) end)

    System.put_env("SYMPHONY_PUBLISHER_TEST_SECRET", "inherited-secret-must-not-reach-child")

    System.put_env(
      "PATH",
      if(old_path, do: wrapper_dir <> ":" <> old_path, else: wrapper_dir)
    )

    assert {:error, :candidate_head_changed} =
             PublisherGit.push(workspace, sha, "feature/publish", "installation-token", nil)

    calls = File.read!(wrapper <> ".calls") |> String.split("\n", trim: true)
    assert length(calls) == 2
    assert Enum.all?(calls, &String.contains?(&1, "rev-parse"))
    assert String.contains?(hd(calls), sha <> "^{commit}")
    assert String.contains?(List.last(calls), "HEAD^{commit}")
    refute Enum.any?(calls, &String.contains?(&1, "push"))
  end

  test "Git failure details cannot return an installation token", %{workspace: workspace, sha: sha} do
    secret = "private-token-in-error"
    parent = self()

    command_fun = fn _git, args, _cwd, _env ->
      send(parent, {:args, args})
      if "rev-parse" in args, do: {:ok, sha}, else: {:error, secret}
    end

    assert {:error, {:git_push_unknown, :command_failed}} =
             PublisherGit.push_for_test(workspace, sha, "feature/publish", secret, nil, command_fun)

    assert_receive {:args, _verify_args}
    assert_receive {:args, _head_args}
    assert_receive {:args, _push_args}
  end

  test "rejects malformed commit ids before starting Git", %{workspace: workspace} do
    command_fun = fn _git, _args, _cwd, _env -> flunk("Git should not run") end

    assert {:error, :invalid_publisher_commit_sha} =
             PublisherGit.push_for_test(workspace, "--upload-pack=evil", "feature/publish", "token", nil, command_fun)
  end

  defp git!(workspace, args) do
    {output, 0} =
      System.cmd("git", ["-C", workspace | args],
        env: isolated_git_environment(),
        stderr_to_stdout: true
      )

    output
  end

  defp isolated_git_environment do
    System.get_env()
    |> Enum.flat_map(fn {key, _value} -> if String.starts_with?(key, "GIT_"), do: [{key, nil}], else: [] end)
    |> Kernel.++([{"GIT_CONFIG_GLOBAL", "/dev/null"}, {"GIT_CONFIG_NOSYSTEM", "1"}])
  end

  defp restore_env(key, nil), do: System.delete_env(key)
  defp restore_env(key, value), do: System.put_env(key, value)
end
