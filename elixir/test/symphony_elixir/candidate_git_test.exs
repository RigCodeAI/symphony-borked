defmodule SymphonyElixir.CandidateGitTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.CandidateGit

  setup do
    root = Path.join(System.tmp_dir!(), "candidate-git-test-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false))
    workspace = Path.join(root, "repo")
    File.mkdir_p!(root)
    create_repo(workspace)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, root: root, workspace: workspace}
  end

  test "isolates candidate config and inherited Git config while detecting a stale-stat edit", %{root: root, workspace: workspace} do
    marker = Path.join(root, "filter-ran")
    file = Path.join(workspace, "file.txt")
    mtime_reference = Path.join(root, "old-mtime.txt")

    File.write!(Path.join(workspace, ".gitattributes"), "file.txt filter=arbitrary\n")
    File.write!(file, "before\n")
    git!(workspace, ["add", ".gitattributes", "file.txt"])
    git!(workspace, ["commit", "--quiet", "-m", "baseline"])

    {_, 0} = System.cmd("cp", ["-p", file, mtime_reference])
    File.write!(file, "after!\n")
    {_, 0} = System.cmd("touch", ["-r", mtime_reference, file])

    include_path = Path.join(root, "malicious.inc")

    File.write!(include_path, """
    [core]
        fsmonitor = !touch #{marker}
    [filter "arbitrary"]
        clean = touch #{marker}; cat
        process = touch #{marker}; exit 1
    """)

    git!(workspace, ["config", "include.path", include_path])

    with_git_env(
      [
        {"GIT_DIR", Path.join(workspace, ".git")},
        {"GIT_WORK_TREE", root},
        {"GIT_INDEX_FILE", Path.join(root, "other-index")},
        {"GIT_CONFIG_COUNT", "1"},
        {"GIT_CONFIG_KEY_0", "filter.arbitrary.clean"},
        {"GIT_CONFIG_VALUE_0", "touch #{marker}; cat"},
        {"GIT_CONFIG_GLOBAL", include_path},
        {"GIT_CONFIG_SYSTEM", include_path},
        {"GIT_ALTERNATE_OBJECT_DIRECTORIES", root}
      ],
      fn ->
        assert {:ok, " M file.txt"} = CandidateGit.run(workspace, ["status", "--porcelain=v1", "--untracked-files=normal"])
      end
    )

    refute File.exists?(marker)
  end

  test "resolves refs and supports diff and archive commands", %{root: root, workspace: workspace} do
    base = git!(workspace, ["rev-parse", "HEAD"]) |> String.trim()
    branch = CandidateGit.run(workspace, ["rev-parse", "--symbolic-full-name", "HEAD"])
    assert {:ok, "refs/heads/" <> _} = branch

    File.write!(Path.join(workspace, "file.txt"), "after!\n")
    git!(workspace, ["add", "file.txt"])
    git!(workspace, ["commit", "--quiet", "-m", "second"])

    assert {:ok, ^base} = CandidateGit.run(workspace, ["rev-parse", "--verify", base <> "^{commit}"])
    assert {:ok, "file.txt"} = CandidateGit.run(workspace, ["diff", "--name-only", base, "HEAD"])

    archive = Path.join(root, "candidate.tar")
    assert {:ok, ""} = CandidateGit.run(workspace, ["archive", "--format=tar", "--output=" <> archive, "HEAD"])
    assert File.regular?(archive)
  end

  test "does not use candidate alternates", %{root: root, workspace: workspace} do
    objects = Path.join(workspace, ".git/objects")
    alternate_objects = Path.join(root, "alternate-objects")

    File.rename!(objects, alternate_objects)
    File.mkdir_p!(Path.join(objects, "info"))
    File.write!(Path.join(objects, "info/alternates"), alternate_objects <> "\n")

    assert {:error, {:candidate_git_command_failed, "rev-parse", _status, _output}} =
             CandidateGit.run(workspace, ["rev-parse", "--verify", "HEAD^{commit}"])
  end

  test "resolves SHA-256 commits without loading their candidate config", %{root: root} do
    workspace = Path.join(root, "sha256")
    File.mkdir_p!(workspace)
    git!(workspace, ["init", "--quiet", "--object-format=sha256"])
    create_repo(workspace)
    head = git!(workspace, ["rev-parse", "HEAD"]) |> String.trim()
    assert byte_size(head) == 64
    assert {:ok, ^head} = CandidateGit.run(workspace, ["rev-parse", "--verify", "HEAD^{commit}"])
    assert {:ok, ""} = CandidateGit.run(workspace, ["status", "--porcelain=v1"])
  end

  test "rejects a candidate .git symlink", %{workspace: workspace} do
    git_dir = Path.join(workspace, ".git")
    renamed_git_dir = Path.join(workspace, ".git-real")
    File.rename!(git_dir, renamed_git_dir)
    File.ln_s!(renamed_git_dir, git_dir)

    assert {:error, :candidate_git_metadata_symlink} = CandidateGit.run(workspace, ["rev-parse", "HEAD"])
  end

  test "rejects nested metadata symlinks", %{root: root, workspace: workspace} do
    oid = git!(workspace, ["rev-parse", "HEAD"]) |> String.trim()
    outside_ref = Path.join(root, "outside-ref")
    File.write!(outside_ref, oid <> "\n")
    File.ln_s!(outside_ref, Path.join(workspace, ".git/refs/heads/linked"))

    assert {:error, :candidate_git_metadata_symlink} = CandidateGit.run(workspace, ["rev-parse", "HEAD"])
  end

  test "rejects oversized metadata before copying a sparse file", %{workspace: workspace} do
    oversized = Path.join(workspace, ".git/objects/oversized-sparse-object")
    max_metadata_bytes = 512 * 1024 * 1024

    File.open!(oversized, [:write, :binary], fn file ->
      assert {:ok, ^max_metadata_bytes} = :file.position(file, max_metadata_bytes)
      assert :ok = IO.binwrite(file, <<0>>)
    end)

    assert {:error, :candidate_git_metadata_limit_exceeded} =
             CandidateGit.run(workspace, ["rev-parse", "HEAD"])
  end

  test "does not inspect submodules while checking status", %{root: root, workspace: workspace} do
    submodule = Path.join(root, "submodule-source")
    create_repo(submodule)
    File.write!(Path.join(submodule, ".gitattributes"), "file.txt filter=arbitrary\n")
    git!(submodule, ["add", ".gitattributes"])
    git!(submodule, ["commit", "--quiet", "-m", "attribute"])

    git!(workspace, ["-c", "protocol.file.allow=always", "submodule", "add", submodule, "nested"])
    git!(workspace, ["commit", "--quiet", "-m", "submodule"])

    marker = Path.join(root, "nested-filter-ran")
    nested = Path.join(workspace, "nested")
    git!(nested, ["config", "core.fsmonitor", "!touch #{marker}"])
    git!(nested, ["config", "filter.arbitrary.clean", "touch #{marker}; cat"])
    File.write!(Path.join(nested, "file.txt"), "after!\n")

    assert {:ok, ""} = CandidateGit.run(workspace, ["status", "--porcelain=v1"])
    refute File.exists?(marker)
  end

  defp create_repo(path) do
    File.mkdir_p!(path)
    git!(path, ["init", "--quiet"])
    git!(path, ["config", "user.name", "Candidate"])
    git!(path, ["config", "user.email", "candidate@example.test"])
    File.write!(Path.join(path, "file.txt"), "before\n")
    git!(path, ["add", "file.txt"])
    git!(path, ["commit", "--quiet", "-m", "initial"])
  end

  defp git!(path, args) do
    {output, 0} =
      System.cmd("git", ["-C", path | args],
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

  defp with_git_env(overrides, fun) do
    previous = Map.new(overrides, fn {key, _value} -> {key, System.get_env(key)} end)
    Enum.each(overrides, fn {key, value} -> System.put_env(key, value) end)

    try do
      fun.()
    after
      Enum.each(previous, fn
        {key, nil} -> System.delete_env(key)
        {key, value} -> System.put_env(key, value)
      end)
    end
  end
end
