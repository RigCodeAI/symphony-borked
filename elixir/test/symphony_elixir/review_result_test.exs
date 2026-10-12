defmodule SymphonyElixir.ReviewResultTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.ReviewResult

  @literal_path "src/space ü:literal[1]*.txt"
  @long_path "long.txt"
  @source_contents "first\nsecond\nthird\nfourth\nfifth\nsixth\n"

  setup do
    root = Path.join(System.tmp_dir!(), "review-result-test-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false))
    workspace = Path.join(root, "repo")
    File.mkdir_p!(workspace)
    create_repo(workspace)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, root: root, workspace: workspace, base_sha: git!(workspace, ["rev-parse", "HEAD"])}
  end

  test "exposes a stable pinned rubric and verifies its embedded bytes" do
    pin = ReviewResult.rubric()

    assert Enum.sort(Map.keys(pin)) == [:commit, :git_blob, :path, :sha256, :text]
    assert pin.path == "factory/review/rubric.md"
    assert is_binary(pin.text)
    assert pin.commit == "5fe4fc8f7cd16688b5c661d8cdb76e2a340b046d"
    assert pin.git_blob == "85e89cb7eeadb97fa9e4868d2b9977c87225506d"
    assert pin.sha256 == "ec60e7f36a1d1c2679ce095c0205ecc56f7dd8fb57707a13ef362072390f219f"
    assert pin.sha256 == sha256(pin.text)

    blob_algorithm = if byte_size(pin.git_blob) == 64, do: :sha256, else: :sha
    expected_blob = :crypto.hash(blob_algorithm, "blob #{byte_size(pin.text)}\0" <> pin.text) |> Base.encode16(case: :lower)
    assert pin.git_blob == expected_blob
  end

  test "accepts a finding on a literal unusual path and returns the exact result", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    path = Path.join(workspace, @literal_path)
    result = review_result([finding(path)])
    json = Jason.encode!(result)

    assert {:ok, validated} = validate(json, workspace, base_sha, head_sha)
    assert validated.base_sha == base_sha
    assert validated.head_sha == head_sha
    assert validated.result == result
    assert validated.locations == [%{path: @literal_path, start_line: 3, end_line: 3, side: :right}]
    assert validated.rubric == Map.drop(ReviewResult.rubric(), [:text])
  end

  test "accepts omitted and null priorities while retaining the rubric verdict", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    path = Path.join(workspace, @literal_path)
    omitted_priority = Map.delete(finding(path), "priority")
    null_priority = Map.put(finding(path), "priority", nil)
    result = review_result([omitted_priority, null_priority], %{"overall_correctness" => "patch is incorrect"})

    assert {:ok, validated} = validate(Jason.encode!(result), workspace, base_sha, head_sha)
    assert validated.result == result
    assert validated.result["overall_correctness"] == "patch is incorrect"
    assert length(validated.locations) == 2
  end

  test "validates remote reviewer paths against independently imported commits", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    reviewer_workspace = "/worker/reviews/one"
    result = review_result([finding(Path.join(reviewer_workspace, @literal_path))])

    assert {:ok, validated} = ReviewResult.validate(Jason.encode!(result), recorded_revision(base_sha, head_sha), revision(base_sha, head_sha), workspace, reviewer_workspace)
    assert validated.locations == [%{path: @literal_path, start_line: 3, end_line: 3, side: :right}]

    assert {:error, {:invalid_review_location, 0, :review_path_outside_workspace}} =
             ReviewResult.validate(Jason.encode!(result), recorded_revision(base_sha, head_sha), revision(base_sha, head_sha), workspace, "/worker/reviews/two")
  end

  test "accepts a clean result without changing its verdict", %{workspace: workspace, base_sha: base_sha} do
    result = review_result([], %{"overall_correctness" => "patch is incorrect"})

    assert {:ok, validated} = validate(Jason.encode!(result), workspace, base_sha, base_sha)
    assert validated.result == result
    assert validated.result["overall_correctness"] == "patch is incorrect"
    assert validated.locations == []
  end

  test "rejects stale base or head before parsing, including a clean result", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    clean_json = Jason.encode!(review_result())

    assert {:error, :stale_review} =
             call_validate("{", workspace, recorded_revision(base_sha, head_sha), revision(base_sha, base_sha))

    assert {:error, :stale_review} =
             call_validate(clean_json, workspace, recorded_revision(head_sha, head_sha), revision(base_sha, head_sha))
  end

  test "requires the recorded rubric digest", %{workspace: workspace, base_sha: base_sha} do
    result_json = Jason.encode!(review_result())
    recorded = recorded_revision(base_sha, base_sha)

    assert {:error, _reason} = call_validate(result_json, workspace, Map.delete(recorded, :rubric_sha256), revision(base_sha, base_sha))

    assert {:error, _reason} =
             call_validate(result_json, workspace, Map.put(recorded, :rubric_sha256, String.duplicate("0", 64)), revision(base_sha, base_sha))
  end

  test "rejects a checkout head that no longer matches the recorded revision", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    git!(workspace, ["checkout", "--quiet", base_sha])

    assert {:error, _reason} = validate(Jason.encode!(review_result()), workspace, base_sha, head_sha)
  end

  test "rejects annotated tag objects and abbreviated revision hashes", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    git!(workspace, ["tag", "-a", "review-tag", "-m", "annotated tag", head_sha])
    tag_object_sha = git!(workspace, ["rev-parse", "refs/tags/review-tag"])

    refute tag_object_sha == head_sha

    assert {:error, _reason} =
             validate(Jason.encode!(review_result()), workspace, base_sha, tag_object_sha)

    abbreviated_head = String.slice(head_sha, 0, 12)

    assert {:error, _reason} =
             validate(Jason.encode!(review_result()), workspace, base_sha, abbreviated_head)
  end

  test "requires exact keys at every result level", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    path = Path.join(workspace, @literal_path)
    valid = review_result([finding(path)])

    invalid_results = [
      Map.put(valid, "unexpected", true),
      Map.delete(valid, "overall_explanation"),
      update_first_finding(valid, &Map.put(&1, "unexpected", true)),
      update_first_finding(valid, &Map.delete(&1, "body")),
      update_location(valid, &Map.put(&1, "unexpected", true)),
      update_location(valid, &Map.delete(&1, "absolute_file_path")),
      update_line_range(valid, &Map.put(&1, "unexpected", true)),
      update_line_range(valid, &Map.delete(&1, "end"))
    ]

    for invalid <- invalid_results do
      assert {:error, _reason} = validate(Jason.encode!(invalid), workspace, base_sha, head_sha)
    end
  end

  test "rejects repeated JSON keys at the top level and inside line ranges", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    path = Path.join(workspace, @literal_path)

    duplicate_top =
      ~s|{"findings":[],"findings":[],"overall_correctness":"patch is correct","overall_explanation":"No findings.","overall_confidence_score":0.9}|

    duplicate_nested_finding =
      ~s|{"title":"[P1] Review changed line","body":"The changed line needs a guard.","confidence_score":0.9,"priority":1,"code_location":{"absolute_file_path":#{Jason.encode!(path)},"line_range":{"start":3,"start":3,"end":3}}}|

    assert {:error, _reason} = validate(duplicate_top, workspace, base_sha, head_sha)
    assert {:error, _reason} = validate(raw_result([duplicate_nested_finding]), workspace, base_sha, head_sha)
  end

  test "rejects prose and markdown fences around the JSON result", %{workspace: workspace, base_sha: base_sha} do
    json = Jason.encode!(review_result())
    fenced = "```json\n" <> json <> "\n```"
    prose = "Review result:\n" <> json

    assert {:error, _reason} = validate(fenced, workspace, base_sha, base_sha)
    assert {:error, _reason} = validate(prose, workspace, base_sha, base_sha)
  end

  test "rejects invalid priority, title, explanation, confidence, and range values", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    path = Path.join(workspace, @literal_path)
    valid = review_result([finding(path)])

    invalid_results = [
      update_first_finding(valid, &Map.put(&1, "title", "")),
      update_first_finding(valid, &Map.put(&1, "title", "[P1] " <> String.duplicate("x", 76))),
      update_first_finding(valid, &Map.put(&1, "title", "No priority prefix")),
      update_first_finding(valid, &Map.put(&1, "title", "[P2] Wrong numeric priority")),
      update_first_finding(valid, &Map.put(&1, "priority", 1.0)),
      update_first_finding(valid, &Map.put(&1, "body", " \n ")),
      update_first_finding(valid, &Map.put(&1, "confidence_score", -0.01)),
      update_first_finding(valid, &Map.put(&1, "confidence_score", 1.01)),
      update_first_finding(valid, &Map.put(&1, "confidence_score", "high")),
      Map.put(valid, "overall_explanation", "  "),
      Map.put(valid, "overall_confidence_score", 1.01),
      update_line_range(valid, &Map.put(&1, "start", 0)),
      update_line_range(valid, &Map.put(&1, "start", 4))
    ]

    for invalid <- invalid_results do
      assert {:error, _reason} = validate(Jason.encode!(invalid), workspace, base_sha, head_sha)
    end
  end

  test "allows Markdown whitespace while rejecting other controls in text and titles", %{workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")
    path = Path.join(workspace, @literal_path)

    valid =
      review_result([finding(path)])
      |> update_first_finding(&Map.put(&1, "body", "Line one\twith detail.\r\nLine two continues."))
      |> Map.put("overall_explanation", "Checked:\ncommitted diff.")

    assert {:ok, _validated} = validate(Jason.encode!(valid), workspace, base_sha, head_sha)

    invalid_results = [
      update_first_finding(valid, &Map.put(&1, "body", "Hidden\u0000control")),
      update_first_finding(valid, &Map.put(&1, "body", "Hidden\u0007control")),
      Map.put(valid, "overall_explanation", "Hidden\u007fcontrol"),
      update_first_finding(valid, &Map.put(&1, "title", "[P1] Review\nchanged")),
      update_first_finding(valid, &Map.put(&1, "title", "[P1] Review\tchanged"))
    ]

    for invalid <- invalid_results do
      assert {:error, _reason} = validate(Jason.encode!(invalid), workspace, base_sha, head_sha)
    end
  end

  test "requires locations to be absolute paths inside the workspace without dot segments", %{root: root, workspace: workspace, base_sha: base_sha} do
    head_sha = change_literal_file(workspace, "third changed\n")

    unsafe_paths = [
      workspace <> "/../repo/" <> @literal_path,
      workspace <> "/./" <> @literal_path,
      Path.join(root, "outside.txt")
    ]

    for path <- unsafe_paths do
      assert {:error, _reason} =
               validate(Jason.encode!(review_result([finding(path)])), workspace, base_sha, head_sha)
    end

    untracked = Path.join(workspace, "untracked.txt")
    File.write!(untracked, "untracked line\n")

    assert {:error, _reason} =
             validate(Jason.encode!(review_result([finding(untracked, 1, 1)])), workspace, base_sha, head_sha)

    unchanged = Path.join(workspace, "unchanged.txt")

    assert {:error, _reason} =
             validate(Jason.encode!(review_result([finding(unchanged, 1, 1)])), workspace, base_sha, head_sha)
  end

  test "rejects deleted-only paths", %{workspace: workspace, base_sha: base_sha} do
    deleted_path = Path.join(workspace, "deleted.txt")
    File.rm!(deleted_path)
    head_sha = commit!(workspace, "delete a file")

    assert {:error, _reason} =
             validate(Jason.encode!(review_result([finding(deleted_path, 1, 1)])), workspace, base_sha, head_sha)
  end

  test "rejects binary, symlink, and gitlink locations", %{workspace: workspace, base_sha: base_sha} do
    binary_path = Path.join(workspace, "binary.dat")
    File.write!(binary_path, <<0, 1, 3, 0>>)
    binary_head = commit!(workspace, "change binary file")

    assert {:error, _reason} =
             validate(Jason.encode!(review_result([finding(binary_path, 1, 1)])), workspace, base_sha, binary_head)

    link_path = Path.join(workspace, "link.txt")
    File.rm!(link_path)
    File.ln_s!("unchanged.txt", link_path)
    symlink_head = commit!(workspace, "replace a file with a symlink")

    assert {:error, _reason} =
             validate(Jason.encode!(review_result([finding(link_path, 1, 1)])), workspace, binary_head, symlink_head)

    submodule_sha = git!(workspace, ["rev-parse", "HEAD"])
    git!(workspace, ["update-index", "--add", "--cacheinfo", "160000,#{submodule_sha},nested-submodule"])
    git!(workspace, ["commit", "--quiet", "-m", "add a gitlink"])
    gitlink_head = git!(workspace, ["rev-parse", "HEAD"])
    gitlink_path = Path.join(workspace, "nested-submodule")

    assert {:error, _reason} =
             validate(Jason.encode!(review_result([finding(gitlink_path, 1, 1)])), workspace, symlink_head, gitlink_head)
  end

  test "accepts a renamed file at its new path with rename detection disabled", %{workspace: workspace, base_sha: base_sha} do
    old_path = Path.join(workspace, "deleted.txt")
    new_relative_path = "renamed file.txt"
    new_path = Path.join(workspace, new_relative_path)
    File.rename!(old_path, new_path)
    File.write!(new_path, "renamed and changed\n")
    head_sha = commit!(workspace, "rename and change file")

    assert {:ok, validated} =
             validate(Jason.encode!(review_result([finding(new_path, 1, 1)])), workspace, base_sha, head_sha)

    assert validated.locations == [%{path: new_relative_path, start_line: 1, end_line: 1, side: :right}]
  end

  test "rejects NUL-containing blobs even when attributes force textual diffs", %{workspace: workspace, base_sha: base_sha} do
    path = Path.join(workspace, "binary.dat")
    File.write!(Path.join(workspace, ".gitattributes"), "binary.dat diff\n")
    File.write!(path, <<0, 1, 4, 0>>)
    head_sha = commit!(workspace, "force a binary blob to display as text")

    assert {:error, _reason} = validate(Jason.encode!(review_result([finding(path, 1, 1)])), workspace, base_sha, head_sha)
  end

  test "rejects extra selected paths instead of assigning their hunks to a finding", %{workspace: workspace, base_sha: base_sha} do
    path = Path.join(workspace, "unchanged.txt")
    File.write!(Path.join(workspace, "--ignore-submodules=all"), "a different file's changed line\n")
    head_sha = commit!(workspace, "add a file that resembles the adapter option")

    assert {:error, _reason} = validate(Jason.encode!(review_result([finding(path, 1, 1)])), workspace, base_sha, head_sha)
  end

  test "requires changed lines in one unified diff hunk and rejects partial result acceptance", %{workspace: workspace, base_sha: base_sha} do
    path = Path.join(workspace, @long_path)
    lines = long_lines() |> List.update_at(1, fn _line -> "changed line 2" end) |> List.update_at(17, fn _line -> "changed line 18" end)
    File.write!(path, Enum.join(lines, "\n") <> "\n")
    head_sha = commit!(workspace, "change two separated lines")

    assert {:ok, _validated} = validate(Jason.encode!(review_result([finding(path, 2, 2)])), workspace, base_sha, head_sha)

    for range <- [{1, 1}, {10, 10}, {2, 18}] do
      assert {:error, _reason} =
               validate(Jason.encode!(review_result([finding(path, elem(range, 0), elem(range, 1))])), workspace, base_sha, head_sha)
    end

    mixed_findings = review_result([finding(path, 2, 2), finding(path, 10, 10)])
    assert {:error, _reason} = validate(Jason.encode!(mixed_findings), workspace, base_sha, head_sha)
  end

  test "checks committed diffs instead of uncommitted working-tree edits", %{workspace: workspace, base_sha: base_sha} do
    path = Path.join(workspace, @long_path)
    lines = long_lines() |> List.update_at(1, fn _line -> "committed line 2" end)
    File.write!(path, Enum.join(lines, "\n") <> "\n")
    head_sha = commit!(workspace, "change line 2")

    working_tree_lines = List.update_at(lines, 17, fn _line -> "uncommitted line 18" end)
    File.write!(path, Enum.join(working_tree_lines, "\n") <> "\n")

    assert {:error, _reason} =
             validate(Jason.encode!(review_result([finding(path, 18, 18)])), workspace, base_sha, head_sha)
  end

  defp create_repo(workspace) do
    git!(workspace, ["init", "--quiet"])
    git!(workspace, ["config", "user.name", "Review Test"])
    git!(workspace, ["config", "user.email", "review-test@example.test"])

    File.mkdir_p!(Path.dirname(Path.join(workspace, @literal_path)))
    File.write!(Path.join(workspace, @literal_path), @source_contents)
    File.write!(Path.join(workspace, "unchanged.txt"), "unchanged\n")
    File.write!(Path.join(workspace, "deleted.txt"), "deleted in later commit\n")
    File.write!(Path.join(workspace, "link.txt"), "plain file before symlink\n")
    File.write!(Path.join(workspace, "binary.dat"), <<0, 1, 2, 0>>)
    File.write!(Path.join(workspace, @long_path), Enum.join(long_lines(), "\n") <> "\n")

    git!(workspace, ["add", "--all"])
    git!(workspace, ["commit", "--quiet", "-m", "initial"])
  end

  defp change_literal_file(workspace, replacement_line) do
    path = Path.join(workspace, @literal_path)
    File.write!(path, "first\nsecond\n" <> replacement_line <> "fourth\nfifth\nsixth\n")
    commit!(workspace, "change reviewed file")
  end

  defp commit!(workspace, message) do
    git!(workspace, ["add", "--all"])
    git!(workspace, ["commit", "--quiet", "-m", message])
    git!(workspace, ["rev-parse", "HEAD"])
  end

  defp git!(workspace, args) do
    {output, 0} =
      System.cmd("git", ["-C", workspace | args],
        env: isolated_git_environment(),
        stderr_to_stdout: true
      )

    String.trim_trailing(output)
  end

  defp isolated_git_environment do
    System.get_env()
    |> Enum.flat_map(fn {key, _value} -> if String.starts_with?(key, "GIT_"), do: [{key, nil}], else: [] end)
    |> Kernel.++([{"GIT_CONFIG_GLOBAL", "/dev/null"}, {"GIT_CONFIG_NOSYSTEM", "1"}, {"GIT_TERMINAL_PROMPT", "0"}])
  end

  defp long_lines, do: Enum.map(1..20, fn line -> "line #{line}" end)

  defp sha256(text), do: :crypto.hash(:sha256, text) |> Base.encode16(case: :lower)

  defp review_result(findings \\ [], overrides \\ %{}) do
    Map.merge(
      %{
        "findings" => findings,
        "overall_correctness" => "patch is correct",
        "overall_explanation" => "The result was checked against the changed commits.",
        "overall_confidence_score" => 0.94
      },
      overrides
    )
  end

  defp finding(path), do: finding(path, 3, 3)

  defp finding(path, start_line, end_line) do
    %{
      "title" => "[P1] Check the changed line",
      "body" => "This changed behavior needs a guard before it is used.",
      "confidence_score" => 0.91,
      "priority" => 1,
      "code_location" => %{
        "absolute_file_path" => path,
        "line_range" => %{"start" => start_line, "end" => end_line}
      }
    }
  end

  defp recorded_revision(base_sha, head_sha) do
    revision(base_sha, head_sha)
    |> Map.put(:rubric_sha256, ReviewResult.rubric().sha256)
  end

  defp revision(base_sha, head_sha), do: %{base_sha: base_sha, head_sha: head_sha}

  defp validate(json, workspace, base_sha, head_sha) do
    call_validate(json, workspace, recorded_revision(base_sha, head_sha), revision(base_sha, head_sha))
  end

  defp call_validate(json, workspace, recorded, current) do
    ReviewResult.validate(json, recorded, current, workspace)
  end

  defp update_first_finding(result, fun) do
    Map.update!(result, "findings", fn [first | rest] -> [fun.(first) | rest] end)
  end

  defp update_location(result, fun) do
    update_first_finding(result, fn finding -> Map.update!(finding, "code_location", fun) end)
  end

  defp update_line_range(result, fun) do
    update_location(result, fn location -> Map.update!(location, "line_range", fun) end)
  end

  defp raw_result(findings) do
    ~s|{"findings":[#{Enum.join(findings, ",")}],"overall_correctness":"patch is correct","overall_explanation":"The result was checked against the changed commits.","overall_confidence_score":0.94}|
  end
end
