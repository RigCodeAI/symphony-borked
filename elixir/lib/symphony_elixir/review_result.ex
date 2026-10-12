defmodule SymphonyElixir.ReviewResult do
  @moduledoc """
  Validates the pinned review rubric's JSON against service-recorded revisions and
  committed head-side diff locations. This does not authorize publication or merge.

  The caller supplies the recorded review identity and an independently reconciled
  current identity. Neither identity comes from the model's result. The workspace
  must be a separate clone checked out at the recorded head. Candidate Git config,
  diff drivers and working-tree file contents are not trusted.
  """

  alias SymphonyElixir.CandidateGit

  @rubric_path Path.expand("../../../factory/review/rubric.md", __DIR__)
  @external_resource @rubric_path
  @rubric_text File.read!(@rubric_path)
  @rubric_pin %{
    commit: "5fe4fc8f7cd16688b5c661d8cdb76e2a340b046d",
    git_blob: "85e89cb7eeadb97fa9e4868d2b9977c87225506d",
    sha256: "ec60e7f36a1d1c2679ce095c0205ecc56f7dd8fb57707a13ef362072390f219f",
    path: "factory/review/rubric.md"
  }

  @rubric_sha256 :crypto.hash(:sha256, @rubric_text) |> Base.encode16(case: :lower)
  @rubric_blob :crypto.hash(:sha, "blob #{byte_size(@rubric_text)}\0" <> @rubric_text) |> Base.encode16(case: :lower)
  if @rubric_sha256 != @rubric_pin.sha256 or @rubric_blob != @rubric_pin.git_blob do
    raise "review rubric does not match its pinned upstream resource"
  end

  @max_json_bytes 1_048_576
  @result_keys ~w(findings overall_correctness overall_explanation overall_confidence_score)
  @finding_keys ~w(title body confidence_score code_location)
  @hunk_header ~r/^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@/

  @type revision :: %{base_sha: String.t(), head_sha: String.t()}
  @type recorded_revision :: %{base_sha: String.t(), head_sha: String.t(), rubric_sha256: String.t()}
  @type location :: %{path: String.t(), start_line: pos_integer(), end_line: pos_integer(), side: :right}
  @type validated_result :: %{
          base_sha: String.t(),
          head_sha: String.t(),
          rubric: map(),
          result: map(),
          locations: [location()]
        }

  @doc "Returns the verified, embedded upstream rubric and its immutable identities."
  @spec rubric() :: map()
  def rubric, do: Map.put(@rubric_pin, :text, @rubric_text)

  @doc """
  Rejects stale identities, malformed output, and any finding without a valid inline location.

  Locations use head-side coordinates, lie within one rendered diff hunk and overlap
  an added line. Deleted-only, binary, symlink and gitlink findings cannot be placed
  unambiguously from this rubric's output and return an error. No finding is dropped.
  A successful return validates structure and locations, not the review's reasoning
  or trusted candidate validation. The original correctness verdict is preserved.
  """
  @spec validate(binary(), recorded_revision(), revision(), Path.t()) :: {:ok, validated_result()} | {:error, term()}
  def validate(json, recorded, current, workspace) do
    with :ok <- identities(recorded, current),
         {:ok, workspace} <- workspace_path(workspace),
         {:ok, result} <- decode(json),
         :ok <- result_shape(result),
         :ok <- commits(workspace, recorded),
         {:ok, locations} <- locations(result["findings"], workspace, recorded) do
      validated = %{
        base_sha: recorded.base_sha,
        head_sha: recorded.head_sha,
        rubric: @rubric_pin,
        result: result,
        locations: locations
      }

      {:ok, validated}
    end
  end

  defp identities(recorded, current) when is_map(recorded) and is_map(current) do
    cond do
      not revision?(recorded) or not revision?(current) -> {:error, :invalid_review_identity}
      Map.get(recorded, :rubric_sha256) != @rubric_pin.sha256 -> {:error, :invalid_review_rubric}
      recorded.base_sha != current.base_sha or recorded.head_sha != current.head_sha -> {:error, :stale_review}
      true -> :ok
    end
  end

  defp identities(_recorded, _current), do: {:error, :invalid_review_identity}

  defp revision?(revision) do
    base = Map.get(revision, :base_sha)
    head = Map.get(revision, :head_sha)
    sha?(base) and sha?(head) and byte_size(base) == byte_size(head)
  end

  defp sha?(value) when is_binary(value), do: Regex.match?(~r/\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/, value)
  defp sha?(_value), do: false

  defp workspace_path(workspace) when is_binary(workspace) do
    if String.valid?(workspace) and Path.type(workspace) == :absolute and workspace != "/" and not control?(workspace) do
      {:ok, Path.expand(workspace)}
    else
      {:error, :invalid_review_workspace}
    end
  end

  defp workspace_path(_workspace), do: {:error, :invalid_review_workspace}

  defp decode(json) when is_binary(json) and byte_size(json) <= @max_json_bytes do
    case Jason.decode(json, objects: :ordered_objects) do
      {:ok, value} -> unique_objects(value)
      {:error, _reason} -> {:error, :malformed_review_json}
    end
  end

  defp decode(_json), do: {:error, :invalid_review_json}

  defp unique_objects(%Jason.OrderedObject{values: pairs}) do
    keys = Enum.map(pairs, &elem(&1, 0))

    if length(keys) == MapSet.size(MapSet.new(keys)) do
      Enum.reduce_while(pairs, {:ok, %{}}, &unique_pair/2)
    else
      {:error, :duplicate_review_json_key}
    end
  end

  defp unique_objects(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case unique_objects(value) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp unique_objects(value), do: {:ok, value}

  defp unique_pair({key, value}, {:ok, acc}) do
    case unique_objects(value) do
      {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp result_shape(result) do
    with :ok <- fields(result, @result_keys, []),
         true <- is_list(result["findings"]),
         true <- result["overall_correctness"] in ["patch is correct", "patch is incorrect"],
         true <- text?(result["overall_explanation"]),
         true <- confidence?(result["overall_confidence_score"]) do
      finding_shapes(result["findings"])
    else
      _ -> {:error, :invalid_review_result}
    end
  end

  defp finding_shapes(findings) do
    findings
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {finding, index}, :ok ->
      case finding_shape(finding) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:invalid_review_finding, index, reason}}}
      end
    end)
  end

  defp finding_shape(finding) do
    with :ok <- fields(finding, @finding_keys, ["priority"]),
         true <- text?(finding["title"]) and not control?(finding["title"]) and String.length(finding["title"]) <= 80,
         [_, priority] <- Regex.run(~r/\A\[P([0-3])\] \S/, finding["title"]),
         true <- Map.get(finding, "priority") in [nil, String.to_integer(priority)],
         true <- is_nil(finding["priority"]) or is_integer(finding["priority"]),
         true <- text?(finding["body"]) and confidence?(finding["confidence_score"]),
         :ok <- fields(finding["code_location"], ~w(absolute_file_path line_range), []),
         true <- text?(finding["code_location"]["absolute_file_path"]),
         :ok <- fields(finding["code_location"]["line_range"], ~w(start end), []),
         %{"start" => first, "end" => last} <- finding["code_location"]["line_range"],
         true <- is_integer(first) and is_integer(last) and first > 0 and last >= first do
      :ok
    else
      _ -> {:error, :invalid_shape}
    end
  end

  defp fields(value, required, optional) when is_map(value) do
    keys = Map.keys(value)
    if required -- keys == [] and keys -- (required ++ optional) == [], do: :ok, else: {:error, :invalid_fields}
  end

  defp fields(_value, _required, _optional), do: {:error, :invalid_fields}
  defp text?(value) when is_binary(value), do: String.valid?(value) and String.trim(value) != "" and not Regex.match?(~r/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/, value)
  defp text?(_value), do: false
  defp confidence?(value), do: is_number(value) and value >= 0 and value <= 1
  defp control?(value), do: Regex.match?(~r/[\x00-\x1f\x7f]/, value)

  defp commits(workspace, recorded) do
    with :ok <- commit(workspace, recorded.base_sha),
         :ok <- commit(workspace, recorded.head_sha),
         {:ok, head} <- CandidateGit.run(workspace, ["rev-parse", "--verify", "HEAD^{commit}"]),
         true <- head == recorded.head_sha,
         {:ok, _merge_base} <- CandidateGit.run(workspace, ["merge-base", recorded.base_sha, recorded.head_sha]) do
      :ok
    else
      false -> {:error, :review_checkout_head_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp commit(workspace, sha) do
    case CandidateGit.run(workspace, ["rev-parse", "--verify", sha <> "^{commit}"]) do
      {:ok, ^sha} -> :ok
      {:ok, _other} -> {:error, :review_identity_not_commit}
      {:error, _reason} = error -> error
    end
  end

  defp locations(findings, workspace, recorded) do
    Enum.reduce_while(Enum.with_index(findings), {:ok, [], %{}}, fn {finding, index}, {:ok, acc, cache} ->
      range = finding["code_location"]["line_range"]

      with {:ok, path} <- relative_path(finding["code_location"]["absolute_file_path"], workspace),
           {:ok, hunks} <- file_hunks(path, workspace, recorded, cache),
           true <- Enum.any?(hunks, &placeable?(&1, range)) do
        location = %{path: path, start_line: range["start"], end_line: range["end"], side: :right}
        {:cont, {:ok, [location | acc], Map.put(cache, path, hunks)}}
      else
        false -> {:halt, {:error, {:unplaceable_review_finding, index}}}
        {:error, reason} -> {:halt, {:error, {:invalid_review_location, index, reason}}}
      end
    end)
    |> case do
      {:ok, locations, _cache} -> {:ok, Enum.reverse(locations)}
      error -> error
    end
  end

  defp relative_path(file, workspace) do
    if Path.type(file) == :absolute and not control?(file) and
         String.starts_with?(file, workspace <> "/") and
         not Enum.any?(Path.split(file), &(&1 in [".", ".."])) and Path.expand(file) == file do
      {:ok, Path.relative_to(file, workspace)}
    else
      {:error, :review_path_outside_workspace}
    end
  end

  defp file_hunks(path, workspace, recorded, cache) do
    case Map.fetch(cache, path) do
      {:ok, hunks} -> {:ok, hunks}
      :error -> read_hunks(path, workspace, recorded)
    end
  end

  defp read_hunks(path, workspace, recorded) do
    # CandidateGit appends a diff option. Terminate path arguments explicitly and
    # verify the selected file set, so extra paths cannot supply another file's hunks.
    selection = [recorded.base_sha <> "..." <> recorded.head_sha, "--", ":(literal)" <> path]

    with {:ok, entry} <- CandidateGit.run(workspace, ["ls-tree", "-z", recorded.head_sha, "--", ":(literal)" <> path]),
         {:ok, blob} <- regular_blob(entry, path),
         {:ok, content} <- CandidateGit.run(workspace, ["cat-file", "-p", blob]),
         false <- String.contains?(content, <<0>>),
         {:ok, selected} <- CandidateGit.run(workspace, ["diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--name-only", "-z" | selection]),
         true <- selected == path <> "\0",
         {:ok, diff} <- CandidateGit.run(workspace, ["diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--unified=3" | selection]) do
      parse_hunks(diff)
    else
      value when is_boolean(value) -> {:error, :unplaceable_review_path}
      {:error, _reason} = error -> error
    end
  end

  defp regular_blob(entry, path) do
    case :binary.split(entry, "\t") do
      [metadata, file] when file == path <> "\0" ->
        regular_blob_metadata(metadata)

      _ ->
        {:error, :review_path_not_regular_head_file}
    end
  end

  defp regular_blob_metadata(metadata) do
    case String.split(metadata, " ") do
      [mode, "blob", oid] when mode in ["100644", "100755"] ->
        if sha?(oid), do: {:ok, oid}, else: {:error, :invalid_review_blob}

      _ ->
        {:error, :review_path_not_regular_head_file}
    end
  end

  defp parse_hunks(diff) do
    diff
    |> String.split("\n")
    |> Enum.reduce_while({:ok, [], nil}, &parse_hunk_line/2)
    |> case do
      {:ok, hunks, current} -> finish_hunk(hunks, current)
      error -> error
    end
  end

  defp parse_hunk_line(line, {:ok, hunks, current}) do
    case Regex.run(@hunk_header, line, capture: :all_but_first) do
      nil -> hunk_line(line, hunks, current)
      captures -> replace_hunk(captures, hunks, current)
    end
  end

  defp replace_hunk(captures, hunks, current) do
    case finish_hunk(hunks, current) do
      {:ok, hunks} -> start_hunk(captures, hunks)
      error -> {:halt, error}
    end
  end

  defp start_hunk([first | count], hunks) do
    first = String.to_integer(first)
    count = if count in [[], [""]], do: 1, else: String.to_integer(hd(count))
    {:cont, {:ok, hunks, %{first: first, last: first + count - 1, next: first, added: []}}}
  end

  defp hunk_line(_line, hunks, nil), do: {:cont, {:ok, hunks, nil}}
  defp hunk_line("+" <> _line, hunks, current), do: {:cont, {:ok, hunks, %{current | next: current.next + 1, added: [current.next | current.added]}}}
  defp hunk_line(" " <> _line, hunks, current), do: {:cont, {:ok, hunks, %{current | next: current.next + 1}}}
  defp hunk_line("-" <> _line, hunks, current), do: {:cont, {:ok, hunks, current}}
  defp hunk_line("\\ No newline at end of file", hunks, current), do: {:cont, {:ok, hunks, current}}
  defp hunk_line(_line, _hunks, _current), do: {:halt, {:error, :invalid_review_diff}}

  defp finish_hunk(hunks, nil), do: {:ok, hunks}

  defp finish_hunk(hunks, current) do
    if current.next == current.last + 1, do: {:ok, [current | hunks]}, else: {:error, :invalid_review_diff}
  end

  defp placeable?(hunk, range) do
    range["start"] >= hunk.first and range["end"] <= hunk.last and
      Enum.any?(hunk.added, &(&1 >= range["start"] and &1 <= range["end"]))
  end
end
