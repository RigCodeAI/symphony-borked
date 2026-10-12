defmodule SymphonyElixir.CandidateGit do
  @moduledoc false

  @read_only_commands ~w(archive cat-file diff ls-tree merge-base rev-parse show show-ref status symbolic-ref)
  @max_metadata_bytes 512 * 1024 * 1024
  @max_metadata_entries 100_000
  @max_metadata_depth 64
  @max_shallow_bytes 65_536
  @copy_chunk_bytes 65_536

  @spec run(Path.t(), [String.t()]) :: {:ok, binary()} | {:error, term()}
  def run(workspace, args) when is_binary(workspace) and is_list(args) do
    with {:ok, command} <- validate_args(args),
         {:ok, workspace} <- SymphonyElixir.PathSafety.canonicalize(workspace),
         :ok <- validate_workspace(workspace),
         {:ok, source_git_dir} <- candidate_git_dir(workspace),
         :ok <- reject_metadata_links(source_git_dir),
         {:ok, snapshot} <- create_private_directory() do
      try do
        with :ok <- copy_metadata(source_git_dir, snapshot),
             {:ok, object_format} <- object_format(snapshot),
             :ok <- write_safe_config(snapshot, workspace, object_format) do
          run_git(snapshot, workspace, args, command)
        end
      after
        File.rm_rf(snapshot)
      end
    end
  end

  def run(_workspace, _args), do: {:error, :invalid_candidate_git_arguments}

  @doc "Runs trusted service code with a private metadata copy that excludes candidate config and hooks."
  @spec with_snapshot(Path.t(), (Path.t() -> term())) :: term()
  def with_snapshot(workspace, callback) when is_binary(workspace) and is_function(callback, 1) do
    with {:ok, workspace} <- SymphonyElixir.PathSafety.canonicalize(workspace),
         :ok <- validate_workspace(workspace),
         {:ok, source_git_dir} <- candidate_git_dir(workspace),
         :ok <- reject_metadata_links(source_git_dir),
         {:ok, snapshot} <- create_private_directory() do
      try do
        with :ok <- copy_metadata(source_git_dir, snapshot),
             {:ok, object_format} <- object_format(snapshot),
             :ok <- write_safe_config(snapshot, workspace, object_format) do
          callback.(snapshot)
        end
      after
        File.rm_rf(snapshot)
      end
    end
  end

  def with_snapshot(_workspace, _callback), do: {:error, :invalid_candidate_git_arguments}

  defp validate_args([command | args]) when command in @read_only_commands do
    if Enum.all?(args, &is_binary/1), do: {:ok, command}, else: {:error, :invalid_candidate_git_arguments}
  end

  defp validate_args(_args), do: {:error, :invalid_candidate_git_command}

  defp validate_workspace(workspace) do
    case File.lstat(workspace) do
      {:ok, %File.Stat{type: :directory}} -> :ok
      _ -> {:error, :candidate_git_workspace_missing}
    end
  end

  defp candidate_git_dir(workspace) do
    git_dir = Path.join(workspace, ".git")

    case File.lstat(git_dir) do
      {:ok, %File.Stat{type: :directory}} -> {:ok, git_dir}
      {:ok, %File.Stat{type: :symlink}} -> {:error, :candidate_git_metadata_symlink}
      _ -> {:error, :candidate_git_directory_required}
    end
  end

  defp reject_metadata_links(path) do
    case scan_metadata(path, 0, %{bytes: 0, entries: 0}) do
      {:ok, _usage} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp scan_metadata(path, depth, usage) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :symlink}} ->
        {:error, :candidate_git_metadata_symlink}

      {:ok, stat} when stat.type in [:regular, :directory] ->
        with {:ok, usage} <- account_metadata_entry(usage, stat, depth) do
          if stat.type == :directory do
            with {:ok, entries} <- File.ls(path),
                 :ok <- ensure_child_count(usage, length(entries)) do
              Enum.reduce_while(entries, {:ok, usage}, fn entry, {:ok, acc} ->
                case scan_metadata(Path.join(path, entry), depth + 1, acc) do
                  {:ok, next} -> {:cont, {:ok, next}}
                  {:error, _} = error -> {:halt, error}
                end
              end)
            end
          else
            {:ok, usage}
          end
        end

      {:ok, _stat} ->
        {:error, :invalid_candidate_git_metadata}

      {:error, reason} ->
        {:error, {:candidate_git_metadata_read_failed, reason}}
    end
  end

  defp account_metadata_entry(usage, stat, depth) do
    bytes = if stat.type == :regular, do: stat.size, else: 0

    if depth > @max_metadata_depth or usage.entries >= @max_metadata_entries or
         usage.bytes + bytes > @max_metadata_bytes do
      {:error, :candidate_git_metadata_limit_exceeded}
    else
      {:ok, %{usage | bytes: usage.bytes + bytes, entries: usage.entries + 1}}
    end
  end

  defp ensure_child_count(usage, count) do
    if usage.entries + count > @max_metadata_entries do
      {:error, :candidate_git_metadata_limit_exceeded}
    else
      :ok
    end
  end

  defp create_private_directory do
    name = "symphony-candidate-git-" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    path = Path.join(System.tmp_dir!(), name)

    with :ok <- File.mkdir(path),
         :ok <- File.chmod(path, 0o700) do
      {:ok, path}
    else
      {:error, reason} ->
        File.rm_rf(path)
        {:error, {:candidate_git_temporary_directory_failed, reason}}
    end
  end

  defp copy_metadata(source_git_dir, snapshot) do
    usage = %{bytes: 0, entries: 0}

    with {:ok, usage} <- copy_required(Path.join(source_git_dir, "HEAD"), Path.join(snapshot, "HEAD"), usage),
         {:ok, usage} <- copy_optional(Path.join(source_git_dir, "refs"), Path.join(snapshot, "refs"), usage),
         {:ok, usage} <- copy_optional(Path.join(source_git_dir, "packed-refs"), Path.join(snapshot, "packed-refs"), usage),
         {:ok, usage} <- copy_shallow(source_git_dir, snapshot, usage),
         {:ok, usage} <- copy_optional(Path.join(source_git_dir, "index"), Path.join(snapshot, "index"), usage),
         {:ok, _usage} <- copy_objects(Path.join(source_git_dir, "objects"), Path.join(snapshot, "objects"), usage) do
      :ok
    end
  end

  defp copy_shallow(source_git_dir, snapshot, usage) do
    source = Path.join(source_git_dir, "shallow")
    destination = Path.join(snapshot, "shallow")

    case File.lstat(source) do
      {:error, :enoent} ->
        {:ok, usage}

      {:ok, %{type: :regular, size: size}} when size <= @max_shallow_bytes ->
        with {:ok, usage} <- copy_entry(source, destination, false, usage, 1, @max_shallow_bytes),
             {:ok, contents} <- File.read(destination),
             boundaries = String.split(contents, "\n", trim: true),
             true <- boundaries != [] and Enum.all?(boundaries, &(oid_format(&1) in [:sha1, :sha256])) do
          {:ok, usage}
        else
          _ -> {:error, :invalid_candidate_shallow_boundaries}
        end

      _ ->
        {:error, :invalid_candidate_shallow_boundaries}
    end
  end

  defp copy_required(source, destination, usage), do: copy_entry(source, destination, false, usage, 1)

  defp copy_optional(source, destination, usage) do
    case File.lstat(source) do
      {:error, :enoent} -> {:ok, usage}
      {:ok, _stat} -> copy_entry(source, destination, false, usage, 1)
      {:error, reason} -> {:error, {:candidate_git_metadata_read_failed, reason}}
    end
  end

  defp copy_objects(source, destination, usage) do
    case File.lstat(source) do
      {:ok, %File.Stat{type: :directory}} -> copy_entry(source, destination, true, usage, 1)
      {:ok, %File.Stat{type: :symlink}} -> {:error, :candidate_git_metadata_symlink}
      _ -> {:error, :candidate_git_objects_required}
    end
  end

  defp copy_entry(source, destination, skip_object_info?, usage, depth, max_file_bytes \\ @max_metadata_bytes) do
    case File.lstat(source) do
      {:ok, %File.Stat{type: :symlink}} ->
        {:error, :candidate_git_metadata_symlink}

      {:ok, %File.Stat{type: :regular} = stat} ->
        with {:ok, next_usage} <- account_copy_entry(usage, depth),
             {:ok, next_usage} <- copy_regular_file(source, destination, next_usage, stat.size, max_file_bytes) do
          {:ok, next_usage}
        end

      {:ok, %File.Stat{type: :directory}} ->
        with {:ok, next_usage} <- account_copy_entry(usage, depth),
             :ok <- File.mkdir_p(destination),
             {:ok, entries} <- File.ls(source),
             copied_entries <- if(skip_object_info? and Path.basename(source) == "objects", do: Enum.reject(entries, &(&1 == "info")), else: entries),
             :ok <- ensure_child_count(next_usage, length(copied_entries)) do
          Enum.reduce_while(copied_entries, {:ok, next_usage}, fn entry, {:ok, acc} ->
            case copy_entry(Path.join(source, entry), Path.join(destination, entry), skip_object_info?, acc, depth + 1) do
              {:ok, next} -> {:cont, {:ok, next}}
              {:error, _} = error -> {:halt, error}
            end
          end)
        end

      {:ok, _stat} ->
        {:error, :invalid_candidate_git_metadata}

      {:error, reason} ->
        {:error, {:candidate_git_metadata_copy_failed, reason}}
    end
  end

  defp account_copy_entry(usage, depth) do
    if depth > @max_metadata_depth or usage.entries >= @max_metadata_entries do
      {:error, :candidate_git_metadata_limit_exceeded}
    else
      {:ok, %{usage | entries: usage.entries + 1}}
    end
  end

  defp copy_regular_file(source, destination, usage, expected_size, max_file_bytes) do
    cond do
      expected_size > max_file_bytes or usage.bytes + expected_size > @max_metadata_bytes ->
        {:error, :candidate_git_metadata_limit_exceeded}

      true ->
        with {:ok, input} <- File.open(source, [:read, :binary]) do
          try do
            with {:ok, output} <- File.open(destination, [:write, :binary, :exclusive]) do
              try do
                copy_file_chunks(input, output, usage, expected_size, 0)
              after
                File.close(output)
              end
            else
              {:error, reason} -> {:error, {:candidate_git_metadata_copy_failed, reason}}
            end
          after
            File.close(input)
          end
        else
          {:error, reason} -> {:error, {:candidate_git_metadata_copy_failed, reason}}
        end
    end
  end

  defp copy_file_chunks(input, output, usage, expected_size, copied_bytes) do
    case IO.binread(input, @copy_chunk_bytes) do
      :eof when copied_bytes == expected_size ->
        {:ok, %{usage | bytes: usage.bytes + copied_bytes}}

      :eof ->
        {:error, :candidate_git_metadata_changed_during_copy}

      {:error, reason} ->
        {:error, {:candidate_git_metadata_copy_failed, reason}}

      chunk when is_binary(chunk) ->
        next_bytes = copied_bytes + byte_size(chunk)

        cond do
          next_bytes > expected_size ->
            {:error, :candidate_git_metadata_changed_during_copy}

          usage.bytes + next_bytes > @max_metadata_bytes ->
            {:error, :candidate_git_metadata_limit_exceeded}

          true ->
            case IO.binwrite(output, chunk) do
              :ok -> copy_file_chunks(input, output, usage, expected_size, next_bytes)
              {:error, reason} -> {:error, {:candidate_git_metadata_copy_failed, reason}}
            end
        end
    end
  end

  defp object_format(snapshot) do
    with {:ok, ref_formats} <- ref_formats(snapshot),
         {:ok, object_formats} <- object_path_formats(Path.join(snapshot, "objects")) do
      formats = Enum.uniq(ref_formats ++ object_formats)

      case formats do
        [] -> {:ok, :sha1}
        [:sha1] -> {:ok, :sha1}
        [:sha256] -> {:ok, :sha256}
        _ -> {:error, :candidate_git_object_format_mismatch}
      end
    end
  end

  defp ref_formats(snapshot) do
    paths = [Path.join(snapshot, "HEAD"), Path.join(snapshot, "packed-refs")]

    with {:ok, ref_paths} <- regular_files(Path.join(snapshot, "refs")) do
      read_ref_formats(paths ++ ref_paths, [])
    end
  end

  defp read_ref_formats([], formats), do: {:ok, formats}

  defp read_ref_formats([path | rest], formats) do
    case File.read(path) do
      {:ok, contents} -> read_ref_formats(rest, formats ++ formats_in_ref(contents))
      {:error, :enoent} -> read_ref_formats(rest, formats)
      {:error, reason} -> {:error, {:candidate_git_metadata_read_failed, reason}}
    end
  end

  defp formats_in_ref(contents) do
    contents
    |> :binary.split("\n", [:global])
    |> Enum.flat_map(fn line ->
      oid = line |> :binary.split(" ") |> hd()

      case oid_format(oid) do
        nil -> []
        format -> [format]
      end
    end)
  end

  defp regular_files(path) do
    case File.lstat(path) do
      {:error, :enoent} ->
        {:ok, []}

      {:ok, %File.Stat{type: :regular}} ->
        {:ok, [path]}

      {:ok, %File.Stat{type: :directory}} ->
        with {:ok, entries} <- File.ls(path) do
          Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, paths} ->
            case regular_files(Path.join(path, entry)) do
              {:ok, nested_paths} -> {:cont, {:ok, paths ++ nested_paths}}
              {:error, _} = error -> {:halt, error}
            end
          end)
        end

      {:ok, %File.Stat{type: :symlink}} ->
        {:error, :candidate_git_metadata_symlink}

      {:ok, _stat} ->
        {:error, :invalid_candidate_git_metadata}

      {:error, reason} ->
        {:error, {:candidate_git_metadata_read_failed, reason}}
    end
  end

  defp object_path_formats(objects_dir) do
    with {:ok, paths} <- object_files(objects_dir, []) do
      formats = Enum.flat_map(paths, &format_from_object_path(objects_dir, &1))
      {:ok, formats}
    end
  end

  defp object_files(path, paths) do
    case File.lstat(path) do
      {:error, :enoent} ->
        {:ok, paths}

      {:ok, %File.Stat{type: :regular}} ->
        {:ok, [path | paths]}

      {:ok, %File.Stat{type: :directory}} ->
        with {:ok, entries} <- File.ls(path) do
          Enum.reduce_while(entries, {:ok, paths}, fn entry, {:ok, acc} ->
            if path == Path.join(Path.dirname(path), "objects") and entry == "info" do
              {:cont, {:ok, acc}}
            else
              case object_files(Path.join(path, entry), acc) do
                {:ok, nested_paths} -> {:cont, {:ok, nested_paths}}
                {:error, _} = error -> {:halt, error}
              end
            end
          end)
        end

      {:ok, %File.Stat{type: :symlink}} ->
        {:error, :candidate_git_metadata_symlink}

      {:ok, _stat} ->
        {:error, :invalid_candidate_git_metadata}

      {:error, reason} ->
        {:error, {:candidate_git_metadata_read_failed, reason}}
    end
  end

  defp format_from_object_path(objects_dir, path) do
    relative = Path.relative_to(path, objects_dir) |> Path.split()

    case relative do
      [prefix, suffix] when byte_size(prefix) == 2 ->
        case oid_format(prefix <> suffix) do
          nil -> []
          format -> [format]
        end

      ["pack", filename] ->
        filename
        |> String.replace_prefix("pack-", "")
        |> String.replace_suffix(".pack", "")
        |> String.replace_suffix(".idx", "")
        |> oid_format_list()

      _ ->
        []
    end
  end

  defp oid_format_list(oid) do
    case oid_format(oid) do
      nil -> []
      format -> [format]
    end
  end

  defp oid_format(oid) when byte_size(oid) == 40 do
    if lowercase_hex?(oid), do: :sha1, else: nil
  end

  defp oid_format(oid) when byte_size(oid) == 64 do
    if lowercase_hex?(oid), do: :sha256, else: nil
  end

  defp oid_format(_oid), do: nil

  defp lowercase_hex?(value) do
    for <<byte <- value>>, reduce: true do
      valid? -> valid? and (byte in ?0..?9 or byte in ?a..?f)
    end
  end

  defp write_safe_config(snapshot, _workspace, object_format) do
    config = """
    [core]
    repositoryformatversion = #{if(object_format == :sha256, do: 1, else: 0)}
    bare = false
    fsmonitor = false
    trustctime = true
    checkstat = default
    hookspath = /dev/null
    attributesfile = /dev/null
    pager = cat
    [protocol]
    allow = never
    [submodule]
    recurse = false
    [diff]
    ignoresubmodules = all
    #{object_format_config(object_format)}
    """

    case File.write(Path.join(snapshot, "config"), config) do
      :ok -> :ok
      {:error, reason} -> {:error, {:candidate_git_config_write_failed, reason}}
    end
  end

  defp object_format_config(:sha1), do: ""
  defp object_format_config(:sha256), do: "[extensions]\n\tobjectFormat = sha256\n"

  defp run_git(snapshot, workspace, args, command) do
    args = if command in ["status", "diff"], do: args ++ ["--ignore-submodules=all"], else: args

    git_args = [
      "--no-replace-objects",
      "--no-pager",
      "--no-optional-locks",
      "--git-dir=" <> snapshot,
      "--work-tree=" <> workspace
      | args
    ]

    case System.cmd("git", git_args, cd: workspace, env: git_environment(), stderr_to_stdout: true) do
      {output, 0} -> {:ok, trim_newlines(output)}
      {output, status} -> {:error, {:candidate_git_command_failed, command, status, trim_newlines(output)}}
    end
  rescue
    error in ErlangError ->
      {:error, {:candidate_git_execution_failed, command, Exception.message(error)}}
  end

  defp git_environment do
    locked = [
      {"GIT_CONFIG_COUNT", "0"},
      {"GIT_CONFIG_GLOBAL", "/dev/null"},
      {"GIT_CONFIG_SYSTEM", "/dev/null"},
      {"GIT_CONFIG_NOSYSTEM", "1"},
      {"GIT_ATTR_NOSYSTEM", "1"},
      {"GIT_OPTIONAL_LOCKS", "0"},
      {"GIT_TERMINAL_PROMPT", "0"},
      {"GIT_PAGER", "cat"},
      {"GIT_NO_LAZY_FETCH", "1"}
    ]

    locked_keys = MapSet.new(Enum.map(locked, &elem(&1, 0)))

    unset =
      System.get_env()
      |> Enum.flat_map(fn {key, _value} ->
        if String.starts_with?(key, "GIT_") and not MapSet.member?(locked_keys, key), do: [{key, nil}], else: []
      end)

    unset ++ locked
  end

  defp trim_newlines(output), do: trim_newlines(output, byte_size(output))

  defp trim_newlines(output, 0), do: output

  defp trim_newlines(output, size) do
    case :binary.at(output, size - 1) do
      byte when byte in [?\n, ?\r] -> trim_newlines(binary_part(output, 0, size - 1), size - 1)
      _byte -> output
    end
  end
end
