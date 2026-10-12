defmodule SymphonyElixir.RecoveryPlan do
  @moduledoc """
  Loads a small, operator-owned recovery inspection plan.

  File checks use `lstat` before and after a bounded read. They do not provide an
  atomic no-follow open against concurrent filesystem changes.
  """

  import Bitwise

  @max_plan_bytes 64 * 1024
  @plan_keys [
    "version",
    "mode",
    "root",
    "id",
    "workspace_roots",
    "manifest_sha256",
    "service_revision",
    "policy"
  ]
  @policy_keys [
    "version",
    "gate_policy",
    "lifecycle_sha256",
    "executor_sha256",
    "validation_sha256"
  ]
  @capture_id ~r/\A[A-Za-z0-9][A-Za-z0-9._-]{0,63}\z/
  @digest ~r/\A[0-9a-f]{64}\z/
  @revision ~r/\A[0-9a-f]{40}([0-9a-f]{24})?\z/

  @doc "Loads and validates an operator-provided recovery inspection plan."
  @spec load(Path.t()) :: {:ok, map()} | {:error, atom()}
  def load(path) when is_binary(path) do
    with {:ok, path} <- validate_path(path),
         {:ok, parent_stat} <- private_parent_stat(Path.dirname(path)),
         {:ok, file_stat} <- private_file_stat(path),
         :ok <- validate_file_size(file_stat.size),
         {:ok, bytes} <- read_bounded_file(path),
         true <- byte_size(bytes) == file_stat.size,
         :ok <- unchanged_path(path, parent_stat, file_stat),
         {:ok, ordered} <- Jason.decode(bytes, objects: :ordered_objects, strings: :copy),
         {:ok, decoded} <- reject_duplicate_keys(ordered),
         {:ok, config} <- plan_config(decoded),
         :ok <- SymphonyElixir.RecoveryInspection.validate_configuration(config) do
      {:ok, config}
    else
      false -> {:error, :invalid_recovery_plan}
      {:error, reason} when is_atom(reason) -> {:error, reason}
      _other -> {:error, :invalid_recovery_plan}
    end
  rescue
    _error -> {:error, :invalid_recovery_plan}
  catch
    _, _ -> {:error, :invalid_recovery_plan}
  end

  def load(_path), do: {:error, :invalid_recovery_plan_path}

  defp validate_path(path) do
    if Path.type(path) == :absolute and not String.contains?(path, <<0>>) and
         ".." not in Path.split(path) do
      {:ok, Path.expand(path)}
    else
      {:error, :invalid_recovery_plan_path}
    end
  end

  defp private_parent_stat(parent) do
    with :ok <- no_symlink_ancestry(parent),
         {:ok, %{type: :directory} = stat} <- File.lstat(parent),
         {:ok, uid} <- current_uid(),
         true <- stat.uid == uid,
         true <- band(stat.mode, 0o777) == 0o700 do
      {:ok, stat}
    else
      _other -> {:error, :unsafe_recovery_plan_path}
    end
  end

  defp private_file_stat(path) do
    with {:ok, %{type: :regular} = stat} <- File.lstat(path),
         {:ok, uid} <- current_uid(),
         true <- stat.uid == uid,
         true <- stat.links == 1,
         true <- band(stat.mode, 0o777) == 0o600 do
      {:ok, stat}
    else
      _other -> {:error, :unsafe_recovery_plan_file}
    end
  end

  defp validate_file_size(size) when is_integer(size) and size > 0 and size <= @max_plan_bytes,
    do: :ok

  defp validate_file_size(size) when is_integer(size) and size > @max_plan_bytes,
    do: {:error, :recovery_plan_too_large}

  defp validate_file_size(_size), do: {:error, :invalid_recovery_plan}

  defp read_bounded_file(path) do
    case :file.open(String.to_charlist(path), [:read, :binary, :raw]) do
      {:ok, file} ->
        try do
          read_chunks(file, 0, [])
        after
          _ = :file.close(file)
        end

      {:error, _reason} ->
        {:error, :recovery_plan_read_failed}
    end
  rescue
    _error -> {:error, :recovery_plan_read_failed}
  catch
    _, _ -> {:error, :recovery_plan_read_failed}
  end

  defp read_chunks(file, bytes_read, chunks) do
    requested = min(16 * 1024, @max_plan_bytes + 1 - bytes_read)

    case :file.read(file, requested) do
      :eof ->
        {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}

      {:ok, <<>>} ->
        {:error, :recovery_plan_read_failed}

      {:ok, chunk} ->
        next_size = bytes_read + byte_size(chunk)

        if next_size > @max_plan_bytes do
          {:error, :recovery_plan_too_large}
        else
          read_chunks(file, next_size, [chunk | chunks])
        end

      {:error, _reason} ->
        {:error, :recovery_plan_read_failed}
    end
  end

  defp unchanged_path(path, initial_parent_stat, initial_file_stat) do
    with {:ok, final_parent_stat} <- private_parent_stat(Path.dirname(path)),
         {:ok, final_file_stat} <- private_file_stat(path),
         true <- file_identity(initial_parent_stat) == file_identity(final_parent_stat),
         true <- file_identity(initial_file_stat) == file_identity(final_file_stat) do
      :ok
    else
      _other -> {:error, :recovery_plan_changed}
    end
  end

  defp file_identity(stat) do
    {
      stat.type,
      stat.links,
      stat.mode,
      stat.uid,
      stat.size,
      stat.inode,
      stat.major_device,
      stat.minor_device,
      stat.mtime,
      stat.ctime
    }
  end

  defp no_symlink_ancestry(path) do
    path
    |> path_prefixes()
    |> Enum.reduce_while(:ok, fn prefix, :ok ->
      case File.lstat(prefix) do
        {:ok, %{type: :directory}} -> {:cont, :ok}
        _other -> {:halt, {:error, :unsafe_recovery_plan_path}}
      end
    end)
  end

  defp path_prefixes(path) do
    path
    |> Path.split()
    |> Enum.reduce({[], ""}, fn component, {prefixes, parent} ->
      current = if component == "/", do: "/", else: Path.join(parent, component)
      {[current | prefixes], current}
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp current_uid do
    case System.cmd("/usr/bin/id", ["-u"], stderr_to_stdout: true) do
      {value, 0} ->
        case Integer.parse(String.trim(value)) do
          {uid, ""} -> {:ok, uid}
          _other -> {:error, :uid_unavailable}
        end

      _other ->
        {:error, :uid_unavailable}
    end
  rescue
    _error -> {:error, :uid_unavailable}
  end

  defp reject_duplicate_keys(%Jason.OrderedObject{values: pairs}) do
    Enum.reduce_while(pairs, {:ok, %{}, MapSet.new()}, fn
      {key, value}, {:ok, acc, seen} when is_binary(key) ->
        if MapSet.member?(seen, key) do
          {:halt, {:error, :duplicate_recovery_plan_key}}
        else
          case reject_duplicate_keys(value) do
            {:ok, decoded} -> {:cont, {:ok, Map.put(acc, key, decoded), MapSet.put(seen, key)}}
            error -> {:halt, error}
          end
        end

      _other, _acc ->
        {:halt, {:error, :invalid_recovery_plan}}
    end)
    |> case do
      {:ok, map, _seen} -> {:ok, map}
      error -> error
    end
  end

  defp reject_duplicate_keys(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case reject_duplicate_keys(value) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      error -> error
    end
  end

  defp reject_duplicate_keys(value) when is_map(value), do: {:ok, value}
  defp reject_duplicate_keys(value), do: {:ok, value}

  defp plan_config(plan) when is_map(plan) do
    with true <- exact_keys?(plan, @plan_keys),
         true <- plan["version"] == 1,
         true <- plan["mode"] == "held_inspection",
         true <- clean_absolute_path?(plan["root"]),
         true <- valid_capture_id?(plan["id"]),
         true <- valid_workspace_roots?(plan["workspace_roots"]),
         true <- digest?(plan["manifest_sha256"]),
         true <- revision?(plan["service_revision"]),
         {:ok, policy} <- policy_config(plan["policy"]) do
      {:ok,
       %{
         root: plan["root"],
         id: plan["id"],
         workspace_roots: plan["workspace_roots"],
         manifest_sha256: plan["manifest_sha256"],
         service_revision: plan["service_revision"],
         policy: policy
       }}
    else
      _other -> {:error, :invalid_recovery_plan}
    end
  end

  defp plan_config(_plan), do: {:error, :invalid_recovery_plan}

  defp policy_config(policy) when is_map(policy) do
    with true <- exact_keys?(policy, @policy_keys) or exact_keys?(policy, ["child_sha256" | @policy_keys]),
         true <- policy["version"] == 1,
         true <- policy["gate_policy"] == "exit-status-v1",
         true <- digest?(policy["lifecycle_sha256"]),
         true <- digest?(policy["executor_sha256"]),
         true <- digest?(policy["validation_sha256"]),
         true <- not Map.has_key?(policy, "child_sha256") or digest?(policy["child_sha256"]) do
      {:ok,
       %{
         version: 1,
         gate_policy: "exit-status-v1",
         lifecycle_sha256: policy["lifecycle_sha256"],
         executor_sha256: policy["executor_sha256"],
         validation_sha256: policy["validation_sha256"]
       }
       |> maybe_child_policy(policy)}
    else
      _other -> {:error, :invalid_recovery_plan}
    end
  end

  defp policy_config(_policy), do: {:error, :invalid_recovery_plan}

  defp maybe_child_policy(decoded, %{"child_sha256" => sha}), do: Map.put(decoded, :child_sha256, sha)
  defp maybe_child_policy(decoded, _policy), do: decoded

  defp valid_workspace_roots?(roots) when is_list(roots) and roots != [],
    do: Enum.all?(roots, &clean_absolute_path?/1)

  defp valid_workspace_roots?(_roots), do: false

  defp clean_absolute_path?(path) when is_binary(path) do
    Path.type(path) == :absolute and not String.contains?(path, <<0>>) and
      ".." not in Path.split(path)
  end

  defp clean_absolute_path?(_path), do: false

  defp valid_capture_id?(id) when is_binary(id), do: Regex.match?(@capture_id, id)
  defp valid_capture_id?(_id), do: false

  defp digest?(value) when is_binary(value), do: Regex.match?(@digest, value)
  defp digest?(_value), do: false

  defp revision?(value) when is_binary(value), do: Regex.match?(@revision, value)
  defp revision?(_value), do: false

  defp exact_keys?(map, expected) when is_map(map),
    do: map_size(map) == length(expected) and Enum.all?(expected, &Map.has_key?(map, &1))

  defp exact_keys?(_map, _expected), do: false
end
