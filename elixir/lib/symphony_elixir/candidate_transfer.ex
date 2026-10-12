defmodule SymphonyElixir.CandidateTransfer do
  @moduledoc false

  alias SymphonyElixir.{CandidateGit, PathSafety, WorkerOperation}

  @candidate_output "candidate-commit"
  @producer_keys ~w(candidate_commit base_commit candidate_input_sha256 branch candidate_bundle bundle_sha256 bundle_bytes)
  @bundle_limit 64 * 1024 * 1024
  @candidate_file_limit 64 * 1024 * 1024
  @candidate_tree_limit 256 * 1024 * 1024
  @candidate_blob_count_limit 50_000
  @tree_listing_output_limit 32 * 1024 * 1024
  @git_virtual_memory_limit_kb 1_048_576
  @git_file_limit_blocks 524_288
  @shallow_file_limit 65_536
  @candidate_input_limit 1_048_576
  @candidate_input_depth 16
  @candidate_input_collection 4_096
  @command_timeout_ms 30_000
  @process_group_kill_grace_ms 1_000
  @max_command_output 65_536
  @marker_relative Path.join([".git", "transfer.json"])

  @spec sync(map(), map(), keyword()) :: {:ok, Path.t(), map()} | {:error, term()}
  def sync(run, validation_stage, opts) when is_map(run) and is_map(validation_stage) and is_list(opts) do
    case field(field(run, :execution) || %{}, :worker_control) do
      nil ->
        with {:ok, workspace} <- original_workspace(run) do
          {:ok, workspace, %{transferred: false}}
        end

      control when is_map(control) ->
        worker_opts = worker_operation_opts(opts)

        with :ok <- candidate_validation_stage(validation_stage),
             :ok <- pinned_validation_stage(run, validation_stage),
             {:ok, producer} <- trusted_producer(run, field(validation_stage, :id)),
             {:ok, identity} <- validate_completed_worker(producer, control, worker_opts),
             {:ok, root} <- candidate_root(run),
             {:ok, workspace, tree_sha} <- existing_or_import(run, root, producer, identity, control, worker_opts),
             {:ok, base_tree_sha} <- snapshot_base_tree(workspace, producer.metadata) do
          marker = marker_for(run, producer, identity, tree_sha, base_tree_sha)
          {:ok, workspace, public_metadata(producer.metadata, marker)}
        end

      _invalid_control ->
        {:error, :candidate_transfer_worker_control_invalid}
    end
  rescue
    _ -> {:error, :candidate_transfer_failed}
  catch
    _, _ -> {:error, :candidate_transfer_failed}
  end

  def sync(_run, _validation_stage, _opts), do: {:error, :candidate_transfer_arguments_invalid}

  @spec workspace(map()) :: {:ok, Path.t()} | {:error, term()}
  def workspace(run) when is_map(run) do
    case field(field(run, :execution) || %{}, :worker_control) do
      nil -> original_workspace(run)
      control when is_map(control) -> transferred_workspace(run)
      _ -> {:error, :candidate_transfer_worker_control_invalid}
    end
  rescue
    _ -> {:error, :candidate_workspace_unavailable}
  catch
    _, _ -> {:error, :candidate_workspace_unavailable}
  end

  def workspace(_run), do: {:error, :candidate_workspace_unavailable}

  defp candidate_validation_stage(stage) do
    if normalized(field(stage, :type)) == :check and
         normalized(field(field(stage, :gate) || %{}, :evaluator)) == :candidate_validation do
      :ok
    else
      {:error, :candidate_transfer_requires_validation_stage}
    end
  end

  defp pinned_validation_stage(run, stage) do
    stage_id = field(stage, :id)
    stages = field(field(run, :definition) || %{}, :stages) || %{}

    if is_binary(stage_id) and map_fetch(stages, stage_id) == stage do
      :ok
    else
      {:error, :candidate_transfer_validation_stage_not_pinned}
    end
  end

  defp latest_validation_stage_id(run) do
    stages = field(field(run, :definition) || %{}, :stages) || %{}

    validation_ids =
      Enum.flat_map(stages, fn {id, stage} ->
        if is_map(stage) and normalized(field(stage, :type)) == :check and
             normalized(field(field(stage, :gate) || %{}, :evaluator)) == :candidate_validation do
          [field(stage, :id) || id]
        else
          []
        end
      end)

    attempts = field(run, :attempts) || []

    case Enum.find(Enum.reverse(attempts), &(field(&1, :stage) in validation_ids)) do
      attempt when is_map(attempt) ->
        {:ok, field(attempt, :stage)}

      _ ->
        if field(run, :stage_id) in validation_ids do
          {:ok, field(run, :stage_id)}
        else
          {:error, :candidate_validation_attempt_missing}
        end
    end
  end

  defp trusted_producer(run, validation_stage_id) do
    execution = field(run, :execution) || %{}
    producer_stage_id = field(execution, :candidate_producer_stage)
    pinned_command_sha = field(execution, :candidate_producer_command_sha256)
    definition = field(run, :definition) || %{}
    stages = field(definition, :stages) || %{}
    stage = map_fetch(stages, producer_stage_id)
    validation_stage = map_fetch(stages, validation_stage_id)
    attempts = field(run, :attempts)

    with true <- is_binary(producer_stage_id) and is_binary(pinned_command_sha),
         true <- is_map(stage) and is_map(validation_stage) and is_list(attempts),
         :ok <- candidate_validation_stage(validation_stage),
         true <- @candidate_output in (field(validation_stage, :inputs) || []),
         :ok <- validate_producer_definition(stage, producer_stage_id, pinned_command_sha),
         {:ok, attempt} <- latest_producer_attempt_before(run, attempts, producer_stage_id, validation_stage_id),
         {:ok, metadata} <- producer_metadata(run, attempt),
         {:ok, operation, identity} <- registered_operation(run, attempt),
         :ok <- validate_metadata_against_run(run, metadata) do
      {:ok,
       %{
         stage_id: producer_stage_id,
         stage: stage,
         attempt: attempt,
         attempt_id: field(attempt, :id),
         operation: operation,
         identity: identity,
         metadata: metadata
       }}
    else
      false -> {:error, :candidate_transfer_producer_not_pinned}
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_producer_unavailable}
    end
  end

  defp validate_producer_definition(stage, stage_id, pinned_command_sha) do
    gate = field(stage, :gate) || %{}
    command = field(gate, :command)
    outputs = field(stage, :outputs)
    digest = command_digest(command)

    if normalized(field(stage, :type)) == :check and is_list(outputs) and @candidate_output in outputs and
         is_binary(digest) and digest == pinned_command_sha do
      :ok
    else
      {:error, {:candidate_transfer_producer_definition_mismatch, stage_id}}
    end
  end

  defp command_digest(command) when is_list(command) and command != [] do
    if Enum.all?(command, &is_binary/1) do
      :crypto.hash(:sha256, Jason.encode!(command)) |> Base.encode16(case: :lower)
    end
  rescue
    _ -> nil
  end

  defp command_digest(_command), do: nil

  defp latest_producer_attempt_before(run, attempts, producer_stage_id, validation_stage_id) do
    with {:ok, validation_index} <- validation_attempt_index(run, attempts, validation_stage_id),
         producer_index <- producer_attempt_index(attempts, validation_index, producer_stage_id),
         true <- is_integer(producer_index),
         attempt when is_map(attempt) <- Enum.at(attempts, producer_index),
         :ok <- only_completed_checks_between(attempts, producer_index + 1, validation_index) do
      validate_latest_producer_attempt(attempt)
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_producer_attempt_missing}
    end
  end

  defp producer_attempt_index(attempts, validation_index, producer_stage_id) do
    attempts
    |> Enum.with_index()
    |> Enum.filter(fn {attempt, index} -> index < validation_index and field(attempt, :stage) == producer_stage_id end)
    |> List.last()
    |> case do
      {_attempt, index} -> index
      nil -> nil
    end
  end

  defp validation_attempt_index(run, attempts, validation_stage_id) do
    current_id = field(run, :current_attempt_id)

    current_index =
      if is_binary(current_id) do
        Enum.find_index(attempts, &(field(&1, :id) == current_id))
      end

    cond do
      is_integer(current_index) and field(Enum.at(attempts, current_index), :stage) == validation_stage_id ->
        {:ok, current_index}

      is_nil(current_id) and field(run, :stage_id) == validation_stage_id ->
        {:ok, length(attempts)}

      true ->
        case Enum.find_index(Enum.reverse(attempts), &(field(&1, :stage) == validation_stage_id)) do
          nil -> {:error, :candidate_validation_attempt_missing}
          reverse_index -> {:ok, length(attempts) - reverse_index - 1}
        end
    end
  end

  defp only_completed_checks_between(attempts, first, last) do
    intervening = Enum.slice(attempts, first, max(last - first, 0))

    if Enum.all?(intervening, fn attempt ->
         normalized(field(attempt, :type)) == :check and
           normalized(field(attempt, :status)) == :completed and
           normalized(field(attempt, :gate)) == :passed
       end) do
      :ok
    else
      {:error, :candidate_transfer_producer_superseded}
    end
  end

  defp validate_latest_producer_attempt(attempt) when is_map(attempt) do
    with true <- normalized(field(attempt, :type)) == :check,
         true <- normalized(field(attempt, :status)) == :completed,
         true <- normalized(field(attempt, :gate)) == :passed,
         true <- is_binary(field(attempt, :id)),
         :ok <- successful_check_output(field(attempt, :result)) do
      {:ok, attempt}
    else
      _ -> {:error, :candidate_transfer_producer_check_not_successful}
    end
  end

  defp successful_check_output(result) when is_map(result) do
    evidence = field(result, :evidence)

    with true <- normalized(field(result, :status)) == :ok,
         true <- is_map(evidence),
         true <- field(evidence, :exit_status) == 0,
         true <- field(evidence, :exit_code) == "exited",
         true <- field(evidence, :truncated) == false,
         true <- field(evidence, :timed_out) == false,
         output when is_binary(output) <- field(evidence, :output),
         {:ok, metadata} <- Jason.decode(output),
         :ok <- validate_producer_payload(metadata) do
      :ok
    else
      _ -> {:error, :candidate_transfer_producer_output_invalid}
    end
  end

  defp successful_check_output(_result), do: {:error, :candidate_transfer_producer_output_missing}

  defp producer_metadata(run, attempt) do
    result = field(attempt, :result)
    evidence = field(result, :evidence) || %{}
    output = field(evidence, :output)

    with true <- is_binary(output),
         {:ok, metadata} <- Jason.decode(output),
         :ok <- validate_producer_payload(metadata),
         candidate_output when is_map(candidate_output) <-
           map_fetch(field(run, :outputs) || %{}, @candidate_output),
         true <- field(candidate_output, :output) == output,
         {:ok, _workspace} <- original_workspace(run),
         true <- metadata["candidate_bundle"] == relative_bundle_path(metadata["candidate_input_sha256"]),
         true <- field(run, :branch) != nil and metadata["branch"] == "refs/heads/" <> field(run, :branch) do
      {:ok, metadata}
    else
      _ -> {:error, :candidate_transfer_producer_metadata_invalid}
    end
  end

  defp validate_producer_payload(metadata) when is_map(metadata) do
    if Enum.sort(Map.keys(metadata)) == Enum.sort(@producer_keys) and
         valid_git_sha?(metadata["candidate_commit"]) and
         valid_git_sha?(metadata["base_commit"]) and
         byte_size(metadata["candidate_commit"]) == byte_size(metadata["base_commit"]) and
         valid_sha256?(metadata["candidate_input_sha256"]) and
         valid_sha256?(metadata["bundle_sha256"]) and
         is_integer(metadata["bundle_bytes"]) and metadata["bundle_bytes"] in 1..@bundle_limit and
         is_binary(metadata["branch"]) and valid_branch_ref?(metadata["branch"]) and
         is_binary(metadata["candidate_bundle"]) do
      :ok
    else
      {:error, :candidate_transfer_producer_metadata_invalid}
    end
  end

  defp validate_producer_payload(_metadata), do: {:error, :candidate_transfer_producer_metadata_invalid}

  defp registered_operation(run, attempt) do
    attempt_id = field(attempt, :id)
    operations = field(run, :operations) || %{}
    operation = map_fetch(operations, attempt_id)
    identity = field(operation || %{}, :worker)

    with true <- is_map(operation) and is_map(identity),
         true <- field(operation, :id) == attempt_id,
         true <- field(operation, :attempt_id) == attempt_id,
         true <- field(operation, :stage) == field(attempt, :stage),
         true <- normalized(field(operation, :type)) == :check,
         true <- normalized(field(operation, :status)) == :completed,
         true <- identity["operation_id"] == attempt_id,
         true <- identity["kind"] == "systemd-unit" do
      {:ok, operation, identity}
    else
      _ -> {:error, :candidate_transfer_registered_operation_missing}
    end
  end

  defp validate_metadata_against_run(run, metadata) do
    execution = field(run, :execution) || %{}
    base = field(execution, :validation_base)
    branch = field(run, :branch)

    with :ok <- validate_current_candidate_input(run, metadata),
         true <-
           valid_git_sha?(base) and metadata["base_commit"] == base and is_binary(branch) and
             metadata["branch"] == "refs/heads/" <> branch and metadata["candidate_commit"] != base do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_base_or_branch_mismatch}
    end
  end

  defp validate_current_candidate_input(run, metadata) do
    candidate = map_fetch(field(run, :outputs) || %{}, "candidate")

    with {:ok, encoded} <- canonical_candidate_json(candidate),
         true <- sha256(encoded) == metadata["candidate_input_sha256"] do
      :ok
    else
      _ -> {:error, :candidate_transfer_candidate_input_superseded}
    end
  end

  defp canonical_candidate_json(value) do
    with {:ok, encoded} <- canonical_candidate_value(value, 0),
         true <- IO.iodata_length(encoded) <= @candidate_input_limit do
      {:ok, IO.iodata_to_binary(encoded)}
    else
      _ -> {:error, :candidate_transfer_candidate_input_unsupported}
    end
  rescue
    _ -> {:error, :candidate_transfer_candidate_input_unsupported}
  end

  defp canonical_candidate_value(_value, depth) when depth > @candidate_input_depth,
    do: {:error, :candidate_transfer_candidate_input_unsupported}

  defp canonical_candidate_value(nil, _depth), do: {:ok, "null"}
  defp canonical_candidate_value(true, _depth), do: {:ok, "true"}
  defp canonical_candidate_value(false, _depth), do: {:ok, "false"}
  defp canonical_candidate_value(value, _depth) when is_integer(value), do: {:ok, Integer.to_string(value)}

  defp canonical_candidate_value(value, _depth) when is_binary(value) do
    if ascii_string?(value), do: {:ok, Jason.encode!(value)}, else: {:error, :candidate_transfer_candidate_input_unsupported}
  end

  defp canonical_candidate_value(value, depth) when is_list(value) and length(value) <= @candidate_input_collection do
    with {:ok, items} <- canonical_candidate_items(value, depth + 1, []) do
      {:ok, ["[", Enum.intersperse(items, ","), "]"]}
    end
  end

  defp canonical_candidate_value(value, depth) when is_map(value) and map_size(value) <= @candidate_input_collection do
    keys = Map.keys(value)

    if Enum.all?(keys, &(is_binary(&1) and ascii_string?(&1))) do
      keys
      |> Enum.sort()
      |> canonical_candidate_pairs(value, depth + 1, [])
    else
      {:error, :candidate_transfer_candidate_input_unsupported}
    end
  end

  defp canonical_candidate_value(_value, _depth), do: {:error, :candidate_transfer_candidate_input_unsupported}

  defp canonical_candidate_items([], _depth, items), do: {:ok, Enum.reverse(items)}

  defp canonical_candidate_items([value | rest], depth, items) do
    with {:ok, encoded} <- canonical_candidate_value(value, depth) do
      canonical_candidate_items(rest, depth, [encoded | items])
    end
  end

  defp canonical_candidate_pairs([], _map, _depth, pairs), do: {:ok, ["{", Enum.intersperse(Enum.reverse(pairs), ","), "}"]}

  defp canonical_candidate_pairs([key | rest], map, depth, pairs) do
    with {:ok, encoded_value} <- canonical_candidate_value(Map.fetch!(map, key), depth) do
      pair = [Jason.encode!(key), ":", encoded_value]
      canonical_candidate_pairs(rest, map, depth, [pair | pairs])
    end
  end

  defp ascii_string?(value) when is_binary(value) do
    String.valid?(value) and Enum.all?(:binary.bin_to_list(value), &(&1 < 128))
  end

  defp ascii_string?(_value), do: false

  defp validate_completed_worker(producer, control, worker_opts) do
    with {:ok, identity} <- WorkerOperation.validate_identity(producer.identity, producer.attempt_id, control, worker_opts),
         {:ok, %{"status" => "terminated", "exit_code" => "exited", "exit_status" => 0} = status} <-
           WorkerOperation.status(control, identity, worker_opts),
         true <- status["identity"] == identity,
         true <- terminal_proof_matches?(status, identity) do
      {:ok, identity}
    else
      _ -> {:error, :candidate_transfer_worker_not_successfully_terminated}
    end
  rescue
    _ -> {:error, :candidate_transfer_worker_not_successfully_terminated}
  catch
    _, _ -> {:error, :candidate_transfer_worker_not_successfully_terminated}
  end

  defp terminal_proof_matches?(status, identity) do
    proof = status["termination_proof"]

    is_map(proof) and proof["machine_id"] == identity["machine_id"] and
      proof["boot_id"] == identity["boot_id"] and proof["unit"] == identity["unit"] and
      proof["invocation_id"] == identity["invocation_id"] and
      proof["control_group"] == identity["control_group"] and proof["cgroup_populated"] == 0 and
      proof["exec_main_code"] == "exited" and proof["exec_main_status"] == 0
  end

  defp existing_or_import(run, root, producer, identity, control, worker_opts) do
    final_path = snapshot_path(root, run, producer.metadata)

    case File.lstat(final_path) do
      {:ok, %File.Stat{type: :directory}} ->
        case verify_snapshot(run, root, producer, identity, final_path) do
          {:ok, tree_sha} -> {:ok, final_path, tree_sha}
          {:error, _} = error -> error
        end

      {:error, :enoent} ->
        import_result =
          with :ok <- verify_original_workspace(run, producer.metadata),
               {:ok, bundle} <- WorkerOperation.candidate_bundle(control, identity, producer.metadata, worker_opts),
               :ok <- verify_bundle_bytes(bundle, producer.metadata) do
            import_snapshot(run, root, final_path, producer, identity, bundle)
          end

        case import_result do
          {:ok, workspace, tree_sha} ->
            {:ok, workspace, tree_sha}

          {:error, :candidate_transfer_snapshot_already_published} ->
            with {:ok, tree_sha} <- verify_snapshot(run, root, producer, identity, final_path) do
              {:ok, final_path, tree_sha}
            end

          {:error, _} = error ->
            error
        end

      _ ->
        {:error, :candidate_transfer_snapshot_path_invalid}
    end
  end

  defp verify_bundle_bytes(bundle, metadata) when is_binary(bundle) do
    if byte_size(bundle) == metadata["bundle_bytes"] and byte_size(bundle) <= @bundle_limit and
         sha256(bundle) == metadata["bundle_sha256"] do
      :ok
    else
      {:error, :candidate_transfer_bundle_mismatch}
    end
  end

  defp verify_bundle_bytes(_bundle, _metadata), do: {:error, :candidate_transfer_bundle_invalid}

  defp verify_original_workspace(run, metadata) do
    with {:ok, workspace} <- original_workspace(run),
         {:ok, head} <- CandidateGit.run(workspace, ["rev-parse", "--verify", "HEAD^{commit}"]),
         {:ok, status} <- CandidateGit.run(workspace, ["status", "--porcelain=v1", "--untracked-files=all"]),
         true <- head in [metadata["base_commit"], metadata["candidate_commit"]],
         true <- status == "" do
      :ok
    else
      _ -> {:error, :candidate_transfer_original_workspace_not_clean}
    end
  end

  defp import_snapshot(run, root, final_path, producer, identity, bundle) do
    case :global.trans({{__MODULE__, final_path}, self()}, fn ->
           import_snapshot_locked(run, root, final_path, producer, identity, bundle)
         end) do
      :aborted -> {:error, :candidate_transfer_snapshot_lock_unavailable}
      result -> result
    end
  rescue
    _ -> {:error, :candidate_transfer_snapshot_lock_unavailable}
  catch
    _, _ -> {:error, :candidate_transfer_snapshot_lock_unavailable}
  end

  defp import_snapshot_locked(run, root, final_path, producer, identity, bundle) do
    prefix = ".incoming-" <> snapshot_key(run, producer.metadata) <> "-"

    with :ok <- clean_incomplete_snapshots(root, prefix),
         {:ok, git} <- git_executable(),
         {:ok, temporary} <- create_private_directory(root, prefix),
         do: import_temporary(git, temporary, final_path, run, producer, identity, bundle)
  end

  defp import_temporary(git, temporary, final_path, run, producer, identity, bundle) do
    try do
      CandidateGit.with_snapshot(field(run, :workspace), fn source_git ->
        import_into_temporary(git, temporary, source_git, final_path, run, producer, identity, bundle)
      end)
    rescue
      _ -> {:error, :candidate_transfer_import_failed}
    catch
      _, _ -> {:error, :candidate_transfer_import_failed}
    after
      File.rm_rf(temporary)
    end
  end

  defp import_into_temporary(git, temporary, source_git, final_path, run, producer, identity, bundle) do
    git_dir = Path.join(temporary, ".git")
    bundle_path = Path.join(git_dir, "candidate.bundle")

    with :ok <- copy_git_metadata(source_git, git_dir),
         :ok <- File.chmod(git_dir, 0o700),
         {:ok, base_tree_sha} <- prepare_shallow_base(git, git_dir, temporary, producer.metadata),
         :ok <- File.write(bundle_path, bundle, [:binary, :exclusive]),
         :ok <- File.chmod(bundle_path, 0o600),
         :ok <- verify_bundle(git, git_dir, temporary, bundle_path, producer.metadata),
         :ok <- fetch_bundle(git, git_dir, temporary, bundle_path, producer.metadata),
         :ok <- verify_imported_commits(git, git_dir, temporary, producer.metadata),
         :ok <- verify_candidate_tree_budget(git, git_dir, temporary, producer.metadata),
         {:ok, tree_sha} <- checkout_candidate(git, git_dir, temporary, producer.metadata),
         :ok <- File.rm(bundle_path),
         :ok <- ensure_clean_candidate(git, git_dir, temporary, producer.metadata, tree_sha),
         :ok <- verify_shallow_boundary(temporary, producer.metadata),
         :ok <- write_marker(temporary, marker_for(run, producer, identity, tree_sha, base_tree_sha)),
         :ok <- rename_snapshot(temporary, final_path) do
      {:ok, final_path, tree_sha}
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_import_failed}
    end
  end

  defp copy_git_metadata(source_git, destination) do
    case File.cp_r(source_git, destination) do
      {:ok, _files} -> :ok
      {:error, reason, _source} -> {:error, {:candidate_transfer_metadata_copy_failed, reason}}
      {:error, reason} -> {:error, {:candidate_transfer_metadata_copy_failed, reason}}
    end
  end

  defp verify_bundle(git, git_dir, workspace, bundle_path, metadata) do
    with {:ok, _output} <- git_command(git, git_dir, workspace, ["bundle", "verify", bundle_path]),
         {:ok, heads} <- git_command(git, git_dir, workspace, ["bundle", "list-heads", bundle_path]),
         true <- String.trim(heads) == metadata["candidate_commit"] <> " HEAD" do
      :ok
    else
      _ -> {:error, :candidate_transfer_bundle_heads_invalid}
    end
  end

  defp prepare_shallow_base(git, git_dir, workspace, metadata) do
    base = metadata["base_commit"]
    shallow_path = Path.join(git_dir, "shallow")

    with {:ok, resolved_base} <- git_command(git, git_dir, workspace, ["rev-parse", "--verify", base <> "^{commit}"]),
         true <- String.trim(resolved_base) == base,
         {:ok, tree} <- git_command(git, git_dir, workspace, ["rev-parse", "--verify", base <> "^{tree}"]),
         base_tree <- String.trim(tree),
         true <- valid_git_sha?(base_tree),
         :ok <- only_pinned_shallow_boundary(shallow_path, base),
         :ok <- File.write(shallow_path, base <> "\n", [:binary]),
         :ok <- File.chmod(shallow_path, 0o600) do
      {:ok, base_tree}
    else
      _ -> {:error, :candidate_transfer_base_object_invalid}
    end
  end

  defp only_pinned_shallow_boundary(path, base) do
    case File.lstat(path) do
      {:error, :enoent} ->
        :ok

      {:ok, %File.Stat{type: :regular, size: size, links: 1}} when size <= @shallow_file_limit ->
        with {:ok, contents} <- File.read(path),
             [^base] <- String.split(contents, "\n", trim: true) do
          :ok
        else
          _ -> {:error, :candidate_transfer_unexpected_shallow_boundary}
        end

      _ ->
        {:error, :candidate_transfer_unexpected_shallow_boundary}
    end
  end

  defp verify_shallow_boundary(workspace, metadata) do
    path = Path.join([workspace, ".git", "shallow"])
    base = metadata["base_commit"]

    with {:ok, uid} <- effective_uid(),
         {:ok, %File.Stat{type: :regular, size: size, mode: mode, links: 1, uid: ^uid}} <- File.lstat(path),
         true <- size <= @shallow_file_limit and Bitwise.band(mode, 0o777) == 0o600,
         {:ok, contents} <- File.read(path),
         [^base] <- String.split(contents, "\n", trim: true) do
      :ok
    else
      _ -> {:error, :candidate_transfer_shallow_boundary_invalid}
    end
  end

  defp fetch_bundle(git, git_dir, workspace, bundle_path, metadata) do
    case git_command(git, git_dir, workspace, [
           "-c",
           "protocol.file.allow=always",
           "fetch",
           "--no-tags",
           "--no-recurse-submodules",
           bundle_path,
           metadata["candidate_commit"]
         ]) do
      {:ok, _} -> :ok
      {:error, _} -> {:error, :candidate_transfer_bundle_import_failed}
    end
  end

  defp verify_imported_commits(git, git_dir, workspace, metadata) do
    with {:ok, base} <- git_command(git, git_dir, workspace, ["rev-parse", "--verify", metadata["base_commit"] <> "^{commit}"]),
         true <- String.trim(base) == metadata["base_commit"],
         {:ok, candidate} <- git_command(git, git_dir, workspace, ["rev-parse", "--verify", metadata["candidate_commit"] <> "^{commit}"]),
         true <- String.trim(candidate) == metadata["candidate_commit"],
         {:ok, parents} <- git_command(git, git_dir, workspace, ["rev-list", "--parents", "--max-count=1", metadata["candidate_commit"]]),
         true <- String.split(String.trim(parents)) == [metadata["candidate_commit"], metadata["base_commit"]] do
      :ok
    else
      _ -> {:error, :candidate_transfer_commit_or_parent_mismatch}
    end
  end

  defp verify_candidate_tree_budget(git, git_dir, workspace, metadata) do
    case git_command(
           git,
           git_dir,
           workspace,
           ["ls-tree", "-r", "-l", "-z", metadata["candidate_commit"]],
           @tree_listing_output_limit
         ) do
      {:ok, listing} ->
        case candidate_tree_totals(listing, 0, 0) do
          {:ok, _blob_count, _total_bytes} -> :ok
          {:error, reason} -> {:error, reason}
        end

      {:error, :candidate_transfer_git_output_limit} ->
        {:error, :candidate_transfer_tree_listing_limit}

      {:error, _} ->
        {:error, :candidate_transfer_tree_listing_failed}
    end
  end

  defp candidate_tree_totals(<<>>, blob_count, total_bytes), do: {:ok, blob_count, total_bytes}

  defp candidate_tree_totals(listing, blob_count, total_bytes) do
    case :binary.match(listing, <<0>>) do
      {entry_size, 1} ->
        entry = binary_part(listing, 0, entry_size)
        remaining = binary_part(listing, entry_size + 1, byte_size(listing) - entry_size - 1)

        case candidate_tree_entry(entry, blob_count, total_bytes) do
          {:ok, next_count, _next_total} when next_count > @candidate_blob_count_limit ->
            {:error, :candidate_transfer_blob_count_exceeded}

          {:ok, _next_count, next_total} when next_total > @candidate_tree_limit ->
            {:error, :candidate_transfer_tree_budget_exceeded}

          {:ok, next_count, next_total} ->
            candidate_tree_totals(remaining, next_count, next_total)

          {:error, _} = error ->
            error
        end

      :nomatch ->
        {:error, :candidate_transfer_tree_listing_invalid}
    end
  end

  defp candidate_tree_entry(entry, blob_count, total_bytes) do
    case :binary.match(entry, <<9>>) do
      {header_size, 1} ->
        header = binary_part(entry, 0, header_size)

        case String.split(header) do
          [_mode, "blob", object_sha, size] ->
            case {valid_git_sha?(object_sha), Integer.parse(size)} do
              {true, {file_size, ""}} when file_size >= 0 and file_size <= @candidate_file_limit ->
                {:ok, blob_count + 1, total_bytes + file_size}

              {true, {file_size, ""}} when file_size > @candidate_file_limit ->
                {:error, :candidate_transfer_file_budget_exceeded}

              _ ->
                {:error, :candidate_transfer_tree_listing_invalid}
            end

          ["160000", "commit", object_sha] ->
            if valid_git_sha?(object_sha), do: {:ok, blob_count, total_bytes}, else: {:error, :candidate_transfer_tree_listing_invalid}

          _ ->
            {:error, :candidate_transfer_tree_listing_invalid}
        end

      :nomatch ->
        {:error, :candidate_transfer_tree_listing_invalid}
    end
  rescue
    _ -> {:error, :candidate_transfer_tree_listing_invalid}
  end

  defp checkout_candidate(git, git_dir, workspace, metadata) do
    with {:ok, _} <- git_command(git, git_dir, workspace, ["checkout", "--force", "--detach", metadata["candidate_commit"]]),
         {:ok, tree} <- git_command(git, git_dir, workspace, ["rev-parse", "--verify", "HEAD^{tree}"]) do
      {:ok, String.trim(tree)}
    else
      _ -> {:error, :candidate_transfer_checkout_failed}
    end
  end

  defp ensure_clean_candidate(git, git_dir, workspace, metadata, tree_sha) do
    with {:ok, head} <- git_command(git, git_dir, workspace, ["rev-parse", "--verify", "HEAD^{commit}"]),
         true <- String.trim(head) == metadata["candidate_commit"],
         {:ok, tree} <- git_command(git, git_dir, workspace, ["rev-parse", "--verify", "HEAD^{tree}"]),
         true <- String.trim(tree) == tree_sha,
         {:ok, status} <- git_command(git, git_dir, workspace, ["status", "--porcelain=v1", "--untracked-files=all"]),
         true <- status == "" do
      :ok
    else
      _ -> {:error, :candidate_transfer_source_not_clean}
    end
  end

  defp write_marker(workspace, marker) do
    path = Path.join(workspace, @marker_relative)

    with :ok <- File.write(path, Jason.encode!(marker), [:binary, :exclusive]),
         :ok <- File.chmod(path, 0o600) do
      :ok
    else
      {:error, reason} -> {:error, {:candidate_transfer_marker_write_failed, reason}}
    end
  rescue
    _ -> {:error, :candidate_transfer_marker_write_failed}
  end

  defp rename_snapshot(temporary, final_path) do
    case File.rename(temporary, final_path) do
      :ok ->
        :ok

      {:error, reason} when reason in [:eexist, :enotempty] ->
        {:error, :candidate_transfer_snapshot_already_published}

      {:error, reason} ->
        {:error, {:candidate_transfer_snapshot_publish_failed, reason}}
    end
  end

  defp transferred_workspace(run) do
    with {:ok, validation_stage_id} <- latest_validation_stage_id(run),
         {:ok, producer} <- trusted_producer(run, validation_stage_id),
         {:ok, root} <- candidate_root(run),
         final_path <- snapshot_path(root, run, producer.metadata),
         {:ok, marker} <- read_marker(final_path),
         {:ok, base_tree_sha} <- snapshot_base_tree(final_path, producer.metadata),
         true <- marker_matches_producer?(marker, run, producer, base_tree_sha),
         :ok <- validate_snapshot_path(root, final_path),
         :ok <- verify_shallow_boundary(final_path, producer.metadata),
         :ok <- verify_snapshot_tree(final_path, producer.metadata, marker["tree_sha"]) do
      {:ok, final_path}
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_workspace_transfer_marker_mismatch}
    end
  end

  defp verify_snapshot(run, root, producer, identity, path) do
    with {:ok, marker} <- read_marker(path),
         {:ok, base_tree_sha} <- snapshot_base_tree(path, producer.metadata),
         true <- marker == marker_for(run, producer, identity, marker["tree_sha"], base_tree_sha),
         :ok <- validate_snapshot_path(root, path),
         :ok <- verify_shallow_boundary(path, producer.metadata),
         :ok <- verify_snapshot_tree(path, producer.metadata, marker["tree_sha"]) do
      {:ok, marker["tree_sha"]}
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_snapshot_marker_mismatch}
    end
  end

  defp verify_snapshot_tree(path, metadata, tree_sha) do
    with true <- valid_git_sha?(tree_sha),
         {:ok, head} <- CandidateGit.run(path, ["rev-parse", "--verify", "HEAD^{commit}"]),
         true <- String.trim(head) == metadata["candidate_commit"],
         {:ok, tree} <- CandidateGit.run(path, ["rev-parse", "--verify", "HEAD^{tree}"]),
         true <- String.trim(tree) == tree_sha,
         {:ok, parents} <- CandidateGit.run(path, ["rev-parse", "--verify", metadata["candidate_commit"] <> "^1^{commit}"]),
         true <- String.trim(parents) == metadata["base_commit"],
         {:ok, status} <- CandidateGit.run(path, ["status", "--porcelain=v1", "--untracked-files=all"]),
         true <- String.trim(status) == "" do
      :ok
    else
      _ -> {:error, :candidate_transfer_snapshot_source_invalid}
    end
  end

  defp marker_for(run, producer, identity, tree_sha, base_tree_sha) do
    Map.merge(producer.metadata, %{
      "format" => "candidate-transfer-v1",
      "run_id" => field(run, :id),
      "producer_attempt_id" => producer.attempt_id,
      "operation_id" => producer.attempt_id,
      "worker_identity" => identity,
      "tree_sha" => tree_sha,
      "shallow_base_commit" => producer.metadata["base_commit"],
      "shallow_base_tree" => base_tree_sha
    })
  end

  defp marker_matches_producer?(marker, run, producer, base_tree_sha) do
    identity = producer.identity
    expected = marker_for(run, producer, identity, marker["tree_sha"], base_tree_sha)
    marker == expected
  end

  defp snapshot_base_tree(path, metadata) do
    with {:ok, tree} <- CandidateGit.run(path, ["rev-parse", "--verify", metadata["base_commit"] <> "^{tree}"]),
         tree <- String.trim(tree),
         true <- valid_git_sha?(tree) do
      {:ok, tree}
    else
      _ -> {:error, :candidate_transfer_base_tree_invalid}
    end
  end

  defp read_marker(path) do
    marker_path = Path.join(path, @marker_relative)

    with {:ok, uid} <- effective_uid(),
         {:ok, %File.Stat{type: :directory, uid: ^uid}} <- File.lstat(path),
         {:ok, %File.Stat{type: :directory, mode: git_mode, uid: ^uid}} <- File.lstat(Path.join(path, ".git")),
         true <- Bitwise.band(git_mode, 0o777) == 0o700,
         {:ok, %File.Stat{type: :regular, mode: marker_mode, links: 1, uid: ^uid}} <- File.lstat(marker_path),
         true <- Bitwise.band(marker_mode, 0o777) == 0o600,
         {:ok, contents} <- File.read(marker_path),
         {:ok, marker} <- Jason.decode(contents),
         true <- is_map(marker) do
      {:ok, marker}
    else
      _ -> {:error, :candidate_transfer_marker_invalid}
    end
  end

  defp validate_snapshot_path(root, path) do
    with {:ok, uid} <- effective_uid(),
         {:ok, canonical_root} <- PathSafety.canonicalize(root),
         {:ok, canonical_path} <- PathSafety.canonicalize(path),
         true <- canonical_root == Path.expand(root),
         true <- canonical_path == path and within?(canonical_path, canonical_root),
         {:ok, %File.Stat{type: :directory, mode: root_mode, uid: ^uid}} <- File.lstat(root),
         true <- Bitwise.band(root_mode, 0o777) == 0o700,
         {:ok, %File.Stat{type: :directory, mode: snapshot_mode, uid: ^uid}} <- File.lstat(path),
         true <- Bitwise.band(snapshot_mode, 0o777) == 0o700 do
      :ok
    else
      _ -> {:error, :candidate_transfer_snapshot_path_invalid}
    end
  end

  defp candidate_root(run) do
    path = field(field(run, :execution) || %{}, :validation_candidates)

    with true <- is_binary(path) and Path.type(path) == :absolute,
         expanded <- Path.expand(path),
         {:ok, canonical} <- PathSafety.canonicalize(expanded),
         true <- canonical == expanded,
         {:ok, uid} <- effective_uid(),
         :ok <- ensure_private_root(expanded, uid) do
      {:ok, expanded}
    else
      _ -> {:error, :candidate_transfer_storage_root_unavailable}
    end
  end

  defp ensure_private_root(path, uid) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, mode: mode, uid: ^uid}} ->
        if Bitwise.band(mode, 0o777) == 0o700, do: :ok, else: {:error, :candidate_transfer_storage_permissions_invalid}

      {:error, :enoent} ->
        with :ok <- File.mkdir_p(path),
             :ok <- File.chmod(path, 0o700),
             {:ok, %File.Stat{type: :directory, mode: mode, uid: ^uid}} <- File.lstat(path),
             true <- Bitwise.band(mode, 0o777) == 0o700 do
          :ok
        else
          _ -> {:error, :candidate_transfer_storage_permissions_invalid}
        end

      _ ->
        {:error, :candidate_transfer_storage_root_invalid}
    end
  end

  defp create_private_directory(root, prefix) do
    path = Path.join(root, prefix <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false))

    with {:ok, uid} <- effective_uid(),
         :ok <- File.mkdir(path),
         :ok <- File.chmod(path, 0o700),
         {:ok, %File.Stat{type: :directory, mode: mode, uid: ^uid}} <- File.lstat(path),
         true <- Bitwise.band(mode, 0o777) == 0o700 do
      {:ok, path}
    else
      {:error, reason} ->
        File.rm_rf(path)
        {:error, {:candidate_transfer_temporary_directory_failed, reason}}

      _ ->
        File.rm_rf(path)
        {:error, :candidate_transfer_temporary_directory_failed}
    end
  end

  defp clean_incomplete_snapshots(root, prefix) do
    with {:ok, entries} <- File.ls(root) do
      Enum.reduce_while(entries, :ok, fn entry, :ok ->
        if String.starts_with?(entry, prefix) do
          path = Path.join(root, entry)

          case File.lstat(path) do
            {:ok, %File.Stat{type: :directory}} ->
              case File.rm_rf(path) do
                {:ok, _} -> {:cont, :ok}
                {:error, reason, _} -> {:halt, {:error, {:candidate_transfer_temporary_cleanup_failed, reason}}}
              end

            _ ->
              {:halt, {:error, :candidate_transfer_temporary_path_invalid}}
          end
        else
          {:cont, :ok}
        end
      end)
    end
  end

  defp original_workspace(run) do
    path = field(run, :workspace)

    with true <- is_binary(path) and Path.type(path) == :absolute,
         expanded <- Path.expand(path),
         {:ok, canonical} <- PathSafety.canonicalize(expanded),
         true <- canonical == expanded,
         {:ok, %File.Stat{type: :directory}} <- File.lstat(expanded) do
      {:ok, expanded}
    else
      _ -> {:error, :candidate_transfer_original_workspace_invalid}
    end
  end

  defp effective_uid do
    case System.cmd("/usr/bin/id", ["-u"], env: [{"PATH", "/usr/bin:/bin"}], stderr_to_stdout: true) do
      {value, 0} ->
        case Integer.parse(String.trim(value)) do
          {uid, ""} when uid >= 0 -> {:ok, uid}
          _ -> {:error, :candidate_transfer_service_owner_unavailable}
        end

      _ ->
        {:error, :candidate_transfer_service_owner_unavailable}
    end
  rescue
    _ -> {:error, :candidate_transfer_service_owner_unavailable}
  catch
    _, _ -> {:error, :candidate_transfer_service_owner_unavailable}
  end

  defp snapshot_path(root, run, metadata) do
    Path.join(root, "candidate-" <> snapshot_key(run, metadata))
  end

  defp snapshot_key(run, metadata) do
    run_id = field(run, :id)
    material = [run_id, metadata["candidate_input_sha256"], metadata["candidate_commit"]]

    if Enum.all?(material, &is_binary/1) do
      :crypto.hash(:sha256, Enum.join(material, <<0>>)) |> Base.encode16(case: :lower)
    else
      "invalid"
    end
  end

  defp relative_bundle_path(input_sha), do: ".git/factory-candidates/#{input_sha}.bundle"

  defp valid_branch_ref?(branch) when is_binary(branch) and byte_size(branch) <= 250 do
    Regex.match?(~r/\Arefs\/heads\/[A-Za-z0-9][A-Za-z0-9._\/-]*\z/, branch) and
      not String.contains?(branch, "..") and not String.contains?(branch, "//") and
      not String.ends_with?(branch, ["/", ".", ".lock"]) and not String.contains?(branch, "@{")
  end

  defp valid_branch_ref?(_branch), do: false

  defp valid_git_sha?(sha) when is_binary(sha), do: Regex.match?(~r/\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/, sha)
  defp valid_git_sha?(_sha), do: false

  defp valid_sha256?(sha), do: is_binary(sha) and Regex.match?(~r/\A[a-f0-9]{64}\z/, sha)

  defp git_executable do
    case System.find_executable("git") do
      path when is_binary(path) -> {:ok, path}
      _ -> {:error, :candidate_transfer_git_unavailable}
    end
  end

  defp git_command(git, git_dir, workspace, args, output_limit \\ @max_command_output) do
    with timeout when is_binary(timeout) <- System.find_executable("timeout") do
      shell_args = [
        "-c",
        "ulimit -v #{@git_virtual_memory_limit_kb} || exit 125; ulimit -f #{@git_file_limit_blocks} || exit 125; read -r start || exit 125; exec \"$@\"",
        "candidate-transfer",
        timeout,
        "--signal=TERM",
        "--kill-after=#{div(@process_group_kill_grace_ms, 1_000)}s",
        "#{div(@command_timeout_ms, 1_000)}s",
        git,
        "--no-replace-objects",
        "--no-pager",
        "--no-optional-locks",
        "--git-dir=" <> git_dir,
        "--work-tree=" <> workspace,
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.attributesFile=/dev/null",
        "-c",
        "protocol.allow=never",
        "-c",
        "submodule.recurse=false"
        | args
      ]

      port =
        Port.open({:spawn_executable, ~c"/bin/sh"}, [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          :hide,
          cd: String.to_charlist(workspace),
          env: port_environment(git_environment()),
          args: Enum.map(shell_args, &String.to_charlist/1)
        ])

      {:os_pid, group_id} = Port.info(port, :os_pid)
      Port.command(port, "start\n")

      collect_git_output(
        port,
        group_id,
        [],
        0,
        System.monotonic_time(:millisecond) + @command_timeout_ms + 6_000,
        output_limit
      )
    else
      nil -> {:error, :candidate_transfer_timeout_unavailable}
    end
  rescue
    _ -> {:error, :candidate_transfer_git_execution_failed}
  catch
    _, _ -> {:error, :candidate_transfer_git_execution_failed}
  end

  defp collect_git_output(port, group_id, chunks, output_size, deadline, output_limit) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, data}} ->
        next_size = output_size + byte_size(data)

        if next_size > output_limit do
          kill_process_group(group_id)
          close_port(port)
          {:error, :candidate_transfer_git_output_limit}
        else
          collect_git_output(port, group_id, [data | chunks], next_size, deadline, output_limit)
        end

      {^port, {:exit_status, 0}} ->
        kill_process_group(group_id)
        {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary() |> trim_newlines()}

      {^port, {:exit_status, status}} ->
        kill_process_group(group_id)
        if status in [124, 137], do: {:error, :candidate_transfer_git_timeout}, else: {:error, :candidate_transfer_git_command_failed}
    after
      remaining ->
        kill_process_group(group_id)
        close_port(port)
        {:error, :candidate_transfer_git_timeout}
    end
  end

  defp git_environment do
    [
      {"PATH", System.get_env("PATH") || "/usr/bin:/bin"},
      {"HOME", "/dev/null"},
      {"LANG", "C"},
      {"LC_ALL", "C"},
      {"GIT_CONFIG_COUNT", "0"},
      {"GIT_CONFIG_GLOBAL", "/dev/null"},
      {"GIT_CONFIG_SYSTEM", "/dev/null"},
      {"GIT_CONFIG_NOSYSTEM", "1"},
      {"GIT_ATTR_NOSYSTEM", "1"},
      {"GIT_OPTIONAL_LOCKS", "0"},
      {"GIT_TERMINAL_PROMPT", "0"},
      {"GIT_PAGER", "cat"},
      {"GIT_NO_REPLACE_OBJECTS", "1"},
      {"GIT_NO_LAZY_FETCH", "1"},
      {"GIT_TRACE", "0"},
      {"GIT_TRACE_CURL", "0"},
      {"GIT_CURL_VERBOSE", "0"}
    ]
  end

  defp port_environment(env) do
    cleared = System.get_env() |> Map.keys() |> Enum.map(&{String.to_charlist(&1), false})
    configured = Enum.map(env, fn {key, value} -> {String.to_charlist(key), String.to_charlist(value)} end)
    cleared ++ configured
  end

  defp kill_process_group(group_id) do
    _ = System.cmd("kill", ["-KILL", "--", "-#{group_id}"], stderr_to_stdout: true)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp close_port(port) do
    if Port.info(port), do: Port.close(port)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp trim_newlines(output) do
    output
    |> String.trim_trailing("\n")
    |> String.trim_trailing("\r")
  end

  defp worker_operation_opts(opts) do
    case Keyword.get(opts, :worker_operation_opts, []) do
      worker_opts when is_list(worker_opts) -> worker_opts
      _ -> []
    end
  end

  defp public_metadata(metadata, marker) do
    %{
      candidate_commit: metadata["candidate_commit"],
      base_commit: metadata["base_commit"],
      branch: metadata["branch"],
      candidate_input_sha256: metadata["candidate_input_sha256"],
      bundle_sha256: metadata["bundle_sha256"],
      bundle_bytes: metadata["bundle_bytes"],
      producer_attempt_id: marker["producer_attempt_id"]
    }
  end

  defp within?(path, root), do: String.starts_with?(path, root <> "/")

  defp map_fetch(map, key) when is_map(map) and is_binary(key), do: Map.get(map, key)
  defp map_fetch(map, key) when is_map(map), do: Map.get(map, key)
  defp map_fetch(_map, _key), do: nil

  defp field(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp field(_map, _key), do: nil

  defp normalized(value) when value in [:check, :completed, :passed, :ok, :candidate_validation], do: value
  defp normalized(value) when value in ["check", "completed", "passed", "ok", "candidate_validation"], do: String.to_existing_atom(value)
  defp normalized(_value), do: nil

  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
