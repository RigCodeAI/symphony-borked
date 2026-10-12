defmodule SymphonyElixir.CandidateTransfer do
  @moduledoc false

  alias SymphonyElixir.{CandidateContext, CandidateGit, PathSafety, Validation, ValidationPolicy, WorkerOperation}

  @candidate_output "candidate-commit"
  @legacy_producer_keys ~w(candidate_commit base_commit candidate_input_sha256 branch candidate_bundle bundle_sha256 bundle_bytes)
  @context_producer_keys @legacy_producer_keys ++ ~w(candidate_context_id repair_parent_sha validation_base)
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
  @candidate_history_depth_limit 3
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
             run <- producer.run,
             control <- field(field(run, :execution) || %{}, :worker_control),
             {:ok, identity} <- validate_completed_worker(producer, control, worker_opts),
             {:ok, root} <- candidate_root(run),
             {:ok, workspace, tree_sha} <- existing_or_import(run, root, producer, identity, control, worker_opts),
             {:ok, boundary} <- snapshot_boundary(workspace, producer) do
          marker = marker_for(run, producer, identity, tree_sha, boundary)
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
         {:ok, validation_index} <- validation_attempt_index(run, attempts, validation_stage_id),
         validation_attempt <- Enum.at(attempts, validation_index),
         validation_attempt_id <- if(is_map(validation_attempt), do: field(validation_attempt, :id), else: nil),
         {:ok, attempt} <- latest_producer_attempt_at(run, attempts, producer_stage_id, validation_index),
         :ok <- same_current_context(run, field(attempt, :id), validation_attempt_id),
         {:ok, authorized_run} <- CandidateContext.view(run, validation_attempt_id),
         {:ok, validation_identity} <- validation_identity(run, validation_attempt_id),
         {:ok, context} <- authorized_candidate_context(authorized_run, validation_identity),
         {:ok, metadata} <- producer_metadata(authorized_run, attempt),
         {:ok, operation, identity} <- registered_operation(authorized_run, attempt),
         :ok <- validate_metadata_against_run(authorized_run, metadata, context, validation_identity),
         {:ok, history_seed} <- history_seed(authorized_run, context),
         producer = %{
           stage_id: producer_stage_id,
           stage: stage,
           attempt: attempt,
           attempt_id: field(attempt, :id),
           operation: operation,
           identity: identity,
           metadata: metadata,
           run: authorized_run,
           context: context,
           validation_identity: validation_identity,
           validation_attempt: validation_attempt,
           history_seed: history_seed
         } do
      {:ok, producer}
    else
      false -> {:error, :candidate_transfer_producer_not_pinned}
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_producer_unavailable}
    end
  end

  defp same_current_context(run, producer_attempt_id, validation_attempt_id) when is_binary(validation_attempt_id) do
    if CandidateContext.same_context?(run, producer_attempt_id, validation_attempt_id),
      do: :ok,
      else: {:error, :candidate_transfer_context_not_authorized}
  end

  defp same_current_context(run, producer_attempt_id, nil) do
    with {:ok, producer_view} <- CandidateContext.view(run, producer_attempt_id),
         {:ok, current_view} <- CandidateContext.view(run, nil),
         true <- producer_view.candidate_context_id == current_view.candidate_context_id do
      :ok
    else
      _ -> {:error, :candidate_transfer_context_not_authorized}
    end
  end

  defp validation_identity(run, attempt_id) when is_binary(attempt_id),
    do: CandidateContext.validation_context(run, attempt_id)

  defp validation_identity(run, nil) do
    with {:ok, view} <- CandidateContext.view(run, nil),
         base when is_binary(base) <- field(view.execution, :validation_base),
         identity = %{base_sha: base},
         identity <- if(view.candidate_context_id, do: Map.put(identity, :candidate_context_id, view.candidate_context_id), else: identity) do
      {:ok, identity}
    else
      _ -> {:error, :candidate_transfer_context_not_authorized}
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

  defp latest_producer_attempt_at(_run, attempts, producer_stage_id, validation_index) do
    with producer_index <- producer_attempt_index(attempts, validation_index, producer_stage_id),
         true <- is_integer(producer_index),
         attempt when is_map(attempt) <- Enum.at(attempts, producer_index),
         :ok <- only_completed_checks_between(attempts, producer_index + 1, validation_index) do
      validate_latest_producer_attempt(attempt)
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_producer_attempt_missing}
    end
  end

  defp authorized_candidate_context(run, validation_identity) do
    id = field(validation_identity, :candidate_context_id)
    selector = field(run, :current_candidate_context_id)
    execution = field(run, :execution) || %{}

    cond do
      is_nil(id) and is_nil(selector) and is_nil(field(execution, :candidate_context_id)) and
          is_nil(field(execution, :repair_parent_sha)) ->
        {:ok, nil}

      is_binary(id) and id == selector and valid_sha256?(id) ->
        context = map_fetch(field(run, :candidate_contexts) || %{}, id)

        with true <- is_map(context),
             true <- field(context, :id) == id,
             parent when is_binary(parent) <- field(context, :repair_parent_sha),
             base when is_binary(base) <- field(context, :validation_base),
             true <- valid_git_sha?(parent) and valid_git_sha?(base),
             true <- field(execution, :candidate_context_id) == id,
             true <- field(execution, :repair_parent_sha) == parent,
             true <- field(execution, :validation_base) == base,
             true <- field(validation_identity, :base_sha) == base do
          {:ok, %{"id" => id, "repair_parent_sha" => parent, "validation_base" => base}}
        else
          _ -> {:error, :candidate_transfer_context_not_authorized}
        end

      true ->
        {:error, :candidate_transfer_context_not_authorized}
    end
  end

  defp valid_context_metadata?(metadata) do
    valid_sha256?(metadata["candidate_context_id"]) and
      valid_git_sha?(metadata["repair_parent_sha"]) and valid_git_sha?(metadata["validation_base"]) and
      byte_size(metadata["candidate_commit"]) == byte_size(metadata["repair_parent_sha"]) and
      byte_size(metadata["candidate_commit"]) == byte_size(metadata["validation_base"])
  end

  defp validate_context_metadata(metadata, nil, validation_identity) do
    base = field(validation_identity, :base_sha)

    if Enum.sort(Map.keys(metadata)) == Enum.sort(@legacy_producer_keys) and valid_git_sha?(base) and
         metadata["base_commit"] == base and metadata["candidate_commit"] != base do
      :ok
    else
      {:error, :candidate_transfer_base_or_branch_mismatch}
    end
  end

  defp validate_context_metadata(metadata, context, validation_identity) when is_map(context) do
    with true <- Enum.sort(Map.keys(metadata)) == Enum.sort(@context_producer_keys),
         true <- metadata["candidate_context_id"] == context["id"],
         true <- metadata["repair_parent_sha"] == context["repair_parent_sha"],
         true <- metadata["validation_base"] == context["validation_base"],
         true <- metadata["base_commit"] == context["repair_parent_sha"],
         true <- field(validation_identity, :base_sha) == context["validation_base"],
         true <- metadata["candidate_commit"] != context["repair_parent_sha"] do
      :ok
    else
      _ -> {:error, :candidate_transfer_base_or_branch_mismatch}
    end
  end

  defp history_seed(_run, nil), do: {:ok, nil}

  defp history_seed(run, %{"id" => id}) when is_binary(id), do: history_seed(run, id, MapSet.new(), 0)

  defp history_seed(_run, _context), do: {:error, :candidate_transfer_context_not_authorized}

  defp history_seed(run, id, seen, depth) do
    with true <- depth < @candidate_history_depth_limit,
         false <- MapSet.member?(seen, id),
         context when is_map(context) <- map_fetch(field(run, :candidate_contexts) || %{}, id),
         true <- field(context, :id) == id,
         true <- field(context, :run_id) == field(run, :id) and field(context, :task_id) == field(run, :task_id),
         parent when is_binary(parent) <- field(context, :repair_parent_sha),
         validation_base when is_binary(validation_base) <- field(context, :validation_base),
         true <- valid_git_sha?(parent) and valid_git_sha?(validation_base),
         source_context_id <- field(context, :source_context_id),
         {:ok, source_context} <- historical_context(run, source_context_id),
         {:ok, publication, published} <- historical_publication(run, context, source_context_id, parent),
         {:ok, validation_stage, validation_attempt} <- historical_validation(run, publication, source_context_id),
         :ok <- authenticate_historical_validation(run, validation_stage, validation_attempt, published, source_context),
         {:ok, producer} <- historical_producer(run, validation_attempt, published, source_context, MapSet.put(seen, id), depth + 1),
         {:ok, root} <- candidate_root(run),
         path <- snapshot_path(root, run, producer.metadata),
         {:ok, _tree_sha} <- verify_snapshot(run, root, producer, producer.identity, path),
         :ok <- context_ancestry(path, validation_base, parent),
         {:ok, marker} <- read_marker(path),
         {:ok, boundary} <- marker_boundary(marker) do
      {:ok,
       %{
         path: path,
         metadata: producer.metadata,
         marker: marker,
         boundary: boundary,
         validation_base: validation_base,
         repair_parent_sha: parent,
         source_context_id: source_context_id,
         publication_attempt_id: field(context, :publication_attempt_id)
       }}
    else
      false -> {:error, :candidate_transfer_context_history_invalid}
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_context_history_invalid}
    end
  rescue
    _ -> {:error, :candidate_transfer_context_history_invalid}
  end

  defp historical_context(_run, nil), do: {:ok, nil}

  defp historical_context(run, id) when is_binary(id) do
    context = map_fetch(field(run, :candidate_contexts) || %{}, id)

    with true <- is_map(context),
         true <- field(context, :id) == id,
         true <- field(context, :run_id) == field(run, :id) and field(context, :task_id) == field(run, :task_id),
         parent when is_binary(parent) <- field(context, :repair_parent_sha),
         base when is_binary(base) <- field(context, :validation_base),
         true <- valid_git_sha?(parent) and valid_git_sha?(base) do
      {:ok, %{"id" => id, "repair_parent_sha" => parent, "validation_base" => base}}
    else
      _ -> {:error, :candidate_transfer_context_history_invalid}
    end
  end

  defp historical_context(_run, _id), do: {:error, :candidate_transfer_context_history_invalid}

  defp historical_publication(run, context, source_context_id, parent) do
    publication_id = field(context, :publication_attempt_id)
    publication = attempt_by_id(run, publication_id)
    evidence = field(field(publication || %{}, :result) || %{}, :evidence) || %{}
    pr = field(evidence, :pr)
    expected_pr = field(context, :pr)

    with true <- is_binary(publication_id) and is_map(publication),
         true <- normalized(field(publication, :type)) == :publish,
         true <- normalized(field(publication, :status)) == :completed and normalized(field(publication, :gate)) == :passed,
         true <- field(publication, :candidate_context_id) == source_context_id,
         true <- field(evidence, :candidate_sha) == parent,
         true <- is_map(pr) and is_map(expected_pr),
         true <- Enum.all?(~w(number url head_branch base_branch)a, &(field(pr, &1) == field(expected_pr, &1))),
         true <- field(pr, :head_branch) == field(run, :branch) and field(pr, :base_branch) == "main",
         validation_id when is_binary(validation_id) <- field(evidence, :validation_attempt_id),
         {:ok, normalized_evidence} <- json_value(evidence) do
      {:ok, publication, normalized_evidence}
    else
      _ -> {:error, :candidate_transfer_context_publication_invalid}
    end
  end

  defp historical_validation(run, publication, source_context_id) do
    evidence = field(field(publication, :result) || %{}, :evidence) || %{}
    validation_id = field(evidence, :validation_attempt_id)
    validation_attempt = attempt_by_id(run, validation_id)
    stages = field(field(run, :definition) || %{}, :stages) || %{}
    publication_stage = map_fetch(stages, field(publication, :stage))
    validation_stage_id = field(field(publication_stage || %{}, :gate) || %{}, :validation_stage)
    validation_stage = map_fetch(stages, validation_stage_id)

    with true <- is_binary(validation_id) and is_map(validation_attempt),
         true <- normalized(field(validation_attempt, :type)) == :check,
         true <- field(validation_attempt, :stage) == validation_stage_id,
         true <- normalized(field(validation_attempt, :status)) == :completed and normalized(field(validation_attempt, :gate)) == :passed,
         true <- field(validation_attempt, :candidate_context_id) == source_context_id,
         :ok <- candidate_validation_stage(validation_stage) do
      {:ok, validation_stage, validation_attempt}
    else
      _ -> {:error, :candidate_transfer_context_validation_missing}
    end
  end

  defp authenticate_historical_validation(run, validation_stage, validation_attempt, publication_evidence, source_context) do
    result = field(validation_attempt, :result) || %{}
    evidence = field(result, :evidence) || %{}
    receipt = field(evidence, :receipt)
    validation_id = field(validation_attempt, :id)
    source_id = if source_context, do: source_context["id"], else: nil
    source_base = if source_context, do: source_context["validation_base"], else: field(original_execution(run), :validation_base)

    context = %{
      task_id: field(run, :task_id),
      run_id: field(run, :id),
      attempt_id: validation_id,
      base_sha: source_base
    }

    context = if source_id, do: Map.put(context, :candidate_context_id, source_id), else: context
    expected_context = Jason.encode!(context) |> Jason.decode!()
    archive = field(original_execution(run), :validation_archive)
    policy = field(original_execution(run), :validation_policy)

    with true <- is_binary(archive) and is_binary(source_base) and is_map(receipt),
         {:ok, manifest} <- Validation.read_receipt(receipt, archive),
         {:ok, _feedback} <- Validation.feedback(receipt, archive),
         true <- manifest["candidate_sha"] == field(publication_evidence, :candidate_sha),
         true <- manifest["base_sha"] == source_base,
         true <- manifest["context"] == expected_context,
         true <- manifest["complete"] == true and manifest["mode"] == "final",
         true <- manifest["policy_digest"] == field(policy, :digest),
         true <- manifest["policy_revision"] == field(policy, :revision),
         true <- manifest["environment"] == field(policy, :environment) |> Jason.encode!() |> Jason.decode!(),
         %{verdict: :passed} <- ValidationPolicy.gate(manifest, field(field(validation_stage, :gate) || %{}, :required)) do
      :ok
    else
      _ -> {:error, :candidate_transfer_context_receipt_invalid}
    end
  rescue
    _ -> {:error, :candidate_transfer_context_receipt_invalid}
  end

  defp historical_producer(run, validation_attempt, publication_evidence, source_context, seen, depth) do
    execution = original_execution(run)
    producer_stage_id = field(execution, :candidate_producer_stage)
    attempts = field(run, :attempts) || []
    stages = field(field(run, :definition) || %{}, :stages) || %{}
    producer_stage = map_fetch(stages, producer_stage_id)
    validation_index = Enum.find_index(attempts, &(field(&1, :id) == field(validation_attempt, :id)))
    validation_identity = %{base_sha: if(source_context, do: source_context["validation_base"], else: field(execution, :validation_base))}
    validation_identity = if source_context, do: Map.put(validation_identity, :candidate_context_id, source_context["id"]), else: validation_identity

    with true <- is_binary(producer_stage_id) and is_integer(validation_index),
         :ok <- validate_producer_definition(producer_stage, producer_stage_id, field(execution, :candidate_producer_command_sha256)),
         {:ok, attempt} <- latest_producer_attempt_at(run, attempts, producer_stage_id, validation_index),
         true <- field(attempt, :candidate_context_id) == if(source_context, do: source_context["id"], else: nil),
         {:ok, metadata} <- historical_producer_metadata(run, attempt),
         true <- metadata["candidate_commit"] == field(publication_evidence, :candidate_sha),
         :ok <- validate_context_metadata(metadata, source_context, validation_identity),
         true <- metadata["branch"] == "refs/heads/" <> field(run, :branch),
         true <- metadata["candidate_bundle"] == relative_bundle_path(metadata["candidate_input_sha256"]),
         {:ok, operation, identity} <- registered_operation(run, attempt),
         {:ok, parent_history} <- history_seed_for_producer(run, metadata, seen, depth) do
      {:ok,
       %{
         stage_id: producer_stage_id,
         stage: producer_stage,
         attempt: attempt,
         attempt_id: field(attempt, :id),
         operation: operation,
         identity: identity,
         metadata: metadata,
         run: run,
         context: source_context,
         history_seed: parent_history
       }}
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_context_producer_invalid}
    end
  end

  defp history_seed_for_producer(run, metadata, seen, depth) do
    case metadata["candidate_context_id"] do
      nil -> {:ok, nil}
      id when is_binary(id) -> history_seed(run, id, seen, depth)
      _ -> {:error, :candidate_transfer_context_producer_invalid}
    end
  end

  defp historical_producer_metadata(run, attempt) do
    evidence = field(field(attempt, :result) || %{}, :evidence) || %{}
    output = field(evidence, :output)

    with true <- is_binary(output),
         {:ok, metadata} <- Jason.decode(output),
         :ok <- validate_producer_payload(metadata),
         {:ok, _workspace} <- original_workspace(run) do
      {:ok, metadata}
    else
      _ -> {:error, :candidate_transfer_context_producer_invalid}
    end
  end

  defp context_ancestry(workspace, validation_base, parent) do
    with true <- valid_git_sha?(validation_base) and valid_git_sha?(parent),
         {:ok, parent_object} <- CandidateGit.run(workspace, ["rev-parse", "--verify", parent <> "^{commit}"]),
         true <- String.trim(parent_object) == parent,
         {:ok, base_object} <- CandidateGit.run(workspace, ["rev-parse", "--verify", validation_base <> "^{commit}"]),
         true <- String.trim(base_object) == validation_base,
         {:ok, _} <- CandidateGit.run(workspace, ["merge-base", "--is-ancestor", validation_base, parent]) do
      :ok
    else
      _ -> {:error, :candidate_transfer_validation_base_not_ancestor}
    end
  end

  defp verify_context_lineage(workspace, producer) do
    if is_map(producer.context) do
      with :ok <- context_ancestry(workspace, producer.metadata["validation_base"], producer.metadata["repair_parent_sha"]),
           :ok <- context_ancestry(workspace, producer.metadata["validation_base"], producer.metadata["candidate_commit"]) do
        :ok
      end
    else
      :ok
    end
  end

  defp allowed_original_heads(metadata, producer) do
    if valid_context_metadata?(metadata) and is_binary(metadata["candidate_context_id"]) do
      original_base = field(original_execution(producer.run), :validation_base)

      Enum.uniq([metadata["validation_base"], metadata["repair_parent_sha"], metadata["candidate_commit"], original_base])
    else
      [metadata["base_commit"], metadata["candidate_commit"]]
    end
  end

  defp marker_boundary(marker) when is_map(marker) do
    commit = field(marker, :shallow_base_commit)
    tree = field(marker, :shallow_base_tree)

    cond do
      is_nil(commit) and is_nil(tree) -> {:ok, nil}
      valid_git_sha?(commit) and valid_git_sha?(tree) -> {:ok, %{commit: commit, tree: tree}}
      true -> {:error, :candidate_transfer_shallow_boundary_invalid}
    end
  end

  defp marker_boundary(_marker), do: {:error, :candidate_transfer_shallow_boundary_invalid}

  defp json_value(value) do
    {:ok, Jason.encode!(value) |> Jason.decode!()}
  rescue
    _ -> {:error, :candidate_transfer_context_publication_invalid}
  end

  defp attempt_by_id(run, id) when is_binary(id), do: Enum.find(field(run, :attempts) || [], &(field(&1, :id) == id))
  defp attempt_by_id(_run, _id), do: nil

  defp original_execution(run) do
    field(run, :candidate_execution_original) || field(run, :execution) || %{}
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
    keys = Enum.sort(Map.keys(metadata))
    context_metadata = keys == Enum.sort(@context_producer_keys)

    if (context_metadata or keys == Enum.sort(@legacy_producer_keys)) and
         valid_git_sha?(metadata["candidate_commit"]) and
         valid_git_sha?(metadata["base_commit"]) and
         byte_size(metadata["candidate_commit"]) == byte_size(metadata["base_commit"]) and
         valid_sha256?(metadata["candidate_input_sha256"]) and
         valid_sha256?(metadata["bundle_sha256"]) and
         is_integer(metadata["bundle_bytes"]) and metadata["bundle_bytes"] in 1..@bundle_limit and
         is_binary(metadata["branch"]) and valid_branch_ref?(metadata["branch"]) and
         is_binary(metadata["candidate_bundle"]) and
         (not context_metadata or valid_context_metadata?(metadata)) do
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

  defp validate_metadata_against_run(run, metadata, context, validation_identity) do
    branch = field(run, :branch)

    with :ok <- validate_current_candidate_input(run, metadata, context),
         true <- is_binary(branch) and metadata["branch"] == "refs/heads/" <> branch,
         :ok <- validate_context_metadata(metadata, context, validation_identity) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_base_or_branch_mismatch}
    end
  end

  defp validate_current_candidate_input(run, metadata, context) do
    candidate = map_fetch(field(run, :outputs) || %{}, "candidate")
    value = if context, do: %{"candidate" => candidate, "candidate_context" => context}, else: candidate

    with {:ok, encoded} <- canonical_candidate_json(value),
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
    if String.valid?(value) and byte_size(value) <= @candidate_input_limit,
      do: {:ok, python_ascii_json_string(value)},
      else: {:error, :candidate_transfer_candidate_input_unsupported}
  end

  defp canonical_candidate_value(value, depth) when is_list(value) and length(value) <= @candidate_input_collection do
    with {:ok, items} <- canonical_candidate_items(value, depth + 1, [], 0) do
      {:ok, ["[", Enum.intersperse(items, ","), "]"]}
    end
  end

  defp canonical_candidate_value(value, depth) when is_map(value) and map_size(value) <= @candidate_input_collection do
    keys = Map.keys(value)

    if Enum.all?(keys, &(is_binary(&1) and ascii_string?(&1))) do
      keys
      |> Enum.sort()
      |> canonical_candidate_pairs(value, depth + 1, [], 0)
    else
      {:error, :candidate_transfer_candidate_input_unsupported}
    end
  end

  defp canonical_candidate_value(_value, _depth), do: {:error, :candidate_transfer_candidate_input_unsupported}

  defp canonical_candidate_items([], _depth, items, _size), do: {:ok, Enum.reverse(items)}

  defp canonical_candidate_items([value | rest], depth, items, size) do
    with {:ok, encoded} <- canonical_candidate_value(value, depth) do
      next_size = size + IO.iodata_length(encoded) + if(items == [], do: 0, else: 1)

      if next_size + 2 <= @candidate_input_limit,
        do: canonical_candidate_items(rest, depth, [encoded | items], next_size),
        else: {:error, :candidate_transfer_candidate_input_unsupported}
    end
  end

  defp canonical_candidate_pairs([], _map, _depth, pairs, _size), do: {:ok, ["{", Enum.intersperse(Enum.reverse(pairs), ","), "}"]}

  defp canonical_candidate_pairs([key | rest], map, depth, pairs, size) do
    with {:ok, encoded_value} <- canonical_candidate_value(Map.fetch!(map, key), depth) do
      encoded_key = python_ascii_json_string(key)
      pair = [encoded_key, ":", encoded_value]
      next_size = size + IO.iodata_length(pair) + if(pairs == [], do: 0, else: 1)

      if next_size + 2 <= @candidate_input_limit,
        do: canonical_candidate_pairs(rest, map, depth, [pair | pairs], next_size),
        else: {:error, :candidate_transfer_candidate_input_unsupported}
    end
  end

  defp python_ascii_json_string(value) when is_binary(value) do
    encoded =
      value
      |> String.to_charlist()
      |> Enum.map(&python_ascii_codepoint/1)

    [34, encoded, 34] |> IO.iodata_to_binary()
  end

  defp python_ascii_codepoint(?\"), do: "\\\""
  defp python_ascii_codepoint(?\\), do: "\\\\"
  defp python_ascii_codepoint(?\b), do: "\\b"
  defp python_ascii_codepoint(?\f), do: "\\f"
  defp python_ascii_codepoint(?\n), do: "\\n"
  defp python_ascii_codepoint(?\r), do: "\\r"
  defp python_ascii_codepoint(?\t), do: "\\t"
  defp python_ascii_codepoint(codepoint) when codepoint < 0x20, do: "\\u" <> hex4(codepoint)
  defp python_ascii_codepoint(codepoint) when codepoint < 0x7F, do: <<codepoint>>
  defp python_ascii_codepoint(codepoint) when codepoint <= 0xFFFF, do: "\\u" <> hex4(codepoint)

  defp python_ascii_codepoint(codepoint) when codepoint <= 0x10FFFF do
    value = codepoint - 0x10000
    high = 0xD800 + Bitwise.bsr(value, 10)
    low = 0xDC00 + Bitwise.band(value, 0x3FF)
    "\\u" <> hex4(high) <> "\\u" <> hex4(low)
  end

  defp hex4(codepoint), do: codepoint |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(4, "0")

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
          with :ok <- verify_original_workspace(run, producer),
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

  defp verify_original_workspace(run, producer) do
    metadata = producer.metadata

    with {:ok, workspace} <- original_workspace(run),
         {:ok, head} <- CandidateGit.run(workspace, ["rev-parse", "--verify", "HEAD^{commit}"]),
         {:ok, status} <- CandidateGit.run(workspace, ["status", "--porcelain=v1", "--untracked-files=all"]),
         true <- String.trim(head) in allowed_original_heads(metadata, producer),
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
      source_workspace = if producer.context, do: producer.history_seed.path, else: field(run, :workspace)

      CandidateGit.with_snapshot(source_workspace, fn source_git ->
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
         {:ok, boundary} <- prepare_import_history(git, git_dir, temporary, producer),
         :ok <- File.write(bundle_path, bundle, [:binary, :exclusive]),
         :ok <- File.chmod(bundle_path, 0o600),
         :ok <- verify_bundle(git, git_dir, temporary, bundle_path, producer.metadata),
         :ok <- fetch_bundle(git, git_dir, temporary, bundle_path, producer.metadata),
         :ok <- verify_imported_commits(git, git_dir, temporary, producer.metadata),
         :ok <- verify_context_lineage(temporary, producer),
         :ok <- verify_candidate_tree_budget(git, git_dir, temporary, producer.metadata),
         {:ok, tree_sha} <- checkout_candidate(git, git_dir, temporary, producer.metadata),
         :ok <- File.rm(bundle_path),
         :ok <- ensure_clean_candidate(git, git_dir, temporary, producer.metadata, tree_sha),
         :ok <- verify_expected_boundary(temporary, boundary),
         :ok <- write_marker(temporary, marker_for(run, producer, identity, tree_sha, boundary)),
         :ok <- rename_snapshot(temporary, final_path) do
      {:ok, final_path, tree_sha}
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_import_failed}
    end
  end

  defp prepare_import_history(git, git_dir, workspace, %{context: nil} = producer) do
    with {:ok, boundary} <- prepare_shallow_base(git, git_dir, workspace, producer.metadata) do
      {:ok, boundary}
    end
  end

  defp prepare_import_history(_git, _git_dir, workspace, %{context: context, history_seed: seed} = producer)
       when is_map(context) and is_map(seed) do
    with :ok <- private_boundary_mode(workspace, seed.boundary),
         :ok <- verify_expected_boundary(workspace, seed.boundary),
         :ok <- context_ancestry(workspace, producer.metadata["validation_base"], producer.metadata["repair_parent_sha"]) do
      {:ok, seed.boundary}
    end
  end

  defp prepare_import_history(_git, _git_dir, _workspace, _producer),
    do: {:error, :candidate_transfer_context_history_invalid}

  defp private_boundary_mode(_workspace, nil), do: :ok
  defp private_boundary_mode(workspace, _boundary), do: File.chmod(Path.join([workspace, ".git", "shallow"]), 0o600)

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
      {:ok, %{commit: base, tree: base_tree}}
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

  defp verify_expected_boundary(workspace, expected) do
    with {:ok, actual} <- read_snapshot_boundary(workspace),
         true <- actual == expected do
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
         {:ok, boundary} <- snapshot_boundary(final_path, producer),
         true <- marker_matches_producer?(marker, run, producer, boundary),
         :ok <- validate_snapshot_path(root, final_path),
         :ok <- verify_snapshot_tree(final_path, producer.metadata, marker["tree_sha"]),
         :ok <- verify_context_lineage(final_path, producer) do
      {:ok, final_path}
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_workspace_transfer_marker_mismatch}
    end
  end

  defp verify_snapshot(run, root, producer, identity, path) do
    with {:ok, marker} <- read_marker(path),
         {:ok, boundary} <- snapshot_boundary(path, producer),
         true <- marker == marker_for(run, producer, identity, marker["tree_sha"], boundary),
         :ok <- validate_snapshot_path(root, path),
         :ok <- verify_snapshot_tree(path, producer.metadata, marker["tree_sha"]),
         :ok <- verify_context_lineage(path, producer) do
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
      "shallow_base_commit" => if(base_tree_sha, do: base_tree_sha.commit),
      "shallow_base_tree" => if(base_tree_sha, do: base_tree_sha.tree)
    })
  end

  defp marker_matches_producer?(marker, run, producer, boundary) do
    identity = producer.identity
    expected = marker_for(run, producer, identity, marker["tree_sha"], boundary)
    marker == expected
  end

  defp snapshot_boundary(path, producer) do
    with {:ok, actual} <- read_snapshot_boundary(path),
         :ok <- expected_snapshot_boundary(actual, producer) do
      {:ok, actual}
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_transfer_shallow_boundary_invalid}
    end
  end

  defp expected_snapshot_boundary(actual, %{context: nil, metadata: metadata}) do
    if is_map(actual) and actual.commit == metadata["base_commit"],
      do: :ok,
      else: {:error, :candidate_transfer_shallow_boundary_invalid}
  end

  defp expected_snapshot_boundary(actual, %{context: context, history_seed: seed}) when is_map(context) and is_map(seed) do
    if actual == seed.boundary, do: :ok, else: {:error, :candidate_transfer_shallow_boundary_invalid}
  end

  defp expected_snapshot_boundary(_actual, _producer), do: {:error, :candidate_transfer_shallow_boundary_invalid}

  defp read_snapshot_boundary(workspace) do
    path = Path.join([workspace, ".git", "shallow"])

    case File.lstat(path) do
      {:error, :enoent} ->
        {:ok, nil}

      {:ok, %File.Stat{type: :regular, size: size, mode: mode, links: 1, uid: uid}} when size <= @shallow_file_limit ->
        with {:ok, expected_uid} <- effective_uid(),
             true <- uid == expected_uid and Bitwise.band(mode, 0o777) == 0o600,
             {:ok, contents} <- File.read(path),
             [commit] <- String.split(contents, "\n", trim: true),
             true <- valid_git_sha?(commit),
             {:ok, resolved_commit} <- CandidateGit.run(workspace, ["rev-parse", "--verify", commit <> "^{commit}"]),
             true <- String.trim(resolved_commit) == commit,
             {:ok, resolved_tree} <- CandidateGit.run(workspace, ["rev-parse", "--verify", commit <> "^{tree}"]),
             tree <- String.trim(resolved_tree),
             true <- valid_git_sha?(tree) do
          {:ok, %{commit: commit, tree: tree}}
        else
          _ -> {:error, :candidate_transfer_shallow_boundary_invalid}
        end

      _ ->
        {:error, :candidate_transfer_shallow_boundary_invalid}
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
    legacy = %{
      candidate_commit: metadata["candidate_commit"],
      base_commit: metadata["base_commit"],
      branch: metadata["branch"],
      candidate_input_sha256: metadata["candidate_input_sha256"],
      bundle_sha256: metadata["bundle_sha256"],
      bundle_bytes: metadata["bundle_bytes"],
      producer_attempt_id: marker["producer_attempt_id"]
    }

    if is_binary(metadata["candidate_context_id"]) do
      Map.merge(legacy, %{
        candidate_context_id: metadata["candidate_context_id"],
        repair_parent_sha: metadata["repair_parent_sha"],
        validation_base: metadata["validation_base"]
      })
    else
      legacy
    end
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

  defp normalized(value) when value in [:check, :publish, :completed, :passed, :ok, :candidate_validation], do: value
  defp normalized(value) when value in ["check", "publish", "completed", "passed", "ok", "candidate_validation"], do: String.to_existing_atom(value)
  defp normalized(_value), do: nil

  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
