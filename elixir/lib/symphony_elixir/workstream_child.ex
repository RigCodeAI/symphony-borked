defmodule SymphonyElixir.WorkstreamChild do
  @moduledoc """
  Identity planning and read-only proof checks for one coordinator-owned child handoff.

  The caller prepares and pins the child through the existing workspace/definition
  checks. This module performs no persistence, dispatch or cancellation. Merge
  verification delegates to the service-owned cleanup API and requires retained
  child outputs. The parent and linked child must be committed atomically before
  dispatch. Result correlation alone never authorizes parent advancement.

  Recovery inspection checks saved identities, lineage, pinned definition shape,
  and attempt/operation structure only. A handoff fingerprint cannot be rebuilt
  from the child after it has progressed, so its digest syntax and deterministic
  run IDs are checked without treating the fingerprint or saved proof as authorization.

  This internal identity format is not a Coverage Factory scenario/scorer contract.
  """

  @identity_keys ~w(handoff_id parent_run_id parent_stage_id parent_attempt_id child_run_id fingerprint)a
  @run_pin_keys ~w(task_id issue_id workspace branch definition execution policy outputs stage_id)a
  @recovery_intent_keys [:version, :handoff_id, :parent_run_id, :parent_stage_id, :parent_attempt_id, :child_run_id, :fingerprint]
  @recovery_lineage_keys [:handoff_id, :parent_run_id, :parent_stage_id, :parent_attempt_id, :fingerprint]
  @recovery_attempt_keys [:id, :stage, :type, :ordinal, :status, :result, :gate, :gate_detail]
  @recovery_operation_keys [:id, :attempt_id, :stage, :type, :status, :worker, :side_effect_id, :transport_retries, :child_handoff]
  @recovery_completed_operation_keys @recovery_operation_keys ++ [:completion_proof]
  @recovery_child_definition_keys [:version, :name, :inputs, :input_values, :entry, :stages, :agents, :definitions]

  @doc "Resolves only the declared parent values into the pinned child's required inputs."
  @spec inputs(map(), map()) :: {:ok, map()} | {:error, atom()}
  def inputs(%{input_mapping: mapping, child_definition: %{inputs: required}}, outputs)
      when is_map(mapping) and is_list(required) and is_map(outputs) do
    valid_mapping = Enum.sort(Map.keys(mapping)) == Enum.sort(required) and Enum.all?(mapping, fn {key, source} -> is_binary(key) and is_binary(source) end)

    if valid_mapping do
      Enum.reduce_while(mapping, {:ok, %{}}, fn {key, source}, {:ok, resolved} ->
        case Map.fetch(outputs, source) do
          {:ok, value} when not is_nil(value) -> {:cont, {:ok, Map.put(resolved, key, value)}}
          _ -> {:halt, {:error, :missing_child_input}}
        end
      end)
    else
      {:error, :invalid_child_input_mapping}
    end
  end

  def inputs(_stage, _outputs), do: {:error, :invalid_child_input_mapping}

  @doc "Plans stable identities from an undispatched parent stage and prepared software-change child."
  @spec prepare(map(), map()) :: {:ok, map(), map()} | {:error, atom()}
  def prepare(parent, child) when is_map(parent) and is_map(child) do
    with :ok <- ready_parent(parent),
         :ok <- fresh_child(child),
         :ok <- distinct_ownership(parent, child),
         stage = parent.definition.stages[parent.stage_id],
         {:ok, inputs} <- inputs(stage, parent.outputs),
         :ok <- bound_inputs(child, inputs),
         attempt_id = "#{parent.id}/#{parent.stage_id}/#{length(parent.attempts) + 1}",
         handoff_id = "child-" <> digest([parent.id, attempt_id]),
         child_id = "run-" <> binary_part(digest(handoff_id), 0, 32),
         pins = %{
           parent_stage: stage,
           parent: Map.take(parent, @run_pin_keys),
           parent_artifacts: parent.artifacts,
           child: Map.take(child, @run_pin_keys)
         },
         true <- portable?(pins) do
      intent = %{
        version: 1,
        handoff_id: handoff_id,
        parent_run_id: parent.id,
        parent_stage_id: parent.stage_id,
        parent_attempt_id: attempt_id,
        child_run_id: child_id,
        fingerprint: digest(pins)
      }

      lineage = Map.take(intent, ~w(handoff_id parent_run_id parent_stage_id parent_attempt_id fingerprint)a)
      linked = child |> Map.put(:id, child_id) |> Map.put(:parent_handoff, lineage) |> Map.put(:parent_run_id, parent.id)
      {:ok, intent, linked}
    else
      false -> {:error, :nonportable_child_inputs}
      {:error, _} = error -> error
    end
  rescue
    _ -> {:error, :invalid_child_handoff}
  end

  def prepare(_parent, _child), do: {:error, :invalid_child_handoff}

  @doc "Checks that replay names the same operation and immutable input pins."
  @spec replay(map(), map()) :: :ok | {:error, atom()}
  def replay(saved, proposed) when is_map(saved) and is_map(proposed) do
    cond do
      not valid_intent?(saved) or not valid_intent?(proposed) -> {:error, :invalid_child_handoff}
      Map.take(saved, @identity_keys -- [:fingerprint]) != Map.take(proposed, @identity_keys -- [:fingerprint]) -> {:error, :child_handoff_identity_mismatch}
      saved.fingerprint != proposed.fingerprint -> {:error, :child_handoff_inputs_changed}
      true -> :ok
    end
  end

  def replay(_saved, _proposed), do: {:error, :invalid_child_handoff}

  @doc "Correlates service result identities only; it does not verify merge or evidence."
  @spec correlate(map(), map()) :: :ok | {:error, atom()}
  def correlate(intent, result_identity) when is_map(intent) and is_map(result_identity) do
    if valid_intent?(intent) and Map.take(result_identity, @identity_keys) == Map.take(intent, @identity_keys),
      do: :ok,
      else: {:error, :child_result_identity_mismatch}
  end

  def correlate(_intent, _result_identity), do: {:error, :child_result_identity_mismatch}

  @doc "Validates saved child links without proving, replaying or advancing any handoff."
  @spec inspect_recovery_links(term()) :: :ok | {:error, :invalid_child_recovery_link}
  def inspect_recovery_links(runs) when is_list(runs) do
    with true <- Enum.all?(runs, &is_map/1),
         {:ok, by_id} <- recovery_run_index(runs),
         :ok <- recovery_link_counts(runs),
         :ok <- recovery_parent_links(runs, by_id),
         :ok <- recovery_child_links(runs, by_id) do
      :ok
    else
      _other -> {:error, :invalid_child_recovery_link}
    end
  rescue
    _error -> {:error, :invalid_child_recovery_link}
  catch
    _kind, _reason -> {:error, :invalid_child_recovery_link}
  end

  def inspect_recovery_links(_runs), do: {:error, :invalid_child_recovery_link}

  @doc "Reads the service merge proof for the exact waiting handoff; never advances the parent."
  @spec completion_proof(map(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def completion_proof(parent, child, opts \\ []) do
    with %{status: :waiting_for_child, child_handoff: intent} <- parent,
         true <- valid_intent?(intent),
         true <- SymphonyElixir.WorkstreamRun.compatible_policy?(parent),
         true <- parent.id == intent.parent_run_id and parent.stage_id == intent.parent_stage_id and parent.current_attempt_id == intent.parent_attempt_id,
         %{type: :child, status: :waiting_for_child, child_handoff: ^intent} <- parent.operations[intent.parent_attempt_id],
         %{type: :child, status: :waiting, stage: stage} <- Enum.find(parent.attempts, &(&1.id == intent.parent_attempt_id)),
         true <- stage == intent.parent_stage_id,
         true <- child[:parent_run_id] == parent.id and is_map(child[:parent_handoff]),
         :ok <- correlate(intent, Map.put(child.parent_handoff, :child_run_id, child.id)),
         true <- child.status in [:complete, :waiting_for_answer],
         :ok <- retained_child_outputs(child),
         {:ok, proof} <- SymphonyElixir.WorkstreamCleanup.merge_proof(child, opts),
         true <- proof.run_id == child.id and proof.task_id == child.task_id and proof.retained_outputs == child.retained_output_artifacts,
         true <- proof[:retained_outputs_complete] == true,
         {:ok, outputs} <- retained_values(child, opts) do
      {:ok, Map.put(proof, :outputs, outputs)}
    else
      {:error, _} = error -> error
      _ -> {:error, :child_completion_not_current}
    end
  rescue
    _ -> {:error, :child_completion_not_current}
  end

  defp retained_child_outputs(child) do
    artifacts = child[:artifacts]
    refs = child[:retained_output_artifacts]

    if is_map(artifacts) and map_size(artifacts) > 0 and is_map(refs) and Enum.sort(Map.keys(refs)) == Enum.sort(Map.keys(artifacts)) and
         Enum.all?(artifacts, fn {name, id} -> is_map(refs[name]) and refs[name][:version] == 2 and refs[name][:output_id] == id and Map.has_key?(child.outputs, name) end),
       do: :ok,
       else: {:error, :child_outputs_not_retained}
  end

  defp retained_values(child, opts) do
    Enum.reduce_while(Map.keys(child.artifacts), {:ok, %{}}, fn name, {:ok, values} ->
      case SymphonyElixir.WorkstreamCleanup.retained_output(child, name, opts) do
        {:ok, %{"version" => 2} = value} -> {:cont, {:ok, Map.put(values, name, value)}}
        _ -> {:halt, {:error, :child_outputs_not_retained}}
      end
    end)
  end

  @doc "Names the linked child of a stopped parent for the existing cancellation owner."
  @spec cancellation_target(map(), map(), map()) :: {:ok, String.t()} | {:error, atom()}
  def cancellation_target(parent, child, intent) when is_map(parent) and is_map(child) and is_map(intent) do
    lineage = Map.take(intent, ~w(handoff_id parent_run_id parent_stage_id parent_attempt_id fingerprint)a)
    operation = get_in(parent, [:operations, intent[:parent_attempt_id]])
    attempt = Enum.find(parent[:attempts] || [], &(&1[:id] == intent[:parent_attempt_id]))

    if valid_intent?(intent) and parent[:status] == :stopped and parent[:id] == intent.parent_run_id and parent[:child_handoff] == intent and
         parent[:stage_id] == intent.parent_stage_id and parent[:current_attempt_id] == intent.parent_attempt_id and
         match?(%{type: :child, status: :waiting_for_child, child_handoff: ^intent}, operation) and match?(%{type: :child, status: :waiting}, attempt) and
         child[:id] == intent.child_run_id and child[:parent_run_id] == parent.id and child[:parent_handoff] == lineage,
       do: {:ok, child.id},
       else: {:error, :unowned_child_cancellation}
  end

  def cancellation_target(_parent, _child, _intent), do: {:error, :unowned_child_cancellation}

  defp recovery_run_index(runs) do
    Enum.reduce_while(runs, {:ok, %{}}, fn run, {:ok, index} ->
      case run[:id] do
        id when is_binary(id) and id != "" ->
          if Map.has_key?(index, id),
            do: {:halt, {:error, :invalid_child_recovery_link}},
            else: {:cont, {:ok, Map.put(index, id, run)}}

        _other ->
          {:halt, {:error, :invalid_child_recovery_link}}
      end
    end)
  end

  defp recovery_link_counts(runs) do
    parents = Enum.filter(runs, &recovery_parent_candidate?/1)
    children = Enum.filter(runs, &(Map.has_key?(&1, :parent_run_id) or Map.has_key?(&1, :parent_handoff)))

    if Enum.all?(parents, &(Map.has_key?(&1, :child_handoff) and not Map.has_key?(&1, :parent_run_id) and not Map.has_key?(&1, :parent_handoff))) and
         Enum.all?(children, &(not Map.has_key?(&1, :child_handoff) and not recovery_child_operations?(&1))) do
      :ok
    else
      {:error, :invalid_child_recovery_link}
    end
  end

  defp recovery_parent_candidate?(run) do
    Map.has_key?(run, :child_handoff) or recovery_child_operations?(run) or
      run[:status] == :waiting_for_child or run[:phase] == :waiting_for_child
  end

  defp recovery_child_operations?(run) do
    attempts = run[:attempts]
    operations = run[:operations]

    (is_list(attempts) and Enum.any?(attempts, &(is_map(&1) and &1[:type] == :child))) or
      (is_map(operations) and Enum.any?(operations, fn {_id, operation} -> is_map(operation) and operation[:type] == :child end))
  end

  defp recovery_parent_links(runs, by_id) do
    runs
    |> Enum.filter(&recovery_parent_candidate?/1)
    |> Enum.reduce_while(:ok, fn parent, :ok ->
      case validate_recovery_parent_links(parent, by_id) do
        :ok -> {:cont, :ok}
        _other -> {:halt, {:error, :invalid_child_recovery_link}}
      end
    end)
  end

  defp recovery_child_links(runs, by_id) do
    runs
    |> Enum.filter(&(Map.has_key?(&1, :parent_run_id) or Map.has_key?(&1, :parent_handoff)))
    |> Enum.reduce_while(:ok, fn child, :ok ->
      parent_id = child[:parent_run_id]

      result =
        with true <- is_binary(parent_id),
             {:ok, parent} <- Map.fetch(by_id, parent_id),
             true <- is_map(child[:parent_handoff]),
             true <- Enum.sort(Map.keys(child.parent_handoff)) == Enum.sort(@recovery_lineage_keys),
             attempt_id when is_binary(attempt_id) <- child.parent_handoff[:parent_attempt_id],
             operation when is_map(operation) <- get_in(parent, [:operations, attempt_id]),
             intent when is_map(intent) <- operation[:child_handoff],
             true <- valid_recovery_intent?(intent),
             true <- intent[:parent_attempt_id] == attempt_id and intent[:child_run_id] == child[:id],
             true <- child.parent_handoff === Map.take(intent, @recovery_lineage_keys) do
          :ok
        else
          _other -> {:error, :invalid_child_recovery_link}
        end

      if result == :ok, do: {:cont, :ok}, else: {:halt, result}
    end)
  end

  defp validate_recovery_parent_links(parent, by_id) do
    operations = parent[:operations]
    attempts = parent[:attempts]

    child_operations =
      if is_map(operations), do: Enum.filter(operations, fn {_id, operation} -> is_map(operation) and operation[:type] == :child end), else: []

    child_attempts =
      if is_list(attempts), do: Enum.filter(attempts, &(is_map(&1) and &1[:type] == :child)), else: []

    matching_attempts =
      Enum.all?(child_operations, fn {attempt_id, _operation} ->
        Enum.count(child_attempts, &(&1[:id] == attempt_id)) == 1
      end)

    latest_intent =
      child_attempts
      |> Enum.max_by(&(&1[:ordinal] || 0), fn -> nil end)
      |> case do
        nil -> nil
        attempt -> get_in(operations, [attempt.id, :child_handoff])
      end

    if child_operations != [] and length(child_operations) == length(child_attempts) and matching_attempts and
         is_map(latest_intent) and parent[:child_handoff] === latest_intent do
      Enum.reduce_while(child_operations, :ok, fn {attempt_id, operation}, :ok ->
        intent = operation[:child_handoff]

        result =
          with true <- is_map(intent) and valid_recovery_intent?(intent),
               true <- intent[:parent_attempt_id] == attempt_id and intent[:parent_run_id] == parent[:id],
               true <- intent[:handoff_id] == recovery_handoff_id(parent[:id], attempt_id),
               true <- intent[:child_run_id] == recovery_child_id(intent[:handoff_id]),
               {:ok, child} <- Map.fetch(by_id, intent[:child_run_id]),
               true <- child[:parent_run_id] == parent[:id],
               true <- child[:parent_handoff] === Map.take(intent, @recovery_lineage_keys),
               :ok <- recovery_ownership_distinct?(parent, child),
               :ok <- recovery_policies_match?(parent, child),
               {:ok, stage} <- recovery_child_stage(parent, intent),
               :ok <- recovery_child_definition_matches?(parent, child, stage),
               {:ok, attempt, saved_operation, ordinal} <- recovery_parent_attempt(parent, intent),
               true <- saved_operation === operation,
               :ok <- recovery_parent_operation(parent, child, intent, attempt, operation, ordinal, stage) do
            :ok
          else
            _other -> {:error, :invalid_child_recovery_link}
          end

        if result == :ok, do: {:cont, :ok}, else: {:halt, result}
      end)
    else
      {:error, :invalid_child_recovery_link}
    end
  end

  defp valid_recovery_intent?(intent) when is_map(intent) do
    Enum.sort(Map.keys(intent)) == Enum.sort(@recovery_intent_keys) and intent[:version] == 1 and
      Enum.all?([:handoff_id, :parent_run_id, :parent_stage_id, :parent_attempt_id, :child_run_id], fn key ->
        is_binary(intent[key]) and intent[key] != ""
      end) and valid_child_digest?(intent[:fingerprint])
  end

  defp valid_recovery_intent?(_intent), do: false

  defp recovery_handoff_id(parent_id, attempt_id),
    do: "child-" <> digest([parent_id, attempt_id])

  defp recovery_child_id(handoff_id),
    do: "run-" <> binary_part(digest(handoff_id), 0, 32)

  defp recovery_ownership_distinct?(parent, child) do
    parent_task = parent[:task_id]
    child_task = child[:task_id]
    parent_workspace = parent[:workspace]
    child_workspace = child[:workspace]
    parent_branch = parent[:branch]
    child_branch = child[:branch]

    valid =
      Enum.all?([parent_task, child_task, parent_workspace, child_workspace, parent_branch, child_branch], &(is_binary(&1) and &1 != ""))

    if valid and parent_task != child_task and not workspace_overlap?(parent_workspace, child_workspace) and parent_branch != child_branch,
      do: :ok,
      else: {:error, :invalid_child_recovery_link}
  end

  defp recovery_policies_match?(parent, child) do
    if is_map(parent[:policy]) and parent[:policy] === child[:policy],
      do: :ok,
      else: {:error, :invalid_child_recovery_link}
  end

  defp recovery_child_stage(parent, intent) do
    definition = parent[:definition]
    stages = if is_map(definition), do: definition[:stages], else: nil
    stage = if is_map(stages), do: Map.get(stages, intent.parent_stage_id), else: nil

    if is_map(stage) and stage[:id] == intent.parent_stage_id and stage[:type] == :child,
      do: {:ok, stage},
      else: {:error, :invalid_child_recovery_link}
  end

  defp recovery_child_definition_matches?(parent, child, stage) do
    parent_definition = parent[:definition]
    pinned_child = stage[:child_definition]
    child_definition = child[:definition]
    input_mapping = stage[:input_mapping]
    child_inputs = if is_map(pinned_child), do: pinned_child[:inputs], else: nil
    stage_inputs = stage[:inputs]
    source_inputs = if is_map(input_mapping), do: Map.values(input_mapping), else: nil

    valid =
      is_map(parent_definition) and is_map(pinned_child) and is_map(child_definition) and
        Enum.sort(Map.keys(pinned_child)) == Enum.sort(@recovery_child_definition_keys) and
        Enum.sort(Map.keys(child_definition)) == Enum.sort(@recovery_child_definition_keys) and
        Map.drop(pinned_child, [:input_values]) === Map.drop(child_definition, [:input_values]) and
        pinned_child[:version] == 1 and pinned_child[:name] == "software-change" and
        is_list(child_inputs) and unique_names?(child_inputs) and is_map(input_mapping) and
        Enum.sort(Map.keys(input_mapping)) == Enum.sort(child_inputs) and
        is_list(stage_inputs) and unique_names?(stage_inputs) and Enum.sort(Enum.uniq(source_inputs)) == Enum.sort(stage_inputs) and
        pinned_child[:input_values] == Map.new(input_mapping, fn {key, source} -> {key, {:mapped_parent_input, source}} end) and
        is_map(child_definition[:input_values]) and Enum.sort(Map.keys(child_definition.input_values)) == Enum.sort(child_inputs) and
        is_map(pinned_child[:stages]) and map_size(pinned_child.stages) > 0 and
        not Enum.any?(pinned_child.stages, fn {_id, child_stage} -> is_map(child_stage) and child_stage[:type] == :child end) and
        is_map(pinned_child[:definitions]) and is_map(parent_definition[:definitions]) and
        Enum.all?(pinned_child[:definitions], fn {path, resource} ->
          is_binary(path) and is_map(resource) and parent_definition.definitions[path] === resource
        end)

    if valid, do: :ok, else: {:error, :invalid_child_recovery_link}
  end

  defp unique_names?(values), do: Enum.all?(values, &(is_binary(&1) and &1 != "")) and length(values) == length(Enum.uniq(values))

  defp recovery_parent_attempt(parent, intent) do
    attempts = parent[:attempts]
    operations = parent[:operations]

    if is_list(attempts) and is_map(operations) do
      matches =
        attempts
        |> Enum.with_index(1)
        |> Enum.filter(fn {attempt, _ordinal} -> is_map(attempt) and attempt[:id] == intent.parent_attempt_id end)

      case matches do
        [{attempt, ordinal}] ->
          operation = Map.get(operations, intent.parent_attempt_id)

          if is_map(operation),
            do: {:ok, attempt, operation, ordinal},
            else: {:error, :invalid_child_recovery_link}

        _other ->
          {:error, :invalid_child_recovery_link}
      end
    else
      {:error, :invalid_child_recovery_link}
    end
  end

  defp recovery_parent_operation(parent, child, intent, attempt, operation, ordinal, stage) do
    attempt_id = intent.parent_attempt_id

    base_valid =
      Enum.sort(Map.keys(attempt)) == Enum.sort(@recovery_attempt_keys) and
        attempt[:id] == attempt_id and attempt[:stage] == intent.parent_stage_id and attempt[:type] == :child and
        attempt[:ordinal] == ordinal and attempt_id == "#{parent[:id]}/#{intent.parent_stage_id}/#{ordinal}" and
        is_map(operation) and operation[:id] == attempt_id and operation[:attempt_id] == attempt_id and
        operation[:stage] == intent.parent_stage_id and operation[:type] == :child and
        operation[:side_effect_id] == intent.handoff_id and operation[:transport_retries] == 0 and
        operation[:worker] == nil and operation[:child_handoff] === intent

    if base_valid and operation[:status] == :waiting_for_child do
      waiting_parent_valid?(parent, child, attempt, operation, intent, stage)
    else
      completed_parent_valid?(attempt, operation, child, intent)
    end
  end

  defp waiting_parent_valid?(parent, child, attempt, operation, intent, stage) do
    waiting_operation_keys = Enum.sort(Map.keys(operation)) == Enum.sort(@recovery_operation_keys)
    waiting_attempt = attempt[:status] == :waiting and is_nil(attempt[:result]) and is_nil(attempt[:gate]) and is_nil(attempt[:gate_detail])

    waiting_parent =
      parent[:status] in [:waiting_for_child, :stopped] and parent[:stage_id] == intent.parent_stage_id and
        parent[:current_attempt_id] == intent.parent_attempt_id and parent[:child_handoff] === intent and
        ((parent[:status] == :waiting_for_child and parent[:phase] == :waiting_for_child) or
           (parent[:status] == :stopped and parent[:phase] == :stopped))

    if waiting_operation_keys and waiting_attempt and waiting_parent and recovery_child_inputs_bound?(parent, child, stage),
      do: :ok,
      else: {:error, :invalid_child_recovery_link}
  end

  defp recovery_child_inputs_bound?(parent, child, stage) do
    with mapping when is_map(mapping) <- stage[:input_mapping],
         outputs when is_map(outputs) <- parent[:outputs],
         child_definition when is_map(child_definition) <- child[:definition],
         {:ok, expected} <- mapped_recovery_inputs(mapping, outputs) do
      child_definition[:input_values] === expected
    else
      _other -> false
    end
  end

  defp mapped_recovery_inputs(mapping, outputs) do
    Enum.reduce_while(mapping, {:ok, %{}}, fn {key, source}, {:ok, acc} ->
      case Map.fetch(outputs, source) do
        {:ok, value} when not is_nil(value) -> {:cont, {:ok, Map.put(acc, key, value)}}
        _other -> {:halt, {:error, :invalid_child_recovery_link}}
      end
    end)
  end

  defp completed_parent_valid?(attempt, operation, child, intent) do
    completed_operation_keys = Enum.sort(Map.keys(operation)) == Enum.sort(@recovery_completed_operation_keys)
    result = attempt[:result]
    evidence = if is_map(result), do: result[:evidence], else: nil
    detail = attempt[:gate_detail]
    proof = operation[:completion_proof]

    completed_attempt =
      attempt[:status] == :completed and attempt[:gate] == :passed and is_map(result) and
        Enum.sort(Map.keys(result)) == Enum.sort([:status, :evidence]) and result[:status] == :ok and
        is_map(evidence) and evidence["child_run_id"] == intent.child_run_id and is_map(detail) and
        Enum.sort(Map.keys(detail)) == Enum.sort([:child_run_id, :publication_id, :merge_commit]) and
        detail[:child_run_id] == intent.child_run_id and valid_child_digest?(detail[:publication_id]) and valid_child_sha?(detail[:merge_commit])

    completed_operation =
      operation[:status] == :completed and is_map(proof) and proof[:version] == 1 and
        proof[:run_id] == child[:id] and proof[:task_id] == child[:task_id] and proof[:retained_outputs_complete] == true

    proof_merge = if is_map(proof), do: proof[:merge], else: nil
    evidence_merge = if is_map(evidence), do: evidence["merge"], else: nil

    correlated_proof =
      is_map(proof) and proof[:repository] == "RigCodeAI/rig" and is_map(proof_merge) and
        proof[:publication_id] == detail[:publication_id] and proof_merge[:commit] == detail[:merge_commit] and
        is_map(evidence_merge) and evidence_merge["commit"] == detail[:merge_commit]

    if completed_operation_keys and completed_attempt and completed_operation and correlated_proof,
      do: :ok,
      else: {:error, :invalid_child_recovery_link}
  end

  defp ready_parent(parent) do
    with %{status: :ready, current_attempt_id: nil, attempts: attempts, definition: %{stages: stages}} <- parent,
         true <- is_list(attempts) and is_binary(parent[:id]) and parent.id != "",
         %{type: :child} <- stages[parent[:stage_id]],
         true <- not Map.has_key?(parent, :parent_handoff) do
      :ok
    else
      _ -> {:error, :parent_not_ready_for_child}
    end
  end

  defp fresh_child(child) do
    with %{status: :ready, current_attempt_id: nil, attempts: [], operations: operations, artifacts: %{}, definition: %{name: "software-change", stages: stages, entry: entry, input_values: inputs}} <-
           child,
         true <- operations == %{} and child[:artifacts] == %{} and not Map.has_key?(child, :parent_handoff) and not Map.has_key?(child, :parent_run_id),
         true <- child[:stage_id] == entry and child[:outputs] == inputs,
         false <- Enum.any?(stages, fn {_id, stage} -> stage[:type] == :child end) do
      :ok
    else
      _ -> {:error, :child_not_fresh_software_change}
    end
  end

  defp bound_inputs(child, inputs) do
    if child.definition.input_values == inputs,
      do: :ok,
      else: {:error, :child_inputs_not_bound_to_parent}
  end

  defp distinct_ownership(parent, child) do
    valid = Enum.all?([parent[:task_id], child[:task_id], parent[:workspace], child[:workspace], parent[:branch], child[:branch]], &(is_binary(&1) and &1 != ""))

    if valid and parent.task_id != child.task_id and not workspace_overlap?(parent.workspace, child.workspace) and parent.branch != child.branch,
      do: :ok,
      else: {:error, :child_ownership_overlaps_parent}
  end

  defp valid_intent?(%{version: 1, fingerprint: fingerprint} = intent) do
    Enum.all?(@identity_keys, &(is_binary(intent[&1]) and intent[&1] != "")) and
      is_binary(fingerprint) and Regex.match?(~r/\A[0-9a-f]{64}\z/, fingerprint)
  end

  defp valid_intent?(_intent), do: false

  defp valid_child_digest?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)

  defp valid_child_sha?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{40}([0-9a-f]{24})?\z/, value)

  defp workspace_overlap?(parent, child) do
    parent = Path.expand(parent)
    child = Path.expand(child)
    parent == child or String.starts_with?(parent, child <> "/") or String.starts_with?(child, parent <> "/")
  end

  defp portable?(value) when is_atom(value) or is_binary(value) or is_number(value), do: true
  defp portable?(value) when is_list(value), do: Enum.all?(value, &portable?/1)
  defp portable?(value) when is_tuple(value), do: value |> Tuple.to_list() |> Enum.all?(&portable?/1)
  defp portable?(value) when is_map(value), do: Enum.all?(value, fn {key, nested} -> portable?(key) and portable?(nested) end)
  defp portable?(_value), do: false

  defp digest(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic])) |> Base.encode16(case: :lower)
end
