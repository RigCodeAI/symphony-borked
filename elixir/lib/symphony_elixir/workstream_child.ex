defmodule SymphonyElixir.WorkstreamChild do
  @moduledoc """
  Identity planning and read-only proof checks for one coordinator-owned child handoff.

  The caller prepares and pins the child through the existing workspace/definition
  checks. This module performs no persistence, dispatch or cancellation. Merge
  verification delegates to the service-owned cleanup API and requires retained
  child outputs. The parent and linked child must be committed atomically before
  dispatch. Result correlation alone never authorizes parent advancement.

  This internal identity format is not a Coverage Factory scenario/scorer contract.
  """

  @identity_keys ~w(handoff_id parent_run_id parent_stage_id parent_attempt_id child_run_id fingerprint)a
  @run_pin_keys ~w(task_id issue_id workspace branch definition execution policy outputs stage_id)a

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
