defmodule SymphonyElixir.WorkstreamOutputs do
  @moduledoc "Typed retained values bound to the original producer attempt and output identity."

  alias SymphonyElixir.{CandidateContext, EvidenceRedaction, Review, ServiceCheckCompletion}

  @agent_keys ~w(thread_id turn_id session_id model reasoning_effort workspace)
  @command_keys ~w(exit_status output truncated timed_out)
  @context_fields [candidate_context_id: "candidate_context_id", repair_parent_sha: "repair_parent_sha", validation_base: "validation_base"]
  @context_keys Enum.map(@context_fields, &elem(&1, 1))
  @checkout_keys ~w(root workspace source_workspace current_operation_id operation_id number url base_sha head_sha head_branch base_branch rubric_sha256 cloud_thread_id candidate_context_id run_id stage_id contract_sha256 seal)
  @review_keys ~w(base_sha head_sha pr_number pr_url review_id review_url author_login event result locations rubric_sha256 reviewer_thread_id reviewer_session_id actionable disagreement observed_at run_id stage_id operation_id candidate_context_id contract_sha256 seal)

  @spec projection(map(), String.t()) :: {:ok, map()} | {:error, atom()}
  def projection(run, name) do
    with {:ok, artifact} <- Map.fetch(run.artifacts, name),
         true <- artifact.run_id == run.id,
         attempt when is_map(attempt) <- Enum.find(run.attempts, &(&1.id == artifact.attempt_id)),
         %{status: :completed, result: %{status: :ok, evidence: value}} <- attempt,
         true <- run.outputs[name] == value and artifact.sha256 == digest(value),
         stage when is_map(stage) <- run.definition.stages[attempt.stage],
         true <- attempt.type == stage.type,
         true <- name in stage.outputs,
         {:ok, kind, retained} <- classify(run, stage, attempt, value) do
      {:ok, EvidenceRedaction.safe(%{version: 2, kind: kind, output_id: artifact, value: retained})}
    else
      _ -> {:error, :workspace_outputs_not_retained}
    end
  rescue
    _ -> {:error, :workspace_outputs_not_retained}
  end

  defp cloud_metadata(run, attempt, value) do
    repair = is_binary(attempt[:candidate_context_id])
    extra = if repair, do: ~w(review_disagreement repair_explanation), else: []

    with true <- is_map(value) and Map.keys(value) -- (@agent_keys ++ @context_keys ++ extra) == [],
         true <- value["workspace"] == run.workspace and metadata?(Map.take(value, @agent_keys -- ["workspace"])),
         {:ok, context} <- CandidateContext.evidence_context(run, attempt.id),
         true <- context_values?(value, context, repair),
         true <- not repair or (is_boolean(value["review_disagreement"]) and is_binary(value["repair_explanation"]) and String.trim(value["repair_explanation"]) != "") do
      {:ok, if(repair, do: "repair_agent_metadata", else: "agent_metadata"), Map.delete(value, "workspace")}
    else
      _ -> {:error, :opaque_agent_output}
    end
  end

  defp context_values?(value, context, required) do
    Enum.all?(@context_fields, fn {field, key} ->
      (not required and not Map.has_key?(value, key)) or (Map.has_key?(value, key) and value[key] == context[field])
    end)
  end

  defp metadata?(values), do: Enum.all?(values, fn {_key, v} -> is_nil(v) or (is_binary(v) and Regex.match?(~r/\A[A-Za-z0-9_.:-]+\z/, v)) end)

  defp reviewer_report(run, stage, attempt, value) do
    artifact = %{run_id: run.id, attempt_id: attempt.id, sha256: digest(value)}

    with true <- is_map(value) and Map.keys(value) -- (@agent_keys ++ ["review_json"]) == [],
         true <- metadata?(Map.take(value, @agent_keys -- ["workspace"])),
         json when is_binary(json) and byte_size(json) <= 1_048_576 <- value["review_json"],
         {:ok, parsed} <- Jason.decode(json),
         {:ok, submitted, inputs} <- submitted_report(run, stage, attempt, artifact),
         true <- parsed == submitted["result"] and value["thread_id"] == submitted["reviewer_thread_id"] and value["session_id"] == submitted["reviewer_session_id"],
         {:ok, checkout} <- report_checkout(run, stage, attempt, inputs),
         true <- value["workspace"] == checkout["workspace"] do
      # Report locations are annotations in source text, never retained file handles.
      {:ok, "review_agent_report", %{"metadata" => Map.take(value, @agent_keys -- ["workspace"]), "report" => submitted["result"], "locations" => submitted["locations"]}}
    else
      _ -> {:error, :review_report_not_authenticated}
    end
  end

  defp submitted_report(run, reviewer_stage, reviewer, artifact) do
    Enum.find_value(run.attempts, {:error, :review_report_not_authenticated}, fn attempt ->
      stage = run.definition.stages[attempt.stage]

      with %{type: :check, gate: %{evaluator: :review_publication, review_stage: id}} <- stage,
           true <- id == reviewer_stage.id and attempt.status == :completed and attempt.gate == :passed,
           {:ok, inputs} <- ServiceCheckCompletion.inputs(run, attempt.id),
           {:ok, context} <- CandidateContext.evidence_context(run, reviewer.id),
           {:ok, ^context} <- CandidateContext.evidence_context(run, attempt.id),
           true <- Enum.any?(inputs, fn {name, source} -> name in reviewer_stage.outputs and source == artifact end),
           %{status: :ok, evidence: value} <- attempt.result do
        {:ok, value, inputs}
      else
        _ -> nil
      end
    end)
  end

  defp report_checkout(run, reviewer_stage, reviewer, inputs) do
    stage = run.definition.stages[reviewer_stage.review_checkout_stage]

    with source when is_map(source) <- Enum.find_value(stage.outputs, &inputs[&1]),
         attempt when is_map(attempt) <- Enum.find(run.attempts, &(&1.id == source.attempt_id)),
         true <- attempt.stage == stage.id and ServiceCheckCompletion.verified?(run, attempt.id),
         {:ok, context} <- CandidateContext.evidence_context(run, reviewer.id),
         {:ok, ^context} <- CandidateContext.evidence_context(run, attempt.id),
         %{status: :ok, evidence: checkout} <- attempt.result,
         true <- checkout["workspace"] != run.workspace do
      {:ok, checkout}
    else
      _ -> {:error, :review_checkout_not_authenticated}
    end
  end

  defp classify(run, %{type: :agent} = stage, attempt, value) do
    if Review.reviewer?(stage) do
      reviewer_report(run, stage, attempt, value)
    else
      cloud_metadata(run, attempt, value)
    end
  end

  defp classify(run, %{type: :check, gate: %{evaluator: evaluator}}, attempt, value)
       when evaluator in [:candidate_validation, :github_validation, :review_checkout, :review_publication] do
    keys =
      case evaluator do
        :candidate_validation -> ~w(receipt feedback gate)
        :github_validation -> ~w(number head_sha base_sha head_branch base_branch url external_id check_id qualified evidence_revision reason observed_at attestation)
        :review_checkout -> @checkout_keys
        :review_publication -> @review_keys
      end

    if Map.keys(value) -- keys == [] and ServiceCheckCompletion.verified?(run, attempt.id) do
      retained = if evaluator == :review_checkout, do: Map.drop(value, ~w(root workspace source_workspace)), else: value
      {:ok, Atom.to_string(evaluator), retained}
    else
      {:error, :unverified_service_output}
    end
  end

  defp classify(_run, %{type: :check, gate: gate}, _attempt, value) when is_map(gate) do
    keys = if is_map(value), do: Enum.sort(Map.keys(value)), else: []

    known =
      keys == Enum.sort(@command_keys) or
        (keys == Enum.sort(["exit_code" | @command_keys]) and value["exit_code"] in ["none", "exited", "killed", "dumped"])

    if is_list(gate[:command]) and is_map(value) and known and
         is_integer(value["exit_status"]) and is_binary(value["output"]) and is_boolean(value["truncated"]) and is_boolean(value["timed_out"]) do
      # This is stdout text, including any printed filenames; it promises no file bytes.
      {:ok, "command_log", value}
    else
      {:error, :opaque_check_output}
    end
  end

  defp classify(_run, %{type: :publish}, %{gate: :passed}, value) do
    if Map.keys(value) -- ~w(candidate_sha branch pr publication_id evidence_revision validation_attempt_id) == [] and is_map(value["pr"]) and
         Map.keys(value["pr"]) -- ~w(number url head_sha head_branch base_branch state body merged merge_commit_sha merged_at merged_by) == [],
       do: {:ok, "publication", value},
       else: {:error, :opaque_publication_output}
  end

  defp classify(_run, _stage, _attempt, value), do: if(inline?(value), do: {:ok, "inline", value}, else: {:error, :opaque_output})

  @spec inline?(term()) :: boolean()
  def inline?(value) when is_number(value) or is_boolean(value) or is_nil(value), do: true
  def inline?(value) when is_list(value), do: Enum.all?(value, &inline?/1)
  def inline?(value) when is_map(value), do: Enum.all?(value, fn {_key, nested} -> inline?(nested) end)
  def inline?(_value), do: false

  defp digest(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value)) |> Base.encode16(case: :lower)
end
