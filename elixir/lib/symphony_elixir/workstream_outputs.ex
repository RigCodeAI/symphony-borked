defmodule SymphonyElixir.WorkstreamOutputs do
  @moduledoc "Typed retained values bound to the original producer attempt and output identity."

  alias SymphonyElixir.{EvidenceRedaction, ServiceCheckCompletion}

  @agent_keys ~w(thread_id turn_id session_id model reasoning_effort workspace)
  @command_keys ~w(exit_status output truncated timed_out)

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

  defp classify(run, %{type: :agent}, _attempt, value) do
    if is_map(value) and Map.keys(value) -- @agent_keys == [] and value["workspace"] == run.workspace and
         Enum.all?(Map.delete(value, "workspace"), fn {_key, v} -> is_nil(v) or (is_binary(v) and Regex.match?(~r/\A[A-Za-z0-9_.:-]+\z/, v)) end) do
      {:ok, "agent_metadata", Map.delete(value, "workspace")}
    else
      {:error, :opaque_agent_output}
    end
  end

  defp classify(run, %{type: :check, gate: %{evaluator: evaluator}}, attempt, value)
       when evaluator in [:candidate_validation, :github_validation] do
    keys =
      if evaluator == :candidate_validation,
        do: ~w(receipt feedback gate),
        else: ~w(number head_sha base_sha head_branch base_branch url external_id check_id qualified evidence_revision reason observed_at attestation)

    if Map.keys(value) -- keys == [] and ServiceCheckCompletion.verified?(run, attempt.id), do: {:ok, Atom.to_string(evaluator), value}, else: {:error, :unverified_service_output}
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
