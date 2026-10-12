defmodule SymphonyElixir.ServiceCheckCompletion do
  @moduledoc "Durable coordinator attestations for verified service checks; external workers still need fresh termination."

  alias SymphonyElixir.{CandidateContext, PathSafety}

  @legacy_domain "service-check-completion-v1\n"
  @domain "service-check-completion-v2\n"
  @service_evaluators [:candidate_validation, :github_validation, :review_checkout, :review_publication]
  @execution_pins [:validation_policy, :validation_base, :validation_archive, :validation_scratch, :validation_candidates, :validation_check, :worker_control]

  @doc "Called only after the coordinator recomputes a supported service gate."
  @spec record(map()) :: map()
  def record(run) do
    id = run.current_attempt_id

    with true <- run.status in [:executing, :reconciling],
         {:ok, _} <- CandidateContext.view(run, id),
         {:ok, body} <- body(run, id, 2),
         true <- field(body.gate_detail, :verified) == true,
         {:ok, key} <- key(run) do
      proof = %{version: 2, sha256: digest(body), mac: mac(body, key, @domain), input_ids: body.input_ids}
      put_in(run.operations[id][:service_completion], proof)
    else
      _ -> run
    end
  rescue
    _ -> run
  end

  @spec verified?(map(), String.t()) :: boolean()
  def verified?(run, id) do
    with %{version: version, sha256: sha, mac: signature} when version in [1, 2] <- run.operations[id][:service_completion],
         {:ok, body} <- body(run, id, version, if(version == 2, do: run.operations[id].service_completion[:input_ids], else: %{})),
         true <- field(body.gate_detail, :verified) == true,
         {:ok, key} <- key(run) do
      domain = if version == 1, do: @legacy_domain, else: @domain
      sha == digest(body) and signature == mac(body, key, domain)
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  @doc "Returns the producer identities captured by an authenticated service completion."
  @spec inputs(map(), String.t()) :: {:ok, map()} | {:error, atom()}
  def inputs(run, id) do
    if verified?(run, id) and is_map(run.operations[id].service_completion[:input_ids]), do: {:ok, run.operations[id].service_completion.input_ids}, else: {:error, :service_inputs_not_authenticated}
  end

  defp body(run, id, version, inputs \\ :current) do
    operation = run.operations[id]
    attempt = Enum.find(run.attempts, &(&1.id == id))
    stage = if attempt, do: run.definition.stages[attempt.stage], else: nil

    with %{id: ^id, type: :check, status: :completed} <- operation,
         %{id: ^id, type: :check, status: :completed, result: %{status: :ok}} <- attempt,
         true <- operation.stage == attempt.stage,
         %{type: :check, gate: %{evaluator: evaluator}} <- stage,
         true <- evaluator in @service_evaluators,
         true <- is_map(attempt[:gate_detail]) do
      body = %{
        version: 1,
        run_id: run.id,
        task_id: run.task_id,
        policy: run.policy,
        workspace: run.workspace,
        execution: Map.take(run.execution, @execution_pins),
        definitions: run.definition.definitions,
        stage: stage,
        operation_id: id,
        result: attempt.result,
        gate: attempt.gate,
        gate_detail: attempt.gate_detail
      }

      versioned_body(run, attempt, operation, evaluator, body, version, inputs)
    else
      _ -> {:error, :unsupported_service_completion}
    end
  end

  # Legacy seals are readable only for their original context-free, service-only contract.
  defp versioned_body(run, attempt, operation, evaluator, body, 1, _inputs) do
    with {:ok, %{candidate_context_id: nil}} <- CandidateContext.evidence_context(run, attempt.id),
         true <- is_nil(operation[:external_process]) and evaluator in [:candidate_validation, :github_validation] do
      {:ok, body}
    else
      _ -> {:error, :unsupported_service_completion}
    end
  end

  defp versioned_body(run, attempt, operation, _evaluator, body, 2, inputs) do
    names = body.stage[:inputs] || []
    inputs = if inputs == :current, do: Map.take(run.artifacts, names), else: inputs

    with {:ok, context} <- CandidateContext.evidence_context(run, attempt.id),
         true <- is_map(inputs) and Map.keys(inputs) -- names == [],
         true <- Enum.all?(inputs, &valid_input?(run, attempt, &1)) do
      {:ok,
       Map.merge(body, %{
         version: 2,
         candidate_context: context,
         external_process: operation[:external_process],
         review_execution: Map.take(run.execution, [:review, :review_rubric_sha256]),
         input_ids: inputs
       })}
    else
      _ -> {:error, :service_context_or_inputs_not_authenticated}
    end
  end

  defp valid_input?(run, consumer, {name, artifact}) do
    with %{run_id: run_id, attempt_id: id, sha256: sha} <- artifact,
         true <- run_id == run.id,
         producer when is_map(producer) <- Enum.find(run.attempts, &(&1.id == id)),
         true <- producer.ordinal < consumer.ordinal and producer.status == :completed,
         %{status: :ok, evidence: value} <- producer.result,
         stage when is_map(stage) <- run.definition.stages[producer.stage],
         true <- name in stage.outputs do
      sha == digest(value)
    else
      _ -> false
    end
  end

  defp key(run) do
    with archive when is_binary(archive) <- run.execution[:validation_archive],
         path = Path.join(archive, "producer.key"),
         {:ok, ^path} <- PathSafety.canonicalize(path),
         false <- overlap?(path, run.workspace),
         {:ok, %{type: :regular, links: 1, mode: mode}} <- File.lstat(path),
         true <- Bitwise.band(mode, 0o777) == 0o600,
         {:ok, key} when byte_size(key) == 32 <- File.read(path) do
      {:ok, key}
    else
      _ -> {:error, :service_completion_key_unavailable}
    end
  end

  defp overlap?(path, root), do: path == root or String.starts_with?(path, root <> "/")
  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
  defp digest(body), do: :crypto.hash(:sha256, :erlang.term_to_binary(body)) |> Base.encode16(case: :lower)
  defp mac(body, key, domain), do: :crypto.mac(:hmac, :sha256, key, domain <> :erlang.term_to_binary(body)) |> Base.encode16(case: :lower)
end
