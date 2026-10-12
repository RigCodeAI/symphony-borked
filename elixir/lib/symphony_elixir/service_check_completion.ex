defmodule SymphonyElixir.ServiceCheckCompletion do
  @moduledoc "Durable coordinator attestations for verified service checks, never executable workers."

  alias SymphonyElixir.PathSafety

  @domain "service-check-completion-v1\n"

  @doc "Called only after the coordinator recomputes a supported service gate."
  @spec record(map()) :: map()
  def record(run) do
    id = run.current_attempt_id

    with {:ok, body} <- body(run, id),
         true <- field(body.gate_detail, :verified) == true,
         {:ok, key} <- key(run) do
      proof = %{version: 1, sha256: digest(body), mac: mac(body, key)}
      put_in(run.operations[id][:service_completion], proof)
    else
      _ -> run
    end
  end

  @spec verified?(map(), String.t()) :: boolean()
  def verified?(run, id) do
    with %{version: 1, sha256: sha, mac: signature} <- run.operations[id][:service_completion],
         {:ok, body} <- body(run, id),
         true <- field(body.gate_detail, :verified) == true,
         {:ok, key} <- key(run) do
      sha == digest(body) and signature == mac(body, key)
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  defp body(run, id) do
    operation = run.operations[id]
    attempt = Enum.find(run.attempts, &(&1.id == id))
    stage = if attempt, do: run.definition.stages[attempt.stage], else: nil

    with %{id: ^id, type: :check, status: :completed} <- operation,
         true <- is_nil(operation[:external_process]),
         %{id: ^id, type: :check, status: :completed, result: %{status: :ok}} <- attempt,
         true <- operation.stage == attempt.stage,
         %{type: :check, gate: %{evaluator: evaluator}} <- stage,
         true <- evaluator in [:candidate_validation, :github_validation] do
      {:ok,
       %{
         version: 1,
         run_id: run.id,
         task_id: run.task_id,
         policy: run.policy,
         workspace: run.workspace,
         execution: Map.take(run.execution, [:validation_policy, :validation_base, :validation_archive, :validation_scratch, :validation_candidates, :validation_check, :worker_control]),
         definitions: run.definition.definitions,
         stage: stage,
         operation_id: id,
         result: attempt.result,
         gate: attempt.gate,
         gate_detail: attempt.gate_detail
       }}
    else
      _ -> {:error, :unsupported_service_completion}
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
  defp mac(body, key), do: :crypto.mac(:hmac, :sha256, key, @domain <> :erlang.term_to_binary(body)) |> Base.encode16(case: :lower)
end
