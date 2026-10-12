defmodule SymphonyElixir.CandidateContext do
  @moduledoc """
  Append-only authorization for another candidate on the same published PR.
  Original execution pins and completed attempts remain unchanged. Only the
  coordinator may append a context after authenticating a submitted review.
  """

  @domain "factory-candidate-context-v1\n"

  @spec view(map(), String.t() | nil) :: {:ok, map()} | {:error, term()}
  def view(run, attempt_id) do
    with {:ok, id} <- attempt_context(run, attempt_id),
         true <- id == Map.get(run, :current_candidate_context_id),
         {:ok, pins} <- resolve(run, id) do
      original = Map.get(run, :candidate_execution_original, run.execution)
      execution = Map.merge(original, pins)
      {:ok, run |> Map.put(:candidate_execution_original, original) |> Map.put(:execution, execution) |> Map.put(:candidate_context_id, id) |> Map.put(:repair_parent_sha, pins[:repair_parent_sha])}
    else
      _ -> {:error, :candidate_context_not_authorized}
    end
  end

  @spec validation_context(map(), String.t()) :: {:ok, map()} | {:error, term()}
  def validation_context(run, attempt_id) do
    with {:ok, view} <- view(run, attempt_id) do
      identity = %{task_id: run.task_id, run_id: run.id, attempt_id: attempt_id, base_sha: view.execution[:validation_base]}
      {:ok, if(view.candidate_context_id, do: Map.put(identity, :candidate_context_id, view.candidate_context_id), else: identity)}
    end
  end

  @spec same_context?(map(), String.t(), String.t()) :: boolean()
  def same_context?(run, left, right) do
    with {:ok, l} <- view(run, left), {:ok, r} <- view(run, right), do: l.candidate_context_id == r.candidate_context_id, else: (_ -> false)
  end

  @doc "Called only after the coordinator authenticates the current submitted review."
  @spec authorize_repair(map(), map()) :: {:ok, map()} | {:error, term()}
  def authorize_repair(run, review) do
    review = json(review)
    existing = Enum.find(Map.values(Map.get(run, :candidate_contexts, %{})), &(&1.review_attempt_id == run.current_attempt_id))

    if existing do
      with {:ok, _} <- resolve(run, existing.id), do: {:ok, run}, else: (_ -> {:error, :candidate_context_not_authorized})
    else
      append_repair(run, review)
    end
  rescue
    _ -> {:error, :repair_not_authorized}
  end

  @spec complete_fix(map(), String.t()) :: map()
  def complete_fix(run, attempt_id) do
    id = Map.get(run, :current_candidate_context_id)
    completed = Map.get(run, :review_fix_completions, %{})

    if id && not Map.has_key?(completed, id) && match?({:ok, _}, view(run, attempt_id)) do
      run |> Map.put(:review_fix_completions, Map.put(completed, id, attempt_id)) |> Map.put(:review_repair_rounds, map_size(completed) + 1)
    else
      run
    end
  end

  defp append_repair(run, review) do
    stage = run.definition.stages[run.stage_id]

    with true <- run.review_repair_rounds < 3,
         :ok <- SymphonyElixir.Review.verify_completion(run, stage, {:ok, review}),
         true <- review["actionable"] == true and review["disagreement"] == false,
         publication when is_map(publication) <- latest_publication(run),
         published = publication.result.evidence,
         true <- published["candidate_sha"] == review["head_sha"],
         pr when is_map(pr) <- published["pr"],
         true <- pr["number"] == review["pr_number"] and pr["url"] == review["pr_url"],
         {:ok, _} <- view(run, run.current_attempt_id),
         {:ok, key} <- key(run) do
      context = %{
        version: 1,
        run_id: run.id,
        task_id: run.task_id,
        execution_sha256: digest(run.execution),
        definition_sha256: digest(run.definition),
        policy_sha256: digest(run.policy),
        source_context_id: Map.get(run, :current_candidate_context_id),
        validation_base: review["base_sha"],
        repair_parent_sha: review["head_sha"],
        publication_attempt_id: publication.id,
        publication_sha256: digest(published),
        review_attempt_id: run.current_attempt_id,
        review_sha256: digest(review),
        pr: Map.take(pr, ["number", "url", "head_branch", "base_branch"])
      }

      id = digest(context)
      context = context |> Map.put(:id, id) |> then(&Map.put(&1, :seal, seal(&1, key)))

      {:ok, run |> Map.put(:candidate_contexts, Map.put(Map.get(run, :candidate_contexts, %{}), id, context)) |> Map.put(:current_candidate_context_id, id)}
    else
      false -> {:error, :repair_not_authorized_or_limit_reached}
      {:error, _} = error -> error
      _ -> {:error, :repair_publication_lineage_missing}
    end
  end

  defp resolve(_run, nil), do: {:ok, %{}}

  defp resolve(run, id) when is_binary(id) do
    with context when is_map(context) <- Map.get(run, :candidate_contexts, %{})[id],
         true <- context.id == id and context.version == 1,
         true <- digest(Map.drop(context, [:id, :seal])) == id,
         true <- context.run_id == run.id and context.task_id == run.task_id,
         true <- context.execution_sha256 == digest(Map.get(run, :candidate_execution_original, run.execution)),
         true <- context.definition_sha256 == digest(run.definition) and context.policy_sha256 == digest(run.policy),
         {:ok, key} <- key(run),
         true <- context.seal == seal(Map.delete(context, :seal), key),
         true <- context.source_context_id != id,
         {:ok, _} <- resolve(run, context.source_context_id),
         publication when is_map(publication) <- find_attempt(run, context.publication_attempt_id),
         review when is_map(review) <- find_attempt(run, context.review_attempt_id),
         true <- publication.status == :completed and publication.gate == :passed and review.status == :completed,
         true <- Map.get(publication, :candidate_context_id) == context.source_context_id and Map.get(review, :candidate_context_id) == context.source_context_id,
         true <- digest(publication.result.evidence) == context.publication_sha256 and digest(review.result.evidence) == context.review_sha256,
         true <- publication.result.evidence["candidate_sha"] == context.repair_parent_sha,
         true <- review.result.evidence["head_sha"] == context.repair_parent_sha and review.result.evidence["base_sha"] == context.validation_base do
      {:ok, %{validation_base: context.validation_base, candidate_context_id: id, repair_parent_sha: context.repair_parent_sha}}
    else
      _ -> {:error, :candidate_context_not_authorized}
    end
  rescue
    _ -> {:error, :candidate_context_not_authorized}
  end

  defp resolve(_run, _id), do: {:error, :candidate_context_not_authorized}

  defp attempt_context(run, nil), do: {:ok, Map.get(run, :current_candidate_context_id)}

  defp attempt_context(run, id) do
    case find_attempt(run, id) do
      %{candidate_context_id: context} -> {:ok, context}
      attempt when is_map(attempt) -> {:ok, nil}
      _ -> {:error, :candidate_attempt_missing}
    end
  end

  defp find_attempt(run, id), do: Enum.find(run.attempts, &(&1.id == id))
  defp latest_publication(run), do: run.attempts |> Enum.reverse() |> Enum.find(&(&1.type == :publish and &1.status == :completed and &1.gate == :passed))

  defp key(run) do
    case File.read(Path.join(run.execution[:validation_archive], "producer.key")) do
      {:ok, key} when byte_size(key) == 32 -> {:ok, key}
      _ -> {:error, :candidate_context_key_unavailable}
    end
  rescue
    _ -> {:error, :candidate_context_key_unavailable}
  end

  defp json(value), do: value |> Jason.encode!() |> Jason.decode!()
  defp bytes(value), do: value |> json() |> :erlang.term_to_binary([:deterministic])
  defp digest(value), do: :crypto.hash(:sha256, bytes(value)) |> Base.encode16(case: :lower)
  defp seal(value, key), do: :crypto.mac(:hmac, :sha256, key, @domain <> bytes(value)) |> Base.encode16(case: :lower)
end
