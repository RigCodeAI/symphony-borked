defmodule SymphonyElixir.Review do
  @moduledoc """
  Service-owned review checkout, result validation and GitHub submission.
  Reviews remain bound to the current validated base/head and a separate App.
  A submitted review authorizes neither merging nor a model-selected transition.
  """

  alias SymphonyElixir.{CandidateContext, CandidateTransfer, Config, PathSafety, Publisher, ReviewGithub, ReviewResult, ReviewWorkspace, ValidationCheck}

  @domain "factory-review-evidence-v1\n"

  @spec pin(keyword(), Path.t(), map()) :: {:ok, map()} | {:error, term()}
  def pin(opts, workspace, definition) do
    if Enum.any?(definition.stages, fn {_id, s} -> get_in(s, [:gate, :evaluator]) == :review_publication end) do
      config = Keyword.get_lazy(opts, :review, &Config.review/0)
      config = if match?({:ok, _}, config), do: elem(config, 1), else: config

      with {:ok, config} <- Publisher.validate_config(config),
           {:ok, path} <- PathSafety.canonicalize(config.private_key_path),
           true <- path == config.private_key_path and not overlap?(workspace, path) and not overlap?(opts[:workspace_root], path),
           true <- Enum.all?([opts[:publisher], opts[:validation_check]], &separate?(config, &1)) do
        {:ok, %{review: config, review_rubric_sha256: ReviewResult.rubric().sha256}}
      else
        _ -> {:error, :separate_review_app_not_configured}
      end
    else
      {:ok, %{}}
    end
  end

  @spec prepare(map(), map(), keyword()) :: {:ok, map()} | {:error, term()} | {:uncertain, term()}
  def prepare(run, stage, opts) do
    with {:ok, view} <- CandidateContext.view(run, run.current_attempt_id),
         :ok <- owner(opts),
         validation when is_map(validation) <- view.definition.stages[stage.gate.validation_stage],
         {:ok, remote} <- ValidationCheck.verify_ready(view, validation, validation_opts(opts)),
         :ok <- published_pr(view, remote),
         {:ok, config} <- configuration(view),
         {:ok, pr} <- transport(view, opts).get_pr(config, remote.number),
         :ok <- same_pr(pr, remote),
         identity =
           Map.merge(Map.take(remote, [:number, :url, :base_sha, :head_sha, :head_branch, :base_branch]), %{
             rubric_sha256: view.execution.review_rubric_sha256,
             cloud_thread_id: run.thread_id,
             candidate_context_id: view.execution[:candidate_context_id]
           }),
         :ok <- owner(opts),
         {:ok, checkout} <- ReviewWorkspace.prepare(view, identity, opts),
         :ok <- owner(opts),
         {:ok, fresh} <- ValidationCheck.verify_ready(view, validation, validation_opts(opts)),
         :ok <- published_pr(view, fresh),
         true <- Map.take(fresh, [:number, :base_sha, :head_sha, :url]) == Map.take(remote, [:number, :base_sha, :head_sha, :url]) do
      seal_result(view, stage, checkout)
    else
      {:uncertain, _} = uncertain -> uncertain
      {:error, _} = error -> error
      _ -> {:error, :review_candidate_not_current}
    end
  end

  @spec checkout(map(), map()) :: {:ok, map()} | {:error, term()}
  def checkout(run, agent_stage) do
    checkout_stage = run.definition.stages[agent_stage.review_checkout_stage]
    attempt = latest_attempt(run, checkout_stage.id)

    with %{status: :completed, gate: :passed, result: %{evidence: result}} <- attempt,
         true <- CandidateContext.same_context?(run, run.current_attempt_id, attempt.id),
         :ok <- authenticate(run, checkout_stage, result, attempt.id),
         true <- result["workspace"] != run.workspace and result["cloud_thread_id"] == run.thread_id do
      {:ok, result}
    else
      _ -> {:error, :review_checkout_not_authorized}
    end
  rescue
    _ -> {:error, :review_checkout_not_authorized}
  end

  @spec verify_checkout(map(), map(), term()) :: :ok | {:error, term()}
  def verify_checkout(run, stage, {:ok, result}) when is_map(result), do: authenticate(run, stage, result, run.current_attempt_id)
  def verify_checkout(_run, _stage, _result), do: {:error, :review_checkout_not_verified}

  @spec ready_checkout(map(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def ready_checkout(run, agent_stage, opts) do
    with {:ok, checkout} <- checkout(run, agent_stage),
         checkout_stage = run.definition.stages[agent_stage.review_checkout_stage],
         validation = run.definition.stages[checkout_stage.gate.validation_stage],
         :ok <- owner(opts),
         {:ok, current} <- ValidationCheck.verify_ready(run, validation, validation_opts(opts)),
         :ok <- published_pr(run, current),
         true <- checkout_matches?(checkout, current) do
      {:ok, checkout}
    else
      {:error, _} = error -> error
      _ -> {:error, :review_checkout_stale_before_execution}
    end
  end

  @spec submit(map(), map(), keyword()) :: {:ok, map()} | {:error, term()} | {:uncertain, term()}
  def submit(run, stage, opts) do
    :global.trans({{__MODULE__, run.task_id}, self()}, fn -> submit_locked(run, stage, opts) end)
  rescue
    _ -> {:error, :review_submission_failed}
  catch
    _, _ -> {:error, :review_submission_failed}
  end

  defp submit_locked(run, stage, opts) do
    with {:ok, view} <- CandidateContext.view(run, run.current_attempt_id),
         {:ok, config} <- configuration(view),
         :ok <- owner(opts),
         {:ok, reviewer, checkout} <- reviewer_result(view, stage),
         {:ok, remote} <- ValidationCheck.verify_ready(view, view.definition.stages[stage.gate.validation_stage], validation_opts(opts)),
         :ok <- published_pr(view, remote),
         true <- checkout_matches?(checkout, remote),
         {:ok, workspace} <- CandidateTransfer.workspace(view),
         recorded = %{base_sha: checkout["base_sha"], head_sha: checkout["head_sha"], rubric_sha256: checkout["rubric_sha256"]},
         {:ok, validated} <- ReviewResult.validate(reviewer["review_json"], recorded, remote, workspace, checkout["workspace"]),
         {:ok, pr} <- transport(view, opts).get_pr(config, remote.number),
         :ok <- same_pr(pr, remote),
         payload = payload(view, stage, validated),
         :ok <- owner(opts),
         {:ok, existing} <- transport(view, opts).reviews(config, remote.number),
         {:ok, submitted} <- existing_or_create(view, config, remote.number, existing, payload, opts),
         :ok <- matches_submission(submitted, payload, pr),
         :ok <- owner(opts),
         {:ok, fresh} <- ValidationCheck.verify_ready(view, view.definition.stages[stage.gate.validation_stage], validation_opts(opts)),
         :ok <- published_pr(view, fresh),
         true <- fresh.base_sha == remote.base_sha and fresh.head_sha == remote.head_sha,
         {:ok, confirmed} <- transport(view, opts).reviews(config, remote.number),
         [actual] <- Enum.filter(confirmed, &(&1.id == submitted.id)),
         :ok <- matches_submission(actual, payload, pr),
         {:ok, comments} <- transport(view, opts).review_comments(config, remote.number, actual.id),
         :ok <- matches_comments(comments, payload),
         :ok <- owner(opts),
         {:ok, final} <- ValidationCheck.verify_ready(view, view.definition.stages[stage.gate.validation_stage], validation_opts(opts)),
         :ok <- published_pr(view, final),
         true <- final.base_sha == remote.base_sha and final.head_sha == remote.head_sha do
      result = %{
        base_sha: remote.base_sha,
        head_sha: remote.head_sha,
        pr_number: remote.number,
        pr_url: remote.url,
        review_id: actual.id,
        review_url: actual.url,
        author_login: actual.author_login,
        event: payload["event"],
        result: validated.result,
        locations: validated.locations,
        rubric_sha256: validated.rubric.sha256,
        reviewer_thread_id: reviewer["thread_id"],
        reviewer_session_id: reviewer["session_id"],
        actionable: validated.result["findings"] != [],
        disagreement: false,
        observed_at: System.system_time(:second)
      }

      seal_result(view, stage, result)
    else
      {:error, {:github_request_failed, _}} -> {:uncertain, :review_submission_outcome_unknown}
      {:error, _} = error -> error
      _ -> {:error, :review_result_or_remote_identity_invalid}
    end
  end

  defp matches_comments(comments, payload) do
    expected =
      Enum.map(payload["comments"], fn comment ->
        %{path: comment["path"], line: comment["line"], side: "RIGHT", start_line: comment["start_line"], start_side: comment["start_side"], body: comment["body"]}
      end)

    actual = Enum.map(comments, &Map.take(&1, [:path, :line, :side, :start_line, :start_side, :body]))

    if Enum.all?(comments, &(&1.owned == true and &1.head_sha == payload["commit_id"])) and Enum.sort(actual) == Enum.sort(expected),
      do: :ok,
      else: {:error, :review_inline_comments_not_verified}
  end

  @spec verify_completion(map(), map(), term()) :: :ok | {:error, term()}
  def verify_completion(run, stage, {:ok, result}) when is_map(result) do
    result = json(result)

    with :ok <- authenticate(run, stage, result, run.current_attempt_id),
         true <- get_in(stage, [:gate, :evaluator]) == :review_publication,
         observed when is_integer(observed) <- result["observed_at"],
         true <- observed <= System.system_time(:second) and System.system_time(:second) - observed <= 30,
         true <- result["rubric_sha256"] == ReviewResult.rubric().sha256,
         true <- is_integer(result["review_id"]) and result["review_id"] > 0 do
      :ok
    else
      _ -> {:error, :review_completion_not_verified}
    end
  end

  def verify_completion(_run, _stage, _result), do: {:error, :review_completion_not_verified}

  @spec reviewer?(map()) :: boolean()
  def reviewer?(stage), do: is_binary(stage[:review_checkout_stage])

  @spec service_stage?(map()) :: boolean()
  def service_stage?(stage), do: get_in(stage, [:gate, :evaluator]) in [:review_checkout, :review_publication]

  @spec retry_safe?(map()) :: boolean()
  def retry_safe?(stage), do: get_in(stage, [:gate, :evaluator]) == :review_publication

  @doc "Binds a sealed review to the pinned definitions, policy and service execution references."
  @spec contract_identity(map(), map()) :: {:ok, String.t()} | {:error, term()}
  def contract_identity(run, stage) do
    with {:ok, view} <- CandidateContext.view(run, nil) do
      pins =
        Map.take(view.execution, [
          :workspace_root,
          :codex_command,
          :authentication_reference,
          :secret_environment_names,
          :worker_control,
          :validation_policy,
          :validation_base,
          :validation_archive,
          :validation_scratch,
          :validation_candidates,
          :publisher,
          :validation_check,
          :review,
          :review_rubric_sha256,
          :candidate_context_id,
          :repair_parent_sha
        ])

      {:ok, digest(%{definition: run.definition, policy: run.policy, execution: pins, stage: stage})}
    end
  rescue
    _ -> {:error, :review_contract_not_serializable}
  end

  defp reviewer_result(run, stage) do
    reviewer_stage = run.definition.stages[stage.gate.review_stage]
    attempt = latest_attempt(run, reviewer_stage.id)

    with %{status: :completed, result: %{status: :ok, evidence: reviewer}} <- attempt,
         true <- CandidateContext.same_context?(run, run.current_attempt_id, attempt.id),
         {:ok, checkout} <- checkout(%{run | current_attempt_id: attempt.id}, reviewer_stage),
         true <- reviewer["thread_id"] != run.thread_id and is_binary(reviewer["thread_id"]),
         true <- reviewer["workspace"] == checkout["workspace"] and is_binary(reviewer["review_json"]) do
      {:ok, reviewer, checkout}
    else
      _ -> {:error, :independent_reviewer_result_missing}
    end
  end

  defp payload(run, stage, validated) do
    result = validated.result

    event =
      cond do
        result["overall_correctness"] == "patch is incorrect" -> "REQUEST_CHANGES"
        result["findings"] == [] -> "APPROVE"
        true -> "COMMENT"
      end

    comments =
      Enum.zip_with(result["findings"], validated.locations, fn finding, location ->
        comment = %{"path" => location.path, "line" => location.end_line, "side" => "RIGHT", "body" => finding["title"] <> "\n\n" <> finding["body"]}
        if location.start_line != location.end_line, do: Map.merge(comment, %{"start_line" => location.start_line, "start_side" => "RIGHT"}), else: comment
      end)

    identity =
      digest(%{run: run.id, stage: stage.id, operation: operation_id(run, run.current_attempt_id), base: validated.base_sha, head: validated.head_sha, rubric: validated.rubric.sha256, result: result})

    body = "<!-- factory-review:#{identity} -->\n" <> result["overall_explanation"] <> "\n\nBase: #{validated.base_sha}\nHead: #{validated.head_sha}\nRubric: #{validated.rubric.sha256}"
    %{"body" => body, "commit_id" => validated.head_sha, "event" => event, "comments" => comments}
  end

  defp existing_or_create(run, config, number, reviews, payload, opts) do
    matches = Enum.filter(reviews, &(&1.body == payload["body"]))

    case matches do
      [] ->
        with :ok <- owner(opts), do: transport(run, opts).create_review(config, number, payload)

      [review] ->
        {:ok, review}

      _ ->
        {:error, :duplicate_review_identity}
    end
  end

  defp matches_submission(review, payload, pr) do
    expected = %{"APPROVE" => "APPROVED", "REQUEST_CHANGES" => "CHANGES_REQUESTED", "COMMENT" => "COMMENTED"}[payload["event"]]

    if review.body == payload["body"] and review.head_sha == payload["commit_id"] and review.state == expected and review.author_type == "Bot" and review.owned == true and
         review.author_login != pr.author_login,
       do: :ok,
       else: {:error, :review_submission_identity_mismatch}
  end

  defp same_pr(pr, remote) do
    if pr.number == remote.number and pr.url == remote.url and pr.head_sha == remote.head_sha and pr.base_sha == remote.base_sha and pr.head_branch == remote.head_branch and pr.base_branch == "main" and
         pr.state == "open" and not pr.draft,
       do: :ok,
       else: {:error, :review_pr_changed}
  end

  defp published_pr(run, remote) do
    publication =
      run.attempts
      |> Enum.reverse()
      |> Enum.find(&(&1.type == :publish and &1.status == :completed and &1.gate == :passed))

    with %{result: %{status: :ok, evidence: evidence}} <- publication,
         true <- CandidateContext.same_context?(run, run.current_attempt_id, publication.id),
         evidence = json(evidence),
         true <- evidence["candidate_sha"] == remote.head_sha,
         pr when is_map(pr) <- evidence["pr"],
         true <- Enum.all?([:number, :url, :head_sha, :head_branch, :base_branch], &(pr[Atom.to_string(&1)] == Map.get(remote, &1))) do
      :ok
    else
      _ -> {:error, :review_publication_identity_mismatch}
    end
  rescue
    _ -> {:error, :review_publication_identity_mismatch}
  end

  defp checkout_matches?(checkout, remote) do
    Enum.all?([:number, :url, :base_sha, :head_sha, :head_branch, :base_branch], &(checkout[Atom.to_string(&1)] == Map.get(remote, &1)))
  end

  defp seal_result(run, stage, result) do
    with {:ok, key} <- key(run),
         {:ok, contract} <- contract_identity(run, stage) do
      result =
        result
        |> json()
        |> Map.merge(%{
          "run_id" => run.id,
          "stage_id" => stage.id,
          "operation_id" => operation_id(run, run.current_attempt_id),
          "candidate_context_id" => run.execution[:candidate_context_id],
          "contract_sha256" => contract
        })

      {:ok, Map.put(result, "seal", seal(result, key))}
    end
  end

  defp authenticate(run, stage, result, attempt_id) do
    result = json(result)

    with {:ok, view} <- CandidateContext.view(run, attempt_id),
         true <- result["run_id"] == run.id and result["stage_id"] == stage.id and result["operation_id"] == operation_id(run, attempt_id),
         true <- result["candidate_context_id"] == view.execution[:candidate_context_id],
         {:ok, contract} <- contract_identity(run, stage),
         true <- result["contract_sha256"] == contract,
         {:ok, key} <- key(view),
         true <- result["seal"] == seal(Map.delete(result, "seal"), key) do
      :ok
    else
      _ -> {:error, :review_evidence_not_authenticated}
    end
  rescue
    _ -> {:error, :review_evidence_not_authenticated}
  end

  defp operation_id(run, id), do: get_in(run, [:operations, id, :side_effect_id]) || id
  defp latest_attempt(run, stage_id), do: run.attempts |> Enum.reverse() |> Enum.find(&(&1.stage == stage_id))
  defp configuration(run), do: Publisher.validate_config(run.execution[:review])
  defp transport(run, opts), do: opts[:review_transport] || run.execution[:review_transport] || ReviewGithub
  defp validation_opts(opts), do: Keyword.put(opts, :on_validation_check, opts[:on_publication_check])
  defp separate?(config, other) when is_map(other), do: to_string(config.app_id) != to_string(other.app_id) and config.private_key_path != other.private_key_path
  defp separate?(_config, _other), do: false
  defp overlap?(a, b), do: a == b or String.starts_with?(a, b <> "/") or String.starts_with?(b, a <> "/")

  defp owner(opts) do
    case opts[:on_publication_check] do
      callback when is_function(callback, 0) -> callback.()
      _ -> {:error, :review_coordinator_owner_required}
    end
  end

  defp key(run) do
    case File.read(Path.join(run.execution[:validation_archive], "producer.key")) do
      {:ok, key} when byte_size(key) == 32 -> {:ok, key}
      _ -> {:error, :review_attestation_unavailable}
    end
  end

  defp json(value), do: value |> Jason.encode!() |> Jason.decode!()
  defp bytes(value), do: value |> json() |> :erlang.term_to_binary([:deterministic])
  defp digest(value), do: :crypto.hash(:sha256, bytes(value)) |> Base.encode16(case: :lower)
  defp seal(value, key), do: :crypto.mac(:hmac, :sha256, key, @domain <> bytes(value)) |> Base.encode16(case: :lower)
end
