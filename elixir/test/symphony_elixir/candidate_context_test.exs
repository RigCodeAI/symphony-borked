defmodule SymphonyElixir.CandidateContextTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{CandidateContext, Review, ReviewResult, WorkstreamStore}

  setup do
    root = Path.join(System.tmp_dir!(), "candidate-context-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    key = :crypto.strong_rand_bytes(32)
    File.write!(Path.join(root, "producer.key"), key)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, root: root, key: key}
  end

  test "review authorization appends one sealed context without changing execution or attempts", c do
    run = published_review(c.root, c.key)
    original_execution = run.execution
    original_attempts = run.attempts
    review = current_review(run, c.key)

    assert {:ok, authorized} = CandidateContext.authorize_repair(run, review)
    context_id = authorized.current_candidate_context_id
    assert is_binary(context_id)
    assert authorized.execution == original_execution
    assert authorized.attempts == original_attempts
    assert map_size(authorized.candidate_contexts) == 1
    assert {:ok, same} = CandidateContext.authorize_repair(authorized, review)
    assert same == authorized

    assert {:error, :candidate_context_not_authorized} = CandidateContext.view(authorized, "review-result-1")

    repair = repair_attempt(authorized, "implement-1")
    assert {:ok, view} = CandidateContext.view(repair, "implement-1")
    assert view.candidate_context_id == context_id
    assert view.execution[:validation_base] == "base-main"
    assert view.execution[:repair_parent_sha] == "head-0"
    assert view.execution[:candidate_context_id] == context_id
  end

  test "legacy and repair validation contexts are exact and old attempts cannot consume a new context", c do
    run = published_review(c.root, c.key)

    assert {:ok, legacy} = CandidateContext.validation_context(run, "review-result-1")
    assert legacy == %{task_id: "task-240", run_id: "run-240", attempt_id: "review-result-1", base_sha: "base-original"}

    assert {:ok, authorized} = CandidateContext.authorize_repair(run, current_review(run, c.key))
    repair = repair_attempt(authorized, "implement-1")
    assert {:ok, repair_context} = CandidateContext.validation_context(repair, "implement-1")

    assert repair_context == %{
             task_id: "task-240",
             run_id: "run-240",
             attempt_id: "implement-1",
             base_sha: "base-main",
             candidate_context_id: authorized.current_candidate_context_id
           }

    assert {:ok, validation_context} = CandidateContext.validation_context(repair, "validation-1")
    assert validation_context == Map.put(repair_context, :attempt_id, "validation-1")
    refute CandidateContext.same_context?(repair, "review-result-1", "implement-1")
    assert CandidateContext.same_context?(repair, "implement-1", "validation-1")
    assert {:error, :candidate_context_not_authorized} = CandidateContext.validation_context(repair, "review-result-1")
  end

  test "a serialized authorized context resolves after restart with the same proof", c do
    run = published_review(c.root, c.key)
    assert {:ok, authorized} = CandidateContext.authorize_repair(run, current_review(run, c.key))
    repair = repair_attempt(authorized, "implement-1")

    restored = repair |> :erlang.term_to_binary() |> :erlang.binary_to_term([:safe])
    assert {:ok, restored_view} = CandidateContext.view(restored, "implement-1")
    assert restored_view.candidate_context_id == authorized.current_candidate_context_id
    assert restored_view.execution[:validation_base] == "base-main"
    assert restored_view.execution[:repair_parent_sha] == "head-0"
    assert CandidateContext.same_context?(restored, "implement-1", "implement-1")

    database = Path.join([c.root, "state", "factory.sqlite3"])
    assert {:ok, store} = WorkstreamStore.start_link(path: database, owner: self())
    assert :ok = WorkstreamStore.commit(store, repair, "review-context-snapshot", %{kind: :review_context})
    assert :ok = GenServer.stop(store)

    assert {:ok, reopened} = WorkstreamStore.start_link(path: database, owner: self())
    assert {:ok, durable} = WorkstreamStore.fetch(reopened, repair.id)
    assert {:ok, durable_view} = CandidateContext.view(durable, "implement-1")
    assert durable_view.candidate_context_id == authorized.current_candidate_context_id
    assert durable_view.execution[:repair_parent_sha] == "head-0"
    assert :ok = GenServer.stop(reopened)
  end

  test "execution, context, seal, and current-context selector tampering fail closed", c do
    run = published_review(c.root, c.key)
    assert {:ok, authorized} = CandidateContext.authorize_repair(run, current_review(run, c.key))
    repair = repair_attempt(authorized, "implement-1")
    id = authorized.current_candidate_context_id

    changed_execution = put_in(repair, [:execution, :validation_base], "attacker-base")
    assert {:error, :candidate_context_not_authorized} = CandidateContext.view(changed_execution, "implement-1")

    changed_context = update_in(repair, [:candidate_contexts, id, :validation_base], fn _ -> "attacker-base" end)
    assert {:error, :candidate_context_not_authorized} = CandidateContext.view(changed_context, "implement-1")

    changed_seal = update_in(repair, [:candidate_contexts, id, :seal], fn _ -> String.duplicate("0", 64) end)
    assert {:error, :candidate_context_not_authorized} = CandidateContext.view(changed_seal, "implement-1")

    wrong_selector = %{repair | current_candidate_context_id: String.duplicate("f", 64)}
    assert {:error, :candidate_context_not_authorized} = CandidateContext.view(wrong_selector, "implement-1")
  end

  test "malformed, stale, or altered review evidence cannot authorize a repair", c do
    run = published_review(c.root, c.key)
    good = current_review(run, c.key)
    stale = signed_review(run, "review-result", "review-result-1", c.key, %{"observed_at" => System.system_time(:second) - 31})
    altered = Map.put(good, "head_sha", "different-head")
    forged = Map.put(good, "seal", String.duplicate("0", 64))

    stage = run.definition.stages["review-result"]
    assert {:error, :review_completion_not_verified} = Review.verify_completion(run, stage, {:ok, stale})
    assert {:error, :review_completion_not_verified} = Review.verify_completion(run, stage, {:ok, altered})
    assert {:error, :review_completion_not_verified} = Review.verify_completion(run, stage, {:ok, forged})
    assert {:error, _} = CandidateContext.authorize_repair(run, altered)
    assert {:error, _} = CandidateContext.authorize_repair(run, forged)
  end

  test "review completion is bound to the pinned definition and policy", c do
    run = published_review(c.root, c.key)
    stage = run.definition.stages["review-result"]
    good = current_review(run, c.key)

    assert :ok = Review.verify_completion(run, stage, {:ok, good})

    changed_definition = put_in(run, [:definition, :definitions, "reviewer", :sha256], "reassigned-definition-sha")
    changed_definition_stage = changed_definition.definition.stages["review-result"]

    assert {:error, :review_completion_not_verified} =
             Review.verify_completion(changed_definition, changed_definition_stage, {:ok, good})

    changed_policy = put_in(run, [:policy, :validation_sha256], "reassigned-validation-policy")

    assert {:error, :review_completion_not_verified} = Review.verify_completion(changed_policy, stage, {:ok, good})
  end

  test "disagreement and a missing passed publication never create a candidate context", c do
    run = published_review(c.root, c.key)
    disagreement = signed_review(run, "review-result", "review-result-1", c.key, %{"disagreement" => true})
    assert {:error, :repair_not_authorized_or_limit_reached} = CandidateContext.authorize_repair(run, disagreement)
    assert run.candidate_contexts == %{}

    no_publication = %{run | attempts: Enum.reject(run.attempts, &(&1.type == :publish))}
    assert {:error, :repair_publication_lineage_missing} = CandidateContext.authorize_repair(no_publication, current_review(no_publication, c.key))
    assert no_publication.candidate_contexts == %{}
  end

  defp published_review(root, key) do
    definition = %{
      stages: %{
        "review-result" => %{
          id: "review-result",
          type: :check,
          gate: %{
            evaluator: :review_publication,
            validation_stage: "github-validation",
            review_stage: "review-agent",
            checkout_stage: "review-checkout",
            success: :complete,
            failure: %{repair: "implement", max_attempts: 3}
          }
        }
      },
      definitions: %{"reviewer" => %{sha256: "reviewer-definition-sha"}}
    }

    publication = %{
      id: "publish-1",
      stage: "publish",
      type: :publish,
      ordinal: 1,
      status: :completed,
      gate: :passed,
      candidate_context_id: nil,
      result: %{
        status: :ok,
        evidence: %{
          "candidate_sha" => "head-0",
          "pr" => %{
            "number" => 240,
            "url" => "https://github.com/rigai/symphony/pull/240",
            "head_branch" => "cycle/dev-240",
            "base_branch" => "main"
          }
        }
      }
    }

    run = %{
      id: "run-240",
      task_id: "task-240",
      stage_id: "review-result",
      current_attempt_id: "review-result-1",
      status: :executing,
      definition: definition,
      policy: %{version: 1, lifecycle_sha256: "lifecycle", executor_sha256: "executor", validation_sha256: "validation"},
      execution: %{
        validation_archive: root,
        validation_base: "base-original",
        codex_command: "pinned-codex",
        review_rubric_sha256: ReviewResult.rubric().sha256
      },
      attempts: [publication, review_attempt_record()],
      operations: %{},
      candidate_contexts: %{},
      current_candidate_context_id: nil,
      review_repair_rounds: 0
    }

    review = current_review(run, key)

    %{
      run
      | attempts:
          Enum.map(run.attempts, fn
            %{id: "review-result-1"} = attempt -> put_in(attempt, [:result, :evidence], review)
            attempt -> attempt
          end)
    }
  end

  defp review_attempt_record do
    %{
      id: "review-result-1",
      stage: "review-result",
      type: :check,
      ordinal: 2,
      status: :completed,
      gate: :passed,
      gate_detail: %{verified: true},
      candidate_context_id: nil,
      result: %{status: :ok, evidence: %{}}
    }
  end

  defp current_review(run, key), do: signed_review(run, "review-result", run.current_attempt_id, key, %{})

  defp signed_review(run, stage_id, attempt_id, key, changes) do
    stage = run.definition.stages[stage_id]
    {:ok, view} = CandidateContext.view(run, attempt_id)
    operation_id = get_in(run, [:operations, attempt_id, :side_effect_id]) || attempt_id

    result = %{
      "run_id" => run.id,
      "stage_id" => stage.id,
      "operation_id" => operation_id,
      "candidate_context_id" => view.execution[:candidate_context_id],
      "observed_at" => System.system_time(:second),
      "rubric_sha256" => ReviewResult.rubric().sha256,
      "review_id" => 24_000,
      "review_url" => "https://github.com/rigai/symphony/pull/240#pullrequestreview-24000",
      "pr_number" => 240,
      "pr_url" => "https://github.com/rigai/symphony/pull/240",
      "base_sha" => "base-main",
      "head_sha" => "head-0",
      "actionable" => true,
      "disagreement" => false,
      "event" => "COMMENT"
    }

    result = Map.merge(result, changes)
    {:ok, contract_sha256} = Review.contract_identity(run, stage)
    result = result |> Map.put("contract_sha256", contract_sha256) |> normalize_json()
    Map.put(result, "seal", sign(result, key, "factory-review-evidence-v1\n"))
  end

  defp repair_attempt(run, id) do
    attempts = [
      %{id: id, stage: "implement", type: :agent, status: :executing, candidate_context_id: run.current_candidate_context_id},
      %{id: "validation-1", stage: "github-validation", type: :check, status: :executing, candidate_context_id: run.current_candidate_context_id}
    ]

    %{run | attempts: run.attempts ++ attempts, current_attempt_id: id}
  end

  defp normalize_json(value), do: value |> Jason.encode!() |> Jason.decode!()

  defp sign(value, key, domain) do
    bytes = value |> normalize_json() |> :erlang.term_to_binary([:deterministic])
    :crypto.mac(:hmac, :sha256, key, domain <> bytes) |> Base.encode16(case: :lower)
  end
end
