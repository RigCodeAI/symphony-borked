defmodule SymphonyElixir.ReviewWorkstreamTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{CandidateContext, Review, ReviewResult, WorkstreamRun}

  setup do
    root = Path.join(System.tmp_dir!(), "review-workstream-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    key = :crypto.strong_rand_bytes(32)
    File.write!(Path.join(root, "producer.key"), key)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, root: root, key: key}
  end

  test "review fixes retain the same PR, capture context per attempt, and stop after three completed rounds", c do
    run = new_run(c.root) |> append_publication(nil, "head-0")
    run = start_review(run)
    review_attempt = run.current_attempt_id
    review = signed_review(run, c.key, "head-0")
    run = WorkstreamRun.finish_stage(run, {:ok, review})

    context_one = run.current_candidate_context_id
    assert is_binary(context_one)
    assert run.stage_id == "implement"
    assert run.phase == :fixing
    assert run.execution.validation_base == "base-original"
    assert run.attempts |> Enum.find(&(&1.id == review_attempt)) |> Map.fetch!(:candidate_context_id) == nil

    run = WorkstreamRun.start_stage(run)
    first_fix_attempt = run.current_attempt_id
    assert List.last(run.attempts).candidate_context_id == context_one

    retried = run |> WorkstreamRun.uncertain(:transport_unavailable) |> WorkstreamRun.retry_transport() |> WorkstreamRun.start_stage()
    assert retried.review_repair_rounds == 0
    assert List.last(retried.attempts).candidate_context_id == context_one
    assert retried.operations[retried.current_attempt_id].side_effect_id == retried.operations[first_fix_attempt].side_effect_id
    assert retried.operations[retried.current_attempt_id].transport_retries == 1

    run = WorkstreamRun.finish_stage(retried, {:ok, %{thread_id: "cloud-thread", session_id: "cloud-session"}})
    assert run.review_repair_rounds == 1
    assert run.current_candidate_context_id == context_one
    assert CandidateContext.complete_fix(run, retried.current_attempt_id) == run

    run = complete_fix_and_review(run, c.key, 1)
    run = complete_fix_and_review(run, c.key, 2)
    assert run.review_repair_rounds == 3
    assert map_size(run.candidate_contexts) == 3

    fourth_review = run |> append_publication(run.current_candidate_context_id, "head-3") |> start_review()
    assert List.last(fourth_review.attempts).candidate_context_id == run.current_candidate_context_id
    blocked = WorkstreamRun.finish_stage(fourth_review, {:ok, signed_review(fourth_review, c.key, "head-3")})

    assert blocked.status == :blocked
    assert blocked.review_escalation == :review_repair_limit_reached
    assert blocked.review_repair_rounds == 3
    assert blocked.current_candidate_context_id == run.current_candidate_context_id
    assert map_size(blocked.candidate_contexts) == 3

    publications = Enum.filter(blocked.attempts, &(&1.type == :publish))
    assert length(publications) == 4
    assert Enum.all?(publications, &(&1.result.evidence["pr"]["number"] == 240))
    assert Enum.all?(publications, &(&1.result.evidence["pr"]["url"] == "https://github.com/rigai/symphony/pull/240"))
    assert Enum.map(publications, & &1.result.evidence["candidate_sha"]) == ["head-0", "head-1", "head-2", "head-3"]
  end

  test "reviewer attempts keep their own session and do not replace the cloud agent session", c do
    run = new_run(c.root)
    run = %{run | stage_id: "review-agent", thread_id: "cloud-thread", session_id: "cloud-session"}
    run = WorkstreamRun.start_stage(run)
    attempt_id = run.current_attempt_id

    assert run.phase == :reviewing
    assert run.thread_id == "cloud-thread"
    assert run.session_id == "cloud-session"

    reviewed = WorkstreamRun.finish_stage(run, {:ok, %{thread_id: "review-thread", session_id: "review-session", workspace: "/tmp/review-checkout", findings: []}})

    assert reviewed.status == :ready
    assert reviewed.stage_id == "review-result"
    assert reviewed.thread_id == "cloud-thread"
    assert reviewed.session_id == "cloud-session"

    assert reviewed.reviewer_sessions[attempt_id] == %{
             thread_id: "review-thread",
             session_id: "review-session",
             workspace: "/tmp/review-checkout"
           }
  end

  test "malformed review evidence blocks without authorizing a cloud repair", c do
    run = new_run(c.root) |> append_publication(nil, "head-0") |> start_review()
    blocked = WorkstreamRun.finish_stage(run, {:ok, %{"actionable" => true, "head_sha" => "head-0"}})

    assert blocked.status == :blocked
    assert blocked.review_escalation == :review_not_verified
    assert blocked.review_repair_rounds == 0
    assert blocked.current_candidate_context_id == nil
    assert blocked.candidate_contexts == %{}
    assert List.last(blocked.attempts).gate == :failed
  end

  defp complete_fix_and_review(run, key, round) do
    head = "head-#{round}"
    run = run |> append_publication(run.current_candidate_context_id, head) |> start_review()

    assert List.last(run.attempts).candidate_context_id == run.current_candidate_context_id
    run = WorkstreamRun.finish_stage(run, {:ok, signed_review(run, key, head)})
    assert run.stage_id == "implement"
    assert run.phase == :fixing

    run = run |> WorkstreamRun.start_stage() |> WorkstreamRun.finish_stage({:ok, %{thread_id: "cloud-thread-#{round}", session_id: "cloud-session-#{round}"}})
    assert run.review_repair_rounds == round + 1
    assert run.current_candidate_context_id != nil
    run
  end

  defp new_run(root) do
    definition = %{
      name: "review-cycle",
      entry: "review-result",
      inputs: [],
      input_values: %{},
      agents: %{},
      definitions: %{"reviewer" => %{sha256: "reviewer-definition"}},
      stages: %{
        "review-result" => %{
          id: "review-result",
          type: :check,
          outputs: ["review-result"],
          gate: %{
            evaluator: :review_publication,
            validation_stage: "github-validation",
            review_stage: "review-agent",
            checkout_stage: "review-checkout",
            success: :complete,
            failure: %{repair: "implement", max_attempts: 3}
          }
        },
        "implement" => %{
          id: "implement",
          type: :agent,
          outputs: ["patch"],
          next: "review-result"
        },
        "review-agent" => %{
          id: "review-agent",
          type: :agent,
          outputs: ["review-findings"],
          review_checkout_stage: "review-checkout",
          next: "review-result"
        }
      }
    }

    run =
      WorkstreamRun.new("task-240", definition, root,
        workspace_root: root,
        codex_command: "pinned-codex",
        branch: "cycle/dev-240"
      )

    execution = Map.merge(run.execution, %{validation_archive: root, validation_base: "base-original", review_rubric_sha256: ReviewResult.rubric().sha256})
    %{run | execution: execution}
  end

  defp append_publication(run, context_id, head) do
    attempt = %{
      id: "publish-#{head}",
      stage: "publish",
      type: :publish,
      ordinal: length(run.attempts) + 1,
      status: :completed,
      gate: :passed,
      candidate_context_id: context_id,
      result: %{status: :ok, evidence: publication_evidence(head)}
    }

    %{run | attempts: run.attempts ++ [attempt]}
  end

  defp publication_evidence(head) do
    %{
      "candidate_sha" => head,
      "pr" => %{
        "number" => 240,
        "url" => "https://github.com/rigai/symphony/pull/240",
        "head_branch" => "cycle/dev-240",
        "base_branch" => "main"
      }
    }
  end

  defp start_review(run) do
    run = %{run | stage_id: "review-result", status: :ready, current_attempt_id: nil}
    WorkstreamRun.start_stage(run)
  end

  defp signed_review(run, key, head) do
    attempt_id = run.current_attempt_id
    stage = run.definition.stages[run.stage_id]
    {:ok, view} = CandidateContext.view(run, attempt_id)
    operation_id = get_in(run, [:operations, attempt_id, :side_effect_id]) || attempt_id

    result = %{
      "run_id" => run.id,
      "stage_id" => stage.id,
      "operation_id" => operation_id,
      "candidate_context_id" => view.execution[:candidate_context_id],
      "observed_at" => System.system_time(:second),
      "rubric_sha256" => ReviewResult.rubric().sha256,
      "review_id" => System.unique_integer([:positive]) + 24_000,
      "review_url" => "https://github.com/rigai/symphony/pull/240#pullrequestreview-24000",
      "pr_number" => 240,
      "pr_url" => "https://github.com/rigai/symphony/pull/240",
      "base_sha" => "base-main",
      "head_sha" => head,
      "actionable" => true,
      "disagreement" => false,
      "event" => "COMMENT"
    }

    {:ok, contract_sha256} = Review.contract_identity(run, stage)
    result = Map.put(result, "contract_sha256", contract_sha256)

    result = normalize_json(result)
    Map.put(result, "seal", sign(result, key, "factory-review-evidence-v1\n"))
  end

  defp normalize_json(value), do: value |> Jason.encode!() |> Jason.decode!()

  defp sign(value, key, domain) do
    bytes = value |> normalize_json() |> :erlang.term_to_binary([:deterministic])
    :crypto.mac(:hmac, :sha256, key, domain <> bytes) |> Base.encode16(case: :lower)
  end
end
