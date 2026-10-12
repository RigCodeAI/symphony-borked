defmodule SymphonyElixir.ReviewOutputCleanupTest do
  use SymphonyElixir.TestSupport

  # Test-only signatures and receipts exercise the stored-output contract without contacting GitHub or a worker.
  alias SymphonyElixir.CandidateContext
  alias SymphonyElixir.Evidence
  alias SymphonyElixir.Review
  alias SymphonyElixir.ReviewResult
  alias SymphonyElixir.ServiceCheckCompletion
  alias SymphonyElixir.WorkstreamCleanup
  alias SymphonyElixir.WorkstreamOutputs
  alias SymphonyElixir.WorkstreamRun
  alias SymphonyElixir.WorkstreamStore

  @review_domain "factory-review-evidence-v1\n"
  @service_legacy_domain "service-check-completion-v1\n"

  setup do
    root = Path.join(System.tmp_dir!(), "review-output-cleanup-#{System.unique_integer([:positive])}")
    disposable = Path.join(root, "disposable")
    workspace_root = Path.join(disposable, "workspaces")
    workspace = Path.join(workspace_root, "owned-task")
    review_root = Path.join(disposable, "review-checkouts")
    review_workspace = Path.join(review_root, "checkout")
    archive = Path.join(root, "producer-archive")
    File.mkdir_p!(workspace)
    File.mkdir_p!(review_workspace)
    File.mkdir_p!(archive)
    key = :crypto.strong_rand_bytes(32)
    key_path = Path.join(archive, "producer.key")
    File.write!(key_path, key)
    File.chmod!(key_path, 0o600)

    base_sha = String.duplicate("b", 40)
    head_sha = String.duplicate("a", 40)
    pull_url = "https://github.com/RigCodeAI/rig/pull/241"
    rubric_sha = ReviewResult.rubric().sha256
    definition = definition(rubric_sha)

    execution = %{
      workspace_root: workspace_root,
      codex_command: "pinned-codex",
      validation_policy: %{sha256: "validation-policy-sha"},
      validation_base: base_sha,
      validation_archive: archive,
      validation_scratch: Path.join(root, "scratch"),
      validation_candidates: Path.join(root, "candidates"),
      publisher: %{app_id: 101, installation_id: 201},
      validation_check: %{app_id: 102, installation_id: 202},
      review: %{app_id: 103, installation_id: 203},
      review_rubric_sha256: rubric_sha,
      worker_control: %{kind: "fixture"}
    }

    run = WorkstreamRun.new("task-review-output", definition, workspace, [branch: "cycle/dev-241"], execution)
    run = %{run | id: "run-review-output", thread_id: "cloud-thread-241"}

    publication = %{
      "candidate_sha" => head_sha,
      "branch" => "cycle/dev-241",
      "pr" => %{
        "number" => 241,
        "url" => pull_url,
        "head_sha" => head_sha,
        "head_branch" => "cycle/dev-241",
        "base_branch" => "main"
      }
    }

    publisher_id = run.id <> "/publish/1"

    publisher_attempt = %{
      id: publisher_id,
      stage: "publish",
      type: :publish,
      ordinal: 1,
      status: :completed,
      gate: :passed,
      candidate_context_id: nil,
      result: %{status: :ok, evidence: publication}
    }

    publication_artifact = artifact(run.id, publisher_id, publication)

    run = %{
      run
      | stage_id: "review-checkout",
        attempts: [publisher_attempt],
        outputs: %{"publication" => publication},
        artifacts: %{"publication" => publication_artifact}
    }

    on_exit(fn -> File.rm_rf!(root) end)

    {:ok,
     root: root,
     disposable: disposable,
     workspace: workspace,
     review_root: review_root,
     review_workspace: review_workspace,
     archive: archive,
     key: key,
     base_sha: base_sha,
     head_sha: head_sha,
     pull_url: pull_url,
     run: run,
     db: Path.join(root, "runs.sqlite3")}
  end

  test "review outputs retain authenticated values and source identities after workspace removal", c do
    run = completed_review(c)
    checkout_id = c.run.id <> "/review-checkout/2"
    publication_id = c.run.id <> "/review-publication/4"

    assert ServiceCheckCompletion.verified?(run, checkout_id)
    assert {:ok, %{"publication" => publication_source}} = ServiceCheckCompletion.inputs(run, checkout_id)
    assert publication_source == c.run.artifacts["publication"]

    assert {:ok, %{"kind" => "review_checkout", "value" => checkout_value}} =
             WorkstreamOutputs.projection(run, "review-checkout")

    refute Map.has_key?(checkout_value, "root")
    refute Map.has_key?(checkout_value, "workspace")
    refute Map.has_key?(checkout_value, "source_workspace")

    assert ServiceCheckCompletion.verified?(run, publication_id)
    assert {:ok, publication_inputs} = ServiceCheckCompletion.inputs(run, publication_id)
    assert publication_inputs["review-checkout"] == run.artifacts["review-checkout"]
    assert publication_inputs["review-agent"] == run.artifacts["review-agent"]

    assert {:ok, %{"kind" => "review_publication", "value" => submitted}} =
             WorkstreamOutputs.projection(run, "review-publication")

    assert submitted["reviewer_thread_id"] == "reviewer-thread-241"
    assert submitted["result"]["findings"] == []

    assert {:ok, %{"kind" => "review_agent_report", "value" => report}} =
             WorkstreamOutputs.projection(run, "review-agent")

    assert report["report"] == submitted["result"]
    assert report["locations"] == submitted["locations"]

    File.rm_rf!(c.workspace)
    refute File.exists?(c.workspace)
    assert File.dir?(c.review_root)
    assert File.dir?(c.review_workspace)

    assert {:ok, _} = WorkstreamOutputs.projection(run, "review-checkout")
    assert {:ok, _} = WorkstreamOutputs.projection(run, "review-agent")
    assert {:ok, _} = WorkstreamOutputs.projection(run, "review-publication")
  end

  test "reviewer JSON, workspace, IDs, service seals, definitions and file fields fail closed", c do
    run = completed_review(c)

    checkout = run.outputs["review-checkout"]
    changed_seal = replace_output(run, "review-checkout", Map.put(checkout, "seal", String.duplicate("0", 64)))

    changed_definition =
      put_in(run, [:definition, :stages, "review-checkout", :gate, :validation_stage], "different-validation")

    reviewer = run.outputs["review-agent"]
    changed_json = replace_output(run, "review-agent", Map.put(reviewer, "review_json", "{}"))
    changed_workspace = replace_output(run, "review-agent", Map.put(reviewer, "workspace", c.workspace))
    changed_thread = replace_output(run, "review-agent", Map.put(reviewer, "thread_id", "unrelated-thread"))
    extra_file = replace_output(run, "review-agent", Map.put(reviewer, "file", "review.json"))

    for changed <- [changed_seal, changed_definition, changed_json, changed_workspace, changed_thread, extra_file] do
      assert {:error, :workspace_outputs_not_retained} = WorkstreamOutputs.projection(changed, "review-agent")
    end

    assert {:error, :workspace_outputs_not_retained} = WorkstreamOutputs.projection(changed_seal, "review-checkout")

    assert {:error, :workspace_outputs_not_retained} =
             WorkstreamOutputs.projection(changed_definition, "review-checkout")
  end

  test "captured service inputs survive a store restart and reject producer substitution", c do
    run = completed_review(c)
    checkout_id = c.run.id <> "/review-checkout/2"
    publication_id = c.run.id <> "/review-publication/4"

    {:ok, store} = WorkstreamStore.start_link(path: c.db, owner: self())
    assert :ok = WorkstreamStore.commit(store, run, "review-output-fixture", %{kind: :completed})
    :ok = GenServer.stop(store)

    {:ok, restarted} = WorkstreamStore.start_link(path: c.db, owner: self())

    try do
      assert {:ok, restored} = WorkstreamStore.fetch(restarted, run.id)
      assert ServiceCheckCompletion.verified?(restored, checkout_id)
      assert ServiceCheckCompletion.verified?(restored, publication_id)
      assert {:ok, checkout_inputs} = ServiceCheckCompletion.inputs(restored, checkout_id)
      assert checkout_inputs["publication"] == restored.artifacts["publication"]

      source = restored.operations[publication_id].service_completion.input_ids["review-agent"]

      substituted =
        put_in(
          restored.operations[publication_id].service_completion.input_ids["review-agent"],
          %{source | attempt_id: checkout_id}
        )

      refute ServiceCheckCompletion.verified?(substituted, publication_id)
      assert {:error, :service_inputs_not_authenticated} = ServiceCheckCompletion.inputs(substituted, publication_id)
      assert {:error, :workspace_outputs_not_retained} = WorkstreamOutputs.projection(substituted, "review-publication")
    after
      GenServer.stop(restarted)
    end
  end

  test "review report and service outputs retain immutable bytes beyond main workspace cleanup", c do
    run = completed_review(c)
    evidence_root = Path.join(c.root, "evidence")

    config = %{
      "root" => evidence_root,
      "base_url" => "https://factory.example.test",
      "iap_audience" => "/projects/1/global/backendServices/2",
      "allowed_domains" => ["example.test"],
      "allowed_emails" => []
    }

    {:ok, evidence} = Evidence.start_link(config: {:ok, config}, name: nil, receipt_uid: File.stat!(c.root).uid)
    options = [cleanup_evidence_server: evidence]

    try do
      assert :ok = Evidence.snapshot(run, "review-output-archive", %{kind: :synthetic_review_output_fixture}, evidence)
      assert {:error, :retained_output_archive_unavailable} = WorkstreamCleanup.retain_outputs(run, options)

      assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id), evidence)
      write_upload_receipt(evidence_root, view)

      assert {:ok, refs} = WorkstreamCleanup.retain_outputs(run, options)
      retained = Map.put(run, :retained_output_artifacts, refs)
      File.rm_rf!(c.workspace)

      assert {:ok, %{"kind" => "review_checkout", "value" => checkout}} =
               WorkstreamCleanup.retained_output(retained, "review-checkout", options)

      assert {:ok, %{"kind" => "review_agent_report", "value" => report}} =
               WorkstreamCleanup.retained_output(retained, "review-agent", options)

      assert {:ok, %{"kind" => "review_publication", "value" => submitted}} =
               WorkstreamCleanup.retained_output(retained, "review-publication", options)

      refute Map.has_key?(checkout, "workspace")
      assert report["report"] == submitted["result"]
      assert File.dir?(c.review_workspace)
    after
      GenServer.stop(evidence)
    end
  end

  test "reviewer evidence stays readable after a signed repair context advance", c do
    run = completed_review(c, actionable: true)
    old_review_attempt = c.run.id <> "/review-publication/4"

    assert run.current_candidate_context_id
    assert run.stage_id == "implement"
    assert ServiceCheckCompletion.verified?(run, old_review_attempt)
    assert {:ok, _} = WorkstreamOutputs.projection(run, "review-agent")

    repair =
      run
      |> WorkstreamRun.start_stage()
      |> WorkstreamRun.finish_stage(
        {:ok,
         %{
           thread_id: "repair-thread",
           session_id: "repair-session",
           workspace: c.workspace,
           candidate_context_id: run.current_candidate_context_id,
           repair_parent_sha: c.head_sha,
           validation_base: c.base_sha,
           review_disagreement: false,
           repair_explanation: "Addressed the submitted finding."
         }}
      )

    assert {:ok, context} = CandidateContext.evidence_context(repair, repair.attempts |> List.last() |> Map.fetch!(:id))
    assert repair.outputs["repair"]["candidate_context_id"] == context.candidate_context_id
    assert {:ok, %{"kind" => "repair_agent_metadata"}} = WorkstreamOutputs.projection(repair, "repair")
    assert {:ok, _} = WorkstreamOutputs.projection(repair, "review-agent")

    for {field, wrong_value} <- [
          {"candidate_context_id", "attacker-context"},
          {"repair_parent_sha", String.duplicate("f", 40)},
          {"validation_base", "attacker-base"}
        ] do
      value = repair.outputs["repair"]
      changed = replace_output(repair, "repair", Map.put(value, field, wrong_value))
      assert {:error, :workspace_outputs_not_retained} = WorkstreamOutputs.projection(changed, "repair")

      missing = replace_output(repair, "repair", Map.delete(value, field))
      assert {:error, :workspace_outputs_not_retained} = WorkstreamOutputs.projection(missing, "repair")
    end
  end

  test "legacy service completion v1 remains readable for context-free validation output", c do
    value = %{"receipt" => %{"manifest_sha256" => String.duplicate("c", 64)}}

    stage = %{
      id: "legacy-validation",
      type: :check,
      inputs: [],
      outputs: ["legacy-validation"],
      gate: %{evaluator: :candidate_validation}
    }

    attempt_id = c.run.id <> "/legacy-validation/2"

    attempt = %{
      id: attempt_id,
      stage: stage.id,
      type: :check,
      ordinal: 2,
      status: :completed,
      candidate_context_id: nil,
      gate: :passed,
      gate_detail: %{verdict: :passed, verified: true},
      result: %{status: :ok, evidence: value}
    }

    operation = %{id: attempt_id, stage: stage.id, type: :check, status: :completed}
    artifact = artifact(c.run.id, attempt_id, value)

    run = %{
      c.run
      | status: :complete,
        stage_id: :complete,
        current_attempt_id: nil,
        definition: %{c.run.definition | stages: Map.put(c.run.definition.stages, stage.id, stage)},
        attempts: c.run.attempts ++ [attempt],
        operations: Map.put(c.run.operations, attempt_id, operation),
        outputs: Map.put(c.run.outputs, "legacy-validation", value),
        artifacts: Map.put(c.run.artifacts, "legacy-validation", artifact)
    }

    body = %{
      version: 1,
      run_id: run.id,
      task_id: run.task_id,
      policy: run.policy,
      workspace: run.workspace,
      execution:
        Map.take(run.execution, [
          :validation_policy,
          :validation_base,
          :validation_archive,
          :validation_scratch,
          :validation_candidates,
          :validation_check,
          :worker_control
        ]),
      definitions: run.definition.definitions,
      stage: stage,
      operation_id: attempt_id,
      result: attempt.result,
      gate: attempt.gate,
      gate_detail: attempt.gate_detail
    }

    proof = %{
      version: 1,
      sha256: digest(body),
      mac:
        :crypto.mac(:hmac, :sha256, c.key, @service_legacy_domain <> :erlang.term_to_binary(body))
        |> Base.encode16(case: :lower)
    }

    run = put_in(run.operations[attempt_id][:service_completion], proof)

    assert ServiceCheckCompletion.verified?(run, attempt_id)

    assert {:ok, %{"kind" => "candidate_validation", "value" => ^value}} =
             WorkstreamOutputs.projection(run, "legacy-validation")
  end

  defp completed_review(c, opts \\ []) do
    run = WorkstreamRun.start_stage(c.run)
    checkout_stage = run.definition.stages["review-checkout"]

    checkout = %{
      "root" => c.review_root,
      "workspace" => c.review_workspace,
      "source_workspace" => run.workspace,
      "current_operation_id" => run.current_attempt_id,
      "number" => 241,
      "url" => c.pull_url,
      "base_sha" => c.base_sha,
      "head_sha" => c.head_sha,
      "head_branch" => "cycle/dev-241",
      "base_branch" => "main",
      "rubric_sha256" => ReviewResult.rubric().sha256,
      "cloud_thread_id" => run.thread_id
    }

    run = WorkstreamRun.finish_stage(run, {:ok, signed_review_result(run, checkout_stage, checkout, c.key)})

    result = review_result(c, Keyword.get(opts, :actionable, false))

    run =
      run
      |> WorkstreamRun.start_stage()
      |> WorkstreamRun.finish_stage(
        {:ok,
         %{
           thread_id: "reviewer-thread-241",
           session_id: "reviewer-session-241",
           workspace: c.review_workspace,
           review_json: Jason.encode!(result)
         }}
      )

    publication_stage = run.definition.stages["review-publication"]
    run = WorkstreamRun.start_stage(run)
    publication = review_evidence(c, result, run)
    WorkstreamRun.finish_stage(run, {:ok, signed_review_result(run, publication_stage, publication, c.key)})
  end

  defp signed_review_result(run, stage, value, key) do
    attempt_id = run.current_attempt_id
    operation_id = run.operations[attempt_id].side_effect_id || attempt_id
    assert {:ok, view} = CandidateContext.view(run, attempt_id)
    assert {:ok, contract} = Review.contract_identity(run, stage)

    body =
      value
      |> json_value()
      |> Map.merge(%{
        "run_id" => run.id,
        "stage_id" => stage.id,
        "operation_id" => operation_id,
        "candidate_context_id" => view.candidate_context_id,
        "contract_sha256" => contract
      })

    bytes = body |> json_value() |> :erlang.term_to_binary([:deterministic])
    signature = :crypto.mac(:hmac, :sha256, key, @review_domain <> bytes) |> Base.encode16(case: :lower)
    Map.put(body, "seal", signature)
  end

  defp review_evidence(c, result, _run) do
    %{
      "base_sha" => c.base_sha,
      "head_sha" => c.head_sha,
      "pr_number" => 241,
      "pr_url" => c.pull_url,
      "review_id" => 90241,
      "review_url" => c.pull_url <> "#pullrequestreview-90241",
      "author_login" => "rig-review-app[bot]",
      "event" => if(result["findings"] == [], do: "APPROVE", else: "COMMENT"),
      "result" => result,
      "locations" =>
        Enum.map(result["findings"], fn _ ->
          %{"path" => "README.md", "start_line" => 1, "end_line" => 1, "side" => "right"}
        end),
      "rubric_sha256" => ReviewResult.rubric().sha256,
      "reviewer_thread_id" => "reviewer-thread-241",
      "reviewer_session_id" => "reviewer-session-241",
      "actionable" => result["findings"] != [],
      "disagreement" => false,
      "observed_at" => System.system_time(:second)
    }
  end

  defp review_result(_c, false) do
    %{
      "overall_correctness" => "patch is correct",
      "overall_explanation" => "No actionable findings.",
      "overall_confidence_score" => 0.99,
      "findings" => []
    }
  end

  defp review_result(c, true) do
    %{
      "overall_correctness" => "patch is correct",
      "overall_explanation" => "One issue needs a repair.",
      "overall_confidence_score" => 0.91,
      "findings" => [
        %{
          "priority" => 1,
          "title" => "Validate this boundary",
          "body" => "Reject the unsafe value before use.",
          "confidence_score" => 0.91,
          "code_location" => %{"absolute_file_path" => Path.join(c.review_workspace, "README.md"), "line_range" => %{"start" => 1, "end" => 1}}
        }
      ]
    }
  end

  defp definition(rubric_sha) do
    %{
      entry: "review-checkout",
      agents: %{},
      inputs: [],
      input_values: %{},
      definitions: %{"flow.yaml" => %{sha256: String.duplicate("d", 64)}},
      stages: %{
        "publish" => %{id: "publish", type: :publish, outputs: ["publication"]},
        "review-checkout" => %{
          id: "review-checkout",
          type: :check,
          inputs: ["publication"],
          outputs: ["review-checkout"],
          gate: %{evaluator: :review_checkout, validation_stage: "github-validation", success: "review-agent", failure: :blocked}
        },
        "review-agent" => %{
          id: "review-agent",
          type: :agent,
          outputs: ["review-agent"],
          review_checkout_stage: "review-checkout",
          next: "review-publication"
        },
        "review-publication" => %{
          id: "review-publication",
          type: :check,
          inputs: ["review-checkout", "review-agent"],
          outputs: ["review-publication"],
          gate: %{
            evaluator: :review_publication,
            validation_stage: "github-validation",
            checkout_stage: "review-checkout",
            review_stage: "review-agent",
            success: :complete,
            failure: %{repair: "implement", max_attempts: 3}
          }
        },
        "implement" => %{id: "implement", type: :agent, outputs: ["repair"], next: :complete}
      },
      pinned_rubric: rubric_sha
    }
  end

  defp replace_output(run, name, value) do
    artifact = run.artifacts[name]
    attempt_id = artifact.attempt_id

    attempts =
      Enum.map(run.attempts, fn attempt ->
        if attempt.id == attempt_id do
          %{attempt | result: Map.put(attempt.result, :evidence, value)}
        else
          attempt
        end
      end)

    %{
      run
      | outputs: Map.put(run.outputs, name, value),
        artifacts: Map.put(run.artifacts, name, %{artifact | sha256: digest(value)}),
        attempts: attempts
    }
  end

  defp artifact(run_id, attempt_id, value), do: %{run_id: run_id, attempt_id: attempt_id, sha256: digest(value)}
  defp digest(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value)) |> Base.encode16(case: :lower)
  defp json_value(value), do: value |> Jason.encode!() |> Jason.decode!()

  defp write_upload_receipt(root, view) do
    # This is a synthetic uploader receipt for contract coverage, not a remote upload claim.
    receipts = Path.join(root, "receipts")
    File.mkdir_p!(receipts)

    receipt = %{
      "version" => 1,
      "status" => "verified",
      "run_id" => view.snapshot["id"],
      "revision" => view.revision,
      "manifest_sha256" => view.revision,
      "artifacts" => view.artifacts
    }

    path = Path.join(receipts, view.revision <> ".json")
    File.write!(path, Jason.encode!(receipt))
    File.chmod!(path, 0o640)
  end
end
