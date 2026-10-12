defmodule SymphonyElixir.ReviewExecutionTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{CandidateContext, Config, Evidence, Review, ReviewResult, ReviewWorkspace, Validation, ValidationCheck, ValidationPolicy, Workstream, WorkstreamRun, WorkstreamRunner}

  @validation_image "python@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea"
  @reviewer_json Jason.encode!(%{
                   "overall_correctness" => "patch is correct",
                   "overall_explanation" => "No actionable findings.",
                   "overall_confidence_score" => 0.99,
                   "findings" => []
                 })

  setup do
    assert Config.evidence() == :disabled
    assert Process.whereis(Evidence) == nil

    root = Path.join(System.tmp_dir!(), "review-execution-#{System.unique_integer([:positive])}")
    seed = Path.join(root, "seed")
    workspace_root = Path.join(root, "workspaces")
    workspace = Path.join(workspace_root, "rig")
    review_root = Path.join(root, "review-checkouts")
    definition_root = Path.join(root, "definitions")
    validation_archive = Path.join(root, "validation-archive")
    validation_scratch = Path.join(root, "validation-scratch")

    File.mkdir_p!(seed)
    File.mkdir_p!(review_root)
    git!(seed, ["init", "--quiet", "-b", "main"])
    git!(seed, ["config", "user.name", "Review Fixture"])
    git!(seed, ["config", "user.email", "review-fixture@example.test"])
    File.write!(Path.join(seed, "README.md"), "base version\n")
    git!(seed, ["add", "README.md"])
    git!(seed, ["commit", "--quiet", "-m", "base"])
    base_sha = git!(seed, ["rev-parse", "HEAD"])

    File.mkdir_p!(workspace_root)
    git!(root, ["clone", "--quiet", "--", seed, workspace])
    git!(workspace, ["config", "user.name", "Review Fixture"])
    git!(workspace, ["config", "user.email", "review-fixture@example.test"])
    git!(workspace, ["checkout", "--quiet", "-b", "feature/review"])
    File.write!(Path.join(workspace, "README.md"), "candidate version\n")
    git!(workspace, ["add", "README.md"])
    git!(workspace, ["commit", "--quiet", "-m", "candidate"])
    head_sha = git!(workspace, ["rev-parse", "HEAD"])

    definition_path = write_definition!(definition_root)
    assert {:ok, definition} = Workstream.load(definition_path, %{"task" => "review this patch"})

    policy_path = Path.join(root, "validation-policy.yaml")
    File.write!(policy_path, validation_policy_yaml())
    assert {:ok, policy} = ValidationPolicy.load(policy_path, workspace)

    on_exit(fn -> File.rm_rf!(root) end)

    {:ok,
     root: root,
     seed: seed,
     workspace_root: workspace_root,
     workspace: workspace,
     review_root: review_root,
     definition: definition,
     validation_archive: validation_archive,
     validation_scratch: validation_scratch,
     policy: policy,
     base_sha: base_sha,
     head_sha: head_sha,
     publisher_config: config_file!(root, "publisher.pem", 101, 201),
     validation_config: config_file!(root, "validation.pem", 102, 202)}
  end

  test "prepares a separate clean Git clone, replays it, and rejects a moved source head", c do
    run = %{id: "review-workspace-run", workspace: c.workspace, current_attempt_id: "checkout-attempt", operations: %{"checkout-attempt" => %{side_effect_id: "stable-checkout"}}}
    identity = workspace_identity(c, "cloud-thread", nil)

    assert {:ok, prepared} = ReviewWorkspace.prepare(run, identity, workspace_root: c.review_root)
    assert prepared.workspace != c.workspace
    assert File.dir?(Path.join(prepared.workspace, ".git"))
    assert git!(prepared.workspace, ["rev-parse", "HEAD"]) == c.head_sha
    assert git!(prepared.workspace, ["status", "--porcelain=v1"]) == ""
    assert {:ok, ^prepared} = ReviewWorkspace.prepare(run, identity, workspace_root: c.review_root)

    File.write!(Path.join(c.workspace, "README.md"), "moved candidate version\n")
    git!(c.workspace, ["add", "README.md"])
    git!(c.workspace, ["commit", "--quiet", "-m", "move head"])

    assert {:error, :review_checkout_failed} = ReviewWorkspace.prepare(run, identity, workspace_root: c.review_root)
  end

  test "rejects unsafe Git configuration", c do
    git!(c.workspace, ["config", "alias.untrusted", "!touch escaped"])
    run = %{id: "unsafe-review-run", workspace: c.workspace, current_attempt_id: "checkout-attempt", operations: %{}}
    identity = workspace_identity(c, "cloud-thread", nil)

    assert {:error, :review_checkout_failed} = ReviewWorkspace.prepare(run, identity, workspace_root: c.review_root)
  end

  test "rejects a symlinked review root", c do
    File.mkdir_p!(c.review_root)
    symlink_root = Path.join(c.root, "review-root-link")
    File.ln_s!(c.review_root, symlink_root)
    run = %{id: "symlink-review-run", workspace: c.workspace, current_attempt_id: "checkout-attempt", operations: %{}}
    identity = workspace_identity(c, "cloud-thread", nil)

    assert {:error, :review_checkout_failed} = ReviewWorkspace.prepare(run, identity, workspace_root: symlink_root)
  end

  test "a stale validation receipt blocks reviewer launch", c do
    run = base_run(c)
    File.mkdir_p!(c.validation_archive)
    write_key!(c.validation_archive)
    validation_id = run.id <> "/validate/1"
    validation_attempt = validation_attempt(validation_id, %{})
    run = %{run | attempts: [validation_attempt], operations: %{validation_id => operation(validation_id)}}
    {remote, _pr} = start_validation_remote(c)
    review_run = prepared_review_run(c, run)

    executor = fn _workspace, _prompt, _issue, _opts ->
      send(self(), :reviewer_agent_launched)
      {:ok, %{thread_id: "unexpected-review-thread"}}
    end

    assert {:error, _reason} = execute_reviewer(c, review_run, executor)
    refute_received :reviewer_agent_launched
    stop_validation_remote(remote)
  end

  test "launches an isolated reviewer only with authenticated validation evidence and captures one exact final JSON message", c do
    c = validated_fixture(c)
    run = review_ready_run(c)
    assert {:ok, %{qualified: true}} = ValidationCheck.reconcile(run, c.definition.stages["github-validation"], validation_options(c))
    review_run = prepared_review_run(c, run)

    {remote, pr} = c.validation_remote
    Agent.update(remote, &put_in(&1.pr.head_sha, String.duplicate("a", 40)))

    launch_probe = fn _workspace, _prompt, _issue, _opts ->
      send(self(), :reviewer_agent_launched)
      {:ok, %{thread_id: "unexpected-review-thread"}}
    end

    assert {:error, _reason} = execute_reviewer(c, review_run, launch_probe)
    refute_received :reviewer_agent_launched
    Agent.update(remote, &Map.put(&1, :pr, pr))

    parent = self()

    question = fn _request ->
      send(parent, :reviewer_question_tool_called)
      :ok
    end

    wait = fn _request ->
      send(parent, :reviewer_wait_tool_called)
      :ok
    end

    executor = fn workspace, prompt, issue, app_opts ->
      assert workspace == review_run.outputs["review-checkout"]["workspace"]
      assert workspace != c.workspace
      assert issue.identifier == "review-execution"
      assert app_opts[:factory_tools] == false
      assert app_opts[:dynamic_tools] == false
      assert app_opts[:on_question] == question
      assert app_opts[:on_wait] == wait
      assert prompt =~ ReviewResult.rubric().text
      refute prompt =~ "pending cloud clarification"
      refute prompt =~ "cloud-only continuation"
      refute prompt =~ "Gate failure triggering repair:"
      refute prompt =~ "prior_thread_id"
      refute prompt =~ "prior_session_id"
      refute prompt =~ "clarifications"
      assert :ok = app_opts[:on_message].(completed_message(@reviewer_json))
      {:ok, %{thread_id: "independent-review-thread", session_id: "review-session", turn_id: "review-turn"}}
    end

    assert {:ok, captured} = execute_reviewer(c, review_run, executor, on_question: question, on_wait: wait)
    assert captured.thread_id == "independent-review-thread"
    assert captured.session_id == "review-session"
    assert captured.workspace == review_run.outputs["review-checkout"]["workspace"]
    assert captured.review_json == @reviewer_json
    refute_received :reviewer_question_tool_called
    refute_received :reviewer_wait_tool_called

    invalid_executors = [
      fn _workspace, _prompt, _issue, _opts -> {:ok, %{thread_id: "separate-thread"}} end,
      fn _workspace, _prompt, _issue, app_opts ->
        assert :ok = app_opts[:on_message].(completed_message(String.duplicate("x", 1_048_577)))
        {:ok, %{thread_id: "separate-thread"}}
      end,
      fn _workspace, _prompt, _issue, app_opts ->
        assert :ok = app_opts[:on_message].(completed_message(@reviewer_json))
        assert :ok = app_opts[:on_message].(completed_message(@reviewer_json))
        {:ok, %{thread_id: "separate-thread"}}
      end,
      fn _workspace, _prompt, _issue, app_opts ->
        assert :ok = app_opts[:on_message].(completed_message(@reviewer_json))
        {:ok, %{thread_id: review_run.thread_id}}
      end
    ]

    for invalid_executor <- invalid_executors do
      assert {:error, :independent_review_output_missing} = execute_reviewer(c, review_run, invalid_executor)
    end

    stop_validation_remote(remote)
  end

  test "maps a repair Cloud Agent final disagreement response to structured fields", c do
    explanation = "The finding does not apply because the caller already validates this boundary."
    response = Jason.encode!(%{"review_disagreement" => true, "explanation" => explanation})
    state = repair_run(c)
    assert {:ok, view} = CandidateContext.view(state, state.current_attempt_id)
    assert view.execution[:candidate_context_id] == state.current_candidate_context_id
    assert view.execution[:repair_parent_sha] == c.head_sha

    executor = fn _workspace, prompt, _issue, app_opts ->
      assert prompt =~ "Address this submitted review"
      assert prompt =~ "review_disagreement (boolean)"
      assert prompt =~ "Validate the boundary"
      assert :ok = app_opts[:on_message].(completed_message(response))
      {:ok, %{thread_id: "cloud-repair-thread", session_id: "cloud-repair-session"}}
    end

    assert {:ok, repair} =
             WorkstreamRunner.execute_stage(
               "implement",
               c.definition,
               state,
               workspace: c.workspace,
               workspace_root: c.workspace_root,
               agent_executor: executor
             )

    assert repair.review_disagreement
    assert repair.repair_explanation == explanation
    assert repair.thread_id == "cloud-repair-thread"
  end

  defp repair_run(c) do
    write_key!(c.validation_archive)
    run = base_run(c)
    publication_id = run.id <> "/publish/1"
    review_id = run.id <> "/review-result/2"
    pr_url = "https://github.com/RigCodeAI/rig/pull/241"

    publication = %{
      id: publication_id,
      stage: "publish",
      type: :publish,
      ordinal: 1,
      status: :completed,
      gate: :passed,
      candidate_context_id: nil,
      result: %{
        status: :ok,
        evidence: %{
          "candidate_sha" => c.head_sha,
          "pr" => %{"number" => 241, "url" => pr_url, "head_branch" => "feature/review", "base_branch" => "main"}
        }
      }
    }

    unsigned_review = %{
      "run_id" => run.id,
      "stage_id" => "review-result",
      "operation_id" => review_id,
      "candidate_context_id" => nil,
      "observed_at" => System.system_time(:second),
      "rubric_sha256" => ReviewResult.rubric().sha256,
      "review_id" => 24_100,
      "review_url" => pr_url <> "#pullrequestreview-24100",
      "pr_number" => 241,
      "pr_url" => pr_url,
      "base_sha" => c.base_sha,
      "head_sha" => c.head_sha,
      "actionable" => true,
      "disagreement" => false,
      "event" => "COMMENT",
      "result" => %{
        "overall_correctness" => "patch is incorrect",
        "findings" => [%{"title" => "[P1] Validate the boundary", "body" => "Reject this value before dispatch."}]
      }
    }

    review_stage = run.definition.stages["review-result"]
    assert {:ok, contract_sha256} = Review.contract_identity(run, review_stage)
    unsigned_review = Map.put(unsigned_review, "contract_sha256", contract_sha256)
    key = File.read!(Path.join(c.validation_archive, "producer.key"))
    review = Map.put(unsigned_review, "seal", review_seal(unsigned_review, key))

    review_attempt = %{
      id: review_id,
      stage: "review-result",
      type: :check,
      ordinal: 2,
      status: :completed,
      gate: :passed,
      candidate_context_id: nil,
      result: %{status: :ok, evidence: review}
    }

    run = %{
      run
      | status: :executing,
        stage_id: "review-result",
        current_attempt_id: review_id,
        attempts: [publication, review_attempt]
    }

    assert :ok = Review.verify_completion(run, run.definition.stages["review-result"], {:ok, review})
    assert {:ok, authorized} = CandidateContext.authorize_repair(run, review)

    repair =
      authorized
      |> Map.merge(%{status: :ready, stage_id: "implement", current_attempt_id: nil, continuation: nil})
      |> WorkstreamRun.start_stage()

    restored = repair |> :erlang.term_to_binary() |> :erlang.binary_to_term([:safe])
    assert {:ok, _view} = CandidateContext.view(restored, restored.current_attempt_id)
    restored
  end

  defp validated_fixture(c) do
    evidence_config = %{
      "root" => Path.join(c.root, "private-evidence"),
      "base_url" => "https://evidence.example.test",
      "iap_audience" => "/projects/1/global/backendServices/2",
      "allowed_domains" => ["example.test"],
      "allowed_emails" => []
    }

    {:ok, evidence_server} =
      start_supervised({Evidence, [config: {:ok, evidence_config}, name: nil, receipt_uid: File.stat!(c.root).uid]})

    run = base_run(c, %{evidence_server: evidence_server, evidence_url: "https://evidence.example.test/tasks/review-fixture"})
    validation_id = run.id <> "/validate/1"
    context = %{task_id: run.task_id, run_id: run.id, attempt_id: validation_id, base_sha: c.base_sha}
    required = [%{check: "contract", assertion: "exit_status", equals: 0}]
    {receipt, manifest_bytes} = write_signed_contract(c.workspace, c.validation_archive, c.policy, context, c.base_sha)
    assert {:ok, %{verdict: :passed}} = Validation.verify_result({:ok, %{receipt: receipt}}, c.policy, context, required, validation_options(c))
    validation_attempt = validation_attempt(validation_id, %{"receipt" => receipt})
    run = %{run | attempts: [validation_attempt], operations: %{validation_id => operation(validation_id)}}

    evidence_run = %{run | status: :executing, stage_id: "validate", current_attempt_id: validation_id}
    assert :ok = Evidence.snapshot(evidence_run, "validation-imported", %{"kind" => "validation_imported", "manifest_sha256" => receipt["manifest_sha256"]}, evidence_server)

    assert :ok =
             Evidence.artifact(
               %{id: run.id, stage_id: "validate", current_attempt_id: validation_id},
               "validation/" <> validation_id <> "/manifest.json",
               manifest_bytes,
               evidence_server
             )

    assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id), evidence_server)
    write_upload_receipt!(evidence_config["root"], view)

    {remote, pr} = start_validation_remote(c)
    Map.merge(c, %{run: run, validation_remote: {remote, pr}, evidence_server: evidence_server})
  end

  defp review_ready_run(c) do
    %{
      c.run
      | status: :executing,
        stage_id: "review",
        thread_id: "cloud-thread",
        session_id: "cloud-session",
        continuation: %{
          thread_id: "cloud-thread",
          session_id: "cloud-session",
          stage_id: "implement",
          question_prompt: "pending cloud clarification",
          reply: "cloud-only continuation reply",
          messages: [%{"role" => "assistant", "content" => "cloud-only continuation transcript"}],
          questions: [%{id: "clarification", prompt: "pending cloud clarification", reply: %{body: "cloud-only continuation reply"}}]
        }
    }
  end

  defp base_run(c, execution_overrides \\ %{}) do
    execution =
      Map.merge(
        %{
          workspace_root: c.workspace_root,
          codex_command: "pinned-codex",
          validation_policy: c.policy,
          validation_archive: c.validation_archive,
          validation_scratch: c.validation_scratch,
          validation_base: c.base_sha,
          validation_check: c.validation_config,
          validation_check_transport: SymphonyElixir.ReviewExecutionTest.ValidationTransport,
          validation_pr: 241,
          publisher: c.publisher_config,
          review_rubric_sha256: ReviewResult.rubric().sha256
        },
        execution_overrides
      )

    WorkstreamRun.new("DEV-240", c.definition, c.workspace, [], execution)
  end

  defp prepared_review_run(c, run) do
    checkout_id = run.id <> "/checkout/2"
    checkout_operation = %{checkout_id => operation("review-checkout-effect")}
    preparing = %{run | stage_id: "checkout", current_attempt_id: checkout_id, operations: Map.merge(run.operations, checkout_operation)}
    identity = workspace_identity(c, run.thread_id || "cloud-thread", Map.get(run, :current_candidate_context_id))
    assert {:ok, checkout} = ReviewWorkspace.prepare(preparing, identity, workspace_root: c.review_root)
    evidence = seal_checkout_fixture(preparing, "checkout", checkout_id, checkout)

    checkout_attempt = %{
      id: checkout_id,
      stage: "checkout",
      type: :check,
      ordinal: 2,
      status: :completed,
      gate: :passed,
      candidate_context_id: Map.get(run, :current_candidate_context_id),
      result: %{status: :ok, evidence: evidence}
    }

    run = %{
      run
      | status: :ready,
        stage_id: "review",
        current_attempt_id: nil,
        thread_id: run.thread_id || "cloud-thread",
        session_id: run.session_id || "cloud-session",
        attempts: run.attempts ++ [checkout_attempt],
        operations: Map.merge(run.operations, checkout_operation),
        outputs: Map.put(run.outputs, "review-checkout", evidence)
    }

    WorkstreamRun.start_stage(run)
  end

  defp execute_reviewer(c, run, executor, extra_opts \\ []) do
    WorkstreamRunner.execute_stage(
      "review",
      c.definition,
      run,
      Keyword.merge(
        [
          workspace: c.workspace,
          workspace_root: c.workspace_root,
          agent_executor: executor,
          validation_check_transport: SymphonyElixir.ReviewExecutionTest.ValidationTransport,
          on_publication_check: fn -> :ok end
        ],
        extra_opts
      )
    )
  end

  defp workspace_identity(c, cloud_thread, candidate_context_id) do
    %{
      number: 241,
      url: "https://github.com/RigCodeAI/rig/pull/241",
      base_sha: c.base_sha,
      head_sha: c.head_sha,
      head_branch: "feature/review",
      base_branch: "main",
      rubric_sha256: ReviewResult.rubric().sha256,
      cloud_thread_id: cloud_thread,
      candidate_context_id: candidate_context_id
    }
  end

  defp seal_checkout_fixture(run, stage_id, attempt_id, checkout) do
    stage = run.definition.stages[stage_id]
    assert {:ok, contract_sha256} = Review.contract_identity(run, stage)

    result =
      checkout
      |> Jason.encode!()
      |> Jason.decode!()
      |> Map.merge(%{
        "run_id" => run.id,
        "stage_id" => stage_id,
        "operation_id" => run.operations[attempt_id].side_effect_id,
        "candidate_context_id" => run[:current_candidate_context_id],
        "contract_sha256" => contract_sha256
      })

    key = File.read!(Path.join(run.execution.validation_archive, "producer.key"))
    Map.put(result, "seal", review_seal(result, key))
  end

  defp review_seal(value, key) do
    normalized = value |> Jason.encode!() |> Jason.decode!()
    bytes = :erlang.term_to_binary(normalized, [:deterministic])
    :crypto.mac(:hmac, :sha256, key, "factory-review-evidence-v1\n" <> bytes) |> Base.encode16(case: :lower)
  end

  defp validation_attempt(id, evidence) do
    %{id: id, stage: "validate", type: :check, ordinal: 1, status: :completed, gate: :passed, gate_detail: nil, result: %{evidence: evidence}}
  end

  defp operation(side_effect_id), do: %{side_effect_id: side_effect_id, status: :completed}

  defp start_validation_remote(c) do
    pr = %{
      number: 241,
      url: "https://github.com/RigCodeAI/rig/pull/241",
      state: "open",
      draft: false,
      head_sha: c.head_sha,
      base_sha: c.base_sha,
      head_branch: "feature/review",
      base_branch: "main",
      head_repository_full_name: "RigCodeAI/rig",
      base_repository_full_name: "RigCodeAI/rig"
    }

    {:ok, remote} = Agent.start_link(fn -> %{pr: pr, base_sha: c.base_sha, checks: [], creates: 0, updates: 0} end)
    :persistent_term.put({__MODULE__, :validation_remote}, remote)

    on_exit(fn ->
      :persistent_term.erase({__MODULE__, :validation_remote})
      if Process.alive?(remote), do: Agent.stop(remote)
    end)

    {remote, pr}
  end

  defp stop_validation_remote(remote) do
    :persistent_term.erase({__MODULE__, :validation_remote})
    if Process.alive?(remote), do: Agent.stop(remote)
  end

  defp validation_options(c) do
    [
      on_publication_check: fn -> :ok end,
      validation_check_transport: SymphonyElixir.ReviewExecutionTest.ValidationTransport,
      workspace: c.workspace,
      validation_archive: c.validation_archive,
      validation_scratch: c.validation_scratch
    ]
  end

  defp completed_message(text) do
    %{
      payload: %{
        "method" => "item/completed",
        "params" => %{"item" => %{"id" => "review-output", "type" => "agentMessage", "phase" => "final", "text" => text}}
      },
      thread_id: "independent-review-thread",
      session_id: "review-session",
      turn_id: "review-turn"
    }
  end

  defp write_upload_receipt!(root, view) do
    receipts = Path.join(root, "receipts")
    File.mkdir_p!(receipts)

    receipt = %{
      "status" => "verified",
      "revision" => view.revision,
      "run_id" => view.snapshot["id"],
      "manifest_sha256" => view.revision,
      "artifacts" => view.artifacts
    }

    path = Path.join(receipts, view.revision <> ".json")
    File.write!(path, Jason.encode!(receipt))
    File.chmod!(path, 0o640)
  end

  # Contract-only evidence fixture. The manifest is authenticated and archived, but its
  # command result is explicitly synthetic and does not represent an executed check.
  defp write_signed_contract(workspace, archive, policy, context, base_sha) do
    File.mkdir_p!(archive)
    paths = git!(workspace, ["ls-tree", "-rz", "--name-only", "HEAD"]) |> String.split("\0", trim: true)
    changed_paths = git!(workspace, ["diff", "--no-ext-diff", "--no-renames", "--ignore-submodules=all", "--name-only", "-z", base_sha, "HEAD"]) |> String.split("\0", trim: true)
    {:ok, source_digest} = Validation.source_digest(workspace, paths)
    {:ok, checks} = ValidationPolicy.resolve(policy, changed_paths)
    candidate_sha = git!(workspace, ["rev-parse", "HEAD"])
    source_tree = git!(workspace, ["rev-parse", "HEAD^{tree}"])

    request = %{
      "candidate_sha" => candidate_sha,
      "base_sha" => base_sha,
      "source_tree" => source_tree,
      "source_digest" => source_digest,
      "changed_paths" => changed_paths,
      "version" => "candidate-validation-v1",
      "service_digest" => validation_service_digest(),
      "mode" => "final",
      "policy_digest" => policy.digest,
      "policy_revision" => policy.revision,
      "environment" => Jason.decode!(Jason.encode!(policy.environment)),
      "context" => Jason.decode!(Jason.encode!(context)),
      "check_ids" => Enum.map(checks, & &1.id)
    }

    id = digest(Jason.encode!(request))
    key_path = Path.join(archive, "producer.key")
    key = :crypto.strong_rand_bytes(32)
    File.write!(key_path, key)
    File.chmod!(key_path, 0o600)

    output = "synthetic contract check; not executed in this test\n"
    artifact = %{"file" => "check-" <> digest("contract") <> ".log", "sha256" => digest(output), "bytes" => byte_size(output)}

    check = %{
      "id" => "contract",
      "command" => ["python3", "-c", "pass"],
      "adapter" => "command-v1",
      "timeout_ms" => 5_000,
      "duration_ms" => 0,
      "status" => "completed",
      "reason" => "exited",
      "exit_status" => 0,
      "source_after" => source_digest,
      "build_digest" => nil,
      "limits" => %{},
      "assertions" => %{"exit_status" => 0, "completed" => true, "quality_score" => 100},
      "artifact" => artifact
    }

    manifest =
      Map.merge(request, %{
        "id" => id,
        "producer" => digest(key),
        "started_at" => "2026-10-11T00:00:00Z",
        "finished_at" => "2026-10-11T00:00:00Z",
        "complete" => true,
        "checks" => [check],
        "artifacts" => [artifact]
      })

    bytes = Jason.encode!(manifest, pretty: true)
    directory = Path.join(archive, id)
    File.mkdir_p!(directory)
    File.write!(Path.join(directory, "manifest.json"), bytes)
    signature = :crypto.mac(:hmac, :sha256, key, bytes) |> Base.encode16(case: :lower)
    File.write!(Path.join(directory, "manifest.mac"), signature)
    File.write!(Path.join(directory, artifact["file"]), output)

    {%{"id" => id, "manifest_sha256" => digest(bytes)}, bytes}
  end

  defp validation_service_digest do
    lib = Path.expand("../../lib/symphony_elixir", __DIR__)

    ["validation.ex", "validation_policy.ex", "validation_command.ex", "candidate_git.ex"]
    |> Enum.map(&File.read!(Path.join(lib, &1)))
    |> IO.iodata_to_binary()
    |> digest()
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp write_definition!(root) do
    agents = Path.join(root, "agents")
    File.mkdir_p!(Path.join(agents, "skills/review"))
    File.write!(Path.join(agents, "implementation.md"), "Implement the requested patch.")
    File.write!(Path.join(agents, "reviewer.md"), "Independently inspect the committed change.")
    File.write!(Path.join(agents, "skills/review/SKILL.md"), "Read only; do not edit source files.")
    File.write!(Path.join(agents, "implementer.yaml"), agent_yaml("implementer", "implementation.md", "skills/review/SKILL.md"))
    File.write!(Path.join(agents, "reviewer.yaml"), agent_yaml("reviewer", "reviewer.md", "skills/review/SKILL.md"))

    path = Path.join(root, "workstreams/review.yaml")
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, review_workstream_yaml())
    path
  end

  defp agent_yaml(name, instruction, skill) do
    """
    version: 1
    name: #{name}
    model: gpt-6-luna
    reasoning_effort: high
    daybreak: false
    approval_policy: never
    sandbox: workspace-write
    instructions: [#{instruction}]
    skills: [#{skill}]
    """
  end

  defp review_workstream_yaml do
    """
    version: 1
    name: review-execution
    inputs: [task]
    agents:
      implementer: ../agents/implementer.yaml
      reviewer: ../agents/reviewer.yaml
    entry: implement
    stages:
      - id: implement
        type: agent
        inputs: [task]
        outputs: [patch]
        agent: implementer
        prompt: Implement the requested change.
        next: validate
      - id: validate
        type: check
        inputs: [patch]
        outputs: [validated]
        gate:
          evaluator: candidate_validation
          required:
            - {check: contract, assertion: exit_status, equals: 0}
          success: publish
          failure: blocked
      - id: publish
        type: publish
        inputs: [validated]
        outputs: [publication]
        gate:
          validation_stage: validate
          required:
            - {check: contract, assertion: exit_status, equals: 0}
          success: github-validation
          failure: blocked
      - id: github-validation
        type: check
        inputs: [publication, validated]
        outputs: [github-validation]
        gate:
          evaluator: github_validation
          validation_stage: validate
          required:
            - {check: contract, assertion: exit_status, equals: 0}
          success: checkout
          failure: blocked
      - id: checkout
        type: check
        inputs: [github-validation]
        outputs: [review-checkout]
        gate:
          evaluator: review_checkout
          validation_stage: github-validation
          success: review
          failure: blocked
      - id: review
        type: agent
        inputs: [review-checkout]
        outputs: [review-findings]
        agent: reviewer
        prompt: Review the recorded candidate.
        review_checkout_stage: checkout
        next: review-result
      - id: review-result
        type: check
        inputs: [review-findings]
        outputs: [review-publication]
        gate:
          evaluator: review_publication
          validation_stage: github-validation
          review_stage: review
          checkout_stage: checkout
          success: complete
          failure:
            repair: implement
            max_attempts: 3
    """
  end

  defp validation_policy_yaml do
    """
    version: 1
    revision: review-execution-v1
    environment:
      image: #{@validation_image}
      runner_version: docker-v1
    checks:
      - id: contract
        adapter: command
        command: [python3, -c, "print('validation passed')"]
        timeout_ms: 5000
        paths: [README.md]
        result_format: exit_status
    """
  end

  defp git!(directory, args) do
    {output, 0} = System.cmd("git", ["-C", directory | args], env: [{"GIT_CONFIG_GLOBAL", "/dev/null"}, {"GIT_CONFIG_NOSYSTEM", "1"}], stderr_to_stdout: true)
    String.trim(output)
  end

  defp config_file!(root, name, app_id, installation_id) do
    path = Path.join(root, name)
    File.write!(path, "test-only credential path\n")
    %{app_id: app_id, installation_id: installation_id, private_key_path: path}
  end

  defp write_key!(archive) do
    File.mkdir_p!(archive)
    path = Path.join(archive, "producer.key")
    File.write!(path, :crypto.strong_rand_bytes(32))
    File.chmod!(path, 0o600)
  end
end

defmodule SymphonyElixir.ReviewExecutionTest.ValidationTransport do
  def get_pr(_config, _number), do: {:ok, Agent.get(remote(), & &1.pr)}
  def base_sha(_config), do: {:ok, Agent.get(remote(), & &1.base_sha)}
  def checks(_config, head_sha), do: {:ok, Agent.get(remote(), &Enum.filter(&1.checks, fn check -> check.head_sha == head_sha end))}

  def create_check(config, payload) do
    Agent.get_and_update(remote(), fn state ->
      check = Map.merge(payload, %{id: state.creates + 1, app_id: config.app_id})
      {{:ok, check}, %{state | checks: state.checks ++ [check], creates: state.creates + 1}}
    end)
  end

  def update_check(config, id, payload) do
    Agent.get_and_update(remote(), fn state ->
      check = Map.merge(payload, %{id: id, app_id: config.app_id})
      checks = Enum.map(state.checks, fn old -> if old.id == id, do: check, else: old end)
      {{:ok, check}, %{state | checks: checks, updates: state.updates + 1}}
    end)
  end

  defp remote, do: :persistent_term.get({SymphonyElixir.ReviewExecutionTest, :validation_remote})
end
