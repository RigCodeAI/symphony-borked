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
    review_root = workspace_root
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
     validation_config: config_file!(root, "validation.pem", 102, 202),
     review_config: config_file!(root, "review.pem", 103, 203)}
  end

  test "preparation command pin covers the actual isolated script" do
    argv = ["/usr/bin/python3", "-I", "-c", review_workspace_script() <> "\n"]
    assert byte_size(List.last(argv)) > 4_096
    assert ReviewWorkspace.command_sha256() == Base.encode16(:crypto.hash(:sha256, Jason.encode!(argv)), case: :lower)
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
    retried = %{run | current_attempt_id: "retry-attempt", operations: %{"retry-attempt" => %{side_effect_id: "stable-checkout"}}}
    assert {:ok, replayed} = ReviewWorkspace.prepare(retried, identity, workspace_root: c.review_root)
    assert replayed.workspace == prepared.workspace
    assert replayed.current_operation_id == "retry-attempt"

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

  test "interruption after cloning leaves only private staging and retry publishes a complete checkout", c do
    run = %{id: "interrupted-review-run", workspace: c.workspace, current_attempt_id: "checkout-attempt", operations: %{}}
    identity = workspace_identity(c, "cloud-thread", nil)
    name = :crypto.hash(:sha256, run.id <> "\n" <> run.current_attempt_id) |> Base.encode16(case: :lower)
    target = Path.join([c.review_root, ".factory-reviews", name, "checkout"])

    pins =
      Map.merge(identity, %{root: c.review_root, workspace: target, operation_id: run.current_attempt_id, run_id: run.id, source_workspace: run.workspace, current_operation_id: run.current_attempt_id})

    interrupted_path = Path.join(c.root, "interrupted-staging.json")

    interrupt = """
    import os, pathlib, signal, subprocess
    original_run = subprocess.run
    def interrupted_run(args, **kwargs):
        result = original_run(args, **kwargs)
        if 'clone' in args and result.returncode == 0:
            pathlib.Path(#{Jason.encode!(interrupted_path)}).write_text(args[-1])
            os.kill(os.getpid(), signal.SIGKILL)
        return result
    subprocess.run = interrupted_run
    """

    {_output, status} =
      System.cmd("/usr/bin/python3", ["-I", "-c", interrupt <> review_workspace_script()],
        cd: c.workspace,
        env: [{"WORKSTREAM_INPUTS_JSON", Jason.encode!(pins)}],
        stderr_to_stdout: true
      )

    assert status != 0
    staged = File.read!(interrupted_path)
    assert File.dir?(Path.join(staged, ".git"))
    refute File.exists?(Path.join(staged, ".git/factory-review.json"))
    refute File.exists?(target)
    File.write!(Path.join(staged, "retained-interrupted-data"), "retain me")

    assert {:ok, prepared} = ReviewWorkspace.prepare(run, identity, workspace_root: c.review_root)
    assert prepared.workspace == target
    assert git!(target, ["rev-parse", "HEAD"]) == c.head_sha
    assert git!(target, ["status", "--porcelain=v1"]) == ""
    assert File.read!(Path.join(staged, "retained-interrupted-data")) == "retain me"
    assert {:ok, ^prepared} = ReviewWorkspace.prepare(run, identity, workspace_root: c.review_root)
  end

  test "completed replay rejects changed identity and unknown final directories without deleting data", c do
    run = %{id: "review-replay-identity", workspace: c.workspace, current_attempt_id: "checkout-attempt", operations: %{}}
    identity = workspace_identity(c, "cloud-thread", nil)
    assert {:ok, prepared} = ReviewWorkspace.prepare(run, identity, workspace_root: c.review_root)
    marker = Path.join(prepared.workspace, ".git/factory-review.json")
    original_marker = File.read!(marker)

    assert {:error, :review_checkout_failed} = ReviewWorkspace.prepare(run, %{identity | cloud_thread_id: "another-cloud-thread"}, workspace_root: c.review_root)
    assert File.read!(marker) == original_marker
    assert git!(prepared.workspace, ["rev-parse", "HEAD"]) == c.head_sha
    assert {:ok, ^prepared} = ReviewWorkspace.prepare(run, identity, workspace_root: c.review_root)

    other = %{run | id: "unknown-review-directory"}
    name = :crypto.hash(:sha256, other.id <> "\n" <> other.current_attempt_id) |> Base.encode16(case: :lower)
    target = Path.join([c.review_root, ".factory-reviews", name, "checkout"])
    File.mkdir_p!(target)
    File.write!(Path.join(target, "unknown-data"), "never remove")
    assert {:error, :review_checkout_failed} = ReviewWorkspace.prepare(other, identity, workspace_root: c.review_root)
    assert File.read!(Path.join(target, "unknown-data")) == "never remove"
  end

  test "exclusive publication refuses an empty target created during preparation", c do
    run = %{id: "raced-review-target", workspace: c.workspace, current_attempt_id: "checkout-attempt", operations: %{}}
    identity = workspace_identity(c, "cloud-thread", nil)
    name = :crypto.hash(:sha256, run.id <> "\n" <> run.current_attempt_id) |> Base.encode16(case: :lower)
    target = Path.join([c.review_root, ".factory-reviews", name, "checkout"])

    pins =
      Map.merge(identity, %{root: c.review_root, workspace: target, operation_id: run.current_attempt_id, run_id: run.id, source_workspace: run.workspace, current_operation_id: run.current_attempt_id})

    inode_path = Path.join(c.root, "unknown-target-inode")

    race = """
    import json, os, pathlib, subprocess
    original_run = subprocess.run
    def raced_run(args, **kwargs):
        result = original_run(args, **kwargs)
        if 'checkout' in args and result.returncode == 0:
            target = pathlib.Path(json.loads(os.environ['WORKSTREAM_INPUTS_JSON'])['workspace'])
            target.mkdir()
            pathlib.Path(#{Jason.encode!(inode_path)}).write_text(str(target.stat().st_ino))
        return result
    subprocess.run = raced_run
    """

    {output, status} =
      System.cmd("/usr/bin/python3", ["-I", "-c", race <> review_workspace_script()],
        cd: c.workspace,
        env: [{"WORKSTREAM_INPUTS_JSON", Jason.encode!(pins)}],
        stderr_to_stdout: true
      )

    assert status != 0
    assert output =~ "REVIEW_CHECKOUT_FAILED"
    assert File.ls!(target) == []
    assert File.stat!(target).inode == String.to_integer(File.read!(inode_path))
    assert [_completed_staging_marker] = Path.wildcard(Path.join([c.review_root, ".factory-reviews", name, ".*.prepare-*", "checkout", ".git", "factory-review.json"]), match_dot: true)
    assert {:error, :review_checkout_failed} = ReviewWorkspace.prepare(run, identity, workspace_root: c.review_root)
    assert File.stat!(target).inode == String.to_integer(File.read!(inode_path))
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

  test "submits a correct finding as COMMENT with every inline comment and advances into repair", c do
    c = validated_fixture(c)
    {review_remote, _pr} = start_review_remote(c)

    {c, _reviewer_finished, submission_run, captured} =
      prepare_submission_run(c, "patch is correct", [
        %{priority: 1, title: "Validate the boundary", body: "Reject this value before dispatch."},
        %{priority: 2, title: "Keep the state consistent", body: "Update the related state with this change."}
      ])

    assert captured.review_json =~ "Validate the boundary"
    assert {:ok, submitted} = execute_submission(c, submission_run)
    assert submitted["event"] == "COMMENT"
    assert submitted["actionable"]
    assert submitted["result"]["overall_correctness"] == "patch is correct"

    assert submitted["locations"] == [
             %{"path" => "README.md", "start_line" => 1, "end_line" => 1, "side" => "right"},
             %{"path" => "README.md", "start_line" => 1, "end_line" => 1, "side" => "right"}
           ]

    state = Agent.get(review_remote, & &1)
    assert state.creates == 1
    assert Enum.count(state.calls, &(&1 == :review_comments)) == 1
    assert Enum.take(state.calls, 3) == [:get_pr, :reviews, :create_review]
    assert [first_comment, second_comment] = state.last_payload["comments"]
    assert first_comment["path"] == "README.md"
    assert first_comment["line"] == 1
    assert first_comment["side"] == "RIGHT"
    assert first_comment["body"] =~ "Validate the boundary"
    assert second_comment["body"] =~ "Keep the state consistent"

    completed = WorkstreamRun.finish_stage(submission_run, {:ok, submitted})
    assert completed.status == :ready
    assert completed.stage_id == "implement"
    assert is_binary(completed.current_candidate_context_id)
    stop_review_remote(review_remote)
  end

  test "submits a clean correct verdict as APPROVE and reuses it on replay", c do
    c = validated_fixture(c)
    {review_remote, _pr} = start_review_remote(c)
    {c, _reviewer_finished, submission_run, _captured} = prepare_submission_run(c, "patch is correct", [])

    assert {:ok, submitted} = execute_submission(c, submission_run)
    assert submitted["event"] == "APPROVE"
    assert submitted["actionable"] == false
    assert submitted["result"]["findings"] == []
    assert Agent.get(review_remote, & &1.last_payload["comments"]) == []

    assert {:ok, replayed} = execute_submission(c, submission_run)
    assert replayed["review_id"] == submitted["review_id"]
    assert replayed["event"] == "APPROVE"

    state = Agent.get(review_remote, & &1)
    assert state.creates == 1
    assert Enum.count(state.calls, &(&1 == :reviews)) == 4
    assert Enum.count(state.calls, &(&1 == :review_comments)) == 2
    assert Enum.find_index(state.calls, &(&1 == :create_review)) > Enum.find_index(state.calls, &(&1 == :reviews))

    completed = WorkstreamRun.finish_stage(submission_run, {:ok, submitted})
    assert completed.status == :complete
    assert completed.stage_id == :complete
    stop_review_remote(review_remote)
  end

  test "preserves an incorrect clean verdict as REQUEST_CHANGES and blocks for a human", c do
    c = validated_fixture(c)
    {review_remote, _pr} = start_review_remote(c)
    {c, _reviewer_finished, submission_run, _captured} = prepare_submission_run(c, "patch is incorrect", [])

    assert {:ok, submitted} = execute_submission(c, submission_run)
    assert submitted["event"] == "REQUEST_CHANGES"
    assert submitted["actionable"] == false
    assert submitted["result"]["overall_correctness"] == "patch is incorrect"
    assert submitted["result"]["findings"] == []

    blocked = WorkstreamRun.finish_stage(submission_run, {:ok, submitted})
    assert blocked.status == :blocked
    assert blocked.review_escalation == :review_requires_human
    assert Agent.get(review_remote, & &1.creates) == 1
    stop_review_remote(review_remote)
  end

  test "refuses a missing inline comment response", c do
    c = validated_fixture(c)
    {review_remote, _pr} = start_review_remote(c, comment_mode: :missing)

    {c, _reviewer_finished, submission_run, _captured} =
      prepare_submission_run(c, "patch is correct", [%{priority: 1, title: "Validate the boundary", body: "Reject this value before dispatch."}])

    assert {:error, :review_inline_comments_not_verified} = execute_submission(c, submission_run)
    assert Agent.get(review_remote, & &1.creates) == 1
    stop_review_remote(review_remote)
  end

  test "refuses an inline comment response with a changed body", c do
    c = validated_fixture(c)
    {review_remote, _pr} = start_review_remote(c, comment_mode: :wrong)

    {c, _reviewer_finished, submission_run, _captured} =
      prepare_submission_run(c, "patch is correct", [%{priority: 1, title: "Validate the boundary", body: "Reject this value before dispatch."}])

    assert {:error, :review_inline_comments_not_verified} = execute_submission(c, submission_run)
    assert Agent.get(review_remote, & &1.creates) == 1
    stop_review_remote(review_remote)
  end

  test "does not seal a review after the candidate head moves during submission", c do
    c = validated_fixture(c)
    {validation_remote, _pr} = c.validation_remote
    moved_head = String.duplicate("b", 40)
    on_create = fn -> Agent.update(validation_remote, &put_in(&1.pr.head_sha, moved_head)) end
    {review_remote, _pr} = start_review_remote(c, on_create: on_create)
    {c, _reviewer_finished, submission_run, _captured} = prepare_submission_run(c, "patch is correct", [])

    assert {:error, _reason} = execute_submission(c, submission_run)
    assert Agent.get(review_remote, & &1.creates) == 1
    stop_review_remote(review_remote)
  end

  test "rejects malformed captured JSON before creating a GitHub review", c do
    c = validated_fixture(c)
    {review_remote, _pr} = start_review_remote(c)
    {c, _reviewer_finished, submission_run, captured} = prepare_submission_run(c, "patch is correct", [], review_json: "not JSON")

    assert captured.review_json == "not JSON"
    assert {:error, _reason} = execute_submission(c, submission_run)
    state = Agent.get(review_remote, & &1)
    assert state.creates == 0
    refute :create_review in state.calls
    stop_review_remote(review_remote)
  end

  test "rejects a publication for a different PR even when its base and head match", c do
    c = validated_fixture(c)
    {validation_remote, _pr} = c.validation_remote
    run = review_ready_run(c)
    assert {:ok, %{qualified: true}} = ValidationCheck.reconcile(run, c.definition.stages["github-validation"], validation_options(c))
    run = include_publication(run, c)
    review_run = prepared_review_run(c, run, workspace_root: c.workspace_root)
    review_run = change_publication_pr(review_run, %{"number" => 242, "url" => "https://github.com/RigCodeAI/rig/pull/242"})
    review_started = review_run
    parent = self()

    executor = fn _workspace, _prompt, _issue, _opts ->
      send(parent, :reviewer_launched_for_wrong_pr)
      {:ok, %{thread_id: "unexpected-thread"}}
    end

    assert {:error, _reason} = execute_reviewer(c, review_started, executor)
    refute_received :reviewer_launched_for_wrong_pr
    assert Agent.get(validation_remote, & &1.pr.head_sha) == c.head_sha
  end

  test "prepares a published checkout through the review service", c do
    c = validated_fixture(c)
    {review_remote, _pr} = start_review_remote(c)
    run = review_ready_run(c)
    assert {:ok, %{qualified: true}} = ValidationCheck.reconcile(run, c.definition.stages["github-validation"], validation_options(c))
    run = include_publication(run, c)
    checkout_run = %{run | status: :ready, phase: :queued, stage_id: "checkout", current_attempt_id: nil}
    checkout_started = WorkstreamRun.start_stage(checkout_run)

    assert {:ok, checkout} =
             WorkstreamRunner.execute_stage(
               "checkout",
               c.definition,
               checkout_started,
               Keyword.merge(validation_options(c),
                 workspace: c.workspace,
                 workspace_root: c.workspace_root,
                 review_transport: SymphonyElixir.ReviewExecutionTest.ReviewTransport
               )
             )

    assert checkout["head_sha"] == c.head_sha
    assert checkout["base_sha"] == c.base_sha
    assert checkout["workspace"] != c.workspace
    assert Path.expand(checkout["workspace"]) == checkout["workspace"]
    completed = WorkstreamRun.finish_stage(checkout_started, {:ok, checkout})
    assert completed.status == :ready
    assert completed.stage_id == "review"
    assert completed.outputs["review-checkout"]["workspace"] == checkout["workspace"]
    stop_review_remote(review_remote)
  end

  test "rejects a different publication PR before posting a review with the same head", c do
    c = validated_fixture(c)
    {review_remote, _pr} = start_review_remote(c)
    {c, _reviewer_finished, submission_run, _captured} = prepare_submission_run(c, "patch is correct", [])
    submission_run = change_publication_pr(submission_run, %{"number" => 242, "url" => "https://github.com/RigCodeAI/rig/pull/242"})

    assert {:error, :review_publication_identity_mismatch} = execute_submission(c, submission_run)
    assert Agent.get(review_remote, & &1.creates) == 0
    refute :create_review in Agent.get(review_remote, & &1.calls)
    stop_review_remote(review_remote)
  end

  test "the protected producer receives only the authenticated repair context", c do
    c = %{c | definition: producer_probe_definition(c.definition)}
    repair = repair_run(c)
    forged = %{"id" => String.duplicate("f", 64), "repair_parent_sha" => String.duplicate("d", 40), "validation_base" => String.duplicate("e", 40)}
    inputs = %{"candidate" => %{"thread_id" => "repair-thread"}, "candidate_context" => forged}
    run = %{repair | status: :ready, stage_id: "local-commit", current_attempt_id: nil, outputs: inputs} |> WorkstreamRun.start_stage()

    assert {:ok, %{exit_status: 0, output: output}} =
             WorkstreamRunner.execute_stage("local-commit", c.definition, run, workspace: c.workspace, workspace_root: c.workspace_root)

    actual = Jason.decode!(output)
    assert actual["candidate"] == inputs["candidate"]
    assert actual["candidate_context"] == %{"id" => repair.current_candidate_context_id, "repair_parent_sha" => c.head_sha, "validation_base" => c.base_sha}
    assert run.execution[:candidate_context_id] == nil

    attempts = Enum.map(run.attempts, fn attempt -> if attempt.id == run.current_attempt_id, do: Map.put(attempt, :candidate_context_id, forged["id"]), else: attempt end)

    assert {:error, :candidate_context_not_authorized} =
             WorkstreamRunner.execute_stage("local-commit", c.definition, %{run | attempts: attempts}, workspace: c.workspace, workspace_root: c.workspace_root)
  end

  test "the legacy protected producer discards a model-supplied repair context", c do
    definition = producer_probe_definition(c.definition)
    inputs = %{"candidate" => %{"thread_id" => "cloud-thread"}, "candidate_context" => %{"id" => "untrusted-model-claim"}}
    run = %{base_run(%{c | definition: definition}) | stage_id: "local-commit", outputs: inputs} |> WorkstreamRun.start_stage()

    assert {:ok, %{exit_status: 0, output: output}} =
             WorkstreamRunner.execute_stage("local-commit", definition, run, workspace: c.workspace, workspace_root: c.workspace_root)

    assert Jason.decode!(output) == Map.take(inputs, ["candidate"])
  end

  defp producer_probe_definition(definition) do
    probe = %{
      id: "local-commit",
      type: :check,
      inputs: ["candidate", "candidate_context"],
      outputs: ["candidate-commit"],
      gate: %{command: ["/usr/bin/python3", "-I", "-c", "import os; print(os.environ['WORKSTREAM_INPUTS_JSON'])"], timeout_ms: 10_000}
    }

    %{definition | stages: Map.put(definition.stages, probe.id, probe)}
  end

  defp prepare_submission_run(c, correctness, findings, opts \\ []) do
    run = review_ready_run(c)
    validation_stage = c.definition.stages["github-validation"]
    assert {:ok, %{qualified: true}} = ValidationCheck.reconcile(run, validation_stage, validation_options(c))
    run = include_publication(run, c)
    review_run = prepared_review_run(c, run, workspace_root: c.workspace_root)
    reviewer_started = review_run
    parent = self()

    executor = fn workspace, prompt, _issue, app_opts ->
      assert prompt =~ ReviewResult.rubric().text
      review_json = opts[:review_json] || submission_review_json(workspace, correctness, findings)
      send(parent, {:captured_review_output, review_json})
      assert :ok = app_opts[:on_message].(completed_message(review_json))
      {:ok, %{thread_id: "independent-review-thread", session_id: "review-session", turn_id: "review-turn"}}
    end

    assert {:ok, captured} =
             WorkstreamRunner.execute_stage(
               "review",
               c.definition,
               reviewer_started,
               Keyword.merge(validation_options(c),
                 workspace: c.workspace,
                 workspace_root: c.workspace_root,
                 agent_executor: executor
               )
             )

    expected_json = captured.review_json
    assert_receive {:captured_review_output, ^expected_json}
    reviewer_finished = WorkstreamRun.finish_stage(reviewer_started, {:ok, captured})
    assert reviewer_finished.status == :ready
    assert reviewer_finished.stage_id == "review-result"
    submission_run = WorkstreamRun.start_stage(reviewer_finished)
    {c, reviewer_finished, submission_run, captured}
  end

  defp execute_submission(c, run) do
    WorkstreamRunner.execute_stage(
      "review-result",
      c.definition,
      run,
      Keyword.merge(validation_options(c),
        workspace: c.workspace,
        workspace_root: c.workspace_root,
        review_transport: SymphonyElixir.ReviewExecutionTest.ReviewTransport
      )
    )
  end

  defp submission_review_json(workspace, correctness, findings) do
    findings =
      Enum.map(findings, fn finding ->
        priority = finding.priority

        %{
          "title" => "[P#{priority}] " <> finding.title,
          "body" => finding.body,
          "confidence_score" => 0.94,
          "priority" => priority,
          "code_location" => %{
            "absolute_file_path" => Path.join(workspace, "README.md"),
            "line_range" => %{"start" => 1, "end" => 1}
          }
        }
      end)

    Jason.encode!(%{
      "overall_correctness" => correctness,
      "overall_explanation" => "The committed candidate diff was reviewed.",
      "overall_confidence_score" => 0.97,
      "findings" => findings
    })
  end

  defp include_publication(run, c, pr \\ nil) do
    if Enum.any?(run.attempts, &(&1.type == :publish and &1.status == :completed and &1.gate == :passed)) do
      run
    else
      pr =
        pr ||
          %{
            number: 241,
            url: "https://github.com/RigCodeAI/rig/pull/241",
            head_sha: c.head_sha,
            base_sha: c.base_sha,
            head_branch: "feature/review",
            base_branch: "main"
          }

      publication_id = run.id <> "/publish/1"

      publication = %{
        id: publication_id,
        stage: "publish",
        type: :publish,
        ordinal: 1,
        status: :completed,
        gate: :passed,
        candidate_context_id: nil,
        result: %{status: :ok, evidence: %{"candidate_sha" => c.head_sha, "pr" => Map.new(pr, fn {key, value} -> {to_string(key), value} end)}}
      }

      %{run | attempts: run.attempts ++ [publication]}
    end
  end

  defp change_publication_pr(run, changes) do
    attempts =
      Enum.map(run.attempts, fn attempt ->
        if attempt.type == :publish do
          evidence = attempt.result.evidence
          pr = Map.merge(evidence["pr"], changes)
          %{attempt | result: %{attempt.result | evidence: Map.put(evidence, "pr", pr)}}
        else
          attempt
        end
      end)

    %{run | attempts: attempts}
  end

  defp start_review_remote(c, opts \\ []) do
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
      base_repository_full_name: "RigCodeAI/rig",
      author_login: "human-author"
    }

    state = %{
      pr: pr,
      reviews: [],
      comments: %{},
      creates: 0,
      calls: [],
      comment_mode: Keyword.get(opts, :comment_mode, :normal),
      on_create: Keyword.get(opts, :on_create),
      last_payload: nil
    }

    {:ok, remote} = Agent.start_link(fn -> state end)
    :persistent_term.put({__MODULE__, :review_remote}, remote)

    on_exit(fn ->
      if :persistent_term.get({__MODULE__, :review_remote}, nil) == remote do
        :persistent_term.erase({__MODULE__, :review_remote})
      end

      if Process.alive?(remote), do: Agent.stop(remote)
    end)

    {remote, pr}
  end

  defp stop_review_remote(remote) do
    if :persistent_term.get({__MODULE__, :review_remote}, nil) == remote do
      :persistent_term.erase({__MODULE__, :review_remote})
    end

    if Process.alive?(remote), do: Agent.stop(remote)
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

    run = base_run(c, %{evidence_url: "https://evidence.example.test/tasks/review-fixture"})
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
          review: c.review_config,
          review_rubric_sha256: ReviewResult.rubric().sha256
        },
        execution_overrides
      )

    WorkstreamRun.new("DEV-240", c.definition, c.workspace, [], execution)
  end

  defp prepared_review_run(c, run, opts \\ []) do
    run = include_publication(run, c)
    checkout_id = run.id <> "/checkout/2"
    checkout_operation = %{checkout_id => operation("review-checkout-effect")}
    preparing = %{run | stage_id: "checkout", current_attempt_id: checkout_id, operations: Map.merge(run.operations, checkout_operation)}
    identity = workspace_identity(c, run.thread_id || "cloud-thread", Map.get(run, :current_candidate_context_id))
    assert {:ok, checkout} = ReviewWorkspace.prepare(preparing, identity, workspace_root: Keyword.get(opts, :workspace_root, c.review_root))
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
        Keyword.merge(validation_options(c),
          workspace: c.workspace,
          workspace_root: c.workspace_root,
          agent_executor: executor,
          review_transport: SymphonyElixir.ReviewExecutionTest.ReviewTransport
        ),
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

  defp review_workspace_script do
    source = File.read!(Path.expand("../../lib/symphony_elixir/review_workspace.ex", __DIR__))
    [_, script] = Regex.run(~r/@script ~S"""\n(.*?)\n  """/s, source)
    script |> String.split("\n") |> Enum.map(&String.replace_prefix(&1, "  ", "")) |> Enum.join("\n")
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
      validation_scratch: c.validation_scratch,
      evidence_server: c[:evidence_server]
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

defmodule SymphonyElixir.ReviewExecutionTest.ReviewTransport do
  def get_pr(_config, _number) do
    Agent.get_and_update(remote(), fn state ->
      {{:ok, state.pr}, %{state | calls: state.calls ++ [:get_pr]}}
    end)
  end

  def reviews(_config, _number) do
    Agent.get_and_update(remote(), fn state ->
      {{:ok, state.reviews}, %{state | calls: state.calls ++ [:reviews]}}
    end)
  end

  def create_review(_config, _number, payload) do
    {{:ok, review}, on_create} =
      Agent.get_and_update(remote(), fn state ->
        id = state.creates + 1
        state_name = %{"APPROVE" => "APPROVED", "REQUEST_CHANGES" => "CHANGES_REQUESTED", "COMMENT" => "COMMENTED"}[payload["event"]]

        review = %{
          id: id,
          url: "https://github.com/RigCodeAI/rig/pull/241#pullrequestreview-#{id}",
          head_sha: payload["commit_id"],
          state: state_name,
          body: payload["body"],
          author_login: "review-app[bot]",
          author_type: "Bot",
          owned: true
        }

        comments =
          payload["comments"]
          |> Enum.with_index(1)
          |> Enum.map(fn {comment, index} ->
            %{
              id: id * 1_000 + index,
              path: comment["path"],
              line: comment["line"],
              side: comment["side"],
              start_line: comment["start_line"],
              start_side: comment["start_side"],
              body: comment["body"],
              head_sha: payload["commit_id"],
              owned: true
            }
          end)

        next = %{
          state
          | reviews: state.reviews ++ [review],
            comments: Map.put(state.comments, id, comments),
            creates: id,
            calls: state.calls ++ [:create_review],
            last_payload: payload
        }

        {{{:ok, review}, state.on_create}, next}
      end)

    if is_function(on_create, 0), do: on_create.()
    {:ok, review}
  end

  def review_comments(_config, _number, review_id) do
    Agent.get_and_update(remote(), fn state ->
      comments = Map.get(state.comments, review_id, [])

      comments =
        case state.comment_mode do
          :missing -> Enum.drop(comments, -1)
          :wrong -> List.update_at(comments, 0, &Map.put(&1, :body, "unexpected inline comment"))
          _ -> comments
        end

      {{:ok, comments}, %{state | calls: state.calls ++ [:review_comments]}}
    end)
  end

  defp remote, do: :persistent_term.get({SymphonyElixir.ReviewExecutionTest, :review_remote})
end
