defmodule SymphonyElixir.WorkstreamPublisherTemplateRecoveryTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{
    ControlledReviewQualification,
    Workstream,
    WorkstreamPublisherTemplateRecovery,
    WorkstreamRun,
    WorkstreamStore
  }

  @ancestor_id "run-800b6eb2fa6aae291689e3a3fcc37e18"
  @run_id "run-9b54a568bc22f61fca60a2b9327e44f8"
  @attempt_id @run_id <> "/publish/16"
  @issue_id "f3cf6638-d7a1-42e9-871f-90969bfe3289"
  @activity_id "template-recovery-contract-activity"
  @stopped_activity_id "cbd5ce6f-2235-4077-b1ac-96711aa87346"
  @created_stop_id "85023415-16a1-4784-affa-34957aff4604"
  @created_stop_timestamp 1_791_840_142_408
  @prior_retry_timestamp 1_791_859_697_194
  @stopped_retry_timestamp 1_791_867_806_468
  @candidate_sha "97219444e808b3bb55dc6717ddcb29cdeb687c04"
  @branch "cycle/factory-e440f747273786859d453798"
  @base_sha "fbb1819a1098a4a740d07e0b89e607e2866a518d"
  @candidate_input_sha256 "115801d20b7de99fcaf4780a88645dcf92aef5b9f72815141478e9339382c112"
  @app_user_id "linear-app-user-259"
  @oauth_client_id "linear-oauth-client-259"
  @organization_id "linear-org-259"
  @worker_control %{
    "host" => "worker-a",
    "machine_id" => String.duplicate("a", 32),
    "service_revision" => String.duplicate("b", 40),
    "release_sha256" => String.duplicate("c", 64),
    "argv" => ["/opt/rig/bin/codex", "app-server"]
  }
  @old_policy %{
    lifecycle_sha256: "96b3f754fae20657a74993320094ab5213735a84157f6022915099babc671545",
    executor_sha256: "ec8ae546aeb408243529f96a1a7048c92d4a61bbe268eee6decbccfb169f4c22",
    child_sha256: "a598793bd67ae523b441445143c548b0ed28e9572c471196f3f7291e20b465d5",
    validation_sha256: "47a77cb57082970ce8f6ca5515b327fc49c31a68a19482fff07893d93efc9cbf",
    gate_policy: "exit-status-v1",
    version: 1
  }
  @parent_policy Map.put(@old_policy, :executor_sha256, "30c942265ee1c5b9e40f6ff36ee6dab2bbdb11b398c11bf11aae2ae27f5177c1")

  setup do
    root = Path.join(System.tmp_dir!(), "publisher-template-recovery-#{System.unique_integer([:positive])}")
    path = Path.join(root, "private/state.sqlite3")
    File.mkdir_p!(Path.dirname(path))

    config = %{
      "organization_id" => "linear-org",
      "team_id" => "linear-dev-team",
      "app_user_id" => "linear-app-user",
      "oauth_client_id" => "linear-oauth-client",
      "webhook_secret_env" => "LINEAR_WEBHOOK_SECRET",
      "client_secret_env" => "LINEAR_CLIENT_SECRET",
      "store_path" => path,
      "workspace_root" => Path.join(root, "workspaces"),
      "workstream_path" => Path.join(root, "software-change.yaml"),
      "agent_id" => "default-cloud",
      "rig_label" => "factory:rig"
    }

    File.mkdir_p!(config["workspace_root"])
    {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    :ok = GenServer.stop(store)

    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root, path: path, config: config}
  end

  test "rejects any request identity or policy override outside the qualified publication", c do
    assert {:error, :invalid_publisher_template_recovery_identity} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(
               request(c) |> Keyword.put(:run_id, "another-run"),
               %{}
             )

    assert {:error, :invalid_publisher_template_recovery_identity} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(
               request(c) |> Keyword.put(:attempt_id, @run_id <> "/publish/15"),
               %{}
             )

    assert {:error, :invalid_publisher_template_recovery_request} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(
               request(c) |> Keyword.put(:policy, %{version: 999}),
               %{}
             )
  end

  test "planning against an empty store is read-only and does not synthesize a run", c do
    before = File.read!(c.path)

    assert {:error, :publisher_template_recovery_run_not_found} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(request(c), %{})

    assert File.read!(c.path) == before

    {:ok, store} = WorkstreamStore.start_link(path: c.path, owner: self())
    assert {:ok, []} = WorkstreamStore.load(store)
    assert {:ok, %{events: events, tasks: tasks}} = WorkstreamStore.linear_load(store)
    assert events == %{}
    assert tasks == %{}
    :ok = GenServer.stop(store)
  end

  test "does not open or mutate a store owned by the running coordinator", c do
    {:ok, store} = WorkstreamStore.start_link(path: c.path, owner: self())
    before = File.read!(c.path)

    assert {:error, :publisher_template_recovery_store_owned} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(request(c), %{})

    assert File.read!(c.path) == before
    assert {:ok, []} = WorkstreamStore.load(store)
    :ok = GenServer.stop(store)
  end

  test "the store rejects an unproven successor transaction without changing history", c do
    {:ok, store} = WorkstreamStore.start_link(path: c.path, owner: self())
    before = File.read!(c.path)

    assert {:error, :invalid_publisher_template_recovery} =
             WorkstreamStore.commit_publisher_template_recovery_successor(store, %{})

    assert File.read!(c.path) == before
    assert {:ok, []} = WorkstreamStore.load(store)
    :ok = GenServer.stop(store)
  end

  test "applies one same-task successor and replays it after restart without duplicating history", c do
    c = eligible_context(c)
    probes = eligible_probes(c)
    request = eligible_request(c, :plan)

    assert {:ok, %{status: :planned, stage: "local-commit"} = plan} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(request, probes)

    assert plan.old_run_id == @run_id
    assert {:ok, unchanged_ancestor} = fetch(c.path, @ancestor_id)
    assert unchanged_ancestor == c.ancestor
    assert {:ok, unchanged_run} = fetch(c.path, @run_id)
    assert unchanged_run == c.run
    assert :not_found = event(c.path, template_event_id(@activity_id, "origin"))

    assert {:ok, %{status: :queued, successor_run_id: successor_id}} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(Keyword.put(request, :mode, :apply), probes)

    assert {:ok, stopped_run} = fetch(c.path, @run_id)
    assert stopped_run == Map.merge(c.run, %{status: :stopped, phase: :stopped, recovery_successor_run_id: successor_id})
    assert stopped_run.attempts == c.run.attempts
    assert stopped_run.operations == c.run.operations
    assert stopped_run.policy == c.run.policy
    assert stopped_run.definition == c.run.definition
    assert stopped_run.outputs == c.run.outputs
    assert stopped_run.artifacts == c.run.artifacts

    assert {:ok, successor} = fetch(c.path, successor_id)
    assert successor.status == :ready
    assert successor.phase == :queued
    assert successor.stage_id == "local-commit"
    assert successor.task_id == c.run.task_id
    assert successor.issue_id == c.run.issue_id
    assert successor.workspace == c.run.workspace
    assert successor.branch == c.run.branch
    assert successor.thread_id == c.run.thread_id
    assert successor.session_id == c.run.session_id
    assert successor.policy == WorkstreamRun.policy()
    assert successor.definition == c.run.definition
    assert successor.execution == c.run.execution
    assert successor.outputs == Map.put(c.run.definition.input_values, "candidate", c.run.outputs["candidate"])

    assert Map.take(successor.outputs, successor.definition.stages["local-commit"].inputs) == %{
             "candidate" => c.run.outputs["candidate"]
           }

    assert successor.attempts == []
    assert successor.operations == %{}
    assert successor.repairs == c.run.repairs
    assert successor.validation_repair_rounds == c.run.validation_repair_rounds
    assert successor.review_repair_rounds == c.run.review_repair_rounds
    assert successor.transport_retries == c.run.transport_retries
    assert successor.recovery_parent_run_id == @run_id
    refute Map.has_key?(successor, :parent_run_id)

    assert {:ok, %{tasks: tasks}} = linear(c.path)
    assert tasks[@issue_id].run_id == successor_id
    assert tasks[@issue_id].scope_revision == c.task.scope_revision + 1

    assert {:ok, %{run_id: @run_id, event: origin}} = event(c.path, template_event_id(@activity_id, "origin"))
    assert origin.kind == :publisher_template_recovery_origin_stopped
    assert origin.old_run_id == @run_id
    assert origin.ancestor_run_id == @ancestor_id
    assert origin.successor_run_id == successor_id
    assert origin.proof.old_policy == @parent_policy
    assert origin.proof.new_policy == WorkstreamRun.policy()
    assert origin.proof.candidate_sha == @candidate_sha
    assert origin.proof.remote_head_sha == @candidate_sha
    assert origin.proof.pull_request_absent
    assert origin.proof.template_absent_at_base

    assert {:ok, %{run_id: ^successor_id, event: successor_event}} =
             event(c.path, template_event_id(@activity_id, "successor"))

    assert successor_event.kind == :publisher_template_recovery_successor_queued

    {:ok, store} = start_store(c.path)
    advanced = Map.merge(successor, %{stage_id: "inspect-candidate", phase: :queued})
    assert :ok = WorkstreamStore.commit(store, advanced, "successor-advanced", %{kind: :fixture_advance})
    assert :ok = GenServer.stop(store)

    assert {:ok, %{status: :already_applied, successor_run_id: ^successor_id, stage: "inspect-candidate"}} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(Keyword.put(request, :mode, :plan), %{})

    assert {:ok, %{status: :already_applied, successor_run_id: ^successor_id}} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(Keyword.put(request, :mode, :apply), %{})

    assert {:ok, runs} = load_runs(c.path)
    assert Enum.count(runs, &(&1.task_id == c.run.task_id)) == 3
  end

  test "a fresh retry can recover the exact original-created-event stop without clearing old history", c do
    c = eligible_context(c, stopped: true, activity_id: @stopped_activity_id, retry_timestamp: @stopped_retry_timestamp)
    probes = eligible_probes(c)
    request = eligible_request(c, :plan)
    before = File.read!(c.path)

    assert {:ok, %{status: :planned, stage: "local-commit"}} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(request, probes)

    assert File.read!(c.path) == before

    assert {:ok, %{status: :queued, successor_run_id: successor_id}} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(Keyword.put(request, :mode, :apply), probes)

    assert {:ok, stopped_run} = fetch(c.path, @run_id)

    assert stopped_run ==
             Map.merge(c.run, %{status: :stopped, phase: :stopped, recovery_successor_run_id: successor_id})

    assert stopped_run.attempts == c.run.attempts
    assert stopped_run.operations == c.run.operations
    assert stopped_run.policy == c.run.policy
    assert stopped_run.recovery_successor_run_id == successor_id

    assert {:ok, successor} = fetch(c.path, successor_id)
    assert successor.outputs["candidate"] == c.run.outputs["candidate"]

    assert Map.take(successor.outputs, successor.definition.stages["local-commit"].inputs) == %{
             "candidate" => c.run.outputs["candidate"]
           }

    assert {:ok, %{tasks: tasks}} = linear(c.path)
    successor_task = tasks[@issue_id]
    assert successor_task.run_id == successor_id
    assert successor_task.status == :acknowledged
    assert successor_task.scope_revision == 4
    assert is_nil(successor_task.stop_event_id)
    assert is_nil(successor_task.stop_timestamp)

    assert {:ok, %{events: events}} = linear(c.path)
    assert events[@created_stop_id].status == :handled
    assert events[@created_stop_id].event == c.task.event

    assert {:ok, %{run_id: @run_id, event: origin}} = event(c.path, template_event_id(@stopped_activity_id, "origin"))
    assert origin.proof.service_stop_provenance.event_id == @created_stop_id
    assert origin.proof.service_stop_provenance.timestamp == @created_stop_timestamp
    assert origin.proof.service_stop_provenance.scope_revision == 3

    assert {:ok, %{run_id: ^successor_id}} = event(c.path, template_event_id(@stopped_activity_id, "successor"))
  end

  test "a native stop or changed original stop receipt blocks stopped-origin recovery", c do
    c = eligible_context(c, stopped: true, activity_id: @stopped_activity_id, retry_timestamp: @stopped_retry_timestamp)
    changed_event = Map.put(c.task.event, :timestamp, c.task.stop_timestamp + 1)
    changed = c.task |> Map.put(:event, changed_event) |> Map.put(:stop_timestamp, changed_event.timestamp)
    {:ok, store} = start_store(c.path)
    assert :ok = WorkstreamStore.linear_task(store, changed)
    assert :ok = GenServer.stop(store)

    assert {:error, :publisher_template_recovery_not_authorized} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(
               eligible_request(c, :plan),
               eligible_probes(c)
             )

    assert :not_found = event(c.path, template_event_id(@stopped_activity_id, "origin"))

    c = eligible_context(c, stopped: true, activity_id: @stopped_activity_id, retry_timestamp: @stopped_retry_timestamp)
    persist_lifecycle_event(c, %{id: "native-stop-for-template-test", kind: :stop, reason: :stop_signal, issue_id: @issue_id, session_id: c.task.session_id, timestamp: @stopped_retry_timestamp + 1})

    assert {:error, :publisher_template_recovery_not_authorized} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(
               eligible_request(c, :plan),
               eligible_probes(c)
             )

    assert :not_found = event(c.path, template_event_id(@stopped_activity_id, "origin"))
    assert {:ok, unchanged} = fetch(c.path, @run_id)
    assert unchanged == c.run
  end

  test "rejects an unrelated same-task run or a conflicting activity event without writes", c do
    c = eligible_context(c, activity_id: "unrelated-history-activity")

    unrelated =
      WorkstreamRun.new(c.run.task_id, c.run.definition, c.run.workspace, [issue_id: c.run.issue_id, branch: c.run.branch], c.run.execution)
      |> Map.merge(%{id: "run-unrelated", status: :blocked, phase: :blocked, stage_id: :blocked})

    {:ok, store} = start_store(c.path)
    assert :ok = WorkstreamStore.commit(store, unrelated, "unrelated-run-event", %{kind: :fixture})
    assert :ok = GenServer.stop(store)

    assert {:error, :publisher_template_recovery_store_conflict} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(
               eligible_request(c, :apply),
               eligible_probes(c)
             )

    assert {:ok, unchanged_run} = fetch(c.path, @run_id)
    assert unchanged_run == c.run
    assert :not_found = event(c.path, template_event_id("unrelated-history-activity", "origin"))

    c = eligible_context(c, activity_id: "conflicting-event-activity")
    conflict_id = template_event_id("conflicting-event-activity", "origin")
    {:ok, store} = start_store(c.path)
    assert :ok = WorkstreamStore.commit(store, c.ancestor, conflict_id, %{kind: :unrelated_event})
    assert :ok = GenServer.stop(store)

    assert {:error, :publisher_template_recovery_activity_conflict} =
             WorkstreamPublisherTemplateRecovery.run_with_probes(
               eligible_request(c, :apply),
               eligible_probes(c)
             )

    assert {:ok, unchanged_run} = fetch(c.path, @run_id)
    assert unchanged_run == c.run
    assert {:ok, %{run_id: @ancestor_id, event: %{kind: :unrelated_event}}} = event(c.path, conflict_id)
  end

  defp request(c) do
    [
      mode: :plan,
      store_path: c.path,
      linear_config: c.config,
      run_id: @run_id,
      attempt_id: @attempt_id,
      activity_id: "template-recovery-test-activity",
      operator: "test-operator"
    ]
  end

  defp eligible_request(c, mode) do
    [
      mode: mode,
      store_path: c.path,
      linear_config: c.config,
      run_id: @run_id,
      attempt_id: @attempt_id,
      activity_id: c.activity_id,
      operator: "test-operator"
    ]
  end

  defp eligible_context(c, opts \\ []) do
    fixture_root = Path.join(c.root, "eligible-#{System.unique_integer([:positive])}")
    path = Path.join(fixture_root, "private/state.sqlite3")
    workspace_root = Path.join(fixture_root, "workspaces")
    workspace = Path.join(workspace_root, "DEV-259")
    File.mkdir_p!(Path.dirname(path))
    File.mkdir_p!(workspace)

    original_factory = Path.expand("../../../factory", __DIR__)
    factory = Path.join(fixture_root, "factory")
    _copied = File.cp_r!(original_factory, factory)
    original_path = Path.join(factory, "workstreams/software-change.yaml")
    qualification_path = Path.join(factory, "workstreams/qualification/software-change-controlled-review-alpha-v1.yaml")
    task_text = "Repository: RigCodeAI/rig\nDEV-259: publication recovery contract fixture"
    {:ok, old_definition} = Workstream.load(original_path, %{"task" => task_text})
    qualified_source = qualified_source(original_path, old_definition)
    File.mkdir_p!(Path.dirname(qualification_path))
    File.write!(qualification_path, qualified_source)
    {:ok, definition, provenance} = ControlledReviewQualification.successor_definition(old_definition, qualification_path)

    config = %{
      "organization_id" => @organization_id,
      "team_id" => "linear-dev-team",
      "app_user_id" => @app_user_id,
      "oauth_client_id" => @oauth_client_id,
      "webhook_secret_env" => "LINEAR_WEBHOOK_SECRET",
      "client_secret_env" => "LINEAR_CLIENT_SECRET",
      "store_path" => path,
      "workspace_root" => workspace_root,
      "workstream_path" => original_path,
      "agent_id" => "default-cloud",
      "rig_label" => "factory:rig",
      "worker_control" => @worker_control
    }

    candidate = %{
      "model" => "gpt-6-luna",
      "reasoning_effort" => "medium",
      "session_id" => "session-dev259-contract",
      "thread_id" => "thread-dev259-contract",
      "turn_id" => "turn-dev259-contract",
      "validation_base" => @base_sha,
      "workspace" => workspace
    }

    execution = %{
      worker_control: @worker_control,
      candidate_producer_stage: "local-commit",
      candidate_producer_command_sha256: digest(Jason.encode!(old_definition.stages["local-commit"].gate.command)),
      publisher: %{app_id: 1, installation_id: 2},
      validation_archive: Path.join(fixture_root, "validation-archive"),
      validation_scratch: Path.join(fixture_root, "validation-scratch"),
      validation_policy: %{digest: String.duplicate("e", 64), revision: "fixture-policy", environment: %{}}
    }

    stopped? = Keyword.get(opts, :stopped, false)
    ancestor = build_ancestor(old_definition, candidate, execution, workspace)
    run = build_blocked_run(definition, candidate, execution, workspace)
    run = if stopped?, do: Map.merge(run, %{status: :stopped, phase: :stopped}), else: run
    task = task_record(candidate, stopped?: stopped?)
    prior_events = prior_recovery_events(ancestor, run, provenance)
    activity_id = Keyword.get(opts, :activity_id, @activity_id)
    retry_timestamp = Keyword.get(opts, :retry_timestamp, 1_800_000_002_000)
    current_record = retry_record(run, task, activity_id, retry_timestamp)
    prior_retry_record = prior_retry_record(task, if(stopped?, do: @prior_retry_timestamp, else: 1_800_000_001_000))

    seed_eligible(path, ancestor, run, prior_events, task, current_record, prior_retry_record)

    Map.merge(c, %{
      root: fixture_root,
      path: path,
      config: config,
      ancestor: ancestor,
      run: run,
      task: task,
      provenance: provenance,
      current_record: current_record,
      prior_retry_record: prior_retry_record,
      activity_id: activity_id
    })
  end

  defp qualified_source(original_path, old_definition) do
    source = File.read!(original_path)

    source =
      Enum.reduce(["default-cloud", "high-effort-cloud", "daybreak-cloud", "default-review"], source, fn agent, text ->
        String.replace(text, "#{agent}: ../agents/", "#{agent}: ../../agents/")
      end)

    old_prompt = old_definition.stages["review"].prompt
    new_prompt = old_prompt <> ControlledReviewQualification.instruction()
    prompt_line = "    prompt: " <> old_prompt
    prompt_block = "    prompt: |-\n" <> Enum.map_join(String.split(new_prompt, "\n"), "\n", &("      " <> &1))

    if length(:binary.matches(source, prompt_line)) != 1, do: raise("expected one review prompt in pinned source")
    String.replace(source, prompt_line, prompt_block)
  end

  defp build_ancestor(definition, candidate, execution, workspace) do
    ancestor =
      WorkstreamRun.new("linear/" <> @issue_id, definition, workspace, [issue_id: @issue_id, branch: @branch], execution)
      |> Map.merge(%{
        id: @ancestor_id,
        policy: @old_policy,
        status: :ready,
        phase: :queued,
        stage_id: "implement",
        thread_id: candidate["thread_id"],
        session_id: candidate["session_id"],
        repairs: %{"validate" => 1},
        validation_repair_rounds: 1,
        review_repair_rounds: 0,
        review_fix_completions: %{},
        transport_retries: 10
      })

    first = ancestor |> WorkstreamRun.start_stage() |> WorkstreamRun.finish_stage({:error, :agent_execution_failed})

    implementation =
      first
      |> Map.merge(%{status: :ready, phase: :queued, stage_id: "implement", current_attempt_id: nil})
      |> WorkstreamRun.start_stage()
      |> WorkstreamRun.finish_stage({:ok, candidate})

    operation_id = @ancestor_id <> "/implement/2"
    identity = worker_identity(operation_id)

    intent = %{
      branch: @branch,
      candidate_sha: @candidate_sha,
      publication_id: String.duplicate("f", 64),
      validation_attempt_id: @ancestor_id <> "/validate/5",
      evidence_revision: String.duplicate("e", 64)
    }

    implementation
    |> complete_check(%{"candidate_commit" => @candidate_sha})
    |> complete_check(%{"head_sha" => @candidate_sha})
    |> complete_check(%{"receipt" => %{"id" => "ancestor-validation"}})
    |> then(fn run ->
      run =
        Enum.reduce(6..15, run, fn _ordinal, current ->
          current
          |> WorkstreamRun.start_stage()
          |> WorkstreamRun.uncertain(:validation_not_permanently_archived)
          |> WorkstreamRun.retry_transport()
        end)

      failed = WorkstreamRun.start_stage(run)

      failed_attempt =
        Map.merge(List.last(failed.attempts), %{
          status: :completed,
          result: %{status: :error, reason: "publisher failed"},
          gate: :failed
        })

      failed_operation = Map.merge(failed.operations[failed.current_attempt_id], %{status: :completed, publication_intent: intent})

      failed
      |> Map.put(:attempts, List.replace_at(failed.attempts, 15, failed_attempt))
      |> Map.put(:operations, Map.put(failed.operations, failed.current_attempt_id, failed_operation))
    end)
    |> Map.merge(%{
      status: :stopped,
      phase: :stopped,
      stage_id: :blocked,
      current_attempt_id: nil,
      recovery_successor_run_id: @run_id,
      thread_id: candidate["thread_id"],
      session_id: candidate["session_id"]
    })
    |> update_in([:operations, operation_id], &Map.put(&1, :external_process, identity))
  end

  defp complete_check(run, output) do
    stage = run.definition.stages[run.stage_id]
    completed = WorkstreamRun.start_stage(run)
    attempt = List.last(completed.attempts)
    operation = completed.operations[attempt.id]

    completed_attempt =
      Map.merge(attempt, %{
        status: :completed,
        result: %{status: :ok, evidence: output},
        gate: :passed,
        gate_detail: %{"verdict" => "passed", "verified" => true}
      })

    completed_operation = Map.put(operation, :status, :completed)
    output_name = List.first(stage.outputs)

    completed
    |> Map.put(:attempts, List.replace_at(completed.attempts, length(completed.attempts) - 1, completed_attempt))
    |> Map.put(:operations, Map.put(completed.operations, attempt.id, completed_operation))
    |> Map.update!(:outputs, &Map.put(&1, output_name, output))
    |> Map.update!(:artifacts, &Map.put(&1, output_name, artifact(completed.id, attempt.id, output)))
    |> Map.merge(%{status: :ready, phase: :queued, stage_id: stage.gate.success, current_attempt_id: nil})
  end

  defp build_blocked_run(definition, candidate, execution, workspace) do
    run =
      WorkstreamRun.new("linear/" <> @issue_id, definition, workspace, [issue_id: @issue_id, branch: @branch], execution)
      |> Map.merge(%{
        id: @run_id,
        policy: @parent_policy,
        status: :ready,
        phase: :queued,
        stage_id: "local-commit",
        recovery_parent_run_id: @ancestor_id,
        thread_id: candidate["thread_id"],
        session_id: candidate["session_id"],
        outputs: Map.put(definition.input_values, "candidate", candidate),
        repairs: %{"validate" => 1},
        validation_repair_rounds: 1,
        review_repair_rounds: 0,
        review_fix_completions: %{},
        transport_retries: 12
      })

    producer_json = %{
      "base_commit" => @base_sha,
      "branch" => "refs/heads/" <> @branch,
      "bundle_bytes" => 1488,
      "bundle_sha256" => String.duplicate("3", 64),
      "candidate_bundle" => ".git/factory-candidates/" <> @candidate_input_sha256 <> ".bundle",
      "candidate_commit" => @candidate_sha,
      "candidate_input_sha256" => @candidate_input_sha256
    }

    producer_output = %{
      "exit_code" => "exited",
      "exit_status" => 0,
      "output" => Jason.encode!(producer_json) <> "\n",
      "timed_out" => false,
      "truncated" => false
    }

    run =
      run
      |> WorkstreamRun.start_stage()
      |> WorkstreamRun.finish_stage({:ok, %{exit_status: 0, timed_out: false, output: producer_output["output"]}})
      |> WorkstreamRun.start_stage()
      |> WorkstreamRun.finish_stage({:ok, %{exit_status: 0, timed_out: false, output: ""}})
      |> WorkstreamRun.start_stage()

    validation_id = @run_id <> "/validate/3"
    receipt = %{"id" => String.duplicate("1", 64), "manifest_sha256" => String.duplicate("2", 64)}
    validation_evidence = %{"receipt" => receipt, "feedback" => []}

    validation_attempt =
      List.last(run.attempts)
      |> Map.merge(%{
        status: :completed,
        result: %{status: :ok, evidence: validation_evidence},
        gate: :passed,
        gate_detail: %{"verdict" => "passed", "verified" => true}
      })

    run =
      run
      |> Map.put(:attempts, List.replace_at(run.attempts, 2, validation_attempt))
      |> update_in([:operations, validation_id, :status], fn _ -> :completed end)
      |> Map.update!(:outputs, &Map.put(&1, "validation", validation_evidence))
      |> Map.update!(:artifacts, &Map.put(&1, "validation", artifact(@run_id, validation_id, validation_evidence)))
      |> Map.merge(%{status: :ready, phase: :queued, stage_id: "publish", current_attempt_id: nil})

    run =
      Enum.reduce(4..15, run, fn _ordinal, current ->
        current
        |> WorkstreamRun.start_stage()
        |> WorkstreamRun.uncertain(:validation_not_permanently_archived)
        |> WorkstreamRun.retry_transport()
      end)

    failed = WorkstreamRun.start_stage(run)

    intent = %{
      branch: @branch,
      candidate_sha: @candidate_sha,
      publication_id: String.duplicate("a", 64),
      validation_attempt_id: validation_id,
      evidence_revision: String.duplicate("d", 64)
    }

    attempt_id = @run_id <> "/publish/16"

    failed_attempt =
      List.last(failed.attempts)
      |> Map.merge(%{
        status: :completed,
        result: %{status: :error, reason: "{:publication_completion_not_verified, :publication_completion_not_verified}"},
        gate: :failed,
        gate_detail: %{"verdict" => "failed", "verified" => false}
      })

    failed_operation =
      failed.operations[attempt_id]
      |> Map.merge(%{
        status: :completed,
        publication_intent: intent,
        publication_observation: %{"branch" => @branch, "remote_sha" => @candidate_sha, "pr" => nil}
      })

    failed
    |> Map.put(:attempts, List.replace_at(failed.attempts, 15, failed_attempt))
    |> Map.put(:operations, Map.put(failed.operations, attempt_id, failed_operation))
    |> Map.merge(%{status: :blocked, phase: :blocked, stage_id: :blocked, current_attempt_id: nil})
  end

  defp task_record(candidate, opts) do
    stopped? = Keyword.get(opts, :stopped?, false)

    route = %{
      "organization_id" => @organization_id,
      "app_user_id" => @app_user_id,
      "oauth_client_id" => @oauth_client_id,
      "agent_id" => "default-cloud"
    }

    event = %{
      id: if(stopped?, do: @created_stop_id, else: "assignment-created-259"),
      kind: :created,
      issue_id: @issue_id,
      session_id: candidate["session_id"],
      app_user_id: @app_user_id,
      verified_app_user_id: @app_user_id,
      oauth_client_id: @oauth_client_id,
      organization_id: @organization_id,
      timestamp: if(stopped?, do: @created_stop_timestamp, else: 1_800_000_000_000)
    }

    task = %{
      issue_id: @issue_id,
      run_id: @run_id,
      status: if(stopped?, do: :stopped, else: :acknowledged),
      session_id: candidate["session_id"],
      assignee_id: "human-owner-259",
      app_user_id: @app_user_id,
      agent_id: "default-cloud",
      oauth_client_id: @oauth_client_id,
      route: route,
      event: event,
      publications: %{},
      scope_revision: if(stopped?, do: 3, else: 2),
      assignment_history: [],
      pending_reassignment: nil,
      blocked_reassignment: nil,
      stop_event_id: if(stopped?, do: @created_stop_id, else: nil),
      stop_timestamp: if(stopped?, do: @created_stop_timestamp, else: nil)
    }

    task
  end

  defp retry_record(run, task, activity_id, timestamp) do
    event = %{
      id: "retry-delivery-" <> activity_id,
      kind: :prompted,
      issue_id: task.issue_id,
      session_id: task.session_id,
      app_user_id: @app_user_id,
      verified_app_user_id: @app_user_id,
      oauth_client_id: @oauth_client_id,
      organization_id: @organization_id,
      timestamp: timestamp,
      activity_id: activity_id,
      signal: nil,
      body: "retry #{run.id} #{run.id}/publish/16"
    }

    %{id: event.id, event: event, status: :reply_rejected}
  end

  defp prior_retry_record(task, timestamp) do
    event = %{
      id: "retry-before-template-failure",
      kind: :prompted,
      issue_id: task.issue_id,
      session_id: task.session_id,
      activity_id: "e54c6a27-3f17-421b-9899-1e0f8ce815c4",
      timestamp: timestamp,
      body: "retry #{@ancestor_id} #{@ancestor_id}/publish/16"
    }

    %{id: event.id, event: event, status: :reply_rejected}
  end

  defp prior_recovery_events(ancestor, run, provenance) do
    handoff = %{
      version: 1,
      parent_run_id: ancestor.id,
      producer_attempt_id: ancestor.id <> "/implement/2",
      candidate_output_id: ancestor.artifacts["candidate"],
      candidate_output_sha256: ancestor.artifacts["candidate"].sha256,
      candidate_sha: @candidate_sha
    }

    origin_id = port_event_id("origin")
    successor_id = port_event_id("successor")

    common = %{
      helper_source_sha256: "722704131ba289b4c98e3e6f9ddf77cd3779769df562d6f29ff73bdd7e2db866",
      old_policy: @old_policy,
      new_policy: @parent_policy,
      old_publisher_git_sha256: "8263f004acaf5c3805e5b1196fb4aea6d273eda217e31dda331d8207c8797c94",
      new_publisher_git_sha256: "75ad2bb3821a90d5573578d20200be0416546de3751686db4205dbb65d9188c4",
      candidate_sha: @candidate_sha,
      branch: @branch,
      remote_branch_absent: true,
      remote_pr_absent: true,
      controlled_review_provenance: provenance,
      candidate_handoff: handoff,
      old_run_id: ancestor.id,
      successor_run_id: run.id,
      worker_operation_id: ancestor.id <> "/implement/2"
    }

    origin = Map.merge(common, %{kind: :publisher_port_recovery_origin_stopped, event_id: origin_id})
    successor = Map.merge(common, %{kind: :publisher_port_recovery_successor_queued, event_id: successor_id})
    %{origin_id: origin_id, origin: origin, successor_id: successor_id, successor: successor}
  end

  defp seed_eligible(path, ancestor, run, prior_events, task, current_record, prior_record) do
    created_record = %{id: task.event.id, event: task.event, status: :handled}
    {:ok, store} = start_store(path)
    assert :ok = WorkstreamStore.commit(store, ancestor, prior_events.origin_id, prior_events.origin)
    assert :ok = WorkstreamStore.commit(store, run, prior_events.successor_id, prior_events.successor)
    persist_linear_record(store, created_record, task)
    persist_linear_record(store, prior_record, nil)
    persist_linear_record(store, current_record, nil)
    :ok = GenServer.stop(store)
  end

  defp persist_linear_record(store, record, task) do
    assert :ok = WorkstreamStore.linear_receive(store, record.id, record.event)
    assert :ok = WorkstreamStore.linear_commit(store, record, task)
  end

  defp persist_lifecycle_event(c, event) do
    record = %{id: event.id, event: event, status: :handled}
    {:ok, store} = start_store(c.path)
    persist_linear_record(store, record, nil)
    assert :ok = GenServer.stop(store)
  end

  defp eligible_probes(c) do
    candidate_sha = @candidate_sha

    %{
      linear_scope: fn _event, _route -> {:ok, %{assignee_id: c.task.assignee_id}} end,
      worker_status: fn _control, identity ->
        {:ok,
         %{
           "status" => "terminated",
           "operation_id" => identity["operation_id"],
           "identity" => identity,
           "exit_code" => "exited",
           "exit_status" => 0,
           "termination_proof" => %{"main_pid" => 0, "cgroup_populated" => 0}
         }}
      end,
      candidate: fn run -> {:ok, %{workspace: run.workspace, head_sha: candidate_sha, status: ""}} end,
      validation: fn _run, _stage, _attempt, _candidate ->
        {:ok,
         %{
           manifest: %{"candidate_sha" => candidate_sha},
           validation_gate: %{verdict: :passed},
           publication_gate: %{verdict: :passed}
         }}
      end,
      archive: fn _run, validation, intent, _config ->
        {:ok, %{revision: intent.evidence_revision, validation_attempt_id: validation.attempt.id}}
      end,
      remote: fn _config, branch -> {:ok, %{branch: branch, branch_sha: candidate_sha, pull_request: nil}} end,
      template: fn _workspace, _base_sha -> {:ok, []} end
    }
  end

  defp worker_identity(operation_id) do
    unit = "factory-operation-#{digest(operation_id)}.service"

    %{
      "kind" => "systemd-unit",
      "operation_id" => operation_id,
      "machine_id" => @worker_control["machine_id"],
      "boot_id" => String.duplicate("1", 32),
      "unit" => unit,
      "invocation_id" => String.duplicate("2", 32),
      "control_group" => "/system.slice/" <> unit,
      "request_sha256" => String.duplicate("c", 64)
    }
  end

  defp artifact(run_id, attempt_id, output) do
    %{run_id: run_id, attempt_id: attempt_id, sha256: term_digest(output)}
  end

  defp start_store(path), do: WorkstreamStore.start_link(path: path, owner: self())

  defp fetch(path, run_id) do
    {:ok, store} = start_store(path)
    result = WorkstreamStore.fetch(store, run_id)
    :ok = GenServer.stop(store)
    result
  end

  defp event(path, event_id) do
    {:ok, store} = start_store(path)
    result = WorkstreamStore.event(store, event_id)
    :ok = GenServer.stop(store)
    result
  end

  defp linear(path) do
    {:ok, store} = start_store(path)
    result = WorkstreamStore.linear_load(store)
    :ok = GenServer.stop(store)
    result
  end

  defp load_runs(path) do
    {:ok, store} = start_store(path)
    result = WorkstreamStore.load(store)
    :ok = GenServer.stop(store)
    result
  end

  defp template_event_id(activity_id, suffix),
    do: "publisher-template-recovery/#{digest(activity_id)}/#{suffix}"

  defp port_event_id(suffix),
    do: "publisher-port-recovery/#{digest("e54c6a27-3f17-421b-9899-1e0f8ce815c4")}/#{suffix}"

  defp term_digest(term), do: :crypto.hash(:sha256, :erlang.term_to_binary(term)) |> Base.encode16(case: :lower)
  defp digest(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
