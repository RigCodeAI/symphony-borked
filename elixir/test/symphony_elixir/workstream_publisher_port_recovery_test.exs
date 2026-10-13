defmodule SymphonyElixir.WorkstreamPublisherPortRecoveryTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{ControlledReviewQualification, Publisher, Workstream, WorkstreamRun, WorkstreamRunner, WorkstreamStore, WorkstreamPublisherPortRecovery}

  @issue_id "linear-issue-259"
  @activity_id "retry-publish-activity-259"
  @run_id "run-259-publisher"
  @failed_attempt_id @run_id <> "/publish/16"
  @app_user_id "linear-app-user-1"
  @oauth_client_id "linear-oauth-client-1"
  @organization_id "linear-org-1"
  @worker_control %{
    "host" => "worker-a",
    "machine_id" => String.duplicate("a", 32),
    "service_revision" => String.duplicate("a", 40),
    "release_sha256" => String.duplicate("b", 64),
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

  setup do
    root = Path.join(System.tmp_dir!(), "publisher-port-recovery-#{System.unique_integer([:positive])}")
    path = Path.join(root, "private/state.sqlite3")
    File.mkdir_p!(Path.dirname(path))

    config = %{
      "organization_id" => @organization_id,
      "team_id" => "linear-dev-team",
      "app_user_id" => @app_user_id,
      "oauth_client_id" => @oauth_client_id,
      "webhook_secret_env" => "LINEAR_WEBHOOK_SECRET",
      "client_secret_env" => "LINEAR_CLIENT_SECRET",
      "store_path" => path,
      "workspace_root" => Path.join(root, "workspaces"),
      "workstream_path" => Path.join(root, "software-change.yaml"),
      "agent_id" => "default-cloud",
      "rig_label" => "factory:rig",
      "worker_control" => @worker_control
    }

    File.mkdir_p!(config["workspace_root"])
    {definition, qualification_path} = workstream_definitions(root)
    config = Map.put(config, "workstream_path", Path.join(root, "factory/workstreams/software-change.yaml"))

    context = %{
      root: root,
      path: path,
      config: config,
      qualification_path: qualification_path,
      run: blocked_run(config, definition),
      task: task_record(),
      record: rejected_record()
    }

    seed(context)

    on_exit(fn -> File.rm_rf!(root) end)
    context
  end

  test "plan is read-only; apply atomically creates a pinned same-task successor and replays after restart", c do
    probes = probes(c)
    request = request(c, :plan)

    assert {:ok, %{status: :planned, stage: "local-commit"} = plan} =
             WorkstreamPublisherPortRecovery.run_with_probes(request, probes)

    assert plan.old_run_id == @run_id
    assert {:ok, before} = fetch(c.path, @run_id)
    assert before == c.run
    assert :not_found = fetch(c.path, plan.successor_run_id)
    assert :not_found = event(c.path, event_id(@activity_id, "origin"))

    assert {:ok, %{status: :queued, successor_run_id: successor_id}} =
             WorkstreamPublisherPortRecovery.run_with_probes(Keyword.put(request, :mode, :apply), probes)

    assert {:ok, old_after} = fetch(c.path, @run_id)
    assert old_after.status == :stopped
    assert old_after.phase == :stopped
    assert old_after.stage_id == :blocked
    assert old_after.policy == @old_policy
    assert old_after.attempts == c.run.attempts
    assert old_after.operations == c.run.operations
    assert old_after.recovery_successor_run_id == successor_id

    assert {:ok, successor} = fetch(c.path, successor_id)
    assert successor.status == :ready
    assert successor.phase == :queued
    assert successor.stage_id == "local-commit"
    assert successor.task_id == c.run.task_id
    assert successor.issue_id == c.run.issue_id
    assert successor.branch == c.run.branch
    assert successor.workspace == c.run.workspace
    assert ControlledReviewQualification.equivalent?(c.run.definition, successor.definition)

    assert successor.definition.stages["review"].prompt ==
             c.run.definition.stages["review"].prompt <> ControlledReviewQualification.instruction()

    assert successor.execution == c.run.execution
    assert successor.policy == WorkstreamRun.policy()
    assert successor.policy != old_after.policy
    assert successor.thread_id == c.run.thread_id
    assert successor.session_id == c.run.session_id
    assert successor.outputs["candidate"] == c.run.outputs["candidate"]
    assert successor.outputs["task"] == c.run.outputs["task"]
    assert successor.attempts == []
    assert successor.operations == %{}
    assert successor.repairs == c.run.repairs
    assert successor.validation_repair_rounds == c.run.validation_repair_rounds
    assert successor.review_repair_rounds == c.run.review_repair_rounds
    assert successor[:recovery_parent_run_id] == @run_id
    refute Map.has_key?(successor, :parent_run_id)

    assert {:ok, %{tasks: tasks}} = linear(c.path)
    assert tasks[@issue_id].run_id == successor_id
    assert tasks[@issue_id].scope_revision == c.task.scope_revision + 1

    assert {:ok, %{run_id: @run_id, event: origin}} = event(c.path, event_id(@activity_id, "origin"))
    assert origin.kind == :publisher_port_recovery_origin_stopped
    assert origin.old_policy == @old_policy
    assert origin.new_policy == WorkstreamRun.policy()
    assert origin.worker_termination_status == "terminated"
    assert String.match?(origin.helper_source_sha256, ~r/\A[0-9a-f]{64}\z/)
    assert origin.controlled_review_provenance.new_source_path == c.qualification_path
    assert String.match?(origin.controlled_review_provenance.new_source_sha256, ~r/\A[0-9a-f]{64}\z/)
    assert String.match?(origin.controlled_review_provenance.old_definition_sha256, ~r/\A[0-9a-f]{64}\z/)
    assert origin.controlled_review_provenance.new_definition_sha256 != origin.controlled_review_provenance.old_definition_sha256
    assert origin.candidate_handoff.parent_run_id == @run_id
    assert origin.candidate_handoff.producer_attempt_id == @run_id <> "/implement/2"
    assert origin.candidate_handoff.candidate_output_id == c.run.artifacts["candidate"]
    assert String.match?(origin.candidate_handoff.candidate_output_sha256, ~r/\A[0-9a-f]{64}\z/)

    assert {:ok, %{run_id: ^successor_id, event: successor_event}} = event(c.path, event_id(@activity_id, "successor"))
    assert successor_event.kind == :publisher_port_recovery_successor_queued

    restarted_probes = %{linear_scope: fn _, _ -> {:error, :stale} end}

    assert {:ok, %{status: :already_applied, successor_run_id: ^successor_id}} =
             WorkstreamPublisherPortRecovery.run_with_probes(Keyword.put(request, :mode, :apply), restarted_probes)

    {:ok, store} = start_store(c.path)
    advanced = Map.merge(successor, %{stage_id: "validate", phase: :queued})
    assert :ok = WorkstreamStore.commit(store, advanced, "successor-advanced", %{kind: :fixture_advance})
    assert :ok = GenServer.stop(store)

    assert {:ok, %{status: :already_applied, successor_run_id: ^successor_id, stage: "validate"}} =
             WorkstreamPublisherPortRecovery.run_with_probes(Keyword.put(request, :mode, :plan), restarted_probes)
  end

  test "stale Linear scope, live or unknown worker, and an existing GitHub branch fail without writes", c do
    invalid_probes = [
      put_in(probes(c), [:linear_scope], fn _, _ -> {:ok, %{assignee_id: "someone-else"}} end),
      put_in(probes(c), [:worker_status], fn _, identity -> {:ok, %{"status" => "running", "operation_id" => identity["operation_id"], "identity" => identity}} end),
      put_in(probes(c), [:worker_status], fn _, _ -> :unknown end),
      put_in(probes(c), [:remote], fn _, branch -> {:ok, %{branch_sha: String.duplicate("c", 40), pull_request: nil, branch: branch}} end)
    ]

    for invalid <- invalid_probes do
      assert {:error, _reason} = WorkstreamPublisherPortRecovery.run_with_probes(request(c, :apply), invalid)
      assert {:ok, unchanged} = fetch(c.path, @run_id)
      assert unchanged == c.run
      assert :not_found = event(c.path, event_id(@activity_id, "origin"))
      assert {:ok, %{tasks: tasks}} = linear(c.path)
      assert tasks[@issue_id].run_id == @run_id
    end
  end

  test "changed policy, malformed retry activity, and stopped delegation fail closed", c do
    {:ok, store} = start_store(c.path)
    wrong_policy = %{c.run | policy: %{version: 999}}
    assert :ok = WorkstreamStore.commit(store, wrong_policy, "wrong-policy-fixture", %{kind: :fixture})
    assert :ok = GenServer.stop(store)

    assert {:error, :publisher_port_recovery_run_not_eligible} =
             WorkstreamPublisherPortRecovery.run_with_probes(request(c, :plan), probes(c))

    replace_run(c, c.run)
    wrong_record = put_in(c.record, [:event, :body], "retry another-run another-attempt")
    replace_record(c, wrong_record)

    assert {:error, :publisher_port_recovery_not_authorized} =
             WorkstreamPublisherPortRecovery.run_with_probes(request(c, :plan), probes(c))

    replace_record(c, c.record)
    {:ok, store} = start_store(c.path)
    stopped_task = Map.put(c.task, :status, :stopped)
    assert :ok = WorkstreamStore.linear_task(store, stopped_task)
    assert :ok = GenServer.stop(store)

    assert {:error, :publisher_port_recovery_not_authorized} =
             WorkstreamPublisherPortRecovery.run_with_probes(request(c, :plan), probes(c))
  end

  test "an unknown earlier publication operation blocks recovery", c do
    operation_id = @run_id <> "/publish/10"
    unknown_run = put_in(c.run, [:operations, operation_id, :status], :unknown)
    replace_run(c, unknown_run)

    assert {:error, :publisher_port_recovery_run_not_eligible} =
             WorkstreamPublisherPortRecovery.run_with_probes(request(c, :plan), probes(c))

    assert {:ok, unchanged} = fetch(c.path, @run_id)
    assert unchanged == unknown_run
    assert :not_found = event(c.path, event_id(@activity_id, "origin"))
    assert {:ok, %{tasks: tasks}} = linear(c.path)
    assert tasks[@issue_id].run_id == @run_id
  end

  test "an unknown archive wait reason blocks recovery", c do
    operation_id = @run_id <> "/publish/10"
    unknown_reason_run = put_in(c.run, [:operations, operation_id, :reconciliation], ":unrecognized_archive_wait")
    replace_run(c, unknown_reason_run)

    assert {:error, :publisher_port_recovery_run_not_eligible} =
             WorkstreamPublisherPortRecovery.run_with_probes(request(c, :plan), probes(c))

    assert {:ok, unchanged} = fetch(c.path, @run_id)
    assert unchanged == unknown_reason_run
    assert :not_found = event(c.path, event_id(@activity_id, "origin"))
    assert {:ok, %{tasks: tasks}} = linear(c.path)
    assert tasks[@issue_id].run_id == @run_id
  end

  test "active coordinator ownership prevents the maintenance helper from opening the store", c do
    {:ok, store} = start_store(c.path)
    bytes = File.read!(c.path)

    assert {:error, :publisher_port_recovery_store_owned} =
             WorkstreamPublisherPortRecovery.run_with_probes(request(c, :apply), probes(c))

    assert {:ok, unchanged} = WorkstreamStore.fetch(store, @run_id)
    assert unchanged == c.run
    assert File.read!(c.path) == bytes
    assert :ok = GenServer.stop(store)
  end

  test "successor producer reuses the old candidate marker only when candidate input is unchanged", c do
    workspace = c.run.workspace
    File.mkdir_p!(workspace)
    {_, 0} = System.cmd("git", ["init", "--initial-branch=cycle/dev259-publisher", workspace], stderr_to_stdout: true)
    {_, 0} = System.cmd("git", ["-C", workspace, "config", "user.name", "Factory Test"], stderr_to_stdout: true)
    {_, 0} = System.cmd("git", ["-C", workspace, "config", "user.email", "factory-test@example.invalid"], stderr_to_stdout: true)
    File.write!(Path.join(workspace, "candidate.txt"), "candidate source\n")
    {_, 0} = System.cmd("git", ["-C", workspace, "add", "candidate.txt"], stderr_to_stdout: true)
    {_, 0} = System.cmd("git", ["-C", workspace, "commit", "-m", "base"], stderr_to_stdout: true)

    candidate_digest = python_candidate_digest(c.run.outputs["candidate"], c.root)
    marker = "Factory-Candidate: " <> candidate_digest
    {_, 0} = System.cmd("git", ["-C", workspace, "commit", "--allow-empty", "-m", "Prepare local factory candidate", "-m", marker], stderr_to_stdout: true)
    {head_before, 0} = System.cmd("git", ["-C", workspace, "rev-parse", "HEAD"], stderr_to_stdout: true)

    assert {:ok, definition, _provenance} =
             ControlledReviewQualification.successor_definition(c.run.definition, c.qualification_path)

    state = %{
      id: "local",
      task_id: c.run.task_id,
      workspace: workspace,
      outputs: %{"candidate" => c.run.outputs["candidate"]},
      execution: %{candidate_producer_stage: "local-commit"},
      current_attempt_id: nil
    }

    assert {:ok, %{exit_status: 0, output: output}} =
             WorkstreamRunner.execute_stage("local-commit", definition, state, workspace_root: c.config["workspace_root"])

    assert {:ok, result} = Jason.decode(output)
    assert result["candidate_commit"] == String.trim(head_before)
    assert result["candidate_input_sha256"] == candidate_digest

    changed_state = put_in(state.outputs["candidate"]["turn_id"], "changed-turn")

    assert {:ok, %{exit_status: exit_status}} =
             WorkstreamRunner.execute_stage("local-commit", definition, changed_state, workspace_root: c.config["workspace_root"])

    assert exit_status != 0
    {head_after, 0} = System.cmd("git", ["-C", workspace, "rev-parse", "HEAD"], stderr_to_stdout: true)
    assert String.trim(head_after) == String.trim(head_before)
  end

  defp seed(c) do
    {:ok, store} = start_store(c.path)
    assert :ok = WorkstreamStore.commit(store, c.run, "initial-run", %{kind: :fixture_run})
    assert :ok = WorkstreamStore.linear_receive(store, c.record.id, c.record.event)
    assert :ok = WorkstreamStore.linear_commit(store, c.record, c.task)
    assert :ok = GenServer.stop(store)
  end

  defp blocked_run(config, definition) do
    execution = %{
      worker_control: @worker_control,
      candidate_producer_stage: "local-commit",
      candidate_producer_command_sha256: digest(Jason.encode!(definition.stages["local-commit"].gate.command)),
      publisher: %{app_id: 1, installation_id: 1, private_key_path: Path.join(config["workspace_root"], "publisher-key.pem")},
      validation_archive: Path.join(config["workspace_root"], "validation-archive"),
      validation_scratch: Path.join(config["workspace_root"], "validation-scratch"),
      validation_policy: %{digest: String.duplicate("e", 64), revision: "policy-1", environment: %{}}
    }

    candidate_output = %{
      "model" => "gpt-6-luna",
      "reasoning_effort" => "medium",
      "session_id" => "cloud-session-259",
      "thread_id" => "cloud-thread-259",
      "turn_id" => "cloud-turn-259",
      "validation_base" => String.duplicate("b", 40),
      "workspace" => Path.join(config["workspace_root"], "DEV-259")
    }

    run =
      WorkstreamRun.new("linear/" <> @issue_id, definition, Path.join(config["workspace_root"], "DEV-259"), [issue_id: @issue_id, branch: "cycle/dev259-publisher"], execution)
      |> Map.merge(%{
        id: @run_id,
        policy: @old_policy,
        status: :blocked,
        phase: :blocked,
        stage_id: :blocked,
        thread_id: "cloud-thread-259",
        session_id: "cloud-session-259",
        outputs: %{"task" => "Retry publication failure", "candidate" => candidate_output},
        artifacts: %{
          "candidate" => %{
            run_id: @run_id,
            attempt_id: @run_id <> "/implement/2",
            sha256: digest(:erlang.term_to_binary(candidate_output))
          }
        },
        repairs: %{"validate" => 1},
        validation_repair_rounds: 1,
        review_repair_rounds: 0
      })

    failed_implementation_id = @run_id <> "/implement/1"
    implementation_id = @run_id <> "/implement/2"
    producer_id = @run_id <> "/local-commit/3"
    inspect_id = @run_id <> "/inspect-candidate/4"
    validation_id = @run_id <> "/validate/5"
    candidate_sha = String.duplicate("a", 40)

    intent = %{
      branch: Publisher.branch(run),
      candidate_sha: candidate_sha,
      publication_id: "publication-259",
      validation_attempt_id: validation_id,
      evidence_revision: String.duplicate("f", 64)
    }

    validation_receipt = %{"id" => String.duplicate("1", 64), "manifest_sha256" => String.duplicate("2", 64)}

    interrupted_publications =
      Enum.map(6..15, fn ordinal ->
        id = @run_id <> "/publish/" <> Integer.to_string(ordinal)

        {
          %{id: id, stage: "publish", type: :publish, ordinal: ordinal, status: :interrupted, result: nil, gate: nil, gate_detail: nil, candidate_context_id: nil},
          {id,
           %{
             id: id,
             attempt_id: id,
             stage: "publish",
             type: :publish,
             ordinal: ordinal,
             status: :not_applied,
             reconciliation: ":validation_not_permanently_archived",
             transport_retries: ordinal - 5
           }}
        }
      end)

    attempts =
      [
        %{
          id: failed_implementation_id,
          stage: "implement",
          type: :agent,
          ordinal: 1,
          status: :completed,
          result: %{status: :error, reason: "agent_execution_failed"},
          gate: nil,
          gate_detail: nil,
          candidate_context_id: nil
        },
        %{
          id: implementation_id,
          stage: "implement",
          type: :agent,
          ordinal: 2,
          status: :completed,
          result: %{status: :ok, evidence: candidate_output},
          gate: nil,
          gate_detail: nil,
          candidate_context_id: nil
        },
        %{id: producer_id, stage: "local-commit", type: :check, ordinal: 3, status: :completed, result: %{status: :ok, evidence: %{}}, gate: :passed, gate_detail: %{}, candidate_context_id: nil},
        %{id: inspect_id, stage: "inspect-candidate", type: :check, ordinal: 4, status: :completed, result: %{status: :ok, evidence: %{}}, gate: :passed, gate_detail: %{}, candidate_context_id: nil},
        %{
          id: validation_id,
          stage: "validate",
          type: :check,
          ordinal: 5,
          status: :completed,
          result: %{status: :ok, evidence: %{"receipt" => validation_receipt}},
          gate: :passed,
          gate_detail: %{"verdict" => "passed"},
          candidate_context_id: nil
        },
        Enum.map(interrupted_publications, &elem(&1, 0)) ++
          [
            %{
              id: @failed_attempt_id,
              stage: "publish",
              type: :publish,
              ordinal: 16,
              status: :completed,
              result: %{status: :error, reason: "{:publication_completion_not_verified, :publication_completion_not_verified}"},
              gate: :failed,
              gate_detail: %{"verdict" => "failed", "verified" => false},
              candidate_context_id: nil
            }
          ]
      ]
      |> List.flatten()

    operations =
      %{
        failed_implementation_id => %{id: failed_implementation_id, attempt_id: failed_implementation_id, stage: "implement", type: :agent, status: :completed, transport_retries: 0},
        implementation_id => %{
          id: implementation_id,
          attempt_id: implementation_id,
          stage: "implement",
          type: :agent,
          status: :completed,
          transport_retries: 0,
          external_process: worker_identity(implementation_id)
        },
        producer_id => %{id: producer_id, attempt_id: producer_id, stage: "local-commit", type: :check, status: :completed, transport_retries: 0},
        inspect_id => %{id: inspect_id, attempt_id: inspect_id, stage: "inspect-candidate", type: :check, status: :completed, transport_retries: 0},
        validation_id => %{id: validation_id, attempt_id: validation_id, stage: "validate", type: :check, status: :completed, transport_retries: 0},
        @failed_attempt_id => %{id: @failed_attempt_id, attempt_id: @failed_attempt_id, stage: "publish", type: :publish, status: :completed, transport_retries: 0, publication_intent: intent}
      }
      |> Map.merge(Map.new(interrupted_publications, &elem(&1, 1)))

    %{run | attempts: attempts, operations: operations}
  end

  defp workstream_definitions(root) do
    repository_factory = Path.expand("../../../factory", __DIR__)
    factory = Path.join(root, "factory")
    _copied = File.cp_r!(repository_factory, factory)

    original_path = Path.join(factory, "workstreams/software-change.yaml")
    qualification_path = Path.join(factory, "workstreams/qualification/software-change-controlled-review-alpha-v1.yaml")
    input_values = %{"task" => "Retry publication failure"}
    {:ok, definition} = Workstream.load(original_path, input_values)
    original_source = File.read!(original_path)

    qualified_source =
      Enum.reduce(["default-cloud", "high-effort-cloud", "daybreak-cloud", "default-review"], original_source, fn agent, source ->
        String.replace(source, "#{agent}: ../agents/", "#{agent}: ../../agents/")
      end)

    old_prompt = definition.stages["review"].prompt
    qualified_prompt = old_prompt <> ControlledReviewQualification.instruction()
    prompt_line = "    prompt: " <> old_prompt

    prompt_block =
      "    prompt: |-\n" <>
        (qualified_prompt |> String.split("\n") |> Enum.map_join("\n", &("      " <> &1)))

    prompt_matches = :binary.matches(qualified_source, prompt_line)
    if length(prompt_matches) != 1, do: raise("expected exactly one pinned review prompt")
    qualified_source = String.replace(qualified_source, prompt_line, prompt_block)
    File.mkdir_p!(Path.dirname(qualification_path))
    File.write!(qualification_path, qualified_source)

    {definition, qualification_path}
  end

  defp worker_identity(operation_id) do
    unit = "factory-operation-#{Base.encode16(:crypto.hash(:sha256, operation_id), case: :lower)}.service"

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

  defp task_record do
    %{
      issue_id: @issue_id,
      run_id: @run_id,
      status: :acknowledged,
      session_id: "linear-session-259",
      assignee_id: "human-owner-17",
      agent_id: "default-cloud",
      route: %{"organization_id" => @organization_id, "app_user_id" => @app_user_id, "oauth_client_id" => @oauth_client_id, "agent_id" => "default-cloud"},
      event: %{timestamp: 1_800_000_000_000},
      publications: %{},
      scope_revision: 4
    }
  end

  defp rejected_record do
    event = %{
      id: "retry-publish-delivery-259",
      kind: :prompted,
      issue_id: @issue_id,
      session_id: "linear-session-259",
      app_user_id: @app_user_id,
      verified_app_user_id: @app_user_id,
      oauth_client_id: @oauth_client_id,
      organization_id: @organization_id,
      timestamp: 1_800_000_001_000,
      activity_id: @activity_id,
      signal: "continue",
      body: "retry #{@run_id} #{@failed_attempt_id}"
    }

    %{id: event.id, event: event, status: :reply_rejected}
  end

  defp probes(c) do
    candidate_sha = c.run.operations[@failed_attempt_id].publication_intent.candidate_sha

    %{
      linear_scope: fn _event, _route -> {:ok, %{assignee_id: c.task.assignee_id}} end,
      qualification_source_path: fn _definition -> {:ok, c.qualification_path} end,
      worker_status: fn _control, identity ->
        {:ok, %{"status" => "terminated", "operation_id" => identity["operation_id"], "identity" => identity, "termination_proof" => %{"cgroup_populated" => 0}}}
      end,
      candidate: fn run -> {:ok, %{workspace: run.workspace, head_sha: candidate_sha, status: ""}} end,
      validation: fn _run, _stage, _attempt, _candidate -> {:ok, %{manifest: %{"candidate_sha" => candidate_sha}, validation_gate: %{verdict: :passed}, publication_gate: %{verdict: :passed}}} end,
      archive: fn _run, validation, intent, _config -> {:ok, %{revision: intent.evidence_revision, validation_attempt_id: validation.attempt.id}} end,
      remote: fn _config, branch -> {:ok, %{branch: branch, branch_sha: nil, pull_request: nil}} end
    }
  end

  defp request(c, mode) do
    [
      mode: mode,
      store_path: c.path,
      linear_config: c.config,
      run_id: @run_id,
      attempt_id: @failed_attempt_id,
      activity_id: @activity_id,
      operator: "adam"
    ]
  end

  defp replace_run(c, run) do
    {:ok, store} = start_store(c.path)
    assert :ok = WorkstreamStore.commit(store, run, "replace-run-#{System.unique_integer([:positive])}", %{kind: :fixture_replace})
    assert :ok = GenServer.stop(store)
  end

  defp replace_record(c, record) do
    :ok = File.rm(c.path)
    seed(%{c | record: record})
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

  defp event_id(activity_id, suffix), do: "publisher-port-recovery/" <> (:crypto.hash(:sha256, activity_id) |> Base.encode16(case: :lower)) <> "/" <> suffix
  defp digest(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)

  defp python_candidate_digest(candidate, root) do
    path = Path.join(root, "candidate-input.json")
    File.write!(path, Jason.encode!(%{"candidate" => candidate}))
    code = "import hashlib,json,sys; v=json.load(open(sys.argv[1]))['candidate']; print(hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',', ':')).encode()).hexdigest())"
    {digest, 0} = System.cmd("python3", ["-c", code, path], stderr_to_stdout: true)
    String.trim(digest)
  end
end
