defmodule SymphonyElixir.PublisherTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{Evidence, Publisher, Validation, ValidationPolicy, WorkstreamRun, WorkstreamStore}

  # These fixtures sign the current Validation request contract with a local test key.
  # They are synthetic authenticated receipts, not evidence of a real check or GCS upload.
  # Publisher transport calls in this file use an in-process fake and are not live publication.
  test "missing, forged, failed, stale, and dirty validation results cause no remote writes" do
    for failure <- [:missing, :forged, :failed, :stale, :dirty] do
      c = fixture()
      remote = start_remote()
      Process.put(:publisher_test_remote, remote)

      try do
        run = failed_run(c, failure)

        if failure == :failed do
          assert {:error, :unqualified_publication_candidate} = Publisher.publish(run, c.publish_stage)
        else
          assert {:error, _reason} = Publisher.publish(run, c.publish_stage)
        end

        assert %{pushes: 0, creates: 0, updates: 0} = Agent.get(remote, & &1)
      after
        Process.delete(:publisher_test_remote)
        Agent.stop(remote)
      end
    end
  end

  test "a pending evidence upload blocks before branch or pull request mutations" do
    c = fixture(verified_upload: false)
    remote = start_remote()
    Process.put(:publisher_test_remote, remote)

    try do
      assert {:uncertain, :validation_not_permanently_archived} = Publisher.publish(c.run, c.publish_stage)
      assert %{pushes: 0, creates: 0, updates: 0} = Agent.get(remote, & &1)
    after
      Process.delete(:publisher_test_remote)
      Agent.stop(remote)
    end
  end

  test "a changed candidate after validation approval is rejected before branch or pull request writes" do
    c = fixture()
    remote = start_remote(workspace: c.workspace, mutate_on_find: true)
    Process.put(:publisher_test_remote, remote)

    try do
      assert {:error, _reason} = Publisher.publish(c.run, c.publish_stage)

      state = Agent.get(remote, & &1)
      assert state.mutated_candidate
      assert state.pushes == 0
      assert state.creates == 0
      assert state.updates == 0
    after
      Process.delete(:publisher_test_remote)
      Agent.stop(remote)
    end
  end

  test "a lost push response is uncertain and retry creates one PR without pushing again" do
    c = fixture()
    remote = start_remote(fail_after_push: true)
    Process.put(:publisher_test_remote, remote)

    try do
      assert {:uncertain, :publication_outcome_unknown} = Publisher.publish(c.run, c.publish_stage)

      after_lost_response = Agent.get(remote, & &1)
      assert after_lost_response.branch_sha == c.candidate_sha
      assert after_lost_response.pushes == 1
      assert after_lost_response.creates == 0
      assert is_nil(after_lost_response.pull_request)

      assert {:ok, result} = Publisher.publish(c.run, c.publish_stage)
      assert result.candidate_sha == c.candidate_sha
      assert result.branch == Publisher.branch(c.run)
      assert result.pr.number == 41
      assert result.pr.head_sha == c.candidate_sha

      final = Agent.get(remote, & &1)
      assert final.branch_sha == c.candidate_sha
      assert final.pushes == 1
      assert final.creates == 1
      assert final.updates == 0
      assert final.pull_request.number == 41
    after
      Process.delete(:publisher_test_remote)
      Agent.stop(remote)
    end
  end

  test "owner revocation at initial, pre-push, or post-push checks stops later publication writes" do
    for revoke_at <- [1, 2, 3] do
      c = fixture()
      remote = start_remote()
      Process.put(:publisher_test_remote, remote)

      try do
        assert {:error, :publication_owner_revoked} =
                 Publisher.publish(c.run, c.publish_stage, owner_callbacks(remote, revoke_at))

        state = Agent.get(remote, & &1)
        assert state.owner_checks == revoke_at
        assert state.creates == 0
        assert state.updates == 0

        case revoke_at do
          1 ->
            assert state.finds == 0
            assert state.branch_reads == 0
            assert state.pushes == 0
            assert is_nil(state.publication_intent)

          2 ->
            assert state.finds == 1
            assert state.branch_reads == 1
            assert state.pushes == 0
            assert is_nil(state.publication_intent)

          3 ->
            assert state.pushes == 1
            assert is_map(state.publication_intent)
            assert state.intent_at_push == state.publication_intent
            assert state.publication_intent.candidate_sha == c.candidate_sha
            assert state.publication_intent.branch == Publisher.branch(c.run)
        end
      after
        Process.delete(:publisher_test_remote)
        Agent.stop(remote)
      end
    end
  end

  test "stopped publication observes an ambiguously created PR without another write" do
    c = fixture()
    publish_run = %{c.run | status: :ready, phase: :queued} |> WorkstreamRun.start_stage()
    assert {:ok, proof} = Publisher.eligible(publish_run, c.publish_stage)
    intent = Publisher.intent(publish_run, c.publish_stage, proof)
    remote = start_remote(fail_after_create: true)
    Process.put(:publisher_test_remote, remote)

    try do
      assert {:uncertain, :publication_outcome_unknown} =
               Publisher.publish(publish_run, c.publish_stage,
                 on_publication_intent: fn saved_intent ->
                   Agent.update(remote, &%{&1 | publication_intent: saved_intent})
                   :ok
                 end
               )

      assert Agent.get(remote, & &1).publication_intent == intent
      operation_id = publish_run.current_attempt_id
      operation = Map.put(publish_run.operations[operation_id], :publication_intent, intent)

      stopped_run = %{
        publish_run
        | status: :stopped,
          phase: :stopped,
          operations: Map.put(publish_run.operations, operation_id, operation)
      }

      before_observation = Agent.get(remote, & &1)
      assert {:ok, observation} = Publisher.observe_stopped(stopped_run, operation)
      after_observation = Agent.get(remote, & &1)

      assert observation.status == :observed
      assert observation.branch == intent.branch
      assert observation.candidate_sha == c.candidate_sha
      assert observation.remote_sha == c.candidate_sha
      assert observation.publication_id == intent.publication_id
      assert observation.pr.number == 41
      refute Map.has_key?(observation.pr, :body)

      assert after_observation.pushes == before_observation.pushes
      assert after_observation.creates == before_observation.creates
      assert after_observation.updates == before_observation.updates
      assert after_observation.pushes == 1
      assert after_observation.creates == 1
      assert after_observation.updates == 0
      assert after_observation.finds == before_observation.finds + 1
      assert after_observation.branch_reads == before_observation.branch_reads + 1
    after
      Process.delete(:publisher_test_remote)
      Agent.stop(remote)
    end
  end

  test "an ambiguous create resumes from the durable publish-stage run and keeps one branch and pull request" do
    c = fixture()
    remote = start_remote(fail_after_create: true)
    Process.put(:publisher_test_remote, remote)

    try do
      database = Path.join(c.root, "workstreams.sqlite3")
      publish_run = %{c.run | status: :ready, phase: :queued} |> WorkstreamRun.start_stage()
      assert publish_run.status == :executing
      assert publish_run.stage_id == "publish"
      assert publish_run.current_attempt_id == "run-publisher-fixture/publish/2"

      with_store(database, fn store ->
        assert :ok = WorkstreamStore.commit(store, publish_run, "publish-stage-started", %{kind: :stage_started})
        assert {:ok, saved_run} = WorkstreamStore.fetch(store, publish_run.id)
        assert saved_run == publish_run
        assert saved_run.execution.evidence_server == c.evidence_server

        # The fake service creates the PR but drops its response, matching an ambiguous remote outcome.
        assert {:uncertain, :publication_outcome_unknown} = Publisher.publish(saved_run, c.publish_stage)
      end)

      after_timeout = Agent.get(remote, & &1)
      assert after_timeout.pushes == 1
      assert after_timeout.creates == 1
      assert after_timeout.pull_request.number == 41

      completed =
        with_store(database, fn store ->
          assert {:ok, restored_run} = WorkstreamStore.fetch(store, publish_run.id)
          assert restored_run.current_attempt_id == publish_run.current_attempt_id
          assert restored_run.execution.evidence_server == c.evidence_server

          human_body = "Human note before\n\n" <> after_timeout.pull_request.body <> "\n\nHuman note after"
          Agent.update(remote, &put_in(&1, [:pull_request, :body], human_body))

          # The remote state survives the process/store restart and the operation identity is deterministic.
          assert {:ok, result} = Publisher.publish(restored_run, c.publish_stage)
          assert result.candidate_sha == c.candidate_sha
          assert result.branch == Publisher.branch(restored_run)
          assert result.pr.number == 41
          assert result.pr.body =~ "Human note before"
          assert result.pr.body =~ "Human note after"
          assert result.pr.body =~ "<!-- factory-evidence:start -->"

          before_replay = Agent.get(remote, & &1).updates
          assert {:ok, replayed} = Publisher.publish(restored_run, c.publish_stage)
          assert replayed.publication_id == result.publication_id
          assert Agent.get(remote, & &1).updates == before_replay

          # finish_stage independently verifies the actual fake branch and PR through Publisher.verify_completion.
          finished = WorkstreamRun.finish_stage(restored_run, {:ok, result})
          assert finished.status == :complete
          assert finished.stage_id == :complete
          publish_attempt = Enum.find(finished.attempts, &(&1.id == restored_run.current_attempt_id))
          assert publish_attempt.gate == :passed
          assert publish_attempt.gate_detail["verified"]

          run_id = publish_run.id
          completion_event = run_id <> "/publish-completed"
          assert :ok = WorkstreamStore.commit(store, finished, completion_event, %{kind: :publish_completed})

          assert {:duplicate, ^run_id} =
                   WorkstreamStore.commit(store, %{finished | status: :blocked}, completion_event, %{kind: :duplicate_completion})

          assert {:ok, persisted} = WorkstreamStore.fetch(store, publish_run.id)
          assert persisted == finished
          finished
        end)

      with_store(database, fn store ->
        assert {:ok, persisted} = WorkstreamStore.fetch(store, publish_run.id)
        assert persisted == completed
        assert persisted.status == :complete
      end)

      final = Agent.get(remote, & &1)
      assert final.pushes == 1
      assert final.creates == 1
      # Human text already sits outside an identical managed section, so replay performs no update.
      assert final.updates == 0
      assert final.pull_request.number == 41
    after
      Process.delete(:publisher_test_remote)
      Agent.stop(remote)
    end
  end

  test "managed body replacement preserves bytes outside its section" do
    body = "Human heading\r\n\r\n<!-- factory-evidence:start -->\r\nold\r\n<!-- factory-evidence:end -->\r\n\r\nHuman footer\r\n"
    assert {:ok, updated} = Publisher.managed_body(body, "new evidence")
    assert updated == "Human heading\r\n\r\n<!-- factory-evidence:start -->\nnew evidence\n<!-- factory-evidence:end -->\r\n\r\nHuman footer\r\n"
  end

  test "completion rejects a remote PR whose evidence section changed after publication" do
    c = fixture()
    remote = start_remote()
    Process.put(:publisher_test_remote, remote)

    try do
      assert {:ok, result} = Publisher.publish(c.run, c.publish_stage)
      before = Agent.get(remote, & &1)
      Agent.update(remote, &put_in(&1, [:pull_request, :body], "Human text without the required evidence table"))
      assert {:error, :publication_evidence_section_changed} = Publisher.verify_completion(c.run, c.publish_stage, {:ok, result})
      after_check = Agent.get(remote, & &1)
      assert after_check.pushes == before.pushes
      assert after_check.creates == before.creates
      assert after_check.updates == before.updates
    after
      Process.delete(:publisher_test_remote)
      Agent.stop(remote)
    end
  end

  defp fixture(opts \\ []) do
    root = Path.join(System.tmp_dir!(), "publisher-contract-#{System.unique_integer([:positive])}")
    seed = Path.join(root, "source")
    workspace = Path.join(root, "managed-clone")
    File.mkdir_p!(seed)
    git!(seed, ["init", "--quiet"])
    git!(seed, ["config", "user.name", "Candidate"])
    git!(seed, ["config", "user.email", "candidate@example.test"])
    File.mkdir_p!(Path.join(seed, ".github"))
    File.write!(Path.join(seed, ".github/pull_request_template.md"), "Review notes\n")
    File.write!(Path.join(seed, "README.md"), "base\n")
    git!(seed, ["add", "."])
    git!(seed, ["commit", "--quiet", "-m", "base"])
    base_sha = git!(seed, ["rev-parse", "HEAD"])

    git!(root, ["clone", "--quiet", "--local", seed, workspace])
    git!(workspace, ["config", "user.name", "Candidate"])
    git!(workspace, ["config", "user.email", "candidate@example.test"])
    File.write!(Path.join(workspace, "README.md"), "candidate\n")
    git!(workspace, ["add", "README.md"])
    git!(workspace, ["commit", "--quiet", "-m", "candidate"])
    candidate_sha = git!(workspace, ["rev-parse", "HEAD"])

    policy_path = Path.join(root, "validation-policy.yaml")
    File.write!(policy_path, policy_yaml())
    assert {:ok, policy} = ValidationPolicy.load(policy_path, workspace)
    validation_archive = Path.join(root, "validation-archive")
    validation_scratch = Path.join(root, "validation-scratch")
    context = %{task_id: "DEV-238", run_id: "run-publisher-fixture", attempt_id: "run-publisher-fixture/validate/1", base_sha: base_sha}
    {receipt, manifest_bytes} = write_signed_contract(workspace, validation_archive, policy, context, base_sha)

    validation_attempt = %{
      id: context.attempt_id,
      stage: "validate",
      type: :check,
      ordinal: 1,
      status: :completed,
      gate: :passed,
      gate_detail: nil,
      result: %{evidence: %{"receipt" => receipt}}
    }

    definition = %{
      name: "publisher-contract-fixture",
      inputs: ["task"],
      input_values: %{"task" => "DEV-238 publication fixture"},
      entry: "validate",
      agents: %{"worker" => %{model: "fixture", reasoning_effort: "medium", daybreak: false}},
      definitions: %{"flow.yaml" => %{sha256: digest("flow-v1"), text: "contract fixture"}},
      stages: %{
        "validate" => %{
          id: "validate",
          type: :check,
          outputs: ["validation"],
          gate: %{
            evaluator: :candidate_validation,
            required: [%{check: "contract", assertion: "exit_status", equals: 0}]
          }
        },
        "publish" => %{
          id: "publish",
          type: :publish,
          inputs: ["validation"],
          outputs: ["publication"],
          gate: %{
            validation_stage: "validate",
            required: [%{check: "contract", assertion: "exit_status", equals: 0}],
            success: :complete,
            failure: :blocked
          }
        }
      }
    }

    evidence_root = Path.join(root, "evidence")

    evidence_config = %{
      "root" => evidence_root,
      "base_url" => "https://evidence.example.test",
      "iap_audience" => "/projects/1/global/backendServices/2",
      "allowed_domains" => ["example.test"],
      "allowed_emails" => []
    }

    {:ok, evidence_server} = Evidence.start_link(config: {:ok, evidence_config}, name: nil, receipt_uid: File.stat!(root).uid)

    on_exit(fn ->
      if Process.alive?(evidence_server), do: GenServer.stop(evidence_server)
      File.rm_rf(root)
    end)

    execution = %{
      validation_policy: policy,
      validation_archive: validation_archive,
      validation_scratch: validation_scratch,
      validation_base: base_sha,
      publisher: %{app_id: 1, installation_id: 2, private_key_path: Path.join(root, "publisher.pem")},
      publisher_transport: SymphonyElixir.PublisherTest.Transport,
      evidence_server: evidence_server,
      evidence_url: "https://evidence.example.test/tasks/fixture"
    }

    run =
      WorkstreamRun.new("DEV-238", definition, workspace, [], execution)
      |> Map.merge(%{
        id: "run-publisher-fixture",
        status: :executing,
        stage_id: "publish",
        current_attempt_id: nil,
        attempts: [validation_attempt]
      })

    evidence_run = %{run | stage_id: "validate", current_attempt_id: validation_attempt.id}
    :ok = Evidence.snapshot(evidence_run, "validation-imported", %{"kind" => "validation_imported", "manifest_sha256" => receipt["manifest_sha256"]}, evidence_server)

    :ok =
      Evidence.artifact(
        %{id: run.id, stage_id: validation_attempt.stage, current_attempt_id: validation_attempt.id},
        "validation/" <> validation_attempt.id <> "/manifest.json",
        manifest_bytes,
        evidence_server
      )

    if Keyword.get(opts, :verified_upload, true) do
      {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id), evidence_server)
      write_upload_receipt(evidence_root, view)
    end

    publish_stage = definition.stages["publish"]

    %{
      root: root,
      workspace: workspace,
      base_sha: base_sha,
      candidate_sha: candidate_sha,
      run: run,
      publish_stage: publish_stage,
      validation_attempt: validation_attempt,
      evidence_server: evidence_server
    }
  end

  defp failed_run(c, :missing) do
    run = c.run

    update_validation_attempt(run, fn attempt ->
      put_in(attempt, [:result, :evidence], %{})
    end)
  end

  defp failed_run(c, :forged) do
    run = c.run

    update_validation_attempt(run, fn attempt ->
      put_in(attempt, [:result, :evidence, "receipt"], %{"id" => String.duplicate("0", 64), "manifest_sha256" => String.duplicate("1", 64)})
    end)
  end

  defp failed_run(c, :failed) do
    run = c.run
    context = %{task_id: run.task_id, run_id: run.id, attempt_id: run.id <> "/validate/failed", base_sha: c.base_sha}

    {receipt, _manifest} =
      write_signed_contract(
        c.workspace,
        run.execution.validation_archive,
        run.execution.validation_policy,
        context,
        c.base_sha,
        exit_status: 1
      )

    update_validation_attempt(run, fn attempt ->
      %{attempt | id: context.attempt_id, gate: :passed, result: %{evidence: %{"receipt" => receipt}}}
    end)
  end

  defp failed_run(c, :stale) do
    run = c.run
    update_validation_attempt(run, &Map.put(&1, :id, "run-publisher-fixture/validate/2"))
  end

  defp failed_run(c, :dirty) do
    File.write!(Path.join(c.workspace, "untracked.txt"), "dirty candidate\n")
    c.run
  end

  defp update_validation_attempt(run, update) do
    attempts = List.update_at(run.attempts, -1, update)
    %{run | attempts: attempts}
  end

  defp write_signed_contract(workspace, archive, policy, context, base_sha, opts \\ []) do
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

    key =
      case File.read(key_path) do
        {:ok, existing} ->
          existing

        {:error, :enoent} ->
          generated = :crypto.strong_rand_bytes(32)
          File.write!(key_path, generated)
          File.chmod!(key_path, 0o600)
          generated
      end

    exit_status = Keyword.get(opts, :exit_status, 0)
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
      "exit_status" => exit_status,
      "source_after" => source_digest,
      "build_digest" => nil,
      "limits" => %{},
      "assertions" => %{"exit_status" => exit_status, "completed" => true},
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
    File.write!(Path.join(directory, "manifest.mac"), :crypto.mac(:hmac, :sha256, key, bytes) |> Base.encode16(case: :lower))
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

  defp policy_yaml do
    """
    version: 1
    revision: synthetic-publisher-contract-v1
    environment:
      image: python@sha256:#{String.duplicate("a", 64)}
      runner_version: docker-v1
    checks:
      - id: contract
        adapter: command
        command: [python3, -c, "pass"]
        timeout_ms: 5000
        paths: [README.md]
        result_format: exit_status
    """
  end

  defp write_upload_receipt(root, view) do
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

  defp start_remote(opts \\ []) do
    Agent.start_link(fn ->
      %{
        branch_sha: nil,
        pull_request: nil,
        finds: 0,
        branch_reads: 0,
        pushes: 0,
        creates: 0,
        updates: 0,
        fail_after_create: Keyword.get(opts, :fail_after_create, false),
        fail_after_push: Keyword.get(opts, :fail_after_push, false),
        workspace: Keyword.get(opts, :workspace),
        mutate_on_find: Keyword.get(opts, :mutate_on_find, false),
        mutated_candidate: false,
        owner_checks: 0,
        publication_intent: nil,
        intent_at_push: nil
      }
    end)
    |> elem(1)
  end

  defp owner_callbacks(remote, revoke_at) do
    check = fn ->
      count =
        Agent.get_and_update(remote, fn state ->
          count = state.owner_checks + 1
          {count, %{state | owner_checks: count}}
        end)

      if count == revoke_at, do: {:error, :publication_owner_revoked}, else: :ok
    end

    intent = fn publication_intent ->
      Agent.update(remote, &%{&1 | publication_intent: publication_intent})
      :ok
    end

    [on_publication_check: check, on_publication_intent: intent]
  end

  defp with_store(path, fun) do
    {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())

    try do
      fun.(store)
    after
      if Process.alive?(store), do: GenServer.stop(store)
    end
  end

  defp git!(directory, args) do
    {output, 0} = System.cmd("git", ["-C", directory | args], env: isolated_git_environment(), stderr_to_stdout: true)
    String.trim(output)
  end

  defp isolated_git_environment do
    System.get_env()
    |> Enum.flat_map(fn {key, _value} -> if String.starts_with?(key, "GIT_"), do: [{key, nil}], else: [] end)
    |> Kernel.++([{"GIT_CONFIG_GLOBAL", "/dev/null"}, {"GIT_CONFIG_NOSYSTEM", "1"}])
  end

  defp digest(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end

defmodule SymphonyElixir.PublisherTest.Transport do
  def find_pr(_config, branch) do
    state = state()
    increment(:finds)
    mutate_candidate_if_requested(state)
    pr = if state.pull_request && state.pull_request.head_branch == branch, do: state.pull_request, else: nil
    {:ok, pr}
  end

  def branch_sha(_config, _branch) do
    state = state()
    increment(:branch_reads)
    {:ok, state.branch_sha}
  end

  def push(_config, _workspace, sha, _branch, expected_sha) do
    state = state()

    if state.branch_sha == expected_sha do
      Agent.get_and_update(state.agent, fn value ->
        updated = %{value | branch_sha: sha, pushes: value.pushes + 1, intent_at_push: value.publication_intent}

        if value.fail_after_push do
          {{:error, {:git_push_unknown, :connection_lost_after_push}}, %{updated | fail_after_push: false}}
        else
          {:ok, updated}
        end
      end)
    else
      {:error, :remote_branch_changed}
    end
  end

  def create_pr(_config, branch, _title, body) do
    state = state()
    number = 41

    pr = %{
      number: number,
      url: "https://github.com/RigCodeAI/rig/pull/#{number}",
      state: "open",
      head_sha: state.branch_sha,
      head_branch: branch,
      base_branch: "main",
      body: body
    }

    Agent.update(state.agent, fn value -> %{value | pull_request: pr, creates: value.creates + 1} end)

    if state.fail_after_create do
      Agent.update(state.agent, &%{&1 | fail_after_create: false})
      {:error, {:github_request_failed, :connection_lost_after_create}}
    else
      {:ok, pr}
    end
  end

  def update_pr(_config, number, body, expected_body) do
    state = state()

    Agent.get_and_update(state.agent, fn value ->
      case value.pull_request do
        %{number: ^number, body: ^expected_body} = pr ->
          updated = %{pr | body: body}
          {{:ok, updated}, %{value | pull_request: updated, updates: value.updates + 1}}

        _ ->
          {{:error, :pull_request_body_changed}, value}
      end
    end)
  end

  def get_pr(_config, number) do
    case state().pull_request do
      %{number: ^number} = pr -> {:ok, pr}
      _ -> {:error, :missing_pull_request}
    end
  end

  defp state do
    agent = Process.get(:publisher_test_remote)
    Map.put(Agent.get(agent, & &1), :agent, agent)
  end

  defp mutate_candidate_if_requested(%{mutate_on_find: true, workspace: workspace, agent: agent}) do
    File.write!(Path.join(workspace, "README.md"), "changed after validation approval\n")
    git!(workspace, ["add", "README.md"])
    git!(workspace, ["commit", "--quiet", "-m", "concurrent candidate change"])
    Agent.update(agent, &%{&1 | mutate_on_find: false, mutated_candidate: true})
  end

  defp mutate_candidate_if_requested(_state), do: :ok

  defp git!(directory, args) do
    {output, 0} = System.cmd("git", ["-C", directory | args], env: isolated_git_environment(), stderr_to_stdout: true)
    String.trim(output)
  end

  defp isolated_git_environment do
    System.get_env()
    |> Enum.flat_map(fn {key, _value} -> if String.starts_with?(key, "GIT_"), do: [{key, nil}], else: [] end)
    |> Kernel.++([{"GIT_CONFIG_GLOBAL", "/dev/null"}, {"GIT_CONFIG_NOSYSTEM", "1"}])
  end

  defp increment(key) do
    agent = Process.get(:publisher_test_remote)
    Agent.update(agent, &Map.update!(&1, key, fn value -> value + 1 end))
  end
end
