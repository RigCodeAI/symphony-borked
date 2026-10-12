defmodule SymphonyElixir.CandidateTransferTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{CandidateContext, CandidateGit, CandidateTransfer}

  @control %{
    "host" => "factory-control@worker",
    "machine_id" => String.duplicate("a", 32),
    "service_revision" => String.duplicate("b", 40),
    "release_sha256" => String.duplicate("c", 64),
    "argv" => ["/usr/bin/true"]
  }
  @producer_command ["/usr/bin/python3", "-I", "-c", "trusted producer fixture"]
  @candidate_input %{
    "thread_id" => "fixture-thread",
    "metadata" => %{"z" => 4, "a" => ["DEV-238", true]}
  }

  test "imports the exact candidate when the coordinator lacks history before its pinned base" do
    fixture = fixture()
    rpc = rpc(fixture, fixture.bundle)

    assert {:ok, transferred, metadata} = CandidateTransfer.sync(fixture.run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])
    assert transferred != fixture.workspace
    assert metadata.candidate_commit == fixture.metadata["candidate_commit"]
    assert metadata.base_commit == fixture.base_sha
    assert metadata.branch == fixture.metadata["branch"]
    assert {:ok, ^transferred} = CandidateTransfer.workspace(fixture.run)
    assert File.read!(Path.join(transferred, "README.md")) == "candidate\n"
    assert {:ok, head} = CandidateGit.run(transferred, ["rev-parse", "--verify", "HEAD^{commit}"])
    assert String.trim(head) == fixture.metadata["candidate_commit"]
    assert {:ok, original_head} = CandidateGit.run(fixture.workspace, ["rev-parse", "--verify", "HEAD^{commit}"])
    assert String.trim(original_head) == fixture.base_sha
    assert {:ok, ""} = CandidateGit.run(fixture.workspace, ["status", "--porcelain=v1", "--untracked-files=all"])

    assert_receive {:candidate_bundle_requested, _request}
    assert {:ok, %File.Stat{mode: root_mode}} = File.stat(fixture.validation_candidates)
    assert Bitwise.band(root_mode, 0o777) == 0o700
    assert {:ok, %File.Stat{mode: snapshot_mode}} = File.stat(transferred)
    assert Bitwise.band(snapshot_mode, 0o777) == 0o700
    assert {:ok, %File.Stat{mode: marker_mode}} = File.stat(Path.join(transferred, ".git/transfer.json"))
    assert Bitwise.band(marker_mode, 0o777) == 0o600
    assert File.read!(Path.join(transferred, ".git/shallow")) == fixture.base_sha <> "\n"
    assert :binary.match(File.read!(Path.join(transferred, ".git/transfer.json")), fixture.base_tree_sha) != :nomatch
    refute File.exists?(Path.join(fixture.workspace, ".git/shallow"))

    # An exact replay validates the saved snapshot instead of requesting the worker bundle again.
    assert {:ok, ^transferred, _metadata} = CandidateTransfer.sync(fixture.run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])
    refute_receive {:candidate_bundle_requested, _request}, 50
  end

  test "rejects a stale current candidate artifact before contacting the worker" do
    fixture = fixture()
    changed = Map.put(fixture.run.outputs, "candidate", %{"thread_id" => "newer-thread"})
    run = %{fixture.run | outputs: changed}
    rpc = rpc(fixture, fixture.bundle)

    assert {:error, :candidate_transfer_candidate_input_superseded} =
             CandidateTransfer.sync(run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])

    refute_receive {:candidate_bundle_requested, _request}, 50
    refute_receive {:worker_status_requested, _request}, 50
    assert {:ok, head} = CandidateGit.run(fixture.workspace, ["rev-parse", "--verify", "HEAD^{commit}"])
    assert String.trim(head) == fixture.base_sha
  end

  test "rejects producer metadata whose base differs from the pinned validation base" do
    fixture = fixture()
    metadata = Map.put(fixture.metadata, "base_commit", String.duplicate("f", 40))
    [producer_attempt | rest] = fixture.run.attempts
    producer_attempt = put_in(producer_attempt, [:result, :evidence, "output"], Jason.encode!(metadata))
    outputs = put_in(fixture.run.outputs, ["candidate-commit", "output"], Jason.encode!(metadata))
    run = %{fixture.run | attempts: [producer_attempt | rest], outputs: outputs}
    rpc = rpc(fixture, fixture.bundle)

    assert {:error, :candidate_transfer_base_or_branch_mismatch} =
             CandidateTransfer.sync(run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])

    refute_receive {:worker_status_requested, _request}, 50
  end

  test "rejects a bundle whose candidate has a different parent and leaves no snapshot" do
    fixture = fixture(other_parent: true)
    rpc = rpc(fixture, fixture.bundle)

    assert {:error, :candidate_transfer_commit_or_parent_mismatch} =
             CandidateTransfer.sync(fixture.run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])

    assert_receive {:candidate_bundle_requested, _request}
    assert {:ok, entries} = File.ls(fixture.validation_candidates)
    assert Enum.all?(entries, &String.starts_with?(&1, ".incoming-")) or entries == []
    assert {:ok, head} = CandidateGit.run(fixture.workspace, ["rev-parse", "--verify", "HEAD^{commit}"])
    assert String.trim(head) == fixture.base_sha
    assert {:ok, ""} = CandidateGit.run(fixture.workspace, ["status", "--porcelain=v1", "--untracked-files=all"])
    refute File.exists?(Path.join(fixture.workspace, ".git/shallow"))
  end

  test "rejects an oversized file from a small compressed bundle before checkout" do
    fixture = fixture(oversized_file: true)
    assert byte_size(fixture.bundle) < 1_048_576
    rpc = rpc(fixture, fixture.bundle)

    assert {:error, :candidate_transfer_file_budget_exceeded} =
             CandidateTransfer.sync(fixture.run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])

    assert_receive {:candidate_bundle_requested, _request}
    assert {:ok, []} = File.ls(fixture.validation_candidates)
    assert {:ok, head} = CandidateGit.run(fixture.workspace, ["rev-parse", "--verify", "HEAD^{commit}"])
    assert String.trim(head) == fixture.base_sha
    assert {:ok, ""} = CandidateGit.run(fixture.workspace, ["status", "--porcelain=v1", "--untracked-files=all"])
  end

  test "rejects a corrupt bundle even when its producer metadata carries the matching digest" do
    fixture = fixture()
    damaged_bundle = corrupt_last_byte(fixture.bundle)
    metadata = Map.put(fixture.metadata, "bundle_sha256", digest(damaged_bundle))
    [producer_attempt | rest] = fixture.run.attempts
    result = put_in(producer_attempt, [:result, :evidence, "output"], Jason.encode!(metadata))
    outputs = put_in(fixture.run.outputs, ["candidate-commit", "output"], Jason.encode!(metadata))
    run = %{fixture.run | attempts: [result | rest], outputs: outputs}
    fixture = %{fixture | bundle: damaged_bundle, metadata: metadata, run: run}
    rpc = rpc(fixture, damaged_bundle)

    assert {:error, :candidate_transfer_bundle_import_failed} =
             CandidateTransfer.sync(run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])

    assert_receive {:candidate_bundle_requested, _request}
    assert {:ok, entries} = File.ls(fixture.validation_candidates)
    assert entries == []
    assert {:ok, head} = CandidateGit.run(fixture.workspace, ["rev-parse", "--verify", "HEAD^{commit}"])
    assert String.trim(head) == fixture.base_sha
  end

  test "rejects stale snapshots and never replaces a damaged marker on replay" do
    fixture = fixture()
    rpc = rpc(fixture, fixture.bundle)
    assert {:ok, transferred, _metadata} = CandidateTransfer.sync(fixture.run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])
    assert_receive {:candidate_bundle_requested, _request}

    marker_path = Path.join(transferred, ".git/transfer.json")
    marker = marker_path |> File.read!() |> Jason.decode!() |> Map.put("shallow_base_tree", String.duplicate("f", 40))
    File.write!(marker_path, Jason.encode!(marker))
    File.chmod!(marker_path, 0o600)

    assert {:error, _} = CandidateTransfer.workspace(fixture.run)
    assert {:error, _} = CandidateTransfer.sync(fixture.run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])
    refute_receive {:candidate_bundle_requested, _request}, 50
    assert Jason.decode!(File.read!(marker_path))["shallow_base_tree"] == String.duplicate("f", 40)
  end

  test "rejects a later non-check attempt between the producer and validation" do
    fixture = fixture()
    superseded = %{id: "run-candidate-transfer/implement/3", stage: "implement", type: :agent, status: :completed, gate: nil}
    run = %{fixture.run | attempts: List.insert_at(fixture.run.attempts, 1, superseded)}
    rpc = rpc(fixture, fixture.bundle)

    assert {:error, :candidate_transfer_producer_superseded} =
             CandidateTransfer.sync(run, fixture.validation_stage, worker_operation_opts: [rpc_fun: rpc])

    refute_receive {:worker_status_requested, _request}, 50
  end

  test "repairs import from the authenticated prior snapshot and replay keeps its history boundary" do
    original = fixture()

    assert {:ok, prior_snapshot, _metadata} =
             CandidateTransfer.sync(original.run, original.validation_stage, worker_operation_opts: [rpc_fun: rpc(original, original.bundle)])

    drain_rpc_messages()
    repair = contextual_fixture(original, prior_snapshot)
    repair_rpc = rpc(repair, repair.bundle)

    assert {:ok, transferred, metadata} =
             CandidateTransfer.sync(repair.run, repair.validation_stage, worker_operation_opts: [rpc_fun: repair_rpc])

    assert metadata.candidate_context_id == repair.context_id
    assert metadata.repair_parent_sha == repair.prior_candidate_sha
    assert metadata.validation_base == repair.validation_base
    assert metadata.base_commit == repair.prior_candidate_sha
    assert File.read!(Path.join(transferred, "README.md")) == "repaired candidate\n"
    assert File.read!(Path.join(transferred, ".git/shallow")) == repair.base_sha <> "\n"
    assert {:ok, parent} = CandidateGit.run(transferred, ["rev-parse", "--verify", "HEAD^1^{commit}"])
    assert String.trim(parent) == repair.prior_candidate_sha
    assert CandidateContext.same_context?(repair.run, repair.producer_attempt_id, repair.validation_attempt_id)

    marker = Jason.decode!(File.read!(Path.join(transferred, ".git/transfer.json")))
    assert marker["candidate_context_id"] == repair.context_id
    assert marker["repair_parent_sha"] == repair.prior_candidate_sha
    assert marker["validation_base"] == repair.validation_base
    assert marker["shallow_base_commit"] == repair.base_sha

    assert_receive {:worker_status_requested, _request}
    assert_receive {:candidate_bundle_requested, _request}

    assert {:ok, ^transferred, replay_metadata} =
             CandidateTransfer.sync(repair.run, repair.validation_stage, worker_operation_opts: [rpc_fun: repair_rpc])

    assert replay_metadata.candidate_context_id == repair.context_id
    assert_receive {:worker_status_requested, _request}
    refute_receive {:candidate_bundle_requested, _request}, 50
  end

  test "rejects a tampered sealed context before contacting the worker" do
    original = fixture()

    assert {:ok, prior_snapshot, _metadata} =
             CandidateTransfer.sync(original.run, original.validation_stage, worker_operation_opts: [rpc_fun: rpc(original, original.bundle)])

    drain_rpc_messages()
    repair = contextual_fixture(original, prior_snapshot)
    tampered = put_in(repair.run, [:candidate_contexts, repair.context_id, :validation_base], repair.prior_candidate_sha)

    assert {:error, :candidate_transfer_context_not_authorized} =
             CandidateTransfer.sync(tampered, repair.validation_stage, worker_operation_opts: [rpc_fun: rpc(repair, repair.bundle)])

    refute_receive {:worker_status_requested, _request}, 50
    refute_receive {:candidate_bundle_requested, _request}, 50
  end

  test "rejects a main validation base outside the published candidate history before worker access" do
    original = fixture()

    assert {:ok, prior_snapshot, _metadata} =
             CandidateTransfer.sync(original.run, original.validation_stage, worker_operation_opts: [rpc_fun: rpc(original, original.bundle)])

    drain_rpc_messages()
    repair = contextual_fixture(original, prior_snapshot, outside_main: true)

    assert {:error, :candidate_transfer_validation_base_not_ancestor} =
             CandidateTransfer.sync(repair.run, repair.validation_stage, worker_operation_opts: [rpc_fun: rpc(repair, repair.bundle)])

    refute_receive {:worker_status_requested, _request}, 50
    refute_receive {:candidate_bundle_requested, _request}, 50
  end

  test "rejects an old context-free marker when the current candidate is contextual" do
    original = fixture()

    assert {:ok, prior_snapshot, _metadata} =
             CandidateTransfer.sync(original.run, original.validation_stage, worker_operation_opts: [rpc_fun: rpc(original, original.bundle)])

    drain_rpc_messages()
    repair = contextual_fixture(original, prior_snapshot)
    repair_rpc = rpc(repair, repair.bundle)

    assert {:ok, transferred, _metadata} =
             CandidateTransfer.sync(repair.run, repair.validation_stage, worker_operation_opts: [rpc_fun: repair_rpc])

    drain_rpc_messages()
    marker_path = Path.join(transferred, ".git/transfer.json")
    File.write!(marker_path, File.read!(Path.join(prior_snapshot, ".git/transfer.json")))
    File.chmod!(marker_path, 0o600)

    assert {:error, :candidate_transfer_snapshot_marker_mismatch} =
             CandidateTransfer.sync(repair.run, repair.validation_stage, worker_operation_opts: [rpc_fun: repair_rpc])

    refute_receive {:candidate_bundle_requested, _request}, 50
  end

  test "allows validation at the repair parent while retaining the original main checkout" do
    original = fixture()

    assert {:ok, prior_snapshot, _metadata} =
             CandidateTransfer.sync(original.run, original.validation_stage, worker_operation_opts: [rpc_fun: rpc(original, original.bundle)])

    drain_rpc_messages()
    repair = contextual_fixture(original, prior_snapshot, validation_base: original.metadata["candidate_commit"])

    assert {:ok, transferred, metadata} =
             CandidateTransfer.sync(repair.run, repair.validation_stage, worker_operation_opts: [rpc_fun: rpc(repair, repair.bundle)])

    assert_receive {:worker_status_requested, _request}
    assert_receive {:candidate_bundle_requested, _request}
    assert metadata.validation_base == repair.prior_candidate_sha
    assert {:ok, original_head} = CandidateGit.run(original.workspace, ["rev-parse", "--verify", "HEAD^{commit}"])
    assert String.trim(original_head) == original.base_sha
    assert File.read!(Path.join(transferred, ".git/shallow")) == original.base_sha <> "\n"
  end

  test "hashes Unicode candidate input with Python ASCII JSON escaping" do
    original = fixture()

    assert {:ok, prior_snapshot, _metadata} =
             CandidateTransfer.sync(original.run, original.validation_stage, worker_operation_opts: [rpc_fun: rpc(original, original.bundle)])

    drain_rpc_messages()
    repair = contextual_fixture(original, prior_snapshot, unicode_input: true)

    candidate_json =
      "{\"metadata\":{\"a\":[\"DEV-238\",true],\"z\":4},\"repair_explanation\":\"caf\\u00e9 \\u2014 \\ud83d\\ude00\\u007f\",\"thread_id\":\"fixture-thread\"}"

    expected_json =
      ~s|{"candidate":#{candidate_json},"candidate_context":{"id":"#{repair.context_id}","repair_parent_sha":"#{repair.prior_candidate_sha}","validation_base":"#{repair.validation_base}"}}|

    assert digest(expected_json) == repair.metadata["candidate_input_sha256"]

    assert {:ok, transferred, _metadata} =
             CandidateTransfer.sync(repair.run, repair.validation_stage, worker_operation_opts: [rpc_fun: rpc(repair, repair.bundle)])

    assert File.read!(Path.join(transferred, "README.md")) == "repaired candidate\n"
    assert_receive {:worker_status_requested, _request}
    assert_receive {:candidate_bundle_requested, _request}
  end

  test "historical development, altered policy, and unauthenticated evidence cannot seed a repair" do
    for failure <- [:development, :policy, :environment, :forged_mac, :missing_log] do
      original = fixture()
      assert {:ok, snapshot, _} = CandidateTransfer.sync(original.run, original.validation_stage, worker_operation_opts: [rpc_fun: rpc(original, original.bundle)])
      drain_rpc_messages()
      repair = contextual_fixture(original, snapshot)
      previous = Enum.at(repair.run.attempts, 2)
      receipt = previous.result.evidence["receipt"]
      directory = Path.join(original.validation_archive, receipt["id"])
      manifest_path = Path.join(directory, "manifest.json")
      manifest = Jason.decode!(File.read!(manifest_path))

      manifest =
        case failure do
          :development -> Map.put(manifest, "mode", "development")
          :policy -> Map.put(manifest, "policy_revision", "different-policy")
          :environment -> Map.put(manifest, "environment", %{"runner_version" => "different"})
          _ -> manifest
        end

      bytes = Jason.encode!(manifest)
      File.write!(manifest_path, bytes)
      key = File.read!(Path.join(original.validation_archive, "producer.key"))
      signature = :crypto.mac(:hmac, :sha256, key, bytes) |> Base.encode16(case: :lower)
      File.write!(Path.join(directory, "manifest.mac"), if(failure == :forged_mac, do: String.duplicate("0", 64), else: signature))
      if failure == :missing_log, do: File.rm!(Path.join(directory, "check-contract.log"))
      updated = put_in(previous, [:result, :evidence, "receipt", "manifest_sha256"], digest(bytes))
      run = %{repair.run | attempts: List.replace_at(repair.run.attempts, 2, updated)}
      assert {:error, :candidate_transfer_context_receipt_invalid} = CandidateTransfer.sync(run, repair.validation_stage, worker_operation_opts: [rpc_fun: rpc(repair, repair.bundle)])
      refute_receive {:worker_status_requested, _}, 10
      refute_receive {:candidate_bundle_requested, _}, 10
    end
  end

  defp drain_rpc_messages do
    receive do
      {:worker_status_requested, _} -> drain_rpc_messages()
      {:candidate_bundle_requested, _} -> drain_rpc_messages()
    after
      0 -> :ok
    end
  end

  defp fixture(opts \\ []) do
    root = Path.join(System.tmp_dir!(), "candidate-transfer-#{System.unique_integer([:positive])}")
    seed = Path.join(root, "seed")
    workspace = Path.join(root, "coordinator")
    worker = Path.join(root, "worker")
    validation_candidates = Path.join(root, "validation-candidates")
    validation_archive = Path.join(root, "validation-archive")
    File.mkdir_p!(seed)
    File.mkdir_p!(workspace)
    File.mkdir_p!(validation_candidates)
    File.mkdir_p!(validation_archive)
    File.chmod!(validation_candidates, 0o700)
    File.chmod!(validation_archive, 0o700)
    File.write!(Path.join(validation_archive, "producer.key"), :crypto.strong_rand_bytes(32))
    File.chmod!(Path.join(validation_archive, "producer.key"), 0o600)

    git!(seed, ["init", "--quiet"])
    configure_git(seed)
    File.write!(Path.join(seed, "README.md"), "older\n")
    git!(seed, ["add", "."])
    git!(seed, ["commit", "--quiet", "-m", "older"])
    older_sha = git!(seed, ["rev-parse", "HEAD"])
    File.write!(Path.join(seed, "README.md"), "base\n")
    git!(seed, ["add", "README.md"])
    git!(seed, ["commit", "--quiet", "-m", "base"])
    base_sha = git!(seed, ["rev-parse", "HEAD"])
    base_tree_sha = git!(seed, ["rev-parse", "--verify", base_sha <> "^{tree}"])

    # Keep the base commit and tree while omitting its parent history from the coordinator clone.
    git!(root, ["clone", "--quiet", "--depth=1", "--no-local", "file://" <> seed, workspace])
    File.rm!(Path.join(workspace, ".git/shallow"))
    {_missing_parent, missing_parent_status} = System.cmd("git", ["cat-file", "-e", older_sha], cd: workspace, stderr_to_stdout: true)
    assert missing_parent_status != 0

    git!(root, ["clone", "--quiet", "--local", seed, worker])
    configure_git(worker)

    if opts[:oversized_file] do
      oversized_path = Path.join(worker, "oversized.bin")
      chunk = :binary.copy("R", 1_048_576)

      File.open!(oversized_path, [:write, :binary], fn file ->
        Enum.each(1..64, fn _ -> IO.binwrite(file, chunk) end)
        IO.binwrite(file, "R")
      end)
    end

    if opts[:other_parent] do
      File.write!(Path.join(worker, "side.txt"), "side parent\n")
      git!(worker, ["add", "side.txt"])
      git!(worker, ["commit", "--quiet", "-m", "unexpected parent"])
    end

    File.write!(Path.join(worker, "README.md"), "candidate\n")
    git!(worker, ["add", "--all"])
    git!(worker, ["commit", "--quiet", "-m", "candidate"])
    candidate_sha = git!(worker, ["rev-parse", "HEAD"])

    candidate_input_sha = candidate_input_sha(@candidate_input)
    bundle_path = Path.join(root, "candidate.bundle")
    git!(worker, ["bundle", "create", bundle_path, "HEAD", "^" <> base_sha])
    bundle = File.read!(bundle_path)

    metadata = %{
      "candidate_commit" => candidate_sha,
      "base_commit" => base_sha,
      "candidate_input_sha256" => candidate_input_sha,
      "branch" => "refs/heads/feature/candidate-transfer",
      "candidate_bundle" => ".git/factory-candidates/#{candidate_input_sha}.bundle",
      "bundle_sha256" => digest(bundle),
      "bundle_bytes" => byte_size(bundle)
    }

    producer_attempt_id = "run-candidate-transfer/local-commit/1"
    validation_attempt_id = "run-candidate-transfer/validate/3"
    identity = worker_identity(producer_attempt_id)

    producer_attempt = %{
      id: producer_attempt_id,
      stage: "local-commit",
      type: :check,
      status: :completed,
      gate: :passed,
      result: %{
        status: :ok,
        evidence: %{
          "exit_status" => 0,
          "exit_code" => "exited",
          "truncated" => false,
          "timed_out" => false,
          "output" => Jason.encode!(metadata)
        }
      }
    }

    inspect_attempt = %{
      id: "run-candidate-transfer/inspect-candidate/2",
      stage: "inspect-candidate",
      type: :check,
      status: :completed,
      gate: :passed
    }

    validation_stage = %{
      id: "validate",
      type: :check,
      inputs: ["candidate", "candidate-commit"],
      outputs: ["validation"],
      gate: %{evaluator: :candidate_validation, required: [%{check: "contract", assertion: "exit_status", equals: 0}]}
    }

    definition = %{
      stages: %{
        "local-commit" => %{
          id: "local-commit",
          type: :check,
          inputs: ["candidate"],
          outputs: ["candidate-commit"],
          gate: %{command: @producer_command}
        },
        "inspect-candidate" => %{id: "inspect-candidate", type: :check, inputs: ["candidate", "candidate-commit"]},
        "validate" => validation_stage,
        "publish" => %{id: "publish", type: :publish, gate: %{validation_stage: "validate"}}
      }
    }

    execution = %{
      worker_control: @control,
      validation_base: base_sha,
      validation_candidates: validation_candidates,
      validation_archive: validation_archive,
      validation_policy: %{digest: digest("validation-policy"), revision: "fixture-v1", environment: %{}},
      candidate_producer_stage: "local-commit",
      candidate_producer_command_sha256: digest(Jason.encode!(@producer_command))
    }

    run = %{
      id: "run-candidate-transfer",
      task_id: "DEV-238",
      policy: %{version: 1, validation_sha256: "fixture-validation"},
      branch: "feature/candidate-transfer",
      workspace: workspace,
      definition: definition,
      execution: execution,
      current_candidate_context_id: nil,
      candidate_contexts: %{},
      candidate_execution_original: execution,
      attempts: [producer_attempt, inspect_attempt, %{id: validation_attempt_id, stage: "validate", type: :check, status: :executing}],
      current_attempt_id: validation_attempt_id,
      stage_id: "validate",
      outputs: %{
        "candidate" => @candidate_input,
        "candidate-commit" => %{
          "exit_status" => 0,
          "output" => Jason.encode!(metadata),
          "truncated" => false,
          "timed_out" => false
        }
      },
      operations: %{
        producer_attempt_id => %{
          id: producer_attempt_id,
          attempt_id: producer_attempt_id,
          stage: "local-commit",
          type: :check,
          status: :completed,
          worker: identity
        }
      }
    }

    on_exit(fn -> File.rm_rf(root) end)

    %{
      root: root,
      workspace: workspace,
      worker: worker,
      validation_candidates: validation_candidates,
      validation_archive: validation_archive,
      base_sha: base_sha,
      base_tree_sha: base_tree_sha,
      bundle: bundle,
      metadata: metadata,
      run: run,
      validation_stage: validation_stage,
      identity: identity
    }
  end

  defp contextual_fixture(original, prior_snapshot, opts \\ []) do
    prior_candidate_sha = original.metadata["candidate_commit"]

    validation_base =
      cond do
        is_binary(opts[:validation_base]) ->
          opts[:validation_base]

        opts[:outside_main] ->
          configure_git(original.workspace)
          git!(original.workspace, ["checkout", "--quiet", "--detach", original.base_sha])
          File.write!(Path.join(original.workspace, "unrelated-main.txt"), "outside published history\n")
          git!(original.workspace, ["add", "unrelated-main.txt"])
          git!(original.workspace, ["commit", "--quiet", "-m", "unrelated main advance"])
          git!(original.workspace, ["rev-parse", "HEAD"])

        true ->
          original.base_sha
      end

    git!(original.worker, ["checkout", "--quiet", "--detach", prior_candidate_sha])
    File.write!(Path.join(original.worker, "README.md"), "repaired candidate\n")
    git!(original.worker, ["add", "--all"])
    git!(original.worker, ["commit", "--quiet", "-m", "repair candidate"])
    candidate_sha = git!(original.worker, ["rev-parse", "HEAD"])

    prior_validation_id = "run-candidate-transfer/validate/2"

    prior_validation_context = %{
      task_id: original.run.task_id,
      run_id: original.run.id,
      attempt_id: prior_validation_id,
      base_sha: original.base_sha
    }

    receipt = signed_receipt(original, prior_validation_context, prior_candidate_sha, original.base_sha)

    pr = %{
      "number" => 238,
      "url" => "https://github.com/RigCodeAI/rig/pull/238",
      "head_branch" => original.run.branch,
      "base_branch" => "main"
    }

    publication_evidence = %{
      "candidate_sha" => prior_candidate_sha,
      "validation_attempt_id" => prior_validation_id,
      "pr" => pr
    }

    review_evidence = %{
      "head_sha" => prior_candidate_sha,
      "base_sha" => validation_base,
      "pr_number" => pr["number"],
      "pr_url" => pr["url"]
    }

    prior_validation = %{
      id: prior_validation_id,
      stage: "validate",
      type: :check,
      status: :completed,
      gate: :passed,
      candidate_context_id: nil,
      result: %{evidence: %{"receipt" => receipt}}
    }

    publication = %{
      id: "run-candidate-transfer/publish/1",
      stage: "publish",
      type: :publish,
      status: :completed,
      gate: :passed,
      candidate_context_id: nil,
      result: %{evidence: publication_evidence}
    }

    review = %{
      id: "run-candidate-transfer/review/1",
      stage: "review",
      type: :check,
      status: :completed,
      gate: :passed,
      candidate_context_id: nil,
      result: %{evidence: review_evidence}
    }

    original_execution = original.run.execution

    context = %{
      version: 1,
      run_id: original.run.id,
      task_id: original.run.task_id,
      execution_sha256: context_digest(original_execution),
      definition_sha256: context_digest(original.run.definition),
      policy_sha256: context_digest(original.run.policy),
      source_context_id: nil,
      validation_base: validation_base,
      repair_parent_sha: prior_candidate_sha,
      publication_attempt_id: publication.id,
      publication_sha256: context_digest(publication_evidence),
      review_attempt_id: review.id,
      review_sha256: context_digest(review_evidence),
      pr: pr
    }

    context_id = context_digest(context)
    context = Map.put(context, :id, context_id)
    key = File.read!(Path.join(original.validation_archive, "producer.key"))
    context = Map.put(context, :seal, context_seal(context, key))
    context_json = Map.take(context, [:id, :repair_parent_sha, :validation_base])
    context_json = Map.new(context_json, fn {key, value} -> {Atom.to_string(key), value} end)

    candidate_input =
      if opts[:unicode_input],
        do: Map.put(@candidate_input, "repair_explanation", "café — 😀\u007f"),
        else: @candidate_input

    candidate_input_sha = contextual_candidate_input_sha(candidate_input, context_json)

    bundle_path = Path.join(original.root, "repair-candidate.bundle")
    git!(original.worker, ["bundle", "create", bundle_path, "HEAD", "^" <> prior_candidate_sha])
    bundle = File.read!(bundle_path)

    metadata = %{
      "candidate_commit" => candidate_sha,
      "base_commit" => prior_candidate_sha,
      "candidate_input_sha256" => candidate_input_sha,
      "branch" => "refs/heads/" <> original.run.branch,
      "candidate_bundle" => ".git/factory-candidates/#{candidate_input_sha}.bundle",
      "bundle_sha256" => digest(bundle),
      "bundle_bytes" => byte_size(bundle),
      "candidate_context_id" => context_id,
      "repair_parent_sha" => prior_candidate_sha,
      "validation_base" => validation_base
    }

    producer_attempt_id = "run-candidate-transfer/local-commit/2"
    validation_attempt_id = "run-candidate-transfer/validate/3"
    identity = worker_identity(producer_attempt_id)
    output = Jason.encode!(metadata)

    producer_attempt = %{
      id: producer_attempt_id,
      stage: "local-commit",
      type: :check,
      status: :completed,
      gate: :passed,
      candidate_context_id: context_id,
      result: %{
        status: :ok,
        evidence: %{
          "exit_status" => 0,
          "exit_code" => "exited",
          "truncated" => false,
          "timed_out" => false,
          "output" => output
        }
      }
    }

    validation_attempt = %{
      id: validation_attempt_id,
      stage: "validate",
      type: :check,
      status: :executing,
      gate: nil,
      candidate_context_id: context_id
    }

    run = %{
      original.run
      | attempts: [hd(original.run.attempts), Enum.at(original.run.attempts, 1), prior_validation, publication, review, producer_attempt, validation_attempt],
        current_attempt_id: validation_attempt_id,
        stage_id: "validate",
        current_candidate_context_id: context_id,
        candidate_contexts: %{context_id => context},
        candidate_execution_original: original_execution,
        execution: original_execution,
        outputs: %{
          "candidate" => candidate_input,
          "candidate-commit" => %{
            "exit_status" => 0,
            "output" => output,
            "truncated" => false,
            "timed_out" => false
          }
        },
        operations:
          Map.put(original.run.operations, producer_attempt_id, %{
            id: producer_attempt_id,
            attempt_id: producer_attempt_id,
            stage: "local-commit",
            type: :check,
            status: :completed,
            worker: identity
          })
    }

    Map.merge(original, %{
      run: run,
      worker: original.worker,
      identity: identity,
      metadata: metadata,
      bundle: bundle,
      prior_candidate_sha: prior_candidate_sha,
      validation_base: validation_base,
      context_id: context_id,
      producer_attempt_id: producer_attempt_id,
      validation_attempt_id: validation_attempt_id,
      prior_snapshot: prior_snapshot
    })
  end

  defp signed_receipt(fixture, context, candidate_sha, base_sha) do
    archive = fixture.validation_archive
    key = File.read!(Path.join(archive, "producer.key"))
    context_json = context |> Jason.encode!() |> Jason.decode!()
    policy = fixture.run.execution.validation_policy
    output = "synthetic prior validation evidence\n"
    artifact = %{"file" => "check-contract.log", "sha256" => digest(output), "bytes" => byte_size(output)}
    id = digest(Jason.encode!(context_json))

    manifest = %{
      "id" => id,
      "producer" => digest(key),
      "candidate_sha" => candidate_sha,
      "base_sha" => base_sha,
      "context" => context_json,
      "mode" => "final",
      "policy_digest" => policy.digest,
      "policy_revision" => policy.revision,
      "environment" => policy.environment,
      "complete" => true,
      "check_ids" => ["contract"],
      "checks" => [
        %{
          "id" => "contract",
          "status" => "completed",
          "exit_status" => 0,
          "assertions" => %{"exit_status" => 0, "completed" => true},
          "artifact" => artifact
        }
      ],
      "artifacts" => [artifact]
    }

    bytes = Jason.encode!(manifest)
    directory = Path.join(archive, id)
    File.mkdir_p!(directory)
    File.write!(Path.join(directory, artifact["file"]), output)
    File.write!(Path.join(directory, "manifest.json"), bytes)
    File.write!(Path.join(directory, "manifest.mac"), :crypto.mac(:hmac, :sha256, key, bytes) |> Base.encode16(case: :lower))
    %{"id" => id, "manifest_sha256" => digest(bytes)}
  end

  defp context_bytes(value), do: value |> Jason.encode!() |> Jason.decode!() |> :erlang.term_to_binary([:deterministic])
  defp context_digest(value), do: value |> context_bytes() |> digest()

  defp context_seal(value, key) do
    :crypto.mac(:hmac, :sha256, key, "factory-candidate-context-v1\n" <> context_bytes(value))
    |> Base.encode16(case: :lower)
  end

  defp contextual_candidate_input_sha(candidate, context) do
    digest(python_compact_json(%{"candidate" => candidate, "candidate_context" => context}))
  end

  defp python_compact_json(value) when is_map(value) do
    value
    |> Map.keys()
    |> Enum.sort()
    |> Enum.map_join(",", fn key -> Jason.encode!(key) <> ":" <> python_compact_json(Map.fetch!(value, key)) end)
    |> then(&("{" <> &1 <> "}"))
  end

  defp python_compact_json(value) when is_list(value),
    do: "[" <> Enum.map_join(value, ",", &python_compact_json/1) <> "]"

  defp python_compact_json(value) when is_binary(value), do: python_ascii_json_string(value)

  defp python_compact_json(value), do: Jason.encode!(value)

  defp python_ascii_json_string(value) do
    value
    |> String.to_charlist()
    |> Enum.map_join(&python_ascii_codepoint/1)
    |> then(&("\"" <> &1 <> "\""))
  end

  defp python_ascii_codepoint(?\"), do: "\\\""
  defp python_ascii_codepoint(?\\), do: "\\\\"
  defp python_ascii_codepoint(?\b), do: "\\b"
  defp python_ascii_codepoint(?\f), do: "\\f"
  defp python_ascii_codepoint(?\n), do: "\\n"
  defp python_ascii_codepoint(?\r), do: "\\r"
  defp python_ascii_codepoint(?\t), do: "\\t"
  defp python_ascii_codepoint(codepoint) when codepoint < 0x20, do: "\\u" <> hex4(codepoint)
  defp python_ascii_codepoint(codepoint) when codepoint < 0x7F, do: <<codepoint>>
  defp python_ascii_codepoint(codepoint) when codepoint <= 0xFFFF, do: "\\u" <> hex4(codepoint)

  defp python_ascii_codepoint(codepoint) do
    value = codepoint - 0x10000
    high = 0xD800 + Bitwise.bsr(value, 10)
    low = 0xDC00 + Bitwise.band(value, 0x3FF)
    "\\u" <> hex4(high) <> "\\u" <> hex4(low)
  end

  defp hex4(codepoint), do: codepoint |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(4, "0")

  defp rpc(fixture, bundle) do
    parent = self()

    fn _config, request ->
      case request["action"] do
        "status" ->
          send(parent, {:worker_status_requested, request})
          {:ok, terminated(fixture.identity)}

        "candidate_bundle" ->
          send(parent, {:candidate_bundle_requested, request})
          offset = request["offset"]
          data = binary_part(bundle, offset, min(65_536, byte_size(bundle) - offset))

          {:ok,
           %{
             "operation_id" => fixture.identity["operation_id"],
             "candidate_input_sha256" => fixture.metadata["candidate_input_sha256"],
             "bundle_sha256" => fixture.metadata["bundle_sha256"],
             "bundle_bytes" => fixture.metadata["bundle_bytes"],
             "offset" => offset,
             "data_base64" => Base.encode64(data),
             "eof" => offset + byte_size(data) == fixture.metadata["bundle_bytes"]
           }}
      end
    end
  end

  defp terminated(identity) do
    proof =
      Map.take(identity, ~w(machine_id boot_id unit invocation_id control_group))
      |> Map.merge(%{
        "cgroup_state" => "present",
        "cgroup_populated" => 0,
        "active_state" => "inactive",
        "sub_state" => "dead",
        "exec_main_code" => "exited",
        "exec_main_status" => 0,
        "main_pid" => 0,
        "observed_at" => "2026-10-11T00:00:00Z",
        "systemd_version" => 254
      })

    %{
      "operation_id" => identity["operation_id"],
      "identity" => identity,
      "status" => "terminated",
      "exit_code" => "exited",
      "exit_status" => 0,
      "termination_proof" => proof
    }
  end

  defp worker_identity(operation_id) do
    %{
      "kind" => "systemd-unit",
      "operation_id" => operation_id,
      "machine_id" => @control["machine_id"],
      "boot_id" => String.duplicate("e", 32),
      "unit" => "factory-operation-#{digest(operation_id)}.service",
      "invocation_id" => String.duplicate("f", 32),
      "control_group" => "/system.slice/factory-operation-#{digest(operation_id)}.service",
      "request_sha256" => String.duplicate("d", 64)
    }
  end

  defp configure_git(path) do
    git!(path, ["config", "user.name", "Candidate"])
    git!(path, ["config", "user.email", "candidate@example.test"])
  end

  defp git!(cwd, args) do
    {output, status} = System.cmd("git", args, cd: cwd, stderr_to_stdout: true)
    assert status == 0, "git #{Enum.join(args, " ")} failed: #{output}"
    String.trim(output)
  end

  defp candidate_input_sha(value) do
    canonical = ~s({"metadata":{"a":["DEV-238",true],"z":4},"thread_id":"fixture-thread"})
    assert is_map(value)
    digest(canonical)
  end

  defp corrupt_last_byte(value) do
    size = byte_size(value) - 1
    <<prefix::binary-size(size), byte>> = value
    prefix <> <<Bitwise.bxor(byte, 1)>>
  end

  defp digest(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
