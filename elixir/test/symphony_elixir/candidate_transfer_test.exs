defmodule SymphonyElixir.CandidateTransferTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{CandidateGit, CandidateTransfer}

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

  defp fixture(opts \\ []) do
    root = Path.join(System.tmp_dir!(), "candidate-transfer-#{System.unique_integer([:positive])}")
    seed = Path.join(root, "seed")
    workspace = Path.join(root, "coordinator")
    worker = Path.join(root, "worker")
    validation_candidates = Path.join(root, "validation-candidates")
    File.mkdir_p!(seed)
    File.mkdir_p!(workspace)
    File.mkdir_p!(validation_candidates)
    File.chmod!(validation_candidates, 0o700)

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
      gate: %{evaluator: :candidate_validation}
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
        "validate" => validation_stage
      }
    }

    execution = %{
      worker_control: @control,
      validation_base: base_sha,
      validation_candidates: validation_candidates,
      candidate_producer_stage: "local-commit",
      candidate_producer_command_sha256: digest(Jason.encode!(@producer_command))
    }

    run = %{
      id: "run-candidate-transfer",
      task_id: "DEV-238",
      branch: "feature/candidate-transfer",
      workspace: workspace,
      definition: definition,
      execution: execution,
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
      validation_candidates: validation_candidates,
      base_sha: base_sha,
      base_tree_sha: base_tree_sha,
      bundle: bundle,
      metadata: metadata,
      run: run,
      validation_stage: validation_stage,
      identity: identity
    }
  end

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
