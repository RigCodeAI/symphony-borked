defmodule SymphonyElixir.Dev243ApprovalBindingTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{WorkstreamRun, WorkstreamStore}

  # This is a durable-wait harness, not a production Coverage Factory definition.
  # It tests revision binding; it does not certify security/release-owner identity.
  setup do
    root = Path.join(System.tmp_dir!(), "dev243-approval-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "a restored approval wait retains every artifact and requires an explicit approval", c do
    waiting = waiting_run(c.root)
    loaded = round_trip(c.root, waiting, "initial-wait")
    wait = loaded.pending_wait

    assert loaded == waiting
    assert loaded.status == :waiting_for_answer
    assert wait.artifact_ids == loaded.artifacts
    assert {:error, :explicit_approval_required} = WorkstreamRun.linear_reply(loaded, "looks good", "ordinary-reply")
    assert {:error, :explicit_approval_required} = WorkstreamRun.wait_answer(loaded, wait.id, %{"security-approval" => true})

    assert {:ok, approved} = WorkstreamRun.linear_reply(loaded, approval(wait), "security-activity")
    assert approved.status == :ready
    assert approved.stage_id == "support-approval"
    assert approved.outputs["security-approval"] == true

    evidence = List.last(approved.attempts).result.evidence
    assert evidence.artifact_ids == wait.artifact_ids
    assert evidence.artifact_revision == wait.approval_digest
  end

  test "changed scenario, oracle, stack and specimen digests invalidate a saved approval after restart", c do
    for name <- ["scenario", "oracle", "stack", "vulnerable-specimen", "safe-specimen"] do
      waiting = waiting_run(c.root)
      wait = waiting.pending_wait
      changed = put_in(waiting.artifacts[name].sha256, String.duplicate("f", 64))
      loaded = round_trip(Path.join(c.root, name), changed, "changed-artifact")

      assert loaded.pending_wait == wait
      assert {:error, :stale_artifact_revision} = WorkstreamRun.linear_reply(loaded, approval(wait), "stale-activity")
      assert loaded.status == :waiting_for_answer
      refute Map.has_key?(loaded.outputs, "security-approval")
      assert List.last(loaded.attempts).status == :waiting
    end
  end

  test "removing an artifact or adding a new one invalidates the restored approval set", c do
    waiting = waiting_run(c.root)
    wait = waiting.pending_wait
    artifact = waiting.artifacts["scenario"]

    for {label, artifacts} <- [
          {"removed", Map.delete(waiting.artifacts, "safe-specimen")},
          {"added", Map.put(waiting.artifacts, "new-evidence", artifact)}
        ] do
      loaded = round_trip(Path.join(c.root, label), %{waiting | artifacts: artifacts}, label)
      assert {:error, :stale_artifact_revision} = WorkstreamRun.linear_reply(loaded, approval(wait), "stale-#{label}")
      assert loaded.pending_wait.artifact_ids == wait.artifact_ids
      assert loaded.artifacts == artifacts
    end
  end

  test "security approval does not satisfy the next approval and replay stays idempotent across restart", c do
    waiting = waiting_run(c.root)
    security_wait = waiting.pending_wait
    assert {:ok, approved} = WorkstreamRun.linear_reply(waiting, approval(security_wait), "security-activity")

    support_waiting = approved |> WorkstreamRun.start_stage() |> then(&round_trip(c.root, &1, "support-wait"))
    support_wait = support_waiting.pending_wait

    assert support_wait.id != security_wait.id
    assert support_wait.approval_digest != security_wait.approval_digest
    assert {:ok, duplicate} = WorkstreamRun.linear_reply(support_waiting, approval(security_wait), "security-activity")
    assert duplicate == support_waiting
    assert {:error, :stale_or_missing_approval} = WorkstreamRun.linear_reply(support_waiting, approval(security_wait), "new-activity")
    refute Map.has_key?(support_waiting.outputs, "support-approval")

    assert {:ok, completed} = WorkstreamRun.linear_reply(support_waiting, approval(support_wait), "support-activity")
    completed = round_trip(c.root, completed, "support-approved")
    assert completed.status == :complete
    assert completed.outputs["support-approval"] == true
    assert {:ok, duplicate} = WorkstreamRun.linear_reply(completed, approval(support_wait), "support-activity")
    assert duplicate == completed
    assert length(completed.attempts) == 4

    [security, support] = Enum.filter(completed.attempts, &(&1.type == :human_wait))
    assert security.result.evidence.artifact_revision == security_wait.approval_digest
    assert support.result.evidence.artifact_revision == support_wait.approval_digest
  end

  defp round_trip(root, run, event) do
    path = Path.join(root, "state/runs.sqlite")
    store = start_store(path)
    assert :ok = WorkstreamStore.commit(store, run, event, %{kind: :test_only_snapshot})
    assert :ok = GenServer.stop(store)

    reopened = start_store(path)
    assert {:ok, loaded} = WorkstreamStore.fetch(reopened, run.id)
    assert {:ok, %{run_id: run_id}} = WorkstreamStore.event(reopened, event)
    assert run_id == run.id
    assert :ok = GenServer.stop(reopened)
    loaded
  end

  defp start_store(path) do
    assert {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    on_exit(fn -> if Process.alive?(store), do: GenServer.stop(store) end)
    store
  end

  defp approval(wait), do: "approve #{wait.id} #{wait.approval_digest}"

  defp waiting_run(workspace) do
    WorkstreamRun.new("dev243-test-only", definition(), workspace,
      workspace_root: workspace,
      codex_command: "test-only-never-executed",
      branch: "cycle/test-only"
    )
    |> WorkstreamRun.start_stage()
    |> WorkstreamRun.finish_stage({:ok, %{test_only: "synthetic artifacts"}})
    |> WorkstreamRun.start_stage()
    |> WorkstreamRun.finish_stage({:ok, %{exit_status: 0, timed_out: false, output: "test-only qualification"}})
    |> WorkstreamRun.start_stage()
  end

  defp definition do
    %{
      version: 1,
      name: "dev243-test-only-approvals",
      inputs: ["task"],
      input_values: %{"task" => "synthetic approval binding"},
      agents: %{},
      definitions: %{},
      entry: "prepare",
      stages: %{
        "prepare" => %{
          id: "prepare",
          type: :agent,
          outputs: ["scenario", "oracle", "stack", "vulnerable-specimen", "safe-specimen"],
          next: "qualify"
        },
        "qualify" => %{
          id: "qualify",
          type: :check,
          outputs: ["qualification"],
          gate: %{success: "security-approval", failure: :blocked}
        },
        "security-approval" => approval_stage("security-approval", "support-approval"),
        "support-approval" => approval_stage("support-approval", :complete)
      }
    }
  end

  defp approval_stage(id, next) do
    %{
      id: id,
      type: :human_wait,
      approval: true,
      inputs: ["qualification"],
      outputs: [id],
      prompt: "Test-only approval of pinned artifacts",
      next: next
    }
  end
end
