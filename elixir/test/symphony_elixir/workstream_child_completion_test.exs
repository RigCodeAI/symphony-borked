defmodule SymphonyElixir.WorkstreamChildCompletionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkstreamRun

  test "a service proof completes only the matching child attempt and resumes at re-evaluation" do
    {waiting, child, proof} = completed_handoff()
    prior_attempt = hd(waiting.attempts)

    assert {:ok, resumed} = WorkstreamRun.finish_child(waiting, child, proof)

    assert resumed.status == :ready
    assert resumed.phase == :queued
    assert resumed.stage_id == "re-evaluate"
    assert resumed.current_attempt_id == nil
    assert resumed.pending_wait == nil
    assert hd(resumed.attempts) == prior_attempt

    child_attempt = Enum.find(resumed.attempts, &(&1.id == waiting.current_attempt_id))
    assert child_attempt.status == :completed
    assert child_attempt.gate == :passed
    assert child_attempt.result.status == :ok
    assert child_attempt.result.evidence == json_value(envelope(child, proof))

    operation = resumed.operations[waiting.current_attempt_id]
    assert operation.status == :completed
    assert operation.completion_proof == proof
    assert resumed.outputs["child-result"] == json_value(envelope(child, proof))
    assert resumed.artifacts["child-result"].run_id == resumed.id
    assert resumed.artifacts["child-result"].attempt_id == waiting.current_attempt_id
    assert WorkstreamRun.compatible_policy?(resumed)
    assert {:error, :unowned_child_cancellation} = SymphonyElixir.WorkstreamChild.cancellation_target(%{resumed | status: :stopped}, child, resumed.child_handoff)
  end

  test "a merged flag without the exact child service proof cannot resume the parent" do
    {waiting, child, _proof} = completed_handoff()

    assert {:error, :invalid_child_service_proof} = WorkstreamRun.finish_child(waiting, child, %{merged: true})
    assert waiting.status == :waiting_for_child
    assert List.last(waiting.attempts).status == :waiting
    refute Map.has_key?(waiting.outputs, "child-result")
  end

  test "proof identities and retained artifacts must match the linked child" do
    {waiting, child, proof} = completed_handoff()

    invalid_proofs = [
      %{proof | version: 2},
      %{proof | repository: "other/repository"},
      %{proof | run_id: "other-run"},
      %{proof | task_id: "other-task"},
      %{proof | publication_id: String.duplicate("d", 64)},
      %{proof | publication_attempt_id: "other-attempt"},
      %{proof | published_head: String.duplicate("e", 40)},
      %{proof | pr: %{number: 999, url: "https://github.com/RigCodeAI/rig/pull/999"}},
      %{proof | merge: %{commit: "not-a-commit"}},
      %{proof | archive: %{}},
      %{proof | retained_outputs: %{}},
      %{proof | retained_outputs_complete: false},
      %{proof | outputs: %{}},
      %{proof | retained_outputs: Map.put(proof.retained_outputs, "candidate", %{output_id: "forged-artifact", run_id: child.id})}
    ]

    for invalid <- invalid_proofs do
      assert {:error, :invalid_child_service_proof} = WorkstreamRun.finish_child(waiting, child, invalid)
    end
  end

  test "stopped, stale, mismatched, or already-resumed handoffs cannot complete" do
    {waiting, child, proof} = completed_handoff()
    stale_parent = put_in(waiting.operations[waiting.current_attempt_id].status, :completed)

    assert {:error, :child_completion_not_current} = WorkstreamRun.finish_child(%{waiting | status: :stopped}, child, proof)
    assert {:error, :child_completion_not_current} = WorkstreamRun.finish_child(%{waiting | stage_id: "re-evaluate"}, child, proof)
    assert {:error, :child_completion_not_current} = WorkstreamRun.finish_child(stale_parent, child, proof)
    assert {:error, :child_result_identity_mismatch} = WorkstreamRun.finish_child(waiting, %{child | parent_run_id: "other-parent"}, proof)
    assert {:error, :child_policy_mismatch} = WorkstreamRun.finish_child(waiting, %{child | policy: %{}}, proof)
    assert {:error, :child_completion_not_current} = WorkstreamRun.finish_child(%{waiting | policy: %{}}, child, proof)

    assert {:ok, resumed} = WorkstreamRun.finish_child(waiting, child, proof)
    assert {:error, :child_completion_not_current} = WorkstreamRun.finish_child(resumed, child, proof)
  end

  defp completed_handoff do
    assert {:ok, waiting, linked, _intent} = WorkstreamRun.begin_child(parent(), child())
    publication_id = String.duplicate("c", 64)
    candidate_sha = String.duplicate("a", 40)
    merge_commit = String.duplicate("b", 40)
    publication_attempt_id = "#{linked.id}/publish/1"
    pr_url = "https://github.com/RigCodeAI/rig/pull/73"

    publication = %{
      "publication_id" => publication_id,
      "candidate_sha" => candidate_sha,
      "branch" => SymphonyElixir.Publisher.branch(linked),
      "pr" => %{"number" => 73, "url" => pr_url}
    }

    artifacts = %{"candidate" => "candidate-artifact", "publication" => "publication-artifact"}
    outputs = %{"task" => "isolated fixture", "candidate" => %{"fixture" => true}, "publication" => publication}

    retained =
      Map.new(artifacts, fn {name, artifact_id} ->
        {name,
         %{version: 2, kind: if(name == "publication", do: "publication", else: "inline"), output_id: artifact_id, run_id: linked.id, task_key: "test-only-task", sha256: String.duplicate("f", 64)}}
      end)

    attempt = %{
      id: publication_attempt_id,
      stage: "publish",
      type: :publish,
      ordinal: 1,
      status: :completed,
      result: %{status: :ok, evidence: publication},
      gate: :passed
    }

    completed_child =
      linked
      |> Map.merge(%{
        status: :complete,
        phase: :complete,
        stage_id: :complete,
        current_attempt_id: nil,
        artifacts: artifacts,
        outputs: outputs,
        attempts: [attempt],
        operations: %{
          publication_attempt_id => %{
            id: publication_attempt_id,
            stage: "publish",
            type: :publish,
            status: :completed,
            publication_intent: %{publication_id: publication_id}
          }
        },
        retained_output_artifacts: retained
      })

    # This synthetic SERVICE-shaped proof covers only the pure transition contract;
    # it is not evidence of production merge or archive qualification.
    proof = %{
      version: 1,
      run_id: linked.id,
      task_id: linked.task_id,
      publication_id: publication_id,
      publication_attempt_id: publication_attempt_id,
      repository: "RigCodeAI/rig",
      pr: %{number: 73, url: pr_url},
      published_head: candidate_sha,
      merge: %{commit: merge_commit, merged_at: "2026-10-12T12:00:00Z", merged_by: %{"id" => 42, "login" => "test-owner", "type" => "User"}},
      archive: %{revision: "synthetic-test-archive"},
      retained_outputs: retained,
      retained_outputs_complete: true,
      outputs: Map.new(artifacts, fn {name, artifact_id} -> {name, %{"version" => 2, "kind" => retained[name].kind, "output_id" => artifact_id, "value" => outputs[name]}} end)
    }

    {waiting, completed_child, proof}
  end

  defp envelope(child, proof) do
    %{
      child_run_id: child.id,
      merge: proof.merge,
      archive: proof.archive,
      retained_outputs: proof.retained_outputs,
      outputs: proof.outputs
    }
  end

  defp parent do
    definition = %{
      name: "test-only-child-parent",
      entry: "capability-fix",
      inputs: ["approved-gap"],
      input_values: %{"approved-gap" => "isolated fixture"},
      definitions: %{},
      agents: %{},
      stages: %{
        "capability-fix" => %{
          id: "capability-fix",
          type: :child,
          inputs: ["approved-gap"],
          outputs: ["child-result"],
          input_mapping: %{"task" => "approved-gap"},
          child_definition: %{inputs: ["task"]},
          next: "re-evaluate"
        },
        "re-evaluate" => %{id: "re-evaluate", type: :check, inputs: ["child-result"], outputs: ["evaluation"]}
      }
    }

    run =
      WorkstreamRun.new("test-only-parent-task", definition, "/isolated/parent",
        workspace_root: "/isolated",
        branch: "cycle/test-parent",
        codex_command: "never-executed"
      )

    prior_attempt = %{id: "prior-parent-attempt", stage: "prepare", type: :agent, ordinal: 1, status: :completed, result: %{status: :ok}, gate: :passed}
    %{run | attempts: [prior_attempt], operations: %{prior_attempt.id => %{id: prior_attempt.id, status: :completed}}}
  end

  defp child do
    definition = %{
      name: "software-change",
      entry: "implement",
      inputs: ["task"],
      input_values: %{"task" => "isolated fixture"},
      definitions: %{},
      agents: %{},
      stages: %{
        "implement" => %{id: "implement", type: :agent},
        "publish" => %{id: "publish", type: :publish}
      }
    }

    WorkstreamRun.new("test-only-child-task", definition, "/isolated/child",
      workspace_root: "/isolated",
      branch: "cycle/test-child",
      codex_command: "never-executed"
    )
  end

  defp json_value(value), do: value |> Jason.encode!() |> Jason.decode!()
end
