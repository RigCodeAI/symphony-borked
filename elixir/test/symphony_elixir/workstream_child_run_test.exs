defmodule SymphonyElixir.WorkstreamChildRunTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{WorkstreamChild, WorkstreamRun}

  test "beginning a child stage leaves one waiting parent attempt and a separately ready child" do
    parent = parent()
    child = child()
    assert {:ok, waiting, linked, intent} = WorkstreamRun.begin_child(parent, child)

    assert waiting.status == :waiting_for_child
    assert waiting.phase == :waiting_for_child
    assert waiting.current_attempt_id == intent.parent_attempt_id
    assert waiting.child_handoff == intent
    assert [%{id: attempt_id, status: :waiting, result: nil, type: :child, candidate_context_id: nil}] = waiting.attempts
    assert attempt_id == intent.parent_attempt_id

    operation = waiting.operations[attempt_id]
    assert operation.worker == nil
    assert operation.status == :waiting_for_child
    assert operation.side_effect_id == intent.handoff_id
    assert operation.child_handoff == intent
    assert linked.status == :ready
    assert linked.attempts == []
    assert linked.operations == %{}
    assert waiting.outputs == parent.outputs
    assert waiting.artifacts == parent.artifacts
    refute Map.has_key?(waiting.outputs, "child-result")
    assert WorkstreamRun.compatible_policy?(waiting)
  end

  test "a saved waiting parent cannot create a second child even from a fresh child preparation" do
    assert {:ok, waiting, _, _} = WorkstreamRun.begin_child(parent(), child())
    assert {:error, :parent_not_ready_for_child} = WorkstreamRun.begin_child(waiting, child())
    assert length(waiting.attempts) == 1
    assert map_size(waiting.operations) == 1
  end

  test "matching result identity and child completion do not by themselves finish the parent stage" do
    assert {:ok, waiting, linked, intent} = WorkstreamRun.begin_child(parent(), child())
    completed_child = %{linked | status: :complete}
    assert :ok = WorkstreamChild.correlate(intent, intent)
    assert_raise FunctionClauseError, fn -> WorkstreamRun.finish_stage(waiting, {:ok, %{merged: true, child_run_id: linked.id}}) end
    assert waiting.status == :waiting_for_child
    assert List.last(waiting.attempts).result == nil
    refute Map.has_key?(waiting.outputs, "child-result")

    stopped = %{waiting | status: :stopped}
    assert {:ok, id} = WorkstreamChild.cancellation_target(stopped, completed_child, intent)
    assert id == completed_child.id
    assert stopped.status == :stopped
  end

  test "merge proof composition rejects absent or incomplete child output retention before any service query" do
    assert {:ok, waiting, linked, _intent} = WorkstreamRun.begin_child(parent(), child())
    completed = %{linked | status: :complete}
    assert {:error, :child_outputs_not_retained} = WorkstreamChild.completion_proof(waiting, completed)

    completed = %{completed | artifacts: %{"candidate" => %{id: "candidate-artifact"}}, outputs: %{"candidate" => %{"count" => 1}}}
    assert {:error, :child_outputs_not_retained} = WorkstreamChild.completion_proof(waiting, Map.put(completed, :retained_output_artifacts, %{}))

    mismatched = Map.put(completed, :retained_output_artifacts, %{"candidate" => %{output_id: %{id: "different-artifact"}}})
    assert {:error, :child_outputs_not_retained} = WorkstreamChild.completion_proof(waiting, mismatched)
    assert waiting.status == :waiting_for_child
  end

  test "merge proof composition cannot consume a stopped parent or stale child lineage" do
    assert {:ok, waiting, linked, _intent} = WorkstreamRun.begin_child(parent(), child())
    completed = %{linked | status: :complete}
    assert {:error, :child_completion_not_current} = WorkstreamChild.completion_proof(%{waiting | status: :stopped}, completed)
    assert {:error, :child_result_identity_mismatch} = WorkstreamChild.completion_proof(waiting, put_in(completed.parent_handoff.fingerprint, String.duplicate("f", 64)))
    assert {:error, :child_completion_not_current} = WorkstreamChild.completion_proof(waiting, Map.delete(completed, :parent_run_id))
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

    WorkstreamRun.new("test-only-parent", definition, "/isolated/parent",
      workspace_root: "/isolated",
      branch: "cycle/test-parent",
      codex_command: "never-executed"
    )
  end

  defp child do
    definition = %{
      name: "software-change",
      entry: "implement",
      inputs: ["task"],
      input_values: %{"task" => "isolated fixture"},
      definitions: %{},
      agents: %{},
      stages: %{"implement" => %{type: :agent}}
    }

    WorkstreamRun.new("test-only-child", definition, "/isolated/child",
      workspace_root: "/isolated",
      branch: "cycle/test-child",
      codex_command: "never-executed"
    )
  end
end
