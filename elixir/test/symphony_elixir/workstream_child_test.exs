defmodule SymphonyElixir.WorkstreamChildTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkstreamChild

  # Internal service identity fixtures only. No oracle, scorer or merged-result
  # receipt is invented here, and these maps cannot execute a production child.
  test "input mapping forwards only declared values and rejects missing or extra required bindings" do
    stage = %{input_mapping: %{"task" => "approved-gap"}, child_definition: %{inputs: ["task"]}}
    outputs = %{"approved-gap" => %{"change" => "isolated fixture"}, "private-oracle" => "never forwarded"}
    assert {:ok, %{"task" => %{"change" => "isolated fixture"}}} = WorkstreamChild.inputs(stage, outputs)
    assert {:error, :missing_child_input} = WorkstreamChild.inputs(stage, %{})
    assert {:error, :missing_child_input} = WorkstreamChild.inputs(stage, %{"approved-gap" => nil})
    assert {:error, :invalid_child_input_mapping} = WorkstreamChild.inputs(%{stage | input_mapping: %{}}, outputs)
    assert {:error, :invalid_child_input_mapping} = WorkstreamChild.inputs(%{stage | input_mapping: %{"task" => "approved-gap", "extra" => "private-oracle"}}, outputs)
  end

  test "repeated preparation discards fresh random child IDs and keeps the same handoff" do
    parent = parent()
    child = child()
    assert {:ok, intent, linked} = WorkstreamChild.prepare(parent, child)
    regenerated = child |> Map.put(:id, "another-random-id") |> Map.put(:created_at, "later")
    assert {:ok, repeated, same_child} = WorkstreamChild.prepare(parent, regenerated)

    assert repeated == intent
    assert same_child.id == linked.id
    assert same_child.parent_handoff == linked.parent_handoff
    assert linked.parent_run_id == parent.id
    assert linked.id != child.id
    assert Regex.match?(~r/\Arun-[0-9a-f]{32}\z/, linked.id)
    assert intent.parent_attempt_id == "parent/capability-fix/1"
    assert :ok = WorkstreamChild.replay(intent, repeated)
    assert linked.status == :ready
    assert linked.attempts == []
    assert linked.operations == %{}
  end

  test "input, definition, runtime policy and approved artifact changes conflict with the saved intent" do
    parent = parent()
    child = child()
    assert {:ok, saved, _} = WorkstreamChild.prepare(parent, child)

    for {changed_parent, changed_child} <- [
          {put_in(parent.outputs["approved-gap"], "different approved change"),
           child |> put_in([:definition, :input_values, "task"], "different approved change") |> put_in([:outputs, "task"], "different approved change")},
          {parent, put_in(child.definition.definitions["software-change"].sha256, "changed-definition")},
          {parent, put_in(child.execution.codex_command, "different-executor")},
          {parent, put_in(child.policy.version, 2)},
          {put_in(parent.artifacts["approved-gap"].sha256, "changed-artifact"), child},
          {%{parent | task_id: "changed-parent-task"}, child},
          {put_in(parent.policy.version, 2), child}
        ] do
      assert {:ok, changed, _} = WorkstreamChild.prepare(changed_parent, changed_child)
      assert changed.handoff_id == saved.handoff_id
      assert changed.child_run_id == saved.child_run_id
      assert {:error, :child_handoff_inputs_changed} = WorkstreamChild.replay(saved, changed)
    end

    assert {:error, :child_inputs_not_bound_to_parent} = WorkstreamChild.prepare(put_in(parent.outputs["approved-gap"], "changed consumed input"), child)
  end

  test "a different parent or stage attempt cannot reuse another handoff identity" do
    parent = parent()
    assert {:ok, saved, _} = WorkstreamChild.prepare(parent, child())

    for changed <- [%{parent | id: "another-parent"}, %{parent | attempts: [%{id: "prior-attempt"}]}] do
      assert {:ok, proposed, _} = WorkstreamChild.prepare(changed, child())
      refute proposed.handoff_id == saved.handoff_id
      assert {:error, :child_handoff_identity_mismatch} = WorkstreamChild.replay(saved, proposed)
    end
  end

  test "result correlation requires every lineage field and does not advance the parent" do
    parent = parent()
    assert {:ok, intent, _} = WorkstreamChild.prepare(parent, child())
    identity = Map.take(intent, [:handoff_id, :parent_run_id, :parent_stage_id, :parent_attempt_id, :child_run_id, :fingerprint])
    assert :ok = WorkstreamChild.correlate(intent, identity)

    for key <- Map.keys(identity) do
      assert {:error, :child_result_identity_mismatch} = WorkstreamChild.correlate(intent, Map.put(identity, key, "wrong"))
      assert {:error, :child_result_identity_mismatch} = WorkstreamChild.correlate(intent, Map.delete(identity, key))
    end

    assert parent.status == :ready
    assert parent.attempts == []
    assert {:error, :child_result_identity_mismatch} = WorkstreamChild.correlate(intent, %{status: :complete})
  end

  test "a stopped parent names only the child linked by its saved intent for cancellation" do
    parent = parent()
    assert {:ok, intent, linked} = WorkstreamChild.prepare(parent, child())

    stopped =
      parent
      |> Map.put(:status, :stopped)
      |> Map.put(:child_handoff, intent)
      |> Map.put(:current_attempt_id, intent.parent_attempt_id)
      |> Map.put(:attempts, [%{id: intent.parent_attempt_id, type: :child, status: :waiting}])
      |> Map.put(:operations, %{intent.parent_attempt_id => %{type: :child, status: :waiting_for_child, child_handoff: intent}})

    assert {:ok, id} = WorkstreamChild.cancellation_target(stopped, linked, intent)
    assert id == linked.id
    assert {:ok, ^id} = WorkstreamChild.cancellation_target(stopped, linked, intent)
    assert {:error, :unowned_child_cancellation} = WorkstreamChild.cancellation_target(parent, linked, intent)
    assert {:error, :unowned_child_cancellation} = WorkstreamChild.cancellation_target(%{parent | status: :stopped}, linked, intent)
    assert {:error, :unowned_child_cancellation} = WorkstreamChild.cancellation_target(stopped, child(), intent)
    wrong_link = put_in(linked.parent_handoff.parent_attempt_id, "unrelated-attempt")
    assert {:error, :unowned_child_cancellation} = WorkstreamChild.cancellation_target(stopped, wrong_link, intent)
    assert {:error, :unowned_child_cancellation} = WorkstreamChild.cancellation_target(stopped, Map.delete(linked, :parent_run_id), intent)
    assert {:error, :unowned_child_cancellation} = WorkstreamChild.cancellation_target(%{stopped | current_attempt_id: nil}, linked, intent)
    assert {:error, :unowned_child_cancellation} = WorkstreamChild.cancellation_target(put_in(stopped.operations[intent.parent_attempt_id].status, :completed), linked, intent)
    assert linked.status == :ready
  end

  test "shared task, branch or workspace ownership and non-fresh children are rejected" do
    parent = parent()
    child = child()

    for key <- [:task_id, :workspace, :branch] do
      assert {:error, :child_ownership_overlaps_parent} = WorkstreamChild.prepare(parent, Map.put(child, key, parent[key]))
    end

    assert {:error, :child_ownership_overlaps_parent} = WorkstreamChild.prepare(parent, %{child | workspace: parent.workspace <> "/nested"})
    assert {:error, :child_ownership_overlaps_parent} = WorkstreamChild.prepare(parent, %{child | workspace: parent.workspace <> "/../parent"})

    for changed <- [
          %{child | attempts: [%{id: "already-started"}]},
          %{child | stage_id: "different-stage"},
          put_in(child.outputs["task"], "not the pinned input"),
          Map.put(child, :parent_handoff, %{}),
          Map.put(child, :parent_run_id, "other-parent"),
          put_in(child.definition.name, "coverage-factory"),
          put_in(child.definition.stages, %{"nested" => %{type: :child}})
        ] do
      assert {:error, :child_not_fresh_software_change} = WorkstreamChild.prepare(parent, changed)
    end
  end

  test "a non-ready parent and runtime-only input values cannot plan a durable handoff" do
    assert {:error, :parent_not_ready_for_child} = WorkstreamChild.prepare(%{parent() | status: :stopped}, child())
    assert {:error, :parent_not_ready_for_child} = WorkstreamChild.prepare(Map.put(parent(), :parent_handoff, %{}), child())
    assert {:error, :nonportable_child_inputs} = WorkstreamChild.prepare(parent(), %{child() | execution: %{callback: fn -> :ok end}})
    assert {:error, :invalid_child_handoff} = WorkstreamChild.prepare(nil, child())
    assert {:error, :invalid_child_handoff} = WorkstreamChild.replay(%{}, %{})
  end

  defp parent do
    %{
      id: "parent",
      task_id: "coverage-parent",
      workspace: "/isolated/parent",
      branch: "cycle/parent",
      status: :ready,
      stage_id: "capability-fix",
      current_attempt_id: nil,
      attempts: [],
      outputs: %{"approved-gap" => "approved change"},
      policy: %{version: 1},
      artifacts: %{"approved-gap" => %{sha256: "test-only-artifact"}},
      definition: %{
        definitions: %{"parent" => %{sha256: "pinned-parent-definition"}},
        stages: %{
          "capability-fix" => %{
            type: :child,
            inputs: ["approved-gap"],
            outputs: ["child-result"],
            input_mapping: %{"task" => "approved-gap"},
            child_definition: %{inputs: ["task"]}
          }
        }
      }
    }
  end

  defp child do
    %{
      id: "fresh-random-id",
      task_id: "distinct-approved-change",
      issue_id: nil,
      workspace: "/isolated/child",
      branch: "cycle/child",
      status: :ready,
      current_attempt_id: nil,
      attempts: [],
      operations: %{},
      artifacts: %{},
      stage_id: "implement",
      outputs: %{"task" => "approved change"},
      policy: %{version: 1},
      execution: %{codex_command: "pinned-executor"},
      definition: %{
        name: "software-change",
        entry: "implement",
        input_values: %{"task" => "approved change"},
        definitions: %{"software-change" => %{sha256: "pinned-child-definition"}},
        stages: %{"implement" => %{type: :agent}}
      }
    }
  end
end
