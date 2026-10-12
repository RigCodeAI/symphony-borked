defmodule SymphonyElixir.WorkstreamPublicationTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{Workstream, WorkstreamRun}

  setup do
    root = Path.join(System.tmp_dir!(), "symphony-workstream-publication-#{System.unique_integer([:positive])}")
    workstream_path = Path.join([root, "workstreams", "software-change.yml"])
    agent_path = Path.join([root, "agents", "worker.yml"])
    instruction_path = Path.join([root, "agents", "instructions.md"])

    File.mkdir_p!(Path.dirname(workstream_path))
    File.mkdir_p!(Path.dirname(agent_path))
    File.write!(instruction_path, "Implement the requested change.\n")
    File.write!(workstream_path, workstream_yaml())
    File.write!(agent_path, agent_yaml())

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, workstream_path: workstream_path, agent_path: agent_path, root: root}
  end

  test "loads a fixed service-owned publish stage and normalized validation gate", context do
    assert {:ok, definition} = load(context)

    assert definition.stages["publish"] == %{
             id: "publish",
             type: :publish,
             inputs: ["patch", "validation"],
             outputs: ["publication"],
             gate: %{
               validation_stage: "validate",
               required: [%{check: "unit-tests", assertion: "exit_status", equals: 0}],
               success: :complete,
               failure: :blocked
             }
           }
  end

  test "requires validation provenance among publication inputs", context do
    replace_in_file!(context.workstream_path, "inputs: [patch, validation]", "inputs: [patch]")

    assert {:error, {:publish_validation_output_not_input, "publish", "validate"}} = load(context)
  end

  test "requires a reachable candidate-validation check as the validation stage", context do
    replace_in_file!(context.workstream_path, "validation_stage: validate", "validation_stage: implement")
    assert {:error, {:invalid_publish_validation_stage, "publish", "implement"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "success: publish", "success: complete")

    assert {:error, {:publish_validation_stage_not_ancestor, "publish", "validate"}} = load(context)
  end

  test "publish failures are blocked and agent stages cannot transition directly to publish", context do
    replace_in_file!(context.workstream_path, "success: complete\n      failure: blocked", "success: complete\n      failure: {repair: implement, max_attempts: 1}")

    assert {:error, {:invalid_publish_gate_failure, "publish", _}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "next: validate", "next: publish")

    assert {:error, {:agent_must_transition_to_check, "implement", "publish"}} = load(context)
  end

  test "publish attempts use their own phase and block without verified publisher completion", context do
    assert {:ok, definition} = load(context)

    run =
      WorkstreamRun.new("task", definition, context.root,
        workspace_root: context.root,
        codex_command: "pinned",
        branch: "cycle/publication-test"
      )

    started =
      run
      |> Map.merge(%{status: :ready, stage_id: "publish"})
      |> WorkstreamRun.start_stage()

    assert started.phase == :publishing
    assert List.last(started.attempts).type == :publish

    finished = WorkstreamRun.finish_stage(started, {:ok, %{candidate_sha: String.duplicate("a", 40)}})

    assert finished.status == :blocked
    assert finished.phase == :blocked
    assert List.last(finished.attempts).gate == :failed
    assert List.last(finished.attempts).gate_detail["verdict"] == "failed"
    assert List.last(finished.attempts).gate_detail["verified"] == false
    assert is_list(List.last(finished.attempts).gate_detail["rationale"])
    refute Map.has_key?(finished.outputs, "publication")
  end

  defp load(context), do: Workstream.load(context.workstream_path, %{"request" => "publish candidate"})

  defp workstream_yaml do
    """
    version: 1
    name: software-change
    inputs: [request]
    agents:
      worker: ../agents/worker.yml
    entry: implement
    stages:
      - id: implement
        type: agent
        inputs: [request]
        outputs: [patch]
        agent: worker
        prompt: Implement the requested change.
        next: validate
      - id: validate
        type: check
        inputs: [patch]
        outputs: [validation]
        gate:
          evaluator: candidate_validation
          required:
            - {check: unit-tests, assertion: exit_status, equals: 0}
          success: publish
          failure: blocked
      - id: publish
        type: publish
        inputs: [patch, validation]
        outputs: [publication]
        gate:
          validation_stage: validate
          required:
            - {check: unit-tests, assertion: exit_status, equals: 0}
          success: complete
          failure: blocked
    """
  end

  defp agent_yaml do
    """
    version: 1
    name: Local Worker
    model: gpt-6.1-sol
    reasoning_effort: high
    daybreak: false
    approval_policy: never
    sandbox: workspace-write
    instructions: [instructions.md]
    skills: []
    """
  end

  defp replace_in_file!(path, expected, replacement) do
    source = File.read!(path)
    assert String.contains?(source, expected), "expected fixture to contain #{inspect(expected)}"
    File.write!(path, String.replace(source, expected, replacement, global: false))
  end
end
