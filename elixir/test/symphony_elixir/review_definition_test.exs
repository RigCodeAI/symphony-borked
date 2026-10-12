defmodule SymphonyElixir.ReviewDefinitionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Workstream

  setup do
    root = Path.join(System.tmp_dir!(), "review-definition-#{System.unique_integer([:positive])}")
    workstream_path = Path.join([root, "workstreams", "review.yml"])
    cloud_path = Path.join([root, "agents", "cloud.yml"])
    alternate_cloud_path = Path.join([root, "agents", "alternate.yml"])
    reviewer_path = Path.join([root, "agents", "reviewer.yml"])
    File.mkdir_p!(Path.dirname(workstream_path))
    File.mkdir_p!(Path.join([root, "agents", "instructions"]))
    File.mkdir_p!(Path.join([root, "agents", "skills", "work"]))
    File.write!(workstream_path, review_workstream())
    File.write!(cloud_path, agent("Cloud implementer"))
    File.write!(alternate_cloud_path, agent("Alternate Cloud implementer"))
    File.write!(reviewer_path, agent("Review agent"))
    File.write!(Path.join([root, "agents", "instructions", "work.md"]), "Work instructions.\n")
    File.write!(Path.join([root, "agents", "skills", "work", "SKILL.md"]), "Work skill.\n")

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, workstream_path: workstream_path, cloud_path: cloud_path}
  end

  test "loads review stages and permits a bounded repair through the normal success path", context do
    assert {:ok, definition} = Workstream.load(context.workstream_path, %{"request" => "task"})

    assert definition.stages["review"].review_checkout_stage == "checkout"
    assert Workstream.reviewer?(definition.stages["review"])
    refute Workstream.reviewer?(definition.stages["implement"])

    assert definition.stages["checkout"].gate == %{
             evaluator: :review_checkout,
             validation_stage: "ci",
             success: "review",
             failure: :blocked
           }

    assert definition.stages["review-result"].gate == %{
             evaluator: :review_publication,
             validation_stage: "ci",
             review_stage: "review",
             checkout_stage: "checkout",
             success: :complete,
             failure: %{repair: "implement", max_attempts: 3}
           }

    refute definition.stages["implement"].next == "review-result"
  end

  test "requires review validation to reference a GitHub validation stage", context do
    replace(context.workstream_path, "validation_stage: ci", "validation_stage: validate")

    assert {:error, {:invalid_review_validation_stage, "checkout", "validate"}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "checkout, reviewer, and publication references must agree", context do
    replace(context.workstream_path, "success: review\n      failure: blocked", "success: implement\n      failure: blocked")

    assert {:error, {:invalid_review_checkout_successor, "checkout", "implement"}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})

    File.write!(context.workstream_path, review_workstream())
    replace(context.workstream_path, "checkout_stage: checkout", "checkout_stage: validate")

    assert {:error, _} = Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "review repair cannot target the reviewer or reuse its agent definition", context do
    replace(context.workstream_path, "repair: implement", "repair: review")

    assert {:error, {:review_repair_target_must_be_cloud_agent, "review-result", "review"}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})

    File.write!(context.workstream_path, review_workstream())
    replace(context.workstream_path, "reviewer: ../agents/reviewer.yml", "reviewer: ../agents/cloud.yml")

    assert {:error, {:review_repair_agent_must_differ_from_reviewer, "review-result", "implement"}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "review repair preserves the entry Cloud Agent definition across a multi-agent path", context do
    replace(
      context.workstream_path,
      "implementer: ../agents/cloud.yml",
      "implementer: ../agents/cloud.yml\n  entry-alias: ../agents/cloud.yml\n  alternate: ../agents/alternate.yml"
    )

    replace(
      context.workstream_path,
      "success: publish\n      failure: blocked\n  - id: publish",
      "success: alternate-implement\n      failure: blocked\n  - id: alternate-implement\n    type: agent\n    inputs: [validated]\n    outputs: [patch2]\n    agent: alternate\n    prompt: Continue with the alternate Cloud Agent.\n    next: validate-2\n  - id: validate-2\n    type: check\n    inputs: [patch2]\n    outputs: [validated2]\n    gate:\n      evaluator: candidate_validation\n      required:\n        - {check: contract, assertion: exit_status, equals: 0}\n      success: publish\n      failure: blocked\n  - id: publish"
    )

    replace(context.workstream_path, "repair: implement", "repair: alternate-implement")

    replace(
      context.workstream_path,
      "  - id: publish\n    type: publish\n    inputs: [validated]\n    outputs: [publication]\n    gate:\n      validation_stage: validate",
      "  - id: publish\n    type: publish\n    inputs: [validated2]\n    outputs: [publication]\n    gate:\n      validation_stage: validate-2"
    )

    replace(context.workstream_path, "inputs: [publication, validated]", "inputs: [publication, validated2]")
    replace(context.workstream_path, "evaluator: github_validation\n      validation_stage: validate", "evaluator: github_validation\n      validation_stage: validate-2")

    replace(context.workstream_path, "agent: alternate", "agent: entry-alias")
    assert {:ok, definition} = Workstream.load(context.workstream_path, %{"request" => "task"})
    assert definition.agents[definition.stages["alternate-implement"].agent].revision == definition.agents[definition.stages["implement"].agent].revision

    replace(context.workstream_path, "agent: entry-alias", "agent: alternate")

    assert {:error, {:review_repair_agent_must_preserve_entry_definition, "review-result", "alternate-implement"}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "review gates reject unsupported fields, unbounded attempts, and checkout repair", context do
    append_to_stage_gate(context.workstream_path, "checkout", "      extra: true\n")

    assert {:error, {:invalid_definition_fields, {:gate, "checkout"}, [], ["extra"]}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})

    File.write!(context.workstream_path, review_workstream())
    replace(context.workstream_path, "max_attempts: 3", "max_attempts: 4")

    assert {:error, {:invalid_gate_repair_attempts, "review-result", 4}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})

    File.write!(context.workstream_path, review_workstream())
    replace(context.workstream_path, "failure: blocked\n  - id: review", "failure: {repair: implement, max_attempts: 1}\n  - id: review")

    assert {:error, {:invalid_review_gate_failure, "checkout", _}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  defp review_workstream do
    """
    version: 1
    name: review-definition
    inputs: [request]
    agents:
      implementer: ../agents/cloud.yml
      reviewer: ../agents/reviewer.yml
    entry: implement
    stages:
      - id: implement
        type: agent
        inputs: [request]
        outputs: [patch]
        agent: implementer
        prompt: Implement the change.
        next: validate
      - id: validate
        type: check
        inputs: [patch]
        outputs: [validated]
        gate:
          evaluator: candidate_validation
          required:
            - {check: contract, assertion: exit_status, equals: 0}
          success: publish
          failure: blocked
      - id: publish
        type: publish
        inputs: [validated]
        outputs: [publication]
        gate:
          validation_stage: validate
          required:
            - {check: contract, assertion: exit_status, equals: 0}
          success: ci
          failure: blocked
      - id: ci
        type: check
        inputs: [publication, validated]
        outputs: [ci-result]
        gate:
          evaluator: github_validation
          validation_stage: validate
          required:
            - {check: contract, assertion: exit_status, equals: 0}
          success: checkout
          failure: blocked
      - id: checkout
        type: check
        inputs: [ci-result]
        outputs: [review-checkout]
        gate:
          evaluator: review_checkout
          validation_stage: ci
          success: review
          failure: blocked
      - id: review
        type: agent
        inputs: [review-checkout]
        outputs: [review-findings]
        agent: reviewer
        prompt: Review this checkout.
        review_checkout_stage: checkout
        next: review-result
      - id: review-result
        type: check
        inputs: [review-findings]
        outputs: [published-review]
        gate:
          evaluator: review_publication
          validation_stage: ci
          review_stage: review
          checkout_stage: checkout
          success: complete
          failure:
            repair: implement
            max_attempts: 3
    """
  end

  defp agent(name) do
    """
    version: 1
    name: #{name}
    model: gpt-6.1-sol
    reasoning_effort: high
    daybreak: false
    approval_policy: never
    sandbox: workspace-write
    instructions: [instructions/work.md]
    skills: [skills/work/SKILL.md]
    """
  end

  defp replace(path, expected, replacement) do
    text = File.read!(path)
    assert String.contains?(text, expected), "expected fixture to include #{inspect(expected)}"
    File.write!(path, String.replace(text, expected, replacement, global: false))
  end

  defp append_to_stage_gate(path, stage_id, contents) do
    text = File.read!(path)
    marker = "  - id: #{stage_id}\n"
    assert String.contains?(text, marker)
    [before, after_stage] = String.split(text, marker, parts: 2)
    [stage, rest] = String.split(after_stage, "  - id: ", parts: 2)
    gate_marker = "      failure: blocked\n"
    assert String.contains?(stage, gate_marker)
    File.write!(path, before <> marker <> String.replace(stage, gate_marker, gate_marker <> contents) <> "  - id: " <> rest)
  end
end
