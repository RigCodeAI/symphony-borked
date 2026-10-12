defmodule SymphonyElixir.WorkstreamChildDefinitionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Workstream

  @fixture_dir Path.expand("../fixtures/dev243_child", __DIR__)
  @fixture_files [
    "coverage-factory.yaml",
    "software-change.yaml",
    "researcher.yaml",
    "implementer.yaml",
    "researcher.md",
    "implementer.md",
    "skills/research/SKILL.md",
    "skills/implementation/SKILL.md"
  ]

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-child-definition-test-#{System.unique_integer([:positive])}"
      )

    Enum.each(@fixture_files, fn relative_path ->
      source = Path.join(@fixture_dir, relative_path)
      destination = Path.join(root, relative_path)
      File.mkdir_p!(Path.dirname(destination))
      File.cp!(source, destination)
    end)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok,
     root: root,
     parent_path: Path.join(root, "coverage-factory.yaml"),
     child_path: Path.join(root, "software-change.yaml"),
     researcher_instruction_path: Path.join(root, "researcher.md"),
     researcher_skill_path: Path.join(root, "skills/research/SKILL.md"),
     implementer_instruction_path: Path.join(root, "implementer.md"),
     implementer_skill_path: Path.join(root, "skills/implementation/SKILL.md")}
  end

  test "pins one software-change child and its agents, instructions, and skills", context do
    assert {:ok, loaded} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})

    child = loaded.stages["implement-gap"]
    assert child.type == :child
    assert child.workstream == "software-change.yaml"
    assert child.inputs == ["qualified-gap"]
    assert child.outputs == ["child-result"]
    assert child.input_mapping == %{"task" => "qualified-gap"}
    assert child.next == "re-evaluate"
    refute Map.has_key?(child, :child_inputs)

    child_definition = child.child_definition
    assert child_definition.name == "software-change"
    assert child_definition.inputs == ["task"]
    assert child_definition.input_values == %{"task" => {:mapped_parent_input, "qualified-gap"}}
    assert child_definition.stages["implement"].type == :agent
    assert child_definition.stages["inspect"].type == :check

    implementer = child_definition.agents["implementer"]

    assert hd(implementer.instructions).text ==
             "Prepare a synthetic candidate for loader contract coverage only.\n"

    assert hd(implementer.skills).text =~ "do not publish a candidate"
    assert loaded.definitions[context.child_path].sha256 == sha256(File.read!(context.child_path))
    assert loaded.definitions[context.implementer_instruction_path].text == hd(implementer.instructions).text
    assert loaded.definitions[context.implementer_skill_path].text == hd(implementer.skills).text

    assert loaded.stages["approval-before"].approval
    assert loaded.stages["re-evaluate"].type == :check
    assert loaded.stages["approval-after"].approval

    File.write!(context.implementer_instruction_path, "Changed after the parent load.\n")

    assert hd(child_definition.agents["implementer"].instructions).text ==
             "Prepare a synthetic candidate for loader contract coverage only.\n"
  end

  test "requires an exact mapping for the child's required inputs", context do
    replace_once!(
      context.parent_path,
      "    input_mapping:\n      task: qualified-gap\n",
      "    input_mapping: {}\n"
    )

    assert {:error, {:missing_child_input_mappings, "implement-gap", ["task"]}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})
  end

  test "rejects mapping keys the child does not declare", context do
    replace_once!(
      context.parent_path,
      "      task: qualified-gap\n",
      "      task: qualified-gap\n      unused: qualified-gap\n"
    )

    assert {:error, {:unknown_child_input_mappings, "implement-gap", ["unused"]}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})
  end

  test "requires the child stage inputs to match its mapped parent sources", context do
    replace_once!(context.parent_path, "      task: qualified-gap\n", "      task: request\n")

    assert {:error, {:child_stage_inputs_mismatch, "implement-gap", ["request"], ["qualified-gap"]}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})
  end

  test "requires a relative child workstream path", context do
    absolute_path = Path.join(context.root, "software-change.yaml")

    replace_once!(
      context.parent_path,
      "workstream: software-change.yaml",
      "workstream: #{absolute_path}"
    )

    assert {:error, {:invalid_child_workstream_path, "implement-gap", ^absolute_path}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})
  end

  test "rejects multiple children and nested child definitions", context do
    File.write!(
      context.parent_path,
      File.read!(context.parent_path) <>
        """
          - id: second-child
            type: child
            inputs: [qualified-gap]
            outputs: [second-child-result]
            workstream: software-change.yaml
            input_mapping:
              task: qualified-gap
            next: re-evaluate
        """
    )

    assert {:error, {:multiple_child_stages, ids}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})

    assert Enum.sort(ids) == ["implement-gap", "second-child"]

    File.write!(context.parent_path, File.read!(fixture_path_for_test(context)))

    replace_once!(
      context.child_path,
      "agents:\n  implementer: implementer.yaml\n",
      "agents:\n  missing: missing.yaml\n"
    )

    replace_once!(
      context.child_path,
      "stages:\n  - id: implement\n",
      "stages:\n  - id: nested\n    type: child\n    inputs: [task]\n    outputs: []\n    workstream: coverage-factory.yaml\n    input_mapping: {}\n    next: complete\n  - id: implement\n"
    )

    assert {:error, {:nested_child_workstream, "implement-gap"}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})
  end

  test "rejects a cycle, a non software-change child, and direct child completion", context do
    replace_once!(
      context.parent_path,
      "workstream: software-change.yaml",
      "workstream: coverage-factory.yaml"
    )

    assert {:error, {:child_workstream_cycle, "implement-gap", _path}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})

    File.write!(context.parent_path, File.read!(fixture_path_for_test(context)))
    replace_once!(context.child_path, "name: software-change", "name: another-workstream")

    assert {:error, {:invalid_child_workstream_name, "implement-gap", "another-workstream"}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})

    File.write!(context.child_path, File.read!(fixture_child_path_for_test(context)))

    replace_once!(
      context.parent_path,
      "next: re-evaluate\n  - id: re-evaluate",
      "next: complete\n  - id: re-evaluate"
    )

    assert {:error, {:child_must_transition_to_reevaluation, "implement-gap"}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})

    File.write!(context.parent_path, File.read!(fixture_path_for_test(context)))

    replace_once!(
      context.parent_path,
      "next: re-evaluate\n  - id: re-evaluate",
      "next: approval-after\n  - id: re-evaluate"
    )

    assert {:error, {:child_must_transition_to_check, "implement-gap", "approval-after"}} =
             Workstream.load(context.parent_path, %{"request" => "qualify artifact support"})
  end

  defp fixture_path_for_test(_context),
    do: Path.join(@fixture_dir, "coverage-factory.yaml")

  defp fixture_child_path_for_test(_context),
    do: Path.join(@fixture_dir, "software-change.yaml")

  defp replace_once!(path, old, new) do
    source = File.read!(path)
    assert String.contains?(source, old), "expected #{inspect(old)} in #{path}"
    File.write!(path, String.replace(source, old, new, global: false))
  end

  defp sha256(text), do: :crypto.hash(:sha256, text) |> Base.encode16(case: :lower)
end
