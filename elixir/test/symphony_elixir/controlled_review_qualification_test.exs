defmodule SymphonyElixir.ControlledReviewQualificationTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{ControlledReviewQualification, Workstream}

  @repo_root Path.expand("../../..", __DIR__)
  @source_yaml Path.join(@repo_root, "factory/workstreams/software-change.yaml")

  setup do
    root = Path.join(System.tmp_dir!(), "controlled-review-qualification-#{System.unique_integer([:positive])}")
    release_factory_dir = Path.join(root, "releases/fixture/factory")
    workstream_dir = Path.join(release_factory_dir, "workstreams")
    qualification_dir = Path.join(root, "qualification")
    File.mkdir_p!(workstream_dir)
    File.mkdir_p!(qualification_dir)
    File.cp!(@source_yaml, Path.join(workstream_dir, "software-change.yaml"))

    for directory <- ["agents", "instructions", "skills"] do
      File.cp_r!(Path.join(@repo_root, "factory/#{directory}"), Path.join(release_factory_dir, directory))
    end

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, root: root, source_path: Path.join(workstream_dir, "software-change.yaml"), qualification_dir: qualification_dir}
  end

  test "loads and pins the one exact prompt-only successor", context do
    old = load_old!(context.source_path)
    new_path = write_successor!(context, old)

    assert {:ok, successor, proof} = ControlledReviewQualification.successor_definition(old, new_path)
    assert ControlledReviewQualification.equivalent?(old, successor)
    assert ControlledReviewQualification.valid_transition?(old, successor, proof)
    assert proof.version == 1
    assert proof.old_source_path == context.source_path
    assert proof.new_source_path == new_path
    assert proof.instruction_sha256 == sha256(ControlledReviewQualification.instruction())

    assert {:ok, "/srv/factory/alpha-loop-20261013/qualification/software-change-controlled-review-alpha-v1.yaml"} =
             ControlledReviewQualification.source_path(old)
  end

  test "normalizes relative agent references when the replacement source lives elsewhere", context do
    old = load_old!(context.source_path)
    new_path = write_successor!(context, old)

    assert {:ok, successor, _proof} = ControlledReviewQualification.successor_definition(old, new_path)
    assert ControlledReviewQualification.equivalent?(old, successor)
    assert old.agents == successor.agents
  end

  test "rejects a parsed gate change even when the prompt append is exact", context do
    old = load_old!(context.source_path)
    new_path = write_successor!(context, old)
    source = File.read!(new_path)
    source = replace_once!(source, "        max_attempts: 3", "        max_attempts: 2")
    File.write!(new_path, source)

    assert {:ok, changed} = Workstream.load(new_path, old.input_values)
    refute ControlledReviewQualification.equivalent?(old, changed)
    assert {:error, :invalid_controlled_review_definition} = ControlledReviewQualification.successor_definition(old, new_path)
  end

  test "rejects changed agent and instruction resources", context do
    for {relative_path, before, replacement} <- [
          {"agents/default-review.yaml", "model: gpt-6.1-sol", "model: gpt-6-luna"},
          {"instructions/cloud-review.md", "Follow the stage task", "Follow the altered stage task"}
        ] do
      isolated = isolated_context(context)
      old = load_old!(isolated.source_path)
      resource_path = Path.join(isolated.release_factory_dir, relative_path)
      resource = File.read!(resource_path)
      resource = replace_once!(resource, before, replacement)
      File.write!(resource_path, resource)
      new_path = write_successor!(isolated, old)

      assert {:ok, changed} = Workstream.load(new_path, old.input_values)
      refute ControlledReviewQualification.equivalent?(old, changed)
    end
  end

  test "rejects inconsistent source digests and unreferenced duplicate resources", context do
    old = load_old!(context.source_path)
    new_path = write_successor!(context, old)
    assert {:ok, successor, proof} = ControlledReviewQualification.successor_definition(old, new_path)

    source = successor.definitions[new_path]
    bad_digest = put_in(successor.definitions[new_path].sha256, String.duplicate("0", 64))
    refute ControlledReviewQualification.equivalent?(old, bad_digest)
    refute ControlledReviewQualification.valid_transition?(old, bad_digest, proof)

    duplicate_path = Path.join(context.root, "unreferenced-duplicate.md")
    resource = successor.definitions |> Map.values() |> Enum.find(&(&1.path != new_path))
    duplicated = %{resource | path: duplicate_path}
    with_duplicate = put_in(successor.definitions[duplicate_path], duplicated)
    refute ControlledReviewQualification.equivalent?(old, with_duplicate)
    refute ControlledReviewQualification.valid_transition?(old, with_duplicate, proof)
    assert source.sha256 != bad_digest.definitions[new_path].sha256
  end

  test "rejects copied or forged provenance", context do
    old = load_old!(context.source_path)
    new_path = write_successor!(context, old)
    assert {:ok, successor, proof} = ControlledReviewQualification.successor_definition(old, new_path)

    refute ControlledReviewQualification.valid_transition?(old, successor, Map.put(proof, :extra, true))
    refute ControlledReviewQualification.valid_transition?(successor, old, proof)
  end

  defp isolated_context(context) do
    root = Path.join(System.tmp_dir!(), "controlled-review-qualification-isolated-#{System.unique_integer([:positive])}")
    release_factory_dir = Path.join(root, "releases/fixture/factory")
    workstream_dir = Path.join(release_factory_dir, "workstreams")
    qualification_dir = Path.join(root, "qualification")
    File.mkdir_p!(workstream_dir)
    File.mkdir_p!(qualification_dir)
    File.cp!(context.source_path, Path.join(workstream_dir, "software-change.yaml"))

    for directory <- ["agents", "instructions", "skills"] do
      File.cp_r!(Path.join(context.root, "releases/fixture/factory/#{directory}"), Path.join(release_factory_dir, directory))
    end

    # This test's temporary tree is independent from setup's tree.
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf(root) end)

    %{
      root: root,
      release_factory_dir: release_factory_dir,
      source_path: Path.join(workstream_dir, "software-change.yaml"),
      qualification_dir: qualification_dir
    }
  end

  defp load_old!(path) do
    assert {:ok, definition} = Workstream.load(path, %{"task" => "controlled review qualification test"})
    definition
  end

  defp write_successor!(context, old_definition) do
    new_path = Path.join(context.qualification_dir, "software-change-controlled-review-alpha-v1.yaml")
    source = File.read!(context.source_path)
    review = old_definition.stages["review"]
    old_prompt = review.prompt
    new_prompt = old_prompt <> ControlledReviewQualification.instruction()
    old_review_line = "    prompt: #{old_prompt}\n    next: submit-review"
    new_review_line = "    prompt: #{Jason.encode!(new_prompt)}\n    next: submit-review"
    source = replace_once!(source, old_review_line, new_review_line)
    {:ok, document} = YamlElixir.read_from_string(source)

    source =
      Enum.reduce(document["agents"], source, fn {agent_id, reference}, updated ->
        absolute_path = Path.expand(reference, Path.dirname(context.source_path))
        rebased_reference = Path.join("..", Path.relative_to(absolute_path, context.root))
        replace_once!(updated, "  #{agent_id}: #{reference}\n", "  #{agent_id}: #{rebased_reference}\n")
      end)

    File.write!(new_path, source)
    new_path
  end

  defp replace_once!(source, pattern, replacement) do
    case :binary.matches(source, pattern) do
      [{start, length}] ->
        prefix = binary_part(source, 0, start)
        suffix = binary_part(source, start + length, byte_size(source) - start - length)
        prefix <> replacement <> suffix

      _ ->
        raise "expected exactly one fixture occurrence of #{inspect(pattern)}"
    end
  end

  defp sha256(text), do: :crypto.hash(:sha256, text) |> Base.encode16(case: :lower)
end
