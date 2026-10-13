defmodule SymphonyElixir.ControlledReviewQualification do
  @moduledoc """
  Verifies the one fixed alpha review-qualification prompt append while keeping every other
  loaded workstream setting and pinned resource unchanged.
  """

  alias SymphonyElixir.Workstream

  @instruction_path Path.expand("../../../factory/qualification/controlled-review-alpha-v1.md", __DIR__)
  @external_resource @instruction_path
  @instruction_text "\n\n" <> String.trim_trailing(File.read!(@instruction_path), "\n")
  @instruction_sha256 :crypto.hash(:sha256, @instruction_text) |> Base.encode16(case: :lower)
  @workstream_name "software-change"
  @production_qualification_path "/srv/factory/alpha-loop-20261013/qualification/software-change-controlled-review-alpha-v1.yaml"
  @workstream_fields MapSet.new(~w(version name inputs agents entry stages))

  @doc "Returns the exact text appended to the first-review prompt."
  @spec instruction() :: String.t()
  def instruction, do: @instruction_text

  @doc "Returns the only allowed qualification-definition path for this workstream."
  @spec source_path(map()) :: {:ok, Path.t()} | {:error, atom()}
  def source_path(old_definition) when is_map(old_definition) do
    with {:ok, _source} <- workstream_source(old_definition),
         true <- old_definition.name == @workstream_name do
      {:ok, @production_qualification_path}
    else
      false -> {:error, :unsupported_controlled_review_workstream}
      {:error, _} -> {:error, :invalid_controlled_review_definition}
    end
  end

  def source_path(_old_definition), do: {:error, :invalid_controlled_review_definition}

  @doc "Loads a normal workstream source and accepts it only if it is the fixed qualification successor."
  @spec successor_definition(map(), Path.t()) :: {:ok, map(), map()} | {:error, term()}
  def successor_definition(old_definition, path) when is_map(old_definition) and is_binary(path) do
    with {:ok, _expected_path} <- source_path(old_definition),
         inputs when is_map(inputs) <- Map.get(old_definition, :input_values),
         {:ok, new_definition} <- Workstream.load(path, inputs),
         true <- equivalent?(old_definition, new_definition),
         {:ok, provenance} <- provenance(old_definition, new_definition) do
      {:ok, new_definition, provenance}
    else
      nil -> {:error, :invalid_controlled_review_definition}
      {:error, _} = error -> error
      _ -> {:error, :invalid_controlled_review_definition}
    end
  rescue
    _ -> {:error, :invalid_controlled_review_definition}
  end

  def successor_definition(_old_definition, _path), do: {:error, :invalid_controlled_review_definition}

  @doc "Checks the exact prompt-only transition using loaded definitions and source receipts."
  @spec equivalent?(map(), map()) :: boolean()
  def equivalent?(old_definition, new_definition) when is_map(old_definition) and is_map(new_definition) do
    with {:ok, old_source} <- workstream_source(old_definition),
         {:ok, new_source} <- workstream_source(new_definition),
         true <- old_source.path != new_source.path,
         true <- resources_consistent?(old_definition.definitions),
         true <- resources_consistent?(new_definition.definitions),
         {:ok, review_stage_id, old_prompt} <- unique_review_stage(old_definition),
         true <- not String.contains?(old_prompt, @instruction_text),
         {:ok, new_prompt} <- source_prompt(new_source.document, review_stage_id),
         true <- new_prompt == old_prompt <> @instruction_text,
         true <- equivalent_source_documents?(old_source, new_source, review_stage_id, new_prompt),
         true <- unchanged_referenced_resources?(old_definition, old_source, new_definition, new_source),
         true <-
           same_loaded_definition_except_prompt?(
             old_definition,
             old_source,
             new_definition,
             new_source,
             review_stage_id,
             old_prompt
           ) do
      true
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  def equivalent?(_old_definition, _new_definition), do: false

  @doc "Validates stored provenance for the fixed transition without reading the filesystem."
  @spec valid_transition?(map(), map(), map()) :: boolean()
  def valid_transition?(old_definition, new_definition, provenance) when is_map(provenance) do
    equivalent?(old_definition, new_definition) and provenance(old_definition, new_definition) == {:ok, provenance}
  rescue
    _ -> false
  end

  def valid_transition?(_old_definition, _new_definition, _provenance), do: false

  defp provenance(old_definition, new_definition) do
    with {:ok, old_source} <- workstream_source(old_definition),
         {:ok, new_source} <- workstream_source(new_definition) do
      {:ok,
       %{
         version: 1,
         old_source_path: old_source.path,
         old_source_sha256: old_source.sha256,
         new_source_path: new_source.path,
         new_source_sha256: new_source.sha256,
         old_definition_sha256: definition_sha256(old_definition),
         new_definition_sha256: definition_sha256(new_definition),
         instruction_sha256: @instruction_sha256
       }}
    end
  end

  defp definition_sha256(definition) do
    :crypto.hash(:sha256, :erlang.term_to_binary(definition, [:deterministic])) |> Base.encode16(case: :lower)
  end

  defp workstream_source(%{name: name, inputs: inputs, entry: entry, stages: stages, agents: agents, definitions: definitions})
       when is_binary(name) and is_list(inputs) and is_binary(entry) and is_map(stages) and is_map(agents) and is_map(definitions) do
    if resources_consistent?(definitions) do
      candidates =
        Enum.flat_map(definitions, fn {path, resource} ->
          case parse_source(resource.text) do
            {:ok, document} ->
              if root_source_document?(document, name, inputs, entry, stages, agents),
                do: [%{path: path, sha256: resource.sha256, text: resource.text, document: document}],
                else: []

            _ ->
              []
          end
        end)

      case candidates do
        [source] -> {:ok, source}
        _ -> {:error, :ambiguous_workstream_source}
      end
    else
      {:error, :invalid_definition_resources}
    end
  end

  defp workstream_source(_definition), do: {:error, :invalid_workstream_definition}

  defp resources_consistent?(definitions) when is_map(definitions) and map_size(definitions) > 0 do
    Enum.all?(definitions, fn
      {path, %{path: resource_path, sha256: sha256, text: text}}
      when is_binary(path) and is_binary(resource_path) and is_binary(sha256) and is_binary(text) ->
        path == resource_path and Path.type(path) == :absolute and Path.expand(path) == path and sha256 == digest(text)

      _ ->
        false
    end)
  end

  defp resources_consistent?(_definitions), do: false

  defp root_source_document?(document, name, inputs, entry, stages, agents) do
    keys = MapSet.new(Map.keys(document))
    source_stage_ids = document["stages"] |> List.wrap() |> Enum.map(&Map.get(&1, "id")) |> Enum.sort()

    keys == @workstream_fields and document["version"] == 1 and document["name"] == name and
      document["inputs"] == inputs and document["entry"] == entry and is_map(document["agents"]) and
      Enum.sort(Map.keys(document["agents"])) == Enum.sort(Map.keys(agents)) and
      source_stage_ids == Enum.sort(Map.keys(stages))
  rescue
    _ -> false
  end

  defp parse_source(text) when is_binary(text) do
    case YamlElixir.read_from_string(text) do
      {:ok, document} when is_map(document) -> {:ok, document}
      _ -> {:error, :invalid_workstream_source}
    end
  end

  defp parse_source(_text), do: {:error, :invalid_workstream_source}

  defp unique_review_stage(%{stages: stages}) when is_map(stages) do
    case Enum.filter(stages, fn {_id, stage} -> Workstream.reviewer?(stage) end) do
      [{stage_id, %{prompt: prompt}}] when is_binary(prompt) -> {:ok, stage_id, prompt}
      _ -> {:error, :review_stage_not_unique}
    end
  end

  defp unique_review_stage(_definition), do: {:error, :review_stage_not_unique}

  defp source_prompt(document, stage_id) do
    case source_stage(document, stage_id) do
      {:ok, %{"prompt" => prompt}} when is_binary(prompt) -> {:ok, prompt}
      _ -> {:error, :review_stage_prompt_missing}
    end
  end

  defp source_stage(%{"stages" => stages}, stage_id) when is_list(stages) do
    case Enum.filter(stages, &(&1["id"] == stage_id)) do
      [stage] when is_map(stage) -> {:ok, stage}
      _ -> {:error, :review_stage_not_unique}
    end
  end

  defp source_stage(_document, _stage_id), do: {:error, :invalid_workstream_source}

  defp equivalent_source_documents?(old_source, new_source, review_stage_id, new_prompt) do
    with {:ok, old_document} <- normalize_agent_paths(old_source.document, old_source.path),
         {:ok, new_document} <- normalize_agent_paths(new_source.document, new_source.path),
         {:ok, old_prompt} <- source_prompt(old_document, review_stage_id),
         expected_document <- replace_source_prompt(old_document, review_stage_id, old_prompt <> @instruction_text) do
      new_prompt == old_prompt <> @instruction_text and new_prompt == source_prompt_value(new_document, review_stage_id) and
        new_document == expected_document
    else
      _ -> false
    end
  end

  defp source_prompt_value(document, stage_id) do
    case source_prompt(document, stage_id) do
      {:ok, prompt} -> prompt
      _ -> nil
    end
  end

  defp normalize_agent_paths(%{"agents" => agents} = document, source_path) when is_map(agents) do
    normalized =
      Map.new(agents, fn
        {id, path} when is_binary(id) and is_binary(path) -> {id, Path.expand(path, Path.dirname(source_path))}
        _ -> {:invalid, :invalid}
      end)

    if map_size(normalized) == map_size(agents) and Enum.all?(normalized, fn {id, path} -> is_binary(id) and is_binary(path) end) do
      {:ok, Map.put(document, "agents", normalized)}
    else
      {:error, :invalid_agent_references}
    end
  rescue
    _ -> {:error, :invalid_agent_references}
  end

  defp normalize_agent_paths(_document, _source_path), do: {:error, :invalid_agent_references}

  defp replace_source_prompt(document, stage_id, prompt) do
    Map.update!(document, "stages", fn stages ->
      Enum.map(stages, fn stage -> if stage["id"] == stage_id, do: Map.put(stage, "prompt", prompt), else: stage end)
    end)
  end

  defp unchanged_referenced_resources?(old_definition, old_source, new_definition, new_source) do
    Map.delete(old_definition.definitions, old_source.path) == Map.delete(new_definition.definitions, new_source.path)
  end

  defp same_loaded_definition_except_prompt?(old_definition, old_source, new_definition, new_source, review_stage_id, old_prompt) do
    old_loaded = without_root_source(old_definition, old_source.path)
    new_loaded = new_definition |> without_root_source(new_source.path) |> put_in([:stages, review_stage_id, :prompt], old_prompt)
    old_loaded == new_loaded
  end

  defp without_root_source(definition, source_path) do
    update_in(definition, [:definitions], &Map.delete(&1, source_path))
  end

  defp digest(text), do: :crypto.hash(:sha256, text) |> Base.encode16(case: :lower)
end
