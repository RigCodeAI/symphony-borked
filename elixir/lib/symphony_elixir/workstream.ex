defmodule SymphonyElixir.Workstream do
  @moduledoc """
  Loads and validates a versioned local workstream definition.

  Workstream, agent, instruction, and skill files are read when the workstream is loaded.
  Their source text and SHA-256 digests are returned together so callers can pin the exact
  definitions used by a run.
  """

  @max_repair_attempts 3
  @reasoning_efforts ~w(none minimal low medium high xhigh max ultra)

  @type file_reference :: %{path: Path.t(), sha256: String.t(), text: String.t()}
  @type loaded_agent :: %{
          version: 1,
          name: String.t(),
          model: String.t(),
          reasoning_effort: String.t(),
          daybreak: boolean(),
          authentication: map(),
          revision: String.t(),
          approval_policy: String.t(),
          sandbox: String.t(),
          instructions: [file_reference()],
          skills: [file_reference()]
        }
  @type loaded_stage :: map()
  @typep stage_id_set :: %{optional(String.t()) => true}
  @typep reachability_result :: {:ok, stage_id_set()} | {:error, term()}
  @type loaded_workstream :: %{
          version: 1,
          name: String.t(),
          inputs: [String.t()],
          entry: String.t(),
          stages: %{String.t() => loaded_stage()},
          agents: %{String.t() => loaded_agent()},
          input_values: map(),
          definitions: %{Path.t() => file_reference()}
        }

  @doc """
  Loads a workstream with no supplied runtime inputs.

  See `load/2` for the accepted schema and result.
  """
  @spec load(Path.t()) :: {:ok, loaded_workstream()} | {:error, term()}
  def load(path) when is_binary(path), do: load(path, %{})
  def load(path), do: {:error, {:invalid_workstream_path, path}}

  @doc """
  Loads a version 1 workstream YAML file and validates its definitions and required inputs.

  Relative agent paths are resolved from the workstream file. Relative instruction and skill
  paths are resolved from their agent file. The returned `:definitions` map contains source text
  and a SHA-256 digest for every loaded file.
  """
  @spec load(Path.t(), map()) :: {:ok, loaded_workstream()} | {:error, term()}
  def load(path, input_values) when is_binary(path) and is_map(input_values) do
    with {:ok, workstream_path, source, document} <- read_yaml(path, :workstream),
         :ok <- exact_fields(document, ~w(version name inputs agents entry stages), :workstream),
         :ok <- validate_version(Map.get(document, "version"), :workstream),
         {:ok, name} <- required_name(Map.get(document, "name"), :workstream_name),
         {:ok, input_names} <- names(Map.get(document, "inputs"), :workstream_inputs),
         {:ok, entry} <- required_name(Map.get(document, "entry"), :workstream_entry),
         {:ok, agent_paths} <- validate_agent_paths(Map.get(document, "agents")),
         {:ok, stages} <- normalize_stages(Map.get(document, "stages")),
         {:ok, agents, definitions} <- load_agents(agent_paths, Path.dirname(workstream_path)),
         :ok <- validate_input_values(input_names, input_values),
         :ok <- validate_agent_references(stages, agents),
         :ok <- validate_transitions(stages, entry),
         :ok <- validate_outputs(stages, input_names),
         :ok <- validate_dataflow(stages, entry, input_names) do
      workstream_definition = reference(workstream_path, source)

      {:ok,
       %{
         version: 1,
         name: name,
         inputs: input_names,
         entry: entry,
         stages: stages,
         agents: agents,
         input_values: input_values,
         definitions: Map.put(definitions, workstream_path, workstream_definition)
       }}
    end
  end

  def load(path, input_values) when is_binary(path) and not is_map(input_values),
    do: {:error, {:invalid_workstream_inputs, :expected_map}}

  def load(path, _input_values), do: {:error, {:invalid_workstream_path, path}}

  defp read_yaml(path, context) do
    with {:ok, canonical_path} <- canonical_definition_path(path, context) do
      read_yaml_file(canonical_path, context)
    end
  end

  defp read_yaml_file(path, context) do
    case File.read(path) do
      {:ok, source} -> parse_yaml(source, path, context)
      {:error, reason} -> {:error, {:definition_file_error, context, path, reason}}
    end
  end

  defp parse_yaml(source, path, context) do
    case YamlElixir.read_from_string(source) do
      {:ok, document} when is_map(document) -> {:ok, path, source, document}
      {:ok, _document} -> {:error, {:invalid_definition, context, :expected_map}}
      {:error, reason} -> {:error, {:yaml_parse_error, context, path, reason}}
    end
  end

  defp canonical_definition_path(path, context) do
    case SymphonyElixir.PathSafety.canonicalize(path) do
      {:ok, canonical_path} -> {:ok, canonical_path}
      {:error, reason} -> {:error, {:definition_path_error, context, Path.expand(path), reason}}
    end
  end

  defp exact_fields(document, required, context) when is_map(document) do
    actual = Map.keys(document)
    missing = required -- actual
    unknown = actual -- required

    if missing == [] and unknown == [] do
      :ok
    else
      {:error, {:invalid_definition_fields, context, missing, unknown}}
    end
  end

  defp exact_optional_fields(document, required, optional, context) when is_map(document) do
    actual = Map.keys(document)
    missing = required -- actual
    unknown = actual -- (required ++ optional)

    if missing == [] and unknown == [] do
      :ok
    else
      {:error, {:invalid_definition_fields, context, missing, unknown}}
    end
  end

  defp approval_setting(value, _stage_id) when is_boolean(value), do: {:ok, value}
  defp approval_setting(value, stage_id), do: {:error, {:invalid_human_wait_approval, stage_id, value}}

  defp validate_version(1, _context), do: :ok
  defp validate_version(version, context), do: {:error, {:unsupported_definition_version, context, version}}

  defp required_name(name, _context) when is_binary(name) do
    if String.trim(name) == "" do
      {:error, {:invalid_name, name}}
    else
      {:ok, name}
    end
  end

  defp required_name(value, context), do: {:error, {:invalid_name, context, value}}

  defp names(values, context) when is_list(values) do
    if Enum.all?(values, &(is_binary(&1) and String.trim(&1) != "")) do
      if length(values) == length(Enum.uniq(values)) do
        {:ok, values}
      else
        {:error, {:duplicate_names, context}}
      end
    else
      {:error, {:invalid_names, context}}
    end
  end

  defp names(_values, context), do: {:error, {:invalid_names, context}}

  defp validate_agent_paths(agent_paths) when is_map(agent_paths) and map_size(agent_paths) > 0 do
    Enum.reduce_while(agent_paths, {:ok, %{}}, fn {id, path}, {:ok, acc} ->
      cond do
        not (is_binary(id) and String.trim(id) != "") ->
          {:halt, {:error, {:invalid_agent_id, id}}}

        not relative_path?(path) ->
          {:halt, {:error, {:invalid_agent_path, id, path}}}

        true ->
          {:cont, {:ok, Map.put(acc, id, path)}}
      end
    end)
  end

  defp validate_agent_paths(_agent_paths), do: {:error, {:invalid_workstream_agents, :expected_nonempty_map}}

  defp normalize_stages(stages) when is_list(stages) and stages != [] do
    Enum.reduce_while(stages, {:ok, %{}}, fn stage, {:ok, acc} ->
      with {:ok, normalized} <- normalize_stage(stage),
           false <- Map.has_key?(acc, normalized.id) do
        {:cont, {:ok, Map.put(acc, normalized.id, normalized)}}
      else
        true -> {:halt, {:error, {:duplicate_stage_id, Map.get(stage, "id")}}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp normalize_stages(_stages), do: {:error, {:invalid_workstream_stages, :expected_nonempty_list}}

  defp normalize_stage(%{"type" => "agent"} = stage) do
    with :ok <- exact_fields(stage, ~w(id type inputs outputs agent prompt next), {:stage, Map.get(stage, "id")}),
         {:ok, id} <- required_name(Map.get(stage, "id"), :stage_id),
         {:ok, inputs} <- names(Map.get(stage, "inputs"), {:stage_inputs, id}),
         {:ok, outputs} <- names(Map.get(stage, "outputs"), {:stage_outputs, id}),
         {:ok, agent} <- required_name(Map.get(stage, "agent"), {:stage_agent, id}),
         {:ok, prompt} <- required_name(Map.get(stage, "prompt"), {:stage_prompt, id}),
         {:ok, next} <- required_name(Map.get(stage, "next"), {:stage_next, id}) do
      {:ok, %{id: id, type: :agent, inputs: inputs, outputs: outputs, agent: agent, prompt: prompt, next: next}}
    end
  end

  defp normalize_stage(%{"type" => "check"} = stage) do
    with :ok <- exact_fields(stage, ~w(id type inputs outputs gate), {:stage, Map.get(stage, "id")}),
         {:ok, id} <- required_name(Map.get(stage, "id"), :stage_id),
         {:ok, inputs} <- names(Map.get(stage, "inputs"), {:stage_inputs, id}),
         {:ok, outputs} <- names(Map.get(stage, "outputs"), {:stage_outputs, id}),
         {:ok, gate} <- normalize_gate(Map.get(stage, "gate"), id) do
      {:ok, %{id: id, type: :check, inputs: inputs, outputs: outputs, gate: gate}}
    end
  end

  defp normalize_stage(%{"type" => "human_wait"} = stage) do
    with :ok <- exact_optional_fields(stage, ~w(id type inputs outputs prompt next), ["approval"], {:stage, Map.get(stage, "id")}),
         {:ok, id} <- required_name(Map.get(stage, "id"), :stage_id),
         {:ok, inputs} <- names(Map.get(stage, "inputs"), {:stage_inputs, id}),
         {:ok, outputs} <- names(Map.get(stage, "outputs"), {:stage_outputs, id}),
         {:ok, prompt} <- required_name(Map.get(stage, "prompt"), {:stage_prompt, id}),
         {:ok, next} <- stage_next(Map.get(stage, "next"), {:stage_next, id}),
         {:ok, approval} <- approval_setting(Map.get(stage, "approval", false), id) do
      normalized = %{id: id, type: :human_wait, inputs: inputs, outputs: outputs, prompt: prompt, next: next}
      {:ok, if(approval, do: Map.put(normalized, :approval, true), else: normalized)}
    end
  end

  defp normalize_stage(%{"type" => type}), do: {:error, {:invalid_stage_type, type}}
  defp normalize_stage(stage), do: {:error, {:invalid_stage_definition, stage}}

  defp normalize_gate(%{"evaluator" => evaluator} = gate, stage_id) do
    with :ok <- exact_fields(gate, ~w(evaluator required success failure), {:gate, stage_id}),
         :ok <- validation_evaluator(evaluator, stage_id),
         {:ok, required} <- validation_required(Map.get(gate, "required"), stage_id),
         {:ok, success} <- gate_success(Map.get(gate, "success"), stage_id),
         {:ok, failure} <- gate_failure(Map.get(gate, "failure"), stage_id) do
      {:ok,
       %{
         evaluator: :candidate_validation,
         required: required,
         success: success,
         failure: failure
       }}
    end
  end

  defp normalize_gate(gate, stage_id) when is_map(gate) do
    with :ok <- exact_fields(gate, ~w(command timeout_ms success failure), {:gate, stage_id}),
         {:ok, command} <- command(Map.get(gate, "command"), stage_id),
         {:ok, timeout_ms} <- positive_integer(Map.get(gate, "timeout_ms"), {:gate_timeout, stage_id}),
         {:ok, success} <- gate_success(Map.get(gate, "success"), stage_id),
         {:ok, failure} <- gate_failure(Map.get(gate, "failure"), stage_id) do
      {:ok, %{command: command, timeout_ms: timeout_ms, success: success, failure: failure}}
    end
  end

  defp normalize_gate(_gate, stage_id), do: {:error, {:invalid_gate, stage_id}}

  defp validation_evaluator("candidate_validation", _stage_id), do: :ok

  defp validation_evaluator(value, stage_id),
    do: {:error, {:unsupported_gate_evaluator, stage_id, value}}

  defp validation_required(required, stage_id) do
    case SymphonyElixir.ValidationPolicy.validate_required(required) do
      {:ok, assertions} -> {:ok, assertions}
      {:error, reason} -> {:error, {:invalid_validation_gate_required, stage_id, reason}}
    end
  end

  defp command(values, stage_id) when is_list(values) and values != [] do
    if Enum.all?(values, &(is_binary(&1) and String.trim(&1) != "")) do
      {:ok, values}
    else
      {:error, {:invalid_gate_command, stage_id}}
    end
  end

  defp command(_values, stage_id), do: {:error, {:invalid_gate_command, stage_id}}

  defp positive_integer(value, _context) when is_integer(value) and value > 0, do: {:ok, value}
  defp positive_integer(value, context), do: {:error, {:invalid_positive_integer, context, value}}

  defp gate_success("complete", _stage_id), do: {:ok, :complete}

  defp gate_success(value, stage_id) when is_binary(value) do
    if String.trim(value) == "" do
      {:error, {:invalid_gate_success, stage_id, value}}
    else
      {:ok, value}
    end
  end

  defp gate_success(value, stage_id), do: {:error, {:invalid_gate_success, stage_id, value}}

  defp stage_next("complete", _context), do: {:ok, :complete}

  defp stage_next(value, context) when is_binary(value) do
    if String.trim(value) == "" do
      {:error, {:invalid_stage_next, context, value}}
    else
      {:ok, value}
    end
  end

  defp stage_next(value, context), do: {:error, {:invalid_stage_next, context, value}}

  defp gate_failure("blocked", _stage_id), do: {:ok, :blocked}

  defp gate_failure(%{"repair" => repair, "max_attempts" => max_attempts} = failure, stage_id) do
    with :ok <- exact_fields(failure, ~w(repair max_attempts), {:gate_repair, stage_id}),
         {:ok, repair} <- required_name(repair, {:gate_repair_target, stage_id}),
         true <- is_integer(max_attempts) and max_attempts in 1..@max_repair_attempts do
      {:ok, %{repair: repair, max_attempts: max_attempts}}
    else
      false -> {:error, {:invalid_gate_repair_attempts, stage_id, max_attempts}}
      {:error, _} = error -> error
    end
  end

  defp gate_failure(value, stage_id), do: {:error, {:invalid_gate_failure, stage_id, value}}

  defp load_agents(agent_paths, workstream_dir) do
    Enum.reduce_while(agent_paths, {:ok, %{}, %{}}, fn {id, relative_path}, {:ok, agents, definitions} ->
      agent_path = Path.expand(relative_path, workstream_dir)

      case load_agent(id, agent_path) do
        {:ok, agent, agent_definitions} ->
          {:cont, {:ok, Map.put(agents, id, agent), Map.merge(definitions, agent_definitions)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  @doc "Loads one named agent and pins its shared resources without executing it."
  @spec load_agent(Path.t()) :: {:ok, loaded_agent(), map()} | {:error, term()}
  def load_agent(path) when is_binary(path), do: load_agent(Path.basename(path), path)
  def load_agent(_path), do: {:error, :invalid_agent_path}

  defp load_agent(id, path) do
    with {:ok, canonical_path, source, document} <- read_yaml(path, {:agent, id}),
         :ok <- agent_fields(document, id),
         :ok <- validate_version(Map.get(document, "version"), {:agent, id}),
         {:ok, name} <- required_name(Map.get(document, "name"), {:agent_name, id}),
         {:ok, model} <- required_name(Map.get(document, "model"), {:agent_model, id}),
         {:ok, reasoning_effort} <- reasoning_effort(Map.get(document, "reasoning_effort"), id),
         {:ok, daybreak} <- daybreak_setting(Map.get(document, "daybreak"), id),
         {:ok, authentication} <- authentication(Map.fetch(document, "authentication"), id),
         :ok <- fixed_setting(Map.get(document, "approval_policy"), "never", :approval_policy, id),
         :ok <- fixed_setting(Map.get(document, "sandbox"), "workspace-write", :sandbox, id),
         {:ok, instruction_refs, instruction_definitions} <- load_file_references(Map.get(document, "instructions"), canonical_path, :instructions, id),
         {:ok, skill_refs, skill_definitions} <- load_file_references(Map.get(document, "skills"), canonical_path, :skills, id) do
      agent_definition = reference(canonical_path, source)

      agent = %{
        version: 1,
        name: name,
        model: model,
        reasoning_effort: reasoning_effort,
        daybreak: daybreak,
        authentication: authentication,
        revision: agent_definition.sha256,
        approval_policy: "never",
        sandbox: "workspace-write",
        instructions: instruction_refs,
        skills: skill_refs
      }

      definitions =
        instruction_definitions
        |> Map.merge(skill_definitions)
        |> Map.put(canonical_path, agent_definition)

      {:ok, agent, definitions}
    end
  end

  defp agent_fields(document, id) do
    fields = ~w(version name model reasoning_effort daybreak approval_policy sandbox instructions skills)
    fields = if Map.has_key?(document, "authentication"), do: fields ++ ["authentication"], else: fields
    exact_fields(document, fields, {:agent, id})
  end

  defp daybreak_setting(value, _id) when is_boolean(value), do: {:ok, value}
  defp daybreak_setting(value, id), do: {:error, {:invalid_agent_daybreak, id, value}}

  defp authentication(:error, _id), do: {:ok, %{mode: :subscription, reference: "inherited"}}

  defp authentication({:ok, %{"mode" => "subscription", "reference" => ref} = value}, id) do
    with :ok <- exact_fields(value, ~w(mode reference), {:agent_authentication, id}),
         true <- is_binary(ref) and String.match?(ref, ~r/\A[A-Za-z0-9][A-Za-z0-9_:.\/\-]{0,255}\z/) do
      {:ok, %{mode: :subscription, reference: ref}}
    else
      false -> {:error, {:invalid_authentication_reference, id}}
      error -> error
    end
  end

  defp authentication(_value, id), do: {:error, {:unsupported_agent_authentication, id}}

  defp reasoning_effort(value, _id) when value in @reasoning_efforts, do: {:ok, value}
  defp reasoning_effort(value, id), do: {:error, {:unsupported_reasoning_effort, id, value}}

  defp fixed_setting(value, expected, _setting, _id) when value == expected, do: :ok
  defp fixed_setting(value, expected, setting, id), do: {:error, {:unsupported_agent_setting, id, setting, value, expected}}

  defp load_file_references(values, agent_path, kind, agent_id) when is_list(values) do
    valid_nonempty = kind != :instructions or values != []

    if valid_nonempty do
      load_file_reference_list(values, agent_path, kind, agent_id)
    else
      {:error, {:invalid_agent_references, agent_id, kind, :expected_nonempty_list}}
    end
  end

  defp load_file_references(_values, _agent_path, kind, agent_id),
    do: {:error, {:invalid_agent_references, agent_id, kind, :expected_list}}

  defp load_file_reference_list(values, agent_path, kind, agent_id) do
    result =
      Enum.reduce_while(values, {:ok, [], %{}}, fn value, {:ok, refs, definitions} ->
        case load_file_reference(value, agent_path, kind, agent_id) do
          {:ok, file_ref} ->
            {:cont, {:ok, [file_ref | refs], Map.put(definitions, file_ref.path, file_ref)}}

          {:error, _} = error ->
            {:halt, error}
        end
      end)

    finish_file_references(result)
  end

  defp finish_file_references({:ok, refs, definitions}), do: {:ok, Enum.reverse(refs), definitions}
  defp finish_file_references({:error, _} = error), do: error

  defp load_file_reference(relative_path, agent_path, kind, agent_id) do
    with :ok <- validate_file_reference_path(relative_path, kind, agent_id),
         path = Path.expand(relative_path, Path.dirname(agent_path)),
         context = {:agent_reference, agent_id, kind},
         {:ok, canonical_path} <- canonical_definition_path(path, context),
         {:ok, text} <- read_reference_file(canonical_path, context) do
      {:ok, reference(canonical_path, text)}
    end
  end

  defp validate_file_reference_path(relative_path, kind, agent_id) do
    cond do
      not relative_path?(relative_path) ->
        {:error, {:invalid_agent_reference, agent_id, kind, relative_path}}

      kind == :skills and Path.basename(relative_path) != "SKILL.md" ->
        {:error, {:invalid_skill_reference, agent_id, relative_path}}

      true ->
        :ok
    end
  end

  defp read_reference_file(path, context) do
    case File.read(path) do
      {:ok, text} -> {:ok, text}
      {:error, reason} -> {:error, {:definition_file_error, context, path, reason}}
    end
  end

  defp validate_agent_references(stages, agents) do
    case Enum.find(stages, fn {_id, stage} ->
           stage.type == :agent and not Map.has_key?(agents, stage.agent)
         end) do
      nil -> :ok
      {stage_id, stage} -> {:error, {:unknown_stage_agent, stage_id, stage.agent}}
    end
  end

  defp validate_transitions(stages, entry) do
    cond do
      not Map.has_key?(stages, entry) ->
        {:error, {:unknown_workstream_entry, entry}}

      stages[entry].type != :agent ->
        {:error, {:workstream_entry_must_be_agent, entry}}

      true ->
        with :ok <- validate_stage_transitions(stages),
             :ok <- validate_repair_edges(stages, entry),
             {:ok, reachable} <- reachable_stages(stages, entry),
             :ok <- all_stages_reachable(stages, reachable),
             {:ok, _order} <- topological_order(stages) do
          :ok
        end
    end
  end

  defp validate_stage_transitions(stages) do
    Enum.reduce_while(stages, :ok, fn {id, stage}, :ok ->
      case validate_stage_transition(id, stage, stages) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_stage_transition(id, %{type: :agent, next: next}, stages) do
    cond do
      not Map.has_key?(stages, next) -> {:error, {:unknown_stage_transition, id, next}}
      stages[next].type != :check -> {:error, {:agent_must_transition_to_check, id, next}}
      true -> :ok
    end
  end

  defp validate_stage_transition(_id, %{type: :human_wait, next: :complete}, _stages), do: :ok

  defp validate_stage_transition(id, %{type: :human_wait, next: next}, stages)
       when is_binary(next) do
    if Map.has_key?(stages, next) do
      :ok
    else
      {:error, {:unknown_stage_transition, id, next}}
    end
  end

  defp validate_stage_transition(_id, %{type: :check, gate: %{success: :complete}}, _stages),
    do: :ok

  defp validate_stage_transition(id, %{type: :check, gate: %{success: next}}, stages)
       when is_binary(next) do
    if Map.has_key?(stages, next) do
      :ok
    else
      {:error, {:unknown_stage_transition, id, next}}
    end
  end

  @spec validate_repair_edges(map(), String.t()) :: :ok | {:error, term()}
  defp validate_repair_edges(stages, entry) do
    {:ok, normal_reachable} = normal_reachable_stages(stages, entry)
    validate_all_repair_edges(stages, normal_reachable)
  end

  @spec validate_all_repair_edges(map(), stage_id_set()) :: :ok | {:error, term()}
  defp validate_all_repair_edges(stages, normal_reachable) do
    Enum.reduce_while(stages, :ok, fn {check_id, stage}, :ok ->
      case validate_repair_edge_for_stage(check_id, stage, stages, normal_reachable) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @spec validate_repair_edge_for_stage(String.t(), map(), map(), stage_id_set()) ::
          :ok | {:error, term()}
  defp validate_repair_edge_for_stage(
         check_id,
         %{type: :check, gate: %{failure: %{repair: target, max_attempts: max_attempts}}},
         stages,
         normal_reachable
       ),
       do: validate_repair_edge(check_id, target, max_attempts, stages, normal_reachable)

  defp validate_repair_edge_for_stage(_check_id, _stage, _stages, _normal_reachable), do: :ok

  @spec validate_repair_edge(String.t(), String.t(), pos_integer(), map(), stage_id_set()) ::
          :ok | {:error, term()}
  defp validate_repair_edge(check_id, target, max_attempts, stages, normal_reachable) do
    cond do
      not Map.has_key?(stages, target) ->
        {:error, {:unknown_repair_stage, check_id, target}}

      stages[target].type != :agent ->
        {:error, {:repair_target_must_be_agent, check_id, target}}

      stages[target].next != check_id ->
        {:error, {:repair_target_must_lead_to_check, check_id, target}}

      max_attempts not in 1..@max_repair_attempts ->
        {:error, {:invalid_repair_attempts, check_id, max_attempts}}

      not Map.has_key?(normal_reachable, target) or not Map.has_key?(normal_reachable, check_id) ->
        {:error, {:repair_target_must_be_ancestor, check_id, target}}

      true ->
        :ok
    end
  end

  defp validate_outputs(stages, input_names) do
    outputs = Enum.flat_map(stages, fn {_id, stage} -> stage.outputs end)
    duplicate_outputs = outputs -- Enum.uniq(outputs)
    overwritten_inputs = Enum.filter(outputs, &(&1 in input_names))

    cond do
      duplicate_outputs != [] ->
        {:error, {:duplicate_stage_outputs, Enum.uniq(duplicate_outputs)}}

      overwritten_inputs != [] ->
        {:error, {:stage_outputs_overwrite_inputs, Enum.uniq(overwritten_inputs)}}

      true ->
        :ok
    end
  end

  defp validate_dataflow(stages, _entry, input_names) do
    with {:ok, order} <- topological_order(stages) do
      predecessors = normal_predecessors(stages)
      validate_dataflow_order(order, stages, predecessors, input_names)
    end
  end

  defp validate_dataflow_order(order, stages, predecessors, input_names) do
    result =
      Enum.reduce_while(order, {:ok, %{}}, fn id, {:ok, produced_by_stage} ->
        case validate_dataflow_stage(id, stages, predecessors, produced_by_stage, input_names) do
          {:ok, updated} -> {:cont, {:ok, updated}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    finish_dataflow_validation(result)
  end

  defp finish_dataflow_validation({:ok, _}), do: :ok
  defp finish_dataflow_validation({:error, _} = error), do: error

  defp validate_dataflow_stage(id, stages, predecessors, produced_by_stage, input_names) do
    stage = stages[id]
    available_before = available_inputs(id, predecessors, produced_by_stage, input_names)
    missing = Enum.reject(stage.inputs, &MapSet.member?(available_before, &1))

    if missing == [] do
      updated = Map.put(produced_by_stage, id, MapSet.union(available_before, MapSet.new(stage.outputs)))
      {:ok, updated}
    else
      {:error, {:stage_inputs_unavailable, id, missing}}
    end
  end

  defp available_inputs(id, predecessors, produced_by_stage, input_names) do
    case Map.get(predecessors, id, []) do
      [] ->
        MapSet.new(input_names)

      incoming ->
        incoming
        |> Enum.map(&Map.fetch!(produced_by_stage, &1))
        |> Enum.reduce(&MapSet.intersection/2)
    end
  end

  defp validate_input_values(input_names, input_values) do
    keys = Map.keys(input_values)
    unknown = keys -- input_names
    missing = Enum.filter(input_names, &(not Map.has_key?(input_values, &1) or is_nil(Map.get(input_values, &1))))

    cond do
      Enum.any?(keys, &(not is_binary(&1))) ->
        {:error, {:invalid_workstream_inputs, :keys_must_be_strings}}

      unknown != [] ->
        {:error, {:unknown_workstream_inputs, unknown}}

      missing != [] ->
        {:error, {:missing_workstream_inputs, missing}}

      true ->
        :ok
    end
  end

  @spec reachable_stages(map(), String.t()) :: reachability_result()
  defp reachable_stages(stages, entry) do
    visit_reachable(stages, [entry], %{}, true)
  end

  @spec normal_reachable_stages(map(), String.t()) :: reachability_result()
  defp normal_reachable_stages(stages, entry) do
    visit_reachable(stages, [entry], %{}, false)
  end

  @spec visit_reachable(map(), [String.t()], stage_id_set(), boolean()) :: reachability_result()
  defp visit_reachable(_stages, [], visited, _include_repairs), do: {:ok, visited}

  defp visit_reachable(stages, [id | rest], visited, include_repairs) do
    if Map.has_key?(visited, id) do
      visit_reachable(stages, rest, visited, include_repairs)
    else
      visit_unvisited_stage(stages, id, rest, visited, include_repairs)
    end
  end

  @spec visit_unvisited_stage(map(), String.t(), [String.t()], stage_id_set(), boolean()) ::
          reachability_result()
  defp visit_unvisited_stage(stages, id, rest, visited, include_repairs) do
    stage = Map.fetch!(stages, id)
    repair_targets = if include_repairs, do: stage_repair_targets(stage), else: []
    targets = stage_success_targets(stage) ++ repair_targets
    visit_reachable(stages, targets ++ rest, Map.put(visited, id, true), include_repairs)
  end

  @spec all_stages_reachable(map(), stage_id_set()) :: :ok | {:error, term()}
  defp all_stages_reachable(stages, reachable) do
    unreachable = Map.keys(stages) |> Enum.reject(&Map.has_key?(reachable, &1))

    if unreachable == [], do: :ok, else: {:error, {:unreachable_stages, unreachable}}
  end

  defp topological_order(stages) do
    predecessors = normal_predecessors(stages)
    indegrees = Map.new(stages, fn {id, _stage} -> {id, length(Map.get(predecessors, id, []))} end)
    ready = for {id, 0} <- indegrees, do: id
    kahn_order(stages, indegrees, ready, [])
  end

  defp kahn_order(stages, _indegrees, [], order) do
    if length(order) == map_size(stages) do
      {:ok, Enum.reverse(order)}
    else
      cyclic = Map.keys(stages) -- order
      {:error, {:unbounded_workstream_cycle, cyclic}}
    end
  end

  defp kahn_order(stages, indegrees, [id | ready], order) do
    successors = stage_success_targets(stages[id])

    {next_indegrees, newly_ready} =
      Enum.reduce(successors, {indegrees, []}, fn successor, {degree_map, new_ready} ->
        degree = Map.fetch!(degree_map, successor) - 1
        updated = Map.put(degree_map, successor, degree)
        if degree == 0, do: {updated, [successor | new_ready]}, else: {updated, new_ready}
      end)

    kahn_order(stages, next_indegrees, ready ++ Enum.reverse(newly_ready), [id | order])
  end

  defp normal_predecessors(stages) do
    Enum.reduce(stages, Map.new(stages, fn {id, _} -> {id, []} end), fn {id, stage}, acc ->
      Enum.reduce(stage_success_targets(stage), acc, fn target, predecessors ->
        Map.update!(predecessors, target, &[id | &1])
      end)
    end)
  end

  defp stage_success_targets(%{type: :agent, next: next}), do: [next]
  defp stage_success_targets(%{type: :human_wait, next: next}) when is_binary(next), do: [next]
  defp stage_success_targets(%{type: :human_wait}), do: []
  defp stage_success_targets(%{type: :check, gate: %{success: success}}) when is_binary(success), do: [success]
  defp stage_success_targets(%{type: :check}), do: []

  defp stage_repair_targets(%{type: :check, gate: %{failure: %{repair: repair}}}), do: [repair]
  defp stage_repair_targets(_stage), do: []

  defp relative_path?(path) when is_binary(path) do
    path != "" and not String.contains?(path, <<0>>) and Path.type(path) == :relative
  end

  defp relative_path?(_path), do: false

  defp reference(path, text) do
    %{path: path, sha256: :crypto.hash(:sha256, text) |> Base.encode16(case: :lower), text: text}
  end
end
