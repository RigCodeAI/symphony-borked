defmodule SymphonyElixir.RecoveryInspection do
  @moduledoc """
  Anchored local capture inspection. No installation, provider reconciliation or
  dispatch release is performed. Saved definitions are inspected in memory;
  current definition files are never substituted for the saved pins.
  """

  alias SymphonyElixir.{BackupCapture, Evidence, WorkstreamChild, WorkstreamRun, WorkstreamStore}

  @config_keys [:root, :id, :workspace_roots, :manifest_sha256, :service_revision, :policy]
  @blockers [
    :old_coordinator_not_fenced,
    :workers_not_reconciled,
    :post_capture_provider_events_not_reconciled,
    :permanent_archives_not_verified,
    :child_proofs_not_reconciled,
    :runtime_configuration_not_qualified
  ]

  @doc "Inspects the actual capture images, keeping every continuation held."
  @spec inspect(map()) :: {:ok, %{runs: [map()], report: map()}} | {:error, atom()}
  def inspect(config) when is_map(config) do
    Enum.each(
      [
        SymphonyElixir.Workstream,
        WorkstreamRun,
        WorkstreamChild,
        SymphonyElixir.WorkstreamRunner,
        SymphonyElixir.Linear.AgentMapping,
        SymphonyElixir.AgentReadiness,
        SymphonyElixir.ValidationPolicy,
        SymphonyElixir.Validation,
        SymphonyElixir.ValidationCommand,
        SymphonyElixir.Publisher,
        SymphonyElixir.PublisherGithub,
        SymphonyElixir.CandidateTransfer
      ],
      &Code.ensure_loaded!/1
    )

    with :ok <- validate_config(config),
         {:ok, captured} <- BackupCapture.read_images(config.root, config.id, config.workspace_roots, config.manifest_sha256),
         :ok <- check_anchors(captured.manifest, config),
         {:ok, saved} <- WorkstreamStore.inspect_recovery_image(captured.images["state"]),
         true <- Enum.all?(saved.runs, &valid_run?/1),
         :ok <- WorkstreamChild.inspect_recovery_links(saved.runs),
         true <- Enum.all?(saved.linear.tasks, fn {_id, task} -> is_nil(task[:run_id]) or task[:run_id] in saved.run_ids end),
         :ok <- check_inventory(captured.manifest, saved),
         {:ok, evidence} <- Evidence.inspect_recovery_image(captured.images["evidence"], saved.runs),
         :ok <- check_evidence(captured.manifest, evidence, saved.run_ids) do
      incompatible = saved.runs |> Enum.reject(&WorkstreamRun.compatible_policy?/1) |> Enum.map(& &1.id) |> Enum.sort()

      report = %{
        status: :dispatch_held,
        dispatch_allowed: false,
        restore_ready: false,
        capture_id: config.id,
        manifest_sha256: config.manifest_sha256,
        service_revision: config.service_revision,
        service_revision_source: :operator_pin,
        policy_compatible: incompatible == [],
        incompatible_run_ids: incompatible,
        run_ids: saved.run_ids,
        operation_ids: saved.operation_ids,
        linear_delivery_ids: saved.linear_delivery_ids,
        linear_task_ids: saved.linear.tasks |> Map.keys() |> Enum.sort(),
        evidence_event_watermark: evidence.event_watermark,
        blockers: if(incompatible == [], do: @blockers, else: [:incompatible_saved_policy | @blockers])
      }

      {:ok, %{runs: saved.runs, report: report}}
    else
      :not_found -> {:error, :recovery_capture_not_found}
      false -> {:error, :invalid_recovery_run}
      {:error, reason} when is_atom(reason) -> {:error, reason}
      _ -> {:error, :invalid_recovery_capture}
    end
  rescue
    _ -> {:error, :invalid_recovery_capture}
  catch
    _, _ -> {:error, :invalid_recovery_capture}
  end

  def inspect(_config), do: {:error, :invalid_recovery_configuration}

  @doc "Checks trusted anchor syntax without reading captures or qualifying recovery."
  @spec validate_configuration(term()) :: :ok | {:error, :invalid_recovery_configuration}
  def validate_configuration(config) when is_map(config), do: validate_config(config)
  def validate_configuration(_config), do: {:error, :invalid_recovery_configuration}

  defp validate_config(config) do
    if Enum.sort(Map.keys(config)) == Enum.sort(@config_keys) and
         absolute_clean_path?(config.root) and is_binary(config.id) and
         Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._-]{0,63}\z/, config.id) and
         is_list(config.workspace_roots) and config.workspace_roots != [] and
         Enum.all?(config.workspace_roots, &absolute_clean_path?/1) and
         digest?(config.manifest_sha256) and revision?(config.service_revision) and valid_policy?(config.policy), do: :ok, else: {:error, :invalid_recovery_configuration}
  end

  defp check_anchors(manifest, config) do
    metadata = manifest["metadata"]

    if metadata["service_revision"] == config.service_revision and
         metadata["service_revision_source"] == "operator_pin" and
         metadata["policy"] == json_value(config.policy) and metadata["pending_evidence_outbox"] == 0, do: :ok, else: {:error, :recovery_anchor_mismatch}
  end

  defp check_inventory(manifest, saved) do
    metadata = manifest["metadata"]

    if metadata["run_ids"] == saved.run_ids and metadata["operation_ids"] == saved.operation_ids and
         metadata["linear_delivery_ids"] == saved.linear_delivery_ids, do: :ok, else: {:error, :recovery_inventory_mismatch}
  end

  defp check_evidence(manifest, evidence, run_ids) do
    metadata = manifest["metadata"]
    bundles = metadata["evidence_bundles"]

    valid =
      is_list(bundles) and
        Enum.all?(bundles, fn
          %{"run_id" => id, "revision" => revision} = entry -> map_size(entry) == 2 and is_binary(id) and digest?(revision)
          _ -> false
        end)

    if valid and length(bundles) == length(run_ids) and Enum.sort(Enum.map(bundles, & &1["run_id"])) == run_ids and
         metadata["evidence_event_watermark"] == evidence.event_watermark, do: :ok, else: {:error, :recovery_evidence_inventory_mismatch}
  end

  defp valid_run?(run) do
    definition = run[:definition]

    is_map(definition) and definition[:version] == 1 and
      is_map(definition[:stages]) and is_map(definition[:agents]) and
      Map.has_key?(definition.stages, definition[:entry]) and
      (run[:stage_id] in [:complete, :blocked] or Map.has_key?(definition.stages, run[:stage_id])) and
      run[:status] in [:ready, :executing, :reconciling, :waiting_for_answer, :waiting_for_child, :complete, :blocked, :policy_blocked, :stopped] and
      is_binary(run[:workspace]) and Path.type(run.workspace) == :absolute and
      is_map(run[:execution]) and valid_policy?(run[:policy]) and
      Enum.all?([:validation_repair_rounds, :review_repair_rounds, :transport_retries], fn key -> nonnegative?(Map.get(run, key, 0)) end) and
      frozen_pins_valid?(definition) and attempts_valid?(run, definition) and wait_valid?(run)
  end

  defp frozen_pins_valid?(definition, mode \\ :root) do
    resources = definition[:definitions]

    is_map(resources) and map_size(resources) > 0 and
      Enum.count(definition.stages, fn {_id, stage} -> is_map(stage) and stage[:type] == :child end) <= 1 and
      Enum.all?(resources, fn {path, resource} ->
        is_binary(path) and is_map(resource) and resource[:path] == path and
          is_binary(resource[:text]) and digest?(resource[:sha256]) and digest(resource.text) == resource.sha256
      end) and
      Enum.all?(definition.agents, fn {_id, agent} ->
        is_map(agent) and agent[:version] == 1 and
          Enum.any?(resources, fn {_path, resource} -> resource.sha256 == agent[:revision] end) and
          Enum.all?([:instructions, :skills], fn key ->
            is_list(agent[key]) and Enum.all?(agent[key], fn reference -> is_map(reference) and resources[reference[:path]] == reference end)
          end)
      end) and
      Enum.all?(definition.stages, fn {id, stage} ->
        is_binary(id) and is_map(stage) and stage[:id] == id and
          stage[:type] in [:agent, :check, :publish, :human_wait, :child] and
          (stage[:type] != :agent or Map.has_key?(definition.agents, stage[:agent])) and
          (stage[:type] != :child or (mode == :root and child_stage_valid?(stage, definition)))
      end)
  end

  defp child_stage_valid?(stage, parent_definition) do
    child = stage[:child_definition]
    mapping = stage[:input_mapping]

    is_map(child) and child[:version] == 1 and child[:name] == "software-change" and
      is_map(child[:stages]) and is_map(child[:agents]) and Map.has_key?(child.stages, child[:entry]) and
      names?(child[:inputs]) and is_map(child[:input_values]) and is_map(mapping) and
      Enum.sort(Map.keys(mapping)) == Enum.sort(child.inputs) and
      Enum.all?(mapping, fn {key, source} -> is_binary(key) and is_binary(source) end) and
      names?(stage[:inputs]) and Enum.sort(Enum.uniq(Map.values(mapping))) == Enum.sort(stage.inputs) and
      child.input_values == Map.new(mapping, fn {key, source} -> {key, {:mapped_parent_input, source}} end) and
      match?(%{type: :check}, parent_definition.stages[stage[:next]]) and
      frozen_pins_valid?(child, :child) and
      Enum.all?(child.definitions, fn {path, resource} -> parent_definition.definitions[path] == resource end)
  end

  defp names?(values), do: is_list(values) and length(values) == length(Enum.uniq(values)) and Enum.all?(values, &(is_binary(&1) and &1 != ""))

  defp attempts_valid?(run, definition) do
    is_list(run[:attempts]) and is_map(run[:operations]) and
      Enum.all?(run.attempts, fn attempt -> is_map(attempt) and Map.has_key?(definition.stages, attempt[:stage]) end) and
      (is_nil(run[:current_attempt_id]) or Enum.any?(run.attempts, &(&1[:id] == run.current_attempt_id)))
  end

  defp wait_valid?(%{pending_wait: nil}), do: true
  defp wait_valid?(%{pending_wait: wait, artifacts: artifacts}) when is_map(wait), do: wait[:artifact_ids] == artifacts
  defp wait_valid?(_run), do: false

  defp valid_policy?(policy) when is_map(policy) do
    keys = [:version, :gate_policy, :lifecycle_sha256, :executor_sha256, :validation_sha256]

    (Enum.sort(Map.keys(policy)) == Enum.sort(keys) or Enum.sort(Map.keys(policy)) == Enum.sort([:child_sha256 | keys])) and
      policy[:version] == 1 and policy[:gate_policy] == "exit-status-v1" and
      Enum.all?([:lifecycle_sha256, :executor_sha256, :validation_sha256], &digest?(policy[&1])) and
      (not Map.has_key?(policy, :child_sha256) or digest?(policy.child_sha256))
  end

  defp valid_policy?(_policy), do: false

  defp absolute_clean_path?(path) when is_binary(path),
    do: Path.type(path) == :absolute and not String.contains?(path, <<0>>) and ".." not in Path.split(path)

  defp absolute_clean_path?(_path), do: false
  defp nonnegative?(value), do: is_integer(value) and value >= 0
  defp digest?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp revision?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{40}([0-9a-f]{24})?\z/, value)
  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp json_value(value), do: value |> Jason.encode!() |> Jason.decode!()
end
