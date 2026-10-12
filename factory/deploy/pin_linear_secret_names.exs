defmodule SymphonyElixir.Factory.PinLinearSecretNames do
  @moduledoc """
  One-shot, offline metadata migration for adding service credential exclusions
  to a quiescent durable Linear run without changing its pinned service policy.
  """

  alias Exqlite.Sqlite3
  alias SymphonyElixir.{Config, Evidence, WorkstreamRun, WorkstreamStore, Workflow}
  alias SymphonyElixir.Linear.AgentMapping

  @service_unit "factory-coordinator.service"
  @secret_names_marker "FACTORY_LINEAR_SECRET_NAMES_JSON"
  @environment_name ~r/^[A-Z][A-Z0-9_]*$/
  @supported_policy %{
    lifecycle_sha256: "96b3f754fae20657a74993320094ab5213735a84157f6022915099babc671545",
    executor_sha256: "51c0f81cfa56d8961c744237ada21bcfd87153f5a59197f514ec37c1bd899d17",
    child_sha256: "a598793bd67ae523b441445143c548b0ed28e9572c471196f3f7291e20b465d5",
    validation_sha256: "47a77cb57082970ce8f6ca5515b327fc49c31a68a19482fff07893d93efc9cbf",
    gate_policy: "exit-status-v1",
    version: 1
  }

  @switches [
    apply: :boolean,
    workflow_path: :string,
    store_path: :string,
    run_id: :string,
    expected_status: :string,
    expected_stage_id: :string,
    expected_attempt_id: :string,
    expected_wait_id: :string,
    operator: :string
  ]

  @doc false
  def supported_policy, do: @supported_policy

  @doc false
  def configured_secret_names(config) when is_map(config) do
    names = AgentMapping.credential_environment_names(config) ++ [@secret_names_marker]

    if valid_names?(names) do
      {:ok, names |> Enum.uniq() |> Enum.sort()}
    else
      {:error, :invalid_configured_secret_names}
    end
  end

  @doc false
  def pin_run(store, run_id, configured_names, expected, opts \\ []) do
    with :ok <- supported_source_policy(),
         {:ok, run} <- fetch_run(store, run_id),
         :ok <- expected_run(run, expected),
         :ok <- quiescent_run(run),
         {:ok, merged_names} <- merged_names(run, configured_names),
         :ok <- validate_operator(Keyword.get(opts, :operator, "DEV-235")),
         :ok <- service_stopped(opts),
         updated = put_in(run.execution.secret_environment_names, merged_names),
         event = operator_event(merged_names, opts),
         event_id = event_id(run.id, merged_names, event["operator"]),
         {:ok, outcome} <- persist_exactly(store, run, updated, event_id, event),
         :ok <- verify_evidence_outbox(store, event_id, updated, event, outcome),
         :ok <- service_stopped(opts),
         {:ok, persisted} <- WorkstreamStore.fetch(store, run.id),
         true <- persisted === updated,
         :ok <- evidence_identity_unchanged(run, persisted) do
      {:ok,
       %{
         run_id: run.id,
         event_id: event_id,
         environment_names: merged_names,
         policy: persisted.policy,
         outcome: outcome,
         run: persisted
       }}
    else
      false -> {:error, :persisted_snapshot_mismatch}
      :not_found -> {:error, :run_not_found}
      {:error, _reason} = error -> error
      _ -> {:error, :unsafe_or_incomplete_migration}
    end
  rescue
    _ -> {:error, :secret_name_migration_failed}
  catch
    _, _ -> {:error, :secret_name_migration_failed}
  end

  @doc false
  def run_cli(argv \\ System.argv()) do
    with :ok <- ensure_helper_dependencies(),
         :ok <- ensure_application_module_atoms(),
         {:ok, options} <- parse_args(argv),
         :ok <- validate_cli_options(options),
         :ok <- service_stopped(options),
         :ok <- no_runtime_processes(),
         :ok <- supported_source_policy(),
         :ok <- set_workflow_path(options.workflow_path),
         {:ok, config} <- Config.linear_delegation(),
         {:ok, _evidence_config} <- enabled_evidence_config(),
         :ok <- configured_store_matches(config, options.store_path),
         :ok <- schema_three_database(options.store_path),
         {:ok, names} <- configured_secret_names(config),
         {:ok, store} <- WorkstreamStore.start_link(path: options.store_path, owner: self()) do
      try do
        expected = %{
          expected_status: if(options.expected_status == "waiting_for_answer", do: :waiting_for_answer, else: :ready),
          expected_stage_id: options.expected_stage_id,
          expected_attempt_id: options[:expected_attempt_id],
          expected_wait_id: options[:expected_wait_id]
        }

        pin_run(store, options.run_id, names, expected, operator: options.operator, service_check: options[:service_check])
      after
        if Process.alive?(store), do: GenServer.stop(store)
      end
    else
      false -> {:error, :invalid_operator_request}
      {:error, _reason} = error -> error
      _ -> {:error, :operator_preflight_failed}
    end
  rescue
    _ -> {:error, :operator_preflight_failed}
  catch
    _, _ -> {:error, :operator_preflight_failed}
  end

  @doc false
  def main(argv \\ System.argv()) do
    if "--apply" in argv do
      case run_cli(argv) do
        {:ok, result} ->
          IO.puts("Pinned #{length(result.environment_names)} service environment-name exclusions for #{result.run_id}; durable operator event #{result.event_id}.")
          :ok

        {:error, reason} ->
          IO.puts(:stderr, "Secret-name pin failed closed: #{reason}.")
          System.halt(1)
      end
    else
      IO.puts(:stderr, "Read the DEV-235 runbook, then rerun with --apply and the exact workflow, store, run, wait, and operator arguments.")
      System.halt(2)
    end
  end

  defp parse_args(argv) do
    case OptionParser.parse(argv, strict: @switches, allow_nonexistent_atoms: false) do
      {options, [], []} -> {:ok, Map.new(options)}
      _ -> {:error, :invalid_arguments}
    end
  end

  defp ensure_helper_dependencies do
    Enum.reduce_while([:yaml_elixir, :jason, :exqlite], :ok, fn app, :ok ->
      case Application.ensure_all_started(app) do
        {:ok, _started} -> {:cont, :ok}
        {:error, _reason} -> {:halt, {:error, :helper_dependency_unavailable}}
      end
    end)
  end

  defp ensure_application_module_atoms do
    with :ok <- load_application_spec(),
         modules when is_list(modules) and modules != [] <- Application.spec(:symphony_elixir, :modules),
         :ok <- ensure_modules_loaded(modules) do
      :ok
    else
      _ -> {:error, :application_modules_unavailable}
    end
  rescue
    _ -> {:error, :application_modules_unavailable}
  end

  defp load_application_spec do
    case Application.load(:symphony_elixir) do
      :ok -> :ok
      {:error, {:already_loaded, :symphony_elixir}} -> :ok
      _ -> {:error, :application_spec_unavailable}
    end
  end

  defp ensure_modules_loaded(modules) do
    Enum.reduce_while(modules, :ok, fn module, :ok ->
      case Code.ensure_loaded(module) do
        {:module, ^module} -> {:cont, :ok}
        _ -> {:halt, {:error, :application_module_unavailable}}
      end
    end)
  end

  defp validate_cli_options(options) do
    required = [:apply, :workflow_path, :store_path, :run_id, :expected_status, :expected_stage_id, :operator]

    valid_required? = Enum.all?(required, fn key -> Map.has_key?(options, key) and options[key] not in [nil, ""] end)
    valid_wait? = options[:expected_status] == "waiting_for_answer" and is_binary(options[:expected_attempt_id]) and is_binary(options[:expected_wait_id])
    valid_ready? = options[:expected_status] == "ready" and is_nil(options[:expected_attempt_id]) and is_nil(options[:expected_wait_id])

    if options[:apply] == true and valid_required? and (valid_wait? or valid_ready?) and valid_operator?(options[:operator]),
      do: :ok,
      else: {:error, :invalid_operator_request}
  end

  defp set_workflow_path(path) do
    with :ok <- absolute_regular_file(path),
         :ok <- Workflow.set_workflow_file_path(path) do
      :ok
    end
  end

  defp absolute_regular_file(path) when is_binary(path) do
    with true <- Path.type(path) == :absolute,
         {:ok, %{type: :regular}} <- File.lstat(path) do
      :ok
    else
      _ -> {:error, :invalid_workflow_path}
    end
  end

  defp absolute_regular_file(_), do: {:error, :invalid_workflow_path}

  defp configured_store_matches(config, store_path) do
    with configured when is_binary(configured) <- config["store_path"],
         true <- Path.expand(configured) == Path.expand(store_path) do
      absolute_regular_file(store_path)
    else
      _ -> {:error, :store_path_mismatch}
    end
  end

  defp enabled_evidence_config do
    case Config.evidence() do
      {:ok, config} -> {:ok, config}
      _ -> {:error, :evidence_configuration_required}
    end
  end

  defp schema_three_database(path) do
    with :ok <- absolute_regular_file(path),
         {:ok, db} <- Sqlite3.open(path, mode: :readonly) do
      result =
        with {:ok, statement} <- Sqlite3.prepare(db, "PRAGMA user_version") do
          try do
            case Sqlite3.fetch_all(db, statement) do
              {:ok, [[3]]} -> :ok
              _ -> {:error, :schema_three_required}
            end
          after
            _ = Sqlite3.release(db, statement)
          end
        end

      _ = Sqlite3.close(db)
      result
    else
      {:error, _reason} -> {:error, :schema_three_required}
      _ -> {:error, :schema_three_required}
    end
  rescue
    _ -> {:error, :schema_three_required}
  end

  defp no_runtime_processes do
    if Enum.all?([SymphonyElixir.Orchestrator, WorkstreamStore, Evidence], &is_nil(Process.whereis(&1))) do
      :ok
    else
      {:error, :service_runtime_already_started}
    end
  end

  defp supported_source_policy do
    if WorkstreamRun.policy() === @supported_policy,
      do: :ok,
      else: {:error, :unsupported_service_policy}
  end

  defp fetch_run(store, run_id) do
    case WorkstreamStore.fetch(store, run_id) do
      {:ok, run} -> {:ok, run}
      :not_found -> :not_found
      {:error, _reason} -> {:error, :run_read_failed}
    end
  end

  defp expected_run(run, expected) do
    status = Map.get(expected, :expected_status)
    attempt_id = Map.get(expected, :expected_attempt_id)
    wait_id = Map.get(expected, :expected_wait_id)

    base_match? =
      WorkstreamRun.compatible_policy?(run) and
        run.policy === @supported_policy and
        run.status == status and
        run.stage_id == Map.get(expected, :expected_stage_id)

    case status do
      :waiting_for_answer ->
        wait_stage = get_in(run, [:definition, :stages, run.stage_id])

        attempt = Enum.find(run.attempts, &(&1.id == attempt_id))

        if base_match? and run.phase == :waiting_for_answer and run.current_attempt_id == attempt_id and
             is_map(wait_stage) and Map.get(wait_stage, :type) == :human_wait and
             is_map(run.pending_wait) and run.pending_wait.id == wait_id and
             is_map(attempt) and attempt.stage == run.stage_id and attempt.type == :human_wait and attempt.status == :waiting do
          :ok
        else
          {:error, :unexpected_human_wait_snapshot}
        end

      :ready ->
        if base_match? and run.phase == :queued and is_nil(run.current_attempt_id) and is_nil(run.pending_wait),
          do: :ok,
          else: {:error, :unexpected_ready_snapshot}

      _ ->
        {:error, :unsupported_run_status}
    end
  rescue
    _ -> {:error, :unexpected_run_snapshot}
  end

  defp quiescent_run(run) do
    if is_map(run.operations) and
         Enum.all?(run.operations, fn {_id, operation} ->
           case operation do
             %{status: status} when status in [:completed, :not_applied] -> not Map.has_key?(operation, :reconciliation)
             %{status: :canceled, cancellation: %{termination: :terminated}} -> not Map.has_key?(operation, :reconciliation)
             _ -> false
           end
         end) do
      :ok
    else
      {:error, :run_not_quiescent}
    end
  end

  defp merged_names(run, configured_names) do
    existing = get_in(run, [:execution, :secret_environment_names])

    if valid_names?(existing) and valid_names?(configured_names) do
      {:ok, existing ++ Enum.reject(configured_names, &(&1 in existing))}
    else
      {:error, :invalid_secret_environment_names}
    end
  end

  defp valid_names?(names) when is_list(names),
    do: Enum.all?(names, &(is_binary(&1) and Regex.match?(@environment_name, &1)))

  defp valid_names?(_), do: false

  defp evidence_identity_unchanged(before, after_run) do
    if Evidence.identity(before) === Evidence.identity(after_run),
      do: :ok,
      else: {:error, :evidence_identity_changed}
  end

  defp operator_event(names, opts) do
    %{
      "kind" => "operator_service_secret_environment_names_pinned",
      "change" => "DEV-235",
      "operator" => Keyword.get(opts, :operator, "DEV-235"),
      "environment_names" => names,
      "source_policy_sha256" => @supported_policy.executor_sha256
    }
  end

  defp event_id(run_id, names, operator) do
    digest = :crypto.hash(:sha256, Jason.encode!([run_id, names, operator])) |> Base.encode16(case: :lower)
    "operator/linear-secret-names/" <> digest
  end

  defp persist_exactly(store, original, updated, event_id, event) do
    outcome =
      case WorkstreamStore.commit(store, updated, event_id, event) do
        :ok -> :committed
        {:duplicate, run_id} when run_id == original.id -> :duplicate
        _ -> nil
      end

    with outcome when outcome in [:committed, :duplicate] <- outcome,
         {:ok, %{run_id: run_id, event: ^event}} when run_id == original.id <- WorkstreamStore.event(store, event_id),
         {:ok, persisted} <- WorkstreamStore.fetch(store, original.id),
         true <- persisted === updated,
         :ok <- names_only_change(original, persisted) do
      {:ok, outcome}
    else
      false -> {:error, :persisted_snapshot_mismatch}
      _ -> {:error, :durable_commit_failed}
    end
  rescue
    _ -> {:error, :durable_commit_failed}
  end

  defp names_only_change(before, after_run) do
    expected_execution = Map.put(before.execution, :secret_environment_names, after_run.execution.secret_environment_names)
    expected = Map.put(before, :execution, expected_execution)

    if expected === after_run,
      do: :ok,
      else: {:error, :unexpected_run_mutation}
  end

  defp verify_evidence_outbox(store, event_id, run, event, outcome) do
    with {:ok, pending} <- WorkstreamStore.pending_evidence(store) do
      case Enum.find(pending, &(&1.event_id == event_id)) do
        nil when outcome == :duplicate ->
          :ok

        nil ->
          {:error, :evidence_outbox_missing}

        entry ->
          expected = Evidence.journal(run, event)

          if expected != :invalid and entry.snapshot === expected.snapshot and entry.event === expected.event and
               Map.get(entry.snapshot, :id, Map.get(entry.snapshot, "id")) == run.id do
            :ok
          else
            {:error, :evidence_outbox_mismatch}
          end
      end
    else
      _ -> {:error, :evidence_outbox_unavailable}
    end
  end

  defp valid_operator?(operator) when is_binary(operator),
    do: Regex.match?(~r|^[A-Za-z0-9._@/-]{1,128}$|, operator)

  defp valid_operator?(_), do: false

  defp validate_operator(operator) do
    if valid_operator?(operator), do: :ok, else: {:error, :invalid_operator}
  end

  defp service_stopped(opts) do
    case service_check(opts) do
      check when is_function(check, 0) ->
        case check.() do
          :ok -> :ok
          _ -> {:error, :coordinator_service_not_stopped}
        end

      _ ->
        case System.cmd("systemctl", ["show", @service_unit, "--property=ActiveState", "--value"], stderr_to_stdout: true) do
          {"inactive\n", 0} -> :ok
          {"inactive", 0} -> :ok
          _ -> {:error, :coordinator_service_not_stopped}
        end
    end
  rescue
    _ -> {:error, :coordinator_service_not_stopped}
  end

  defp service_check(opts) when is_map(opts), do: Map.get(opts, :service_check)
  defp service_check(opts) when is_list(opts), do: Keyword.get(opts, :service_check)
  defp service_check(_opts), do: nil
end

if Mix.env() != :test, do: SymphonyElixir.Factory.PinLinearSecretNames.main()
