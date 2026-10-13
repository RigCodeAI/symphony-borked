defmodule SymphonyElixir.WorkstreamRetryMaintenance do
  @moduledoc """
  One-shot, operator-owned retry of a blocked native Linear agent attempt that failed
  before producing a coding session or candidate.

  The helper opens the configured SQLite store as its sole owner, rechecks the exact
  rejected Linear activity and current assignment, and asks the pinned worker broker for
  fresh termination proof before it can queue the original entry stage again.
  """

  alias SymphonyElixir.{WorkerOperation, WorkstreamRun, WorkstreamStore}
  alias SymphonyElixir.Linear.{AgentMapping, Delegation}

  @version 1
  @source_sha256 :crypto.hash(:sha256, File.read!(__ENV__.file)) |> Base.encode16(case: :lower)
  @event_prefix "operator-retry/"

  @doc "Plans or applies one bounded retry. `mode: :apply` is required to write the store."
  @spec run(keyword()) :: {:ok, map()} | {:error, atom()}
  def run(opts) when is_list(opts) do
    with {:ok, args} <- validate_options(opts),
         {:ok, store} <- start_store(args.store_path) do
      try do
        retry(store, args, opts)
      after
        if Process.alive?(store), do: GenServer.stop(store)
      end
    end
  rescue
    _ -> {:error, :operator_retry_unavailable}
  catch
    _, _ -> {:error, :operator_retry_unavailable}
  end

  def run(_opts), do: {:error, :invalid_operator_retry_request}

  defp retry(store, args, opts) do
    event_id = @event_prefix <> args.activity_id

    case WorkstreamStore.event(store, event_id) do
      {:ok, %{run_id: run_id, event: event}} ->
        already_applied(store, args, run_id, event)

      :not_found ->
        with {:ok, run} <- fetch_run(store, args.run_id),
             {:ok, authorization} <- authorize(store, run, args, opts),
             {:ok, termination} <- verify_termination(run, authorization.attempt, opts),
             {:ok, response} <- commit_retry(store, run, authorization, termination, event_id, args) do
          {:ok, response}
        end

      {:error, _reason} ->
        {:error, :operator_retry_store_unavailable}
    end
  end

  defp validate_options(opts) do
    allowed = [
      :mode,
      :store_path,
      :run_id,
      :attempt_id,
      :activity_id,
      :operator,
      :linear_config
    ]

    cond do
      not Keyword.keyword?(opts) or Enum.any?(Keyword.keys(opts), &(&1 not in (allowed ++ [:linear_opts, :worker_operation_opts]))) ->
        {:error, :invalid_operator_retry_request}

      Keyword.get(opts, :mode) not in [:plan, :apply] ->
        {:error, :invalid_operator_retry_mode}

      not valid_id?(Keyword.get(opts, :run_id)) or not valid_id?(Keyword.get(opts, :attempt_id)) or
        not valid_id?(Keyword.get(opts, :activity_id)) or not valid_operator?(Keyword.get(opts, :operator)) ->
        {:error, :invalid_operator_retry_identity}

      not absolute_path?(Keyword.get(opts, :store_path)) or not File.regular?(Keyword.get(opts, :store_path)) ->
        {:error, :operator_retry_store_unavailable}

      true ->
        with {:ok, config} <- validated_config(Keyword.get(opts, :linear_config)),
             true <- Keyword.get(opts, :store_path) == config["store_path"] do
          {:ok,
           %{
             mode: Keyword.fetch!(opts, :mode),
             store_path: config["store_path"],
             run_id: Keyword.fetch!(opts, :run_id),
             attempt_id: Keyword.fetch!(opts, :attempt_id),
             activity_id: Keyword.fetch!(opts, :activity_id),
             operator: Keyword.fetch!(opts, :operator),
             linear_config: config
           }}
        else
          _ -> {:error, :invalid_operator_retry_config}
        end
    end
  end

  defp validated_config(config) do
    case Delegation.validate(config) do
      {:ok, validated} -> {:ok, validated}
      _ -> {:error, :invalid_operator_retry_config}
    end
  end

  defp start_store(path) do
    case WorkstreamStore.start_link(path: path, owner: self()) do
      {:ok, store} -> {:ok, store}
      {:error, {:database_ownership_conflict, _, _}} -> {:error, :operator_retry_store_owned}
      {:error, _reason} -> {:error, :operator_retry_store_unavailable}
    end
  end

  defp fetch_run(store, run_id) do
    case WorkstreamStore.fetch(store, run_id) do
      {:ok, run} -> {:ok, run}
      :not_found -> {:error, :operator_retry_run_not_found}
      {:error, _reason} -> {:error, :operator_retry_store_unavailable}
    end
  end

  defp authorize(store, run, args, opts) do
    with :ok <- retryable_precode_failure(run, args.attempt_id),
         {:ok, records} <- WorkstreamStore.linear_load(store),
         {:ok, record} <- rejected_activity(records.events, args),
         {:ok, task} <- current_task(records.tasks, run),
         {:ok, route, issue} <- current_linear_scope(record.event, task, args.linear_config, opts) do
      {:ok, %{attempt: hd(run.attempts), operation: run.operations[args.attempt_id], record: record, task: task, route: route, issue: issue}}
    else
      {:error, _} = error -> error
      _ -> {:error, :operator_retry_not_authorized}
    end
  end

  defp retryable_precode_failure(run, attempt_id) do
    attempt = Enum.find(run.attempts, &(&1.id == attempt_id))
    operation = get_in(run, [:operations, attempt_id])
    stage = get_in(run, [:definition, :stages, run.definition.entry])

    eligible =
      run.status == :blocked and run.phase == :blocked and run.stage_id == :blocked and
        is_nil(run.current_attempt_id) and is_nil(run.retry_operation_id) and
        WorkstreamRun.compatible_policy?(run) and
        run.attempts == [attempt] and is_map(attempt) and attempt.id == attempt_id and
        attempt.ordinal == 1 and attempt.stage == run.definition.entry and attempt.type == :agent and
        attempt.status == :completed and attempt.result == %{status: :error, reason: ":agent_execution_failed"} and
        is_nil(attempt.candidate_context_id) and is_map(stage) and stage.type == :agent and
        Map.get(run.execution, :worker_control) != nil and
        Map.get(run, :thread_id) == nil and Map.get(run, :session_id) == nil and
        Map.get(run, :artifacts) == %{} and Map.get(run, :candidate_contexts) == %{} and
        Map.get(run, :current_candidate_context_id) == nil and
        not Map.has_key?(run.outputs, "candidate") and
        operation_ids(run) == [attempt_id] and is_map(operation) and operation.id == attempt_id and
        operation.attempt_id == attempt_id and operation.stage == attempt.stage and operation.type == :agent and
        operation.status == :completed and is_nil(operation[:reconciliation]) and
        Map.get(operation, :transport_retries, 0) == 0

    if eligible, do: :ok, else: {:error, :operator_retry_run_not_eligible}
  rescue
    _ -> {:error, :operator_retry_run_not_eligible}
  end

  defp operation_ids(run), do: Map.keys(run.operations) |> Enum.sort()

  defp rejected_activity(events, args) do
    matches =
      events
      |> Map.values()
      |> Enum.filter(fn record -> get_in(record, [:event, :activity_id]) == args.activity_id end)

    case matches do
      [%{id: delivery_id, status: :reply_rejected, event: event} = record] ->
        expected_body = "retry #{args.run_id} #{args.attempt_id}"

        if record.id == delivery_id and event.kind == :prompted and event.signal in [nil, "continue"] and
             event.body == expected_body and is_binary(event.session_id) and is_binary(event.issue_id) do
          {:ok, record}
        else
          {:error, :operator_retry_not_authorized}
        end

      _ ->
        {:error, :operator_retry_not_authorized}
    end
  end

  defp current_task(tasks, run) do
    case Enum.find_value(tasks, fn {_issue_id, task} -> if task[:run_id] == run.id, do: task end) do
      %{status: :acknowledged} = task ->
        # The Linear session is retained on the task; the failed coding attempt itself
        # must not have produced a session or thread on the workstream run.
        if task.issue_id == run.issue_id and run.task_id == "linear/" <> task.issue_id and
             is_binary(task.session_id) and is_binary(task.assignee_id) and
             is_nil(task[:pending_reassignment]) and is_nil(task[:stop_event_id]),
           do: {:ok, task},
           else: {:error, :operator_retry_not_authorized}

      _ ->
        {:error, :operator_retry_not_authorized}
    end
  end

  defp current_linear_scope(event, task, config, opts) do
    linear_opts = Keyword.get(opts, :linear_opts, [])

    with {:ok, route} <- AgentMapping.select(event, config),
         true <-
           route_identity(route) == task[:route] and route["agent_id"] == task.agent_id and
             event.session_id == task.session_id and event.issue_id == task.issue_id and
             event[:verified_app_user_id] == route["app_user_id"],
         {:ok, issue} <- Delegation.inspect_event(event, route, linear_opts),
         true <- issue.assignee_id == task.assignee_id do
      {:ok, route, issue}
    else
      _ -> {:error, :operator_retry_not_authorized}
    end
  rescue
    _ -> {:error, :operator_retry_not_authorized}
  catch
    _, _ -> {:error, :operator_retry_not_authorized}
  end

  defp verify_termination(run, attempt, opts) do
    operation = run.operations[attempt.id]
    identity = operation[:external_process]
    config = run.execution[:worker_control]

    case {WorkerOperation.status(config, identity, Keyword.get(opts, :worker_operation_opts, [])), identity} do
      {{:ok, %{"status" => "terminated"} = receipt}, %{"kind" => "systemd-unit", "operation_id" => id}}
      when id == attempt.id ->
        {:ok, receipt}

      _ ->
        {:error, :worker_termination_unknown}
    end
  rescue
    _ -> {:error, :worker_termination_unknown}
  catch
    _, _ -> {:error, :worker_termination_unknown}
  end

  defp commit_retry(store, run, authorization, termination, event_id, args) do
    updated =
      Map.merge(run, %{
        status: :ready,
        phase: :queued,
        stage_id: run.definition.entry,
        current_attempt_id: nil,
        retry_operation_id: nil
      })

    event = %{
      kind: :operator_precode_retry_queued,
      version: @version,
      helper_source_sha256: @source_sha256,
      run_id: run.id,
      task_id: run.task_id,
      attempt_id: authorization.attempt.id,
      activity_id: authorization.record.event.activity_id,
      linear_delivery_id: authorization.record.id,
      worker_operation_id: authorization.operation.external_process["operation_id"],
      operator: args.operator,
      termination: termination
    }

    case args.mode do
      :plan ->
        {:ok, summary(updated, authorization, event)}

      :apply ->
        case WorkstreamStore.commit(store, updated, event_id, event) do
          :ok -> {:ok, Map.put(summary(updated, authorization, event), :status, :queued)}
          {:duplicate, run_id} when run_id == run.id -> already_applied(store, args, run_id, event)
          {:duplicate, _other_run} -> {:error, :operator_retry_activity_conflict}
          {:error, _reason} -> {:error, :operator_retry_store_unavailable}
        end
    end
  end

  defp already_applied(store, args, run_id, event) do
    expected = %{
      kind: :operator_precode_retry_queued,
      version: @version,
      run_id: args.run_id,
      attempt_id: args.attempt_id,
      activity_id: args.activity_id,
      operator: args.operator
    }

    if run_id == args.run_id and is_binary(event[:task_id]) and
         Enum.all?(expected, fn {key, value} -> Map.get(event, key) == value end) do
      case fetch_run(store, args.run_id) do
        {:ok, run} -> {:ok, %{status: :already_applied, run_id: run.id, attempt_id: args.attempt_id, stage: run.stage_id}}
        _ -> {:error, :operator_retry_store_unavailable}
      end
    else
      {:error, :operator_retry_activity_conflict}
    end
  end

  defp summary(run, authorization, event) do
    %{
      status: :planned,
      run_id: run.id,
      task_id: run.task_id,
      previous_attempt_id: authorization.attempt.id,
      next_attempt_ordinal: length(run.attempts) + 1,
      stage: run.stage_id,
      branch: run.branch,
      workspace: run.workspace,
      activity_id: event.activity_id,
      worker_operation_id: event.worker_operation_id,
      policy: run.policy
    }
  end

  defp route_identity(route), do: Map.take(route, ["organization_id", "app_user_id", "oauth_client_id", "agent_id"])
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 1_024
  defp valid_operator?(value), do: is_binary(value) and String.trim(value) != "" and byte_size(value) <= 128
  defp absolute_path?(value), do: is_binary(value) and Path.type(value) == :absolute
end
