defmodule SymphonyElixir.WorkstreamPublisherPortRecovery do
  @moduledoc """
  Creates a same-task successor for the one blocked publication attempt affected by the
  PublisherGit unset-environment bug. It keeps the original run and its saved policy intact.
  """

  alias SymphonyElixir.{
    CandidateContext,
    CandidateGit,
    CandidateTransfer,
    Config,
    ControlledReviewQualification,
    Evidence,
    Publisher,
    PublisherGithub,
    Validation,
    WorkerOperation,
    WorkstreamRun,
    WorkstreamStore
  }

  alias SymphonyElixir.Linear.{AgentMapping, Delegation}

  @version 1
  @source_sha256 :crypto.hash(:sha256, File.read!(__ENV__.file)) |> Base.encode16(case: :lower)
  @old_publisher_git_sha256 "8263f004acaf5c3805e5b1196fb4aea6d273eda217e31dda331d8207c8797c94"
  @new_publisher_git_sha256 "75ad2bb3821a90d5573578d20200be0416546de3751686db4205dbb65d9188c4"
  @old_policy %{
    lifecycle_sha256: "96b3f754fae20657a74993320094ab5213735a84157f6022915099babc671545",
    executor_sha256: "ec8ae546aeb408243529f96a1a7048c92d4a61bbe268eee6decbccfb169f4c22",
    child_sha256: "a598793bd67ae523b441445143c548b0ed28e9572c471196f3f7291e20b465d5",
    validation_sha256: "47a77cb57082970ce8f6ca5515b327fc49c31a68a19482fff07893d93efc9cbf",
    gate_policy: "exit-status-v1",
    version: 1
  }
  @new_policy %{@old_policy | executor_sha256: "30c942265ee1c5b9e40f6ff36ee6dab2bbdb11b398c11bf11aae2ae27f5177c1"}
  @event_prefix "publisher-port-recovery/"

  @doc "Plans or applies the bounded successor transition. The coordinator must be stopped."
  @spec run(keyword()) :: {:ok, map()} | {:error, atom()}
  def run(opts) when is_list(opts), do: run_with_probes(opts, %{})
  def run(_opts), do: {:error, :invalid_publisher_port_recovery_request}

  @doc false
  @spec run_with_probes(keyword(), map()) :: {:ok, map()} | {:error, atom()}
  def run_with_probes(opts, probes) when is_list(opts) and is_map(probes) do
    with {:ok, args} <- validate_options(opts),
         {:ok, store} <- start_store(args.store_path) do
      try do
        recover(store, args, probes)
      after
        if Process.alive?(store), do: GenServer.stop(store)
      end
    end
  rescue
    _ -> {:error, :publisher_port_recovery_unavailable}
  catch
    _, _ -> {:error, :publisher_port_recovery_unavailable}
  end

  def run_with_probes(_opts, _probes), do: {:error, :invalid_publisher_port_recovery_request}

  defp validate_options(opts) do
    allowed = [:mode, :store_path, :run_id, :attempt_id, :activity_id, :operator, :linear_config]

    cond do
      not Keyword.keyword?(opts) or Enum.any?(Keyword.keys(opts), &(&1 not in allowed)) ->
        {:error, :invalid_publisher_port_recovery_request}

      Keyword.get(opts, :mode) not in [:plan, :apply] ->
        {:error, :invalid_publisher_port_recovery_mode}

      not valid_id?(Keyword.get(opts, :run_id)) or not valid_id?(Keyword.get(opts, :attempt_id)) or
        not valid_id?(Keyword.get(opts, :activity_id)) or not valid_operator?(Keyword.get(opts, :operator)) ->
        {:error, :invalid_publisher_port_recovery_identity}

      not absolute_path?(Keyword.get(opts, :store_path)) or not File.regular?(Keyword.get(opts, :store_path)) ->
        {:error, :publisher_port_recovery_store_unavailable}

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
          _ -> {:error, :invalid_publisher_port_recovery_config}
        end
    end
  end

  defp validated_config(config) do
    case Delegation.validate(config) do
      {:ok, validated} -> {:ok, validated}
      _ -> {:error, :invalid_publisher_port_recovery_config}
    end
  end

  defp start_store(path) do
    case WorkstreamStore.start_link(path: path, owner: self()) do
      {:ok, store} -> {:ok, store}
      {:error, {:database_ownership_conflict, _, _}} -> {:error, :publisher_port_recovery_store_owned}
      {:error, _reason} -> {:error, :publisher_port_recovery_store_unavailable}
    end
  end

  defp recover(store, args, probes) do
    origin_event_id = event_id(args.activity_id, "origin")

    case WorkstreamStore.event(store, origin_event_id) do
      {:ok, %{run_id: run_id, event: event}} -> already_applied(store, args, run_id, event)
      :not_found -> plan_or_apply(store, args, probes, origin_event_id)
      {:error, _reason} -> {:error, :publisher_port_recovery_store_unavailable}
    end
  end

  defp plan_or_apply(store, args, probes, origin_event_id) do
    with {:ok, old_run} <- fetch_run(store, args.run_id),
         :ok <- eligible_run(old_run, args),
         {:ok, records} <- WorkstreamStore.linear_load(store),
         {:ok, authorization} <- authorize(records, old_run, args, probes),
         {:ok, proof} <- verify_proofs(old_run, authorization, args, probes),
         {:ok, qualification} <- qualified_definition(old_run, probes),
         {:ok, transition} <- build_transition(old_run, authorization, proof, qualification, args, origin_event_id) do
      case args.mode do
        :plan -> {:ok, summary(transition, :planned)}
        :apply -> apply_transition(store, transition)
      end
    end
  end

  defp fetch_run(store, run_id) do
    case WorkstreamStore.fetch(store, run_id) do
      {:ok, run} -> {:ok, run}
      :not_found -> {:error, :publisher_port_recovery_run_not_found}
      {:error, _reason} -> {:error, :publisher_port_recovery_store_unavailable}
    end
  end

  defp eligible_run(run, args) do
    attempt = Enum.find(run.attempts, &(&1.id == args.attempt_id))
    operation = get_in(run, [:operations, args.attempt_id])
    publish_stage = get_in(run, [:definition, :stages, "publish"])
    producer_stage = get_in(run, [:definition, :stages, "local-commit"])
    publish_intent = if is_map(operation), do: operation[:publication_intent], else: nil
    expected_attempt_id = run.id <> "/publish/16"
    policy_source = publisher_source_sha256()

    eligible =
      args.attempt_id == expected_attempt_id and run.id == args.run_id and
        run.status == :blocked and run.phase == :blocked and run.stage_id == :blocked and
        is_nil(run.current_attempt_id) and is_nil(run.retry_operation_id) and
        run.policy == @old_policy and WorkstreamRun.policy() == @new_policy and
        policy_source == @new_publisher_git_sha256 and
        is_map(publish_stage) and publish_stage.type == :publish and
        is_map(producer_stage) and producer_stage.id == "local-commit" and producer_stage.type == :check and
        run.execution[:candidate_producer_stage] == "local-commit" and
        run.candidate_contexts == %{} and is_nil(run.current_candidate_context_id) and
        is_nil(run.execution[:candidate_context_id]) and is_nil(run.execution[:repair_parent_sha]) and
        is_map(run.outputs["candidate"]) and run.outputs["candidate"]["thread_id"] == run.thread_id and
        valid_candidate_output_pointer?(run) and
        safe_operation_history?(run) and
        is_map(attempt) and attempt.id == args.attempt_id and attempt.ordinal == 16 and
        attempt.stage == "publish" and attempt.type == :publish and attempt.status == :completed and
        attempt.gate == :failed and attempt.gate_detail["verdict"] == "failed" and
        attempt.gate_detail["verified"] == false and publication_failure?(attempt.result) and
        passed_candidate_gates?(run, operation, publish_stage) and
        is_map(operation) and operation.id == args.attempt_id and operation.attempt_id == args.attempt_id and
        operation.type == :publish and operation.status == :completed and is_nil(operation[:reconciliation]) and
        valid_intent?(publish_intent, run, publish_stage)

    if eligible, do: :ok, else: {:error, :publisher_port_recovery_run_not_eligible}
  rescue
    _ -> {:error, :publisher_port_recovery_run_not_eligible}
  end

  defp safe_operation_history?(run) do
    operations = run.operations
    attempts = Map.new(run.attempts, &{&1.id, &1})

    is_map(operations) and map_size(operations) == map_size(attempts) and
      Enum.all?(operations, fn {id, operation} ->
        attempt = attempts[id]

        case {operation, attempt} do
          {%{id: ^id, status: :completed, attempt_id: ^id, stage: operation_stage, type: operation_type}, %{id: ^id, stage: attempt_stage, status: :completed, type: attempt_type}}
          when operation_stage == attempt_stage and operation_type == attempt_type ->
            is_nil(operation[:reconciliation])

          {%{id: ^id, status: :not_applied, attempt_id: ^id, stage: "publish", type: :publish}, %{id: ^id, stage: "publish", type: :publish, status: :interrupted, ordinal: ordinal}}
          when ordinal in 6..15 ->
            id == "#{run.id}/publish/#{ordinal}" and
              operation[:reconciliation] == ":validation_not_permanently_archived" and
              operation[:transport_retries] == ordinal - 5 and is_nil(operation[:publication_intent]) and
              is_nil(operation[:external_process])

          _ ->
            false
        end
      end)
  end

  defp valid_candidate_output_pointer?(run) do
    output = run.outputs["candidate"]
    artifact = run.artifacts["candidate"]
    expected_attempt_id = run.id <> "/implement/2"

    is_map(artifact) and artifact[:run_id] == run.id and artifact[:attempt_id] == expected_attempt_id and
      artifact[:sha256] == digest(:erlang.term_to_binary(output))
  end

  defp valid_intent?(intent, run, stage) when is_map(intent) do
    is_binary(intent[:candidate_sha]) and Regex.match?(~r/\A[a-f0-9]{40}\z/, intent.candidate_sha) and
      intent[:branch] == Publisher.branch(run) and is_binary(intent[:publication_id]) and
      is_binary(intent[:validation_attempt_id]) and valid_digest?(intent[:evidence_revision]) and
      stage.gate[:validation_stage] == "validate"
  end

  defp valid_intent?(_intent, _run, _stage), do: false

  defp publication_failure?(%{status: :error, reason: reason}) when is_binary(reason),
    do: String.contains?(reason, "publication_completion_not_verified")

  defp publication_failure?(_result), do: false

  defp passed_candidate_gates?(run, publish_operation, publish_stage) do
    validation_id = publish_operation.publication_intent[:validation_attempt_id]
    validation_index = Enum.find_index(run.attempts, &(&1.id == validation_id))

    with true <- is_integer(validation_index),
         validation when is_map(validation) <- Enum.at(run.attempts, validation_index),
         true <- validation.stage == publish_stage.gate.validation_stage and validation.gate == :passed,
         true <- passed_check_before?(run.attempts, validation_index, "local-commit"),
         true <- passed_check_before?(run.attempts, validation_index, "inspect-candidate") do
      true
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  defp passed_check_before?(attempts, before_index, stage_id) do
    attempts
    |> Enum.take(before_index)
    |> Enum.reverse()
    |> Enum.find(&(&1.stage == stage_id))
    |> case do
      %{type: :check, status: :completed, gate: :passed, candidate_context_id: nil} -> true
      _ -> false
    end
  end

  defp authorize(records, run, args, probes) do
    with {:ok, record} <- rejected_activity(records.events, args),
         {:ok, task} <- current_task(records.tasks, run),
         {:ok, route, issue} <- current_linear_scope(record.event, task, args.linear_config, probes) do
      {:ok, %{record: record, task: task, route: route, issue: issue}}
    else
      {:error, _} = error -> error
      _ -> {:error, :publisher_port_recovery_not_authorized}
    end
  end

  defp rejected_activity(events, args) do
    matches =
      events
      |> Map.values()
      |> Enum.filter(fn record -> get_in(record, [:event, :activity_id]) == args.activity_id end)

    case matches do
      [%{status: :reply_rejected, event: event} = record] ->
        if event.kind == :prompted and event.signal in [nil, "continue"] and
             event.body == "retry #{args.run_id} #{args.attempt_id}" and
             is_binary(event.session_id) and is_binary(event.issue_id) do
          {:ok, record}
        else
          {:error, :publisher_port_recovery_not_authorized}
        end

      _ ->
        {:error, :publisher_port_recovery_not_authorized}
    end
  end

  defp current_task(tasks, run) do
    case Enum.find_value(tasks, fn {_issue_id, task} -> if task[:run_id] == run.id, do: task end) do
      %{status: :acknowledged} = task ->
        if task.issue_id == run.issue_id and run.task_id == "linear/" <> task.issue_id and
             is_binary(task.session_id) and is_binary(task.assignee_id) and
             is_nil(task[:pending_reassignment]) and is_nil(task[:stop_event_id]) do
          {:ok, task}
        else
          {:error, :publisher_port_recovery_not_authorized}
        end

      _ ->
        {:error, :publisher_port_recovery_not_authorized}
    end
  end

  defp current_linear_scope(event, task, config, probes) do
    with {:ok, route} <- AgentMapping.select(event, config),
         true <-
           route_identity(route) == task[:route] and route["agent_id"] == task.agent_id and
             event.session_id == task.session_id and event.issue_id == task.issue_id and
             event[:verified_app_user_id] == route["app_user_id"],
         {:ok, issue} <- inspect_current_issue(event, route, probes),
         true <- issue.assignee_id == task.assignee_id do
      {:ok, route, issue}
    else
      _ -> {:error, :publisher_port_recovery_not_authorized}
    end
  rescue
    _ -> {:error, :publisher_port_recovery_not_authorized}
  catch
    _, _ -> {:error, :publisher_port_recovery_not_authorized}
  end

  defp inspect_current_issue(event, route, probes) do
    case probes[:linear_scope] do
      fun when is_function(fun, 2) -> fun.(event, route)
      _ -> Delegation.inspect_event(event, route, [])
    end
  end

  defp verify_proofs(run, authorization, args, probes) do
    attempt_id = run.id <> "/implement/2"
    attempt = Enum.find(run.attempts, &(&1.id == attempt_id))
    operation = get_in(run, [:operations, attempt_id])

    with :ok <- successful_implementation(attempt, operation),
         {:ok, termination} <- verify_termination(run, operation, probes),
         {:ok, candidate} <- verify_candidate(run, operation, probes),
         :ok <- matching_candidate_intent(candidate, get_in(run, [:operations, args.attempt_id, :publication_intent])),
         {:ok, validation} <- verify_validation(run, args.attempt_id, candidate, probes),
         {:ok, archive} <- verify_archive(run, validation, get_in(run, [:operations, args.attempt_id, :publication_intent]), probes),
         {:ok, remote} <- verify_remote_absent(run, args.linear_config, probes) do
      {:ok,
       %{
         implementation_attempt_id: attempt_id,
         worker_operation_id: operation.id,
         termination: termination,
         candidate: candidate,
         validation: validation,
         archive: archive,
         remote: remote,
         linear: %{delivery_id: authorization.record.id, activity_id: authorization.record.event.activity_id}
       }}
    else
      {:error, _} = error -> error
      _ -> {:error, :publisher_port_recovery_proof_unavailable}
    end
  rescue
    _ -> {:error, :publisher_port_recovery_proof_unavailable}
  catch
    _, _ -> {:error, :publisher_port_recovery_proof_unavailable}
  end

  defp successful_implementation(
         %{stage: "implement", type: :agent, status: :completed, result: %{status: :ok}},
         %{status: :completed, type: :agent, id: id, attempt_id: id}
       ) do
    if String.ends_with?(id, "/implement/2"), do: :ok, else: {:error, :publisher_port_recovery_worker_unknown}
  end

  defp successful_implementation(_attempt, _operation), do: {:error, :publisher_port_recovery_worker_unknown}

  defp verify_termination(run, operation, probes) do
    identity = operation[:external_process]
    control = run.execution[:worker_control]

    with %{"kind" => "systemd-unit", "operation_id" => operation_id} <- identity,
         true <- operation_id == operation.id,
         {:ok, %{"status" => "terminated", "operation_id" => ^operation_id} = receipt} <- worker_status(control, identity, probes),
         true <- receipt["identity"] == identity do
      {:ok, receipt}
    else
      _ -> {:error, :publisher_port_recovery_worker_unknown}
    end
  rescue
    _ -> {:error, :publisher_port_recovery_worker_unknown}
  end

  defp worker_status(control, identity, probes) do
    case probes[:worker_status] do
      fun when is_function(fun, 2) -> fun.(control, identity)
      _ -> WorkerOperation.status(control, identity)
    end
  end

  defp verify_candidate(run, operation, probes) do
    case probes[:candidate] do
      fun when is_function(fun, 1) ->
        case fun.(run) do
          {:ok, %{workspace: _workspace, head_sha: _head, status: ""} = candidate} ->
            {:ok, Map.put(candidate, :operation_id, operation.id)}

          _ ->
            {:error, :publisher_port_recovery_candidate_changed}
        end

      _ ->
        with {:ok, workspace} <- CandidateTransfer.workspace(run),
             {:ok, head} <- CandidateGit.run(workspace, ["rev-parse", "--verify", "HEAD^{commit}"]),
             {:ok, status} <- CandidateGit.run(workspace, ["status", "--porcelain=v1", "--untracked-files=normal", "--ignore-submodules=all"]),
             head <- String.trim(head),
             true <- Regex.match?(~r/\A[a-f0-9]{40}\z/, head) and status == "" do
          {:ok, %{workspace: workspace, head_sha: head, status: status, operation_id: operation.id}}
        else
          _ -> {:error, :publisher_port_recovery_candidate_changed}
        end
    end
  rescue
    _ -> {:error, :publisher_port_recovery_candidate_changed}
  end

  defp matching_candidate_intent(candidate, intent) do
    if candidate.head_sha == intent.candidate_sha,
      do: :ok,
      else: {:error, :publisher_port_recovery_candidate_changed}
  end

  defp verify_validation(run, publish_attempt_id, candidate, probes) do
    intent = get_in(run, [:operations, publish_attempt_id, :publication_intent])
    validation_stage = run.definition.stages[intent.validation_attempt_id |> String.split("/") |> Enum.at(-2)] || run.definition.stages["validate"]
    attempt = Enum.find(run.attempts, &(&1.id == intent.validation_attempt_id))
    receipt = get_in(attempt || %{}, [:result, :evidence, "receipt"])

    with true <- is_map(attempt),
         true <- attempt.stage == "validate" and attempt.type == :check and attempt.status == :completed and attempt.gate == :passed,
         true <- is_map(validation_stage) and validation_stage.gate[:evaluator] == :candidate_validation,
         true <- is_map(receipt),
         {:ok, verified} <- validate_candidate_receipt(run, validation_stage, attempt, receipt, candidate, probes),
         true <- verified.manifest["candidate_sha"] == intent.candidate_sha do
      {:ok, Map.merge(verified, %{attempt: attempt, receipt: receipt, stage: validation_stage})}
    else
      _ -> {:error, :publisher_port_recovery_validation_unavailable}
    end
  rescue
    _ -> {:error, :publisher_port_recovery_validation_unavailable}
  end

  defp validate_candidate_receipt(run, stage, attempt, receipt, candidate, probes) do
    case probes[:validation] do
      fun when is_function(fun, 4) ->
        fun.(run, stage, attempt, candidate)

      _ ->
        with {:ok, context} <- CandidateContext.validation_context(run, attempt.id),
             opts = [workspace: candidate.workspace, validation_archive: run.execution[:validation_archive], validation_scratch: run.execution[:validation_scratch]],
             {:ok, %{verdict: :passed} = validation_gate} <-
               Validation.verify_result({:ok, %{receipt: receipt}}, run.execution[:validation_policy], context, stage.gate.required, opts),
             {:ok, manifest} <- Validation.read_receipt(receipt, run.execution[:validation_archive]),
             publish_stage = run.definition.stages["publish"],
             {:ok, %{verdict: :passed} = publication_gate} <-
               Validation.verify_result({:ok, %{receipt: receipt}}, run.execution[:validation_policy], context, publish_stage.gate.required, opts) do
          {:ok, %{manifest: manifest, validation_gate: validation_gate, publication_gate: publication_gate}}
        end
    end
  end

  defp verify_archive(run, validation, intent, probes) do
    case probes[:archive] do
      fun when is_function(fun, 4) ->
        fun.(run, validation, intent, nil)

      _ ->
        with {:ok, config} <- Config.evidence(),
             %{snapshot: snapshot} <- Evidence.journal(run, %{}),
             {:reply, {:ok, archive}, _state} <-
               Evidence.handle_call(
                 {:publication_revision, snapshot, validation.attempt.id, validation.receipt, intent.evidence_revision},
                 nil,
                 # Evidence receipts are issued by the root uploader (UID 0), regardless of root directory owner.
                 %{root: Path.expand(config["root"]), receipt_uid: 0}
               ),
             true <- archive.revision == intent.evidence_revision and archive.validation_attempt_id == validation.attempt.id do
          {:ok, archive}
        else
          _ -> {:error, :publisher_port_recovery_archive_unavailable}
        end
    end
  rescue
    _ -> {:error, :publisher_port_recovery_archive_unavailable}
  end

  defp verify_remote_absent(run, _linear_config, probes) do
    publisher_config = run.execution[:publisher]
    branch = Publisher.branch(run)

    case probes[:remote] do
      fun when is_function(fun, 2) ->
        case fun.(publisher_config, branch) do
          {:ok, %{branch_sha: nil, pull_request: nil} = result} -> {:ok, Map.put(result, :branch, branch)}
          _ -> {:error, :publisher_port_recovery_remote_exists}
        end

      _ ->
        with :ok <- PublisherGithub.validate_config(publisher_config),
             {:ok, nil} <- PublisherGithub.branch_sha(publisher_config, branch),
             {:ok, nil} <- PublisherGithub.find_pr(publisher_config, branch) do
          {:ok, %{branch: branch, branch_sha: nil, pull_request: nil, app_authenticated: true}}
        else
          _ -> {:error, :publisher_port_recovery_remote_exists}
        end
    end
  rescue
    _ -> {:error, :publisher_port_recovery_remote_exists}
  end

  defp qualified_definition(old_run, probes) do
    with {:ok, path} <- qualification_source_path(old_run.definition, probes),
         {:ok, definition, provenance} <- ControlledReviewQualification.successor_definition(old_run.definition, path),
         true <- ControlledReviewQualification.valid_transition?(old_run.definition, definition, provenance) do
      {:ok, %{definition: definition, provenance: provenance}}
    else
      _ -> {:error, :publisher_port_recovery_qualification_unavailable}
    end
  rescue
    _ -> {:error, :publisher_port_recovery_qualification_unavailable}
  end

  defp qualification_source_path(definition, %{qualification_source_path: source_path}) when is_function(source_path, 1),
    do: source_path.(definition)

  defp qualification_source_path(definition, _probes), do: ControlledReviewQualification.source_path(definition)

  defp build_transition(old_run, authorization, proof, qualification, args, origin_event_id) do
    intent = old_run.operations[args.attempt_id].publication_intent
    successor_id = successor_id(old_run.id, args.activity_id)
    successor_event_id = event_id(args.activity_id, "successor")
    successor_definition = qualification.definition

    successor_run =
      WorkstreamRun.new(old_run.task_id, successor_definition, old_run.workspace, [issue_id: old_run.issue_id, branch: old_run.branch], old_run.execution)
      |> Map.merge(%{
        id: successor_id,
        stage_id: "local-commit",
        thread_id: old_run.thread_id,
        session_id: old_run.session_id,
        outputs: Map.put(successor_definition.input_values, "candidate", old_run.outputs["candidate"]),
        repairs: old_run.repairs,
        questions: old_run.questions,
        inbox: old_run.inbox,
        activity_ids: old_run.activity_ids,
        continuation: old_run.continuation,
        validation_repair_rounds: old_run.validation_repair_rounds,
        review_repair_rounds: old_run.review_repair_rounds,
        review_fix_completions: old_run.review_fix_completions,
        reviewer_sessions: old_run.reviewer_sessions,
        transport_retries: old_run.transport_retries,
        recovery_parent_run_id: old_run.id
      })

    stopped_run =
      Map.merge(old_run, %{
        status: :stopped,
        phase: :stopped,
        recovery_successor_run_id: successor_id
      })

    successor_task =
      Map.merge(authorization.task, %{
        run_id: successor_id,
        authorized_stage: nil,
        authorized_until: 0,
        owned_child_grant: nil,
        check_at: 0,
        scope_revision: (authorization.task[:scope_revision] || 0) + 1
      })

    provenance = %{
      version: @version,
      helper_source_sha256: @source_sha256,
      old_run_id: old_run.id,
      successor_run_id: successor_id,
      task_id: old_run.task_id,
      issue_id: old_run.issue_id,
      activity_id: args.activity_id,
      linear_delivery_id: authorization.record.id,
      operator: args.operator,
      failed_publish_attempt_id: args.attempt_id,
      implementation_attempt_id: proof.implementation_attempt_id,
      worker_operation_id: proof.worker_operation_id,
      worker_termination_sha256: digest(Jason.encode!(proof.termination)),
      worker_termination_status: proof.termination["status"],
      candidate_sha: intent.candidate_sha,
      candidate_handoff: %{
        version: 1,
        parent_run_id: old_run.id,
        producer_attempt_id: proof.implementation_attempt_id,
        candidate_output_id: old_run.artifacts["candidate"],
        candidate_output_sha256: old_run.artifacts["candidate"].sha256,
        candidate_sha: intent.candidate_sha
      },
      branch: intent.branch,
      validation_attempt_id: proof.validation.attempt.id,
      validation_evidence_revision: intent.evidence_revision,
      validation_manifest_sha256: proof.validation.receipt["manifest_sha256"],
      archive_revision: proof.archive.revision,
      remote_branch_absent: proof.remote.branch_sha == nil,
      remote_pr_absent: proof.remote.pull_request == nil,
      old_policy: old_run.policy,
      new_policy: @new_policy,
      old_publisher_git_sha256: @old_publisher_git_sha256,
      new_publisher_git_sha256: @new_publisher_git_sha256,
      controlled_review_provenance: qualification.provenance
    }

    origin_event = Map.merge(provenance, %{kind: :publisher_port_recovery_origin_stopped, event_id: origin_event_id})

    successor_event =
      Map.merge(provenance, %{
        kind: :publisher_port_recovery_successor_queued,
        event_id: successor_event_id,
        stage: successor_run.stage_id
      })

    {:ok,
     %{
       expected_run: old_run,
       stopped_run: stopped_run,
       successor_run: successor_run,
       qualification: qualification,
       expected_task: authorization.task,
       successor_task: successor_task,
       reply_record: authorization.record,
       origin_event_id: origin_event_id,
       origin_event: origin_event,
       successor_event_id: successor_event_id,
       successor_event: successor_event,
       proof: proof,
       intent: intent
     }}
  rescue
    _ -> {:error, :publisher_port_recovery_invalid_successor}
  end

  defp apply_transition(store, transition) do
    recovery = Map.drop(transition, [:proof, :intent])

    case WorkstreamStore.commit_publisher_port_recovery_successor(store, recovery) do
      {:ok, _runs} -> {:ok, summary(transition, :queued)}
      {:duplicate, _runs} -> {:ok, summary(transition, :already_applied)}
      {:error, _reason} -> {:error, :publisher_port_recovery_store_conflict}
    end
  end

  defp already_applied(store, args, run_id, origin_event) do
    successor_id = origin_event[:successor_run_id]
    successor_event_id = event_id(args.activity_id, "successor")

    expected =
      origin_event[:kind] == :publisher_port_recovery_origin_stopped and
        origin_event[:event_id] == event_id(args.activity_id, "origin") and
        origin_event[:old_run_id] == args.run_id and origin_event[:successor_run_id] == successor_id and
        origin_event[:failed_publish_attempt_id] == args.attempt_id and
        origin_event[:activity_id] == args.activity_id and origin_event[:operator] == args.operator and
        origin_event[:helper_source_sha256] == @source_sha256 and
        origin_event[:old_policy] == @old_policy and origin_event[:new_policy] == @new_policy and
        origin_event[:old_publisher_git_sha256] == @old_publisher_git_sha256 and
        origin_event[:new_publisher_git_sha256] == @new_publisher_git_sha256

    with true <- run_id == args.run_id and expected and is_binary(successor_id),
         {:ok, %{run_id: ^successor_id, event: successor_event}} <- WorkstreamStore.event(store, successor_event_id),
         true <-
           successor_event[:kind] == :publisher_port_recovery_successor_queued and
             successor_event[:old_run_id] == args.run_id and successor_event[:successor_run_id] == successor_id and
             successor_event[:event_id] == successor_event_id and
             successor_event[:helper_source_sha256] == @source_sha256 and
             successor_event[:controlled_review_provenance] == origin_event[:controlled_review_provenance],
         {:ok, old_run} <- WorkstreamStore.fetch(store, args.run_id),
         {:ok, successor_run} <- WorkstreamStore.fetch(store, successor_id),
         {:ok, records} <- WorkstreamStore.linear_load(store),
         task when is_map(task) <- records.tasks[old_run.issue_id],
         true <-
           task[:run_id] == successor_id and old_run[:recovery_successor_run_id] == successor_id and
             successor_run[:recovery_parent_run_id] == old_run.id and
             ControlledReviewQualification.valid_transition?(
               old_run.definition,
               successor_run.definition,
               origin_event[:controlled_review_provenance]
             ) do
      {:ok, %{status: :already_applied, old_run_id: old_run.id, successor_run_id: successor_run.id, stage: successor_run.stage_id}}
    else
      _ -> {:error, :publisher_port_recovery_activity_conflict}
    end
  rescue
    _ -> {:error, :publisher_port_recovery_store_unavailable}
  end

  defp summary(transition, status) do
    successor = transition.successor_run
    proof = transition.proof

    %{
      status: status,
      old_run_id: transition.expected_run.id,
      successor_run_id: successor.id,
      task_id: successor.task_id,
      stage: successor.stage_id,
      workspace: successor.workspace,
      branch: transition.intent.branch,
      candidate_sha: transition.intent.candidate_sha,
      worker_operation_id: proof.worker_operation_id,
      validation_attempt_id: proof.validation.attempt.id,
      evidence_revision: proof.archive.revision,
      activity_id: transition.origin_event.activity_id,
      old_policy: transition.expected_run.policy,
      new_policy: successor.policy
    }
  end

  defp event_id(activity_id, suffix), do: @event_prefix <> digest(activity_id) <> "/" <> suffix

  defp successor_id(run_id, activity_id),
    do: "run-" <> (digest(run_id <> "\n" <> activity_id) |> binary_part(0, 32))

  defp publisher_source_sha256 do
    Path.join(__DIR__, "publisher_git.ex")
    |> File.read!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp route_identity(route), do: Map.take(route, ["organization_id", "app_user_id", "oauth_client_id", "agent_id"])
  defp digest(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
  defp valid_digest?(value), do: is_binary(value) and Regex.match?(~r/\A[a-f0-9]{64}\z/, value)
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 1_024
  defp valid_operator?(value), do: is_binary(value) and String.trim(value) != "" and byte_size(value) <= 128
  defp absolute_path?(value), do: is_binary(value) and Path.type(value) == :absolute
end
