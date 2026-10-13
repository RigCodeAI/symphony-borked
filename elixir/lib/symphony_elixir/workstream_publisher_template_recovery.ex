defmodule SymphonyElixir.WorkstreamPublisherTemplateRecovery do
  @moduledoc """
  Creates one same-task successor for the qualified DEV-259 publication that pushed its
  candidate but could not create a PR because the pinned base has no PR template.
  """

  alias SymphonyElixir.{
    CandidateContext,
    CandidateGit,
    CandidateTransfer,
    Config,
    ControlledReviewQualification,
    Evidence,
    PublisherGithub,
    Validation,
    WorkerOperation,
    WorkstreamRun,
    WorkstreamStore
  }

  alias SymphonyElixir.Linear.{AgentMapping, Delegation}

  @version 1
  @source_sha256 :crypto.hash(:sha256, File.read!(__ENV__.file)) |> Base.encode16(case: :lower)
  @ancestor_run_id "run-800b6eb2fa6aae291689e3a3fcc37e18"
  @run_id "run-9b54a568bc22f61fca60a2b9327e44f8"
  @issue_id "f3cf6638-d7a1-42e9-871f-90969bfe3289"
  @task_id "linear/" <> @issue_id
  @attempt_id @run_id <> "/publish/16"
  @candidate_sha "97219444e808b3bb55dc6717ddcb29cdeb687c04"
  @branch "cycle/factory-e440f747273786859d453798"
  @base_sha "fbb1819a1098a4a740d07e0b89e607e2866a518d"
  @candidate_input_sha256 "115801d20b7de99fcaf4780a88645dcf92aef5b9f72815141478e9339382c112"
  @prior_activity_id "e54c6a27-3f17-421b-9899-1e0f8ce815c4"
  @prior_helper_sha256 "722704131ba289b4c98e3e6f9ddf77cd3779769df562d6f29ff73bdd7e2db866"
  @old_publisher_sha256 "16abf59c07e5d34c811b408dbe974f06aad51ea495a777c4b0fa216ffbe0d460"
  @new_publisher_sha256 "a043e8b47560d3871a62dd48ebcddf992bd0f4a423ecef34d22aa698af30ad97"
  @policy_base %{
    lifecycle_sha256: "96b3f754fae20657a74993320094ab5213735a84157f6022915099babc671545",
    child_sha256: "a598793bd67ae523b441445143c548b0ed28e9572c471196f3f7291e20b465d5",
    validation_sha256: "47a77cb57082970ce8f6ca5515b327fc49c31a68a19482fff07893d93efc9cbf",
    gate_policy: "exit-status-v1",
    version: 1
  }
  @parent_policy Map.put(@policy_base, :executor_sha256, "30c942265ee1c5b9e40f6ff36ee6dab2bbdb11b398c11bf11aae2ae27f5177c1")
  @new_policy Map.put(@policy_base, :executor_sha256, "2a017e45eeae8856e4d9998f0fdd9d927e952d84090d0b188220ac899161cdc0")
  @event_prefix "publisher-template-recovery/"
  @template_paths [".github/pull_request_template.md", ".github/PULL_REQUEST_TEMPLATE.md"]

  @doc "Plans or applies the one qualified successor transition. The coordinator must be stopped."
  @spec run(keyword()) :: {:ok, map()} | {:error, atom()}
  def run(opts) when is_list(opts), do: run_with_probes(opts, %{})
  def run(_opts), do: {:error, :invalid_publisher_template_recovery_request}

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
    _ -> {:error, :publisher_template_recovery_unavailable}
  catch
    _, _ -> {:error, :publisher_template_recovery_unavailable}
  end

  def run_with_probes(_opts, _probes), do: {:error, :invalid_publisher_template_recovery_request}

  defp validate_options(opts) do
    allowed = [:mode, :store_path, :run_id, :attempt_id, :activity_id, :operator, :linear_config]

    cond do
      not Keyword.keyword?(opts) or Enum.any?(Keyword.keys(opts), &(&1 not in allowed)) ->
        {:error, :invalid_publisher_template_recovery_request}

      Keyword.get(opts, :mode) not in [:plan, :apply] ->
        {:error, :invalid_publisher_template_recovery_mode}

      Keyword.get(opts, :run_id) != @run_id or Keyword.get(opts, :attempt_id) != @attempt_id or
        not valid_id?(Keyword.get(opts, :activity_id)) or not valid_operator?(Keyword.get(opts, :operator)) ->
        {:error, :invalid_publisher_template_recovery_identity}

      not absolute_path?(Keyword.get(opts, :store_path)) or not File.regular?(Keyword.get(opts, :store_path)) ->
        {:error, :publisher_template_recovery_store_unavailable}

      true ->
        with {:ok, config} <- validated_config(Keyword.get(opts, :linear_config)),
             true <- Keyword.get(opts, :store_path) == config["store_path"] do
          {:ok,
           %{
             mode: Keyword.fetch!(opts, :mode),
             store_path: config["store_path"],
             run_id: @run_id,
             attempt_id: @attempt_id,
             activity_id: Keyword.fetch!(opts, :activity_id),
             operator: Keyword.fetch!(opts, :operator),
             linear_config: config
           }}
        else
          _ -> {:error, :invalid_publisher_template_recovery_config}
        end
    end
  end

  defp validated_config(config) do
    case Delegation.validate(config) do
      {:ok, validated} -> {:ok, validated}
      _ -> {:error, :invalid_publisher_template_recovery_config}
    end
  end

  defp start_store(path) do
    case WorkstreamStore.start_link(path: path, owner: self()) do
      {:ok, store} -> {:ok, store}
      {:error, {:database_ownership_conflict, _, _}} -> {:error, :publisher_template_recovery_store_owned}
      {:error, _} -> {:error, :publisher_template_recovery_store_unavailable}
    end
  end

  defp recover(store, args, probes) do
    origin_id = event_id(args.activity_id, "origin")

    case WorkstreamStore.event(store, origin_id) do
      {:ok, %{run_id: run_id, event: event}} -> already_applied(store, args, run_id, event)
      :not_found -> plan_or_apply(store, args, probes, origin_id)
      {:error, _} -> {:error, :publisher_template_recovery_store_unavailable}
    end
  end

  defp plan_or_apply(store, args, probes, origin_event_id) do
    with {:ok, ancestor} <- fetch_run(store, @ancestor_run_id),
         {:ok, run} <- fetch_run(store, args.run_id),
         {:ok, prior} <- prior_recovery(store, ancestor, run),
         :ok <- eligible_runs(ancestor, run, prior),
         {:ok, records} <- WorkstreamStore.linear_load(store),
         {:ok, authorization} <- authorize(records, run, args, probes),
         {:ok, proof} <- verify_proofs(ancestor, run, prior, args, probes),
         {:ok, transition} <- build_transition(ancestor, run, prior, authorization, proof, args, origin_event_id) do
      case args.mode do
        :plan -> {:ok, summary(transition, :planned)}
        :apply -> apply_transition(store, transition)
      end
    end
  end

  defp fetch_run(store, run_id) do
    case WorkstreamStore.fetch(store, run_id) do
      {:ok, run} -> {:ok, run}
      :not_found -> {:error, :publisher_template_recovery_run_not_found}
      {:error, _} -> {:error, :publisher_template_recovery_store_unavailable}
    end
  end

  defp eligible_runs(ancestor, run, prior) do
    valid =
      WorkstreamRun.policy() == @new_policy and publisher_source_sha256() == @new_publisher_sha256 and
        ancestor.id == @ancestor_run_id and ancestor.task_id == @task_id and ancestor.issue_id == @issue_id and
        ancestor.status == :stopped and ancestor.phase == :stopped and ancestor.stage_id == :blocked and
        ancestor.policy == policy_with_executor("ec8ae546aeb408243529f96a1a7048c92d4a61bbe268eee6decbccfb169f4c22") and
        ancestor.recovery_successor_run_id == run.id and run.id == @run_id and run.task_id == @task_id and
        run.issue_id == @issue_id and {run.status, run.phase} in [{:blocked, :blocked}, {:stopped, :stopped}] and run.stage_id == :blocked and
        run.policy == @parent_policy and run.recovery_parent_run_id == ancestor.id and
        is_nil(run[:recovery_successor_run_id]) and run.branch == @branch and ancestor.branch == @branch and
        run.workspace == ancestor.workspace and run.thread_id == ancestor.thread_id and run.session_id == ancestor.session_id and
        run.execution == ancestor.execution and run.definition != nil and
        ControlledReviewQualification.valid_transition?(ancestor.definition, run.definition, prior.origin[:controlled_review_provenance]) and
        is_nil(run.current_attempt_id) and is_nil(run.retry_operation_id) and run.candidate_contexts == %{} and
        is_nil(run.current_candidate_context_id) and is_nil(run.execution[:candidate_context_id]) and
        is_nil(run.execution[:repair_parent_sha]) and is_map(run.outputs["candidate"]) and
        run.outputs["candidate"]["thread_id"] == run.thread_id and safe_history?(run) and
        valid_prior_candidate_handoff?(ancestor) and valid_prior_run_link?(ancestor, run)

    if valid, do: :ok, else: {:error, :publisher_template_recovery_run_not_eligible}
  rescue
    _ -> {:error, :publisher_template_recovery_run_not_eligible}
  end

  defp policy_with_executor(executor), do: Map.put(@policy_base, :executor_sha256, executor)

  defp valid_prior_candidate_handoff?(ancestor) do
    attempt_id = ancestor.id <> "/implement/2"
    attempt = Enum.find(ancestor.attempts, &(&1.id == attempt_id))
    operation = get_in(ancestor, [:operations, attempt_id])
    output = get_in(ancestor, [:outputs, "candidate"])
    artifact = get_in(ancestor, [:artifacts, "candidate"])

    match?(%{stage: "implement", type: :agent, status: :completed, result: %{status: :ok, evidence: ^output}}, attempt) and
      is_map(operation) and operation[:status] == :completed and operation[:type] == :agent and
      is_map(artifact) and artifact[:run_id] == ancestor.id and artifact[:attempt_id] == attempt_id and
      artifact[:sha256] == digest(:erlang.term_to_binary(output))
  rescue
    _ -> false
  end

  defp valid_prior_run_link?(ancestor, run) do
    origin_id = previous_event_id("origin")
    successor_id = previous_event_id("successor")

    ancestor.recovery_successor_run_id == run.id and run.recovery_parent_run_id == ancestor.id and
      is_binary(origin_id) and is_binary(successor_id)
  end

  defp safe_history?(run) do
    attempts = run.attempts
    operations = run.operations

    length(attempts) == 16 and map_size(operations) == 16 and
      Enum.with_index(attempts, 1)
      |> Enum.all?(fn {attempt, ordinal} ->
        operation = operations[attempt.id]

        case {attempt, operation, ordinal} do
          {%{id: id, stage: stage, type: type, ordinal: ^ordinal, status: :completed, gate: :passed},
           %{id: operation_id, attempt_id: operation_attempt_id, stage: operation_stage, type: operation_type, status: :completed} = operation, n}
          when n in 1..3 ->
            id == operation_id and id == operation_attempt_id and stage == operation_stage and type == operation_type and
              id == "#{run.id}/#{stage}/#{n}" and stage == Enum.at(["local-commit", "inspect-candidate", "validate"], n - 1) and
              is_nil(operation[:reconciliation])

          {%{id: id, stage: "publish", type: :publish, ordinal: ^ordinal, status: :interrupted},
           %{id: operation_id, attempt_id: operation_attempt_id, stage: "publish", type: :publish, status: :not_applied} = operation, n}
          when n in 4..15 ->
            id == operation_id and id == operation_attempt_id and id == "#{run.id}/publish/#{n}" and
              operation[:reconciliation] == ":validation_not_permanently_archived" and
              operation[:transport_retries] == n - 3 and is_nil(operation[:publication_intent]) and is_nil(operation[:external_process])

          {%{id: id, stage: "publish", type: :publish, ordinal: 16, status: :completed, gate: :failed, gate_detail: %{"verdict" => "failed", "verified" => false}},
           %{id: operation_id, attempt_id: operation_attempt_id, stage: "publish", type: :publish, status: :completed} = operation, 16} ->
            id == operation_id and id == operation_attempt_id and id == @attempt_id and publication_failure?(attempt.result) and
              is_map(operation[:publication_intent]) and
              operation[:publication_observation]["remote_sha"] == @candidate_sha and
              operation[:publication_observation]["pr"] == nil

          _ ->
            false
        end
      end)
  rescue
    _ -> false
  end

  defp publication_failure?(%{status: :error, reason: reason}) when is_binary(reason),
    do: String.contains?(reason, "publication_completion_not_verified")

  defp publication_failure?(_), do: false

  defp authorize(records, run, args, probes) do
    task = records.tasks[run.issue_id]

    with true <- is_map(task) and task.run_id == run.id and task.issue_id == run.issue_id,
         {:ok, stop_record} <- template_stop_record(records.events, run, task, args.linear_config),
         {:ok, record} <- authorized_activity(records.events, task, stop_record, args),
         true <- no_conflicting_lifecycle?(records.events, task),
         {:ok, route, issue} <- current_linear_scope(record.event, task, args.linear_config, probes) do
      {:ok, %{task: task, stop_record: stop_record, record: record, route: route, issue: issue}}
    else
      _ -> {:error, :publisher_template_recovery_not_authorized}
    end
  rescue
    _ -> {:error, :publisher_template_recovery_not_authorized}
  end

  defp template_stop_record(_events, %{status: :blocked}, %{status: :acknowledged} = task, _config) do
    valid =
      task.scope_revision == 2 and is_nil(task.stop_event_id) and is_nil(task.stop_timestamp) and
        is_nil(task.pending_reassignment) and is_nil(task.blocked_reassignment) and
        task.event[:kind] == :created and task.session_id == task.event[:session_id]

    if valid, do: {:ok, nil}, else: {:error, :publisher_template_recovery_not_authorized}
  end

  defp template_stop_record(events, %{status: :stopped}, %{status: :stopped} = task, config) do
    stop_id = task.stop_event_id
    source_event = task.event

    with true <- is_binary(stop_id) and is_map(source_event),
         true <- stop_id == source_event[:id],
         %{id: ^stop_id, status: :handled, event: ^source_event} = stop_record <- events[stop_id],
         true <- source_event[:kind] == :created and is_nil(source_event[:activity_id]),
         true <- is_integer(source_event[:timestamp]) and source_event.timestamp == task.stop_timestamp,
         true <- source_event[:issue_id] == task.issue_id and source_event[:session_id] == task.session_id,
         true <- task.scope_revision == 3 and task.assignment_history == [],
         true <- is_nil(task.pending_reassignment) and is_nil(task.blocked_reassignment),
         {:ok, route} <- AgentMapping.select(source_event, config),
         true <-
           route_identity(route) == task.route and route["agent_id"] == task.agent_id and
             route["app_user_id"] == task.app_user_id and route["oauth_client_id"] == task.oauth_client_id and
             source_event[:verified_app_user_id] == route["app_user_id"] do
      {:ok, stop_record}
    else
      _ -> {:error, :publisher_template_recovery_not_authorized}
    end
  rescue
    _ -> {:error, :publisher_template_recovery_not_authorized}
  end

  defp template_stop_record(_events, _run, _task, _config),
    do: {:error, :publisher_template_recovery_not_authorized}

  defp authorized_activity(events, task, _stop_record, args) do
    exact_body = "retry #{args.run_id} #{args.attempt_id}"
    candidates = Enum.filter(Map.values(events), &(get_in(&1, [:event, :activity_id]) == args.activity_id))
    previous = Enum.find_value(events, fn {_id, record} -> if record[:event][:activity_id] == @prior_activity_id, do: record end)
    previous_timestamp = get_in(previous || %{}, [:event, :timestamp])

    case candidates do
      [%{id: id, status: :reply_rejected, event: event} = record] ->
        other_retries =
          events
          |> Map.values()
          |> Enum.filter(&(get_in(&1, [:event, :body]) == exact_body))
          |> Enum.reject(&(&1.id == id))
          |> Enum.map(&get_in(&1, [:event, :timestamp]))

        status_valid =
          if task.status == :stopped,
            do: record.status == :reply_rejected and event[:timestamp] > task.stop_timestamp,
            else: record.status == :reply_rejected

        valid =
          id == event[:id] and event[:kind] == :prompted and event[:signal] in [nil, "continue"] and
            event[:body] == exact_body and event[:issue_id] == task.issue_id and event[:session_id] == task.session_id and
            event[:activity_id] == args.activity_id and is_integer(event[:timestamp]) and status_valid and
            is_integer(previous_timestamp) and event.timestamp > previous_timestamp and
            Enum.all?(other_retries, &(is_integer(&1) and &1 < event.timestamp))

        if valid, do: {:ok, record}, else: {:error, :publisher_template_recovery_not_authorized}

      _ ->
        {:error, :publisher_template_recovery_not_authorized}
    end
  end

  defp no_conflicting_lifecycle?(events, task) do
    Enum.all?(events, fn {_id, record} ->
      event = record[:event] || %{}

      case {event[:kind], event[:issue_id]} do
        {kind, issue_id} when issue_id == task.issue_id and kind in [:stop, :issue_updated] -> false
        {:created, issue_id} when issue_id == task.issue_id -> event[:id] == task.event[:id]
        _ -> true
      end
    end)
  end

  defp current_linear_scope(event, task, config, probes) do
    with {:ok, route} <- AgentMapping.select(event, config),
         true <-
           route_identity(route) == route_identity(task.route) and event.issue_id == task.issue_id and
             event.session_id == task.session_id and event[:verified_app_user_id] == route["app_user_id"],
         {:ok, issue} <- inspect_current_issue(event, route, probes),
         true <- issue.assignee_id == task.assignee_id do
      {:ok, route, issue}
    else
      _ -> {:error, :publisher_template_recovery_not_authorized}
    end
  rescue
    _ -> {:error, :publisher_template_recovery_not_authorized}
  catch
    _, _ -> {:error, :publisher_template_recovery_not_authorized}
  end

  defp inspect_current_issue(event, route, probes) do
    case probes[:linear_scope] do
      fun when is_function(fun, 2) -> fun.(event, route)
      _ -> Delegation.inspect_event(event, route, [])
    end
  end

  defp prior_recovery(store, ancestor, run) do
    with {:ok, %{run_id: ancestor_id, event: origin}} <- WorkstreamStore.event(store, previous_event_id("origin")),
         {:ok, %{run_id: run_id, event: successor}} <- WorkstreamStore.event(store, previous_event_id("successor")),
         true <- ancestor_id == ancestor.id and run_id == run.id,
         true <- valid_previous_events?(ancestor, run, origin, successor) do
      {:ok, %{origin: origin, successor: successor}}
    else
      _ -> {:error, :publisher_template_recovery_lineage_unavailable}
    end
  end

  defp valid_previous_events?(ancestor, run, origin, successor) do
    origin[:kind] == :publisher_port_recovery_origin_stopped and
      successor[:kind] == :publisher_port_recovery_successor_queued and
      origin[:event_id] == previous_event_id("origin") and successor[:event_id] == previous_event_id("successor") and
      origin[:old_run_id] == ancestor.id and origin[:successor_run_id] == run.id and
      successor[:old_run_id] == ancestor.id and successor[:successor_run_id] == run.id and
      origin[:helper_source_sha256] == @prior_helper_sha256 and successor[:helper_source_sha256] == @prior_helper_sha256 and
      origin[:old_policy] == policy_with_executor("ec8ae546aeb408243529f96a1a7048c92d4a61bbe268eee6decbccfb169f4c22") and
      origin[:new_policy] == @parent_policy and successor[:old_policy] == origin[:old_policy] and successor[:new_policy] == origin[:new_policy] and
      origin[:old_publisher_git_sha256] == "8263f004acaf5c3805e5b1196fb4aea6d273eda217e31dda331d8207c8797c94" and
      origin[:new_publisher_git_sha256] == "75ad2bb3821a90d5573578d20200be0416546de3751686db4205dbb65d9188c4" and
      origin[:candidate_sha] == @candidate_sha and origin[:branch] == @branch and
      origin[:remote_branch_absent] == true and origin[:remote_pr_absent] == true and
      ControlledReviewQualification.valid_transition?(ancestor.definition, run.definition, origin[:controlled_review_provenance]) and
      successor[:controlled_review_provenance] == origin[:controlled_review_provenance] and
      successor[:candidate_handoff] == origin[:candidate_handoff] and valid_prior_candidate_handoff?(ancestor)
  rescue
    _ -> false
  end

  defp verify_proofs(ancestor, run, prior, args, probes) do
    with {:ok, termination} <- verify_termination(ancestor, prior.origin, probes),
         {:ok, candidate} <- verify_candidate(run, probes),
         {:ok, intent} <- verify_failed_intent(run, candidate),
         {:ok, validation} <- verify_validation(run, candidate, intent, probes),
         {:ok, archive} <- verify_archive(run, validation, intent, probes),
         {:ok, remote} <- verify_remote(run, intent, probes),
         {:ok, template} <- verify_template_absent(candidate.workspace, @base_sha, probes) do
      {:ok,
       %{
         termination: termination,
         worker_operation_id: prior.origin[:worker_operation_id],
         candidate: candidate,
         intent: intent,
         validation: validation,
         archive: archive,
         remote: remote,
         template: template,
         prior_recovery_event_id: prior.origin[:event_id],
         prior_activity_id: @prior_activity_id,
         linear_activity_id: args.activity_id
       }}
    else
      {:error, _} = error -> error
      _ -> {:error, :publisher_template_recovery_proof_unavailable}
    end
  rescue
    _ -> {:error, :publisher_template_recovery_proof_unavailable}
  catch
    _, _ -> {:error, :publisher_template_recovery_proof_unavailable}
  end

  defp verify_termination(ancestor, prior_origin, probes) do
    operation_id = prior_origin[:worker_operation_id]
    attempt = Enum.find(ancestor.attempts, &(&1.id == operation_id))
    operation = get_in(ancestor, [:operations, operation_id])
    identity = if is_map(operation), do: operation[:external_process], else: nil

    with true <- operation_id == @ancestor_run_id <> "/implement/2",
         %{stage: "implement", type: :agent, status: :completed, result: %{status: :ok}} <- attempt,
         %{id: ^operation_id, type: :agent, status: :completed, attempt_id: ^operation_id} <- operation,
         %{"kind" => "systemd-unit", "operation_id" => ^operation_id} <- identity,
         {:ok, %{"status" => "terminated", "operation_id" => ^operation_id, "identity" => ^identity} = receipt} <- worker_status(ancestor.execution[:worker_control], identity, probes) do
      {:ok, receipt}
    else
      _ -> {:error, :publisher_template_recovery_worker_unknown}
    end
  rescue
    _ -> {:error, :publisher_template_recovery_worker_unknown}
  end

  defp worker_status(control, identity, probes) do
    case probes[:worker_status] do
      fun when is_function(fun, 2) -> fun.(control, identity)
      _ -> WorkerOperation.status(control, identity)
    end
  end

  defp verify_candidate(run, probes) do
    candidate_result =
      case probes[:candidate] do
        fun when is_function(fun, 1) -> fun.(run)
        _ -> production_candidate(run)
      end

    with {:ok, %{workspace: workspace, head_sha: @candidate_sha, status: ""} = candidate} <- candidate_result,
         :ok <- candidate_producer_matches(run) do
      {:ok, Map.put(candidate, :workspace, workspace)}
    else
      _ -> {:error, :publisher_template_recovery_candidate_changed}
    end
  rescue
    _ -> {:error, :publisher_template_recovery_candidate_changed}
  end

  defp production_candidate(run) do
    with {:ok, workspace} <- CandidateTransfer.workspace(run),
         {:ok, head} <- CandidateGit.run(workspace, ["rev-parse", "--verify", "HEAD^{commit}"]),
         {:ok, status} <- CandidateGit.run(workspace, ["status", "--porcelain=v1", "--untracked-files=normal", "--ignore-submodules=all"]) do
      {:ok, %{workspace: workspace, head_sha: String.trim(head), status: status}}
    end
  end

  defp candidate_producer_matches(run) do
    producer_id = run.id <> "/local-commit/1"
    producer = Enum.find(run.attempts, &(&1.id == producer_id))
    output = run.outputs["candidate-commit"]
    artifact = run.artifacts["candidate-commit"]

    with %{status: :completed, stage: "local-commit", type: :check, result: %{status: :ok, evidence: ^output}} <- producer,
         true <- artifact == %{run_id: run.id, attempt_id: producer_id, sha256: digest(:erlang.term_to_binary(output))},
         {:ok, parsed} <- Jason.decode(output["output"]),
         true <-
           parsed["candidate_commit"] == @candidate_sha and parsed["candidate_input_sha256"] == @candidate_input_sha256 and
             parsed["branch"] == "refs/heads/" <> @branch and parsed["base_commit"] == @base_sha do
      :ok
    else
      _ -> {:error, :publisher_template_recovery_candidate_changed}
    end
  rescue
    _ -> {:error, :publisher_template_recovery_candidate_changed}
  end

  defp verify_failed_intent(run, candidate) do
    intent = get_in(run, [:operations, @attempt_id, :publication_intent])

    if is_map(intent) and intent[:candidate_sha] == candidate.head_sha and intent[:branch] == @branch and
         intent[:validation_attempt_id] == run.id <> "/validate/3" and valid_digest?(intent[:evidence_revision]) do
      {:ok, intent}
    else
      {:error, :publisher_template_recovery_publication_intent_invalid}
    end
  end

  defp verify_validation(run, candidate, intent, probes) do
    validation_id = intent.validation_attempt_id
    attempt = Enum.find(run.attempts, &(&1.id == validation_id))
    stage = run.definition.stages["validate"]
    receipt = get_in(attempt || %{}, [:result, :evidence, "receipt"])

    with %{stage: "validate", type: :check, status: :completed, gate: :passed, result: %{status: :ok}} <- attempt,
         true <- is_map(stage) and stage.gate[:evaluator] == :candidate_validation and is_map(receipt),
         {:ok, result} <- validate_candidate_receipt(run, stage, attempt, receipt, candidate, probes),
         true <- result.manifest["candidate_sha"] == intent.candidate_sha do
      {:ok, Map.merge(result, %{attempt: attempt, stage: stage, receipt: receipt})}
    else
      _ -> {:error, :publisher_template_recovery_validation_unavailable}
    end
  rescue
    _ -> {:error, :publisher_template_recovery_validation_unavailable}
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
                 %{root: Path.expand(config["root"]), receipt_uid: 0}
               ),
             true <- archive.revision == intent.evidence_revision and archive.validation_attempt_id == validation.attempt.id do
          {:ok, archive}
        else
          _ -> {:error, :publisher_template_recovery_archive_unavailable}
        end
    end
  rescue
    _ -> {:error, :publisher_template_recovery_archive_unavailable}
  end

  defp verify_remote(run, intent, probes) do
    config = run.execution[:publisher]

    case probes[:remote] do
      fun when is_function(fun, 2) ->
        case fun.(config, intent.branch) do
          {:ok, %{branch_sha: @candidate_sha, pull_request: nil} = result} -> {:ok, result}
          _ -> {:error, :publisher_template_recovery_remote_changed}
        end

      _ ->
        with :ok <- PublisherGithub.validate_config(config),
             {:ok, @candidate_sha} <- PublisherGithub.branch_sha(config, intent.branch),
             {:ok, nil} <- PublisherGithub.find_pr(config, intent.branch) do
          {:ok, %{branch_sha: @candidate_sha, pull_request: nil, app_authenticated: true}}
        else
          _ -> {:error, :publisher_template_recovery_remote_changed}
        end
    end
  rescue
    _ -> {:error, :publisher_template_recovery_remote_changed}
  end

  defp verify_template_absent(workspace, base_sha, probes) do
    case probes[:template] do
      fun when is_function(fun, 2) ->
        case fun.(workspace, base_sha) do
          {:ok, []} -> {:ok, %{base_sha: base_sha, paths: []}}
          _ -> {:error, :publisher_template_recovery_template_not_absent}
        end

      _ ->
        case CandidateGit.run(workspace, ["ls-tree", "-z", base_sha, "--" | @template_paths]) do
          {:ok, ""} -> {:ok, %{base_sha: base_sha, paths: []}}
          _ -> {:error, :publisher_template_recovery_template_not_absent}
        end
    end
  rescue
    _ -> {:error, :publisher_template_recovery_template_not_absent}
  end

  defp build_transition(ancestor, run, prior, authorization, proof, args, origin_id) do
    successor_id = successor_id(run.id, args.activity_id)
    successor_event_id = event_id(args.activity_id, "successor")
    intent = proof.intent

    successor_run =
      WorkstreamRun.new(run.task_id, run.definition, run.workspace, [issue_id: run.issue_id, branch: run.branch], run.execution)
      |> Map.merge(%{
        id: successor_id,
        stage_id: "local-commit",
        thread_id: run.thread_id,
        session_id: run.session_id,
        outputs: Map.put(run.definition.input_values, "candidate", run.outputs["candidate"]),
        repairs: run.repairs,
        questions: run.questions,
        inbox: run.inbox,
        activity_ids: run.activity_ids,
        continuation: run.continuation,
        validation_repair_rounds: run.validation_repair_rounds,
        review_repair_rounds: run.review_repair_rounds,
        review_fix_completions: run.review_fix_completions,
        reviewer_sessions: run.reviewer_sessions,
        transport_retries: run.transport_retries,
        recovery_parent_run_id: run.id,
        created_at: DateTime.from_unix!(authorization.record.event.timestamp, :millisecond) |> DateTime.to_iso8601()
      })

    stopped_run = Map.merge(run, %{status: :stopped, phase: :stopped, recovery_successor_run_id: successor_id})

    successor_task =
      Map.merge(authorization.task, %{
        run_id: successor_id,
        status: :acknowledged,
        authorized_stage: nil,
        authorized_until: 0,
        owned_child_grant: nil,
        check_at: 0,
        scope_revision: authorization.task.scope_revision + 1,
        stop_event_id: nil,
        stop_timestamp: nil
      })

    service_stop_provenance = service_stop_provenance(authorization.stop_record, authorization.task)

    proof_record = %{
      version: @version,
      helper_source_sha256: @source_sha256,
      activity_id: args.activity_id,
      linear_activity_id: args.activity_id,
      delivery_id: authorization.record.id,
      operator: args.operator,
      service_stop_provenance: service_stop_provenance,
      previous_recovery_activity_id: @prior_activity_id,
      previous_recovery_event_id: proof.prior_recovery_event_id,
      previous_recovery_helper_sha256: @prior_helper_sha256,
      controlled_review_provenance: prior.origin[:controlled_review_provenance],
      ancestor_run_id: ancestor.id,
      old_policy: run.policy,
      new_policy: @new_policy,
      old_publisher_sha256: @old_publisher_sha256,
      new_publisher_sha256: @new_publisher_sha256,
      failed_publish_attempt_id: args.attempt_id,
      worker_operation_id: proof.worker_operation_id,
      worker_termination_sha256: digest(Jason.encode!(proof.termination)),
      worker_termination_status: proof.termination["status"],
      candidate_producer_attempt_id: run.id <> "/local-commit/1",
      candidate_input_sha256: @candidate_input_sha256,
      candidate_sha: proof.candidate.head_sha,
      branch: intent.branch,
      validation_attempt_id: proof.validation.attempt.id,
      validation_manifest_sha256: proof.validation.receipt["manifest_sha256"],
      evidence_revision: intent.evidence_revision,
      archive_revision: proof.archive.revision,
      remote_head_sha: proof.remote.branch_sha,
      pull_request_absent: is_nil(proof.remote.pull_request),
      template_base_sha: proof.template.base_sha,
      template_absent_at_base: proof.template.paths == [],
      previous_candidate_sha: prior.origin[:candidate_sha]
    }

    origin_event = %{
      kind: :publisher_template_recovery_origin_stopped,
      event_id: origin_id,
      old_run_id: run.id,
      ancestor_run_id: ancestor.id,
      successor_run_id: successor_id,
      linear_activity_id: args.activity_id,
      proof: proof_record
    }

    successor_event = %{
      kind: :publisher_template_recovery_successor_queued,
      event_id: successor_event_id,
      old_run_id: run.id,
      ancestor_run_id: ancestor.id,
      successor_run_id: successor_id,
      linear_activity_id: args.activity_id,
      stage: successor_run.stage_id,
      proof: proof_record
    }

    {:ok,
     %{
       expected_ancestor: ancestor,
       expected_run: run,
       stopped_run: stopped_run,
       successor_run: successor_run,
       expected_task: authorization.task,
       successor_task: successor_task,
       stop_record: authorization.stop_record,
       reply_record: authorization.record,
       origin_event_id: origin_id,
       origin_event: origin_event,
       successor_event_id: successor_event_id,
       successor_event: successor_event,
       intent: intent,
       proof: proof_record
     }}
  rescue
    _ -> {:error, :publisher_template_recovery_invalid_successor}
  end

  defp service_stop_provenance(%{id: id, event: event}, task) do
    %{
      version: 1,
      event_id: id,
      event_sha256: digest(:erlang.term_to_binary(event, [:deterministic])),
      timestamp: event.timestamp,
      scope_revision: task.scope_revision
    }
  end

  defp service_stop_provenance(nil, _task), do: nil

  defp apply_transition(store, transition) do
    case WorkstreamStore.commit_publisher_template_recovery_successor(store, transition) do
      {:ok, _runs} -> {:ok, summary(transition, :queued)}
      {:duplicate, _runs} -> {:ok, summary(transition, :already_applied)}
      {:error, _reason} -> {:error, :publisher_template_recovery_store_conflict}
    end
  end

  defp already_applied(store, args, run_id, origin) do
    successor_id = origin[:successor_run_id]
    successor_event_id = event_id(args.activity_id, "successor")

    with true <-
           run_id == @run_id and origin[:kind] == :publisher_template_recovery_origin_stopped and
             origin[:event_id] == event_id(args.activity_id, "origin") and origin[:old_run_id] == @run_id and
             origin[:linear_activity_id] == args.activity_id and origin[:proof][:operator] == args.operator and
             origin[:proof][:helper_source_sha256] == @source_sha256 and
             origin[:proof][:old_policy] == @parent_policy and origin[:proof][:new_policy] == @new_policy and
             origin[:proof][:old_publisher_sha256] == @old_publisher_sha256 and
             origin[:proof][:new_publisher_sha256] == @new_publisher_sha256 and is_binary(successor_id),
         {:ok, %{run_id: ^successor_id, event: successor_event}} <- WorkstreamStore.event(store, successor_event_id),
         true <-
           successor_event[:kind] == :publisher_template_recovery_successor_queued and
             successor_event[:old_run_id] == @run_id and successor_event[:successor_run_id] == successor_id and
             successor_event[:proof] == origin[:proof],
         {:ok, stopped_run} <- WorkstreamStore.fetch(store, @run_id),
         {:ok, successor_run} <- WorkstreamStore.fetch(store, successor_id),
         {:ok, records} <- WorkstreamStore.linear_load(store),
         task when is_map(task) <- records.tasks[@issue_id],
         true <-
           stopped_run[:recovery_successor_run_id] == successor_id and successor_run[:recovery_parent_run_id] == @run_id and
             successor_run.policy == @new_policy and successor_run.definition == stopped_run.definition and
             task[:run_id] == successor_id do
      {:ok, %{status: :already_applied, old_run_id: @run_id, successor_run_id: successor_id, stage: successor_run.stage_id}}
    else
      _ -> {:error, :publisher_template_recovery_activity_conflict}
    end
  rescue
    _ -> {:error, :publisher_template_recovery_store_unavailable}
  end

  defp summary(transition, status) do
    successor = transition.successor_run

    %{
      status: status,
      old_run_id: transition.expected_run.id,
      ancestor_run_id: transition.expected_ancestor.id,
      successor_run_id: successor.id,
      task_id: successor.task_id,
      stage: successor.stage_id,
      workspace: successor.workspace,
      branch: transition.intent.branch,
      candidate_sha: transition.proof.candidate_sha,
      activity_id: transition.proof.activity_id,
      worker_operation_id: transition.proof.worker_operation_id,
      validation_attempt_id: transition.proof.validation_attempt_id,
      evidence_revision: transition.proof.evidence_revision,
      old_policy: transition.expected_run.policy,
      new_policy: successor.policy
    }
  end

  defp previous_event_id(suffix) do
    digest(@prior_activity_id)
    |> then(&"publisher-port-recovery/#{&1}/#{suffix}")
  end

  defp event_id(activity_id, suffix), do: @event_prefix <> digest(activity_id) <> "/" <> suffix

  defp successor_id(run_id, activity_id),
    do: "run-" <> (digest(run_id <> "\n" <> activity_id) |> binary_part(0, 32))

  defp publisher_source_sha256 do
    Path.join(__DIR__, "publisher.ex")
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
