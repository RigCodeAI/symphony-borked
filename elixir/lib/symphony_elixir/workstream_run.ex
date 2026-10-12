defmodule SymphonyElixir.WorkstreamRun do
  @moduledoc """
  Durable local workstream transitions. The orchestrator alone commits transitions;
  workers execute one pinned stage and cannot choose the next stage or gate outcome.
  """

  @policy_digest :crypto.hash(:sha256, File.read!(__ENV__.file)) |> Base.encode16(case: :lower)

  @external_resource Path.join(__DIR__, "workstream_child.ex")
  @child_digest :crypto.hash(:sha256, File.read!(Path.join(__DIR__, "workstream_child.ex"))) |> Base.encode16(case: :lower)

  @executor_sources ~w(
                       orchestrator.ex
                       workstream_runner.ex
                       worker_operation.ex
                       workstream_cancellation.ex
                       agent_runner.ex
                       ssh.ex
                       codex/app_server.ex
                       linear/agent_mapping.ex
                       linear/coordinator.ex
                       linear/delegation.ex
                       publisher.ex
                       publisher_git.ex
                       publisher_github.ex
                       validation_check.ex
                       validation_github.ex
                       candidate_transfer.ex
                       workstream_cleanup.ex
                       service_check_completion.ex
                       workstream_outputs.ex
                       evidence.ex
                     )

  for source <- @executor_sources do
    @external_resource Path.join(__DIR__, source)
  end

  @executor_digest @executor_sources
                   |> Enum.map(&File.read!(Path.join(__DIR__, &1)))
                   |> IO.iodata_to_binary()
                   |> then(&:crypto.hash(:sha256, &1))
                   |> Base.encode16(case: :lower)

  @external_resource Path.join(__DIR__, "validation.ex")
  @external_resource Path.join(__DIR__, "validation_policy.ex")
  @external_resource Path.join(__DIR__, "validation_command.ex")
  @external_resource Path.join(__DIR__, "candidate_git.ex")
  @external_resource Path.join(__DIR__, "workstream.ex")
  @validation_digest ["validation.ex", "validation_policy.ex", "validation_command.ex", "candidate_git.ex", "workstream.ex"]
                     |> Enum.map(&File.read!(Path.join(__DIR__, &1)))
                     |> IO.iodata_to_binary()
                     |> then(fn source -> :crypto.hash(:sha256, source) end)
                     |> Base.encode16(case: :lower)

  @spec policy() :: map()
  def policy,
    do: %{
      lifecycle_sha256: @policy_digest,
      executor_sha256: @executor_digest,
      child_sha256: @child_digest,
      validation_sha256: @validation_digest,
      gate_policy: "exit-status-v1",
      version: 1
    }

  @spec compatible_policy?(map()) :: boolean()
  def compatible_policy?(run), do: run.policy == policy()

  @spec new(String.t(), map(), Path.t(), keyword()) :: map()
  def new(task_id, definition, workspace, opts) do
    execution = SymphonyElixir.WorkstreamRunner.execution_context(opts)
    execution = Map.merge(execution, Map.get(definition, :pinned_validation_context, %{}))
    new(task_id, definition, workspace, opts, execution)
  end

  @spec new(String.t(), map(), Path.t(), keyword(), map()) :: map()
  def new(task_id, definition, workspace, opts, execution_context) do
    definition = Map.delete(definition, :pinned_validation_context)

    %{
      id: "run-" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower),
      task_id: task_id,
      issue_id: Keyword.get(opts, :issue_id),
      branch: Keyword.get_lazy(opts, :branch, fn -> workspace_branch(workspace) end),
      session_id: nil,
      thread_id: nil,
      workspace: workspace,
      definition: definition,
      policy: policy(),
      execution: execution_context,
      status: :ready,
      phase: :queued,
      stage_id: definition.entry,
      current_attempt_id: nil,
      outputs: definition.input_values,
      artifacts: %{},
      attempts: [],
      operations: %{},
      repairs: %{},
      questions: %{},
      inbox: [],
      activity_ids: %{},
      continuation: nil,
      validation_repair_rounds: 0,
      review_repair_rounds: 0,
      transport_retries: 0,
      retry_operation_id: nil,
      pending_wait: nil,
      created_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  @doc "Plans a waiting parent and linked child; the coordinator must commit both atomically before dispatch."
  @spec begin_child(map(), map()) :: {:ok, map(), map(), map()} | {:error, atom()}
  def begin_child(run, prepared_child) do
    with {:ok, intent, child} <- SymphonyElixir.WorkstreamChild.prepare(run, prepared_child) do
      attempt = %{
        id: intent.parent_attempt_id,
        stage: intent.parent_stage_id,
        type: :child,
        ordinal: length(run.attempts) + 1,
        status: :waiting,
        result: nil,
        gate: nil,
        gate_detail: nil
      }

      operation = %{
        id: attempt.id,
        attempt_id: attempt.id,
        stage: attempt.stage,
        type: :child,
        status: :waiting_for_child,
        worker: nil,
        side_effect_id: intent.handoff_id,
        transport_retries: 0,
        child_handoff: intent
      }

      waiting =
        run
        |> Map.merge(%{
          status: :waiting_for_child,
          phase: :waiting_for_child,
          current_attempt_id: attempt.id,
          attempts: run.attempts ++ [attempt],
          operations: Map.put(run.operations, attempt.id, operation),
          child_handoff: intent
        })

      {:ok, waiting, child, intent}
    end
  end

  @doc false
  # The coordinator calls this only after its tracked service proof task and exact
  # snapshot checks. Structural validation here does not establish proof provenance.
  @spec finish_child(map(), map(), map()) :: {:ok, map()} | {:error, atom()}
  def finish_child(parent, child, proof) when is_map(parent) and is_map(child) and is_map(proof) do
    with {:ok, stage, attempt, operation, intent} <- current_child_wait(parent),
         :ok <- current_child_identity(parent, child, intent),
         :ok <- current_child_policies(parent, child),
         :ok <- valid_child_service_proof(child, proof),
         true <- valid_child_outputs?(stage.outputs),
         envelope = child_result_envelope(child, proof),
         completed_attempt = child_attempt_result(attempt, envelope, proof),
         completed_operation = Map.merge(operation, %{status: :completed, completion_proof: proof}),
         completed =
           parent
           |> Map.put(:attempts, replace_attempt(parent.attempts, attempt.id, completed_attempt))
           |> put_in([:operations, attempt.id], completed_operation)
           |> put_outputs(stage, envelope)
           |> Map.merge(%{
             status: :ready,
             phase: :queued,
             stage_id: stage.next,
             current_attempt_id: nil,
             pending_wait: nil
           }) do
      {:ok, completed}
    else
      {:error, _} = error -> error
      _ -> {:error, :child_completion_not_current}
    end
  rescue
    _ -> {:error, :child_completion_not_current}
  end

  def finish_child(_parent, _child, _proof), do: {:error, :child_completion_not_current}

  defp current_child_wait(
         %{
           status: :waiting_for_child,
           phase: :waiting_for_child,
           current_attempt_id: attempt_id,
           child_handoff: %{version: 1} = intent,
           definition: %{stages: stages}
         } = parent
       )
       when is_binary(attempt_id) do
    stage = Map.get(stages, parent.stage_id)
    operation = get_in(parent, [:operations, attempt_id])
    attempt = Enum.find(parent.attempts, &(&1.id == attempt_id))

    with true <- compatible_policy?(parent),
         true <- intent.parent_run_id == parent.id and intent.parent_stage_id == parent.stage_id and intent.parent_attempt_id == attempt_id,
         %{type: :child, next: next, outputs: outputs} = stage when is_binary(next) and is_list(outputs) <- stage,
         %{type: :check} <- Map.get(stages, stage.next),
         true <- outputs != [] and Enum.all?(outputs, &(is_binary(&1) and &1 != "")) and length(outputs) == length(Enum.uniq(outputs)),
         %{id: ^attempt_id, stage: stage_id, type: :child, ordinal: ordinal, status: :waiting, result: nil, gate: nil, gate_detail: nil} = attempt <- attempt,
         true <- ordinal == length(parent.attempts) and stage_id == stage.id and stage.id == parent.stage_id,
         %{id: ^attempt_id, attempt_id: ^attempt_id, stage: ^stage_id, type: :child, status: :waiting_for_child, worker: nil, side_effect_id: handoff_id, transport_retries: 0, child_handoff: ^intent} =
           operation <- operation,
         true <- handoff_id == intent.handoff_id do
      {:ok, stage, attempt, operation, intent}
    else
      _ -> {:error, :child_completion_not_current}
    end
  end

  defp current_child_wait(_parent), do: {:error, :child_completion_not_current}

  defp current_child_identity(parent, child, intent) do
    if child[:id] == intent.child_run_id and child[:parent_run_id] == parent.id and is_map(child[:parent_handoff]) and
         child.status in [:complete, :waiting_for_answer] and
         SymphonyElixir.WorkstreamChild.correlate(intent, Map.put(child.parent_handoff, :child_run_id, child.id)) == :ok do
      :ok
    else
      {:error, :child_result_identity_mismatch}
    end
  end

  defp current_child_policies(parent, child) do
    if compatible_policy?(parent) and compatible_policy?(child),
      do: :ok,
      else: {:error, :child_policy_mismatch}
  end

  defp valid_child_service_proof(child, proof) do
    publication_attempt_id = proof[:publication_attempt_id]
    publication_id = proof[:publication_id]
    attempt = Enum.find(child.attempts, &(&1.id == publication_attempt_id))

    with true <- proof[:version] == 1,
         true <- proof[:repository] == "RigCodeAI/rig",
         true <- proof[:run_id] == child.id and proof[:task_id] == child.task_id,
         true <- valid_child_digest?(publication_id),
         true <- is_binary(publication_attempt_id) and publication_attempt_id != "",
         %{id: ^publication_attempt_id, stage: stage_id, type: :publish, status: :completed, gate: :passed, result: %{status: :ok, evidence: evidence}} <- attempt,
         %{type: :publish} <- get_in(child, [:definition, :stages, stage_id]),
         %{type: :publish, status: :completed, publication_intent: publication_intent} <-
           get_in(child, [:operations, publication_attempt_id]),
         true <- publication_intent[:publication_id] == publication_id,
         true <- evidence["publication_id"] == publication_id,
         true <- valid_child_sha?(proof[:published_head]) and evidence["candidate_sha"] == proof.published_head,
         true <- evidence["branch"] == SymphonyElixir.Publisher.branch(child),
         %{"number" => number, "url" => url} <- evidence["pr"],
         true <- is_integer(number) and number > 0 and is_binary(url) and url != "",
         true <- proof[:pr] == %{number: number, url: url},
         %{commit: commit} <- proof[:merge],
         true <- valid_child_sha?(commit),
         archive when is_map(archive) and map_size(archive) > 0 <- proof[:archive],
         true <- proof[:retained_outputs_complete] == true,
         :ok <- valid_retained_child_outputs(child, proof[:retained_outputs]),
         :ok <- valid_retained_child_values(child, proof[:outputs]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_child_service_proof}
    end
  end

  defp valid_retained_child_outputs(child, retained) do
    artifacts = child[:artifacts]
    saved = child[:retained_output_artifacts]

    with true <- is_map(artifacts) and map_size(artifacts) > 0,
         true <- is_map(saved) and map_size(saved) > 0 and retained == saved,
         true <- Enum.sort(Map.keys(saved)) == Enum.sort(Map.keys(artifacts)),
         true <-
           Enum.all?(artifacts, fn {name, artifact_id} ->
             is_map(saved[name]) and saved[name][:version] == 2 and saved[name][:output_id] == artifact_id and saved[name][:run_id] == child.id and
               is_map(child.outputs) and Map.has_key?(child.outputs, name)
           end) do
      :ok
    else
      _ -> {:error, :invalid_child_service_proof}
    end
  end

  defp valid_retained_child_values(child, values) do
    if is_map(values) and Enum.sort(Map.keys(values)) == Enum.sort(Map.keys(child.artifacts)) and
         Enum.all?(values, fn {name, value} ->
           is_map(value) and value["version"] == 2 and value["kind"] == child.retained_output_artifacts[name].kind and
             value["output_id"] == json_value(child.artifacts[name]) and Map.has_key?(value, "value")
         end),
       do: :ok,
       else: {:error, :invalid_child_service_proof}
  end

  defp valid_child_outputs?(outputs), do: is_list(outputs) and outputs != [] and Enum.all?(outputs, &(is_binary(&1) and &1 != ""))

  defp child_result_envelope(child, proof) do
    %{
      child_run_id: child.id,
      merge: proof.merge,
      archive: proof.archive,
      retained_outputs: proof.retained_outputs,
      outputs: proof.outputs
    }
  end

  defp child_attempt_result(attempt, envelope, proof) do
    Map.merge(attempt, %{
      status: :completed,
      result: %{status: :ok, evidence: json_value(envelope)},
      gate: :passed,
      gate_detail: %{
        child_run_id: proof.run_id,
        publication_id: proof.publication_id,
        merge_commit: proof.merge.commit
      }
    })
  end

  defp replace_attempt(attempts, id, replacement),
    do: Enum.map(attempts, fn attempt -> if attempt.id == id, do: replacement, else: attempt end)

  defp valid_child_sha?(value), do: is_binary(value) and Regex.match?(~r/\A[a-f0-9]{40}\z/, value)

  defp valid_child_digest?(value), do: is_binary(value) and Regex.match?(~r/\A[a-f0-9]{64}\z/, value)

  @spec start_stage(map()) :: map()
  def start_stage(%{status: :ready} = run) do
    stage = Map.fetch!(run.definition.stages, run.stage_id)
    ordinal = length(run.attempts) + 1
    id = "#{run.id}/#{stage.id}/#{ordinal}"

    attempt = %{
      id: id,
      stage: stage.id,
      type: stage.type,
      ordinal: ordinal,
      status: :executing,
      result: nil,
      gate: nil,
      gate_detail: nil
    }

    if stage.type == :human_wait do
      artifact_ids = if Map.get(stage, :approval, false), do: run.artifacts, else: Map.take(run.artifacts, stage.inputs)

      wait = %{
        id: id <> "/wait",
        prompt: stage.prompt,
        inputs: Map.take(run.outputs, stage.inputs),
        artifact_ids: artifact_ids
      }

      wait =
        if Map.get(stage, :approval, false) do
          Map.merge(wait, %{kind: :approval, approval_digest: artifact_revision(artifact_ids)})
        else
          wait
        end

      %{run | status: :waiting_for_answer, phase: :waiting_for_answer, current_attempt_id: id, attempts: run.attempts ++ [%{attempt | status: :waiting}], pending_wait: wait}
    else
      {inbox, inbox_messages} = if stage.type == :agent, do: assign_inbox(run.inbox, id), else: {run.inbox, []}
      continuation = if stage.type == :agent, do: assign_continuation(run, id, inbox_messages), else: nil
      questions = if continuation, do: activate_continuation_questions(run.questions, stage.id, run.artifacts, id), else: run.questions

      attempt =
        attempt
        |> Map.put(:inbox_activity_ids, Enum.map(inbox_messages, & &1.activity_id))
        |> Map.put(:inbox_messages, inbox_messages)
        |> Map.put(:continuation, continuation)

      previous = run.operations[run.retry_operation_id]

      operation = %{
        id: id,
        attempt_id: id,
        stage: stage.id,
        type: stage.type,
        status: :executing,
        worker: nil,
        side_effect_id: if(previous, do: previous.side_effect_id, else: id),
        transport_retries: if(previous, do: previous.transport_retries, else: 0)
      }

      %{
        run
        | status: :executing,
          phase: stage_phase(stage.type),
          current_attempt_id: id,
          attempts: run.attempts ++ [attempt],
          operations: Map.put(run.operations, id, operation),
          retry_operation_id: nil,
          inbox: inbox,
          continuation: continuation,
          questions: questions
      }
    end
  end

  @spec record_worker(map(), map()) :: map()
  def record_worker(run, identity) do
    update_in(run.operations[run.current_attempt_id].worker, fn _ -> identity end)
  end

  @spec finish_stage(map(), term()) :: map()
  def finish_stage(%{status: status} = run, {:waiting, receipt}) when status in [:executing, :reconciling] and is_map(receipt) do
    finish_question_wait(run, receipt)
  end

  def finish_stage(%{status: status} = run, result) when status in [:executing, :reconciling] do
    stage = Map.fetch!(run.definition.stages, run.stage_id)
    {result, gate, gate_detail} = finish_gate(run, stage, result)
    result = json_stage_result(result)
    evidence = result_evidence(result)
    gate = if stage.type == :check and is_nil(gate), do: if(passing_check?(result), do: :passed, else: :failed), else: gate
    run = if stage.type == :check and stage.gate[:evaluator] == :github_validation and gate == :passed, do: Map.put(run, :validation_observation, elem(result, 1)), else: run
    run = update_attempt(run, %{status: :completed, result: evidence, gate: gate, gate_detail: gate_detail})
    run = put_in(run.operations[run.current_attempt_id].status, :completed)
    run = SymphonyElixir.ServiceCheckCompletion.record(run)
    run = if stage.type == :agent, do: close_pending_questions(run, stage.id, run.current_attempt_id), else: run

    case {stage.type, result} do
      {:agent, {:ok, value}} when is_map(value) ->
        run = put_session(run, value)

        if pending_inbox?(run) do
          queue_inbox_continuation(run, value, stage.id)
        else
          run |> put_outputs(stage, value) |> advance(stage.next)
        end

      {:check, _} ->
        if gate == :passed do
          {:ok, value} = result
          run |> put_outputs(stage, value) |> advance(stage.gate.success)
        else
          if candidate_validation?(stage) do
            candidate_validation_failure(run, stage)
          else
            repair_or_block(run, stage)
          end
        end

      {:publish, {:ok, value}} when is_map(value) and gate == :passed ->
        run |> put_outputs(stage, value) |> advance(stage.gate.success)

      {:publish, _result} ->
        advance(run, :blocked)

      _ ->
        advance(run, :blocked)
    end
  end

  @spec wait_answer(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def wait_answer(%{status: :waiting_for_answer, pending_wait: %{id: id}} = run, id, reply) when is_map(reply) do
    apply_wait_answer(run, id, reply, nil, nil)
  end

  def wait_answer(_run, _id, _reply), do: {:error, :stale_or_missing_human_wait}

  @doc "Persists one clarification request for the current agent attempt."
  @spec ask_question(map(), map()) :: {:ok, map(), %{id: String.t()}} | {:error, term()}
  def ask_question(%{status: :executing, current_attempt_id: attempt_id} = run, request)
      when is_binary(attempt_id) and is_map(request) do
    stage = Map.fetch!(run.definition.stages, run.stage_id)
    prompt = Map.get(request, :prompt, Map.get(request, "prompt"))
    request_id = Map.get(request, :request_id, Map.get(request, "request_id"))
    prompt = if is_binary(prompt), do: String.trim(prompt), else: prompt

    with true <- stage.type == :agent,
         true <- is_binary(prompt) and String.trim(prompt) != "",
         :ok <- valid_question_request_id(request_id) do
      request_key = question_request_key(prompt, request_id)

      case Enum.find(run.questions, fn {_id, question} ->
             question.attempt_id == attempt_id and question.request_key == request_key
           end) do
        {id, %{prompt: ^prompt, artifact_ids: artifact_ids}}
        when artifact_ids == run.artifacts ->
          {:ok, run, %{id: id}}

        {_id, _question} ->
          {:error, :conflicting_question_request}

        nil ->
          id = question_id(run, attempt_id, request_key)

          question = %{
            id: id,
            run_id: run.id,
            stage_id: run.stage_id,
            attempt_id: attempt_id,
            active_attempt_id: attempt_id,
            prompt: prompt,
            ordinal: map_size(run.questions) + 1,
            request_key: request_key,
            artifact_ids: run.artifacts,
            status: :pending,
            reply: nil
          }

          {:ok, put_in(run.questions[id], question), %{id: id}}
      end
    else
      false -> {:error, :question_requires_current_agent_attempt}
      {:error, _} = error -> error
    end
  end

  def ask_question(_run, _request), do: {:error, :question_requires_current_agent_attempt}

  @doc "Records one reply against the current durable clarification."
  @spec question_answer(map(), String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def question_answer(run, wait_id, body, activity_id) do
    store_question_answer(run, wait_id, body, activity_id, body)
  end

  @doc "Routes a Linear reply to a wait, clarification, or the agent inbox."
  @spec linear_reply(map(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def linear_reply(run, body, activity_id) when is_map(run) and is_binary(body) and is_binary(activity_id) do
    with :ok <- valid_activity_id(activity_id),
         :new <- activity_status(run, activity_id, body) do
      route_linear_reply(run, body, activity_id)
    else
      :duplicate -> {:ok, run}
      {:error, _} = error -> error
    end
  end

  def linear_reply(_run, _body, _activity_id), do: {:error, :invalid_linear_reply}

  @spec uncertain(map(), term()) :: map()
  def uncertain(run, reason) do
    %{run | status: :reconciling, phase: :reconciling}
    |> put_in([:operations, run.current_attempt_id, :reconciliation], inspect(reason))
  end

  @doc "Records a trusted termination for the stopped run's current operation."
  @spec confirm_termination(map()) :: map()
  def confirm_termination(%{status: :stopped, current_attempt_id: attempt_id} = run) when is_binary(attempt_id) do
    case Map.get(run.operations, attempt_id) do
      %{status: status} = operation when status in [:executing, :canceled] ->
        attempts = Enum.map(run.attempts, &confirm_canceled_attempt(&1, attempt_id))

        operation_cancellation =
          operation
          |> Map.get(:cancellation)
          |> cancellation_map()
          |> Map.put(:termination, :terminated)

        operation =
          operation
          |> Map.put(:status, :canceled)
          |> Map.delete(:reconciliation)
          |> Map.put(:cancellation, operation_cancellation)

        cancellation =
          run
          |> Map.get(:cancellation)
          |> cancellation_map()
          |> Map.put(:termination, :terminated)

        run
        |> Map.put(:attempts, attempts)
        |> Map.put(:operations, Map.put(run.operations, attempt_id, operation))
        |> Map.put(:cancellation, cancellation)

      _ ->
        run
    end
  end

  def confirm_termination(run), do: run

  defp confirm_canceled_attempt(%{id: id, status: status} = attempt, attempt_id)
       when id == attempt_id and status in [:executing, :canceled],
       do: Map.put(attempt, :status, :canceled)

  defp confirm_canceled_attempt(attempt, _attempt_id), do: attempt

  defp cancellation_map(value) when is_map(value), do: value
  defp cancellation_map(_value), do: %{}

  @spec retry_transport(map()) :: map()
  def retry_transport(run) do
    run = preserve_retry_context(run)
    run = update_attempt(run, %{status: :interrupted})
    run = update_in(run.operations[run.current_attempt_id], fn operation -> %{operation | status: :not_applied, transport_retries: operation.transport_retries + 1} end)
    %{run | status: :ready, phase: :queued, transport_retries: run.transport_retries + 1, retry_operation_id: run.current_attempt_id}
  end

  @spec report(map()) :: map()
  def report(run) do
    run
    |> Map.drop([:definition, :execution])
    |> Map.put(:outputs, Map.drop(run.outputs, run.definition.inputs))
    |> Map.put(:definitions, Map.new(run.definition.definitions, fn {path, resource} -> {path, resource.sha256} end))
    |> Map.put(:agents, Map.new(run.definition.agents, fn {name, agent} -> {name, Map.take(agent, [:model, :reasoning_effort, :daybreak])} end))
  end

  defp finish_question_wait(run, receipt) do
    stage = Map.fetch!(run.definition.stages, run.stage_id)
    wait_id = Map.get(receipt, :wait_id, Map.get(receipt, "wait_id"))
    thread_id = Map.get(receipt, :thread_id, Map.get(receipt, "thread_id"))
    session_id = Map.get(receipt, :session_id, Map.get(receipt, "session_id"))

    question = Map.get(run.questions, wait_id)

    valid_question? =
      stage.type == :agent and is_map(question) and question.stage_id == run.stage_id and
        question.active_attempt_id == run.current_attempt_id and question.artifact_ids == run.artifacts

    if valid_question? do
      finish_valid_question_wait(run, question, thread_id, session_id)
    else
      fail_wait_receipt(run, :unmatched_question_wait)
    end
  end

  defp finish_valid_question_wait(run, question, thread_id, session_id) do
    run = put_session(run, %{thread_id: thread_id, session_id: session_id})
    wait_evidence = %{question_id: question.id, prompt: question.prompt, thread_id: thread_id, session_id: session_id, artifact_ids: question.artifact_ids}
    run = put_in(run.operations[run.current_attempt_id].status, :completed)

    case question.reply do
      %{body: _body} = reply ->
        evidence = Map.merge(wait_evidence, %{reply: reply.body, activity_id: reply.activity_id})
        run = update_attempt(run, %{status: :completed, result: %{status: :ok, evidence: evidence}})
        continuation = question_continuation(run, question, reply, thread_id, session_id)

        %{
          run
          | status: :ready,
            phase: :queued,
            current_attempt_id: nil,
            pending_wait: nil,
            continuation: continuation
        }

      nil ->
        pending_wait = %{
          id: question.id,
          question_id: question.id,
          kind: :clarification,
          stage_id: question.stage_id,
          attempt_id: question.active_attempt_id,
          question_origin_attempt_id: question.attempt_id,
          prompt: question.prompt,
          artifact_ids: question.artifact_ids
        }

        run
        |> update_attempt(%{status: :waiting, result: %{status: :waiting, evidence: wait_evidence}})
        |> Map.merge(%{status: :waiting_for_answer, phase: :waiting_for_answer, pending_wait: pending_wait})
    end
  end

  defp fail_wait_receipt(run, reason) do
    run = update_attempt(run, %{status: :completed, result: %{status: :error, reason: inspect(reason)}})
    run = put_in(run.operations[run.current_attempt_id].status, :completed)
    advance(run, :blocked)
  end

  defp question_continuation(run, question, reply, thread_id, session_id) do
    previous =
      case run.continuation do
        %{stage_id: stage_id} = continuation when stage_id == question.stage_id -> continuation
        _ -> %{}
      end

    answers =
      run.questions
      |> Map.values()
      |> Enum.filter(&(&1.status == :answered and &1.stage_id == question.stage_id and &1.artifact_ids == question.artifact_ids))
      |> Enum.sort_by(& &1.ordinal)
      |> Enum.map(fn item -> %{id: item.id, prompt: item.prompt, reply: Map.take(item.reply, [:body, :activity_id])} end)

    questions = (Map.get(previous, :questions, []) ++ answers) |> Enum.uniq_by(& &1.id)

    %{
      stage_id: question.stage_id,
      thread_id: thread_id || run.thread_id,
      session_id: session_id || run.session_id,
      question_id: question.id,
      question_prompt: question.prompt,
      reply: Map.take(reply, [:body, :activity_id]),
      questions: questions,
      pending_questions: pending_question_context(run, question.stage_id, question.artifact_ids),
      messages: Map.get(previous, :messages, []),
      delivery_attempt_id: nil
    }
  end

  defp store_question_answer(run, wait_id, body, activity_id, activity_body)
       when is_binary(wait_id) and is_binary(body) and is_binary(activity_id) do
    with :ok <- valid_activity_id(activity_id),
         :new <- activity_status(run, activity_id, activity_body),
         %{status: status} = question when status == :pending <- Map.get(run.questions, wait_id),
         true <- question_current?(run, question),
         true <- question.artifact_ids == run.artifacts,
         true <- run.status in [:ready, :executing, :reconciling, :waiting_for_answer] do
      reply = %{body: body, activity_id: activity_id}
      question = %{question | status: :answered, reply: reply}

      run =
        run
        |> put_in([:questions, wait_id], question)
        |> record_activity(activity_id, activity_body)

      cond do
        run.status == :waiting_for_answer and get_in(run.pending_wait || %{}, [:question_id]) == wait_id ->
          continuation = question_continuation(run, question, reply, run.thread_id, run.session_id)

          {:ok,
           %{
             run
             | status: :ready,
               phase: :queued,
               current_attempt_id: nil,
               pending_wait: nil,
               continuation: continuation
           }}

        run.status == :ready ->
          continuation = question_continuation(run, question, reply, run.continuation.thread_id, run.continuation.session_id)
          {:ok, %{run | continuation: continuation}}

        true ->
          {:ok, run}
      end
    else
      :duplicate -> {:ok, run}
      {:error, _} = error -> error
      nil -> {:error, :stale_or_missing_question}
      false -> {:error, :stale_question_or_artifacts}
      _ -> {:error, :stale_or_missing_question}
    end
  end

  defp store_question_answer(_run, _wait_id, _body, _activity_id, _activity_body),
    do: {:error, :invalid_question_reply}

  defp route_linear_reply(run, body, activity_id) do
    case parse_targeted_reply(body) do
      {:answer, wait_id, reply_body} -> route_targeted_answer(run, wait_id, reply_body, activity_id, body)
      {:approve, wait_id, digest} -> approve_wait(run, wait_id, digest, activity_id, body)
      :untargeted -> route_untargeted_reply(run, body, activity_id)
      {:error, _} = error -> error
    end
  end

  defp route_targeted_answer(run, wait_id, reply_body, activity_id, activity_body) do
    cond do
      Map.has_key?(run.questions, wait_id) ->
        store_question_answer(run, wait_id, reply_body, activity_id, activity_body)

      get_in(run.pending_wait || %{}, [:id]) == wait_id and get_in(run.pending_wait || %{}, [:kind]) == :approval ->
        {:error, :explicit_approval_required}

      get_in(run.pending_wait || %{}, [:id]) == wait_id ->
        with {:ok, reply} <- decode_wait_reply(reply_body) do
          apply_wait_answer(run, wait_id, reply, activity_id, activity_body)
        end

      true ->
        {:error, :stale_or_missing_wait}
    end
  end

  defp route_untargeted_reply(run, body, activity_id) do
    cond do
      get_in(run.pending_wait || %{}, [:kind]) == :approval ->
        {:error, :explicit_approval_required}

      normal_human_wait?(run) ->
        with {:ok, reply} <- decode_wait_reply(body) do
          apply_wait_answer(run, run.pending_wait.id, reply, activity_id, body)
        end

      true ->
        case active_questions(run) do
          [question] -> store_question_answer(run, question.id, body, activity_id, body)
          [] -> append_inbox_reply(run, body, activity_id)
          _ -> {:error, :ambiguous_question_reply}
        end
    end
  end

  defp parse_targeted_reply(body) do
    body = String.trim(body)

    case Regex.run(~r/\Aanswer\s+([^\s:]+)\s*:\s*(.+)\z/is, body) do
      [_, wait_id, reply] ->
        {:answer, wait_id, String.trim(reply)}

      _ ->
        case Regex.run(~r/\Aapprove\s+([^\s]+)\s+([0-9a-f]{64})\s*\z/i, body) do
          [_, wait_id, digest] ->
            {:approve, wait_id, String.downcase(digest)}

          _ ->
            cond do
              Regex.match?(~r/\Aanswer(?:\s|\z)/i, body) -> {:error, :invalid_targeted_reply}
              Regex.match?(~r/\Aapprove(?:\s|\z)/i, body) -> {:error, :invalid_approval_reply}
              true -> :untargeted
            end
        end
    end
  end

  @doc "Consumes a queued human reply only after an owned, sealed current validation observation."
  @spec complete_validation_reply(map(), map()) :: {:ok, map()} | {:error, term()}
  def complete_validation_reply(run, observation) do
    pending = run[:pending_validation_reply]

    with true <- is_map(pending) and run.status == :waiting_for_answer,
         true <- run.pending_wait.id == pending.wait_id and run.current_attempt_id == pending.attempt_id and run.artifacts == pending.artifact_ids,
         :ok <- SymphonyElixir.ValidationCheck.verify_attested_completion(run, SymphonyElixir.ValidationCheck.stage(run.definition), {:ok, observation}) do
      verified = Map.put(run, :validated_wait_reply, true)

      result =
        case pending.kind do
          :answer -> apply_wait_answer(verified, pending.wait_id, pending.reply, pending.activity_id, pending.activity_body)
          :approval -> approve_wait(verified, pending.wait_id, pending.digest, pending.activity_id, pending.activity_body)
        end

      case result do
        {:ok, updated} -> {:ok, Map.drop(updated, [:validated_wait_reply, :pending_validation_reply])}
        error -> error
      end
    else
      _ -> {:error, :current_validation_required}
    end
  end

  defp queue_validation_reply(run, reply) do
    pending = Map.merge(reply, %{attempt_id: run.current_attempt_id, artifact_ids: run.artifacts})

    case run[:pending_validation_reply] do
      nil ->
        updated = run |> Map.put(:pending_validation_reply, pending) |> Map.put(:phase, :waiting_for_validation)
        {:ok, if(is_binary(reply.activity_id), do: record_activity(updated, reply.activity_id, reply.activity_body), else: updated)}

      ^pending ->
        {:ok, run}

      _ ->
        {:error, :validation_reply_already_pending}
    end
  end

  defp approve_wait(run, wait_id, digest, activity_id, activity_body) do
    wait = run.pending_wait

    cond do
      not is_map(wait) or Map.get(wait, :id) != wait_id or Map.get(wait, :kind) != :approval ->
        {:error, :stale_or_missing_approval}

      run.status != :waiting_for_answer ->
        {:error, :stale_or_missing_approval}

      run.artifacts != wait.artifact_ids or artifact_revision(run.artifacts) != wait.approval_digest ->
        {:error, :stale_artifact_revision}

      digest != wait.approval_digest ->
        {:error, :stale_artifact_revision}

      SymphonyElixir.ValidationCheck.protects_wait?(run) and not Map.get(run, :validated_wait_reply, false) ->
        queue_validation_reply(run, %{kind: :approval, wait_id: wait_id, digest: digest, activity_id: activity_id, activity_body: activity_body})

      true ->
        stage = Map.fetch!(run.definition.stages, run.stage_id)
        envelope = %{"approval" => true, "artifact_revision" => digest, "wait_id" => wait_id}
        outputs = Map.new(stage.outputs, &{&1, true})
        approval_evidence = %{approval: envelope, wait_id: wait_id, artifact_revision: digest, artifact_ids: wait.artifact_ids}
        run = update_attempt(run, %{status: :completed, result: %{status: :ok, evidence: approval_evidence}})
        run = put_answer_outputs(run, outputs)
        run = record_activity(run, activity_id, activity_body)
        {:ok, run |> Map.put(:pending_wait, nil) |> advance(stage.next)}
    end
  end

  defp apply_wait_answer(run, wait_id, reply, activity_id, activity_body) do
    stage = Map.fetch!(run.definition.stages, run.stage_id)
    wait = run.pending_wait

    cond do
      not is_map(wait) or wait.id != wait_id or run.status != :waiting_for_answer ->
        {:error, :stale_or_missing_human_wait}

      Map.get(stage, :approval, false) or Map.get(wait, :kind) == :approval ->
        {:error, :explicit_approval_required}

      Map.take(run.artifacts, stage.inputs) != wait.artifact_ids ->
        {:error, :stale_artifact_revision}

      not valid_wait_outputs?(reply, stage.outputs) ->
        {:error, :invalid_human_wait_outputs}

      SymphonyElixir.ValidationCheck.protects_wait?(run) and not Map.get(run, :validated_wait_reply, false) ->
        queue_validation_reply(run, %{kind: :answer, wait_id: wait_id, reply: reply, activity_id: activity_id, activity_body: activity_body})

      true ->
        evidence = %{reply: reply, wait_id: wait_id, artifact_ids: wait.artifact_ids}
        run = update_attempt(run, %{status: :completed, result: %{status: :ok, evidence: evidence}})
        run = put_answer_outputs(run, reply)
        run = if is_binary(activity_id), do: record_activity(run, activity_id, activity_body), else: run
        {:ok, run |> Map.put(:pending_wait, nil) |> advance(stage.next)}
    end
  end

  defp valid_wait_outputs?(reply, outputs) when is_map(reply) do
    with true <- Enum.sort(Map.keys(reply)) == Enum.sort(outputs),
         {:ok, encoded} <- Jason.encode(reply),
         true <- Jason.decode!(encoded) == reply,
         true <- Enum.all?(reply, fn {_key, value} -> not is_nil(value) end) do
      true
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  defp valid_wait_outputs?(_reply, _outputs), do: false

  defp decode_wait_reply(body) do
    case Jason.decode(body) do
      {:ok, reply} when is_map(reply) -> {:ok, reply}
      _ -> {:error, :invalid_human_wait_outputs}
    end
  end

  defp put_answer_outputs(run, outputs) do
    Enum.reduce(outputs, run, fn {key, value}, acc ->
      encoded = Jason.encode!(value)
      digest = :crypto.hash(:sha256, encoded) |> Base.encode16(case: :lower)
      artifact = %{run_id: run.id, attempt_id: run.current_attempt_id, sha256: digest}

      %{
        acc
        | outputs: Map.put(acc.outputs, key, value),
          artifacts: Map.put(acc.artifacts, key, artifact)
      }
    end)
  end

  defp active_questions(run) do
    Enum.filter(Map.values(run.questions), fn question ->
      question.status == :pending and question_current?(run, question) and question.artifact_ids == run.artifacts
    end)
  end

  defp question_current?(run, question) do
    if question.stage_id != run.stage_id do
      false
    else
      case run.status do
        :ready ->
          case run.continuation do
            %{stage_id: stage_id, delivery_attempt_id: nil} -> stage_id == run.stage_id
            _ -> false
          end

        status when status in [:executing, :reconciling, :waiting_for_answer] ->
          question.active_attempt_id == run.current_attempt_id

        _ ->
          false
      end
    end
  end

  defp normal_human_wait?(%{status: :waiting_for_answer} = run) do
    stage = Map.get(run.definition.stages, run.stage_id)
    is_map(run.pending_wait) and is_map(stage) and stage.type == :human_wait and not Map.get(stage, :approval, false)
  end

  defp normal_human_wait?(_run), do: false

  defp append_inbox_reply(%{status: status} = run, body, activity_id)
       when status in [:ready, :executing, :reconciling] do
    stage = Map.get(run.definition.stages, run.stage_id)

    if stage && stage.type == :agent do
      message = %{body: body, activity_id: activity_id, delivered_attempt_id: nil}
      {:ok, run |> Map.update!(:inbox, &(&1 ++ [message])) |> record_activity(activity_id, body)}
    else
      {:error, :no_active_agent}
    end
  end

  defp append_inbox_reply(_run, _body, _activity_id), do: {:error, :no_active_agent}

  defp assign_inbox(inbox, attempt_id) do
    {messages, remaining} =
      Enum.map_reduce(inbox, [], fn message, acc ->
        if is_nil(message.delivered_attempt_id) do
          assigned = Map.put(message, :delivered_attempt_id, attempt_id)
          {assigned, [assigned | acc]}
        else
          {message, [message | acc]}
        end
      end)

    assigned = Enum.filter(messages, &(&1.delivered_attempt_id == attempt_id))
    {Enum.reverse(remaining), Enum.map(assigned, &Map.drop(&1, [:delivered_attempt_id]))}
  end

  defp preserve_retry_context(run) do
    attempt = Enum.find(run.attempts, &(&1.id == run.current_attempt_id))

    if is_map(attempt) and attempt.type == :agent do
      delivered_ids = Map.get(attempt, :inbox_activity_ids, [])

      inbox =
        Enum.map(run.inbox, fn message ->
          if message.delivered_attempt_id == run.current_attempt_id or message.activity_id in delivered_ids do
            %{message | delivered_attempt_id: nil}
          else
            message
          end
        end)

      continuation = agent_continuation(run, inbox)
      %{run | inbox: inbox, continuation: continuation}
    else
      run
    end
  end

  defp agent_continuation(run, inbox) do
    previous =
      case run.continuation do
        %{stage_id: stage_id} = continuation when stage_id == run.stage_id -> Map.drop(continuation, [:delivery_attempt_id])
        _ -> %{}
      end

    answers =
      run.questions
      |> Map.values()
      |> Enum.filter(&(&1.status == :answered and &1.stage_id == run.stage_id and &1.artifact_ids == run.artifacts))
      |> Enum.sort_by(& &1.ordinal)
      |> Enum.map(fn question -> %{id: question.id, prompt: question.prompt, reply: Map.take(question.reply, [:body, :activity_id])} end)

    questions = (Map.get(previous, :questions, []) ++ answers) |> Enum.uniq_by(& &1.id)

    messages =
      (Map.get(previous, :messages, []) ++
         (Enum.filter(inbox, &(&1.delivered_attempt_id == run.current_attempt_id or is_nil(&1.delivered_attempt_id)))
          |> Enum.map(&Map.drop(&1, [:delivered_attempt_id]))))
      |> Enum.uniq_by(& &1.activity_id)

    pending_questions = pending_question_context(run, run.stage_id, run.artifacts)

    if map_size(previous) == 0 and answers == [] and messages == [] and pending_questions == [] do
      nil
    else
      latest_answer = List.last(questions)

      previous
      |> Map.put(:stage_id, run.stage_id)
      |> Map.put(:thread_id, Map.get(previous, :thread_id) || run.thread_id)
      |> Map.put(:session_id, Map.get(previous, :session_id) || run.session_id)
      |> Map.put(:question_id, if(latest_answer, do: latest_answer.id, else: Map.get(previous, :question_id)))
      |> Map.put(:question_prompt, if(latest_answer, do: latest_answer.prompt, else: Map.get(previous, :question_prompt)))
      |> Map.put(:reply, if(latest_answer, do: latest_answer.reply, else: Map.get(previous, :reply)))
      |> Map.put(:questions, questions)
      |> Map.put(:pending_questions, pending_questions)
      |> Map.put(:messages, messages)
      |> Map.put(:delivery_attempt_id, nil)
    end
  end

  defp pending_question_context(run, stage_id, artifact_ids) do
    run.questions
    |> Map.values()
    |> Enum.filter(&(&1.status == :pending and &1.stage_id == stage_id and &1.artifact_ids == artifact_ids))
    |> Enum.sort_by(& &1.ordinal)
    |> Enum.map(&Map.take(&1, [:id, :prompt, :artifact_ids]))
  end

  defp assign_continuation(run, attempt_id, inbox_messages) do
    base =
      case run.continuation do
        %{delivery_attempt_id: nil} = continuation -> Map.drop(continuation, [:delivery_attempt_id])
        _ -> %{}
      end

    messages =
      (Map.get(base, :messages, []) ++ inbox_messages)
      |> Enum.uniq_by(& &1.activity_id)

    if map_size(base) == 0 and messages == [] do
      nil
    else
      base
      |> Map.put_new(:stage_id, run.stage_id)
      |> Map.put_new(:thread_id, run.thread_id)
      |> Map.put_new(:session_id, run.session_id)
      |> Map.put_new(:question_id, nil)
      |> Map.put_new(:question_prompt, nil)
      |> Map.put_new(:reply, nil)
      |> Map.put(:messages, messages)
      |> Map.put(:delivery_attempt_id, attempt_id)
    end
  end

  defp pending_inbox?(run), do: Enum.any?(run.inbox, &is_nil(&1.delivered_attempt_id))

  defp queue_inbox_continuation(run, value, stage_id) do
    run = put_session(run, value)
    continuation = agent_continuation(run, run.inbox)

    %{run | status: :ready, phase: :queued, stage_id: stage_id, current_attempt_id: nil, continuation: continuation}
  end

  defp valid_question_request_id(nil), do: :ok

  defp valid_question_request_id(value) when is_binary(value) and byte_size(value) <= 256 do
    if String.trim(value) == "", do: {:error, :invalid_question_request_id}, else: :ok
  end

  defp valid_question_request_id(_value), do: {:error, :invalid_question_request_id}

  defp question_request_key(prompt, request_id) do
    identity = if is_binary(request_id), do: {:request_id, request_id}, else: {:prompt, String.trim(prompt)}
    :crypto.hash(:sha256, :erlang.term_to_binary(identity)) |> Base.encode16(case: :lower)
  end

  defp question_id(run, attempt_id, request_key) do
    identity = {run.id, run.stage_id, attempt_id, request_key, artifact_revision(run.artifacts)}
    digest = :crypto.hash(:sha256, :erlang.term_to_binary(identity)) |> Base.encode16(case: :lower)
    "question-" <> binary_part(digest, 0, 32)
  end

  defp activity_status(run, activity_id, body) do
    digest = activity_fingerprint(body)

    case Map.get(run.activity_ids, activity_id) do
      nil -> :new
      ^digest -> :duplicate
      _ -> {:error, :conflicting_linear_activity}
    end
  end

  defp record_activity(run, activity_id, body) do
    put_in(run.activity_ids[activity_id], activity_fingerprint(body))
  end

  defp activity_fingerprint(body), do: :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)

  defp valid_activity_id(activity_id) do
    if String.trim(activity_id) == "", do: {:error, :invalid_linear_activity_id}, else: :ok
  end

  defp artifact_revision(artifacts) do
    canonical = canonical_artifact_value(artifacts)
    :crypto.hash(:sha256, :erlang.term_to_binary(canonical)) |> Base.encode16(case: :lower)
  end

  defp canonical_artifact_value(value) when is_map(value) do
    value
    |> Enum.sort_by(fn {key, _value} -> key end)
    |> Enum.map(fn {key, nested} -> [canonical_artifact_value(key), canonical_artifact_value(nested)] end)
  end

  defp canonical_artifact_value(value) when is_list(value), do: Enum.map(value, &canonical_artifact_value/1)
  defp canonical_artifact_value(value), do: value

  defp activate_continuation_questions(questions, stage_id, artifact_ids, attempt_id) do
    Map.new(questions, fn {id, question} ->
      if question.status in [:pending, :answered] and question.stage_id == stage_id and question.artifact_ids == artifact_ids do
        {id, Map.put(question, :active_attempt_id, attempt_id)}
      else
        {id, question}
      end
    end)
  end

  defp close_pending_questions(run, stage_id, attempt_id) do
    questions =
      Map.new(run.questions, fn {id, question} ->
        if question.status == :pending and question.stage_id == stage_id and question.active_attempt_id == attempt_id do
          {id, Map.put(question, :status, :superseded)}
        else
          {id, question}
        end
      end)

    %{run | questions: questions}
  end

  defp workspace_branch(workspace) do
    case SymphonyElixir.CandidateGit.run(workspace, ["symbolic-ref", "--quiet", "--short", "HEAD"]) do
      {:ok, branch} -> String.trim(branch)
      _ -> nil
    end
  end

  defp update_attempt(run, changes) do
    %{run | attempts: Enum.map(run.attempts, fn attempt -> if attempt.id == run.current_attempt_id, do: Map.merge(attempt, changes), else: attempt end)}
  end

  defp stage_phase(:agent), do: :implementing
  defp stage_phase(:publish), do: :publishing
  defp stage_phase(_type), do: :validating

  defp finish_gate(run, %{type: :check, gate: %{evaluator: :github_validation}} = stage, result) do
    case SymphonyElixir.ValidationCheck.verify_attested_completion(run, stage, result) do
      :ok -> {result, :passed, %{verdict: :passed, verified: true}}
      {:error, reason} -> {{:error, reason}, :failed, %{verdict: :failed, verified: false, reason: inspect(reason)}}
    end
  end

  defp finish_gate(run, %{type: :check} = stage, result) do
    if candidate_validation?(stage) do
      verify_candidate_result(run, stage, result)
    else
      {result, nil, nil}
    end
  end

  defp finish_gate(run, %{type: :publish} = stage, result) do
    # This trusted check re-evaluates both the referenced validation gate and the publish gate.
    case {SymphonyElixir.Publisher.verify_completion(run, stage, result), result} do
      {:ok, {:ok, value}} when is_map(value) ->
        detail = %{
          "verdict" => "passed",
          "verified" => true,
          "rationale" => ["The trusted publisher verified the candidate and remote pull request."]
        }

        {result, :passed, detail}

      {:ok, _result} ->
        failed_publication_result(:invalid_publisher_completion)

      {{:error, reason}, _result} ->
        failed_publication_result(reason)

      {other, _result} ->
        failed_publication_result({:invalid_publisher_verification_result, other})
    end
  rescue
    error ->
      failed_publication_result({:publisher_verification_failed, error})
  catch
    kind, reason ->
      failed_publication_result({:publisher_verification_failed, kind, reason})
  end

  defp finish_gate(_run, _stage, result), do: {result, nil, nil}

  defp failed_publication_result(reason) do
    result = {:error, {:publication_completion_not_verified, reason}}

    detail = %{
      "verdict" => "failed",
      "verified" => false,
      "rationale" => ["Trusted publication completion could not be verified."],
      "reason" => inspect(reason)
    }

    {result, :failed, detail}
  end

  defp verify_candidate_result(run, stage, result) do
    context = %{
      task_id: run.task_id || "local",
      run_id: run.id || "local",
      attempt_id: run.current_attempt_id || "local",
      base_sha: run.execution[:validation_base]
    }

    opts = [
      workspace: validation_workspace(run),
      validation_archive: run.execution[:validation_archive],
      validation_scratch: run.execution[:validation_scratch]
    ]

    case SymphonyElixir.Validation.verify_result(
           result,
           run.execution[:validation_policy],
           context,
           stage.gate.required,
           opts
         ) do
      {:ok, %{verdict: verdict} = detail} when verdict in [:passed, :failed] ->
        case verified_candidate_feedback(result, run.execution[:validation_archive]) do
          {:ok, feedback} ->
            {put_candidate_result(result, detail, feedback), verdict, json_value(Map.put(detail, :verified, true))}

          {:error, reason} ->
            failed_validation_result(result, {:feedback_unavailable, reason})
        end

      {:error, reason} ->
        failed_validation_result(result, reason)
    end
  rescue
    error ->
      failed_validation_result(result, {:validation_verification_failed, error})
  end

  defp verified_candidate_feedback({:ok, %{receipt: receipt}}, archive) do
    case SymphonyElixir.Validation.feedback(receipt, archive) do
      {:ok, feedback} when is_list(feedback) -> {:ok, json_value(feedback)}
      {:error, _reason} = error -> error
    end
  end

  defp verified_candidate_feedback(_result, _archive), do: {:error, :missing_validation_receipt}

  defp validation_workspace(run) do
    case SymphonyElixir.CandidateTransfer.workspace(run) do
      {:ok, path} -> path
      _ -> nil
    end
  end

  defp failed_validation_result(result, reason) do
    failure = %{verdict: :failed, rationale: ["Validation evidence could not be verified."], evidence_id: nil}
    detail = %{"verdict" => "failed", "verified" => false, "reason" => inspect(reason)}
    {put_candidate_result(result, failure, nil), :failed, detail}
  end

  defp put_candidate_result(
         {:ok, %{receipt: %{"id" => id, "manifest_sha256" => manifest_sha256}}},
         gate,
         feedback
       )
       when is_binary(id) and is_binary(manifest_sha256) and is_list(feedback) do
    receipt = %{"id" => id, "manifest_sha256" => manifest_sha256}
    {:ok, %{receipt: receipt, gate: gate, feedback: feedback}}
  end

  defp put_candidate_result(_result, _gate, _feedback), do: {:error, :unverified_validation_result}

  defp candidate_validation?(%{type: :check, gate: %{evaluator: :candidate_validation}}), do: true
  defp candidate_validation?(_stage), do: false

  defp candidate_validation_failure(run, stage) do
    rounds = run.validation_repair_rounds + 1
    run = %{run | validation_repair_rounds: rounds}

    case stage.gate.failure do
      %{repair: repair, max_attempts: max_attempts} ->
        repairs = Map.get(run.repairs, stage.id, 0)

        if rounds < 3 and repairs < max_attempts do
          %{run | repairs: Map.put(run.repairs, stage.id, repairs + 1)} |> advance(repair)
        else
          advance(run, :blocked)
        end

      _ ->
        advance(run, :blocked)
    end
  end

  defp passing_check?({:ok, %{exit_status: 0, timed_out: false}}), do: true
  defp passing_check?(_), do: false

  # Opaque evaluator metadata uses JSON string keys, never transient BEAM atoms.
  defp json_stage_result({:ok, value} = result) when is_map(value) do
    case Jason.encode(value) do
      {:ok, _} -> result
      {:error, _} -> {:error, :invalid_stage_evidence}
    end
  end

  defp json_stage_result(result), do: result

  defp json_value(value), do: value |> Jason.encode!() |> Jason.decode!()

  defp result_evidence({:ok, value}) when is_map(value), do: %{status: :ok, evidence: json_value(value)}
  defp result_evidence({:error, reason}), do: %{status: :error, reason: inspect(reason)}
  defp result_evidence(_), do: %{status: :error, reason: "invalid_stage_result"}

  defp put_outputs(run, stage, value) do
    value = json_value(value)
    outputs = Enum.reduce(stage.outputs, run.outputs, &Map.put(&2, &1, value))
    digest = :crypto.hash(:sha256, :erlang.term_to_binary(value)) |> Base.encode16(case: :lower)
    artifact = %{run_id: run.id, attempt_id: run.current_attempt_id, sha256: digest}
    artifacts = Enum.reduce(stage.outputs, run.artifacts, &Map.put(&2, &1, artifact))
    %{run | outputs: outputs, artifacts: artifacts}
  end

  defp put_session(run, value), do: %{run | session_id: value[:session_id] || run.session_id, thread_id: value[:thread_id] || run.thread_id}

  defp advance(run, terminal) when terminal in [:complete, :blocked], do: %{run | status: terminal, phase: terminal, stage_id: terminal, current_attempt_id: nil}
  defp advance(run, next), do: %{run | status: :ready, phase: :queued, stage_id: next, current_attempt_id: nil}

  defp repair_or_block(run, %{id: id, gate: %{failure: %{repair: repair, max_attempts: max}}}) do
    count = Map.get(run.repairs, id, 0)
    if count < max, do: %{run | repairs: Map.put(run.repairs, id, count + 1)} |> advance(repair), else: advance(run, :blocked)
  end

  defp repair_or_block(run, _stage), do: advance(run, :blocked)
end
