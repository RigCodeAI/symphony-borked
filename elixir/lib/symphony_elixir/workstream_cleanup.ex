defmodule SymphonyElixir.WorkstreamCleanup do
  @moduledoc """
  Merge/archive gates for cleanup owned by the existing coordinator.

  Cancellation, unrecorded resources and uncertain workers retain their data.
  A merge proof is read-only and does not require deleting a child workspace.
  """

  alias SymphonyElixir.{Evidence, Publisher, PublisherGithub, ServiceCheckCompletion, WorkerOperation, Workspace, WorkstreamOutputs, WorkstreamRun}

  @version 1

  @doc "Pins owned directories before a newly queued run can execute. No retrofit of old paths."
  @spec enroll(map()) :: map()
  def enroll(run) do
    hosts = if run.execution[:worker_control], do: [nil, run.execution.worker_control["host"]], else: [nil]

    resources =
      Enum.reduce_while(Enum.uniq(hosts), {:ok, []}, fn host, {:ok, records} ->
        case Workspace.recorded_resource(run.workspace, host) do
          {:ok, resource} -> {:cont, {:ok, records ++ [Map.merge(resource, %{run_id: run.id, task_id: run.task_id})]}}
          _ -> {:halt, {:error, :resource_registration_unavailable}}
        end
      end)

    case resources do
      {:ok, records} -> Map.put(run, :managed_resources, records)
      {:error, reason} -> Map.put(run, :resource_registration_error, reason)
    end
  end

  @spec candidate?(map()) :: boolean()
  def candidate?(run) do
    run[:cleanup][:status] != :cleaned and eligible_status?(run) and
      not Enum.any?(Map.values(run[:questions] || %{}), &(&1[:status] == :pending)) and
      match?({:ok, _}, publication(run))
  end

  @doc "Service-verified merged revision and retained evidence for parent composition. No deletion."
  @spec merge_proof(map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def merge_proof(run, opts \\ []) do
    with true <- candidate?(run) or run[:cleanup][:status] == :cleaned,
         true <- WorkstreamRun.compatible_policy?(run),
         {:ok, publication} <- publication(run),
         {:ok, merge} <- confirmed_merge(run, publication, opts),
         :ok <- publication_archive(run, publication, opts),
         :ok <- verified_references(run, opts),
         {:ok, archive} <- Evidence.retained(run, evidence_server(opts)) do
      {:ok,
       %{
         version: @version,
         run_id: run.id,
         task_id: run.task_id,
         publication_id: publication.publication_id,
         publication_attempt_id: publication.attempt_id,
         repository: "RigCodeAI/rig",
         pr: publication.pr,
         published_head: publication.candidate_sha,
         merge: merge,
         archive: archive,
         retained_outputs: run[:retained_output_artifacts] || %{},
         retained_outputs_complete: complete_references?(run)
       }}
    else
      {:error, _} = error -> error
      _ -> {:error, :cleanup_run_not_eligible}
    end
  rescue
    _ -> {:error, :merge_proof_unavailable}
  catch
    _, _ -> {:error, :merge_proof_unavailable}
  end

  @doc "Observes merge and quiescence before the coordinator persists a deletion intent."
  @spec prepare(map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def prepare(run, opts \\ []) do
    with true <- candidate?(run),
         true <- WorkstreamRun.compatible_policy?(run),
         {:ok, publication} <- publication(run),
         :ok <- resources(run),
         :ok <- quiescent(run, opts),
         :ok <- detached_outputs(run),
         {:ok, refs} <- retain_outputs(run, opts),
         {:ok, merge} <- confirmed_merge(run, publication, opts) do
      intent = %{
        version: @version,
        policy: WorkstreamRun.policy(),
        run_id: run.id,
        task_id: run.task_id,
        publication: publication,
        merge: merge,
        resources: run.managed_resources,
        operations: operation_identities(run),
        retained_outputs: refs
      }

      {:ok, Map.put(intent, :id, fingerprint(intent))}
    else
      {:error, _} = error -> error
      _ -> {:error, :cleanup_run_not_eligible}
    end
  rescue
    _ -> {:error, :cleanup_observation_unavailable}
  catch
    _, _ -> {:error, :cleanup_observation_unavailable}
  end

  @doc "Rechecks every gate immediately before removing one pinned resource."
  @spec delete(map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def delete(run, opts \\ []) do
    with %{intent: intent, status: :pending} <- run[:cleanup],
         {:ok, ^intent} <- prepare(run, opts),
         :ok <- publication_archive(run, intent.publication, opts),
         {:ok, proof} <- Evidence.retained(run, evidence_server(opts)),
         :ok <- owner_check(opts, proof.revision),
         :ok <- remove_resources(run, proof, opts) do
      {:ok, %{archive_revision: proof.revision, intent_id: intent.id}}
    else
      {:error, _} = error -> error
      _ -> {:error, :cleanup_intent_changed}
    end
  rescue
    _ -> {:error, :cleanup_outcome_unknown}
  catch
    _, _ -> {:error, :cleanup_outcome_unknown}
  end

  @spec plan(map(), map()) :: map()
  def plan(run, intent) do
    attempts =
      Enum.map(run.attempts, fn attempt ->
        if attempt.id == run.current_attempt_id and attempt.type == :human_wait,
          do: Map.merge(attempt, %{status: :completed, result: %{resolved_by: :github_merge}, gate: :passed}),
          else: attempt
      end)

    run
    |> Map.merge(%{status: :complete, phase: :cleanup_pending, stage_id: :complete, current_attempt_id: nil, pending_wait: nil, attempts: attempts})
    |> Map.put(:cleanup, %{intent: intent, status: :pending, attempts: 0, last_error: nil})
    |> Map.put(:retained_output_artifacts, intent.retained_outputs)
  end

  @spec result(map(), {:ok, map()} | {:error, atom()}) :: map()
  def result(run, {:ok, proof}),
    do: put_in(run.cleanup, Map.merge(run.cleanup, %{status: :cleaned, proof: proof, last_error: nil}))

  def result(run, {:error, reason}) do
    if run.cleanup.last_error == reason,
      do: run,
      else: put_in(run.cleanup, Map.merge(run.cleanup, %{attempts: run.cleanup.attempts + 1, last_error: reason}))
  end

  @spec fingerprint(term()) :: String.t()
  def fingerprint(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value)) |> Base.encode16(case: :lower)

  @spec overlaps?(Path.t(), Path.t()) :: boolean()
  def overlaps?(a, b), do: a == b or String.starts_with?(a, b <> "/") or String.starts_with?(b, a <> "/")

  defp eligible_status?(%{status: :complete}), do: true

  defp eligible_status?(%{status: :waiting_for_answer, pending_wait: wait} = run) when is_map(wait) do
    stage = run.definition.stages[run.stage_id]

    is_map(stage) and stage.type == :human_wait and stage[:approval] != true and
      stage.next == :complete and "publication" in stage.inputs and is_nil(wait[:approval_digest])
  end

  defp eligible_status?(_run), do: false

  defp publication(run) do
    attempt = Enum.find(Enum.reverse(run.attempts), &(&1.type == :publish))
    value = if attempt, do: get_in(attempt, [:result, :evidence]), else: nil
    stage = if attempt, do: run.definition.stages[attempt.stage], else: nil
    operation = if attempt, do: run.operations[attempt.id], else: nil

    with %{status: :completed, gate: :passed} <- attempt,
         %{type: :publish} <- stage,
         %{status: :completed, publication_intent: intent} <- operation,
         true <- is_map(value),
         true <- value["branch"] == Publisher.branch(run),
         true <- valid_sha?(value["candidate_sha"]),
         true <- valid_digest?(value["publication_id"]) and valid_digest?(value["evidence_revision"]),
         true <-
           Enum.all?(
             [:branch, :candidate_sha, :publication_id, :evidence_revision, :validation_attempt_id],
             &(intent[&1] == value[Atom.to_string(&1)])
           ),
         %{"number" => number, "url" => url} <- value["pr"],
         true <- is_integer(number) and number > 0 and url == "https://github.com/RigCodeAI/rig/pull/#{number}" do
      {:ok,
       %{
         attempt_id: attempt.id,
         candidate_sha: value["candidate_sha"],
         branch: value["branch"],
         publication_id: value["publication_id"],
         evidence_revision: value["evidence_revision"],
         validation_attempt_id: value["validation_attempt_id"],
         pr: %{number: number, url: url}
       }}
    else
      _ -> {:error, :recorded_publication_unavailable}
    end
  end

  defp confirmed_merge(run, publication, opts) do
    transport = Keyword.get(opts, :cleanup_github, PublisherGithub)

    with {:ok, config} <- Publisher.validate_config(run.execution[:publisher]),
         {:ok, pr} <- transport.get_pr(config, publication.pr.number),
         true <- pr.number == publication.pr.number and pr.url == publication.pr.url,
         true <- pr.head_sha == publication.candidate_sha and pr.head_branch == publication.branch,
         true <- pr.base_branch == "main" and pr.state == "closed" and pr[:merged] == true,
         true <- valid_sha?(pr[:merge_commit_sha]),
         true <- is_binary(pr[:merged_at]),
         {:ok, _, _} <- DateTime.from_iso8601(pr[:merged_at]),
         %{"id" => id, "login" => login, "type" => "User"} <- pr[:merged_by],
         true <- is_integer(id) and id > 0 and is_binary(login) and login != "" and not String.ends_with?(login, "[bot]") do
      {:ok, %{commit: pr.merge_commit_sha, merged_at: pr.merged_at, merged_by: pr.merged_by}}
    else
      _ -> {:error, :github_merge_not_verified}
    end
  end

  defp publication_archive(run, publication, opts) do
    attempt = Enum.find(run.attempts, &(&1.id == publication.validation_attempt_id))
    receipt = if attempt, do: get_in(attempt, [:result, :evidence, "receipt"]), else: nil

    case Evidence.publication_revision(run, publication.validation_attempt_id, receipt, publication.evidence_revision, evidence_server(opts)) do
      {:ok, _} -> :ok
      _ -> {:error, :publication_archive_unavailable}
    end
  end

  defp resources(run) do
    records = run[:managed_resources]
    root = run.execution[:workspace_root]
    expected_hosts = if run.execution[:worker_control], do: [nil, run.execution.worker_control["host"]], else: [nil]

    if is_list(records) and records != [] and Enum.sort(Enum.map(records, & &1.host)) == Enum.sort(Enum.uniq(expected_hosts)) and
         Enum.all?(records, fn resource ->
           resource.run_id == run.id and resource.task_id == run.task_id and resource.path == run.workspace and
             resource.root == root and is_map(resource.identity) and resource.identity["configured_root"] == root
         end), do: :ok, else: {:error, :owned_resources_unavailable}
  end

  defp quiescent(run, opts) do
    if Enum.all?(run.attempts, fn attempt ->
         operation = run.operations[attempt.id]
         attempt.type == :human_wait or (is_map(operation) and operation[:id] == attempt.id and operation[:stage] == attempt.stage and operation[:type] == attempt.type)
       end), do: operations_quiescent(run, opts), else: {:error, :worker_termination_unknown}
  end

  defp operations_quiescent(run, opts) do
    reconcile = Keyword.get(opts, :cleanup_worker_reconciler, &WorkerOperation.reconcile/2)

    Enum.reduce_while(run.operations, :ok, fn {id, operation}, :ok ->
      cond do
        operation.status not in [:completed, :not_applied] ->
          {:halt, {:error, :worker_termination_unknown}}

        operation[:external_process] ->
          identity = operation.external_process

          if identity["kind"] == "systemd-unit" and identity["operation_id"] == id and
               reconcile.(run.execution[:worker_control], identity) == :terminated, do: {:cont, :ok}, else: {:halt, {:error, :worker_termination_unknown}}

        operation.type in [:agent, :check] and operation.status != :not_applied ->
          if ServiceCheckCompletion.verified?(run, id), do: {:cont, :ok}, else: {:halt, {:error, :worker_termination_unknown}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  @doc "Produces typed output references once their actual redacted bytes are permanently archived."
  @spec retain_outputs(map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def retain_outputs(run, opts \\ []) do
    refs = run[:retained_output_artifacts] || %{}
    required = is_binary(run[:parent_run_id]) or not legacy_detached?(run) or map_size(refs) > 0

    cond do
      not required ->
        {:ok, refs}

      map_size(refs) > 0 ->
        if Enum.sort(Map.keys(refs)) == Enum.sort(Map.keys(run.artifacts)) do
          with :ok <- verified_references(run, opts), do: {:ok, refs}
        else
          {:error, :parent_outputs_not_retained}
        end

      map_size(run.artifacts) == 0 ->
        {:error, :parent_outputs_not_retained}

      true ->
        produce_references(run, opts)
    end
  end

  defp produce_references(run, opts) do
    server = evidence_server(opts)

    with {:ok, projections} <- projections(run),
         :ok <- store_projections(run, projections, server),
         {:ok, archive} <- Evidence.retained(run, server) do
      refs =
        Map.new(projections, fn {name, projection} ->
          bytes = Jason.encode!(projection)
          sha = fingerprint_bytes(bytes)

          ref = %{
            version: 2,
            kind: projection["kind"],
            run_id: run.id,
            task_key: Evidence.task_key(run.task_id),
            revision: archive.revision,
            name: "artifact-" <> sha <> ".txt",
            sha256: sha,
            output_id: run.artifacts[name]
          }

          {name, ref}
        end)

      with :ok <- verified_references(Map.put(run, :retained_output_artifacts, refs), opts), do: {:ok, refs}
    else
      {:error, :current_evidence_not_permanently_archived} -> {:error, :retained_output_archive_unavailable}
      {:error, _} = error -> error
    end
  end

  defp projections(run) do
    Enum.reduce_while(Map.keys(run.artifacts), {:ok, %{}}, fn name, {:ok, values} ->
      case WorkstreamOutputs.projection(run, name) do
        {:ok, value} -> {:cont, {:ok, Map.put(values, name, value)}}
        error -> {:halt, error}
      end
    end)
  end

  defp store_projections(run, projections, server) do
    Enum.reduce_while(projections, :ok, fn {name, value}, :ok ->
      case Evidence.artifact(run.id, "retained-output/" <> name, Jason.encode!(value), server) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp verified_references(run, opts) do
    Enum.reduce_while(run[:retained_output_artifacts] || %{}, :ok, fn {name, ref}, :ok ->
      value = run.outputs[name]

      with true <- Map.has_key?(run.artifacts, name),
           true <- ref[:output_id] == run.artifacts[name] and ref[:run_id] == run.id,
           true <- ref[:task_key] == Evidence.task_key(run.task_id) and valid_digest?(ref[:sha256]),
           {:ok, bytes} <- Evidence.retained_artifact(ref, evidence_server(opts)),
           true <- :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower) == ref.sha256,
           {:ok, retained} <- Jason.decode(bytes),
           true <- matching_output?(run, name, ref, value, retained) do
        {:cont, :ok}
      else
        _ -> {:halt, {:error, :parent_outputs_not_retained}}
      end
    end)
  end

  defp complete_references?(run),
    do:
      map_size(run.artifacts) > 0 and
        Enum.sort(Map.keys(run[:retained_output_artifacts] || %{})) == Enum.sort(Map.keys(run.artifacts))

  defp matching_output?(run, name, %{version: 2, kind: kind}, _value, retained) do
    case WorkstreamOutputs.projection(run, name) do
      {:ok, ^retained} -> retained["kind"] == kind
      _ -> false
    end
  end

  defp matching_output?(run, _name, _ref, value, retained), do: value == retained and not workspace_dependent?(value, run.workspace)

  @doc "Reads a verified typed retained output without requiring the disposable workspace."
  @spec retained_output(map(), String.t(), keyword()) :: {:ok, map()} | {:error, atom()}
  def retained_output(run, name, opts \\ []) do
    with ref when is_map(ref) <- run[:retained_output_artifacts][name],
         :ok <- verified_references(%{run | retained_output_artifacts: %{name => ref}}, opts),
         {:ok, bytes} <- Evidence.retained_artifact(ref, evidence_server(opts)),
         {:ok, value} <- Jason.decode(bytes) do
      {:ok, value}
    else
      _ -> {:error, :parent_outputs_not_retained}
    end
  end

  defp detached_outputs(run) do
    if legacy_detached?(run) or match?({:ok, _}, projections(run)), do: :ok, else: {:error, :workspace_outputs_not_retained}
  end

  defp legacy_detached?(run) do
    publication = Enum.find(Enum.reverse(run.attempts), &(&1.type == :publish))
    trusted = if publication, do: get_in(publication, [:result, :evidence]), else: nil

    Enum.all?(Map.keys(run.artifacts), fn name ->
      value = run.outputs[name]
      # This service-owned output is PR metadata, not a filesystem reference.
      (name == "publication" and value == trusted) or inline_value?(value)
    end)
  end

  # Unknown string values may be paths even without an extension or separator.
  # A future retained-file adapter must preserve actual bytes, not path text.
  defp inline_value?(value) when is_number(value) or is_boolean(value) or is_nil(value), do: true
  defp inline_value?(value) when is_list(value), do: Enum.all?(value, &inline_value?/1)
  defp inline_value?(value) when is_map(value), do: Enum.all?(value, fn {_key, nested} -> inline_value?(nested) end)
  defp inline_value?(_value), do: false

  defp workspace_dependent?(value, path) when is_binary(value), do: value == path or String.contains?(value, path <> "/")
  defp workspace_dependent?(value, path) when is_list(value), do: Enum.any?(value, &workspace_dependent?(&1, path))

  defp workspace_dependent?(value, path) when is_map(value) do
    Enum.any?(value, fn {k, v} ->
      (to_string(k) in ~w(path file workspace cwd filename) and is_binary(v)) or
        workspace_dependent?(k, path) or workspace_dependent?(v, path)
    end)
  end

  defp workspace_dependent?(_value, _path), do: false

  defp operation_identities(run), do: Map.new(run.operations, fn {id, op} -> {id, Map.take(op, [:stage, :type, :status, :external_process, :service_completion])} end)
  defp fingerprint_bytes(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp evidence_server(opts), do: Keyword.get(opts, :cleanup_evidence_server, Evidence)

  defp owner_check(opts, revision) do
    case opts[:on_cleanup_check] do
      fun when is_function(fun, 1) -> fun.(revision)
      _ -> {:error, :cleanup_requires_live_owner}
    end
  end

  defp remove_resources(run, proof, opts) do
    Enum.reduce_while(run.managed_resources, :ok, fn resource, :ok ->
      with :ok <- quiescent(run, opts),
           {:ok, %{revision: revision}} when revision == proof.revision <- Evidence.retained(run, evidence_server(opts)),
           :ok <- owner_check(opts, revision),
           {:ok, _} <- Workspace.remove_owned(resource) do
        {:cont, :ok}
      else
        _ -> {:halt, {:error, :owned_resource_removal_failed}}
      end
    end)
  end

  defp valid_sha?(sha), do: is_binary(sha) and Regex.match?(~r/^[a-f0-9]{40}$/, sha)
  defp valid_digest?(sha), do: is_binary(sha) and Regex.match?(~r/^[a-f0-9]{64}$/, sha)
end
