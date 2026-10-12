defmodule SymphonyElixir.Publisher do
  @moduledoc """
  Trusted, candidate-bound publication for Rig. Definitions select this action,
  not a shell command. Credentials are resolved only by the service transport.
  A deterministic task branch and remote reconciliation survive lost responses.
  """

  alias SymphonyElixir.{CandidateGit, CandidateTransfer, Config, Evidence, PathSafety, PublisherGithub, Validation, WorkstreamRun}

  @begin "<!-- factory-evidence:start -->"
  @section_end "<!-- factory-evidence:end -->"

  @doc "Pins non-secret publisher references outside candidate control."
  @spec pin(keyword(), Path.t(), map()) :: {:ok, map()} | {:error, term()}
  def pin(opts, workspace, definition) do
    if Enum.any?(definition.stages, fn {_id, stage} -> stage.type == :publish end) do
      config = Keyword.get_lazy(opts, :publisher, fn -> Config.publisher() end)

      config =
        case config do
          {:ok, value} -> value
          value -> value
        end

      with {:ok, config} <- validate_config(config),
           {:ok, path} <- PathSafety.canonicalize(config.private_key_path),
           true <- path == config.private_key_path and not overlap?(workspace, path) do
        {:ok, %{publisher: config}}
      else
        false -> {:error, :publisher_key_overlaps_candidate_or_changes_path}
        {:error, _} = error -> error
        _ -> {:error, :publisher_not_configured}
      end
    else
      {:ok, %{}}
    end
  end

  @spec validate_config(term()) :: {:ok, map()} | {:error, atom()}
  def validate_config(config) when is_map(config) do
    normalized = %{
      app_id: config[:app_id] || config["app_id"],
      installation_id: config[:installation_id] || config["installation_id"],
      private_key_path: config[:private_key_path] || config["private_key_path"]
    }

    allowed = [:app_id, :installation_id, :private_key_path, "app_id", "installation_id", "private_key_path"]

    if Enum.all?(Map.keys(config), &(&1 in allowed)) and positive_id?(normalized.app_id) and
         positive_id?(normalized.installation_id) and is_binary(normalized.private_key_path) and
         Path.type(normalized.private_key_path) == :absolute do
      {:ok, normalized}
    else
      {:error, :invalid_publisher_config}
    end
  end

  def validate_config(_config), do: {:error, :publisher_not_configured}

  @doc "Executes one service-owned stage. No remote mutation happens before both trusted gates pass."
  @spec publish(map(), map(), keyword()) :: {:ok, map()} | {:error, term()} | {:uncertain, term()}
  def publish(run, stage, opts \\ []) do
    :global.trans({{__MODULE__, branch(run)}, self()}, fn -> publish_locked(run, stage, opts) end)
  rescue
    _ -> {:uncertain, :publication_outcome_unknown}
  catch
    _, _ -> {:uncertain, :publication_outcome_unknown}
  end

  @spec eligible(map(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def eligible(run, stage, opts \\ []) do
    with true <- run.status in [:executing, :reconciling] and run.stage_id == stage.id and stage.type == :publish,
         true <- run.definition.stages[stage.id] == stage and WorkstreamRun.compatible_policy?(run),
         validation_stage when is_map(validation_stage) <- run.definition.stages[stage.gate.validation_stage],
         true <- validation_stage.type == :check and validation_stage.gate[:evaluator] == :candidate_validation,
         attempt when is_map(attempt) <- latest_validation_attempt(run, validation_stage.id),
         true <- attempt.status == :completed and attempt.gate == :passed,
         %{"id" => _, "manifest_sha256" => _} = receipt <- get_in(attempt, [:result, :evidence, "receipt"]),
         {:ok, workspace} <- CandidateTransfer.workspace(run),
         context = validation_context(run, attempt),
         validation_opts = validation_options(run, workspace),
         {:ok, %{verdict: :passed}} <- Validation.verify_result({:ok, %{receipt: receipt}}, run.execution[:validation_policy], context, validation_stage.gate.required, validation_opts),
         {:ok, %{verdict: :passed} = gate} <- Validation.verify_result({:ok, %{receipt: receipt}}, run.execution[:validation_policy], context, stage.gate.required, validation_opts),
         {:ok, manifest} <- Validation.read_receipt(receipt, run.execution[:validation_archive]),
         {:ok, archive} <- archived(run, attempt.id, receipt, opts) do
      {:ok, %{manifest: manifest, receipt: receipt, validation_attempt_id: attempt.id, archive: archive, gate: gate, workspace: workspace}}
    else
      {:error, _} = error -> error
      _ -> {:error, :unqualified_publication_candidate}
    end
  rescue
    _ -> {:error, :unqualified_publication_candidate}
  end

  @doc "Rechecks trusted evidence and the actual GitHub branch/PR before accepting stage completion."
  @spec verify_completion(map(), map(), term()) :: :ok | {:error, term()}
  def verify_completion(run, stage, {:ok, result}) when is_map(result) do
    opts = [evidence_revision: result[:evidence_revision]]
    transport = transport(run, [])

    with {:ok, config} <- validate_config(run.execution[:publisher]),
         true <- is_binary(result[:evidence_revision]) and Regex.match?(~r/^[a-f0-9]{64}$/, result.evidence_revision),
         {:ok, proof} <- eligible(run, stage, opts),
         true <- result[:candidate_sha] == proof.manifest["candidate_sha"] and result[:branch] == branch(run),
         true <- result[:publication_id] == publication_id(run, stage, proof),
         true <- result[:validation_attempt_id] == proof.validation_attempt_id,
         %{number: number} <- result[:pr],
         {:ok, actual} <- transport.get_pr(config, number),
         :ok <- valid_pr(actual, result.branch, result.candidate_sha),
         {:ok, remote_sha} <- transport.branch_sha(config, result.branch),
         true <- remote_sha == result.candidate_sha,
         true <- actual.url == result.pr.url do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :publication_completion_not_verified}
    end
  rescue
    _ -> {:error, :publication_completion_not_verified}
  end

  def verify_completion(_run, _stage, _result), do: {:error, :publication_completion_not_verified}

  @doc "An active publication may retry by reconciling remote state before any mutation."
  @spec reconcile(map(), map()) :: :retry_safe | :unknown
  def reconcile(run, operation) do
    stage = run.definition.stages[operation.stage]
    if stage && stage.type == :publish, do: :retry_safe, else: :unknown
  end

  @doc "Records only safe publication identity before any GitHub write."
  @spec intent(map(), map(), map()) :: map()
  def intent(run, stage, proof) do
    %{
      branch: branch(run),
      candidate_sha: proof.manifest["candidate_sha"],
      publication_id: publication_id(run, stage, proof),
      validation_attempt_id: proof.validation_attempt_id,
      evidence_revision: proof.archive.revision
    }
  end

  @doc "Observes a canceled publication without creating, updating, or pushing anything."
  @spec observe_stopped(map(), map()) :: {:ok, map()} | {:error, atom()}
  def observe_stopped(run, operation) do
    transport = transport(run, [])

    with %{branch: branch, candidate_sha: sha} = intent <- operation[:publication_intent],
         true <- branch == branch(run),
         {:ok, config} <- validate_config(run.execution[:publisher]),
         {:ok, remote_sha} <- transport.branch_sha(config, branch),
         {:ok, pr} <- transport.find_pr(config, branch) do
      pr = if is_map(pr), do: Map.take(pr, [:number, :url, :head_sha, :head_branch, :base_branch, :state]), else: nil

      status =
        cond do
          pr && pr.head_sha == sha && pr.head_branch == branch && pr.base_branch == "main" && remote_sha == sha -> :observed
          pr || (remote_sha && remote_sha != sha) -> :mismatched
          true -> :pending
        end

      {:ok, %{status: status, branch: branch, candidate_sha: sha, remote_sha: remote_sha, pr: pr, publication_id: intent.publication_id}}
    else
      _ -> {:error, :publication_observation_unavailable}
    end
  rescue
    _ -> {:error, :publication_observation_unavailable}
  end

  @spec branch(map()) :: String.t()
  def branch(run), do: "cycle/factory-" <> binary_part(digest(run.task_id), 0, 24)

  defp publish_locked(run, stage, opts) do
    transport = transport(run, opts)

    with {:ok, config} <- validate_config(run.execution[:publisher]),
         :ok <- live_owner(run, opts),
         {:ok, proof} <- eligible(run, stage, opts),
         sha = proof.manifest["candidate_sha"],
         branch = branch(run),
         {:ok, existing} <- transport.find_pr(config, branch),
         :ok <- open_or_absent(existing, branch),
         {:ok, remote_sha} <- transport.branch_sha(config, branch),
         :ok <- expected_head(run, stage, remote_sha, sha, existing, proof.workspace),
         :ok <- recheck(run, stage, proof, opts),
         :ok <- record_intent(run, stage, proof, opts),
         :ok <- push_unless_present(transport, config, proof.workspace, sha, branch, remote_sha),
         {:ok, confirmed_sha} <- transport.branch_sha(config, branch),
         true <- confirmed_sha == sha,
         {:ok, current} <- transport.find_pr(config, branch),
         :ok <- open_or_absent(current, branch),
         :ok <- recheck(run, stage, proof, opts),
         {:ok, pr} <- create_or_update(transport, config, run, stage, proof, current, opts),
         {:ok, actual} <- transport.get_pr(config, pr.number),
         :ok <- valid_pr(actual, branch, sha),
         {:ok, final_sha} <- transport.branch_sha(config, branch),
         true <- final_sha == sha,
         :ok <- recheck(run, stage, proof, opts) do
      {:ok,
       %{
         candidate_sha: sha,
         branch: branch,
         pr: actual,
         publication_id: publication_id(run, stage, proof),
         evidence_revision: proof.archive.revision,
         validation_attempt_id: proof.validation_attempt_id
       }}
    else
      {:error, :validation_not_permanently_archived} -> {:uncertain, :validation_not_permanently_archived}
      {:error, {:github_request_failed, _}} -> {:uncertain, :publication_outcome_unknown}
      {:error, {:git_push_unknown, _}} -> {:uncertain, :publication_outcome_unknown}
      {:error, :publication_coordinator_unavailable} -> {:uncertain, :publication_outcome_unknown}
      {:error, _} = error -> error
      {:uncertain, _} = uncertain -> uncertain
      _ -> {:error, :publication_remote_identity_mismatch}
    end
  end

  defp archived(run, attempt_id, receipt, opts) do
    server = Keyword.get(opts, :evidence_server, run.execution[:evidence_server] || Evidence)

    case opts[:evidence_revision] do
      nil -> Evidence.publication(run, attempt_id, receipt, server)
      revision -> Evidence.publication_revision(run, attempt_id, receipt, revision, server)
    end
  end

  defp recheck(run, stage, proof, opts) do
    with :ok <- live_owner(run, opts) do
      case eligible(run, stage, Keyword.put(opts, :evidence_revision, proof.archive.revision)) do
        {:ok, %{manifest: manifest}} when manifest == proof.manifest -> :ok
        {:error, _} = error -> error
        _ -> {:error, :candidate_changed_before_publication}
      end
    end
  end

  defp live_owner(run, opts) do
    case opts[:on_publication_check] do
      callback when is_function(callback, 0) -> callback.()
      nil -> if(transport(run, opts) == PublisherGithub, do: {:error, :publication_requires_live_coordinator}, else: :ok)
      _ -> {:error, :invalid_publication_owner_callback}
    end
  end

  defp record_intent(run, stage, proof, opts) do
    case opts[:on_publication_intent] do
      callback when is_function(callback, 1) -> callback.(intent(run, stage, proof))
      nil -> if(transport(run, opts) == PublisherGithub, do: {:error, :publication_requires_durable_intent}, else: :ok)
      _ -> {:error, :invalid_publication_intent_callback}
    end
  end

  defp latest_validation_attempt(run, id), do: run.attempts |> Enum.reverse() |> Enum.find(&(&1.stage == id))

  defp validation_context(run, attempt),
    do: %{task_id: run.task_id, run_id: run.id, attempt_id: attempt.id, base_sha: run.execution[:validation_base]}

  defp validation_options(run, workspace),
    do: [workspace: workspace, validation_archive: run.execution[:validation_archive], validation_scratch: run.execution[:validation_scratch]]

  defp transport(run, opts), do: Keyword.get(opts, :publisher_transport, run.execution[:publisher_transport] || PublisherGithub)

  defp open_or_absent(nil, _branch), do: :ok
  defp open_or_absent(%{state: "open", head_branch: branch, base_branch: "main"}, branch), do: :ok
  defp open_or_absent(_pr, _branch), do: {:error, :publication_pr_closed_or_mismatched}

  defp expected_head(_run, _stage, nil, _sha, nil, _workspace), do: :ok
  defp expected_head(_run, _stage, sha, sha, _pr, _workspace), do: :ok

  defp expected_head(run, stage, remote_sha, sha, existing, workspace) do
    previous =
      run.attempts
      |> Enum.reverse()
      |> Enum.find_value(fn attempt ->
        if attempt.stage == stage.id and attempt.gate == :passed, do: get_in(attempt, [:result, :evidence, "candidate_sha"])
      end)

    with true <- is_binary(previous) and remote_sha == previous and is_map(existing) and existing.head_sha == previous,
         {:ok, _} <- CandidateGit.run(workspace, ["merge-base", "--is-ancestor", previous, sha]),
         do: :ok,
         else: (_ -> {:error, :publication_branch_changed_outside_run})
  end

  defp push_unless_present(_transport, _config, _workspace, sha, _branch, sha), do: :ok
  defp push_unless_present(transport, config, workspace, sha, branch, remote_sha), do: transport.push(config, workspace, sha, branch, remote_sha)

  defp create_or_update(transport, config, run, stage, proof, nil, _opts) do
    with {:ok, template} <- CandidateGit.run(proof.workspace, ["show", proof.manifest["base_sha"] <> ":.github/pull_request_template.md"]),
         {:ok, section} <- summary(run, stage, proof),
         {:ok, body} <- managed_body(template <> "\n\n" <> task_link(run), section) do
      transport.create_pr(config, branch(run), title(run), body)
    end
  end

  defp create_or_update(transport, config, run, stage, proof, current, _opts) do
    with {:ok, section} <- summary(run, stage, proof),
         {:ok, body} <- managed_body(current.body, section) do
      if current.body == body, do: {:ok, current}, else: transport.update_pr(config, current.number, body, current.body)
    end
  end

  @doc "Replaces only the managed evidence section, preserving every byte outside it."
  @spec managed_body(String.t(), String.t()) :: {:ok, String.t()} | {:error, atom()}
  def managed_body(body, section) when is_binary(body) and is_binary(section) do
    begins = :binary.matches(body, @begin)
    ends = :binary.matches(body, @section_end)
    replacement = @begin <> "\n" <> section <> "\n" <> @section_end

    case {begins, ends} do
      {[], []} ->
        {:ok, body <> "\n\n" <> replacement}

      {[{start, _}], [{finish, _}]} when start < finish ->
        stop = finish + byte_size(@section_end)
        {:ok, binary_part(body, 0, start) <> replacement <> binary_part(body, stop, byte_size(body) - stop)}

      _ ->
        {:error, :ambiguous_managed_pr_section}
    end
  end

  def managed_body(_body, _section), do: {:error, :invalid_pr_body}

  defp summary(run, stage, proof) do
    case Evidence.url(run) || run.execution[:evidence_url] do
      url when is_binary(url) ->
        id = publication_id(run, stage, proof)
        evidence = url <> "/artifacts/" <> proof.archive.revision <> "/" <> proof.archive.validation_manifest
        rows = Enum.map_join(proof.manifest["checks"], "\n", fn check -> "| #{cell(check["id"])} | passed | `#{proof.manifest["candidate_sha"]}` | [permanent evidence](#{evidence}) |" end)

        {:ok,
         "Publication: `#{id}`\n\n#{task_link(run)}\n\n| Required check | Result | Tested commit | Evidence |\n| --- | --- | --- | --- |\n" <>
           rows <>
           "\n\nPolicy: `#{proof.manifest["policy_revision"]}` (`#{proof.manifest["policy_digest"]}`).\nValidation receipt: `#{proof.receipt["manifest_sha256"]}`.\nPrivate conversation: [open](#{url})."}

      _ ->
        {:error, :permanent_evidence_url_unavailable}
    end
  end

  defp valid_pr(%{number: number, url: url, state: "open", head_sha: sha, head_branch: branch, base_branch: "main"}, branch, sha)
       when is_integer(number) and number > 0 do
    if url == "https://github.com/RigCodeAI/rig/pull/#{number}", do: :ok, else: {:error, :publication_remote_identity_mismatch}
  end

  defp valid_pr(_pr, _branch, _sha), do: {:error, :publication_remote_identity_mismatch}

  defp publication_id(run, stage, proof), do: digest(Jason.encode!([run.id, stage.id, proof.manifest["candidate_sha"], proof.receipt]))

  defp task_identifier(run) do
    case Regex.run(~r/\bDEV-[0-9]+\b/, run.definition.input_values["task"] || "", capture: :first) do
      [id] -> id
      _ -> nil
    end
  end

  defp task_link(run) do
    case task_identifier(run) do
      nil -> "Factory task: `#{cell(run.task_id)}`"
      id -> "Linear: [#{id}](https://linear.app/rigai/issue/#{id})"
    end
  end

  defp title(run), do: (task_identifier(run) || "Factory") <> ": validated Rig candidate"
  defp cell(value), do: value |> to_string() |> String.replace(["|", "\n", "\r", "`"], " ")
  defp digest(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
  defp positive_id?(value) when is_integer(value), do: value > 0
  defp positive_id?(value) when is_binary(value), do: Regex.match?(~r/^[1-9][0-9]*$/, value)
  defp positive_id?(_value), do: false
  defp overlap?(root, path), do: path == root or String.starts_with?(path, root <> "/")
end
