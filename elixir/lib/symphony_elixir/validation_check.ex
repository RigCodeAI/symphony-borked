defmodule SymphonyElixir.ValidationCheck do
  @moduledoc """
  Coordinator-owned GitHub validation gate. A check is a projection of authenticated,
  permanently archived evidence under the pinned stage contract, never a new authority.
  Remote head/base changes invalidate readiness even if a previous check passed.
  """

  alias SymphonyElixir.{CandidateGit, CandidateTransfer, Config, Evidence, PathSafety, Validation, ValidationGithub, WorkstreamRun}

  @name "factory/validation"

  @spec validate_config(term()) :: {:ok, map()} | {:error, atom()}
  def validate_config(config) do
    case SymphonyElixir.Publisher.validate_config(config) do
      {:ok, value} -> {:ok, value}
      _ -> {:error, :invalid_or_missing_validation_check_config}
    end
  end

  @doc "Pins only separate service credential references; nothing is passed to candidate code."
  @spec pin(keyword(), Path.t(), map()) :: {:ok, map()} | {:error, term()}
  def pin(opts, workspace, definition) do
    if stage(definition) do
      config = Keyword.get_lazy(opts, :validation_check, &Config.validation_check/0)

      config =
        case config do
          {:ok, value} -> value
          value -> value
        end

      with {:ok, config} <- validate_config(config),
           {:ok, path} <- PathSafety.canonicalize(config.private_key_path),
           true <- path == config.private_key_path and not overlap?(workspace, path),
           :ok <- separate_credentials(config, opts[:publisher] || definition[:pinned_publisher]) do
        context = %{validation_check: config}
        context = if opts[:validation_pr], do: Map.put(context, :validation_pr, opts[:validation_pr]), else: context
        {:ok, context}
      else
        false -> {:error, :validation_key_overlaps_candidate_or_changes_path}
        {:error, _} = error -> error
        _ -> {:error, :validation_check_not_configured}
      end
    else
      {:ok, %{}}
    end
  end

  @spec stage(map()) :: map() | nil
  def stage(definition) do
    Enum.find_value(definition.stages, fn {_id, stage} -> if get_in(stage, [:gate, :evaluator]) == :github_validation, do: stage end)
  end

  @spec same_observation?(term(), term()) :: boolean()
  def same_observation?(left, right) when is_map(left) and is_map(right), do: Map.drop(left, [:attestation, :observed_at]) == Map.drop(right, [:attestation, :observed_at])
  def same_observation?(_left, _right), do: false

  @spec monitor?(map()) :: boolean()
  def monitor?(run), do: run.status in [:waiting_for_answer, :blocked] and not is_nil(stage(run.definition)) and is_integer(pr_number(run))

  @spec protects_wait?(map()) :: boolean()
  def protects_wait?(run), do: not is_nil(stage(run.definition)) and is_integer(pr_number(run)) and run.definition.stages[run.stage_id].type == :human_wait

  @doc "Reconciles one PR/check. Retries first read the remote check identity, so a lost response is reusable."
  @spec reconcile(map(), map(), keyword()) :: {:ok, map()} | {:error, term()} | {:uncertain, term()}
  def reconcile(run, stage, opts \\ []) do
    :global.trans({{__MODULE__, pr_number(run)}, self()}, fn -> reconcile_locked(run, stage, opts) end)
  rescue
    _ -> {:error, :validation_check_unavailable}
  catch
    _, _ -> {:error, :validation_check_unavailable}
  end

  @doc "Freshly verifies evidence and the expected App's current check without writing anything."
  @spec verify_ready(map(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def verify_ready(run, stage, opts \\ []) do
    with {:ok, config} <- configuration(run),
         {:ok, remote} <- remote(run, config, opts),
         {:ok, proof} <- proof(run, stage, remote, opts),
         id = contract_id(run, stage, remote),
         :ok <- read_owner(opts),
         {:ok, checks} <- transport(run, opts).checks(config, remote.head_sha),
         :ok <- read_owner(opts),
         [check] <- Enum.filter(checks, &source_check?(&1, config, remote.head_sha)),
         true <- matching_check?(check, config, remote.head_sha, id),
         true <- check.status == "completed" and check.conclusion == "success",
         :ok <- unchanged_proof(run, stage, remote, {:ok, proof}, opts),
         {:ok, ^remote} <- remote(run, config, opts) do
      result = decision(remote, id, check.id, true, proof.archive.revision, nil) |> Map.put(:observed_at, System.system_time(:second))
      with {:ok, key} <- attestation_key(run), do: {:ok, Map.put(result, :attestation, attest(result, key))}
    else
      {:error, _} = error -> error
      _ -> {:error, :current_validation_check_not_passing}
    end
  rescue
    _ -> {:error, :current_validation_check_not_passing}
  end

  @spec verify_completion(map(), map(), term()) :: :ok | {:error, term()}
  def verify_completion(run, stage, {:ok, result}) do
    with {:ok, actual} <- verify_ready(run, stage), true <- same_observation?(actual, result) do
      :ok
    else
      _ -> {:error, :validation_check_completion_not_verified}
    end
  end

  def verify_completion(_run, _stage, _result), do: {:error, :validation_check_completion_not_verified}

  @doc "Authenticates the fresh observation from the coordinator's separately owned verification task without network I/O."
  @spec verify_attested_completion(map(), map(), term()) :: :ok | {:error, term()}
  def verify_attested_completion(run, stage, {:ok, result}) when is_map(result) do
    remote = Map.take(result, [:number, :head_sha, :base_sha, :head_branch, :base_branch, :url])

    with true <- result[:qualified] == true,
         observed when is_integer(observed) <- result[:observed_at],
         true <- observed <= System.system_time(:second) and System.system_time(:second) - observed <= 30,
         {:ok, _} <- configuration(run),
         {:ok, proof} <- proof(run, stage, remote, []),
         true <- result[:external_id] == contract_id(run, stage, remote),
         true <- result[:evidence_revision] == proof.archive.revision,
         {:ok, key} <- attestation_key(run),
         true <- result[:attestation] == attest(Map.delete(result, :attestation), key) do
      :ok
    else
      _ -> {:error, :validation_check_completion_not_verified}
    end
  rescue
    _ -> {:error, :validation_check_completion_not_verified}
  end

  def verify_attested_completion(_run, _stage, _result), do: {:error, :validation_check_completion_not_verified}

  defp reconcile_locked(run, stage, opts) do
    with {:ok, config} <- configuration(run),
         :ok <- owner(run, opts),
         {:ok, snapshot} <- remote(run, config, opts),
         qualification = proof(run, stage, snapshot, opts),
         id = contract_id(run, stage, snapshot),
         payload = payload(snapshot, id, qualification),
         {:ok, checks} <- transport(run, opts).checks(config, snapshot.head_sha),
         :ok <- owner(run, opts),
         {:ok, ^snapshot} <- remote(run, config, opts),
         :ok <- unchanged_proof(run, stage, snapshot, qualification, opts),
         {:ok, check} <- upsert(run, config, snapshot, id, checks, payload, opts),
         :ok <- confirm_write(run, stage, config, snapshot, qualification, check, payload, opts) do
      qualified = match?({:ok, _}, qualification)

      revision =
        case qualification do
          {:ok, proof} -> proof.archive.revision
          _ -> nil
        end

      reason =
        case qualification do
          {:error, reason} -> inspect(reason)
          _ -> nil
        end

      result = decision(snapshot, id, check.id, qualified, revision, reason) |> Map.put(:observed_at, System.system_time(:second))

      if qualified do
        with {:ok, key} <- attestation_key(run), do: {:ok, Map.put(result, :attestation, attest(result, key))}
      else
        {:ok, result}
      end
    else
      {:error, {:github_request_failed, _}} -> {:uncertain, :validation_check_outcome_unknown}
      {:error, _} = error -> error
      _ -> {:error, :remote_validation_identity_changed}
    end
  end

  defp confirm_write(run, stage, config, snapshot, qualification, check, payload, opts) do
    with :ok <- owner(run, opts) do
      case {remote(run, config, opts), unchanged_proof(run, stage, snapshot, qualification, opts)} do
        {{:ok, ^snapshot}, :ok} ->
          :ok

        _ ->
          failure = %{
            payload
            | conclusion: "failure",
              output: %{title: "Validation changed", summary: "Remote identity or trusted evidence changed during this check write; validation is required again."}
          }

          case transport(run, opts).update_check(config, check.id, failure) do
            {:ok, _} -> {:error, :remote_validation_identity_changed}
            _ -> {:error, {:github_request_failed, :validation_demotion_unknown}}
          end
      end
    end
  end

  # The service-only producer key seals a final remote observation. Domain separation
  # prevents a signed validation manifest from being replayed as a check observation.
  # Completion performs only local authentication inside the coordinator.
  defp attestation_key(run) do
    case File.read(Path.join(run.execution[:validation_archive], "producer.key")) do
      {:ok, key} when byte_size(key) == 32 -> {:ok, key}
      _ -> {:error, :validation_attestation_unavailable}
    end
  end

  defp attest(result, key), do: :crypto.mac(:hmac, :sha256, key, "github-validation-observation-v1\n" <> :erlang.term_to_binary(result)) |> Base.encode16(case: :lower)

  defp configuration(run) do
    with true <- run.status not in [:stopped, :policy_blocked] and WorkstreamRun.compatible_policy?(run),
         {:ok, config} <- validate_config(run.execution[:validation_check]),
         :ok <- separate_credentials(config, run.execution[:publisher]) do
      {:ok, config}
    else
      {:error, _} = error -> error
      _ -> {:error, :validation_run_not_current}
    end
  end

  defp separate_credentials(_config, nil), do: :ok
  defp separate_credentials(config, {:ok, publisher}), do: separate_credentials(config, publisher)

  defp separate_credentials(config, publisher) do
    with {:ok, publisher} <- SymphonyElixir.Publisher.validate_config(publisher),
         true <- to_string(config.app_id) != to_string(publisher.app_id),
         true <- to_string(config.installation_id) != to_string(publisher.installation_id),
         {:ok, key} <- PathSafety.canonicalize(config.private_key_path),
         {:ok, other} <- PathSafety.canonicalize(publisher.private_key_path),
         true <- key != other do
      :ok
    else
      _ -> {:error, :validation_credentials_not_separate}
    end
  end

  defp remote(run, config, opts) do
    with :ok <- read_owner(opts),
         number when is_integer(number) and number > 0 <- pr_number(run),
         {:ok, pr} <- transport(run, opts).get_pr(config, number),
         true <- pr.number == number and pr.state == "open" and pr.base_branch == "main" and not pr.draft,
         true <- pr.head_repository_full_name == "RigCodeAI/rig" and pr.base_repository_full_name == "RigCodeAI/rig",
         true <- pr.url == "https://github.com/RigCodeAI/rig/pull/#{number}",
         :ok <- read_owner(opts),
         {:ok, base} <- transport(run, opts).base_sha(config),
         :ok <- read_owner(opts),
         true <- pr.base_sha == base do
      {:ok, Map.take(pr, [:number, :head_sha, :base_sha, :head_branch, :base_branch, :url])}
    else
      {:error, _} = error -> error
      _ -> {:error, :unsupported_or_changed_validation_pr}
    end
  end

  defp pr_number(run) do
    run.execution[:validation_pr] || get_in(run, [:outputs, "publication", "pr", "number"]) || get_in(run, [:outputs, "publication", :pr, :number])
  end

  defp proof(run, stage, remote, opts) do
    with true <- run.definition.stages[stage.id] == stage and stage.gate[:evaluator] == :github_validation,
         validation when is_map(validation) <- run.definition.stages[stage.gate.validation_stage],
         true <- validation.gate[:evaluator] == :candidate_validation,
         attempt when is_map(attempt) <- run.attempts |> Enum.reverse() |> Enum.find(&(&1.stage == validation.id)),
         true <- attempt.status == :completed and attempt.gate == :passed,
         %{"id" => _, "manifest_sha256" => _} = receipt <- get_in(attempt, [:result, :evidence, "receipt"]),
         {:ok, workspace} <- CandidateTransfer.workspace(run),
         context = %{task_id: run.task_id, run_id: run.id, attempt_id: attempt.id, base_sha: run.execution[:validation_base]},
         options = [workspace: workspace, validation_archive: run.execution[:validation_archive], validation_scratch: run.execution[:validation_scratch]],
         {:ok, %{verdict: :passed}} <- Validation.verify_result({:ok, %{receipt: receipt}}, run.execution[:validation_policy], context, validation.gate.required, options),
         {:ok, %{verdict: :passed}} <- Validation.verify_result({:ok, %{receipt: receipt}}, run.execution[:validation_policy], context, stage.gate.required, options),
         {:ok, manifest} <- Validation.read_receipt(receipt, run.execution[:validation_archive]),
         true <- manifest["candidate_sha"] == remote.head_sha and manifest["base_sha"] == remote.base_sha,
         {:ok, _} <- CandidateGit.run(workspace, ["merge-base", "--is-ancestor", remote.base_sha, remote.head_sha]),
         {:ok, archive} <- Evidence.publication(run, attempt.id, receipt, opts[:evidence_server] || run.execution[:evidence_server] || Evidence) do
      url = Evidence.url(run) || run.execution[:evidence_url]

      if is_binary(url) and String.starts_with?(url, "https://") do
        {:ok, %{manifest: manifest, receipt: receipt, archive: archive, url: url <> "/artifacts/" <> archive.revision <> "/" <> archive.validation_manifest}}
      else
        {:error, :permanent_evidence_url_unavailable}
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :missing_failed_or_stale_validation}
    end
  rescue
    _ -> {:error, :invalid_validation_contract}
  end

  defp unchanged_proof(run, stage, remote, expected, opts) do
    if proof(run, stage, remote, opts) == expected, do: :ok, else: {:error, :validation_evidence_changed}
  end

  defp contract_id(run, stage, remote) do
    digest(:erlang.term_to_binary({run.id, run.definition.definitions, run.policy, stage.gate, run.execution[:validation_policy], run.execution[:validation_check], remote}))
  end

  defp payload(remote, id, qualification) do
    {conclusion, summary, url} =
      case qualification do
        {:ok, proof} ->
          {"success",
           "All required executable assertions passed for #{remote.head_sha}. Policy #{proof.manifest["policy_revision"]}; receipt #{proof.receipt["manifest_sha256"]}; archived revision #{proof.archive.revision}.",
           proof.url}

        {:error, reason} ->
          {"failure", "Validation is blocked for #{remote.head_sha}: #{inspect(reason)}. A review approval or quality score cannot replace required executable evidence.", nil}
      end

    payload = %{name: @name, head_sha: remote.head_sha, external_id: id, status: "completed", conclusion: conclusion, output: %{title: "Trusted validation #{conclusion}", summary: summary}}
    if is_binary(url), do: Map.put(payload, :details_url, url), else: payload
  end

  defp upsert(run, config, remote, id, checks, payload, opts) do
    existing = Enum.filter(checks, &source_check?(&1, config, remote.head_sha))

    case existing do
      [] ->
        transport(run, opts).create_check(config, payload)

      [check] ->
        if check.external_id == id and check.status == payload.status and check.conclusion == payload.conclusion do
          {:ok, check}
        else
          transport(run, opts).update_check(config, check.id, payload)
        end

      _ ->
        {:error, :ambiguous_validation_check_identity}
    end
  end

  defp source_check?(check, config, sha), do: check.name == @name and to_string(check.app_id) == to_string(config.app_id) and check.head_sha == sha

  defp matching_check?(check, config, sha, id), do: check.name == @name and to_string(check.app_id) == to_string(config.app_id) and check.head_sha == sha and check.external_id == id

  defp decision(remote, id, check_id, qualified, revision, reason),
    do: Map.merge(remote, %{external_id: id, check_id: check_id, qualified: qualified, evidence_revision: revision, reason: reason})

  defp read_owner(opts) do
    case opts[:on_validation_check] do
      callback when is_function(callback, 0) -> callback.()
      nil -> :ok
      _ -> {:error, :invalid_validation_owner_callback}
    end
  end

  defp owner(run, opts) do
    case opts[:on_validation_check] || opts[:on_publication_check] do
      callback when is_function(callback, 0) -> callback.()
      nil -> if transport(run, opts) == ValidationGithub, do: {:error, :validation_requires_live_coordinator}, else: :ok
      _ -> {:error, :invalid_validation_owner_callback}
    end
  end

  defp transport(run, opts), do: opts[:validation_check_transport] || run.execution[:validation_check_transport] || ValidationGithub
  defp overlap?(root, path), do: path == root or String.starts_with?(path, root <> "/")
  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
