defmodule SymphonyElixir.ValidationGithub do
  @moduledoc """
  Trusted GitHub App transport for the `factory/validation` check on RigCodeAI/rig.

  The App token is restricted to that repository and to the permissions needed to
  read the candidate and publish check runs. Tokens are discarded after each call.
  """

  @api_url "https://api.github.com"
  @repo "RigCodeAI/rig"
  @repo_name "rig"
  @check_name "factory/validation"
  @api_version "2022-11-28"
  @user_agent "symphony-validation"
  @request_timeout_ms 10_000
  @connect_timeout_ms 5_000
  @token_ttl_max_seconds 4_200
  @max_check_run_pages 10
  @per_page 100
  @required_permissions %{"checks" => "write", "contents" => "read", "pull_requests" => "read"}
  @requested_permissions Map.put(@required_permissions, "metadata", "read")
  @check_payload_keys ~w(name head_sha external_id status conclusion details_url output)
  @check_statuses ["queued", "in_progress", "completed"]
  @check_conclusions [
    "success",
    "failure",
    "neutral",
    "cancelled",
    "timed_out",
    "action_required",
    "stale",
    "skipped"
  ]

  @type config :: %{app_id: pos_integer() | String.t(), installation_id: pos_integer() | String.t(), private_key_path: Path.t()}
  @type pull_request :: %{
          number: pos_integer(),
          url: String.t(),
          head_repository_full_name: String.t(),
          base_repository_full_name: String.t(),
          head_branch: String.t(),
          base_branch: String.t(),
          head_sha: String.t(),
          base_sha: String.t(),
          state: String.t(),
          draft: boolean()
        }
  @type check_run :: %{
          id: pos_integer(),
          app_id: pos_integer(),
          name: String.t(),
          head_sha: String.t(),
          external_id: String.t() | nil,
          status: String.t(),
          conclusion: String.t() | nil,
          details_url: String.t() | nil,
          output: map() | nil
        }
  @type check_payload :: map()

  @spec validate_config(config()) :: :ok | {:error, term()}
  def validate_config(config), do: validate_config_with(config, &perform_request/5)

  @spec get_pr(config(), pos_integer()) :: {:ok, pull_request()} | {:error, term()}
  def get_pr(config, number), do: get_pr_with(config, number, &perform_request/5)

  @spec base_sha(config()) :: {:ok, String.t()} | {:error, term()}
  def base_sha(config), do: base_sha_with(config, &perform_request/5)

  @spec checks(config(), String.t()) :: {:ok, [check_run()]} | {:error, term()}
  def checks(config, sha), do: checks_with(config, sha, &perform_request/5)

  @spec create_check(config(), check_payload()) :: {:ok, check_run()} | {:error, term()}
  def create_check(config, payload), do: create_check_with(config, payload, &perform_request/5)

  @spec update_check(config(), pos_integer(), check_payload()) :: {:ok, check_run()} | {:error, term()}
  def update_check(config, id, payload), do: update_check_with(config, id, payload, &perform_request/5)

  @doc false
  @spec validate_config_for_test(config(), function()) :: :ok | {:error, term()}
  def validate_config_for_test(config, request_fun) when is_function(request_fun, 5),
    do: validate_config_with(config, request_fun)

  @doc false
  @spec get_pr_for_test(config(), pos_integer(), function()) :: {:ok, pull_request()} | {:error, term()}
  def get_pr_for_test(config, number, request_fun) when is_function(request_fun, 5),
    do: get_pr_with(config, number, request_fun)

  @doc false
  @spec base_sha_for_test(config(), function()) :: {:ok, String.t()} | {:error, term()}
  def base_sha_for_test(config, request_fun) when is_function(request_fun, 5),
    do: base_sha_with(config, request_fun)

  @doc false
  @spec checks_for_test(config(), String.t(), function()) :: {:ok, [check_run()]} | {:error, term()}
  def checks_for_test(config, sha, request_fun) when is_function(request_fun, 5),
    do: checks_with(config, sha, request_fun)

  @doc false
  @spec create_check_for_test(config(), check_payload(), function()) :: {:ok, check_run()} | {:error, term()}
  def create_check_for_test(config, payload, request_fun) when is_function(request_fun, 5),
    do: create_check_with(config, payload, request_fun)

  @doc false
  @spec update_check_for_test(config(), pos_integer(), check_payload(), function()) ::
          {:ok, check_run()} | {:error, term()}
  def update_check_for_test(config, id, payload, request_fun) when is_function(request_fun, 5),
    do: update_check_with(config, id, payload, request_fun)

  defp validate_config_with(config, request_fun) do
    case installation_token(config, request_fun) do
      {:ok, _token, _normalized} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp get_pr_with(config, number, request_fun) do
    with :ok <- validate_pull_request_number(number),
         {:ok, token, _normalized} <- installation_token(config, request_fun),
         {:ok, payload} <- api_request(request_fun, :get, "/repos/#{@repo}/pulls/#{number}", token, %{}, nil),
         {:ok, pull_request} <- normalize_pull_request(payload),
         true <- pull_request.number == number or {:error, :pull_request_number_mismatch} do
      {:ok, pull_request}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_response}
    end
  end

  defp base_sha_with(config, request_fun) do
    with {:ok, token, _normalized} <- installation_token(config, request_fun),
         {:ok, payload} <- api_request(request_fun, :get, "/repos/#{@repo}/git/ref/heads/main", token, %{}, nil),
         %{"object" => %{"sha" => sha, "type" => "commit"}} <- payload,
         true <- valid_sha?(sha) or {:error, :invalid_base_ref_response} do
      {:ok, sha}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_base_ref_response}
    end
  end

  defp checks_with(config, sha, request_fun) do
    with true <- valid_sha?(sha) or {:error, :invalid_check_run_sha},
         {:ok, token, normalized} <- installation_token(config, request_fun),
         {:ok, check_runs} <- list_check_run_pages(token, normalized, sha, 1, nil, [], MapSet.new(), request_fun) do
      {:ok, check_runs}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_check_runs_response}
    end
  end

  defp list_check_run_pages(_token, _normalized, _sha, page, total, acc, _ids, _request_fun)
       when page > @max_check_run_pages do
    if is_integer(total) and length(acc) == total,
      do: {:ok, acc},
      else: {:error, :check_run_results_exceeded}
  end

  defp list_check_run_pages(token, normalized, sha, page, expected_total, acc, ids, request_fun) do
    params = %{
      "app_id" => String.to_integer(normalized.app_id),
      "filter" => "all",
      "per_page" => @per_page,
      "page" => page
    }

    with {:ok, payload} <-
           api_request(
             request_fun,
             :get,
             "/repos/#{@repo}/commits/#{sha}/check-runs",
             token,
             params,
             nil
           ),
         %{"total_count" => total, "check_runs" => raw_check_runs} <- payload,
         true <- (is_integer(total) and total >= 0) or {:error, :invalid_check_runs_response},
         true <- is_nil(expected_total) or expected_total == total or {:error, :check_run_total_changed},
         true <- total <= @max_check_run_pages * @per_page or {:error, :check_run_results_exceeded},
         true <- (is_list(raw_check_runs) and length(raw_check_runs) <= @per_page) or {:error, :invalid_check_runs_response},
         {:ok, page_runs, page_ids} <- normalize_check_runs(raw_check_runs, normalized.app_id, sha, ids),
         next_acc <- acc ++ page_runs,
         next_ids <- MapSet.union(ids, page_ids) do
      cond do
        length(next_acc) == total -> {:ok, next_acc}
        length(next_acc) > total -> {:error, :invalid_check_runs_response}
        page >= @max_check_run_pages -> {:error, :check_run_results_exceeded}
        length(raw_check_runs) < @per_page -> {:error, :incomplete_check_run_results}
        true -> list_check_run_pages(token, normalized, sha, page + 1, total, next_acc, next_ids, request_fun)
      end
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_check_runs_response}
    end
  end

  defp normalize_check_runs(raw_check_runs, expected_app_id, expected_sha, known_ids) do
    expected_app_id = String.to_integer(expected_app_id)

    Enum.reduce_while(raw_check_runs, {:ok, [], known_ids}, fn raw, {:ok, acc, ids} ->
      case normalize_check_run(raw) do
        {:ok, check_run} ->
          cond do
            check_run.app_id != expected_app_id ->
              {:halt, {:error, :check_run_app_mismatch}}

            check_run.head_sha != expected_sha ->
              {:halt, {:error, :check_run_sha_mismatch}}

            MapSet.member?(ids, check_run.id) ->
              {:halt, {:error, :duplicate_check_run}}

            true ->
              {:cont, {:ok, [check_run | acc], MapSet.put(ids, check_run.id)}}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, check_runs, ids} -> {:ok, Enum.reverse(check_runs), MapSet.difference(ids, known_ids)}
      {:error, _reason} = error -> error
    end
  end

  defp create_check_with(config, payload, request_fun) do
    with {:ok, normalized_payload} <- normalize_check_payload(payload, :create),
         {:ok, token, normalized} <- installation_token(config, request_fun),
         {:ok, response} <-
           api_request(
             request_fun,
             :post,
             "/repos/#{@repo}/check-runs",
             token,
             %{},
             normalized_payload
           ),
         {:ok, check_run} <- normalize_check_run(response),
         :ok <- verify_check_response(check_run, normalized, normalized_payload) do
      {:ok, check_run}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_check_run_response}
    end
  end

  defp update_check_with(config, id, payload, request_fun) do
    with :ok <- validate_check_run_id(id),
         {:ok, normalized_payload} <- normalize_check_payload(payload, :update),
         {:ok, token, normalized} <- installation_token(config, request_fun),
         {:ok, current_payload} <-
           api_request(request_fun, :get, "/repos/#{@repo}/check-runs/#{id}", token, %{}, nil),
         {:ok, current} <- normalize_check_run(current_payload),
         true <- current.id == id or {:error, :check_run_id_mismatch},
         :ok <- verify_existing_check_identity(current, normalized, normalized_payload),
         patch_payload <- Map.delete(normalized_payload, "head_sha"),
         {:ok, response} <-
           api_request(
             request_fun,
             :patch,
             "/repos/#{@repo}/check-runs/#{id}",
             token,
             %{},
             patch_payload
           ),
         {:ok, updated} <- normalize_check_run(response),
         true <- updated.id == id or {:error, :check_run_id_mismatch},
         :ok <- verify_check_response(updated, normalized, normalized_payload) do
      {:ok, updated}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_check_run_response}
    end
  end

  defp verify_check_identity(check_run, normalized, payload) do
    if check_run.app_id == String.to_integer(normalized.app_id) and
         check_run.name == payload["name"] and
         check_run.head_sha == payload["head_sha"] and
         check_run.external_id == payload["external_id"] do
      :ok
    else
      {:error, :check_run_identity_mismatch}
    end
  end

  defp verify_existing_check_identity(check_run, normalized, payload) do
    if check_run.app_id == String.to_integer(normalized.app_id) and
         check_run.name == payload["name"] and
         check_run.head_sha == payload["head_sha"] do
      :ok
    else
      {:error, :check_run_identity_mismatch}
    end
  end

  defp verify_check_response(check_run, normalized, payload) do
    with :ok <- verify_check_identity(check_run, normalized, payload),
         true <- check_run.status == payload["status"] or {:error, :check_run_response_mismatch},
         true <- check_run.conclusion == Map.get(payload, "conclusion") or {:error, :check_run_response_mismatch},
         true <-
           not Map.has_key?(payload, "details_url") or check_run.details_url == payload["details_url"] or
             {:error, :check_run_response_mismatch},
         true <- output_matches?(check_run.output, payload) or {:error, :check_run_response_mismatch} do
      :ok
    else
      {:error, _reason} = error -> error
      _ -> {:error, :check_run_response_mismatch}
    end
  end

  defp output_matches?(output, payload) do
    if Map.has_key?(payload, "output") do
      case payload["output"] do
        nil ->
          is_nil(output)

        expected when is_map(expected) ->
          actual = output || %{}
          Enum.all?(expected, fn {key, value} -> Map.get(actual, key) == value end)

        _ ->
          false
      end
    else
      true
    end
  end

  defp normalize_check_payload(payload, operation) when is_map(payload) do
    with {:ok, normalized} <- normalize_payload_keys(payload),
         true <- Map.keys(normalized) -- @check_payload_keys == [] or {:error, :invalid_check_payload},
         @check_name <- normalized["name"],
         sha when is_binary(sha) <- normalized["head_sha"],
         true <- valid_sha?(sha) or {:error, :invalid_check_payload},
         external_id when is_binary(external_id) <- normalized["external_id"],
         true <- valid_external_id?(external_id) or {:error, :invalid_check_payload},
         status when is_binary(status) <- normalized["status"],
         true <- status in @check_statuses or {:error, :invalid_check_payload},
         :ok <- validate_payload_conclusion(operation, status, Map.get(normalized, "conclusion")),
         :ok <- validate_details_url(Map.get(normalized, "details_url")),
         {:ok, output} <- normalize_output_payload(Map.get(normalized, "output")) do
      normalized = if Map.has_key?(normalized, "output"), do: Map.put(normalized, "output", output), else: normalized
      normalized = if is_nil(Map.get(normalized, "conclusion")), do: Map.delete(normalized, "conclusion"), else: normalized
      {:ok, normalized}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_check_payload}
    end
  end

  defp normalize_check_payload(_payload, _operation), do: {:error, :invalid_check_payload}

  defp normalize_payload_keys(payload) do
    Enum.reduce_while(payload, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      key = if is_atom(key), do: Atom.to_string(key), else: key

      if key in @check_payload_keys and not Map.has_key?(acc, key) do
        {:cont, {:ok, Map.put(acc, key, value)}}
      else
        {:halt, {:error, :invalid_check_payload}}
      end
    end)
  end

  defp validate_payload_conclusion(:create, "in_progress", nil), do: :ok
  defp validate_payload_conclusion(:create, "completed", conclusion) when conclusion in ["success", "failure"], do: :ok
  defp validate_payload_conclusion(:update, "completed", conclusion) when conclusion in ["success", "failure"], do: :ok
  defp validate_payload_conclusion(_operation, _status, _conclusion), do: {:error, :invalid_check_payload}

  defp validate_details_url(nil), do: :ok

  defp validate_details_url(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: "https", host: host, userinfo: nil} when is_binary(host) and host != "" -> :ok
      _ -> {:error, :invalid_check_payload}
    end
  rescue
    _error -> {:error, :invalid_check_payload}
  end

  defp validate_details_url(_url), do: {:error, :invalid_check_payload}

  defp validate_output(nil), do: :ok

  defp validate_output(output) when is_map(output) do
    allowed = ["title", "summary", "text", :title, :summary, :text]
    keys = Map.keys(output)
    title = Map.get(output, "title", Map.get(output, :title))
    summary = Map.get(output, "summary", Map.get(output, :summary))

    text = Map.get(output, "text", Map.get(output, :text))

    if Enum.all?(keys, &(&1 in allowed)) and is_binary(title) and is_binary(summary) and
         (is_nil(text) or is_binary(text)) do
      :ok
    else
      {:error, :invalid_check_payload}
    end
  end

  defp validate_output(_output), do: {:error, :invalid_check_payload}

  defp normalize_output_payload(nil), do: {:ok, nil}

  defp normalize_output_payload(output) when is_map(output) do
    canonical_keys = Enum.map(Map.keys(output), fn key -> if is_atom(key), do: Atom.to_string(key), else: key end)
    normalized = Map.new(output, fn {key, value} -> {if(is_atom(key), do: Atom.to_string(key), else: key), value} end)

    if length(Enum.uniq(canonical_keys)) == length(canonical_keys) and validate_output(normalized) == :ok,
      do: {:ok, normalized},
      else: {:error, :invalid_check_payload}
  end

  defp normalize_output_payload(_output), do: {:error, :invalid_check_payload}

  defp installation_token(config, request_fun) do
    with {:ok, normalized} <- normalize_config(config),
         {:ok, key} <- read_private_key(normalized.private_key_path),
         {:ok, jwt} <- app_jwt(normalized.app_id, key),
         {:ok, installation} <-
           api_request(
             request_fun,
             :get,
             "/app/installations/#{normalized.installation_id}",
             jwt,
             %{},
             nil
           ),
         :ok <- validate_installation(installation, normalized),
         {:ok, token_response} <-
           api_request(
             request_fun,
             :post,
             "/app/installations/#{normalized.installation_id}/access_tokens",
             jwt,
             %{},
             %{"repositories" => [@repo_name], "permissions" => @requested_permissions}
           ),
         {:ok, token} <- validate_installation_token_response(token_response) do
      {:ok, token, normalized}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :github_app_authentication_failed}
    end
  rescue
    _error -> {:error, :github_app_authentication_failed}
  catch
    _kind, _reason -> {:error, :github_app_authentication_failed}
  end

  defp validate_installation(%{"id" => id, "app_id" => app_id}, normalized) do
    with {:ok, id} <- normalize_identifier(id),
         {:ok, app_id} <- normalize_identifier(app_id),
         true <- id == normalized.installation_id or {:error, :github_installation_id_mismatch},
         true <- app_id == normalized.app_id or {:error, :github_installation_app_mismatch} do
      :ok
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_github_installation_response}
    end
  end

  defp validate_installation(_installation, _normalized), do: {:error, :invalid_github_installation_response}

  defp validate_installation_token_response(%{"token" => token, "expires_at" => expires_at} = payload)
       when is_binary(token) and is_binary(expires_at) do
    now = DateTime.utc_now() |> DateTime.to_unix()

    with {:ok, expiry} <- parse_expiry(expires_at),
         true <- expiry > now or {:error, :github_installation_token_expired},
         true <- expiry <= now + @token_ttl_max_seconds or {:error, :github_installation_token_ttl_invalid},
         true <- valid_token?(token) or {:error, :github_installation_token_invalid},
         true <- permissions_satisfy?(payload["permissions"]) or {:error, :github_installation_permissions_invalid},
         true <- payload["repository_selection"] == "selected" or {:error, :github_installation_repository_scope_invalid},
         true <- selected_repository?(payload["repositories"]) or {:error, :github_installation_repository_scope_invalid} do
      {:ok, token}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :github_app_authentication_failed}
    end
  end

  defp validate_installation_token_response(_payload), do: {:error, :github_app_authentication_failed}

  defp selected_repository?([%{"full_name" => @repo}]), do: true
  defp selected_repository?(_repositories), do: false

  defp permissions_satisfy?(permissions) when is_map(permissions) do
    Enum.all?(@required_permissions, fn {name, level} -> permissions[name] == level end) and
      Enum.all?(permissions, fn {name, level} ->
        cond do
          name == "checks" -> level == "write"
          name in ["contents", "pull_requests", "metadata"] -> level == "read"
          name in ["status", "statuses"] -> false
          level in ["write", "admin"] -> false
          level == "read" -> true
          true -> false
        end
      end)
  end

  defp permissions_satisfy?(_permissions), do: false

  defp parse_expiry(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, DateTime.to_unix(datetime)}
      _ -> {:error, :invalid_github_installation_expiry}
    end
  end

  defp api_request(request_fun, method, path, token, params, body) do
    case request_fun.(method, path, github_headers(token), params, body) do
      {:ok, %{status: status, body: response_body}} when status in 200..299 ->
        {:ok, response_body}

      {:ok, %{status: status}} when is_integer(status) and status >= 500 ->
        {:error, {:github_request_failed, {:http_status, status}}}

      {:ok, %{status: status}} when is_integer(status) ->
        {:error, {:github_api_status, status}}

      {:error, _reason} ->
        {:error, {:github_request_failed, :transport}}

      _ ->
        {:error, {:github_request_failed, :invalid_response}}
    end
  rescue
    _error -> {:error, {:github_request_failed, :transport}}
  catch
    _kind, _reason -> {:error, {:github_request_failed, :transport}}
  end

  defp perform_request(method, path, headers, params, body) do
    options = [
      method: method,
      url: @api_url <> path,
      headers: headers,
      params: params,
      retry: false,
      redirect: false,
      receive_timeout: @request_timeout_ms,
      connect_options: [timeout: @connect_timeout_ms]
    ]

    options = if is_nil(body), do: options, else: Keyword.put(options, :json, body)

    case Req.request(options) do
      {:ok, response} -> {:ok, %{status: response.status, body: response.body}}
      {:error, _reason} -> {:error, :github_api_request_failed}
    end
  rescue
    _error -> {:error, :github_api_request_failed}
  end

  defp github_headers(token) do
    [
      {"Accept", "application/vnd.github+json"},
      {"Authorization", "Bearer " <> token},
      {"X-GitHub-Api-Version", @api_version},
      {"User-Agent", @user_agent}
    ]
  end

  defp normalize_config(config) when is_map(config) do
    app_id = Map.get(config, :app_id, Map.get(config, "app_id"))
    installation_id = Map.get(config, :installation_id, Map.get(config, "installation_id"))
    private_key_path = Map.get(config, :private_key_path, Map.get(config, "private_key_path"))
    allowed_keys = [:app_id, :installation_id, :private_key_path, "app_id", "installation_id", "private_key_path"]

    cond do
      Enum.any?(Map.keys(config), &(&1 not in allowed_keys)) ->
        {:error, :invalid_github_app_config}

      is_nil(app_id) or is_nil(installation_id) ->
        {:error, :invalid_github_app_config}

      true ->
        with {:ok, app_id} <- normalize_identifier(app_id),
             {:ok, installation_id} <- normalize_identifier(installation_id),
             {:ok, private_key_path} <- normalize_key_path(private_key_path) do
          {:ok, %{app_id: app_id, installation_id: installation_id, private_key_path: private_key_path}}
        end
    end
  end

  defp normalize_config(_config), do: {:error, :invalid_github_app_config}

  defp normalize_identifier(value) when is_integer(value) and value > 0, do: {:ok, Integer.to_string(value)}

  defp normalize_identifier(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer > 0 -> {:ok, Integer.to_string(integer)}
      _ -> {:error, :invalid_github_app_config}
    end
  end

  defp normalize_identifier(_value), do: {:error, :invalid_github_app_config}

  defp normalize_key_path(path) when is_binary(path) do
    if Path.type(path) == :absolute and String.trim(path) == path and path != "" do
      {:ok, Path.expand(path)}
    else
      {:error, :invalid_github_app_config}
    end
  end

  defp normalize_key_path(_path), do: {:error, :invalid_github_app_config}

  defp read_private_key(path) do
    with {:ok, %File.Stat{type: :regular, mode: mode, uid: file_uid}} <- File.lstat(path),
         {:ok, process_uid} <- effective_uid(),
         true <- file_uid == process_uid or {:error, :github_private_key_owner_invalid},
         true <- private_key_mode?(mode) or {:error, :github_private_key_mode_invalid},
         {:ok, pem} <- File.read(path),
         [entry] <- :public_key.pem_decode(pem),
         key <- :public_key.pem_entry_decode(entry),
         true <-
           (is_tuple(key) and tuple_size(key) > 0 and elem(key, 0) == :RSAPrivateKey) or
             {:error, :github_private_key_invalid} do
      {:ok, key}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :github_private_key_invalid}
    end
  rescue
    _error -> {:error, :github_private_key_invalid}
  catch
    _kind, _reason -> {:error, :github_private_key_invalid}
  end

  defp private_key_mode?(mode) do
    permissions = Bitwise.band(mode, 0o777)
    permissions in [0o400, 0o600]
  end

  defp effective_uid do
    case System.cmd("/usr/bin/id", ["-u"], stderr_to_stdout: true) do
      {uid, 0} ->
        case Integer.parse(String.trim(uid)) do
          {parsed, ""} -> {:ok, parsed}
          _ -> {:error, :github_process_owner_unavailable}
        end

      _ ->
        {:error, :github_process_owner_unavailable}
    end
  rescue
    _error -> {:error, :github_process_owner_unavailable}
  end

  defp app_jwt(app_id, private_key) do
    issued_at = DateTime.utc_now() |> DateTime.to_unix() |> Kernel.-(30)
    header = Jason.encode!(%{"alg" => "RS256", "typ" => "JWT"}) |> Base.url_encode64(padding: false)

    payload =
      Jason.encode!(%{"iat" => issued_at, "exp" => issued_at + 540, "iss" => app_id})
      |> Base.url_encode64(padding: false)

    unsigned = header <> "." <> payload
    signature = :public_key.sign(unsigned, :sha256, private_key) |> Base.url_encode64(padding: false)
    {:ok, unsigned <> "." <> signature}
  rescue
    _error -> {:error, :github_app_signing_failed}
  catch
    _kind, _reason -> {:error, :github_app_signing_failed}
  end

  defp normalize_pull_request(raw) when is_map(raw) do
    number = raw["number"]
    url = raw["html_url"]
    head = raw["head"]
    base = raw["base"]

    with true <- (is_integer(number) and number > 0) or {:error, :invalid_pull_request_response},
         true <- url == "https://github.com/#{@repo}/pull/#{number}" or {:error, :invalid_pull_request_response},
         true <- (is_map(head) and is_map(base)) or {:error, :invalid_pull_request_response},
         head_branch when is_binary(head_branch) <- head["ref"],
         base_branch when is_binary(base_branch) <- base["ref"],
         true <- (String.trim(head_branch) != "" and String.trim(base_branch) != "") or {:error, :invalid_pull_request_response},
         head_sha when is_binary(head_sha) <- head["sha"],
         base_sha when is_binary(base_sha) <- base["sha"],
         true <- (valid_sha?(head_sha) and valid_sha?(base_sha)) or {:error, :invalid_pull_request_response},
         state when state in ["open", "closed"] <- raw["state"],
         draft when is_boolean(draft) <- raw["draft"],
         {:ok, head_repository} <- pull_request_repository(head["repo"]),
         true <- valid_repository_name?(head_repository) or {:error, :invalid_pull_request_response},
         {:ok, base_repository} <- pull_request_repository(base["repo"]),
         true <- valid_repository_name?(base_repository) or {:error, :invalid_pull_request_response} do
      {:ok,
       %{
         number: number,
         url: url,
         head_repository_full_name: head_repository,
         base_repository_full_name: base_repository,
         head_branch: head_branch,
         base_branch: base_branch,
         head_sha: head_sha,
         base_sha: base_sha,
         state: state,
         draft: draft
       }}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_response}
    end
  end

  defp normalize_pull_request(_raw), do: {:error, :invalid_pull_request_response}

  defp pull_request_repository(%{"full_name" => name}) when is_binary(name), do: {:ok, name}
  defp pull_request_repository(_repository), do: {:error, :invalid_pull_request_response}

  defp normalize_check_run(raw) when is_map(raw) do
    id = raw["id"]
    app_id = get_in(raw, ["app", "id"])
    name = raw["name"]
    head_sha = raw["head_sha"]
    external_id = raw["external_id"]
    status = raw["status"]
    conclusion = raw["conclusion"]
    details_url = raw["details_url"]
    output = raw["output"]

    with true <- (is_integer(id) and id > 0) or {:error, :invalid_check_run_response},
         true <- (is_integer(app_id) and app_id > 0) or {:error, :invalid_check_run_response},
         true <- (is_binary(name) and String.trim(name) != "") or {:error, :invalid_check_run_response},
         true <- valid_sha?(head_sha) or {:error, :invalid_check_run_response},
         true <- is_nil(external_id) or external_id == "" or valid_external_id?(external_id) or {:error, :invalid_check_run_response},
         true <- status in @check_statuses or {:error, :invalid_check_run_response},
         true <- is_nil(conclusion) or conclusion in @check_conclusions or {:error, :invalid_check_run_response},
         true <- status == "completed" == not is_nil(conclusion) or {:error, :invalid_check_run_response},
         :ok <- validate_details_url(details_url),
         {:ok, normalized_output} <- normalize_response_output(output) do
      {:ok,
       %{
         id: id,
         app_id: app_id,
         name: name,
         head_sha: head_sha,
         external_id: external_id,
         status: status,
         conclusion: conclusion,
         details_url: details_url,
         output: normalized_output
       }}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_check_run_response}
    end
  end

  defp normalize_check_run(_raw), do: {:error, :invalid_check_run_response}

  defp normalize_response_output(nil), do: {:ok, nil}

  defp normalize_response_output(output) when is_map(output) do
    # Annotation counts and URLs are response metadata, not validation content.
    content = Map.take(output, ["title", "summary", "text"])

    if Enum.all?(content, fn {_key, value} -> is_binary(value) or is_nil(value) end) do
      {:ok, content}
    else
      {:error, :invalid_check_run_response}
    end
  end

  defp normalize_response_output(_output), do: {:error, :invalid_check_run_response}

  defp valid_external_id?(external_id) do
    is_binary(external_id) and byte_size(external_id) in 1..255 and String.trim(external_id) == external_id and
      not String.contains?(external_id, ["\r", "\n", "\0"])
  end

  defp valid_sha?(sha) when is_binary(sha) and byte_size(sha) in [40, 64], do: lowercase_hex?(sha)
  defp valid_sha?(_sha), do: false

  defp lowercase_hex?(value) do
    for <<byte <- value>>, reduce: true do
      valid? -> valid? and (byte in ?0..?9 or byte in ?a..?f)
    end
  end

  defp valid_repository_name?(value) when is_binary(value) do
    case String.split(value, "/", parts: 2) do
      [owner, repository] -> owner != "" and repository != "" and not String.contains?(value, [" ", "\\", "\n", "\r"])
      _ -> false
    end
  end

  defp valid_repository_name?(_value), do: false

  defp validate_pull_request_number(number) when is_integer(number) and number > 0, do: :ok
  defp validate_pull_request_number(_number), do: {:error, :invalid_pull_request_number}

  defp validate_check_run_id(id) when is_integer(id) and id > 0, do: :ok
  defp validate_check_run_id(_id), do: {:error, :invalid_check_run_id}

  defp valid_token?(token), do: is_binary(token) and String.trim(token) != "" and not String.contains?(token, ["\r", "\n", "\0"])
end
