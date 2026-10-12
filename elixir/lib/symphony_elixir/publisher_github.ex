defmodule SymphonyElixir.PublisherGithub do
  @moduledoc """
  Trusted GitHub App transport for publishing candidates to RigCodeAI/rig.

  Installation tokens are requested for this repository only and are discarded
  after each operation. `update_pr/3` compares the current body before writing;
  GitHub does not provide a conditional update, so a human edit racing the PATCH
  can still be overwritten.
  """

  @api_url "https://api.github.com"
  @repo "RigCodeAI/rig"
  @api_version "2022-11-28"
  @user_agent "symphony-publisher"
  @request_timeout_ms 10_000
  @connect_timeout_ms 5_000
  @token_ttl_max_seconds 4_200
  @required_permissions %{"contents" => "write", "pull_requests" => "write"}

  @type config :: %{app_id: integer() | String.t(), installation_id: integer() | String.t(), private_key_path: Path.t()}
  @type pull_request :: %{
          number: pos_integer(),
          url: String.t(),
          head_sha: String.t(),
          head_branch: String.t(),
          base_branch: String.t(),
          state: String.t(),
          body: String.t() | nil
        }

  @spec validate_config(config()) :: :ok | {:error, term()}
  def validate_config(config) do
    validate_config_with(config, &perform_request/5)
  end

  @spec find_pr(config(), String.t()) :: {:ok, pull_request() | nil} | {:error, term()}
  def find_pr(config, branch) do
    find_pr_with(config, branch, &perform_request/5)
  end

  @spec branch_sha(config(), String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def branch_sha(config, branch) do
    branch_sha_with(config, branch, &perform_request/5)
  end

  @spec push(config(), Path.t(), String.t(), String.t(), String.t() | nil) :: :ok | {:error, term()}
  def push(config, workspace, sha, branch, expected_sha) do
    with {:ok, token} <- installation_token(config, &perform_request/5) do
      case SymphonyElixir.PublisherGit.push(workspace, sha, branch, token, expected_sha) do
        {:error, {:git_push_unknown, reason}} ->
          {:error, {:github_request_failed, {:push_outcome_unknown, reason}}}

        result ->
          result
      end
    end
  end

  @spec create_pr(config(), String.t(), String.t(), String.t()) :: {:ok, pull_request()} | {:error, term()}
  def create_pr(config, branch, title, body) do
    create_pr_with(config, branch, title, body, &perform_request/5)
  end

  @spec update_pr(config(), pos_integer(), String.t() | nil, String.t() | nil) ::
          {:ok, pull_request()} | {:error, term()}
  def update_pr(config, number, body, expected_body) do
    update_pr_with(config, number, body, expected_body, &perform_request/5)
  end

  @spec get_pr(config(), pos_integer()) :: {:ok, pull_request()} | {:error, term()}
  def get_pr(config, number) do
    get_pr_with(config, number, &perform_request/5)
  end

  @doc false
  @spec validate_config_for_test(config(), function()) :: :ok | {:error, term()}
  def validate_config_for_test(config, request_fun) when is_function(request_fun, 5),
    do: validate_config_with(config, request_fun)

  @doc false
  @spec find_pr_for_test(config(), String.t(), function()) :: {:ok, pull_request() | nil} | {:error, term()}
  def find_pr_for_test(config, branch, request_fun) when is_function(request_fun, 5),
    do: find_pr_with(config, branch, request_fun)

  @doc false
  @spec branch_sha_for_test(config(), String.t(), function()) :: {:ok, String.t() | nil} | {:error, term()}
  def branch_sha_for_test(config, branch, request_fun) when is_function(request_fun, 5),
    do: branch_sha_with(config, branch, request_fun)

  @doc false
  @spec create_pr_for_test(config(), String.t(), String.t(), String.t(), function()) ::
          {:ok, pull_request()} | {:error, term()}
  def create_pr_for_test(config, branch, title, body, request_fun) when is_function(request_fun, 5),
    do: create_pr_with(config, branch, title, body, request_fun)

  @doc false
  @spec update_pr_for_test(config(), pos_integer(), String.t() | nil, String.t() | nil, function()) ::
          {:ok, pull_request()} | {:error, term()}
  def update_pr_for_test(config, number, body, expected_body, request_fun) when is_function(request_fun, 5),
    do: update_pr_with(config, number, body, expected_body, request_fun)

  @doc false
  @spec get_pr_for_test(config(), pos_integer(), function()) :: {:ok, pull_request()} | {:error, term()}
  def get_pr_for_test(config, number, request_fun) when is_function(request_fun, 5),
    do: get_pr_with(config, number, request_fun)

  defp validate_config_with(config, request_fun) do
    case installation_token(config, request_fun) do
      {:ok, _token} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp find_pr_with(config, branch, request_fun) do
    with :ok <- validate_branch(branch),
         {:ok, token} <- installation_token(config, request_fun),
         {:ok, matches} <- find_pr_pages(token, branch, 1, [], request_fun) do
      case matches do
        [] -> {:ok, nil}
        [pull_request] -> {:ok, pull_request}
        _ -> {:error, :multiple_pull_requests_for_branch}
      end
    end
  end

  defp find_pr_pages(_token, _branch, page, acc, _request_fun) when page > 10,
    do: if(acc == [], do: {:ok, []}, else: {:error, :pull_request_results_exceeded})

  defp find_pr_pages(token, branch, page, acc, request_fun) do
    params = %{
      "head" => "RigCodeAI:" <> branch,
      "state" => "all",
      "per_page" => 100,
      "page" => page
    }

    with {:ok, payload} <- api_request(request_fun, :get, "/repos/RigCodeAI/rig/pulls", token, params, nil),
         true <- is_list(payload) or {:error, :invalid_pull_request_response},
         {:ok, pull_requests} <- normalize_pull_requests(payload, branch),
         updated <- acc ++ pull_requests do
      cond do
        length(updated) > 1 -> {:ok, updated}
        length(payload) < 100 -> {:ok, updated}
        true -> find_pr_pages(token, branch, page + 1, updated, request_fun)
      end
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_response}
    end
  end

  defp normalize_pull_requests(raw_pull_requests, branch) do
    Enum.reduce_while(raw_pull_requests, {:ok, []}, fn raw, {:ok, acc} ->
      case normalize_pull_request(raw) do
        {:ok, pull_request} when pull_request.head_branch == branch and pull_request.base_branch == "main" ->
          {:cont, {:ok, [pull_request | acc]}}

        {:ok, _pull_request} ->
          {:halt, {:error, :pull_request_branch_mismatch}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, pull_requests} -> {:ok, Enum.reverse(pull_requests)}
      {:error, _reason} = error -> error
    end
  end

  defp branch_sha_with(config, branch, request_fun) do
    with :ok <- validate_branch(branch),
         {:ok, token} <- installation_token(config, request_fun) do
      path = "/repos/RigCodeAI/rig/git/ref/heads/" <> encoded_ref(branch)

      case api_request(request_fun, :get, path, token, %{}, nil) do
        {:ok, %{"object" => %{"sha" => sha, "type" => "commit"}}} ->
          if valid_sha?(sha), do: {:ok, sha}, else: {:error, :invalid_branch_ref_response}

        {:error, {:github_api_status, 404}} ->
          {:ok, nil}

        {:error, _reason} = error ->
          error

        _ ->
          {:error, :invalid_branch_ref_response}
      end
    end
  end

  defp create_pr_with(config, branch, title, body, request_fun) do
    with :ok <- validate_branch(branch),
         :ok <- validate_title(title),
         true <- is_binary(body) or {:error, :invalid_pull_request_body},
         {:ok, token} <- installation_token(config, request_fun),
         {:ok, payload} <-
           api_request(
             request_fun,
             :post,
             "/repos/RigCodeAI/rig/pulls",
             token,
             %{},
             %{"title" => title, "body" => body, "head" => branch, "base" => "main"}
           ),
         {:ok, pull_request} <- normalize_pull_request(payload),
         true <- (pull_request.head_branch == branch and pull_request.base_branch == "main") or {:error, :pull_request_branch_mismatch} do
      {:ok, pull_request}
    else
      {:error, :invalid_pull_request_response} -> {:error, {:github_request_failed, :ambiguous_create_response}}
      {:error, :pull_request_branch_mismatch} -> {:error, {:github_request_failed, :ambiguous_create_response}}
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_response}
    end
  end

  defp update_pr_with(config, number, body, expected_body, request_fun) do
    with :ok <- validate_pull_request_number(number),
         true <-
           ((is_binary(body) or is_nil(body)) and (is_binary(expected_body) or is_nil(expected_body))) or
             {:error, :invalid_pull_request_body},
         {:ok, token} <- installation_token(config, request_fun),
         {:ok, current} <- request_pull_request(token, number, request_fun),
         true <- current.body == expected_body or {:error, :pull_request_body_changed},
         {:ok, payload} <-
           api_request(request_fun, :patch, "/repos/RigCodeAI/rig/pulls/#{number}", token, %{}, %{"body" => body}),
         {:ok, updated} <- normalize_pull_request(payload),
         true <-
           (updated.number == number and updated.head_branch == current.head_branch and updated.body == body) or
             {:error, :pull_request_update_response_mismatch} do
      {:ok, updated}
    else
      {:error, :invalid_pull_request_response} -> {:error, {:github_request_failed, :ambiguous_update_response}}
      {:error, :pull_request_number_mismatch} -> {:error, {:github_request_failed, :ambiguous_update_response}}
      {:error, :pull_request_update_response_mismatch} -> {:error, {:github_request_failed, :ambiguous_update_response}}
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_response}
    end
  end

  defp get_pr_with(config, number, request_fun) do
    with :ok <- validate_pull_request_number(number),
         {:ok, token} <- installation_token(config, request_fun),
         {:ok, pull_request} <- request_pull_request(token, number, request_fun) do
      {:ok, pull_request}
    end
  end

  defp request_pull_request(token, number, request_fun) do
    with {:ok, payload} <-
           api_request(request_fun, :get, "/repos/RigCodeAI/rig/pulls/#{number}", token, %{}, nil),
         {:ok, pull_request} <- normalize_pull_request(payload),
         true <- pull_request.number == number or {:error, :pull_request_number_mismatch} do
      {:ok, pull_request}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_response}
    end
  end

  defp installation_token(config, request_fun) do
    with {:ok, normalized} <- normalize_config(config),
         {:ok, key} <- read_private_key(normalized.private_key_path),
         {:ok, jwt} <- app_jwt(normalized.app_id, key),
         {:ok, payload} <-
           api_request(
             request_fun,
             :post,
             "/app/installations/#{normalized.installation_id}/access_tokens",
             jwt,
             %{},
             %{"repositories" => ["rig"], "permissions" => @required_permissions}
           ),
         {:ok, token} <- validate_installation_token_response(payload) do
      {:ok, token}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :github_app_authentication_failed}
    end
  rescue
    _error -> {:error, :github_app_authentication_failed}
  catch
    _kind, _reason -> {:error, :github_app_authentication_failed}
  end

  defp validate_installation_token_response(%{"token" => token, "expires_at" => expires_at} = payload)
       when is_binary(token) and is_binary(expires_at) do
    now = DateTime.utc_now() |> DateTime.to_unix()

    with {:ok, expiry} <- parse_expiry(expires_at),
         true <- expiry > now or {:error, :github_installation_token_expired},
         true <- expiry <= now + @token_ttl_max_seconds or {:error, :github_installation_token_ttl_invalid},
         true <- valid_token?(token) or {:error, :github_installation_token_invalid},
         true <- payload["permissions"] |> permissions_satisfy?() or {:error, :github_installation_permissions_missing},
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
    Enum.all?(@required_permissions, fn {name, level} -> permissions[name] == level end)
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

  defp valid_token?(token), do: is_binary(token) and String.trim(token) != "" and not String.contains?(token, ["\r", "\n", "\0"])
  defp valid_sha?(sha) when is_binary(sha) and byte_size(sha) in [40, 64], do: lowercase_hex?(sha)
  defp valid_sha?(_sha), do: false

  defp lowercase_hex?(value) do
    for <<byte <- value>>, reduce: true do
      valid? -> valid? and (byte in ?0..?9 or byte in ?a..?f)
    end
  end

  defp validate_branch(branch) when is_binary(branch) and byte_size(branch) in 1..240 do
    if String.match?(branch, ~r/^[A-Za-z0-9][A-Za-z0-9._\/-]*$/) and
         not String.contains?(branch, ["..", "//", "@{", "\\"]) and
         not String.ends_with?(branch, ["/", ".", ".lock"]) and
         not Enum.any?(String.split(branch, "/"), &String.starts_with?(&1, ".")) do
      :ok
    else
      {:error, :invalid_publisher_branch}
    end
  end

  defp validate_branch(_branch), do: {:error, :invalid_publisher_branch}

  defp validate_title(title) when is_binary(title) and byte_size(title) in 1..256 do
    if String.trim(title) == "", do: {:error, :invalid_pull_request_title}, else: :ok
  end

  defp validate_title(_title), do: {:error, :invalid_pull_request_title}

  defp validate_pull_request_number(number) when is_integer(number) and number > 0, do: :ok
  defp validate_pull_request_number(_number), do: {:error, :invalid_pull_request_number}

  defp normalize_pull_request(raw) when is_map(raw) do
    number = raw["number"]
    url = raw["html_url"]
    head = raw["head"]
    base = raw["base"]
    body = raw["body"]
    state = raw["state"]

    with true <- (is_integer(number) and number > 0) or {:error, :invalid_pull_request_response},
         true <- url == "https://github.com/RigCodeAI/rig/pull/#{number}" or {:error, :invalid_pull_request_response},
         true <- (is_map(head) and is_map(base)) or {:error, :invalid_pull_request_response},
         head_branch when is_binary(head_branch) <- head["ref"],
         base_branch when is_binary(base_branch) <- base["ref"],
         head_sha when is_binary(head_sha) <- get_in(head, ["sha"]),
         true <- valid_sha?(head_sha) or {:error, :invalid_pull_request_response},
         true <- is_nil(body) or is_binary(body) or {:error, :invalid_pull_request_response},
         true <- state in ["open", "closed"] or {:error, :invalid_pull_request_response},
         true <- base_branch == "main" or {:error, :invalid_pull_request_response},
         true <- get_in(base, ["repo", "full_name"]) == @repo or {:error, :invalid_pull_request_response},
         true <-
           valid_head_repository?(get_in(head, ["repo", "full_name"])) or
             {:error, :invalid_pull_request_response} do
      {:ok,
       %{
         number: number,
         url: url,
         head_sha: head_sha,
         head_branch: head_branch,
         base_branch: base_branch,
         state: state,
         body: body
       }}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_response}
    end
  end

  defp normalize_pull_request(_raw), do: {:error, :invalid_pull_request_response}

  defp valid_head_repository?(nil), do: true
  defp valid_head_repository?(@repo), do: true
  defp valid_head_repository?(_repository), do: false

  defp encoded_ref(branch) do
    branch
    |> String.split("/", trim: false)
    |> Enum.map_join("/", fn segment -> URI.encode(segment, &URI.char_unreserved?/1) end)
  end
end
