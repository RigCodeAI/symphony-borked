defmodule SymphonyElixir.ReviewGithub do
  @moduledoc """
  Trusted GitHub App transport for reviewing pull requests in `RigCodeAI/rig`.

  Each operation creates a short-lived installation token scoped to that repository
  and to review permissions. This transport does not publish commits or merge pull
  requests.
  """

  @api_url "https://api.github.com"
  @repo "RigCodeAI/rig"
  @repo_name "rig"
  @api_version "2022-11-28"
  @user_agent "symphony-review"
  @request_timeout_ms 10_000
  @connect_timeout_ms 5_000
  @token_ttl_max_seconds 4_200
  @max_review_pages 10
  @per_page 100
  @max_review_comments 30
  @max_path_bytes 4_096
  @max_comment_body_bytes 65_536
  @max_line 2_147_483_647
  @requested_permissions %{"contents" => "read", "pull_requests" => "write", "metadata" => "read"}
  @review_states ["APPROVED", "CHANGES_REQUESTED", "COMMENTED"]
  @review_events %{
    "APPROVE" => "APPROVED",
    "REQUEST_CHANGES" => "CHANGES_REQUESTED",
    "COMMENT" => "COMMENTED"
  }
  @payload_keys ~w(body event commit_id comments)
  @comment_keys ~w(body path line side)
  @optional_comment_keys ~w(start_line start_side)

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
          draft: boolean(),
          author_login: String.t()
        }
  @type review :: %{
          id: pos_integer(),
          url: String.t(),
          head_sha: String.t(),
          state: String.t(),
          body: String.t() | nil,
          author_login: String.t(),
          author_type: String.t(),
          owned: boolean()
        }
  @type review_comment :: %{
          id: pos_integer(),
          path: String.t(),
          line: pos_integer(),
          side: String.t(),
          start_line: pos_integer() | nil,
          start_side: String.t() | nil,
          body: String.t(),
          head_sha: String.t(),
          owned: boolean()
        }
  @type review_payload :: map()

  @spec validate_config(config()) :: :ok | {:error, term()}
  def validate_config(config), do: validate_config_with(config, &perform_request/5)

  @spec get_pr(config(), pos_integer()) :: {:ok, pull_request()} | {:error, term()}
  def get_pr(config, number), do: get_pr_with(config, number, &perform_request/5)

  @spec reviews(config(), pos_integer()) :: {:ok, [review()]} | {:error, term()}
  def reviews(config, number), do: reviews_with(config, number, &perform_request/5)

  @spec review_comments(config(), pos_integer(), pos_integer()) :: {:ok, [review_comment()]} | {:error, term()}
  def review_comments(config, number, review_id),
    do: review_comments_with(config, number, review_id, &perform_request/5)

  @spec create_review(config(), pos_integer(), review_payload()) :: {:ok, review()} | {:error, term()}
  def create_review(config, number, payload), do: create_review_with(config, number, payload, &perform_request/5)

  @doc false
  @spec validate_config_for_test(config(), function()) :: :ok | {:error, term()}
  def validate_config_for_test(config, request_fun) when is_function(request_fun, 5),
    do: validate_config_with(config, request_fun)

  @doc false
  @spec get_pr_for_test(config(), pos_integer(), function()) :: {:ok, pull_request()} | {:error, term()}
  def get_pr_for_test(config, number, request_fun) when is_function(request_fun, 5),
    do: get_pr_with(config, number, request_fun)

  @doc false
  @spec reviews_for_test(config(), pos_integer(), function()) :: {:ok, [review()]} | {:error, term()}
  def reviews_for_test(config, number, request_fun) when is_function(request_fun, 5),
    do: reviews_with(config, number, request_fun)

  @doc false
  @spec review_comments_for_test(config(), pos_integer(), pos_integer(), function()) ::
          {:ok, [review_comment()]} | {:error, term()}
  def review_comments_for_test(config, number, review_id, request_fun) when is_function(request_fun, 5),
    do: review_comments_with(config, number, review_id, request_fun)

  @doc false
  @spec create_review_for_test(config(), pos_integer(), review_payload(), function()) ::
          {:ok, review()} | {:error, term()}
  def create_review_for_test(config, number, payload, request_fun) when is_function(request_fun, 5),
    do: create_review_with(config, number, payload, request_fun)

  defp validate_config_with(config, request_fun) do
    case installation_token(config, request_fun) do
      {:ok, _token, _normalized} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp get_pr_with(config, number, request_fun) do
    with :ok <- validate_pull_request_number(number),
         {:ok, token, _normalized} <- installation_token(config, request_fun),
         {:ok, raw} <- api_request(request_fun, :get, "/repos/#{@repo}/pulls/#{number}", token, %{}, nil),
         {:ok, pull_request} <- normalize_pull_request(raw, number) do
      {:ok, pull_request}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_response}
    end
  end

  defp reviews_with(config, number, request_fun) do
    with :ok <- validate_pull_request_number(number),
         {:ok, token, normalized} <- installation_token(config, request_fun) do
      list_review_pages(token, number, normalized.reviewer_login, 1, [], MapSet.new(), request_fun)
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_reviews_response}
    end
  end

  defp review_comments_with(config, number, review_id, request_fun) do
    with :ok <- validate_pull_request_number(number),
         :ok <- validate_review_id(review_id),
         {:ok, token, normalized} <- installation_token(config, request_fun) do
      list_review_comment_pages(token, number, review_id, normalized.reviewer_login, 1, [], MapSet.new(), request_fun)
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_review_comments_response}
    end
  end

  defp list_review_pages(token, number, reviewer_login, page, acc, ids, request_fun) do
    params = %{"per_page" => @per_page, "page" => page}
    path = "/repos/#{@repo}/pulls/#{number}/reviews"

    with {:ok, raw_reviews} <- api_request(request_fun, :get, path, token, params, nil),
         true <- (is_list(raw_reviews) and length(raw_reviews) <= @per_page) or {:error, :invalid_pull_request_reviews_response},
         {:ok, page_reviews, page_ids} <- normalize_reviews(raw_reviews, number, reviewer_login, ids),
         next_acc <- acc ++ page_reviews,
         next_ids <- MapSet.union(ids, page_ids) do
      cond do
        length(raw_reviews) < @per_page -> {:ok, next_acc}
        page >= @max_review_pages -> {:error, :pull_request_reviews_exceeded}
        true -> list_review_pages(token, number, reviewer_login, page + 1, next_acc, next_ids, request_fun)
      end
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_reviews_response}
    end
  end

  defp normalize_reviews(raw_reviews, number, reviewer_login, known_ids) do
    Enum.reduce_while(raw_reviews, {:ok, [], known_ids}, fn raw, {:ok, acc, ids} ->
      case normalize_review(raw, number, reviewer_login) do
        {:ok, review} ->
          if MapSet.member?(ids, review.id) do
            {:halt, {:error, :duplicate_pull_request_review}}
          else
            {:cont, {:ok, [review | acc], MapSet.put(ids, review.id)}}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, reviews, ids} -> {:ok, Enum.reverse(reviews), MapSet.difference(ids, known_ids)}
      {:error, _reason} = error -> error
    end
  end

  defp list_review_comment_pages(token, number, review_id, reviewer_login, page, acc, ids, request_fun) do
    params = %{"per_page" => @per_page, "page" => page}
    path = "/repos/#{@repo}/pulls/#{number}/reviews/#{review_id}/comments"

    with {:ok, raw_comments} <- api_request(request_fun, :get, path, token, params, nil),
         true <- (is_list(raw_comments) and length(raw_comments) <= @per_page) or {:error, :invalid_pull_request_review_comments_response},
         {:ok, page_comments, page_ids} <- normalize_review_comments_response(raw_comments, reviewer_login, ids),
         next_acc <- acc ++ page_comments,
         next_ids <- MapSet.union(ids, page_ids) do
      cond do
        length(raw_comments) < @per_page ->
          {:ok, next_acc}

        page >= @max_review_pages ->
          {:error, :pull_request_review_comments_exceeded}

        true ->
          list_review_comment_pages(
            token,
            number,
            review_id,
            reviewer_login,
            page + 1,
            next_acc,
            next_ids,
            request_fun
          )
      end
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_review_comments_response}
    end
  end

  defp normalize_review_comments_response(raw_comments, reviewer_login, known_ids) do
    Enum.reduce_while(raw_comments, {:ok, [], known_ids}, fn raw, {:ok, acc, ids} ->
      case normalize_review_comment_response(raw, reviewer_login) do
        {:ok, comment} ->
          if MapSet.member?(ids, comment.id) do
            {:halt, {:error, :duplicate_pull_request_review_comment}}
          else
            {:cont, {:ok, [comment | acc], MapSet.put(ids, comment.id)}}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, comments, ids} -> {:ok, Enum.reverse(comments), MapSet.difference(ids, known_ids)}
      {:error, _reason} = error -> error
    end
  end

  defp create_review_with(config, number, payload, request_fun) do
    with :ok <- validate_pull_request_number(number),
         {:ok, normalized_payload} <- normalize_review_payload(payload),
         {:ok, token, normalized} <- installation_token(config, request_fun),
         {:ok, raw} <-
           api_request(
             request_fun,
             :post,
             "/repos/#{@repo}/pulls/#{number}/reviews",
             token,
             %{},
             normalized_payload
           ),
         {:ok, review} <- normalize_review(raw, number, normalized.reviewer_login),
         :ok <- verify_created_review(review, normalized_payload, normalized) do
      {:ok, review}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_review_response}
    end
  end

  defp verify_created_review(review, payload, normalized) do
    expected_state = Map.fetch!(@review_events, payload["event"])

    if review.author_login == normalized.reviewer_login and review.author_type == "Bot" and
         review.head_sha == payload["commit_id"] and review.state == expected_state and
         submitted_body_matches?(review.body, payload["body"]) do
      :ok
    else
      {:error, :pull_request_review_response_mismatch}
    end
  end

  defp normalize_review_payload(payload) when is_map(payload) do
    with {:ok, normalized} <- normalize_string_keys(payload),
         :ok <- exact_keys(normalized, @payload_keys, []),
         true <- valid_review_payload_body?(normalized["body"]) or {:error, :invalid_pull_request_review_payload},
         true <- Map.has_key?(@review_events, normalized["event"]) or {:error, :invalid_pull_request_review_payload},
         true <- valid_sha?(normalized["commit_id"]) or {:error, :invalid_pull_request_review_payload},
         {:ok, comments} <- normalize_review_comments(normalized["comments"]) do
      {:ok,
       %{
         "body" => normalized["body"],
         "event" => normalized["event"],
         "commit_id" => normalized["commit_id"],
         "comments" => comments
       }}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_review_payload}
    end
  end

  defp normalize_review_payload(_payload), do: {:error, :invalid_pull_request_review_payload}

  defp normalize_review_comments(comments) when is_list(comments) and length(comments) <= @max_review_comments do
    Enum.reduce_while(comments, {:ok, [], MapSet.new()}, fn comment, {:ok, acc, locations} ->
      case normalize_review_comment(comment) do
        {:ok, normalized, location} ->
          if MapSet.member?(locations, location) do
            {:halt, {:error, :duplicate_pull_request_review_comment}}
          else
            {:cont, {:ok, [normalized | acc], MapSet.put(locations, location)}}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, comments, _locations} -> {:ok, Enum.reverse(comments)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_review_comments(_comments), do: {:error, :invalid_pull_request_review_payload}

  defp normalize_review_comment(comment) when is_map(comment) do
    with {:ok, normalized} <- normalize_string_keys(comment),
         :ok <- exact_keys(normalized, @comment_keys, @optional_comment_keys),
         true <- valid_text?(normalized["body"], @max_comment_body_bytes) or {:error, :invalid_pull_request_review_payload},
         true <- valid_review_path?(normalized["path"]) or {:error, :invalid_pull_request_review_payload},
         true <- valid_line?(normalized["line"]) or {:error, :invalid_pull_request_review_payload},
         true <- normalized["side"] == "RIGHT" or {:error, :invalid_pull_request_review_payload},
         :ok <- validate_review_range(normalized) do
      location = {normalized["path"], Map.get(normalized, "start_line"), normalized["line"]}
      {:ok, normalized, location}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_review_payload}
    end
  end

  defp normalize_review_comment(_comment), do: {:error, :invalid_pull_request_review_payload}

  defp validate_review_range(comment) do
    has_start_line = Map.has_key?(comment, "start_line")
    has_start_side = Map.has_key?(comment, "start_side")

    cond do
      not has_start_line and not has_start_side ->
        :ok

      has_start_line and has_start_side and valid_line?(comment["start_line"]) and
        comment["start_line"] <= comment["line"] and comment["start_side"] == "RIGHT" ->
        :ok

      true ->
        {:error, :invalid_pull_request_review_payload}
    end
  end

  defp normalize_pull_request(raw, number) when is_map(raw) do
    head = raw["head"]
    base = raw["base"]
    author = raw["user"]

    with true <- raw["number"] == number or {:error, :invalid_pull_request_response},
         true <- raw["html_url"] == "https://github.com/#{@repo}/pull/#{number}" or {:error, :invalid_pull_request_response},
         true <- (is_map(head) and is_map(base) and is_map(author)) or {:error, :invalid_pull_request_response},
         head_branch when is_binary(head_branch) <- head["ref"],
         base_branch when is_binary(base_branch) <- base["ref"],
         true <- (valid_branch?(head_branch) and valid_branch?(base_branch)) or {:error, :invalid_pull_request_response},
         head_sha when is_binary(head_sha) <- head["sha"],
         base_sha when is_binary(base_sha) <- base["sha"],
         true <- (valid_sha?(head_sha) and valid_sha?(base_sha)) or {:error, :invalid_pull_request_response},
         state when state in ["open", "closed"] <- raw["state"],
         draft when is_boolean(draft) <- raw["draft"],
         {:ok, head_repository} <- pull_request_repository(head["repo"]),
         {:ok, base_repository} <- pull_request_repository(base["repo"]),
         true <- base_repository == @repo or {:error, :invalid_pull_request_response},
         author_login when is_binary(author_login) <- author["login"],
         true <- valid_login?(author_login) or {:error, :invalid_pull_request_response} do
      {:ok,
       %{
         number: number,
         url: raw["html_url"],
         head_repository_full_name: head_repository,
         base_repository_full_name: base_repository,
         head_branch: head_branch,
         base_branch: base_branch,
         head_sha: head_sha,
         base_sha: base_sha,
         state: state,
         draft: draft,
         author_login: author_login
       }}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_response}
    end
  end

  defp normalize_pull_request(_raw, _number), do: {:error, :invalid_pull_request_response}

  defp normalize_review(raw, number, reviewer_login) when is_map(raw) do
    id = raw["id"]
    author = raw["user"]
    expected_url = if is_integer(id) and id > 0, do: "https://github.com/#{@repo}/pull/#{number}#pullrequestreview-#{id}"

    with true <- (is_integer(id) and id > 0) or {:error, :invalid_pull_request_review_response},
         true <- raw["html_url"] == expected_url or {:error, :invalid_pull_request_review_response},
         sha when is_binary(sha) <- raw["commit_id"],
         true <- valid_sha?(sha) or {:error, :invalid_pull_request_review_response},
         state when state in @review_states <- raw["state"],
         body <- raw["body"],
         true <- valid_review_body?(body) or {:error, :invalid_pull_request_review_response},
         true <- is_map(author) or {:error, :invalid_pull_request_review_response},
         author_login when is_binary(author_login) <- author["login"],
         true <- valid_login?(author_login) or {:error, :invalid_pull_request_review_response},
         author_type when is_binary(author_type) <- author["type"],
         true <-
           (String.valid?(author_type) and String.trim(author_type) != "" and byte_size(author_type) <= 64 and
              not control?(author_type)) or {:error, :invalid_pull_request_review_response} do
      {:ok,
       %{
         id: id,
         url: raw["html_url"],
         head_sha: sha,
         state: state,
         body: body,
         author_login: author_login,
         author_type: author_type,
         owned: author_login == reviewer_login and author_type == "Bot"
       }}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_review_response}
    end
  end

  defp normalize_review(_raw, _number, _reviewer_login), do: {:error, :invalid_pull_request_review_response}

  defp normalize_review_comment_response(raw, reviewer_login) when is_map(raw) do
    author = raw["user"]

    with id when is_integer(id) and id > 0 <- raw["id"],
         path when is_binary(path) <- raw["path"],
         true <- valid_review_path?(path) or {:error, :invalid_pull_request_review_comment_response},
         line when is_integer(line) <- raw["line"],
         true <- valid_line?(line) or {:error, :invalid_pull_request_review_comment_response},
         side when side in ["LEFT", "RIGHT"] <- raw["side"],
         body when is_binary(body) <- raw["body"],
         true <- valid_text?(body, @max_comment_body_bytes) or {:error, :invalid_pull_request_review_comment_response},
         sha when is_binary(sha) <- raw["commit_id"],
         true <- valid_sha?(sha) or {:error, :invalid_pull_request_review_comment_response},
         {:ok, start_line, start_side} <- normalize_review_comment_range(raw),
         true <- is_map(author) or {:error, :invalid_pull_request_review_comment_response},
         author_login when is_binary(author_login) <- author["login"],
         true <- valid_login?(author_login) or {:error, :invalid_pull_request_review_comment_response},
         author_type when is_binary(author_type) <- author["type"],
         true <- valid_author_type?(author_type) or {:error, :invalid_pull_request_review_comment_response} do
      {:ok,
       %{
         id: id,
         path: path,
         line: line,
         side: side,
         start_line: start_line,
         start_side: start_side,
         body: body,
         head_sha: sha,
         owned: author_login == reviewer_login and author_type == "Bot"
       }}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_pull_request_review_comment_response}
    end
  end

  defp normalize_review_comment_response(_raw, _reviewer_login),
    do: {:error, :invalid_pull_request_review_comment_response}

  defp normalize_review_comment_range(raw) do
    start_line = raw["start_line"]
    start_side = raw["start_side"]

    cond do
      is_nil(start_line) and is_nil(start_side) ->
        {:ok, nil, nil}

      valid_line?(start_line) and start_line <= raw["line"] and start_side in ["LEFT", "RIGHT"] ->
        {:ok, start_line, start_side}

      true ->
        {:error, :invalid_pull_request_review_comment_response}
    end
  end

  defp valid_author_type?(author_type) when is_binary(author_type) do
    String.valid?(author_type) and String.trim(author_type) != "" and byte_size(author_type) <= 64 and
      not control?(author_type)
  end

  defp valid_author_type?(_author_type), do: false

  defp installation_token(config, request_fun) do
    with {:ok, normalized} <- normalize_config(config),
         {:ok, key} <- read_private_key(normalized.private_key_path),
         {:ok, jwt} <- app_jwt(normalized.app_id, key),
         {:ok, app} <- api_request(request_fun, :get, "/app", jwt, %{}, nil),
         {:ok, reviewer_login} <- validate_app(app, normalized),
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
      {:ok, token, Map.put(normalized, :reviewer_login, reviewer_login)}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :github_app_authentication_failed}
    end
  rescue
    _error -> {:error, :github_app_authentication_failed}
  catch
    _kind, _reason -> {:error, :github_app_authentication_failed}
  end

  defp validate_app(%{"id" => id, "slug" => slug}, normalized) do
    with {:ok, id} <- normalize_identifier(id),
         true <- id == normalized.app_id or {:error, :github_app_id_mismatch},
         true <- valid_app_slug?(slug) or {:error, :invalid_github_app_response} do
      {:ok, slug <> "[bot]"}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_github_app_response}
    end
  end

  defp validate_app(_app, _normalized), do: {:error, :invalid_github_app_response}

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
         true <- payload["permissions"] == @requested_permissions or {:error, :github_installation_permissions_invalid},
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

  defp normalize_string_keys(map) do
    keys = Enum.map(Map.keys(map), &canonical_key/1)

    if Enum.all?(keys, &is_binary/1) and length(Enum.uniq(keys)) == length(keys) do
      {:ok, Map.new(map, fn {key, value} -> {canonical_key(key), value} end)}
    else
      {:error, :invalid_pull_request_review_payload}
    end
  end

  defp canonical_key(key) when is_atom(key), do: Atom.to_string(key)
  defp canonical_key(key) when is_binary(key), do: key
  defp canonical_key(_key), do: nil

  defp exact_keys(map, required, optional) when is_map(map) do
    keys = Map.keys(map)
    if required -- keys == [] and keys -- (required ++ optional) == [], do: :ok, else: {:error, :invalid_pull_request_review_payload}
  end

  defp exact_keys(_map, _required, _optional), do: {:error, :invalid_pull_request_review_payload}

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
         true <- (is_tuple(key) and tuple_size(key) > 0 and elem(key, 0) == :RSAPrivateKey) or {:error, :github_private_key_invalid} do
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
    payload = Jason.encode!(%{"iat" => issued_at, "exp" => issued_at + 540, "iss" => app_id}) |> Base.url_encode64(padding: false)
    unsigned = header <> "." <> payload
    signature = :public_key.sign(unsigned, :sha256, private_key) |> Base.url_encode64(padding: false)
    {:ok, unsigned <> "." <> signature}
  rescue
    _error -> {:error, :github_app_signing_failed}
  catch
    _kind, _reason -> {:error, :github_app_signing_failed}
  end

  defp valid_review_path?(path) when is_binary(path) do
    segments = String.split(path, "/", trim: false)

    byte_size(path) in 1..@max_path_bytes and String.valid?(path) and not control?(path) and
      not String.contains?(path, "\\") and Path.type(path) == :relative and
      Enum.all?(segments, &(&1 not in ["", ".", ".."]))
  end

  defp valid_review_path?(_path), do: false

  defp valid_text?(value, max_bytes) when is_binary(value) do
    byte_size(value) in 1..max_bytes and String.valid?(value) and String.trim(value) != "" and
      not Regex.match?(~r/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/, value)
  end

  defp valid_text?(_value, _max_bytes), do: false

  defp valid_review_body?(nil), do: true

  defp valid_review_body?(body) when is_binary(body) do
    byte_size(body) <= @max_comment_body_bytes and String.valid?(body) and
      not Regex.match?(~r/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/, body)
  end

  defp valid_review_body?(_body), do: false

  defp valid_review_payload_body?(body) when is_binary(body) do
    byte_size(body) <= @max_comment_body_bytes and String.valid?(body) and
      not Regex.match?(~r/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/, body)
  end

  defp valid_review_payload_body?(_body), do: false

  defp submitted_body_matches?(nil, ""), do: true
  defp submitted_body_matches?(response_body, submitted_body), do: response_body == submitted_body

  defp control?(value), do: Regex.match?(~r/[\x00-\x1f\x7f]/, value)

  defp valid_line?(line), do: is_integer(line) and line in 1..@max_line

  defp valid_branch?(branch) do
    String.trim(branch) != "" and String.valid?(branch) and not control?(branch) and byte_size(branch) <= 255
  end

  defp pull_request_repository(%{"full_name" => name}) when is_binary(name) do
    if valid_repository_name?(name), do: {:ok, name}, else: {:error, :invalid_pull_request_response}
  end

  defp pull_request_repository(_repository), do: {:error, :invalid_pull_request_response}

  defp valid_repository_name?(value) when is_binary(value) do
    case String.split(value, "/", parts: 2) do
      [owner, repository] ->
        owner != "" and repository != "" and byte_size(value) <= 255 and String.valid?(value) and
          not String.contains?(value, [" ", "\\"]) and not control?(value)

      _ ->
        false
    end
  end

  defp valid_repository_name?(_value), do: false

  defp valid_login?(value) when is_binary(value) do
    byte_size(value) in 1..105 and String.trim(value) == value and String.valid?(value) and not control?(value) and
      (Regex.match?(~r/\A[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?\z/, value) or valid_bot_login?(value))
  end

  defp valid_login?(_value), do: false

  defp valid_app_slug?(slug) when is_binary(slug) do
    byte_size(slug) in 1..100 and Regex.match?(~r/\A[a-z0-9]+(?:-[a-z0-9]+)*\z/, slug)
  end

  defp valid_app_slug?(_slug), do: false

  defp valid_bot_login?(login) do
    case String.replace_suffix(login, "[bot]", "") do
      slug when slug != login -> valid_app_slug?(slug)
      _ -> false
    end
  end

  defp validate_pull_request_number(number) when is_integer(number) and number in 1..2_147_483_647, do: :ok
  defp validate_pull_request_number(_number), do: {:error, :invalid_pull_request_number}

  defp validate_review_id(review_id) when is_integer(review_id) and review_id > 0, do: :ok
  defp validate_review_id(_review_id), do: {:error, :invalid_pull_request_review_id}

  defp valid_sha?(sha) when is_binary(sha) and byte_size(sha) in [40, 64], do: lowercase_hex?(sha)
  defp valid_sha?(_sha), do: false

  defp lowercase_hex?(value) do
    for <<byte <- value>>, reduce: true do
      valid? -> valid? and (byte in ?0..?9 or byte in ?a..?f)
    end
  end

  defp valid_token?(token) do
    is_binary(token) and byte_size(token) in 1..512 and String.trim(token) == token and
      not String.contains?(token, ["\r", "\n", "\0"])
  end

  defp parse_expiry(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, DateTime.to_unix(datetime)}
      _ -> {:error, :invalid_github_installation_expiry}
    end
  end
end
