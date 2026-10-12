defmodule SymphonyElixir.ReviewGithubTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.ReviewGithub

  @head_sha String.duplicate("a", 40)
  @base_sha String.duplicate("b", 40)
  @other_sha String.duplicate("c", 40)
  @reviewer_login "review-app[bot]"
  @token "ghs_short_lived_review_token"

  setup do
    root = Path.join(System.tmp_dir!(), "review-github-test-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false))
    File.mkdir_p!(root)

    key = :public_key.generate_key({:rsa, 2048, 65_537})
    key_path = Path.join(root, "github-app.pem")
    pem = :public_key.pem_encode([:public_key.pem_entry_encode(:RSAPrivateKey, key)])
    File.write!(key_path, pem)
    File.chmod!(key_path, 0o600)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, config: %{app_id: 123, installation_id: 2468, private_key_path: key_path}, key: key}
  end

  test "validates the authenticated App and requests only scoped review permissions", %{config: config, key: key} do
    request_fun = app_request_fun(key, fn _method, _path, _headers, _params, _body -> flunk("no repository request expected") end)

    assert :ok = ReviewGithub.validate_config_for_test(config, request_fun)
    assert_receive :review_installation_token_requested
  end

  test "rejects App and installation identity mismatches before repository access", %{config: config, key: key} do
    no_repository_request = fn _method, _path, _headers, _params, _body -> flunk("repository call must not happen") end

    wrong_app = %{"id" => 999, "slug" => "review-app"}
    app_request = app_request_fun(key, no_repository_request, %{app: wrong_app})
    assert {:error, :github_app_id_mismatch} = ReviewGithub.validate_config_for_test(config, app_request)

    wrong_installation_id = %{"id" => 999, "app_id" => 123}
    installation_id_request = app_request_fun(key, no_repository_request, %{installation: wrong_installation_id})
    assert {:error, :github_installation_id_mismatch} = ReviewGithub.validate_config_for_test(config, installation_id_request)

    wrong_installation = %{"id" => 2468, "app_id" => 999}
    installation_request = app_request_fun(key, no_repository_request, %{installation: wrong_installation})
    assert {:error, :github_installation_app_mismatch} = ReviewGithub.validate_config_for_test(config, installation_request)
  end

  test "rejects broad permissions, another repository, and long-lived tokens", %{config: config, key: key} do
    broad_permissions = put_in(token_response(), ["permissions", "contents"], "write")
    extra_permissions = put_in(token_response(), ["permissions", "checks"], "read")
    wrong_repository = put_in(token_response(), ["repositories", Access.at(0), "full_name"], "someone/else")
    extra_repository = Map.update!(token_response(), "repositories", &(&1 ++ [%{"full_name" => "someone/else"}]))
    long_lived = Map.put(token_response(), "expires_at", DateTime.add(DateTime.utc_now(), 7_200, :second) |> DateTime.to_iso8601())
    expired = Map.put(token_response(), "expires_at", DateTime.add(DateTime.utc_now(), -1, :second) |> DateTime.to_iso8601())

    for token <- [broad_permissions, extra_permissions, wrong_repository, extra_repository, long_lived, expired] do
      request_fun =
        app_request_fun(
          key,
          fn _method, _path, _headers, _params, _body -> flunk("repository call must not happen") end,
          %{token: token}
        )

      assert {:error, _reason} = ReviewGithub.validate_config_for_test(config, request_fun)
    end
  end

  test "gets a complete pull request identity including its author", %{config: config, key: key} do
    raw = pull_request(42)
    raw = put_in(raw, ["head", "repo", "full_name"], "contributor/rig")

    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42", headers, %{}, nil ->
        assert_installation_token!(headers)
        {:ok, %{status: 200, body: raw}}
      end)

    assert {:ok, pull_request} = ReviewGithub.get_pr_for_test(config, 42, request_fun)

    assert pull_request == %{
             number: 42,
             url: "https://github.com/RigCodeAI/rig/pull/42",
             head_repository_full_name: "contributor/rig",
             base_repository_full_name: "RigCodeAI/rig",
             head_branch: "feature/review",
             base_branch: "main",
             head_sha: @head_sha,
             base_sha: @base_sha,
             state: "open",
             draft: false,
             author_login: "contributor"
           }
  end

  test "rejects an incomplete or mismatched pull request response", %{config: config, key: key} do
    malformed = put_in(pull_request(42), ["user", "login"], nil)

    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42", _headers, %{}, nil ->
        {:ok, %{status: 200, body: malformed}}
      end)

    assert {:error, _reason} = ReviewGithub.get_pr_for_test(config, 42, request_fun)

    no_request = fn _method, _path, _headers, _params, _body -> flunk("invalid pull request number must not make a request") end
    assert {:error, :invalid_pull_request_number} = ReviewGithub.get_pr_for_test(config, 0, no_request)
  end

  test "lists every review across pages and normalizes all authors and commits", %{config: config, key: key} do
    first_page = Enum.map(1..100, fn id -> raw_review(id, @head_sha, review_state(id), "reviewer-#{id}", "User") end)
    final_page = [raw_review(101, @other_sha, "COMMENTED", @reviewer_login, "Bot", "Review note")]

    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42/reviews", headers, params, nil ->
        assert_installation_token!(headers)
        assert params["per_page"] == 100

        case params["page"] do
          1 -> {:ok, %{status: 200, body: first_page}}
          2 -> {:ok, %{status: 200, body: final_page}}
        end
      end)

    assert {:ok, reviews} = ReviewGithub.reviews_for_test(config, 42, request_fun)
    assert length(reviews) == 101
    assert hd(reviews).id == 1
    assert hd(reviews).head_sha == @head_sha

    assert List.last(reviews) == %{
             id: 101,
             url: review_url(42, 101),
             head_sha: @other_sha,
             state: "COMMENTED",
             body: "Review note",
             author_login: @reviewer_login,
             author_type: "Bot",
             owned: true
           }
  end

  test "marks only reviews authored by this authenticated App bot as owned", %{config: config, key: key} do
    response_reviews = [
      raw_review(1, @head_sha, "APPROVED", @reviewer_login, "Bot"),
      raw_review(2, @head_sha, "COMMENTED", "another-app[bot]", "Bot"),
      raw_review(3, @head_sha, "COMMENTED", "review-app", "User")
    ]

    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42/reviews", _headers, _params, nil ->
        {:ok, %{status: 200, body: response_reviews}}
      end)

    assert {:ok, reviews} = ReviewGithub.reviews_for_test(config, 42, request_fun)
    assert Enum.map(reviews, & &1.owned) == [true, false, false]
  end

  test "lists normalized inline comments and treats absent or null range fields as single-line", %{config: config, key: key} do
    response_comments = [
      raw_review_comment(1, "lib/space name:literal[1]*.ex", 12, "RIGHT", @head_sha, @reviewer_login, "Bot", "Single line"),
      raw_review_comment(2, "lib/null-range.ex", 13, "RIGHT", @head_sha, @reviewer_login, "Bot", "Null range")
      |> Map.merge(%{"start_line" => nil, "start_side" => nil}),
      raw_review_comment(3, "lib/range.ex", 14, "RIGHT", @head_sha, @reviewer_login, "Bot", "Range")
      |> Map.merge(%{"start_line" => 10, "start_side" => "RIGHT"})
    ]

    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42/reviews/77/comments", headers, params, nil ->
        assert_installation_token!(headers)
        assert params == %{"per_page" => 100, "page" => 1}
        {:ok, %{status: 200, body: response_comments}}
      end)

    assert {:ok, comments} = ReviewGithub.review_comments_for_test(config, 42, 77, request_fun)

    assert comments == [
             %{
               id: 1,
               path: "lib/space name:literal[1]*.ex",
               line: 12,
               side: "RIGHT",
               start_line: nil,
               start_side: nil,
               body: "Single line",
               head_sha: @head_sha,
               owned: true
             },
             %{
               id: 2,
               path: "lib/null-range.ex",
               line: 13,
               side: "RIGHT",
               start_line: nil,
               start_side: nil,
               body: "Null range",
               head_sha: @head_sha,
               owned: true
             },
             %{
               id: 3,
               path: "lib/range.ex",
               line: 14,
               side: "RIGHT",
               start_line: 10,
               start_side: "RIGHT",
               body: "Range",
               head_sha: @head_sha,
               owned: true
             }
           ]
  end

  test "rejects duplicate, malformed, and unbounded inline comment pages", %{config: config, key: key} do
    no_request = fn _method, _path, _headers, _params, _body -> flunk("invalid review ID must not make a request") end
    assert {:error, :invalid_pull_request_review_id} = ReviewGithub.review_comments_for_test(config, 42, 0, no_request)

    first_page = Enum.map(1..100, fn id -> raw_review_comment(id, "lib/file.ex", id, "RIGHT", @head_sha, "reviewer", "User") end)

    duplicate_request =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42/reviews/77/comments", _headers, params, nil ->
        body = if params["page"] == 1, do: first_page, else: [hd(first_page)]
        {:ok, %{status: 200, body: body}}
      end)

    assert {:error, :duplicate_pull_request_review_comment} =
             ReviewGithub.review_comments_for_test(config, 42, 77, duplicate_request)

    malformed = Map.put(raw_review_comment(101, "lib/file.ex", 101, "RIGHT", @head_sha, "reviewer", "User"), "line", nil)

    malformed_request =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42/reviews/77/comments", _headers, params, nil ->
        body = if params["page"] == 1, do: first_page, else: [malformed]
        {:ok, %{status: 200, body: body}}
      end)

    assert {:error, :invalid_pull_request_review_comment_response} =
             ReviewGithub.review_comments_for_test(config, 42, 77, malformed_request)

    unbounded_request =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42/reviews/77/comments", _headers, params, nil ->
        page = params["page"]
        body = Enum.map(1..100, fn offset -> raw_review_comment((page - 1) * 100 + offset, "lib/file.ex", 1, "RIGHT", @head_sha, "reviewer", "User") end)
        {:ok, %{status: 200, body: body}}
      end)

    assert {:error, :pull_request_review_comments_exceeded} =
             ReviewGithub.review_comments_for_test(config, 42, 77, unbounded_request)
  end

  test "rejects duplicate and malformed reviews rather than returning a partial list", %{config: config, key: key} do
    first_page = Enum.map(1..100, fn id -> raw_review(id, @head_sha, "COMMENTED", "reviewer", "User") end)

    duplicate_request =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42/reviews", _headers, params, nil ->
        body = if params["page"] == 1, do: first_page, else: [hd(first_page)]
        {:ok, %{status: 200, body: body}}
      end)

    assert {:error, :duplicate_pull_request_review} = ReviewGithub.reviews_for_test(config, 42, duplicate_request)

    malformed_request =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42/reviews", _headers, params, nil ->
        body = if params["page"] == 1, do: first_page, else: [Map.delete(raw_review(101, @head_sha, "COMMENTED", "reviewer", "User"), "commit_id")]
        {:ok, %{status: 200, body: body}}
      end)

    assert {:error, _reason} = ReviewGithub.reviews_for_test(config, 42, malformed_request)
  end

  test "stops at the bounded review page limit instead of returning truncated results", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42/reviews", _headers, params, nil ->
        page = params["page"]
        body = Enum.map(1..100, fn offset -> raw_review((page - 1) * 100 + offset, @head_sha, "COMMENTED", "reviewer", "User") end)
        {:ok, %{status: 200, body: body}}
      end)

    assert {:error, :pull_request_reviews_exceeded} = ReviewGithub.reviews_for_test(config, 42, request_fun)
  end

  test "creates a review only when GitHub confirms this bot, commit, state, and body", %{config: config, key: key} do
    payload = review_payload()
    response = raw_review(77, @head_sha, "CHANGES_REQUESTED", @reviewer_login, "Bot", payload["body"])

    request_fun =
      app_request_fun(key, fn :post, "/repos/RigCodeAI/rig/pulls/42/reviews", headers, %{}, body ->
        assert_installation_token!(headers)
        assert body == payload
        {:ok, %{status: 200, body: response}}
      end)

    assert {:ok, review} = ReviewGithub.create_review_for_test(config, 42, payload, request_fun)
    assert review.author_login == @reviewer_login
    assert review.author_type == "Bot"
    assert review.owned
    assert review.head_sha == @head_sha
    assert review.state == "CHANGES_REQUESTED"
    assert review.body == payload["body"]
  end

  test "maps each allowed review event to its required GitHub state", %{config: config, key: key} do
    for {event, state} <- [{"APPROVE", "APPROVED"}, {"REQUEST_CHANGES", "CHANGES_REQUESTED"}, {"COMMENT", "COMMENTED"}] do
      payload = review_payload() |> Map.put("event", event) |> Map.put("comments", [])
      payload = if event == "APPROVE", do: Map.put(payload, "body", ""), else: payload
      response = raw_review(77, @head_sha, state, @reviewer_login, "Bot", payload["body"])

      request_fun =
        app_request_fun(key, fn :post, "/repos/RigCodeAI/rig/pulls/42/reviews", _headers, %{}, body ->
          assert body == payload
          {:ok, %{status: 200, body: response}}
        end)

      assert {:ok, review} = ReviewGithub.create_review_for_test(config, 42, payload, request_fun)
      assert review.state == state
    end
  end

  test "rejects created review responses with another author, commit, or state", %{config: config, key: key} do
    payload = review_payload()

    mismatched_responses = [
      raw_review(77, @head_sha, "CHANGES_REQUESTED", "another-bot[bot]", "Bot", payload["body"]),
      raw_review(77, @other_sha, "CHANGES_REQUESTED", @reviewer_login, "Bot", payload["body"]),
      raw_review(77, @head_sha, "COMMENTED", @reviewer_login, "Bot", payload["body"]),
      raw_review(77, @head_sha, "CHANGES_REQUESTED", "review-app", "User", payload["body"]),
      raw_review(77, @head_sha, "CHANGES_REQUESTED", @reviewer_login, "Bot", "different body")
    ]

    for response <- mismatched_responses do
      request_fun =
        app_request_fun(key, fn :post, "/repos/RigCodeAI/rig/pulls/42/reviews", _headers, %{}, _body ->
          {:ok, %{status: 200, body: response}}
        end)

      assert {:error, :pull_request_review_response_mismatch} =
               ReviewGithub.create_review_for_test(config, 42, payload, request_fun)
    end
  end

  test "rejects malformed or unsafe inline review payloads before making requests", %{config: config} do
    payload = review_payload()
    ranged_comment = hd(payload["comments"])
    no_request = fn _method, _path, _headers, _params, _body -> flunk("invalid payload must not make a GitHub request") end

    invalid_payloads = [
      Map.put(payload, "extra", true),
      Map.delete(payload, "comments"),
      Map.put(payload, :event, "COMMENT"),
      Map.put(payload, "event", "MERGE"),
      Map.put(payload, "commit_id", String.upcase(@head_sha)),
      Map.put(payload, "body", 42),
      Map.put(payload, "body", "invalid\u0000body"),
      Map.put(payload, "comments", [Map.put(ranged_comment, "extra", true)]),
      Map.put(payload, "comments", [Map.put(ranged_comment, "body", "invalid\u0000body")]),
      Map.put(payload, "comments", [Map.put(ranged_comment, "path", "../secret.txt")]),
      Map.put(payload, "comments", [Map.put(ranged_comment, "path", "/absolute/file.txt")]),
      Map.put(payload, "comments", [Map.put(ranged_comment, "path", "src/./file.txt")]),
      Map.put(payload, "comments", [Map.put(ranged_comment, "line", 0)]),
      Map.put(payload, "comments", [Map.put(ranged_comment, "side", "LEFT")]),
      Map.put(payload, "comments", [Map.delete(ranged_comment, "start_side")]),
      Map.put(payload, "comments", [Map.delete(ranged_comment, "start_line")]),
      Map.put(payload, "comments", [Map.put(ranged_comment, "start_line", 13)]),
      Map.put(payload, "comments", [Map.put(ranged_comment, "start_side", "LEFT")]),
      Map.put(payload, "comments", [Map.put(ranged_comment, "line", 2_147_483_648)]),
      Map.put(payload, "comments", [ranged_comment, Map.put(ranged_comment, "line", 0)]),
      Map.put(payload, "comments", [ranged_comment, ranged_comment]),
      Map.put(payload, "comments", List.duplicate(ranged_comment, 31))
    ]

    for invalid <- invalid_payloads do
      assert {:error, _reason} = ReviewGithub.create_review_for_test(config, 42, invalid, no_request)
    end
  end

  defp app_request_fun(key, handler, overrides \\ %{}) do
    parent = self()
    app = Map.get(overrides, :app, %{"id" => 123, "slug" => "review-app"})
    installation = Map.get(overrides, :installation, %{"id" => 2468, "app_id" => 123})
    token = Map.get(overrides, :token, token_response())

    fn method, path, headers, params, body ->
      case path do
        "/app" ->
          assert method == :get
          verify_app_request!(key, method, headers, params, body)
          {:ok, %{status: 200, body: app}}

        "/app/installations/2468" ->
          assert method == :get
          verify_app_request!(key, method, headers, params, body)
          {:ok, %{status: 200, body: installation}}

        "/app/installations/2468/access_tokens" ->
          assert method == :post
          verify_app_request!(key, method, headers, params, body)

          assert body == %{
                   "repositories" => ["rig"],
                   "permissions" => %{
                     "contents" => "read",
                     "pull_requests" => "write",
                     "metadata" => "read"
                   }
                 }

          send(parent, :review_installation_token_requested)
          {:ok, %{status: 201, body: token}}

        _ ->
          assert_installation_token!(headers)
          handler.(method, path, headers, params, body)
      end
    end
  end

  defp verify_app_request!(private_key, method, headers, params, body) do
    assert params == %{}

    if method == :get do
      assert is_nil(body)
    end

    assert {"Authorization", "Bearer " <> jwt} = List.keyfind(headers, "Authorization", 0)
    assert {"X-GitHub-Api-Version", "2022-11-28"} in headers
    [encoded_header, encoded_payload, encoded_signature] = String.split(jwt, ".")
    header = encoded_header |> Base.url_decode64!(padding: false) |> Jason.decode!()
    payload = encoded_payload |> Base.url_decode64!(padding: false) |> Jason.decode!()
    signature = Base.url_decode64!(encoded_signature, padding: false)
    unsigned = encoded_header <> "." <> encoded_payload
    public_key = {:RSAPublicKey, elem(private_key, 2), elem(private_key, 3)}

    assert header == %{"alg" => "RS256", "typ" => "JWT"}
    assert payload["iss"] == "123"
    now = DateTime.utc_now() |> DateTime.to_unix()
    assert payload["iat"] <= now
    assert payload["exp"] - payload["iat"] == 540
    assert :public_key.verify(unsigned, :sha256, signature, public_key)
  end

  defp assert_installation_token!(headers) do
    assert {"Authorization", "Bearer " <> token} = List.keyfind(headers, "Authorization", 0)
    assert token == @token
    assert {"X-GitHub-Api-Version", "2022-11-28"} in headers
  end

  defp token_response do
    %{
      "token" => @token,
      "expires_at" => DateTime.add(DateTime.utc_now(), 3_600, :second) |> DateTime.to_iso8601(),
      "permissions" => %{"contents" => "read", "pull_requests" => "write", "metadata" => "read"},
      "repository_selection" => "selected",
      "repositories" => [%{"id" => 555, "name" => "rig", "full_name" => "RigCodeAI/rig", "private" => true}]
    }
  end

  defp pull_request(number) do
    %{
      "number" => number,
      "html_url" => "https://github.com/RigCodeAI/rig/pull/#{number}",
      "state" => "open",
      "draft" => false,
      "user" => %{"login" => "contributor"},
      "head" => %{
        "ref" => "feature/review",
        "sha" => @head_sha,
        "repo" => %{"full_name" => "RigCodeAI/rig"}
      },
      "base" => %{
        "ref" => "main",
        "sha" => @base_sha,
        "repo" => %{"full_name" => "RigCodeAI/rig"}
      }
    }
  end

  defp raw_review(id, commit, state, login, author_type, body \\ "") do
    %{
      "id" => id,
      "html_url" => review_url(42, id),
      "commit_id" => commit,
      "state" => state,
      "body" => body,
      "user" => %{"login" => login, "type" => author_type}
    }
  end

  defp raw_review_comment(id, path, line, side, commit, login, author_type, body \\ "Inline comment") do
    %{
      "id" => id,
      "path" => path,
      "line" => line,
      "side" => side,
      "body" => body,
      "commit_id" => commit,
      "user" => %{"login" => login, "type" => author_type}
    }
  end

  defp review_url(number, id), do: "https://github.com/RigCodeAI/rig/pull/#{number}#pullrequestreview-#{id}"

  defp review_state(id) do
    case rem(id, 3) do
      0 -> "APPROVED"
      1 -> "COMMENTED"
      _ -> "CHANGES_REQUESTED"
    end
  end

  defp review_payload do
    %{
      "body" => "Please fix the unchecked result before publication.",
      "event" => "REQUEST_CHANGES",
      "commit_id" => @head_sha,
      "comments" => [
        %{
          "body" => "This added line needs a guard.",
          "path" => "lib/space name:literal[1]*.ex",
          "line" => 12,
          "side" => "RIGHT",
          "start_line" => 10,
          "start_side" => "RIGHT"
        }
      ]
    }
  end
end
