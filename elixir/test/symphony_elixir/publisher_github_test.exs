defmodule SymphonyElixir.PublisherGithubTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.PublisherGithub

  setup do
    root = Path.join(System.tmp_dir!(), "publisher-github-test-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false))
    File.mkdir_p!(root)

    key = :public_key.generate_key({:rsa, 2048, 65_537})
    key_path = Path.join(root, "github-app.pem")
    pem = :public_key.pem_encode([:public_key.pem_entry_encode(:RSAPrivateKey, key)])
    File.write!(key_path, pem)
    File.chmod!(key_path, 0o600)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, config: %{app_id: 123, installation_id: "2468", private_key_path: key_path}, key: key, key_path: key_path}
  end

  test "finds a closed pull request and queries all states for the exact branch", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn method, path, _headers, params, _body ->
        assert method == :get
        assert path == "/repos/RigCodeAI/rig/pulls"
        assert params == %{"head" => "RigCodeAI:feature/alpha", "state" => "all", "per_page" => 100, "page" => 1}

        {:ok, %{status: 200, body: [pull_request(17, "feature/alpha", "closed", "preserve my body")]}}
      end)

    assert {:ok,
            %{
              number: 17,
              url: "https://github.com/RigCodeAI/rig/pull/17",
              head_sha: head_sha,
              head_branch: "feature/alpha",
              base_branch: "main",
              state: "closed",
              body: "preserve my body"
            }} = PublisherGithub.find_pr_for_test(config, "feature/alpha", request_fun)

    assert head_sha == String.duplicate("a", 40)
  end

  test "a pull request targeting another base blocks reuse instead of being hidden", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls", _headers, params, _body ->
        refute Map.has_key?(params, "base")
        pr = put_in(pull_request(17, "feature/alpha", "open", "human body"), ["base", "ref"], "release")
        {:ok, %{status: 200, body: [pr]}}
      end)

    assert {:error, :invalid_pull_request_response} = PublisherGithub.find_pr_for_test(config, "feature/alpha", request_fun)
  end

  test "reports duplicate pull requests for one branch", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn _method, _path, _headers, _params, _body ->
        {:ok,
         %{
           status: 200,
           body: [
             pull_request(17, "feature/alpha", "closed", "old"),
             pull_request(18, "feature/alpha", "open", "new")
           ]
         }}
      end)

    assert {:error, :multiple_pull_requests_for_branch} =
             PublisherGithub.find_pr_for_test(config, "feature/alpha", request_fun)
  end

  test "returns nil for a missing branch and preserves slash paths", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn method, path, _headers, _params, _body ->
        assert method == :get
        assert path == "/repos/RigCodeAI/rig/git/ref/heads/feature/with/slash"
        {:ok, %{status: 404, body: %{"message" => "Not Found"}}}
      end)

    assert {:ok, nil} = PublisherGithub.branch_sha_for_test(config, "feature/with/slash", request_fun)
  end

  test "creates pull requests against main with the requested branch", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn method, path, _headers, _params, body ->
        assert method == :post
        assert path == "/repos/RigCodeAI/rig/pulls"
        assert body == %{"title" => "Candidate", "body" => "summary", "head" => "feature/new", "base" => "main"}

        {:ok, %{status: 201, body: pull_request(21, "feature/new", "open", "summary")}}
      end)

    assert {:ok, %{number: 21, state: "open"}} =
             PublisherGithub.create_pr_for_test(config, "feature/new", "Candidate", "summary", request_fun)
  end

  test "marks a lost create response as an ambiguous GitHub request", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :post, "/repos/RigCodeAI/rig/pulls", _headers, _params, _body ->
        {:error, :timeout}
      end)

    assert {:error, {:github_request_failed, :transport}} =
             PublisherGithub.create_pr_for_test(config, "feature/new", "Candidate", "summary", request_fun)
  end

  test "refuses to replace a human-edited pull request body", %{config: config, key: key} do
    parent = self()

    request_fun =
      app_request_fun(key, fn method, path, _headers, _params, _body ->
        send(parent, {:api_request, method, path})
        {:ok, %{status: 200, body: pull_request(25, "feature/edit", "open", "edited by a human")}}
      end)

    assert {:error, :pull_request_body_changed} =
             PublisherGithub.update_pr_for_test(config, 25, "replacement", "old body", request_fun)

    assert_receive {:api_request, :get, "/repos/RigCodeAI/rig/pulls/25"}
    refute_receive {:api_request, :patch, _path}
  end

  test "updates the pull request body only after an exact comparison", %{config: config, key: key} do
    parent = self()

    request_fun =
      app_request_fun(key, fn method, path, _headers, _params, body ->
        send(parent, {:api_request, method, path, body})

        case method do
          :get -> {:ok, %{status: 200, body: pull_request(29, "feature/edit", "open", "old body")}}
          :patch -> {:ok, %{status: 200, body: pull_request(29, "feature/edit", "open", "new body")}}
        end
      end)

    assert {:ok, %{number: 29, body: "new body"}} =
             PublisherGithub.update_pr_for_test(config, 29, "new body", "old body", request_fun)

    assert_receive {:api_request, :get, "/repos/RigCodeAI/rig/pulls/29", nil}
    assert_receive {:api_request, :patch, "/repos/RigCodeAI/rig/pulls/29", %{"body" => "new body"}}
  end

  test "gets an existing pull request", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/31", _headers, _params, _body ->
        {:ok, %{status: 200, body: pull_request(31, "feature/read", "open", nil)}}
      end)

    assert {:ok, %{number: 31, body: nil}} = PublisherGithub.get_pr_for_test(config, 31, request_fun)
  end

  test "rejects a private key with group or world access before any request", %{config: config, key_path: key_path} do
    File.chmod!(key_path, 0o644)
    request_fun = fn _method, _path, _headers, _params, _body -> flunk("GitHub must not be called") end

    assert {:error, :github_private_key_mode_invalid} =
             PublisherGithub.validate_config_for_test(config, request_fun)
  end

  test "requires write permission for contents and pull requests", %{config: config, key: key} do
    response =
      installation_token_response()
      |> put_in(["permissions", "contents"], "read")

    request_fun =
      app_request_fun(
        key,
        fn _method, _path, _headers, _params, _body ->
          flunk("repository requests must not run with a read-only token")
        end,
        response
      )

    assert {:error, :github_installation_permissions_missing} =
             PublisherGithub.validate_config_for_test(config, request_fun)
  end

  test "does not accept a PAT field as an App-token fallback", %{config: config} do
    request_fun = fn _method, _path, _headers, _params, _body -> flunk("GitHub must not be called") end

    assert {:error, :invalid_github_app_config} =
             PublisherGithub.validate_config_for_test(Map.put(config, :token, "personal-access-token"), request_fun)
  end

  defp app_request_fun(key, handler) do
    app_request_fun(key, handler, installation_token_response())
  end

  defp app_request_fun(key, handler, token_response) do
    fn method, path, headers, params, body ->
      if path == "/app/installations/2468/access_tokens" do
        verify_app_request!(key, method, headers, body)
        {:ok, %{status: 201, body: token_response}}
      else
        handler.(method, path, headers, params, body)
      end
    end
  end

  defp verify_app_request!(private_key, :post, headers, body) do
    assert body == %{
             "repositories" => ["rig"],
             "permissions" => %{"contents" => "write", "pull_requests" => "write"}
           }

    assert {"Authorization", "Bearer " <> jwt} = List.keyfind(headers, "Authorization", 0)
    [encoded_header, encoded_payload, encoded_signature] = String.split(jwt, ".")
    header = encoded_header |> Base.url_decode64!(padding: false) |> Jason.decode!()
    payload = encoded_payload |> Base.url_decode64!(padding: false) |> Jason.decode!()
    signature = Base.url_decode64!(encoded_signature, padding: false)
    unsigned = encoded_header <> "." <> encoded_payload
    public_key = {:RSAPublicKey, elem(private_key, 2), elem(private_key, 3)}

    assert header == %{"alg" => "RS256", "typ" => "JWT"}
    assert payload["iss"] == "123"
    assert payload["iat"] <= DateTime.utc_now() |> DateTime.to_unix()
    assert payload["exp"] - payload["iat"] == 540
    assert :public_key.verify(unsigned, :sha256, signature, public_key)
  end

  defp verify_app_request!(_key, _method, _headers, _body), do: flunk("unexpected App token request")

  defp installation_token_response do
    %{
      "token" => "ghs_short_lived_installation_token",
      "expires_at" => DateTime.add(DateTime.utc_now(), 3_600, :second) |> DateTime.to_iso8601(),
      "permissions" => %{"contents" => "write", "pull_requests" => "write", "metadata" => "read"},
      "repository_selection" => "selected",
      "repositories" => [%{"full_name" => "RigCodeAI/rig"}]
    }
  end

  defp pull_request(number, branch, state, body) do
    %{
      "number" => number,
      "html_url" => "https://github.com/RigCodeAI/rig/pull/#{number}",
      "state" => state,
      "body" => body,
      "head" => %{
        "ref" => branch,
        "sha" => String.duplicate("a", 40),
        "repo" => %{"full_name" => "RigCodeAI/rig"}
      },
      "base" => %{
        "ref" => "main",
        "repo" => %{"full_name" => "RigCodeAI/rig"}
      }
    }
  end
end
