defmodule SymphonyElixir.ValidationGithubTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.ValidationGithub

  @sha String.duplicate("a", 40)
  @base_sha String.duplicate("b", 40)
  @external_id "run-validation-42"

  setup do
    root = Path.join(System.tmp_dir!(), "validation-github-test-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false))
    File.mkdir_p!(root)

    key = :public_key.generate_key({:rsa, 2048, 65_537})
    key_path = Path.join(root, "github-app.pem")
    pem = :public_key.pem_encode([:public_key.pem_entry_encode(:RSAPrivateKey, key)])
    File.write!(key_path, pem)
    File.chmod!(key_path, 0o600)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, config: %{app_id: 123, installation_id: 2468, private_key_path: key_path}, key: key, key_path: key_path}
  end

  test "validates the installed App before requesting a narrowly scoped token", %{config: config, key: key} do
    request_fun = app_request_fun(key, fn _method, _path, _headers, _params, _body -> flunk("no repository request expected") end)

    assert :ok = ValidationGithub.validate_config_for_test(config, request_fun)
    assert_receive :installation_token_granted
  end

  test "does not grant a token when the installation belongs to another App", %{config: config, key: key} do
    request_fun =
      app_request_fun(
        key,
        fn _method, _path, _headers, _params, _body -> flunk("no repository request expected") end,
        token_response(),
        %{"id" => 2468, "app_id" => 999}
      )

    assert {:error, :github_installation_app_mismatch} = ValidationGithub.validate_config_for_test(config, request_fun)
    refute_receive :installation_token_granted
  end

  test "rejects any status permission and any extra write permission", %{config: config, key: key} do
    for extra_permission <- [%{"statuses" => "write"}, %{"deployments" => "write"}, %{"contents" => "write"}] do
      response = put_in(token_response(), ["permissions"], Map.merge(token_response()["permissions"], extra_permission))
      request_fun = app_request_fun(key, fn _method, _path, _headers, _params, _body -> flunk("repository call must not happen") end, response)

      assert {:error, :github_installation_permissions_invalid} = ValidationGithub.validate_config_for_test(config, request_fun)
    end
  end

  test "requires the requested Checks, Contents, and Pull requests permissions", %{config: config, key: key} do
    for permission <- ["checks", "contents", "pull_requests"] do
      level = if permission == "checks", do: "read", else: "write"
      response = put_in(token_response(), ["permissions", permission], level)
      request_fun = app_request_fun(key, fn _method, _path, _headers, _params, _body -> flunk("repository call must not happen") end, response)

      assert {:error, :github_installation_permissions_invalid} = ValidationGithub.validate_config_for_test(config, request_fun)
    end
  end

  test "gets the pull request repository, base and head details", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42", _headers, %{}, nil ->
        {:ok, %{status: 200, body: pull_request(42)}}
      end)

    assert {:ok,
            %{
              number: 42,
              url: "https://github.com/RigCodeAI/rig/pull/42",
              head_repository_full_name: "RigCodeAI/rig",
              base_repository_full_name: "RigCodeAI/rig",
              head_branch: "feature/validation",
              base_branch: "main",
              head_sha: @sha,
              base_sha: @base_sha,
              state: "open",
              draft: false
            }} = ValidationGithub.get_pr_for_test(config, 42, request_fun)
  end

  test "rejects a malformed pull request instead of omitting its base SHA", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/pulls/42", _headers, %{}, nil ->
        {:ok, %{status: 200, body: put_in(pull_request(42), ["base", "sha"], "invalid")}}
      end)

    assert {:error, :invalid_pull_request_response} = ValidationGithub.get_pr_for_test(config, 42, request_fun)
  end

  test "reads the exact main branch commit SHA", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/git/ref/heads/main", _headers, %{}, nil ->
        {:ok, %{status: 200, body: %{"object" => %{"sha" => @sha, "type" => "commit"}}}}
      end)

    assert {:ok, @sha} = ValidationGithub.base_sha_for_test(config, request_fun)
  end

  test "lists every check run for this App and SHA with a bounded page walk", %{config: config, key: key} do
    first_page = Enum.map(1..100, &check_run(&1, 123, @sha, "run-#{&1}", "in_progress", nil))

    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/commits/#{@sha}/check-runs", _headers, params, nil ->
        assert params["app_id"] == 123
        assert params["filter"] == "all"
        assert params["per_page"] == 100

        case params["page"] do
          1 -> {:ok, %{status: 200, body: %{"total_count" => 101, "check_runs" => first_page}}}
          2 -> {:ok, %{status: 200, body: %{"total_count" => 101, "check_runs" => [check_run(101, 123, @sha, "run-101", "completed", "success")]}}}
        end
      end)

    assert {:ok, check_runs} = ValidationGithub.checks_for_test(config, @sha, request_fun)
    assert length(check_runs) == 101
    assert hd(check_runs).id == 1
    assert List.last(check_runs).id == 101
    assert List.last(check_runs).external_id == "run-101"
  end

  test "fails closed when check-run pagination is too large", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/commits/#{@sha}/check-runs", _headers, _params, nil ->
        {:ok, %{status: 200, body: %{"total_count" => 1001, "check_runs" => []}}}
      end)

    assert {:error, :check_run_results_exceeded} = ValidationGithub.checks_for_test(config, @sha, request_fun)
  end

  test "rejects a check-run response from another App", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/commits/#{@sha}/check-runs", _headers, _params, nil ->
        {:ok, %{status: 200, body: %{"total_count" => 1, "check_runs" => [check_run(9, 999, @sha, @external_id, "completed", "success")]}}}
      end)

    assert {:error, :check_run_app_mismatch} = ValidationGithub.checks_for_test(config, @sha, request_fun)
  end

  test "rejects a check-run response for a different commit SHA", %{config: config, key: key} do
    request_fun =
      app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/commits/#{@sha}/check-runs", _headers, _params, nil ->
        {:ok,
         %{
           status: 200,
           body: %{
             "total_count" => 1,
             "check_runs" => [check_run(9, 123, String.duplicate("c", 40), @external_id, "completed", "success")]
           }
         }}
      end)

    assert {:error, :check_run_sha_mismatch} = ValidationGithub.checks_for_test(config, @sha, request_fun)
  end

  test "creates only the factory validation check and verifies the response", %{config: config, key: key} do
    payload = completed_payload()

    request_fun =
      app_request_fun(key, fn :post, "/repos/RigCodeAI/rig/check-runs", _headers, %{}, body ->
        assert body == payload_to_github(payload)
        {:ok, %{status: 201, body: check_run(77, 123, @sha, @external_id, "completed", "success", payload.details_url, payload_to_github(payload)["output"])}}
      end)

    assert {:ok, %{id: 77, app_id: 123, name: "factory/validation", head_sha: @sha, external_id: @external_id, conclusion: "success"}} =
             ValidationGithub.create_check_for_test(config, payload, request_fun)
  end

  test "creates an in-progress check without a conclusion", %{config: config, key: key} do
    payload = %{
      name: "factory/validation",
      head_sha: @sha,
      external_id: @external_id,
      status: "in_progress"
    }

    request_fun =
      app_request_fun(key, fn :post, "/repos/RigCodeAI/rig/check-runs", _headers, %{}, body ->
        assert body == payload_to_github(payload)
        {:ok, %{status: 201, body: check_run(78, 123, @sha, @external_id, "in_progress", nil)}}
      end)

    assert {:ok, %{id: 78, status: "in_progress", conclusion: nil}} =
             ValidationGithub.create_check_for_test(config, payload, request_fun)
  end

  test "refuses neutral and skipped conclusions before making a GitHub request", %{config: config} do
    request_fun = fn _method, _path, _headers, _params, _body -> flunk("invalid check must not call GitHub") end

    for conclusion <- ["neutral", "skipped"] do
      payload = Map.put(completed_payload(), :conclusion, conclusion)
      assert {:error, :invalid_check_payload} = ValidationGithub.create_check_for_test(config, payload, request_fun)
      assert {:error, :invalid_check_payload} = ValidationGithub.update_check_for_test(config, 77, payload, request_fun)
    end
  end

  test "rejects a create response with a different App identity", %{config: config, key: key} do
    payload = completed_payload()

    request_fun =
      app_request_fun(key, fn :post, "/repos/RigCodeAI/rig/check-runs", _headers, %{}, _body ->
        {:ok, %{status: 201, body: check_run(77, 999, @sha, @external_id, "completed", "success", payload.details_url, payload_to_github(payload)["output"])}}
      end)

    assert {:error, :check_run_identity_mismatch} = ValidationGithub.create_check_for_test(config, payload, request_fun)
  end

  test "updates only an existing check with matching App, SHA, and external ID", %{config: config, key: key} do
    payload = completed_payload()

    request_fun =
      app_request_fun(key, fn method, path, _headers, %{}, body ->
        case {method, path} do
          {:get, "/repos/RigCodeAI/rig/check-runs/77"} ->
            {:ok, %{status: 200, body: check_run(77, 123, @sha, @external_id, "in_progress", nil)}}

          {:patch, "/repos/RigCodeAI/rig/check-runs/77"} ->
            assert body == Map.delete(payload_to_github(payload), "head_sha")

            {:ok,
             %{
               status: 200,
               body: check_run(77, 123, @sha, @external_id, "completed", "success", payload.details_url, payload_to_github(payload)["output"])
             }}
        end
      end)

    assert {:ok, %{id: 77, app_id: 123, head_sha: @sha, external_id: @external_id, status: "completed", conclusion: "success"}} =
             ValidationGithub.update_check_for_test(config, 77, payload, request_fun)
  end

  test "replaces an old external ID when the App, name, and SHA still match", %{config: config, key: key} do
    payload = Map.put(completed_payload(), :external_id, "run-validation-43")

    request_fun =
      app_request_fun(key, fn method, path, _headers, %{}, body ->
        case {method, path} do
          {:get, "/repos/RigCodeAI/rig/check-runs/77"} ->
            {:ok, %{status: 200, body: check_run(77, 123, @sha, @external_id, "completed", "success")}}

          {:patch, "/repos/RigCodeAI/rig/check-runs/77"} ->
            assert body["external_id"] == "run-validation-43"
            {:ok, %{status: 200, body: check_run(77, 123, @sha, "run-validation-43", "completed", "success", payload.details_url, payload_to_github(payload)["output"])}}
        end
      end)

    assert {:ok, %{id: 77, app_id: 123, name: "factory/validation", head_sha: @sha, external_id: "run-validation-43"}} =
             ValidationGithub.update_check_for_test(config, 77, payload, request_fun)
  end

  test "does not patch a check run owned by another App or tied to another App/name/SHA identity", %{config: config, key: key} do
    payload = completed_payload()

    for current <- [
          check_run(77, 999, @sha, @external_id, "in_progress", nil),
          check_run(77, 123, String.duplicate("c", 40), @external_id, "in_progress", nil),
          put_in(check_run(77, 123, @sha, @external_id, "in_progress", nil), ["name"], "other-check")
        ] do
      request_fun =
        app_request_fun(key, fn :get, "/repos/RigCodeAI/rig/check-runs/77", _headers, %{}, nil ->
          {:ok, %{status: 200, body: current}}
        end)

      assert {:error, :check_run_identity_mismatch} = ValidationGithub.update_check_for_test(config, 77, payload, request_fun)
    end
  end

  test "sanitizes transport failures and rejects credential fallbacks", %{config: config} do
    request_fun = fn _method, _path, _headers, _params, _body -> {:error, {:timeout, "secret details"}} end

    assert {:error, {:github_request_failed, :transport}} = ValidationGithub.validate_config_for_test(config, request_fun)

    pat_config = Map.put(config, :token, "personal-access-token")
    no_request = fn _method, _path, _headers, _params, _body -> flunk("invalid config must not call GitHub") end
    assert {:error, :invalid_github_app_config} = ValidationGithub.validate_config_for_test(pat_config, no_request)
  end

  test "rejects a private key with group or world access before any request", %{config: config, key_path: key_path} do
    File.chmod!(key_path, 0o644)
    request_fun = fn _method, _path, _headers, _params, _body -> flunk("GitHub must not be called") end

    assert {:error, :github_private_key_mode_invalid} = ValidationGithub.validate_config_for_test(config, request_fun)
  end

  defp app_request_fun(key, handler, token \\ token_response(), installation \\ %{"id" => 2468, "app_id" => 123}) do
    parent = self()

    fn method, path, headers, params, body ->
      case path do
        "/app/installations/2468" ->
          assert method == :get
          verify_app_request!(key, method, headers, params, body)
          {:ok, %{status: 200, body: installation}}

        "/app/installations/2468/access_tokens" ->
          assert method == :post
          verify_app_request!(key, method, headers, params, body)
          send(parent, :installation_token_granted)
          {:ok, %{status: 201, body: token}}

        _ ->
          handler.(method, path, headers, params, body)
      end
    end
  end

  defp verify_app_request!(private_key, method, headers, params, body) do
    assert params == %{}
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

    case method do
      :get ->
        assert body == nil

      :post ->
        assert body == %{
                 "repositories" => ["rig"],
                 "permissions" => %{
                   "checks" => "write",
                   "contents" => "read",
                   "pull_requests" => "read",
                   "metadata" => "read"
                 }
               }
    end
  end

  defp token_response do
    %{
      "token" => "ghs_short_lived_validation_token",
      "expires_at" => DateTime.add(DateTime.utc_now(), 3_600, :second) |> DateTime.to_iso8601(),
      "permissions" => %{"checks" => "write", "contents" => "read", "pull_requests" => "read", "metadata" => "read"},
      "repository_selection" => "selected",
      "repositories" => [%{"full_name" => "RigCodeAI/rig"}]
    }
  end

  defp pull_request(number) do
    %{
      "number" => number,
      "html_url" => "https://github.com/RigCodeAI/rig/pull/#{number}",
      "state" => "open",
      "draft" => false,
      "head" => %{
        "ref" => "feature/validation",
        "sha" => @sha,
        "repo" => %{"full_name" => "RigCodeAI/rig"}
      },
      "base" => %{
        "ref" => "main",
        "sha" => String.duplicate("b", 40),
        "repo" => %{"full_name" => "RigCodeAI/rig"}
      }
    }
  end

  defp check_run(id, app_id, sha, external_id, status, conclusion, details_url \\ nil, output \\ nil) do
    %{
      "id" => id,
      "app" => %{"id" => app_id},
      "name" => "factory/validation",
      "head_sha" => sha,
      "external_id" => external_id,
      "status" => status,
      "conclusion" => conclusion,
      "details_url" => details_url,
      "output" => output
    }
  end

  defp completed_payload do
    %{
      name: "factory/validation",
      head_sha: @sha,
      external_id: @external_id,
      status: "completed",
      conclusion: "success",
      details_url: "https://github.com/RigCodeAI/rig/actions/runs/42",
      output: %{title: "Validation passed", summary: "All required checks passed."}
    }
  end

  defp payload_to_github(payload) do
    payload
    |> Map.new(fn {key, value} ->
      value = if key == :output, do: Map.new(value, fn {output_key, output_value} -> {to_string(output_key), output_value} end), else: value
      {to_string(key), value}
    end)
  end
end
