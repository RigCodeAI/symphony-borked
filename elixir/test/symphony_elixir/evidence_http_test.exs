defmodule SymphonyElixir.EvidenceHttpTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{Evidence, WorkstreamRun}
  alias SymphonyElixirWeb.{Endpoint, EvidenceAccess}

  @audience "/projects/292978199748/global/backendServices/123456789"

  setup do
    root = Path.join(System.tmp_dir!(), "evidence-http-#{System.unique_integer([:positive])}")
    workspace_root = Path.join(root, "workspaces")
    evidence_root = Path.join(root, "private-evidence")
    File.mkdir_p!(root)

    workflow_path = Workflow.workflow_file_path()
    write_workflow_file!(workflow_path, workspace_root: workspace_root)
    append_evidence_config!(workflow_path, evidence_root)
    assert :ok = WorkflowStore.force_reload()

    assert {:ok, evidence_config} = Config.evidence()
    assert evidence_config["root"] == evidence_root
    refute String.starts_with?(evidence_root, workspace_root <> "/")

    if Process.whereis(Evidence) do
      raise "Evidence HTTP test requires ownership of the named evidence store"
    end

    start_supervised!({Evidence, [config: {:ok, evidence_config}, receipt_uid: File.stat!(root).uid]})

    endpoint_config = Application.get_env(:symphony_elixir, Endpoint)
    cache_key = {EvidenceAccess, :iap_public_keys}
    missing_cache = make_ref()
    previous_cache = :persistent_term.get(cache_key, missing_cache)

    {public_key, private_key} = :crypto.generate_key(:ecdh, :secp256r1)
    <<4, x::binary-size(32), y::binary-size(32)>> = public_key
    kid = "evidence-http-test-key"

    :persistent_term.put(
      cache_key,
      {System.monotonic_time(:millisecond) + 300_000, %{kid => <<4, x::binary, y::binary>>}}
    )

    start_supervised!({HttpServer, [port: 0, host: "127.0.0.1"]})

    on_exit(fn ->
      write_workflow_file!(workflow_path)
      restore_endpoint_config(endpoint_config)

      if previous_cache === missing_cache do
        :persistent_term.erase(cache_key)
      else
        :persistent_term.put(cache_key, previous_cache)
      end

      File.rm_rf!(root)
    end)

    %{base_url: "http://127.0.0.1:#{HttpServer.bound_port()}", kid: kid, private_key: private_key}
  end

  test "private task routes require signed IAP identity and serve immutable evidence", context do
    task_id = "linear/<script>alert(1)</script>"
    run = fixture_run("evidence-http-run", task_id)
    event = %{"kind" => "observed", "text" => "<script>alert(1)</script>"}
    assert :ok = Evidence.snapshot(run, "created", event)
    assert :ok = Evidence.artifact(run.id, "fixture.txt", "immutable artifact fixture\n")

    key = Evidence.task_key(task_id)
    assert {:ok, [first_view]} = Evidence.task(key)
    original_revision = first_view.revision
    original_artifact = Enum.find(first_view.artifacts, &String.starts_with?(&1["name"], "artifact-"))
    assert is_map(original_artifact)

    assert :ok = Evidence.artifact(run.id, "later-fixture.txt", "later artifact fixture\n")
    assert {:ok, [latest_view]} = Evidence.task(key)
    refute latest_view.revision == original_revision

    authorized = signed_jwt(context.private_key, context.kid, claims("alice@rig.ai", "rig.ai"))
    headers = iap_headers(authorized)

    index = Req.get!(context.base_url <> "/tasks", headers: headers, retry: false)
    assert index.status == 200
    assert_private_response(index)
    assert index.body =~ "&lt;script&gt;alert(1)&lt;/script&gt;"
    refute index.body =~ "<script>alert(1)</script>"

    task = Req.get!(context.base_url <> "/tasks/" <> key, headers: headers, retry: false)
    assert task.status == 200
    assert_private_response(task)
    assert task.body =~ "&lt;script&gt;alert(1)&lt;/script&gt;"
    refute task.body =~ "<script>alert(1)</script>"

    artifact_url =
      context.base_url <> "/tasks/" <> key <> "/artifacts/" <> original_revision <> "/" <> original_artifact["name"]

    artifact = Req.get!(artifact_url, headers: headers, retry: false)
    assert artifact.status == 200
    assert_private_response(artifact)
    assert artifact.body == "immutable artifact fixture\n"
    assert String.starts_with?(hd(artifact.headers["content-type"]), "text/plain")

    foreign_key = Evidence.task_key("linear/another-task")
    foreign_artifact = Req.get!(String.replace(artifact_url, "/tasks/" <> key <> "/", "/tasks/" <> foreign_key <> "/"), headers: headers, retry: false)
    assert foreign_artifact.status == 404
    assert_private_response(foreign_artifact)

    unsigned = Req.get!(context.base_url <> "/tasks", headers: [{"x-goog-authenticated-user-email", "alice@rig.ai"}], retry: false)
    assert unsigned.status == 403
    assert unsigned.body == "Forbidden"
    assert_private_response(unsigned)

    unauthorized = signed_jwt(context.private_key, context.kid, claims("mallory@outside.example", "outside.example"))
    denied = Req.get!(context.base_url <> "/tasks", headers: iap_headers(unauthorized), retry: false)
    assert denied.status == 403
    assert denied.body == "Forbidden"
    assert_private_response(denied)
    refute denied.body =~ task_id
  end

  defp append_evidence_config!(workflow_path, evidence_root) do
    content = File.read!(workflow_path)
    [front_matter, prompt] = String.split(content, "\n---\n", parts: 2)

    evidence_yaml =
      [
        "evidence:",
        "  root: #{Jason.encode!(evidence_root)}",
        "  base_url: \"https://evidence.example.com\"",
        "  iap_audience: #{Jason.encode!(@audience)}",
        "  allowed_domains: [\"rig.ai\"]",
        "  allowed_emails: []"
      ]
      |> Enum.join("\n")

    File.write!(workflow_path, front_matter <> "\n" <> evidence_yaml <> "\n---\n" <> prompt)
  end

  defp restore_endpoint_config(nil), do: Application.delete_env(:symphony_elixir, Endpoint)
  defp restore_endpoint_config(config), do: Application.put_env(:symphony_elixir, Endpoint, config)

  defp fixture_run(id, task_id) do
    definition = %{
      inputs: [],
      input_values: %{},
      entry: "implement",
      agents: %{},
      stages: %{},
      definitions: %{"workstream.yaml" => %{sha256: "http-fixture-definition", text: "http fixture instructions"}}
    }

    WorkstreamRun.new(task_id, definition, "/outside-workspace-fixture", [branch: "cycle/evidence-http"], %{})
    |> Map.put(:id, id)
  end

  defp claims(email, hosted_domain) do
    now = System.system_time(:second)

    %{
      "iss" => "https://cloud.google.com/iap",
      "aud" => @audience,
      "iat" => now,
      "exp" => now + 600,
      "sub" => "evidence-http-subject",
      "email" => email,
      "hd" => hosted_domain
    }
  end

  defp iap_headers(token), do: [{"x-goog-iap-jwt-assertion", token}]

  defp signed_jwt(private_key, kid, claims) do
    header = %{"alg" => "ES256", "kid" => kid}
    encoded_header = header |> Jason.encode!() |> Base.url_encode64(padding: false)
    encoded_claims = claims |> Jason.encode!() |> Base.url_encode64(padding: false)
    signing_input = encoded_header <> "." <> encoded_claims

    signature =
      :crypto.sign(:ecdsa, :sha256, signing_input, [private_key, :secp256r1])
      |> der_signature_to_raw()
      |> Base.url_encode64(padding: false)

    signing_input <> "." <> signature
  end

  defp der_signature_to_raw(<<r::binary-size(32), s::binary-size(32)>>), do: r <> s

  defp der_signature_to_raw(<<0x30, _sequence_length, body::binary>>) do
    <<0x02, r_length, r::binary-size(r_length), 0x02, s_length, s::binary-size(s_length)>> = body
    <<pad_integer(r)::binary-size(32), pad_integer(s)::binary-size(32)>>
  end

  defp pad_integer(<<0, rest::binary>>), do: pad_integer(rest)
  defp pad_integer(value) when byte_size(value) <= 32, do: :binary.copy(<<0>>, 32 - byte_size(value)) <> value

  defp assert_private_response(response) do
    assert response.headers["cache-control"] == ["private, no-store"]
    assert response.headers["x-content-type-options"] == ["nosniff"]
  end
end
