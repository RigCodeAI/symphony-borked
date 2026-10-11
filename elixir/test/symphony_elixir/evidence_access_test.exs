defmodule SymphonyElixir.EvidenceAccessTest do
  use ExUnit.Case, async: true

  alias Plug.Conn
  alias SymphonyElixirWeb.EvidenceAccess

  @now 1_800_000_000
  @audience "/projects/292978199748/global/backendServices/123456789"

  setup do
    {public_key, private_key} = :crypto.generate_key(:ecdh, :secp256r1)
    <<4, x::binary-size(32), y::binary-size(32)>> = public_key
    kid = "test-iap-key"

    jwk = %{
      "kid" => kid,
      "kty" => "EC",
      "crv" => "P-256",
      "alg" => "ES256",
      "use" => "sig",
      "x" => Base.url_encode64(x, padding: false),
      "y" => Base.url_encode64(y, padding: false)
    }

    config = %{
      "root" => "/tmp/evidence",
      "base_url" => "https://evidence.example.com",
      "iap_audience" => @audience,
      "allowed_domains" => ["example.com"],
      "allowed_emails" => []
    }

    {:ok, config: config, kid: kid, keys: %{kid => jwk}, private_key: private_key, jwk: jwk}
  end

  test "accepts a valid signed identity and ignores the unsigned email header", context do
    token = signed_jwt(context.private_key, valid_claims(), %{"alg" => "ES256", "kid" => context.kid})

    conn =
      request(token, context, extra_headers: [{"x-goog-authenticated-user-email", "attacker@outside.example"}])

    assert conn.assigns.evidence_identity == %{sub: "subject-123", email: "alice@example.com"}
    refute conn.halted
    assert Conn.get_resp_header(conn, "cache-control") == ["private, no-store"]
    assert Conn.get_resp_header(conn, "x-content-type-options") == ["nosniff"]
  end

  test "allows an exact configured email when no hosted domain is present", context do
    claims = Map.delete(valid_claims(), "hd")
    config = %{context.config | "allowed_domains" => [], "allowed_emails" => ["alice@example.com"]}
    token = signed_jwt(context.private_key, claims, %{"alg" => "ES256", "kid" => context.kid})

    conn = request(token, %{context | config: config})

    assert conn.assigns.evidence_identity == %{sub: "subject-123", email: "alice@example.com"}
    refute conn.halted
  end

  test "rejects unsigned identity headers and duplicate IAP assertions", context do
    unsigned =
      request(nil, context, extra_headers: [{"x-goog-authenticated-user-email", "alice@example.com"}])

    assert_forbidden(unsigned)

    token = signed_jwt(context.private_key, valid_claims(), %{"alg" => "ES256", "kid" => context.kid})
    conn = Plug.Test.conn(:get, "/evidence")
    conn = %{conn | req_headers: [{"x-goog-iap-jwt-assertion", token}, {"x-goog-iap-jwt-assertion", token}]}
    conn = EvidenceAccess.call(conn, test_opts(context))

    assert_forbidden(conn)
  end

  test "rejects malformed tokens, invalid signatures, and invalid signed claims", context do
    claims = valid_claims()
    header = %{"alg" => "ES256", "kid" => context.kid}
    valid = signed_jwt(context.private_key, claims, header)

    invalid_tokens = [
      {"malformed", "not.a.jwt"},
      {"bad base64", "%%.%%.%%"},
      {"bad signature", tamper_signature(valid)},
      {"bad algorithm", signed_jwt(context.private_key, claims, Map.put(header, "alg", "HS256"))},
      {"unknown key id", signed_jwt(context.private_key, claims, Map.put(header, "kid", "unknown"))},
      {"wrong audience", signed_jwt(context.private_key, Map.put(claims, "aud", "/projects/wrong"), header)},
      {"expired", signed_jwt(context.private_key, Map.put(claims, "exp", @now - 31), header)},
      {"too long lived", signed_jwt(context.private_key, Map.put(claims, "exp", @now + 661), header)},
      {"wrong hosted domain", signed_jwt(context.private_key, Map.put(claims, "hd", "outside.example"), header)},
      {"email and hosted domain mismatch", signed_jwt(context.private_key, Map.put(claims, "email", "alice@outside.example"), header)}
    ]

    for {label, token} <- invalid_tokens do
      conn = request(token, context)
      assert conn.status == 403, label
      assert conn.resp_body == "Forbidden", label
      assert conn.assigns[:evidence_identity] == nil, label
    end
  end

  test "fails closed when evidence access is disabled or the key service fails", context do
    disabled =
      Plug.Test.conn(:get, "/evidence")
      |> EvidenceAccess.call(config: :disabled, keys: context.keys, now: @now)

    assert_forbidden(disabled)

    token = signed_jwt(context.private_key, valid_claims(), %{"alg" => "ES256", "kid" => context.kid})

    unavailable =
      request(token, context, keys: {:error, :unavailable})

    assert_forbidden(unavailable)
  end

  defp request(token, context, opts \\ []) do
    conn = Plug.Test.conn(:get, "/evidence")

    conn =
      if is_binary(token) do
        Conn.put_req_header(conn, "x-goog-iap-jwt-assertion", token)
      else
        conn
      end

    conn =
      Enum.reduce(Keyword.get(opts, :extra_headers, []), conn, fn {name, value}, conn ->
        Conn.put_req_header(conn, name, value)
      end)

    EvidenceAccess.call(conn, test_opts(context, opts))
  end

  defp test_opts(context, overrides \\ []) do
    [config: context.config, keys: context.keys, now: @now]
    |> Keyword.merge(Keyword.drop(overrides, [:extra_headers]))
  end

  defp assert_forbidden(conn) do
    assert conn.status == 403
    assert conn.resp_body == "Forbidden"
    assert conn.halted
    assert Conn.get_resp_header(conn, "cache-control") == ["private, no-store"]
    assert Conn.get_resp_header(conn, "x-content-type-options") == ["nosniff"]
    assert conn.assigns[:evidence_identity] == nil
  end

  defp valid_claims do
    %{
      "iss" => "https://cloud.google.com/iap",
      "aud" => @audience,
      "iat" => @now,
      "exp" => @now + 600,
      "sub" => "subject-123",
      "email" => "alice@example.com",
      "hd" => "example.com"
    }
  end

  defp signed_jwt(private_key, claims, header) do
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

  defp pad_integer(value) when byte_size(value) <= 32 do
    :binary.copy(<<0>>, 32 - byte_size(value)) <> value
  end

  defp tamper_signature(token) do
    [header, claims, signature] = String.split(token, ".")
    {:ok, raw} = Base.url_decode64(signature, padding: false)
    <<first, rest::binary>> = raw
    altered = <<:erlang.bxor(first, 1), rest::binary>> |> Base.url_encode64(padding: false)
    header <> "." <> claims <> "." <> altered
  end
end
