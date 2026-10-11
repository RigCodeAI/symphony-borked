defmodule SymphonyElixirWeb.EvidenceAccess do
  @moduledoc """
  Verifies the IAP signed identity header before allowing access to evidence.

  The unsigned `x-goog-authenticated-user-*` headers are never used as identity
  evidence. A successful request receives only the signed subject and email in
  `conn.assigns.evidence_identity`.
  """

  @behaviour Plug

  @iap_issuer "https://cloud.google.com/iap"
  @iap_keys_url "https://www.gstatic.com/iap/verify/public_key-jwk"
  @keys_cache_key {__MODULE__, :iap_public_keys}
  @keys_cache_ms 5 * 60 * 1_000
  @keys_fetch_timeout_ms 5_000
  @max_keys_body_bytes 16 * 1_024
  @max_token_bytes 16 * 1_024
  @max_token_lifetime_seconds 660
  @clock_skew_seconds 30

  @type identity :: %{sub: String.t(), email: String.t()}

  @impl true
  @spec init(keyword()) :: keyword()
  def init(opts), do: opts

  @impl true
  @spec call(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def call(conn, opts) do
    conn =
      conn
      |> secure_response_headers()
      |> Plug.Conn.register_before_send(&secure_response_headers/1)

    case evidence_config(opts) do
      {:ok, config} ->
        with {:ok, settings} <- validate_config(config),
             [token] <- Plug.Conn.get_req_header(conn, "x-goog-iap-jwt-assertion"),
             {:ok, identity} <- authenticate(token, settings, opts) do
          Plug.Conn.assign(conn, :evidence_identity, identity)
        else
          _ -> deny(conn)
        end

      _ ->
        deny(conn)
    end
  end

  defp deny(conn) do
    conn
    |> Plug.Conn.send_resp(403, "Forbidden")
    |> Plug.Conn.halt()
  end

  defp secure_response_headers(conn) do
    conn
    |> Plug.Conn.put_resp_header("cache-control", "private, no-store")
    |> Plug.Conn.put_resp_header("x-content-type-options", "nosniff")
  end

  defp evidence_config(opts) do
    case Keyword.fetch(opts, :config) do
      {:ok, :disabled} ->
        :disabled

      {:ok, {:ok, config}} when is_map(config) ->
        {:ok, config}

      {:ok, config} when is_map(config) ->
        {:ok, config}

      {:ok, _invalid} ->
        {:error, :invalid_config}

      :error ->
        try do
          SymphonyElixir.Config.evidence()
        rescue
          _ -> {:error, :config_unavailable}
        catch
          _, _ -> {:error, :config_unavailable}
        end
    end
  end

  defp validate_config(config) do
    with {:ok, audience} <- config_string(config, "iap_audience"),
         {:ok, domains} <- config_string_list(config, "allowed_domains"),
         {:ok, emails} <- config_string_list(config, "allowed_emails"),
         true <- domains != [] or emails != [] do
      {:ok, %{audience: audience, allowed_domains: domains, allowed_emails: emails}}
    else
      _ -> {:error, :invalid_config}
    end
  rescue
    _ -> {:error, :invalid_config}
  end

  defp config_string(config, key) do
    case Map.get(config, key) do
      value when is_binary(value) and value != "" ->
        if String.valid?(value) and String.trim(value) == value do
          {:ok, value}
        else
          {:error, :invalid_config}
        end

      _ ->
        {:error, :invalid_config}
    end
  end

  defp config_string_list(config, key) do
    case Map.get(config, key, []) do
      values when is_list(values) ->
        if Enum.all?(values, &valid_config_string?/1) do
          {:ok, values}
        else
          {:error, :invalid_config}
        end

      _ ->
        {:error, :invalid_config}
    end
  end

  defp valid_config_string?(value) when is_binary(value) and value != "" do
    String.valid?(value) and String.trim(value) == value
  end

  defp valid_config_string?(_value), do: false

  defp authenticate(token, config, opts) when is_binary(token) do
    with true <- byte_size(token) in 1..@max_token_bytes,
         [header_segment, payload_segment, signature_segment] <- String.split(token, "."),
         {:ok, header} <- decode_json_segment(header_segment),
         true <- header["alg"] == "ES256",
         true <- is_nil(header["crit"]) or header["crit"] == [],
         kid when is_binary(kid) and kid != "" <- header["kid"],
         {:ok, claims} <- decode_json_segment(payload_segment),
         {:ok, signature} <- Base.url_decode64(signature_segment, padding: false),
         true <- byte_size(signature) == 64,
         signing_input = header_segment <> "." <> payload_segment,
         {:ok, keys} <- verification_keys(opts),
         {:ok, public_key} <- Map.fetch(keys, kid),
         true <- verify_es256(signing_input, signature, public_key),
         {:ok, identity} <- validate_claims(claims, config, current_time(opts)) do
      {:ok, identity}
    else
      _ -> {:error, :unauthorized}
    end
  end

  defp decode_json_segment(segment) do
    with {:ok, json} <- Base.url_decode64(segment, padding: false),
         {:ok, value} when is_map(value) <- Jason.decode(json) do
      {:ok, value}
    else
      _ -> {:error, :malformed_token}
    end
  end

  defp validate_claims(claims, config, now) do
    subject = claims["sub"]
    email = claims["email"]
    issued_at = claims["iat"]
    expires_at = claims["exp"]

    with true <- is_binary(subject) and String.trim(subject) != "",
         true <- is_binary(email) and String.trim(email) == email,
         {:ok, email_domain} <- email_domain(email),
         true <- claims["iss"] == @iap_issuer,
         true <- claims["aud"] == config.audience,
         true <- is_integer(issued_at) and is_integer(expires_at),
         true <- expires_at >= now - @clock_skew_seconds,
         true <- issued_at <= now + @clock_skew_seconds,
         true <- expires_at > issued_at,
         true <- expires_at - issued_at <= @max_token_lifetime_seconds,
         true <- allowed_identity?(email, email_domain, claims["hd"], config) do
      {:ok, %{sub: subject, email: email}}
    else
      _ -> {:error, :unauthorized}
    end
  end

  defp email_domain(email) do
    case String.split(email, "@") do
      [local, domain] when local != "" and domain != "" ->
        if String.contains?(domain, " "), do: {:error, :invalid_email}, else: {:ok, domain}

      _ ->
        {:error, :invalid_email}
    end
  end

  defp allowed_identity?(email, email_domain, hosted_domain, config) do
    exact_email? = email in config.allowed_emails

    hosted_domain? =
      is_binary(hosted_domain) and hosted_domain != "" and
        normalized_domain(hosted_domain) == normalized_domain(email_domain) and
        Enum.any?(config.allowed_domains, &(normalized_domain(&1) == normalized_domain(hosted_domain)))

    exact_email? or hosted_domain?
  end

  defp normalized_domain(domain), do: String.downcase(domain)

  defp current_time(opts) do
    case Keyword.get(opts, :now) do
      now when is_integer(now) -> now
      _ -> System.system_time(:second)
    end
  end

  defp verification_keys(opts) do
    case Keyword.fetch(opts, :keys) do
      {:ok, {:error, _reason}} ->
        {:error, :key_service_unavailable}

      {:ok, keys} ->
        normalize_key_set(keys)

      :error ->
        cached_verification_keys()
    end
  end

  defp cached_verification_keys do
    now = System.monotonic_time(:millisecond)

    case :persistent_term.get(@keys_cache_key, nil) do
      {expires_at, keys} when expires_at > now ->
        {:ok, keys}

      _expired_or_missing ->
        with {:ok, keys} <- fetch_verification_keys() do
          :persistent_term.put(@keys_cache_key, {now + @keys_cache_ms, keys})
          {:ok, keys}
        end
    end
  end

  defp fetch_verification_keys do
    ref = make_ref()
    Process.put(ref, false)

    into = fn {:data, data}, {req, resp} ->
      body = if is_binary(resp.body), do: resp.body, else: ""

      if byte_size(body) + byte_size(data) > @max_keys_body_bytes do
        Process.put(ref, true)
        {:halt, {req, resp}}
      else
        {:cont, {req, %{resp | body: body <> data}}}
      end
    end

    result =
      try do
        Req.get(@iap_keys_url,
          into: into,
          raw: true,
          redirect: false,
          retry: false,
          receive_timeout: @keys_fetch_timeout_ms,
          connect_options: [timeout: @keys_fetch_timeout_ms]
        )
      rescue
        _ -> {:error, :key_service_unavailable}
      catch
        _, _ -> {:error, :key_service_unavailable}
      end

    oversized? = Process.delete(ref)

    case {result, oversized?} do
      {{:ok, %{status: 200, body: body}}, false} when is_binary(body) ->
        case Jason.decode(body) do
          {:ok, key_set} -> normalize_key_set(key_set)
          _ -> {:error, :invalid_key_set}
        end

      _ ->
        {:error, :key_service_unavailable}
    end
  end

  defp normalize_key_set(%{"keys" => key_list}) when is_list(key_list), do: normalize_jwks(key_list)

  defp normalize_key_set(key_map) when is_map(key_map) and map_size(key_map) > 0 do
    key_list =
      Enum.map(key_map, fn
        {kid, jwk} when is_binary(kid) and is_map(jwk) -> Map.put_new(jwk, "kid", kid)
        _ -> :invalid
      end)

    if Enum.any?(key_list, &(&1 == :invalid)), do: {:error, :invalid_key_set}, else: normalize_jwks(key_list)
  end

  defp normalize_key_set(_), do: {:error, :invalid_key_set}

  defp normalize_jwks(key_list) when key_list != [] and length(key_list) <= 128 do
    Enum.reduce_while(key_list, {:ok, %{}}, fn jwk, {:ok, keys} ->
      case public_key_from_jwk(jwk) do
        {:ok, {kid, public_key}} when not is_map_key(keys, kid) ->
          {:cont, {:ok, Map.put(keys, kid, public_key)}}

        _ ->
          {:halt, {:error, :invalid_key_set}}
      end
    end)
  end

  defp normalize_jwks(_), do: {:error, :invalid_key_set}

  defp public_key_from_jwk(jwk) when is_map(jwk) do
    with kid when is_binary(kid) and kid != "" <- jwk["kid"],
         "EC" <- jwk["kty"],
         "P-256" <- jwk["crv"],
         true <- optional_jwk_value?(jwk, "alg", "ES256"),
         true <- optional_jwk_value?(jwk, "use", "sig"),
         x_value when is_binary(x_value) <- jwk["x"],
         y_value when is_binary(y_value) <- jwk["y"],
         {:ok, x} <- Base.url_decode64(x_value, padding: false),
         {:ok, y} <- Base.url_decode64(y_value, padding: false),
         true <- byte_size(x) == 32 and byte_size(y) == 32 do
      {:ok, {kid, <<4, x::binary, y::binary>>}}
    else
      _ -> {:error, :invalid_jwk}
    end
  end

  defp public_key_from_jwk(_jwk), do: {:error, :invalid_jwk}

  defp optional_jwk_value?(jwk, key, expected), do: is_nil(jwk[key]) or jwk[key] == expected

  defp verify_es256(signing_input, signature, public_key) do
    with {:ok, der_signature} <- signature_to_der(signature) do
      try do
        :crypto.verify(:ecdsa, :sha256, signing_input, der_signature, [public_key, :secp256r1])
      rescue
        _ -> false
      catch
        _, _ -> false
      end
    else
      _ -> false
    end
  end

  defp signature_to_der(<<r::binary-size(32), s::binary-size(32)>>) do
    r_der = der_integer(r)
    s_der = der_integer(s)
    body = r_der <> s_der
    {:ok, <<0x30, byte_size(body), body::binary>>}
  end

  defp signature_to_der(_), do: {:error, :invalid_signature}

  defp der_integer(value) do
    value = strip_leading_zeroes(value)

    value =
      case value do
        <<first, _::binary>> when first >= 0x80 -> <<0, value::binary>>
        _ -> value
      end

    <<0x02, byte_size(value), value::binary>>
  end

  defp strip_leading_zeroes(<<0, rest::binary>>) when byte_size(rest) > 0, do: strip_leading_zeroes(rest)
  defp strip_leading_zeroes(value), do: value
end
