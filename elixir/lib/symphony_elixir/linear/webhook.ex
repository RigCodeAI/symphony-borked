defmodule SymphonyElixir.Linear.Webhook do
  @moduledoc """
  Verifies Linear webhook requests and reduces supported events to durable identities.
  """

  @max_body_bytes 256 * 1024
  @timestamp_tolerance_ms 60_000
  @activity_signals ["auth", "continue", "select", "stop"]
  @delivery_id_regex ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\z/
  @signature_regex ~r/\A[0-9a-fA-F]{64}\z/

  @type normalized_event :: map()

  @doc """
  Verifies a signed Linear webhook and returns a minimal normalized event.

  Only bounded, redacted prompted message text is retained; other webhook content is omitted.
  `:now_ms` may be supplied in `opts` for deterministic timestamp checks.
  """
  @spec verify(term(), term(), term(), keyword()) :: {:ok, normalized_event()} | {:error, atom()}
  def verify(raw_body, headers, secret, opts \\ []) do
    with :ok <- validate_body(raw_body),
         :ok <- validate_secret(secret),
         {:ok, delivery_id, signature, event_header} <- required_headers(headers),
         :ok <- verify_signature(raw_body, signature, secret),
         {:ok, payload} <- decode_payload(raw_body),
         :ok <- verify_timestamp(payload, opts),
         :ok <- verify_event_header(payload, event_header),
         {:ok, event} <- normalize_event(payload, delivery_id) do
      {:ok, event}
    end
  end

  defp validate_body(:oversized), do: {:error, :body_too_large}
  defp validate_body(raw_body) when is_binary(raw_body) and byte_size(raw_body) <= @max_body_bytes, do: :ok
  defp validate_body(raw_body) when is_binary(raw_body), do: {:error, :body_too_large}
  defp validate_body(_raw_body), do: {:error, :invalid_body}

  defp validate_secret(secret) when is_binary(secret) and byte_size(secret) > 0, do: :ok
  defp validate_secret(_secret), do: {:error, :invalid_secret}

  defp required_headers(headers) when is_list(headers) do
    if Enum.all?(headers, &valid_header?/1) do
      with {:ok, delivery_id} <- exactly_one_header(headers, "linear-delivery"),
           true <- Regex.match?(@delivery_id_regex, delivery_id),
           {:ok, signature} <- exactly_one_header(headers, "linear-signature"),
           {:ok, event_header} <- exactly_one_header(headers, "linear-event"),
           true <- byte_size(event_header) > 0 do
        {:ok, delivery_id, signature, event_header}
      else
        false -> {:error, :malformed_headers}
        {:error, _reason} = error -> error
      end
    else
      {:error, :malformed_headers}
    end
  end

  defp required_headers(_headers), do: {:error, :malformed_headers}

  defp valid_header?({name, value}) when is_binary(name) and is_binary(value), do: true
  defp valid_header?(_header), do: false

  defp exactly_one_header(headers, expected_name) do
    values =
      for {name, value} <- headers,
          String.downcase(name) == expected_name,
          do: value

    case values do
      [value] -> {:ok, value}
      _ -> {:error, :malformed_headers}
    end
  end

  defp verify_signature(raw_body, signature, secret) do
    if Regex.match?(@signature_regex, signature) do
      with {:ok, provided_signature} <- Base.decode16(signature, case: :mixed) do
        computed_signature = :crypto.mac(:hmac, :sha256, secret, raw_body)

        if :crypto.hash_equals(computed_signature, provided_signature) do
          :ok
        else
          {:error, :invalid_signature}
        end
      else
        :error -> {:error, :invalid_signature}
      end
    else
      {:error, :invalid_signature}
    end
  end

  defp decode_payload(raw_body) do
    case Jason.decode(raw_body) do
      {:ok, payload} when is_map(payload) -> {:ok, payload}
      {:ok, _payload} -> {:error, :invalid_payload}
      {:error, _reason} -> {:error, :invalid_payload}
    end
  end

  defp verify_timestamp(payload, opts) when is_list(opts) do
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    timestamp = Map.get(payload, "webhookTimestamp")

    if is_number(now_ms) and is_number(timestamp) and abs(now_ms - timestamp) <= @timestamp_tolerance_ms do
      :ok
    else
      {:error, :invalid_timestamp}
    end
  end

  defp verify_timestamp(_payload, _opts), do: {:error, :invalid_timestamp}

  defp verify_event_header(payload, event_header) do
    if Map.get(payload, "type") == event_header do
      :ok
    else
      {:error, :invalid_payload}
    end
  end

  defp normalize_event(%{"type" => "AgentSessionEvent"} = payload, delivery_id) do
    normalize_agent_session_event(payload, delivery_id)
  end

  defp normalize_event(%{"type" => "AppUserNotification"} = payload, delivery_id) do
    normalize_app_user_notification(payload, delivery_id)
  end

  defp normalize_event(%{"type" => "Issue"} = payload, delivery_id) do
    normalize_issue_event(payload, delivery_id)
  end

  defp normalize_event(_payload, _delivery_id), do: {:error, :unrelated_event}

  defp normalize_agent_session_event(payload, delivery_id) do
    with {:ok, identity} <- required_identity(payload),
         %{"id" => session_id, "issueId" => issue_id, "appUserId" => session_app_user_id} <-
           Map.get(payload, "agentSession"),
         true <-
           valid_id?(session_id) and valid_id?(issue_id) and valid_id?(session_app_user_id) and
             session_app_user_id == identity.app_user_id do
      event =
        identity
        |> event(delivery_id)
        |> Map.merge(%{issue_id: issue_id, session_id: session_id})

      normalize_agent_action(Map.get(payload, "action"), payload, event)
    else
      _ -> {:error, :invalid_payload}
    end
  end

  defp normalize_agent_action("created", _payload, event), do: {:ok, Map.put(event, :kind, :created)}

  defp normalize_agent_action("prompted", payload, event) do
    with %{"id" => activity_id, "agentSessionId" => activity_session_id, "content" => content} <-
           Map.get(payload, "agentActivity"),
         signal <- Map.get(payload["agentActivity"], "signal"),
         true <-
           valid_id?(activity_id) and activity_session_id == event.session_id and
             is_map(content) and content["type"] == "prompt" and is_binary(content["body"]) and
             (is_nil(signal) or signal in @activity_signals) do
      prompted_event = Map.merge(event, %{activity_id: activity_id, signal: signal})

      if signal == "stop" do
        {:ok, Map.merge(prompted_event, %{kind: :stop, reason: :stop_signal})}
      else
        case SymphonyElixir.Linear.Text.safe(content["body"]) do
          {:ok, body} -> {:ok, Map.merge(prompted_event, %{kind: :prompted, body: body})}
          {:error, _} -> {:error, :invalid_payload}
        end
      end
    else
      _ -> {:error, :invalid_payload}
    end
  end

  defp normalize_agent_action(_action, _payload, _event), do: {:error, :unrelated_event}

  defp normalize_app_user_notification(payload, delivery_id) do
    if Map.get(payload, "action") == "issueUnassignedFromYou" do
      with {:ok, identity} <- required_identity(payload),
           %{"type" => "issueUnassignedFromYou"} = notification <- Map.get(payload, "notification"),
           {:ok, issue_id} <- notification_issue_id(notification) do
        event =
          identity
          |> event(delivery_id)
          |> Map.merge(%{kind: :stop, issue_id: issue_id, session_id: nil, reason: :undelegated})

        {:ok, event}
      else
        _ -> {:error, :invalid_payload}
      end
    else
      {:error, :unrelated_event}
    end
  end

  defp notification_issue_id(notification) do
    issue_id = Map.get(notification, "issueId")

    nested_issue_id =
      case Map.get(notification, "issue") do
        %{"id" => id} -> id
        _ -> nil
      end

    cond do
      valid_id?(issue_id) and (is_nil(nested_issue_id) or issue_id == nested_issue_id) -> {:ok, issue_id}
      is_nil(issue_id) and valid_id?(nested_issue_id) -> {:ok, nested_issue_id}
      true -> {:error, :invalid_payload}
    end
  end

  defp normalize_issue_event(payload, delivery_id) do
    case {Map.get(payload, "action"), Map.get(payload, "updatedFrom")} do
      {"update", updated_from} when is_map(updated_from) ->
        if Map.has_key?(updated_from, "delegateId") or Map.has_key?(updated_from, "stateId") do
          with {:ok, identity} <- required_identity(payload),
               %{"id" => issue_id} <- Map.get(payload, "data"),
               true <- valid_id?(issue_id) do
            event =
              identity
              |> event(delivery_id)
              |> Map.merge(%{
                kind: :issue_updated,
                issue_id: issue_id,
                session_id: nil,
                app_user_id: nil,
                oauth_client_id: nil
              })

            {:ok, event}
          else
            _ -> {:error, :invalid_payload}
          end
        else
          {:error, :unrelated_event}
        end

      _ ->
        {:error, :unrelated_event}
    end
  end

  defp required_identity(payload) do
    case {
      Map.get(payload, "organizationId"),
      Map.get(payload, "webhookTimestamp"),
      Map.get(payload, "appUserId"),
      Map.get(payload, "oauthClientId")
    } do
      {organization_id, timestamp, app_user_id, oauth_client_id}
      when is_binary(organization_id) and byte_size(organization_id) > 0 and is_number(timestamp) and
             is_binary(app_user_id) and byte_size(app_user_id) > 0 and is_binary(oauth_client_id) and
             byte_size(oauth_client_id) > 0 ->
        {:ok,
         %{
           organization_id: organization_id,
           timestamp: timestamp,
           app_user_id: app_user_id,
           oauth_client_id: oauth_client_id
         }}

      {organization_id, timestamp, nil, nil}
      when is_binary(organization_id) and byte_size(organization_id) > 0 and is_number(timestamp) ->
        {:ok, %{organization_id: organization_id, timestamp: timestamp, app_user_id: nil, oauth_client_id: nil}}

      _ ->
        {:error, :invalid_payload}
    end
  end

  defp event(identity, delivery_id) do
    Map.merge(identity, %{id: delivery_id, activity_id: nil})
  end

  defp valid_id?(value), do: is_binary(value) and byte_size(value) > 0
end
