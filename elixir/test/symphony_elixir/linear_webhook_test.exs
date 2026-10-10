defmodule SymphonyElixir.Linear.WebhookTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Linear.Webhook

  @secret "test-signing-secret"
  @now_ms 1_700_000_000_000
  @delivery_id "550e8400-e29b-41d4-a716-446655440000"

  test "normalizes a created agent session to identities only" do
    payload = agent_session_payload("created")

    assert {:ok, event} = verify_payload(payload)
    assert event.id == @delivery_id
    assert event.kind == :created
    assert event.issue_id == "issue-1"
    assert event.session_id == "session-1"
    assert event.app_user_id == "app-user-1"
    assert event.oauth_client_id == "oauth-client-1"
    assert event.organization_id == "org-1"
    assert event.timestamp == @now_ms
    refute Map.has_key?(event, :prompt_context)
  end

  test "normalizes prompted activity with bounded human reply text" do
    payload =
      agent_session_payload("prompted")
      |> Map.put("agentActivity", %{
        "id" => "activity-1",
        "agentSessionId" => "session-1",
        "content" => %{"type" => "prompt", "body" => "Please keep this out of storage"},
        "signal" => "continue"
      })

    assert {:ok, event} = verify_payload(payload)
    assert event.kind == :prompted
    assert event.activity_id == "activity-1"
    assert event.signal == "continue"
    assert event.body == "Please keep this out of storage"
    refute Map.has_key?(event, :prompt)
  end

  test "agent activities cannot loop back as human prompts" do
    payload =
      agent_session_payload("prompted")
      |> Map.put("agentActivity", %{
        "id" => "agent-update",
        "agentSessionId" => "session-1",
        "content" => %{"type" => "thought", "body" => "Agent progress"}
      })

    assert {:error, :invalid_payload} = verify_payload(payload)
  end

  test "prompt bodies are bounded and recognized credentials redacted before receipt" do
    base = agent_session_payload("prompted")
    activity = %{"id" => "user-reply", "agentSessionId" => "session-1", "content" => %{"type" => "prompt", "body" => "do not store sk-testsecret12345"}}
    assert {:ok, %{body: "do not store [REDACTED]"}} = verify_payload(Map.put(base, "agentActivity", activity))
    oversized = put_in(activity["content"]["body"], String.duplicate("x", 16_385))
    assert {:error, :invalid_payload} = verify_payload(Map.put(base, "agentActivity", oversized))
  end

  test "normalizes a prompted stop signal and retains the signal" do
    payload =
      agent_session_payload("prompted")
      |> Map.put("agentActivity", %{
        "id" => "activity-stop",
        "agentSessionId" => "session-1",
        "content" => %{"type" => "prompt", "body" => "Stop this run"},
        "signal" => "stop"
      })

    assert {:ok, event} = verify_payload(payload)
    assert event.kind == :stop
    assert event.reason == :stop_signal
    assert event.signal == "stop"
    assert event.activity_id == "activity-stop"
    refute Map.has_key?(event, :body)
    assert {:ok, %{kind: :stop}} = verify_payload(put_in(payload["agentActivity"]["content"]["body"], ""))
  end

  test "rejects a prompted activity for another session" do
    payload =
      agent_session_payload("prompted")
      |> Map.put("agentActivity", %{
        "id" => "activity-1",
        "agentSessionId" => "another-session",
        "content" => %{"type" => "prompt", "body" => "Continue"}
      })

    assert {:error, :invalid_payload} = verify_payload(payload)
  end

  test "normalizes a documented unassignment notification as a stop" do
    payload = %{
      "type" => "AppUserNotification",
      "action" => "issueUnassignedFromYou",
      "appUserId" => "app-user-1",
      "oauthClientId" => "oauth-client-1",
      "organizationId" => "org-1",
      "webhookTimestamp" => @now_ms,
      "notification" => %{
        "type" => "issueUnassignedFromYou",
        "issueId" => "issue-1"
      }
    }

    assert {:ok, event} = verify_payload(payload)
    assert event.kind == :stop
    assert event.reason == :undelegated
    assert event.issue_id == "issue-1"
    assert event.session_id == nil
    assert event.app_user_id == "app-user-1"
  end

  test "accepts an unassignment notification with the nested issue identity" do
    payload = %{
      "type" => "AppUserNotification",
      "action" => "issueUnassignedFromYou",
      "appUserId" => "app-user-1",
      "oauthClientId" => "oauth-client-1",
      "organizationId" => "org-1",
      "webhookTimestamp" => @now_ms,
      "notification" => %{
        "type" => "issueUnassignedFromYou",
        "issue" => %{"id" => "issue-1"}
      }
    }

    assert {:ok, event} = verify_payload(payload)
    assert event.issue_id == "issue-1"
    assert event.reason == :undelegated
  end

  test "normalizes issue updates that change delegation or state" do
    payload = %{
      "type" => "Issue",
      "action" => "update",
      "organizationId" => "org-1",
      "webhookTimestamp" => @now_ms,
      "updatedFrom" => %{"delegateId" => nil},
      "data" => %{"id" => "issue-1"}
    }

    assert {:ok, event} = verify_payload(payload)
    assert event.kind == :issue_updated
    assert event.issue_id == "issue-1"
    assert event.session_id == nil
    assert event.app_user_id == nil
    assert event.oauth_client_id == nil
  end

  test "rejects issue updates that do not change delegation or state" do
    payload = %{
      "type" => "Issue",
      "action" => "update",
      "organizationId" => "org-1",
      "webhookTimestamp" => @now_ms,
      "updatedFrom" => %{"title" => "Old title"},
      "data" => %{"id" => "issue-1"}
    }

    assert {:error, :unrelated_event} = verify_payload(payload)
  end

  test "rejects unrelated event types" do
    payload = %{"type" => "Comment", "action" => "create", "webhookTimestamp" => @now_ms}

    assert {:error, :unrelated_event} = verify_payload(payload)
  end

  test "rejects a request with a duplicate required header" do
    payload = agent_session_payload("created")
    body = Jason.encode!(payload)
    headers = signed_headers(body, payload["type"]) ++ [{"Linear-Delivery", @delivery_id}]

    assert {:error, :malformed_headers} = Webhook.verify(body, headers, @secret, now_ms: @now_ms)
  end

  test "rejects an incorrect HMAC signature before decoding the payload" do
    payload = agent_session_payload("created")
    body = Jason.encode!(payload)
    headers = signed_headers(body, payload["type"])

    assert {:error, :invalid_signature} =
             Webhook.verify(body <> " ", headers, @secret, now_ms: @now_ms)
  end

  test "rejects a header that does not match the signed body type" do
    payload = agent_session_payload("created")
    body = Jason.encode!(payload)
    headers = signed_headers(body, "Issue")

    assert {:error, :invalid_payload} = Webhook.verify(body, headers, @secret, now_ms: @now_ms)
  end

  test "rejects stale timestamps and accepts timestamps within the boundary" do
    stale = agent_session_payload("created", @now_ms - 60_001)
    boundary = agent_session_payload("created", @now_ms + 60_000)

    assert {:error, :invalid_timestamp} = verify_payload(stale)
    assert {:ok, _event} = verify_payload(boundary)
  end

  test "rejects bodies larger than 256 KiB" do
    body = String.duplicate("x", 256 * 1024 + 1)

    assert {:error, :body_too_large} = Webhook.verify(body, [], @secret, now_ms: @now_ms)
  end

  test "rejects a missing signing secret" do
    assert {:error, :invalid_secret} = Webhook.verify("{}", [], nil, now_ms: @now_ms)
  end

  defp verify_payload(payload) do
    body = Jason.encode!(payload)
    headers = signed_headers(body, payload["type"])
    Webhook.verify(body, headers, @secret, now_ms: @now_ms)
  end

  defp signed_headers(body, event_type) do
    signature = :crypto.mac(:hmac, :sha256, @secret, body) |> Base.encode16(case: :lower)

    [
      {"linear-delivery", @delivery_id},
      {"linear-signature", signature},
      {"linear-event", event_type}
    ]
  end

  defp agent_session_payload(action, timestamp \\ @now_ms) do
    %{
      "type" => "AgentSessionEvent",
      "action" => action,
      "appUserId" => "app-user-1",
      "oauthClientId" => "oauth-client-1",
      "organizationId" => "org-1",
      "webhookTimestamp" => timestamp,
      "agentSession" => %{
        "id" => "session-1",
        "issueId" => "issue-1",
        "appUserId" => "app-user-1"
      }
    }
  end
end
