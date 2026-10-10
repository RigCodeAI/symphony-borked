defmodule SymphonyElixir.Linear.AgentMappingTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Linear.AgentMapping
  alias SymphonyElixir.{AgentReadiness, Workstream}

  @root Path.expand("../../..", __DIR__)

  test "three exact identities select separate definitions over one workstream" do
    config = config()
    assert {:ok, ^config} = AgentMapping.validate(config)
    assert {:ok, routes} = AgentMapping.routes(config)
    assert length(routes) == 3

    for route <- routes do
      event = %{organization_id: "rig", app_user_id: route["app_user_id"], oauth_client_id: route["oauth_client_id"]}
      assert {:ok, ^route} = AgentMapping.select(event, config)
      assert route["workstream_path"] == config["workstream_path"]
      assert route["workspace_root"] == config["workspace_root"]
      refute Map.has_key?(route, "cloud_agents")
      assert route["client_secret_env"] == "CLIENT_" <> String.upcase(route["agent_id"] |> String.replace("-", "_"))
      assert {:error, :unknown_cloud_agent_identity} = AgentMapping.select(%{event | organization_id: "other"}, config)
      assert {:error, :unknown_cloud_agent_identity} = AgentMapping.select(%{event | oauth_client_id: "other"}, config)
    end
  end

  test "one installed default identity remains supported; internal review cannot be installed" do
    single = Map.delete(config(), "cloud_agents")
    assert {:ok, [_]} = AgentMapping.routes(single)
    assert {:error, :invalid_cloud_agent_mapping} = AgentMapping.validate(Map.put(single, "agent_id", "default-review"))
    assert {:error, :unknown_cloud_agent_identity} = AgentMapping.select(%{organization_id: "rig", app_user_id: nil, oauth_client_id: nil}, single)
  end

  test "each signer can authorize only its own app identity, including stop events" do
    config = config()
    assert {:ok, routes} = AgentMapping.routes(config)
    secrets = Map.new(routes, &{&1["webhook_secret_env"], "secret-" <> &1["app_user_id"]})

    for route <- routes do
      raw = payload(route) |> Jason.encode!()
      signature = :crypto.mac(:hmac, :sha256, secrets[route["webhook_secret_env"]], raw) |> Base.encode16(case: :lower)
      headers = headers(signature)
      assert {:ok, event} = AgentMapping.verify(raw, headers, config, secrets, now_ms: 1_800_000_000_000)
      assert event.verified_app_user_id == route["app_user_id"]
      assert {:ok, ^route} = AgentMapping.select(event, config)

      other = Enum.find(routes, &(&1["app_user_id"] != route["app_user_id"]))
      wrong_signature = :crypto.mac(:hmac, :sha256, secrets[other["webhook_secret_env"]], raw) |> Base.encode16(case: :lower)
      assert {:error, :unknown_cloud_agent_identity} = AgentMapping.verify(raw, headers(wrong_signature), config, secrets, now_ms: 1_800_000_000_000)

      stop =
        payload(route)
        |> Map.put("action", "prompted")
        |> Map.put("agentActivity", %{"id" => "stop-activity", "agentSessionId" => "session", "signal" => "stop", "content" => %{"type" => "prompt", "body" => "stop"}})
        |> Jason.encode!()

      signature = :crypto.mac(:hmac, :sha256, secrets[route["webhook_secret_env"]], stop) |> Base.encode16(case: :lower)
      assert {:ok, %{kind: :stop}} = AgentMapping.verify(stop, headers(signature), config, secrets, now_ms: 1_800_000_000_000)
    end

    assert {:error, :signing_secret_unavailable} = AgentMapping.verify("{}", [], config, %{})
    assert {:error, :invalid_signature} = AgentMapping.verify("{}", headers(String.duplicate("0", 64)), config, secrets)
  end

  test "signed issue updates retain signer provenance without fabricating a delegate" do
    config = Map.delete(config(), "cloud_agents")
    secret = "default-signing"

    raw =
      %{"type" => "Issue", "action" => "update", "organizationId" => "rig", "webhookTimestamp" => 1_800_000_000_000, "data" => %{"id" => "issue"}, "updatedFrom" => %{"delegateId" => "previous"}}
      |> Jason.encode!()

    signature = :crypto.mac(:hmac, :sha256, secret, raw) |> Base.encode16(case: :lower)
    headers = headers(signature) |> List.keyreplace("linear-event", 0, {"linear-event", "Issue"})
    assert {:ok, event} = AgentMapping.verify(raw, headers, config, %{config["webhook_secret_env"] => secret}, now_ms: 1_800_000_000_000)
    assert event.kind == :issue_updated
    assert event.app_user_id == nil
    assert event.verified_app_user_id == config["app_user_id"]
    assert {:error, :unknown_cloud_agent_identity} = AgentMapping.select(event, config)
  end

  test "duplicate resolved signing keys reject even when their references differ" do
    config = config()
    assert {:ok, routes} = AgentMapping.routes(config)
    shared_keys = Map.new(routes, &{&1["webhook_secret_env"], "reused-signing-secret"})
    raw = payload(List.last(routes)) |> Jason.encode!()
    signature = :crypto.mac(:hmac, :sha256, "reused-signing-secret", raw) |> Base.encode16(case: :lower)
    assert {:error, :ambiguous_signing_secrets} = AgentMapping.verify(raw, headers(signature), config, shared_keys, now_ms: 1_800_000_000_000)
  end

  test "ambiguous, malformed or credential-bearing mappings reject" do
    config = config()
    [high, daybreak] = config["cloud_agents"]
    assert {:error, :invalid_cloud_agent_mapping} = AgentMapping.validate(Map.delete(config, "organization_id"))
    assert {:error, :invalid_cloud_agent_mapping} = AgentMapping.validate(Map.put(config, "cloud_agents", [Map.put(high, "client_secret_env", config["webhook_secret_env"])]))

    for key <- ~w(app_user_id oauth_client_id agent_id webhook_secret_env) do
      duplicate = Map.put(high, key, config[key])
      assert {:error, :invalid_cloud_agent_mapping} = AgentMapping.validate(Map.put(config, "cloud_agents", [duplicate, daybreak]))
    end

    for invalid <- [
          nil,
          %{},
          [high, daybreak, high],
          [Map.put(high, "token_env", "TOKEN")],
          [Map.put(high, "client_secret_env", "secret value")],
          [Map.put(high, "webhook_secret_env", high["client_secret_env"])],
          [Map.put(high, "token", "secret value")]
        ] do
      assert {:error, :invalid_cloud_agent_mapping} = AgentMapping.validate(Map.put(config, "cloud_agents", invalid))
    end
  end

  test "binding records exact settings and shared resources while preserving explicit reviewers" do
    definition = definition()
    assert {:ok, default} = AgentMapping.bind(definition, "default-cloud")
    assert {:ok, high} = AgentMapping.bind(definition, "high-effort-cloud")
    assert default.agent_selection.requested.model == "gpt-6-luna"
    assert default.agent_selection.requested.reasoning_effort == "medium"
    assert high.agent_selection.requested.reasoning_effort == "high"
    assert high.agent_selection.requested.daybreak == false
    assert default.agent_selection.resources == high.agent_selection.resources
    refute default.agent_selection.revision == high.agent_selection.revision
    assert high.stages[high.entry].agent == "high-effort-cloud"
    assert high.stages["repair"].agent == "high-effort-cloud"
    assert high.stages["review"].agent == "default-review"
    assert high.definitions == definition.definitions
    assert {:ok, reviewer} = AgentMapping.reviewer(high, high.stages["review"].agent)
    assert reviewer.requested.model == "gpt-6.1-sol"
    assert {:error, :unknown_internal_review_agent} = AgentMapping.reviewer(high, "high-effort-cloud")
    assert {:error, :unknown_cloud_agent_definition} = AgentMapping.bind(definition, "default-review")
  end

  test "Daybreak binding records the request and readiness blocks without downgrade" do
    assert {:ok, bound} = AgentMapping.bind(definition(), "daybreak-cloud")
    assert bound.agent_selection.requested.daybreak == true
    assert bound.agent_selection.requested.model == "gpt-6-sol"
    assert bound.agent_selection.requested.cyber_access_program == "daybreakBlue"
    assert {:error, :daybreak_execution_unverified} = AgentReadiness.dispatch(bound.agents["daybreak-cloud"])
    assert {:error, :daybreak_execution_unverified} = AgentMapping.reassign(run(), "daybreak-cloud", "request-blue")
  end

  test "a reviewer masquerading as a cloud reference cannot receive delegation" do
    definition = definition()
    changed = put_in(definition.agents["high-effort-cloud"], definition.agents["default-review"])
    assert {:error, :unknown_cloud_agent_definition} = AgentMapping.bind(changed, "high-effort-cloud")
  end

  test "recorded safe reassignment preserves task, branch, artifacts, waits and counters" do
    run = %{run() | status: :waiting_for_answer, phase: :waiting_for_answer}
    assert {:ok, updated} = AgentMapping.reassign(run, "high-effort-cloud", "delivery-1")

    for key <- [
          :id,
          :task_id,
          :issue_id,
          :branch,
          :workspace,
          :status,
          :phase,
          :stage_id,
          :outputs,
          :artifacts,
          :attempts,
          :operations,
          :pending_wait,
          :review_repair_rounds,
          :validation_repair_rounds
        ] do
      assert updated[key] == run[key]
    end

    assert updated.definition.agent_selection.agent_id == "high-effort-cloud"
    assert [%{id: "delivery-1", from: %{agent_id: "default-cloud"}, to: %{agent_id: "high-effort-cloud"}, previous_thread_id: "old-thread"}] = updated.agent_transitions
    assert updated.thread_id == nil
    assert updated.session_id == nil
    assert updated.retry_operation_id == nil
    assert {:ok, ^updated} = AgentMapping.reassign(updated, "high-effort-cloud", "delivery-1")
    assert {:error, :agent_transition_conflict} = AgentMapping.reassign(updated, "default-cloud", "delivery-1")
  end

  test "stopped agent clarification restores its pinned wait without launching a replacement" do
    stopped = %{run() | status: :stopped, phase: :stopped, current_attempt_id: "prior-agent-attempt"}
    assert {:ok, restored} = AgentMapping.reassign(stopped, "high-effort-cloud", "question-handoff")
    assert restored.status == :waiting_for_answer
    assert restored.pending_wait == stopped.pending_wait
    assert restored.current_attempt_id == stopped.current_attempt_id
  end

  test "executing, unreachable and incompletely canceled writers prevent reassignment" do
    run = run()

    for status <- [:executing, :reconciling, :complete, :blocked] do
      assert {:error, :run_not_reassignable} = AgentMapping.reassign(%{run | status: status}, "high-effort-cloud", "delivery")
    end

    for operation <- [
          %{status: :executing},
          %{status: :canceled},
          %{status: :completed, reconciliation: "unknown"},
          %{status: :canceled, cancellation: %{termination: :unknown}},
          %{status: :canceled, cancellation: %{termination: :terminated}, reconciliation: "unknown"}
        ] do
      unsafe = %{run | status: :stopped, operations: %{"prior" => operation}}
      assert {:error, :previous_writer_not_reconciled} = AgentMapping.reassign(unsafe, "high-effort-cloud", "delivery")
    end

    stopped = %{run | status: :stopped, pending_wait: nil, operations: %{"prior" => %{status: :canceled, cancellation: %{termination: :terminated}}}}
    assert {:ok, reassigned} = AgentMapping.reassign(stopped, "high-effort-cloud", "delivery")
    assert reassigned.status == :ready
    assert reassigned.current_attempt_id == nil
    assert reassigned.operations == stopped.operations
    assert reassigned.branch == stopped.branch
  end

  test "a reassignment after stopping a human wait restores that same wait" do
    stopped = %{run() | status: :stopped, phase: :stopped, stage_id: "candidate-ready", current_attempt_id: "prior-wait-attempt"}
    assert {:ok, restored} = AgentMapping.reassign(stopped, "high-effort-cloud", "wait-reassignment")
    assert restored.status == :waiting_for_answer
    assert restored.pending_wait == stopped.pending_wait
    assert restored.current_attempt_id == stopped.current_attempt_id
    assert restored.stage_id == stopped.stage_id
  end

  defp definition do
    assert {:ok, definition} = Workstream.load(Path.join(@root, "factory/workstreams/software-change.yaml"), %{"task" => "mapping test"})
    cloud = definition.stages[definition.entry]
    # Additional fixture stages exercise binding without adding the future review pipeline.
    stages = definition.stages |> Map.put("repair", %{cloud | id: "repair"}) |> Map.put("review", %{cloud | id: "review", agent: "default-review"})
    %{definition | stages: stages}
  end

  defp run do
    %{
      id: "run-1",
      task_id: "linear/issue-1",
      issue_id: "issue-1",
      branch: "cycle/issue-1",
      workspace: "/dedicated/issue-1",
      definition: definition(),
      execution: %{authentication_reference: "old", worker_control: %{pinned: true}},
      status: :ready,
      phase: :queued,
      stage_id: "implement",
      current_attempt_id: nil,
      session_id: "old-session",
      thread_id: "old-thread",
      retry_operation_id: "old-retry",
      outputs: %{candidate: "sha"},
      artifacts: %{candidate: "receipt"},
      attempts: [%{id: "prior", status: :completed}],
      operations: %{"prior" => %{status: :completed}},
      pending_wait: %{id: "wait-1", prompt: "approve?"},
      review_repair_rounds: 2,
      validation_repair_rounds: 1
    }
  end

  defp config do
    primary = identity("default-cloud")

    Map.merge(primary, %{
      "organization_id" => "rig",
      "workstream_path" => "/config/software-change.yaml",
      "workspace_root" => "/workspaces",
      "cloud_agents" => [identity("high-effort-cloud"), identity("daybreak-cloud")]
    })
  end

  defp identity(id) do
    env = id |> String.replace("-", "_") |> String.upcase()
    %{"app_user_id" => "app-#{id}", "oauth_client_id" => "client-#{id}", "agent_id" => id, "webhook_secret_env" => "SIGNING_#{env}", "client_secret_env" => "CLIENT_#{env}"}
  end

  defp payload(route) do
    %{
      "type" => "AgentSessionEvent",
      "action" => "created",
      "organizationId" => "rig",
      "webhookTimestamp" => 1_800_000_000_000,
      "appUserId" => route["app_user_id"],
      "oauthClientId" => route["oauth_client_id"],
      "agentSession" => %{"id" => "session", "issueId" => "issue", "appUserId" => route["app_user_id"]}
    }
  end

  defp headers(signature), do: [{"linear-signature", signature}, {"linear-event", "AgentSessionEvent"}, {"linear-delivery", "00000000-0000-4000-8000-000000000001"}]
end
