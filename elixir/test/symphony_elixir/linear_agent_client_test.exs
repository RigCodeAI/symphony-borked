defmodule SymphonyElixir.Linear.AgentClientTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Linear.Client

  test "delegated issue fetch is unfiltered and keeps the human assignee" do
    request_fun = fn payload, _headers ->
      send(self(), {:graphql_request, payload})

      {:ok,
       %{
         status: 200,
         body: %{
           "data" => %{
             "issue" => %{
               "id" => "issue-1",
               "identifier" => "RIG-1",
               "title" => "Delegated work",
               "description" => "Do the work",
               "state" => %{"name" => "In Progress", "type" => "started"},
               "assignee" => %{"id" => "human-1"},
               "delegate" => %{"id" => "agent-1"},
               "team" => %{"id" => "team-1"},
               "labels" => %{"nodes" => [%{"name" => "Factory"}]},
               "url" => "https://linear.test/issue/RIG-1",
               "branchName" => "factory/rig-1"
             }
           }
         }
       }}
    end

    assert {:ok, issue} = Client.fetch_delegated_issue("issue-1", client_opts(request_fun))

    assert issue.id == "issue-1"
    assert issue.assignee_id == "human-1"
    assert issue.delegate_id == "agent-1"
    assert issue.team_id == "team-1"
    assert issue.state_type == "started"
    assert issue.labels == ["factory"]
    assert issue.dispatchable

    assert_receive {:graphql_request, %{"query" => query, "variables" => %{"id" => "issue-1"}}}
    assert query =~ "issue(id: $id)"
    refute query =~ "filter:"
    refute query =~ "projectSlug"
    refute query =~ "assigneeId"
  end

  test "older issue payloads keep their existing assignee routing behavior" do
    raw_issue = %{
      "id" => "issue-1",
      "identifier" => "RIG-1",
      "title" => "Existing poll result",
      "state" => %{"name" => "Todo"},
      "assignee" => %{"id" => "human-1"}
    }

    issue = Client.normalize_issue_for_test(raw_issue, "human-1")
    assert issue.dispatchable
    assert issue.state_type == nil
    assert issue.delegate_id == nil
    assert issue.team_id == nil

    refute Client.normalize_issue_for_test(raw_issue, "another-human").dispatchable
  end

  test "agent session fetch returns the provider fields and reports GraphQL errors" do
    request_fun = fn payload, _headers ->
      send(self(), {:graphql_request, payload})

      {:ok,
       %{
         status: 200,
         body: %{
           "data" => %{
             "agentSession" => %{
               "id" => "session-1",
               "appUser" => %{"id" => "agent-1"},
               "issue" => %{"id" => "issue-1"},
               "dismissedAt" => nil
             }
           }
         }
       }}
    end

    assert {:ok, session} = Client.fetch_agent_session("session-1", client_opts(request_fun))
    assert session["appUser"] == %{"id" => "agent-1"}
    assert session["issueId"] == "issue-1"
    refute Map.has_key?(session, "issue")
    assert_receive {:graphql_request, %{"query" => query, "variables" => %{"id" => "session-1"}}}
    assert query =~ "agentSession(id: $id)"
    assert query =~ ~r/issue\s*\{\s*id\s*\}/
    refute query =~ "issueId"

    graphql_errors = [%{"message" => "session lookup denied"}]

    assert {:error, {:linear_graphql_errors, ^graphql_errors}} =
             Client.fetch_agent_session(
               "session-1",
               client_opts(fn _payload, _headers ->
                 {:ok, %{status: 200, body: %{"errors" => graphql_errors}}}
               end)
             )
  end

  test "acknowledgement creates a thought only after the exact missing-activity GraphQL error" do
    request_fun = fn payload, _headers ->
      send(self(), {:graphql_request, payload})

      if String.contains?(payload["query"], "agentActivityCreate") do
        {:ok,
         %{
           status: 200,
           body: %{
             "data" => %{
               "agentActivityCreate" => %{
                 "success" => true,
                 "agentActivity" => %{"id" => "activity-1"}
               }
             }
           }
         }}
      else
        {:ok,
         %{
           status: 200,
           body: %{
             "data" => nil,
             "errors" => [
               %{
                 "message" => "Entity not found: AgentActivity",
                 "extensions" => %{
                   "code" => "INPUT_ERROR",
                   "type" => "invalid input",
                   "userError" => true
                 }
               }
             ]
           }
         }}
      end
    end

    assert {:ok, %{id: "activity-1"}} =
             Client.acknowledge_agent_session(
               "session-1",
               "activity-1",
               "Starting work",
               client_opts(request_fun)
             )

    assert_receive {:graphql_request, %{"query" => lookup_query, "variables" => %{"id" => "activity-1"}}}
    assert lookup_query =~ "agentActivity(id: $id)"
    assert lookup_query =~ ~r/agentSession\s*\{\s*id\s*\}/
    refute lookup_query =~ "agentSessionId"

    assert_receive {:graphql_request,
                    %{
                      "query" => create_query,
                      "variables" => %{
                        "input" => %{
                          "id" => "activity-1",
                          "agentSessionId" => "session-1",
                          "content" => %{"type" => "thought", "body" => "Starting work"}
                        }
                      }
                    }}

    assert create_query =~ "agentActivityCreate(input: $input)"
  end

  test "acknowledgement accepts an existing activity for the same session" do
    request_fun = fn payload, _headers ->
      send(self(), {:graphql_request, payload})

      {:ok,
       %{
         status: 200,
         body: %{
           "data" => %{
             "agentActivity" => %{"id" => "activity-1", "agentSession" => %{"id" => "session-1"}}
           }
         }
       }}
    end

    assert {:ok, %{id: "activity-1"}} =
             Client.acknowledge_agent_session(
               "session-1",
               "activity-1",
               "Starting work",
               client_opts(request_fun)
             )

    assert_receive {:graphql_request, %{"query" => query}}
    assert query =~ "agentActivity(id: $id)"
    assert query =~ ~r/agentSession\s*\{\s*id\s*\}/
    refute_received {:graphql_request, %{"query" => _query}}
  end

  test "acknowledgement rejects an activity owned by another session" do
    request_fun = fn payload, _headers ->
      send(self(), {:graphql_request, payload})

      {:ok,
       %{
         status: 200,
         body: %{
           "data" => %{
             "agentActivity" => %{"id" => "activity-1", "agentSession" => %{"id" => "other-session"}}
           }
         }
       }}
    end

    assert {:error, {:linear_agent_activity_conflict, "activity-1"}} =
             Client.acknowledge_agent_session(
               "session-1",
               "activity-1",
               "Starting work",
               client_opts(request_fun)
             )

    assert_receive {:graphql_request, %{"query" => query}}
    assert query =~ "agentActivity(id: $id)"
    refute_received {:graphql_request, %{"query" => _query}}
  end

  test "acknowledgement does not create an activity for unrelated lookup errors" do
    graphql_errors = [
      %{
        "message" => "Linear is unavailable",
        "extensions" => %{"code" => "INTERNAL_ERROR"}
      }
    ]

    request_fun = fn payload, _headers ->
      send(self(), {:graphql_request, payload})

      {:ok, %{status: 200, body: %{"data" => nil, "errors" => graphql_errors}}}
    end

    assert {:error, {:linear_graphql_errors, ^graphql_errors}} =
             Client.acknowledge_agent_session(
               "session-1",
               "activity-1",
               "Starting work",
               client_opts(request_fun)
             )

    assert_receive {:graphql_request, %{"query" => query}}
    assert query =~ "agentActivity(id: $id)"
    refute_received {:graphql_request, %{"query" => _query}}
  end

  test "elicitation publication redacts text and reuses its durable provider ID" do
    request_fun = fn payload, _headers ->
      send(self(), {:publication_request, payload})

      if String.contains?(payload["query"], "agentActivityCreate") do
        {:ok, %{status: 200, body: %{"data" => %{"agentActivityCreate" => %{"success" => true, "agentActivity" => %{"id" => "question-1"}}}}}}
      else
        {:ok, %{status: 200, body: %{"data" => %{"agentActivity" => nil}}}}
      end
    end

    assert {:ok, %{id: "question-1"}} =
             Client.publish_agent_activity("session-1", "question-1", %{"type" => "elicitation", "body" => "Choose a label sk-testsecret123456789"}, client_opts(request_fun))

    assert_receive {:publication_request, %{"variables" => %{"id" => "question-1"}}}
    assert_receive {:publication_request, %{"variables" => %{"input" => %{"id" => "question-1", "agentSessionId" => "session-1", "content" => %{"type" => "elicitation", "body" => body}}}}}
    refute body =~ "sk-testsecret"
    assert body =~ "[REDACTED]"

    existing = fn payload, _headers ->
      send(self(), {:existing_publication, payload})
      {:ok, %{status: 200, body: %{"data" => %{"agentActivity" => %{"id" => "question-1", "agentSession" => %{"id" => "session-1"}}}}}}
    end

    assert {:ok, %{id: "question-1"}} = Client.publish_agent_activity("session-1", "question-1", %{"type" => "elicitation", "body" => "Choose a label"}, client_opts(existing))
    assert_receive {:existing_publication, _}
    refute_received {:existing_publication, _}
    assert {:error, :invalid_agent_activity_type} = Client.publish_agent_activity("session-1", "question-2", %{"type" => "prompt", "body" => "Human text"}, client_opts(existing))
  end

  test "delegated status mutation only sends stateId" do
    request_fun = fn payload, _headers ->
      send(self(), {:graphql_request, payload})

      {:ok,
       %{
         status: 200,
         body: %{
           "data" => %{
             "issueUpdate" => %{
               "success" => true,
               "issue" => %{"id" => "issue-1", "state" => %{"id" => "state-2", "name" => "Done", "type" => "completed"}}
             }
           }
         }
       }}
    end

    assert {:ok, %{"success" => true}} =
             Client.update_delegated_status("issue-1", "state-2", client_opts(request_fun))

    assert_receive {:graphql_request,
                    %{
                      "query" => query,
                      "variables" => %{"id" => "issue-1", "input" => %{"stateId" => "state-2"}}
                    }}

    assert query =~ "issueUpdate(id: $id, input: $input)"
    refute query =~ "assignee"
  end

  defp client_opts(request_fun) do
    [
      tracker_settings: %{api_key: "test-token", endpoint: "https://linear.test/graphql"},
      request_fun: request_fun
    ]
  end
end
