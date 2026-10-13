defmodule SymphonyElixir.AgentReadinessTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.AgentReadiness

  defp agent(overrides \\ %{}) do
    Map.merge(
      %{
        name: "worker",
        model: "gpt-6.1-sol",
        reasoning_effort: "high",
        daybreak: false,
        authentication: %{mode: :subscription, reference: "inherited"}
      },
      overrides
    )
  end

  defp runtime(overrides \\ %{}) do
    Map.merge(
      %{
        account: %{type: "chatgpt", plan_type: "plus"},
        models: [
          %{
            "id" => "gpt-6.1-sol",
            "supportedReasoningEfforts" => [%{"reasoningEffort" => "high"}],
            "availableAccessPrograms" => %{"cyber" => ["standard"]}
          }
        ],
        limits: %{"ordinaryUsageAllowed" => true}
      },
      overrides
    )
  end

  defp codex_rate_limit(overrides \\ %{}) do
    Map.merge(
      %{
        "limitId" => "codex",
        "spendControlReached" => false,
        "credits" => %{"hasCredits" => true, "unlimited" => false, "balance" => nil}
      },
      overrides
    )
  end

  defp paused_runtime(rate_limits_by_id, legacy_rate_limit \\ :missing) do
    limits = %{
      "ordinaryUsageAllowed" => false,
      "rateLimitsByLimitId" => rate_limits_by_id
    }

    limits =
      case legacy_rate_limit do
        :missing -> limits
        legacy -> Map.put(limits, "rateLimits", legacy)
      end

    runtime(%{limits: limits})
  end

  test "dispatch accepts subscription and legacy agents but rejects other authentication" do
    assert :ok = AgentReadiness.dispatch(agent())
    assert :ok = AgentReadiness.dispatch(%{daybreak: false})

    assert {:error, :unsupported_authentication} =
             AgentReadiness.dispatch(agent(%{authentication: %{mode: :api, reference: "key"}}))
  end

  test "probe definitions require explicit Daybreak, subscription, model, and effort settings" do
    assert :ok = AgentReadiness.validate_definition(agent())
    assert :ok = AgentReadiness.validate_definition(agent(%{daybreak: true}))

    assert {:error, :invalid_daybreak} =
             AgentReadiness.validate_definition(Map.delete(agent(), :daybreak))

    assert {:error, :chatgpt_subscription_required} =
             AgentReadiness.validate_definition(agent(%{authentication: %{mode: :subscription, reference: ""}}))

    assert {:error, :chatgpt_subscription_required} =
             AgentReadiness.validate_definition(agent(%{authentication: %{mode: :subscription, reference: "   "}}))

    assert {:error, :chatgpt_subscription_required} =
             AgentReadiness.validate_definition(Map.delete(agent(), :authentication))

    assert {:error, :invalid_agent_model} =
             AgentReadiness.validate_definition(agent(%{model: "  "}))

    assert {:error, :invalid_agent_reasoning_effort} =
             AgentReadiness.validate_definition(Map.delete(agent(), :reasoning_effort))
  end

  test "Daybreak is blocked before authentication checks and invalid settings are rejected" do
    assert {:error, :daybreak_execution_unverified} =
             AgentReadiness.dispatch(agent(%{daybreak: true, authentication: %{mode: :api}}))

    assert {:error, :invalid_daybreak} = AgentReadiness.dispatch(agent(%{daybreak: "true"}))
    assert {:error, :invalid_daybreak} = AgentReadiness.check(agent(%{daybreak: nil}), runtime())
  end

  test "readiness requires a ChatGPT subscription and does not fall back to API credentials" do
    assert :ok = AgentReadiness.check(agent(), runtime())

    assert {:error, :chatgpt_subscription_required} =
             AgentReadiness.check(
               agent(%{authentication: %{mode: :api, reference: "key"}}),
               runtime()
             )

    assert {:error, :chatgpt_account_required} =
             AgentReadiness.check(agent(), runtime(%{account: %{type: "api"}}))
  end

  test "readiness requires the exact requested model and reasoning effort" do
    other_model = %{
      "id" => "gpt-6.1-alt",
      "supportedReasoningEfforts" => [%{"reasoningEffort" => "high"}],
      "availableAccessPrograms" => %{"cyber" => ["standard"]}
    }

    assert {:error, :model_not_advertised} =
             AgentReadiness.check(agent(), runtime(%{models: [other_model]}))

    assert {:error, :model_catalog_missing} =
             AgentReadiness.check(agent(), runtime(%{models: nil}))

    assert {:error, :reasoning_effort_not_advertised} =
             AgentReadiness.check(agent(%{reasoning_effort: "max"}), runtime())

    model_without_efforts = %{
      "id" => "gpt-6.1-sol",
      "availableAccessPrograms" => %{"cyber" => ["standard"]}
    }

    assert {:error, :reasoning_effort_catalog_missing} =
             AgentReadiness.check(agent(), runtime(%{models: [model_without_efforts]}))
  end

  test "readiness requires the requested Cyber access program to be advertised" do
    model_without_access = %{
      "id" => "gpt-6.1-sol",
      "supportedReasoningEfforts" => [%{"reasoningEffort" => "high"}]
    }

    assert {:error, :access_program_catalog_missing} =
             AgentReadiness.check(agent(), runtime(%{models: [model_without_access]}))

    model_with_daybreak_only = %{
      "id" => "gpt-6.1-sol",
      "supportedReasoningEfforts" => [%{"reasoningEffort" => "high"}],
      "availableAccessPrograms" => %{"cyber" => ["daybreakBlue"]}
    }

    assert {:error, :access_program_not_advertised} =
             AgentReadiness.check(agent(), runtime(%{models: [model_with_daybreak_only]}))
  end

  test "Daybreak eligibility requires its explicit catalog entry but never enables dispatch" do
    daybreak_agent = agent(%{daybreak: true})

    assert {:error, :access_program_not_advertised} =
             AgentReadiness.check(daybreak_agent, runtime())

    model_with_daybreak = %{
      "id" => "gpt-6.1-sol",
      "supportedReasoningEfforts" => [%{"reasoningEffort" => "high"}],
      "availableAccessPrograms" => %{"cyber" => ["standard", "daybreakBlue"]}
    }

    assert :ok = AgentReadiness.check(daybreak_agent, runtime(%{models: [model_with_daybreak]}))

    assert {:error, :daybreak_execution_unverified} = AgentReadiness.dispatch(daybreak_agent)
  end

  test "credit entitlement can recover an included-usage pause for the exact Codex limit" do
    limits = %{
      "ordinaryUsageAllowed" => false,
      "rateLimitsByLimitId" => %{"codex" => codex_rate_limit()}
    }

    assert :ok = AgentReadiness.check(agent(), runtime(%{limits: limits}))

    legacy_limits = %{
      "ordinaryUsageAllowed" => false,
      "rateLimits" => codex_rate_limit()
    }

    assert :ok = AgentReadiness.check(agent(), runtime(%{limits: legacy_limits}))

    positive_balance =
      codex_rate_limit(%{
        "credits" => %{"hasCredits" => true, "unlimited" => false, "balance" => "24.00"}
      })

    assert :ok =
             AgentReadiness.check(
               agent(),
               runtime(%{
                 limits: %{
                   "ordinaryUsageAllowed" => false,
                   "rateLimitsByLimitId" => %{"codex" => positive_balance}
                 }
               })
             )
  end

  test "explicit usage pauses, malformed credits, and credits from other limits block readiness" do
    assert {:error, :usage_paused} =
             AgentReadiness.check(agent(), runtime(%{limits: %{"ordinaryUsageAllowed" => false}}))

    invalid_credit_snapshots = [
      %{"hasCredits" => false, "unlimited" => false, "balance" => nil},
      %{"unlimited" => false, "balance" => nil},
      %{"hasCredits" => true, "balance" => nil},
      %{"hasCredits" => true, "unlimited" => false},
      %{"hasCredits" => true, "unlimited" => nil, "balance" => nil},
      %{"hasCredits" => true, "unlimited" => false, "balance" => "unknown"},
      %{"hasCredits" => true, "unlimited" => false, "balance" => "0"}
    ]

    for credits <- invalid_credit_snapshots do
      assert {:error, :usage_paused} =
               AgentReadiness.check(
                 agent(),
                 paused_runtime(%{
                   "codex" => codex_rate_limit(%{"credits" => credits})
                 })
               )
    end

    assert {:error, :usage_paused} =
             AgentReadiness.check(
               agent(),
               paused_runtime(%{
                 "codex" => codex_rate_limit(%{"credits" => nil}),
                 "other" => %{
                   "spendControlReached" => false,
                   "credits" => %{"hasCredits" => true, "unlimited" => false, "balance" => nil}
                 }
               })
             )

    assert {:error, :usage_paused} =
             AgentReadiness.check(
               agent(),
               paused_runtime(%{
                 "codex" => Map.delete(codex_rate_limit(), "credits")
               })
             )

    assert {:error, :usage_paused} =
             AgentReadiness.check(
               agent(),
               paused_runtime(%{
                 "codex" => Map.delete(codex_rate_limit(), "spendControlReached")
               })
             )

    assert {:error, :usage_paused} =
             AgentReadiness.check(
               agent(),
               paused_runtime(
                 %{
                   "other" => %{
                     "spendControlReached" => false,
                     "credits" => %{"hasCredits" => true, "unlimited" => false, "balance" => nil}
                   }
                 },
                 %{
                   "limitId" => "other",
                   "spendControlReached" => false,
                   "credits" => %{"hasCredits" => true, "unlimited" => false, "balance" => nil}
                 }
               )
             )
  end

  test "spend control blocks even when Codex credits are available" do
    assert {:error, :usage_paused} =
             AgentReadiness.check(
               agent(),
               runtime(%{
                 limits: %{
                   "ordinaryUsageAllowed" => false,
                   "rateLimitsByLimitId" => %{
                     "codex" => codex_rate_limit(%{"spendControlReached" => true})
                   }
                 }
               })
             )

    assert {:error, :usage_paused} =
             AgentReadiness.check(
               agent(),
               runtime(%{limits: %{"rateLimits" => [%{"spendControlReached" => true}]}})
             )

    assert :ok = AgentReadiness.check(agent(), runtime(%{limits: %{"rateLimits" => [%{}]}}))
  end

  test "requested settings stay on the named agent without model substitution" do
    requested = AgentReadiness.requested(agent(%{model: "chosen-model"}))

    assert requested == %{
             model: "chosen-model",
             reasoning_effort: "high",
             daybreak: false,
             cyber_access_program: "standard",
             authentication: %{mode: :subscription, reference: "inherited"}
           }

    assert {:error, :model_not_advertised} =
             AgentReadiness.check(agent(%{model: "chosen-model"}), runtime())
  end
end
