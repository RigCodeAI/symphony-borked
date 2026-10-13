defmodule SymphonyElixir.AgentReadiness do
  @moduledoc """
  Checks whether a named agent's requested settings are eligible for a runtime.

  These checks use advertised runtime capabilities only. They do not prove entitlement or
  successful inference, and they never execute a qualification probe.
  """

  @type requested_settings :: %{
          model: term(),
          reasoning_effort: term(),
          daybreak: term(),
          cyber_access_program: String.t() | nil,
          authentication: term()
        }

  @doc """
  Allows legacy local agents and subscription-backed agents through normal dispatch.

  Daybreak dispatch stays blocked until execution is separately verified. Missing
  authentication is accepted here only for compatibility with legacy local agents.
  """
  @spec dispatch(map()) :: :ok | {:error, atom()}
  def dispatch(agent) when is_map(agent) do
    case field(agent, :daybreak) do
      {:ok, true} -> {:error, :daybreak_execution_unverified}
      {:ok, false} -> dispatch_authentication(agent)
      _ -> {:error, :invalid_daybreak}
    end
  end

  def dispatch(_agent), do: {:error, :invalid_agent}

  @doc """
  Returns the request settings stored directly on an agent.

  The access program is derived from that agent's Daybreak setting. No profile lookup or
  runtime substitution is performed.
  """
  @spec requested(map()) :: requested_settings()
  def requested(agent) when is_map(agent) do
    daybreak = value(agent, :daybreak)

    %{
      model: value(agent, :model),
      reasoning_effort: value(agent, :reasoning_effort),
      daybreak: daybreak,
      cyber_access_program: access_program(daybreak),
      authentication: value(agent, :authentication)
    }
  end

  @doc """
  Validates settings required to identify an agent for an operator qualification probe.

  Unlike legacy-compatible dispatch, this requires explicit Daybreak, subscription
  authentication, model, and reasoning-effort settings.
  """
  @spec validate_definition(map()) :: :ok | {:error, atom()}
  def validate_definition(agent) when is_map(agent) do
    with :ok <- valid_daybreak(value(agent, :daybreak)),
         :ok <- subscription_authentication(value(agent, :authentication)),
         :ok <- nonempty_setting(value(agent, :model), :invalid_agent_model),
         :ok <-
           nonempty_setting(
             value(agent, :reasoning_effort),
             :invalid_agent_reasoning_effort
           ) do
      :ok
    end
  end

  def validate_definition(_agent), do: {:error, :invalid_agent_definition}

  @doc """
  Checks authentication, the exact model and effort, its advertised Cyber access program,
  and explicit runtime usage pauses.

  An `:ok` result means the request is eligible according to the supplied snapshot. It is
  not evidence that the account is entitled to every use or that inference succeeds.
  """
  @spec check(map(), map()) :: :ok | {:error, atom()}
  def check(agent, runtime) when is_map(agent) and is_map(runtime) do
    request = requested(agent)

    with :ok <- valid_daybreak(request.daybreak),
         :ok <- subscription_authentication(request.authentication),
         :ok <- chatgpt_account(field(runtime, :account)),
         {:ok, model} <- advertised_model(request.model, field(runtime, :models)),
         :ok <-
           advertised_effort(
             request.reasoning_effort,
             field(model, "supportedReasoningEfforts")
           ),
         :ok <-
           advertised_access(
             request.cyber_access_program,
             field(model, "availableAccessPrograms")
           ),
         :ok <- usage_allowed(field(runtime, :limits)) do
      :ok
    end
  end

  def check(_agent, _runtime), do: {:error, :invalid_readiness_input}

  defp dispatch_authentication(agent) do
    case field(agent, :authentication) do
      :error ->
        :ok

      {:ok, authentication} ->
        if valid_subscription_authentication?(authentication),
          do: :ok,
          else: {:error, :unsupported_authentication}
    end
  end

  defp subscription_authentication(authentication) do
    if valid_subscription_authentication?(authentication),
      do: :ok,
      else: {:error, :chatgpt_subscription_required}
  end

  defp valid_subscription_authentication?(authentication) when is_map(authentication) do
    field(authentication, :mode) in [{:ok, :subscription}, {:ok, "subscription"}] and
      nonempty_binary?(value(authentication, :reference))
  end

  defp valid_subscription_authentication?(_authentication), do: false

  defp chatgpt_account({:ok, account}) when is_map(account) do
    if field(account, :type) == {:ok, "chatgpt"},
      do: :ok,
      else: {:error, :chatgpt_account_required}
  end

  defp chatgpt_account(_account), do: {:error, :chatgpt_account_required}

  defp advertised_model(model_id, {:ok, models}) when is_binary(model_id) and is_list(models) do
    case Enum.find(models, fn model -> is_map(model) and model_identifier(model) == model_id end) do
      model when is_map(model) -> {:ok, model}
      _ -> {:error, :model_not_advertised}
    end
  end

  defp advertised_model(_model_id, {:ok, models}) when is_list(models),
    do: {:error, :invalid_agent_model}

  defp advertised_model(_model_id, _models), do: {:error, :model_catalog_missing}

  defp model_identifier(model) do
    case field(model, "id") do
      {:ok, id} when is_binary(id) -> id
      _ -> value(model, "model")
    end
  end

  defp advertised_effort(effort, {:ok, supported}) when is_binary(effort) and is_list(supported) do
    explicitly_supported? =
      Enum.any?(supported, fn entry ->
        is_map(entry) and field(entry, "reasoningEffort") == {:ok, effort}
      end)

    if explicitly_supported?,
      do: :ok,
      else: {:error, :reasoning_effort_not_advertised}
  end

  defp advertised_effort(_effort, {:ok, supported}) when is_list(supported),
    do: {:error, :invalid_agent_reasoning_effort}

  defp advertised_effort(_effort, _supported), do: {:error, :reasoning_effort_catalog_missing}

  defp advertised_access(program, {:ok, programs_by_feature})
       when is_binary(program) and is_map(programs_by_feature) do
    case field(programs_by_feature, "cyber") do
      {:ok, programs} when is_list(programs) ->
        if program in programs, do: :ok, else: {:error, :access_program_not_advertised}

      _ ->
        {:error, :access_program_catalog_missing}
    end
  end

  defp advertised_access(_program, _programs_by_feature),
    do: {:error, :access_program_catalog_missing}

  defp valid_daybreak(daybreak) when is_boolean(daybreak), do: :ok
  defp valid_daybreak(_daybreak), do: {:error, :invalid_daybreak}

  defp nonempty_setting(setting, reason) when is_binary(setting) do
    if nonempty_binary?(setting), do: :ok, else: {:error, reason}
  end

  defp nonempty_setting(_setting, reason), do: {:error, reason}

  defp nonempty_binary?(value) when is_binary(value) do
    String.valid?(value) and String.trim(value) != ""
  end

  defp nonempty_binary?(_value), do: false

  defp usage_allowed({:ok, limits}) do
    cond do
      explicit_flag?(limits, "spendControlReached", true) ->
        {:error, :usage_paused}

      explicit_flag?(limits, "ordinaryUsageAllowed", false) and
          not credit_usage_allowed?(limits) ->
        {:error, :usage_paused}

      true ->
        :ok
    end
  end

  defp usage_allowed(_limits), do: :ok

  defp credit_usage_allowed?(limits) do
    with {:ok, codex_limits} <- codex_rate_limits(limits),
         {:ok, false} <- field(codex_limits, "spendControlReached"),
         {:ok, credits} <- field(codex_limits, "credits") do
      credits_available?(credits)
    else
      _ -> false
    end
  end

  defp codex_rate_limits(limits) do
    case field(limits, "rateLimitsByLimitId") do
      {:ok, rate_limits_by_id} when is_map(rate_limits_by_id) ->
        case field(rate_limits_by_id, "codex") do
          {:ok, snapshot} -> if(is_map(snapshot), do: {:ok, snapshot}, else: :error)
          :error -> legacy_codex_rate_limits(limits)
        end

      {:ok, nil} ->
        legacy_codex_rate_limits(limits)

      :error ->
        legacy_codex_rate_limits(limits)

      _ ->
        :error
    end
  end

  defp legacy_codex_rate_limits(limits) do
    case field(limits, "rateLimits") do
      {:ok, snapshot} when is_map(snapshot) ->
        if field(snapshot, "limitId") == {:ok, "codex"}, do: {:ok, snapshot}, else: :error

      _ ->
        :error
    end
  end

  defp credits_available?(credits) when is_map(credits) do
    has_credits? = field(credits, "hasCredits") == {:ok, true}
    valid_unlimited? = field(credits, "unlimited") in [{:ok, true}, {:ok, false}]

    balance_available? =
      case field(credits, "balance") do
        {:ok, nil} -> true
        {:ok, balance} -> positive_credit_balance?(balance)
        :error -> false
      end

    has_credits? and valid_unlimited? and balance_available?
  end

  defp credits_available?(_credits), do: false

  defp positive_credit_balance?(balance) when is_binary(balance) do
    Regex.match?(~r/\A\d+(?:\.\d+)?\z/, balance) and
      Regex.match?(~r/[1-9]/, balance)
  end

  defp positive_credit_balance?(_balance), do: false

  defp explicit_flag?(value, key, expected) when is_map(value) do
    field(value, key) == {:ok, expected} or
      Enum.any?(Map.values(value), &explicit_flag?(&1, key, expected))
  end

  defp explicit_flag?(value, key, expected) when is_list(value),
    do: Enum.any?(value, &explicit_flag?(&1, key, expected))

  defp explicit_flag?(_value, _key, _expected), do: false

  defp access_program(false), do: "standard"
  defp access_program(true), do: "daybreakBlue"
  defp access_program(_daybreak), do: nil

  defp field(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      :error when is_atom(key) -> Map.fetch(map, Atom.to_string(key))
      result -> result
    end
  end

  defp field(_map, _key), do: :error

  defp value(map, key) do
    case field(map, key) do
      {:ok, value} -> value
      :error -> nil
    end
  end
end
