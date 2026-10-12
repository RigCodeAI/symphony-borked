defmodule SymphonyElixir do
  @moduledoc """
  Entry point for the Symphony orchestrator.
  """

  @doc """
  Start the agent runtime in the current BEAM node.
  """
  @spec start_link() :: Supervisor.on_start()
  def start_link do
    with {:ok, opts} <- runtime_options(), do: SymphonyElixir.AgentRuntimeSupervisor.start_link(opts)
  end

  @doc "Selects a protected held recovery plan before normal runtime configuration is read."
  @spec runtime_options() :: {:ok, keyword()} | {:error, atom()}
  def runtime_options do
    case Application.fetch_env(:symphony_elixir, :recovery_plan_path) do
      :error ->
        {:ok, []}

      {:ok, path} ->
        with {:ok, config} <- SymphonyElixir.RecoveryPlan.load(path), do: {:ok, [recovery_capture: config]}
    end
  end
end

defmodule SymphonyElixir.Application do
  @moduledoc """
  OTP application entrypoint that starts core supervisors and workers.
  """

  use Application
  require Logger

  @dialyzer {:nowarn_function, start_burrito_cli: 0}

  @impl true
  def start(_type, _args) do
    if burrito_runtime?() do
      start_burrito_cli()
    else
      start_runtime()
    end
  end

  @doc false
  @spec start_runtime() :: Supervisor.on_start()
  def start_runtime do
    with {:ok, opts} <- SymphonyElixir.runtime_options() do
      :ok = SymphonyElixir.LogFile.configure()

      children =
        if Keyword.has_key?(opts, :recovery_capture) do
          [{SymphonyElixir.AgentRuntimeSupervisor, opts}]
        else
          [
            {Phoenix.PubSub, name: SymphonyElixir.PubSub},
            SymphonyElixir.WorkflowStore,
            SymphonyElixir.Evidence,
            SymphonyElixir.Linear.OAuth,
            SymphonyElixir.AgentRuntimeSupervisor,
            SymphonyElixir.HttpServer,
            SymphonyElixir.LinearWebhookServer,
            SymphonyElixir.StatusDashboard
          ]
        end

      with {:ok, pid} <- Supervisor.start_link(children, strategy: :one_for_one, name: SymphonyElixir.Supervisor) do
        if Keyword.has_key?(opts, :recovery_capture) do
          {:ok, report} = SymphonyElixir.Orchestrator.recovery_status(SymphonyElixir.Orchestrator)

          Logger.warning(
            "recovery_dispatch_held capture_id=#{report.capture_id} " <>
              "policy_compatible=#{report.policy_compatible} dispatch_allowed=false restore_ready=false"
          )
        end

        {:ok, pid}
      end
    end
  end

  @impl true
  def stop(_state) do
    SymphonyElixir.StatusDashboard.render_offline_status()
    :ok
  end

  defp start_burrito_cli do
    Task.start_link(fn ->
      SymphonyElixir.CLI.main(
        plain_arguments(),
        &start_runtime/0
      )
    end)
  end

  defp burrito_runtime?, do: System.get_env("__BURRITO") == "1"

  defp plain_arguments, do: Enum.map(:init.get_plain_arguments(), &to_string/1)
end
