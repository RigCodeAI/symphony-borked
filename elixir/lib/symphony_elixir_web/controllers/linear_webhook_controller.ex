defmodule SymphonyElixirWeb.LinearWebhookController do
  @moduledoc """
  Receives authenticated Linear webhook events.
  """

  use Phoenix.Controller, formats: [:json]

  alias Plug.Conn
  alias SymphonyElixir.{Config, Linear.AgentMapping, Orchestrator}
  alias SymphonyElixirWeb.Endpoint

  @spec create(Conn.t(), map()) :: Conn.t()
  def create(conn, _params) do
    case Config.linear_delegation() do
      {:ok, config} -> receive_webhook(conn, config)
      :disabled -> unavailable(conn)
      {:error, _reason} -> unavailable(conn)
    end
  end

  defp receive_webhook(conn, config) do
    with {:ok, secrets} <- Config.linear_webhook_secrets(config) do
      case AgentMapping.verify(conn.assigns[:linear_raw_body], conn.req_headers, config, secrets) do
        {:ok, event} -> dispatch_event(conn, event)
        {:error, reason} when reason in [:invalid_signature, :invalid_timestamp] -> error_response(conn, 401, "unauthorized", "Webhook authentication failed")
        {:error, reason} when reason in [:unrelated_event, :unknown_cloud_agent_identity] -> error_response(conn, 422, "unsupported_event", "Unsupported webhook event")
        {:error, reason} when reason in [:signing_secret_unavailable, :ambiguous_signing_secrets] -> unavailable(conn)
        {:error, _reason} -> error_response(conn, 400, "invalid_request", "Invalid webhook request")
      end
    else
      _ -> unavailable(conn)
    end
  end

  defp dispatch_event(conn, event) do
    try do
      case Orchestrator.receive_linear_event(conn.assigns[:linear_orchestrator] || orchestrator(), event) do
        :ok ->
          json(conn, %{status: "accepted"})

        {:duplicate, _existing} ->
          json(conn, %{status: "duplicate"})

        {:error, :unrelated_linear_event} ->
          error_response(conn, 422, "unsupported_event", "Unsupported webhook event")

        {:error, _reason} ->
          unavailable(conn)
      end
    rescue
      _exception -> unavailable(conn)
    catch
      :exit, _reason -> unavailable(conn)
    end
  end

  defp orchestrator do
    Endpoint.config(:orchestrator) || Orchestrator
  end

  defp unavailable(conn) do
    error_response(conn, 503, "unavailable", "Webhook processing is unavailable")
  end

  defp error_response(conn, status, code, message) do
    conn
    |> put_status(status)
    |> json(%{error: %{code: code, message: message}})
  end
end
