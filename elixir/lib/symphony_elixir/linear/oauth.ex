defmodule SymphonyElixir.Linear.OAuth do
  @moduledoc "Coordinator-only, memory-only app tokens scoped to one delegated issue run."
  use GenServer

  @scopes "read,write,app:assignable"
  @token_url "https://api.linear.app/oauth/token"
  @graphql_url "https://api.linear.app/graphql"
  @max_runs 1_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(opts), do: {:ok, %{tokens: %{}, request: Keyword.get(opts, :request_fun, &request_token/1), clock: Keyword.get(opts, :clock, fn -> System.monotonic_time(:second) end)}}

  @spec client_opts(map(), String.t(), keyword()) :: keyword()
  def client_opts(config, run_id, opts \\ []) do
    [tracker_settings: %{endpoint: @graphql_url, api_key: "coordinator-oauth"}, request_fun: fn payload, _headers -> graphql(config, run_id, payload, opts) end]
  end

  @spec graphql(map(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def graphql(config, run_id, payload, opts \\ []) do
    secret = SymphonyElixir.Config.linear_client_secret(config)
    server = Keyword.get(opts, :server, __MODULE__)
    request = Keyword.get(opts, :request_fun, &request_graphql/2)
    credentials = %{client_id: config["oauth_client_id"], client_secret: secret, run_id: run_id}

    with {:ok, token} <- token(server, credentials, nil) do
      case safe_request(fn -> request.(payload, token) end) do
        {:ok, %{status: 401}} ->
          with {:ok, renewed} <- token(server, credentials, token),
               do: safe_request(fn -> request.(payload, renewed) end)

        result ->
          result
      end
    end
  end

  defp token(server, credentials, rejected) do
    if Enum.all?(Map.values(credentials), &(is_binary(&1) and &1 != "")),
      do: GenServer.call(server, {:token, credentials, rejected}, 12_000),
      else: {:error, :linear_oauth_credentials_missing}
  catch
    :exit, _ -> {:error, :linear_oauth_unavailable}
  end

  @impl true
  def handle_call({:token, credentials, rejected}, _from, state) do
    now = state.clock.()
    tokens = Map.reject(state.tokens, fn {_key, value} -> value.expires_at <= now end)
    key = {credentials.client_id, credentials.run_id, :crypto.hash(:sha256, credentials.client_secret)}
    cached = tokens[key]
    tokens = if cached && cached.token == rejected, do: Map.delete(tokens, key), else: tokens

    case tokens[key] do
      %{token: token} ->
        {:reply, {:ok, token}, %{state | tokens: tokens}}

      nil when map_size(tokens) >= @max_runs ->
        {:reply, {:error, :linear_oauth_capacity}, %{state | tokens: tokens}}

      nil ->
        form = [grant_type: "client_credentials", scope: @scopes, client_id: credentials.client_id, client_secret: credentials.client_secret]

        case safe_request(fn -> state.request.(form) end) do
          {:ok, %{status: 200, body: %{"access_token" => token, "token_type" => "Bearer", "expires_in" => expires}}}
          when is_binary(token) and byte_size(token) > 0 and is_integer(expires) and expires > 0 and expires <= 2_592_000 ->
            value = %{token: token, expires_at: now + max(expires - 30, 1)}
            {:reply, {:ok, token}, %{state | tokens: Map.put(tokens, key, value)}}

          _ ->
            {:reply, {:error, :linear_oauth_token_failed}, %{state | tokens: tokens}}
        end
    end
  end

  @impl true
  def format_status(status), do: status |> Map.put(:state, :redacted) |> Map.put(:message, :redacted)

  defp request_token(form), do: Req.post(@token_url, form: form, retry: false, redirect: false, receive_timeout: 5_000, connect_options: [timeout: 5_000])

  defp request_graphql(payload, token),
    do: Req.post(@graphql_url, json: payload, headers: [{"Authorization", "Bearer " <> token}], retry: false, redirect: false, receive_timeout: 5_000, connect_options: [timeout: 5_000])

  defp safe_request(fun) do
    case fun.() do
      {:ok, %{status: status}} when status != 200 -> {:ok, %{status: status, body: :redacted}}
      {:ok, reply} -> {:ok, reply}
      _ -> {:error, :linear_oauth_request_failed}
    end
  rescue
    _ -> {:error, :linear_oauth_request_failed}
  catch
    _, _ -> {:error, :linear_oauth_request_failed}
  end
end
