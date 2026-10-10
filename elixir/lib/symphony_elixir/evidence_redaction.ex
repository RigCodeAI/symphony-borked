defmodule SymphonyElixir.EvidenceRedaction do
  @moduledoc "Redacts observable evidence before it reaches the evidence database or spool."

  @secret_key ~r/^(?:authorization|cookie|set-cookie|(?:[a-z0-9]+[_-])*(?:password|passwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|private[_-]?key))$/i
  @hidden_keys ~w(reasoning analysis chain_of_thought encrypted_content raw)

  @spec safe(term()) :: term()
  def safe(value), do: walk(value, secrets(), 0)

  @spec text(String.t()) :: String.t()
  def text(value), do: redact(value, secrets())

  @spec observable(map()) :: map() | :ignore
  def observable(%{payload: %{"method" => "item/completed", "params" => %{"item" => item}}} = event) do
    allowed = %{
      "agentMessage" => ~w(id type text phase),
      "userMessage" => ~w(id type content),
      "commandExecution" => ~w(id type command cwd status exitCode aggregatedOutput durationMs),
      "mcpToolCall" => ~w(id type server tool status arguments result error),
      "dynamicToolCall" => ~w(id type tool status arguments contentItems success),
      "fileChange" => ~w(id type status changes)
    }

    case if(item["phase"] in ["analysis", "reasoning"], do: nil, else: allowed[item["type"]]) do
      nil -> :ignore
      keys -> safe(%{kind: "item/completed", item: observable_item(item, keys), session_id: event[:session_id], thread_id: event[:thread_id], turn_id: event[:turn_id]})
    end
  end

  def observable(%{event: kind} = event) when kind in [:session_started, :turn_completed, :turn_failed, :turn_cancelled, :startup_failed] do
    safe(Map.take(event, [:event, :session_id, :thread_id, :turn_id]))
  end

  def observable(_), do: :ignore

  defp observable_item(%{"type" => "userMessage"} = item, keys) do
    content =
      Enum.flat_map(List.wrap(item["content"]), fn
        %{"type" => "text", "text" => text} when is_binary(text) -> [%{"type" => "text", "text" => text}]
        _ -> []
      end)

    item |> Map.take(keys) |> Map.put("content", content)
  end

  defp observable_item(item, keys), do: Map.take(item, keys)

  defp secrets do
    System.get_env()
    |> Enum.flat_map(fn {key, value} ->
      if String.match?(key, ~r/TOKEN|SECRET|PASSWORD|API_KEY|ACCESS_KEY|PRIVATE_KEY/i) and byte_size(value) >= 8, do: [value], else: []
    end)
    |> Enum.sort_by(&byte_size/1, :desc)
  end

  defp walk(_value, _secrets, depth) when depth > 32, do: "[OMITTED: depth limit]"

  defp walk(value, secrets, depth) when is_map(value) and not is_struct(value) do
    value
    |> Enum.reject(fn {key, _} -> to_string(key) in @hidden_keys end)
    |> Map.new(fn {key, item} ->
      key = to_string(key)
      {redact(key, secrets), if(String.match?(key, @secret_key), do: "[REDACTED]", else: walk(item, secrets, depth + 1))}
    end)
  end

  defp walk(value, secrets, depth) when is_list(value), do: Enum.map(value, &walk(&1, secrets, depth + 1))
  defp walk(value, secrets, depth) when is_tuple(value), do: walk(Tuple.to_list(value), secrets, depth + 1)
  defp walk(value, secrets, _depth) when is_binary(value), do: redact(value, secrets)
  defp walk(value, _secrets, _depth) when is_boolean(value) or is_nil(value) or is_number(value), do: value
  defp walk(value, _secrets, _depth) when is_atom(value), do: Atom.to_string(value)
  defp walk(_value, _secrets, _depth), do: "[OMITTED: unsupported value]"

  defp redact(text, secrets) do
    text = if String.valid?(text), do: text, else: "[OMITTED: invalid UTF-8]"
    text = Enum.reduce(secrets, text, &String.replace(&2, &1, "[REDACTED]"))

    text
    |> String.replace(~r/-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----/s, "[REDACTED]")
    |> String.replace(~r/(?<![A-Za-z0-9_])(?:sk-|gh[pousr]_|github_pat_)[A-Za-z0-9_-]+|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/, "[REDACTED]")
    |> String.replace(~r/\b(?:Bearer|Basic)\s+[A-Za-z0-9+\/_=.-]+/i, "[REDACTED]")
    |> String.replace(~r/\b(?:[a-z0-9]+[_-])*(?:password|passwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token)\s*[:=]\s*(?:"[^"]*"|'[^']*'|[^\s,;]+)/i, "[REDACTED]")
  end
end
