defmodule SymphonyElixir.Linear.Text do
  @moduledoc "Bounds and redacts human-visible text before durable storage or publication."

  @spec safe(term()) :: {:ok, String.t()} | {:error, atom()}
  def safe(text) when is_binary(text) and byte_size(text) > 0 and byte_size(text) <= 16_384 do
    if String.valid?(text) do
      text = String.replace(text, ~r/(?:sk-|gh[pousr]_|github_pat_)[A-Za-z0-9_-]+/, "[REDACTED]")

      safe =
        Enum.reduce(System.get_env(), text, fn {key, value}, acc ->
          if String.match?(key, ~r/TOKEN|SECRET|PASSWORD|API_KEY|ACCESS_KEY/) and byte_size(value) >= 8, do: String.replace(acc, value, "[REDACTED]"), else: acc
        end)

      if byte_size(safe) <= 16_384, do: {:ok, safe}, else: {:error, :invalid_message_text}
    else
      {:error, :invalid_message_text}
    end
  end

  def safe(_text), do: {:error, :invalid_message_text}
end
