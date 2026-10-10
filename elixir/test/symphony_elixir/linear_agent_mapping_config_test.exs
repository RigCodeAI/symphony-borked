defmodule SymphonyElixir.Linear.AgentMappingConfigTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.Config

  setup do
    suffix = System.unique_integer([:positive])
    signing = "DEV235_SIGNING_#{suffix}"
    high_signing = "DEV235_HIGH_SIGNING_#{suffix}"
    on_exit(fn -> Enum.each([signing, high_signing], &System.delete_env/1) end)

    default = %{"app_user_id" => "default", "oauth_client_id" => "default-client", "agent_id" => "default-cloud", "webhook_secret_env" => signing, "client_secret_env" => "DEV235_DEFAULT_CLIENT"}
    high = %{"app_user_id" => "high", "oauth_client_id" => "high-client", "agent_id" => "high-effort-cloud", "webhook_secret_env" => high_signing, "client_secret_env" => "DEV235_HIGH_CLIENT"}
    %{config: Map.merge(default, %{"organization_id" => "rig", "cloud_agents" => [high]}), signing: signing, high_signing: high_signing}
  end

  test "Config resolves every route's signing reference without changing stored config", c do
    System.put_env(c.signing, "synthetic-default-signing")
    System.put_env(c.high_signing, "synthetic-high-signing")
    assert {:ok, secrets} = Config.linear_webhook_secrets(c.config)
    assert secrets == %{c.signing => "synthetic-default-signing", c.high_signing => "synthetic-high-signing"}
    refute Map.has_key?(c.config, "synthetic-default-signing")
    refute Map.has_key?(secrets, "DEV235_DEFAULT_CLIENT")
  end

  test "Config blocks when any configured signing reference is missing", c do
    System.put_env(c.signing, "synthetic-default-signing")
    assert {:error, :signing_secret_unavailable} = Config.linear_webhook_secrets(c.config)
    assert {:error, :invalid_cloud_agent_mapping} = Config.linear_webhook_secrets(%{})
  end
end
