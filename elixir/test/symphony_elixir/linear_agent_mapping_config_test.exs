defmodule SymphonyElixir.Linear.AgentMappingConfigTest do
  use SymphonyElixir.TestSupport

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

  test "service startup uses the existing Config reader to match protected credential pins", c do
    marker = "FACTORY_LINEAR_SECRET_NAMES_JSON"
    previous = System.get_env(marker)
    on_exit(fn -> restore_env(marker, previous) end)

    config =
      Map.merge(c.config, %{
        "team_id" => "dev",
        "store_path" => "/tmp/dev235/state.sqlite",
        "workspace_root" => "/tmp/dev235/workspaces",
        "workstream_path" => "/tmp/dev235/software-change.yaml",
        "rig_label" => "factory:rig"
      })

    path = Workflow.workflow_file_path()
    File.write!(path, "---\n" <> Jason.encode!(%{"tracker" => %{"kind" => "memory"}, "linear_delegation" => config}) <> "\n---\nControlled JSON frontmatter")
    WorkflowStore.force_reload()
    refs = SymphonyElixir.Linear.AgentMapping.credential_environment_names(config) |> Enum.sort()
    System.put_env(marker, Jason.encode!(refs))
    assert {:ok, loaded} = Config.linear_delegation()
    assert loaded["cloud_agents"] == config["cloud_agents"]
    System.put_env(marker, Jason.encode!(Enum.drop(refs, 1)))
    assert {:error, :linear_runtime_secret_references_changed} = Config.linear_delegation()
    System.put_env(marker, "invalid-json")
    assert {:error, :invalid_linear_runtime_secret_references} = Config.linear_delegation()
    System.delete_env(marker)
    assert {:ok, _} = Config.linear_delegation()
  end

  test "Config blocks when any configured signing reference is missing", c do
    System.put_env(c.signing, "synthetic-default-signing")
    assert {:error, :signing_secret_unavailable} = Config.linear_webhook_secrets(c.config)
    assert {:error, :invalid_cloud_agent_mapping} = Config.linear_webhook_secrets(%{})
  end
end
