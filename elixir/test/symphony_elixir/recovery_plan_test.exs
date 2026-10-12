defmodule SymphonyElixir.RecoveryPlanTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{PathSafety, RecoveryPlan}

  @manifest_sha String.duplicate("a", 64)
  @service_revision String.duplicate("b", 40)

  setup do
    {:ok, temp_dir} = PathSafety.canonicalize(System.tmp_dir!())
    root = Path.join(temp_dir, "recovery-plan-#{System.unique_integer([:positive])}")
    plan_dir = Path.join(root, "plans")
    capture_root = Path.join(root, "captures")

    File.mkdir!(root)
    File.chmod!(root, 0o700)
    File.mkdir!(plan_dir)
    File.chmod!(plan_dir, 0o700)
    File.mkdir!(capture_root)
    File.chmod!(capture_root, 0o700)

    on_exit(fn -> File.rm_rf!(root) end)

    %{root: root, plan_dir: plan_dir, capture_root: capture_root, plan_path: Path.join(plan_dir, "plan.json")}
  end

  test "loads the exact inspection config and leaves plan bytes and file identity unchanged", context do
    bytes = write_plan(context, valid_plan(context))
    {:ok, before_stat} = File.lstat(context.plan_path)

    assert {:ok, config} = RecoveryPlan.load(context.plan_path)
    assert config == expected_config(context)
    assert File.read!(context.plan_path) == bytes

    {:ok, after_stat} = File.lstat(context.plan_path)
    assert stat_identity(after_stat) == stat_identity(before_stat)
  end

  test "preserves current and legacy policy inventories without upgrading either", context do
    legacy = valid_plan(context)
    current = put_in(legacy, ["policy", "child_sha256"], String.duplicate("f", 64))
    write_plan(context, current)
    assert {:ok, config} = RecoveryPlan.load(context.plan_path)
    assert config.policy == Map.put(expected_config(context).policy, :child_sha256, String.duplicate("f", 64))
    write_plan(context, legacy)
    assert {:ok, config} = RecoveryPlan.load(context.plan_path)
    refute Map.has_key?(config.policy, :child_sha256)
  end

  test "rejects non-absolute and parent-traversing paths", context do
    assert {:error, :invalid_recovery_plan_path} = RecoveryPlan.load("plan.json")
    assert {:error, :invalid_recovery_plan_path} = RecoveryPlan.load(context.plan_dir <> "/../plan.json")
    assert {:error, :invalid_recovery_plan_path} = RecoveryPlan.load(context.plan_path <> <<0>>)
  end

  test "rejects symlinks in the plan file or its parent path", context do
    write_plan(context, valid_plan(context))
    file_link = Path.join(context.plan_dir, "file-link.json")
    parent_link = Path.join(context.root, "plan-dir-link")

    assert :ok = File.ln_s(context.plan_path, file_link)
    assert :ok = File.ln_s(context.plan_dir, parent_link)

    assert {:error, :unsafe_recovery_plan_file} = RecoveryPlan.load(file_link)
    assert {:error, :unsafe_recovery_plan_path} = RecoveryPlan.load(Path.join(parent_link, "plan.json"))
  end

  test "rejects hard-linked plans", context do
    write_plan(context, valid_plan(context))
    hard_link = Path.join(context.plan_dir, "second-link.json")
    assert :ok = File.ln(context.plan_path, hard_link)

    assert {:error, :unsafe_recovery_plan_file} = RecoveryPlan.load(context.plan_path)
  end

  test "requires a private owner-only plan file and private owner-only parent directory", context do
    write_plan(context, valid_plan(context))
    File.chmod!(context.plan_path, 0o640)
    assert {:error, :unsafe_recovery_plan_file} = RecoveryPlan.load(context.plan_path)

    File.chmod!(context.plan_path, 0o600)
    File.chmod!(context.plan_dir, 0o755)
    assert {:error, :unsafe_recovery_plan_path} = RecoveryPlan.load(context.plan_path)
  end

  test "rejects a plan larger than 64 KiB before decoding it", context do
    bytes = :binary.copy("x", 64 * 1024 + 1)
    File.write!(context.plan_path, bytes)
    File.chmod!(context.plan_path, 0o600)

    assert {:error, :recovery_plan_too_large} = RecoveryPlan.load(context.plan_path)
  end

  test "rejects duplicate JSON keys, including nested keys", context do
    bytes = Jason.encode!(valid_plan(context))

    duplicates = [
      String.replace(bytes, ~s("mode":), ~s("version":1,"mode":), global: false),
      String.replace(bytes, ~s("policy":{), ~s("policy":{"version":1,), global: false)
    ]

    Enum.each(duplicates, fn duplicate ->
      assert duplicate != bytes
      File.write!(context.plan_path, duplicate)
      File.chmod!(context.plan_path, 0o600)
      assert {:error, :duplicate_recovery_plan_key} = RecoveryPlan.load(context.plan_path)
    end)
  end

  test "rejects unknown or invalid plan and policy fields", context do
    plan = valid_plan(context)

    invalid_plans = [
      Map.put(plan, "extra", true),
      Map.put(plan, "version", 2),
      Map.put(plan, "mode", "restore"),
      Map.put(plan, "root", "relative/captures"),
      Map.put(plan, "id", "../capture"),
      Map.put(plan, "workspace_roots", []),
      Map.put(plan, "workspace_roots", ["relative/workspace"]),
      Map.put(plan, "manifest_sha256", String.duplicate("A", 64)),
      Map.put(plan, "service_revision", "not-a-revision"),
      put_in(plan, ["policy", "extra"], true),
      put_in(plan, ["policy", "version"], 2),
      put_in(plan, ["policy", "gate_policy"], "other-policy"),
      put_in(plan, ["policy", "lifecycle_sha256"], "short"),
      put_in(plan, ["policy", "executor_sha256"], "short"),
      put_in(plan, ["policy", "validation_sha256"], "short"),
      put_in(plan, ["policy", "child_sha256"], "short")
    ]

    Enum.each(invalid_plans, fn invalid_plan ->
      write_plan(context, invalid_plan)
      assert {:error, :invalid_recovery_plan} = RecoveryPlan.load(context.plan_path)
    end)
  end

  defp valid_plan(context) do
    %{
      "version" => 1,
      "mode" => "held_inspection",
      "root" => context.capture_root,
      "id" => "capture-1",
      "workspace_roots" => [Path.join(context.root, "workspaces")],
      "manifest_sha256" => @manifest_sha,
      "service_revision" => @service_revision,
      "policy" => %{
        "version" => 1,
        "gate_policy" => "exit-status-v1",
        "lifecycle_sha256" => String.duplicate("c", 64),
        "executor_sha256" => String.duplicate("d", 64),
        "validation_sha256" => String.duplicate("e", 64)
      }
    }
  end

  defp expected_config(context) do
    %{
      root: context.capture_root,
      id: "capture-1",
      workspace_roots: [Path.join(context.root, "workspaces")],
      manifest_sha256: @manifest_sha,
      service_revision: @service_revision,
      policy: %{
        version: 1,
        gate_policy: "exit-status-v1",
        lifecycle_sha256: String.duplicate("c", 64),
        executor_sha256: String.duplicate("d", 64),
        validation_sha256: String.duplicate("e", 64)
      }
    }
  end

  defp write_plan(context, plan) do
    bytes = Jason.encode!(plan)
    File.write!(context.plan_path, bytes)
    File.chmod!(context.plan_path, 0o600)
    bytes
  end

  defp stat_identity(stat) do
    {stat.type, stat.links, stat.mode, stat.uid, stat.size, stat.inode, stat.major_device, stat.minor_device, stat.mtime, stat.ctime}
  end
end
