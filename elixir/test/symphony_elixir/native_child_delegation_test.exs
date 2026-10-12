defmodule SymphonyElixir.NativeChildDelegationTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Linear.Delegation
  alias SymphonyElixir.Tracker.Issue
  alias SymphonyElixir.{WorkstreamRun, WorkstreamRunner}

  setup do
    root = Path.join(System.tmp_dir!(), "native-child-delegation-#{System.unique_integer([:positive])}")
    workspace_root = Path.join(root, "workspaces")
    parent_workspace = Path.join(workspace_root, "parent")
    child_workspace = Path.join(workspace_root, "child")
    File.mkdir_p!(parent_workspace)
    File.mkdir_p!(child_workspace)
    git_init!(parent_workspace, "cycle/native-parent")
    git_init!(child_workspace, "cycle/native-child")

    workstream_path = Path.join(root, "software-change.yaml")
    write_agent_files!(root)
    write_workstream!(workstream_path)
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

    context = %{
      root: root,
      issue_id: "native-issue-1",
      parent_workspace: parent_workspace,
      child_workspace: child_workspace,
      workspace_root: workspace_root,
      workstream_path: workstream_path,
      config: %{
        "organization_id" => "native-org",
        "team_id" => "native-team",
        "app_user_id" => "native-app-user",
        "oauth_client_id" => "native-oauth-client",
        "webhook_secret_env" => "LINEAR_WEBHOOK_SECRET",
        "token_env" => "LINEAR_API_TOKEN",
        "store_path" => Path.join(root, "state/linear.sqlite"),
        "workspace_root" => workspace_root,
        "workstream_path" => workstream_path,
        "codex_command" => "codex app-server",
        "agent_id" => "default-cloud",
        "rig_label" => "factory:rig",
        "workspaces" => %{"native-issue-1" => parent_workspace},
        "child_workspaces" => %{"native-issue-1" => child_workspace}
      }
    }

    on_exit(fn -> File.rm_rf!(root) end)
    context
  end

  test "child workspace config is optional, validated, and pinned only when configured", context do
    assert {:ok, validated} = Delegation.validate(context.config)
    assert validated["child_workspaces"] == context.config["child_workspaces"]

    invalid = put_in(context.config["child_workspaces"][context.issue_id], "relative/child")
    assert {:error, :invalid_linear_delegation_config} = Delegation.validate(invalid)

    assert {:ok, prepared} = Delegation.prepare(issue(context), context.config, qualification_opts())
    assert Keyword.fetch!(prepared.execution, :child_workspace) == context.child_workspace

    without_mapping = Map.delete(context.config, "child_workspaces")
    assert {:ok, prepared_without_mapping} = Delegation.prepare(issue(context), without_mapping, qualification_opts())
    refute Keyword.has_key?(prepared_without_mapping.execution, :child_workspace)
  end

  test "only the same pinned Cloud Agent selection is qualified for a child", context do
    {parent, child, task, route} = prepared_pair(context)
    owner = self()

    opts = [
      linear_readiness: fn definition, agent ->
        send(owner, {:child_readiness, definition, agent})
        :ok
      end,
      linear_execution_control: fn definition, agent ->
        send(owner, {:child_execution_control, definition, agent})
        :ok
      end
    ]

    assert :ok = Delegation.qualified_child(child, parent, task, route, opts)
    assert_receive {:child_readiness, definition, "default-cloud"}
    assert definition == child.definition
    assert_receive {:child_execution_control, ^definition, "default-cloud"}

    changed_settings = put_in(child.definition.agent_selection.requested.reasoning_effort, "high")

    assert {:error, :native_child_agent_mismatch} =
             Delegation.qualified_child(changed_settings, parent, task, route, opts)

    changed_agent = put_in(child.definition.agents["default-cloud"].model, "gpt-6-astra")

    assert {:error, :native_child_agent_mismatch} =
             Delegation.qualified_child(changed_agent, parent, task, route, opts)

    wrong_agent_task = %{task | agent_id: "high-effort-cloud"}
    assert {:error, :native_child_agent_mismatch} = Delegation.qualified_child(child, parent, wrong_agent_task, route, opts)
  end

  test "child qualification requires identical service-secret exclusions and excludes configured credentials", context do
    {parent, child, task, route} = prepared_pair(context)

    changed_child = put_in(child.execution.secret_environment_names, ["LINEAR_API_TOKEN"])

    assert {:error, :child_credential_exclusions_mismatch} =
             Delegation.qualified_child(changed_child, parent, task, route, qualification_opts())

    unsafe_parent = put_in(parent.execution.secret_environment_names, [])
    unsafe_child = put_in(child.execution.secret_environment_names, [])

    assert {:error, :credential_environment_isolation_unavailable} =
             Delegation.qualified_child(unsafe_child, unsafe_parent, task, route, qualification_opts())
  end

  test "missing child workspace or a replaced non-clone fails closed", context do
    no_mapping = Map.delete(context.config, "child_workspaces")
    {parent, child, task, route} = prepared_pair(context, no_mapping)

    assert {:error, :child_workspace_unconfigured_or_mismatched} =
             Delegation.qualified_child(child, parent, task, route, qualification_opts())

    {parent, child, task, route} = prepared_pair(context)
    File.rm_rf!(Path.join(context.child_workspace, ".git"))

    assert {:error, :workspace_requires_dedicated_clone} =
             Delegation.qualified_child(child, parent, task, route, qualification_opts())
  end

  defp prepared_pair(context, config \\ nil) do
    config = config || context.config
    issue = issue(context)
    assert {:ok, prepared} = Delegation.prepare(issue, config, qualification_opts())

    assert {:ok, parent_definition, parent_workspace} =
             WorkstreamRunner.prepare(prepared.path, prepared.inputs, prepared.execution)

    parent_execution = Map.new(prepared.execution) |> Map.delete(:child_workspace)

    parent_execution =
      case Keyword.fetch(prepared.execution, :child_workspace) do
        {:ok, workspace} -> Map.put(parent_execution, :child_preparation, %{workspace: workspace})
        :error -> parent_execution
      end

    parent =
      WorkstreamRun.new(
        "native-parent",
        parent_definition,
        parent_workspace,
        [issue_id: context.issue_id, branch: "cycle/native-parent"],
        parent_execution
      )

    child_opts =
      prepared.execution
      |> Keyword.put(:workspace, context.child_workspace)
      |> Keyword.delete(:child_workspace)

    assert {:ok, child_definition, child_workspace} =
             WorkstreamRunner.prepare(prepared.path, prepared.inputs, child_opts)

    child =
      WorkstreamRun.new(
        "native-child",
        child_definition,
        child_workspace,
        [issue_id: context.issue_id, branch: "cycle/native-child"],
        Map.new(child_opts)
      )

    task = %{issue_id: context.issue_id, agent_id: prepared.agent_id}
    {parent, child, task, config}
  end

  defp issue(context) do
    %Issue{
      id: context.issue_id,
      identifier: "DEV-243",
      title: "Qualify a scoped native child",
      description: "Contract test only.",
      state: "Todo",
      state_type: "started",
      team_id: "native-team",
      delegate_id: "native-app-user",
      assignee_id: "human-owner",
      labels: ["factory:rig"],
      dispatchable: true
    }
  end

  defp qualification_opts do
    [
      linear_readiness: fn _definition, _agent -> :ok end,
      linear_execution_control: fn _definition, _agent -> :ok end
    ]
  end

  defp write_agent_files!(root) do
    File.write!(Path.join(root, "instructions.md"), "Test-only Cloud Agent instructions.\n")
    File.write!(Path.join(root, "SKILL.md"), "Test-only skill.\n")

    File.write!(Path.join(root, "default-cloud.yaml"), """
    version: 1
    name: DefaultCloud
    model: gpt-6-luna
    reasoning_effort: medium
    daybreak: false
    approval_policy: never
    sandbox: workspace-write
    instructions: [instructions.md]
    skills: [SKILL.md]
    """)
  end

  defp write_workstream!(path) do
    File.write!(path, """
    version: 1
    name: software-change
    inputs: [task]
    agents:
      default-cloud: default-cloud.yaml
    entry: implement
    stages:
      - id: implement
        type: agent
        inputs: [task]
        outputs: [candidate]
        agent: default-cloud
        prompt: Implement this contract fixture.
        next: inspect
      - id: inspect
        type: check
        inputs: [candidate]
        outputs: [child-evidence]
        gate:
          command: ["true"]
          timeout_ms: 1000
          success: complete
          failure: blocked
    """)
  end

  defp git_init!(workspace, branch) do
    {_output, 0} = System.cmd("git", ["init", "-b", branch, workspace], stderr_to_stdout: true)
    :ok
  end
end
