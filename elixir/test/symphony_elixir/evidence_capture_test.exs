defmodule SymphonyElixir.EvidenceCaptureTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{Config, Evidence, Workstream, WorkstreamRun, WorkstreamRunner}

  setup do
    root = Path.join(System.tmp_dir!(), "evidence-capture-#{System.unique_integer([:positive])}")
    workspace_root = Path.join(root, "workspaces")
    workspace = Path.join(workspace_root, "rig")
    File.mkdir_p!(workspace)
    {_, 0} = System.cmd("git", ["init", "--quiet", workspace])

    File.write!(Path.join(root, "SKILL.md"), "Use the captured test executor.")
    File.write!(Path.join(root, "instructions.md"), "Work in the disposable test clone.")

    File.write!(Path.join(root, "agent.yaml"), """
    version: 1
    name: capture-agent
    model: gpt-6-luna
    reasoning_effort: medium
    daybreak: false
    approval_policy: never
    sandbox: workspace-write
    instructions: [instructions.md]
    skills: [SKILL.md]
    """)

    workstream_path = Path.join(root, "workstream.yaml")

    File.write!(workstream_path, """
    version: 1
    name: capture-fixture
    inputs: [task]
    agents:
      worker: agent.yaml
    entry: implement
    stages:
      - id: implement
        type: agent
        inputs: [task]
        outputs: [candidate]
        agent: worker
        prompt: Produce the test fixture.
        next: validate
      - id: validate
        type: check
        inputs: [candidate]
        outputs: [check]
        gate:
          command: ["true"]
          timeout_ms: 1000
          success: complete
          failure: blocked
    """)

    workflow_path = Workflow.workflow_file_path()
    write_workflow_file!(workflow_path, workspace_root: workspace_root)
    assert :ok = WorkflowStore.force_reload()

    on_exit(fn ->
      write_workflow_file!(workflow_path)
      File.rm_rf!(root)
    end)

    %{root: root, workspace_root: workspace_root, workspace: workspace, workstream_path: workstream_path, workflow_path: workflow_path}
  end

  test "durable agent callbacks persist observable completed items and omit reasoning", context do
    evidence_config = enable_evidence!(context)
    start_evidence_store!(context, evidence_config)
    {definition, run} = durable_agent_run(context)
    assert :ok = Evidence.snapshot(run, "created", %{kind: :stage_started})

    executor = fn _workspace, _prompt, _issue, opts ->
      assert :ok = opts[:on_message].(completed_item("agentMessage", "Visible response"))
      assert :ok = opts[:on_message].(completed_item("reasoning", "private model thought"))
      {:ok, %{thread_id: "fixture-thread", turn_id: "fixture-turn", session_id: "fixture-session"}}
    end

    assert {:ok, result} =
             WorkstreamRunner.execute_stage(
               "implement",
               definition,
               run,
               workspace: context.workspace,
               workspace_root: context.workspace_root,
               agent_executor: executor
             )

    assert result.thread_id == "fixture-thread"
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id))
    observable = Enum.find(view.events, &(&1["kind"] == "item/completed"))
    assert observable["item"]["text"] == "Visible response"
    refute Jason.encode!(view.events) =~ "private model thought"
  end

  test "enabled evidence storage failure returns an explicit failed agent result", context do
    _evidence_config = enable_evidence!(context)
    assert Process.whereis(Evidence) == nil
    {definition, run} = durable_agent_run(context)

    executor = fn _workspace, _prompt, _issue, opts ->
      opts[:on_message].(completed_item("agentMessage", "This result must not succeed"))
      {:ok, %{thread_id: "fixture-thread"}}
    end

    assert {:error, :evidence_capture_failed} =
             WorkstreamRunner.execute_stage(
               "implement",
               definition,
               run,
               workspace: context.workspace,
               workspace_root: context.workspace_root,
               agent_executor: executor
             )
  end

  test "disabled local run keeps the existing successful behavior", context do
    assert Config.evidence() == :disabled

    executor = fn workspace, _prompt, _issue, opts ->
      assert :ok = opts[:on_message].(completed_item("agentMessage", "Local response"))
      File.write!(Path.join(workspace, "candidate.txt"), "candidate")
      {:ok, %{thread_id: "local-thread"}}
    end

    assert {:ok, report} =
             WorkstreamRunner.run(
               context.workstream_path,
               %{"task" => "local fixture"},
               workspace: context.workspace,
               workspace_root: context.workspace_root,
               agent_executor: executor
             )

    assert report.status == :complete
    assert report.outputs["candidate"].thread_id == "local-thread"
  end

  defp enable_evidence!(context) do
    evidence_root = Path.join(context.root, "private-evidence")
    content = File.read!(context.workflow_path)
    [front_matter, prompt] = String.split(content, "\n---\n", parts: 2)

    evidence_yaml = """
    evidence:
      root: #{Jason.encode!(evidence_root)}
      base_url: "https://evidence.example.com"
      iap_audience: "/projects/292978199748/global/backendServices/123456789"
      allowed_domains: ["rig.ai"]
      allowed_emails: []
    """

    File.write!(context.workflow_path, front_matter <> "\n" <> evidence_yaml <> "---\n" <> prompt)
    assert :ok = WorkflowStore.force_reload()
    assert {:ok, config} = Config.evidence()
    refute String.starts_with?(config["root"], context.workspace_root <> "/")
    config
  end

  defp start_evidence_store!(context, config) do
    assert Process.whereis(Evidence) == nil
    start_supervised!({Evidence, [config: {:ok, config}, receipt_uid: File.stat!(context.root).uid]})
  end

  defp durable_agent_run(context) do
    assert {:ok, definition} = Workstream.load(context.workstream_path, %{"task" => "capture fixture"})

    run =
      WorkstreamRun.new("linear/evidence-capture", definition, context.workspace,
        workspace_root: context.workspace_root,
        branch: "cycle/evidence-capture"
      )
      |> WorkstreamRun.start_stage()

    {definition, run}
  end

  defp completed_item(type, text) do
    %{
      payload: %{
        "method" => "item/completed",
        "params" => %{"item" => %{"id" => "fixture-item", "type" => type, "text" => text}}
      },
      session_id: "fixture-session",
      thread_id: "fixture-thread",
      turn_id: "fixture-turn"
    }
  end
end
