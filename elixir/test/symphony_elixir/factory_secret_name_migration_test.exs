Code.require_file(Path.expand("../../../factory/deploy/pin_linear_secret_names.exs", __DIR__))

defmodule SymphonyElixir.FactorySecretNameMigrationTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{AgentRuntimeSupervisor, Config, Evidence, WorkstreamRun, WorkstreamStore, Workflow, WorkflowStore}
  alias SymphonyElixir.Factory.PinLinearSecretNames

  @configured_names [
    "FACTORY_DAYBREAK_CLIENT_SECRET",
    "FACTORY_DAYBREAK_WEBHOOK_SECRET",
    "FACTORY_HIGH_CLIENT_SECRET",
    "FACTORY_HIGH_WEBHOOK_SECRET",
    "LINEAR_API_KEY",
    "LINEAR_API_TOKEN"
  ]

  setup do
    root = Path.join(System.tmp_dir!(), "secret-name-migration-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    previous_secret_names = System.get_env("FACTORY_LINEAR_SECRET_NAMES_JSON")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      Workflow.set_workflow_file_path(Path.expand("../fixtures/startup_workflow.md", __DIR__))
      assert :ok = WorkflowStore.force_reload()

      restore_env("FACTORY_LINEAR_SECRET_NAMES_JSON", previous_secret_names)
      restore_env("PATH", previous_path)
      File.rm_rf!(root)
    end)

    %{root: root, store_path: Path.join(root, "state/runs.sqlite3"), workflow_path: Path.join(root, "WORKFLOW.md")}
  end

  test "the --no-start CLI adds configured secret names and its event replays in a fresh VM", context do
    assert WorkstreamRun.policy() === PinLinearSecretNames.supported_policy()
    write_workflow(context, :linear)
    configure_secret_names_environment()
    fake_systemctl = install_inactive_systemctl(context.root)
    System.put_env("PATH", fake_systemctl <> ":" <> System.get_env("PATH", ""))

    run =
      waiting_run("run-dev235-cli", "linear/11111111-1111-4111-8111-111111111111", [
        "LINEAR_API_KEY",
        "LINEAR_API_TOKEN",
        "FACTORY_LINEAR_SECRET_NAMES_JSON"
      ])
      |> with_historical_app_server_atom()

    store = start_store(context.store_path)
    assert :ok = WorkstreamStore.commit(store, run, "initial-human-wait", %{"kind" => "queued"})
    assert :ok = GenServer.stop(store)

    evidence = start_evidence()
    store = start_store(context.store_path)
    assert :ok = Evidence.sync!(store)
    assert {:ok, []} = WorkstreamStore.pending_evidence(store)
    assert :ok = GenServer.stop(store)
    assert :ok = GenServer.stop(evidence)

    cli_args = [
      "--apply",
      "--workflow-path",
      context.workflow_path,
      "--store-path",
      context.store_path,
      "--run-id",
      run.id,
      "--expected-status",
      "waiting_for_answer",
      "--expected-stage-id",
      run.stage_id,
      "--expected-attempt-id",
      run.current_attempt_id,
      "--expected-wait-id",
      run.pending_wait.id,
      "--operator",
      "DEV-235"
    ]

    assert %{
             "outcome" => "committed",
             "run_id" => result_run_id,
             "environment_names" => names,
             "event_id" => event_id,
             "app_server_was_cold" => true,
             "app_server_loaded_after_cli" => true,
             "orchestrator_started" => false
           } =
             invoke_helper_cli(context, cli_args)

    assert result_run_id == run.id
    assert names == merged_names(run.execution.secret_environment_names)

    readback = invoke_fresh_store_reader(context, run.id, event_id)
    assert readback["run_id"] == run.id
    assert readback["status"] == "waiting_for_answer"
    assert readback["attempt_id"] == run.current_attempt_id
    assert readback["wait_id"] == run.pending_wait.id
    assert readback["names"] == names
    assert readback["outbox_count"] == 1
    assert readback["event"]["kind"] == "operator_service_secret_environment_names_pinned"
    assert Enum.all?(Map.keys(readback["event"]), &is_binary/1)
    assert readback["event"]["operator"] == "DEV-235"

    assert %{"outcome" => "duplicate", "run_id" => duplicate_run_id, "event_id" => ^event_id} = invoke_helper_cli(context, cli_args)
    assert duplicate_run_id == run.id
    assert %{"outbox_count" => 1} = invoke_fresh_store_reader(context, run.id, event_id)

    evidence = start_evidence()
    store = start_store(context.store_path)
    assert :ok = Evidence.sync!(store)
    assert {:ok, []} = WorkstreamStore.pending_evidence(store)
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id))
    assert view.snapshot["stage_id"] == run.stage_id
    assert view.snapshot["current_attempt_id"] == run.current_attempt_id
    assert view.snapshot["pending_wait"]["id"] == run.pending_wait.id
    assert view.snapshot["policy"] == Jason.decode!(Jason.encode!(run.policy))
    assert :ok = GenServer.stop(store)
    assert :ok = GenServer.stop(evidence)
  end

  test "a changed executor policy blocks a saved human wait and retains its evidence identity", context do
    write_workflow(context, :evidence_only)
    evidence = start_evidence()

    original =
      waiting_run("run-dev235-policy-mismatch", "linear/22222222-2222-4222-8222-222222222222", ["LINEAR_API_KEY"])

    incompatible = put_in(original.policy.executor_sha256, "466781f24c50eef16dd357abf0f3225f5e8292486a7c42da9d0989036e453555")
    store = start_store(context.store_path)
    assert :ok = WorkstreamStore.commit(store, incompatible, "saved-human-wait", %{"kind" => "queued"})
    assert :ok = Evidence.sync!(store)
    assert :ok = GenServer.stop(store)

    suffix = System.unique_integer([:positive])
    supervisor_name = Module.concat(__MODULE__, "PolicyMismatchRuntime#{suffix}")
    coordinator_name = Module.concat(__MODULE__, "PolicyMismatchCoordinator#{suffix}")
    tasks_name = Module.concat(__MODULE__, "PolicyMismatchTasks#{suffix}")

    assert {:ok, supervisor} =
             AgentRuntimeSupervisor.start_link(
               name: supervisor_name,
               orchestrator_name: coordinator_name,
               task_supervisor_name: tasks_name,
               workstream_store_path: context.store_path,
               auto_advance: false
             )

    on_exit(fn -> if Process.alive?(supervisor), do: Supervisor.stop(supervisor) end)

    state = :sys.get_state(coordinator_name)
    blocked = state.workstreams.runs[original.id]
    assert blocked.status == :policy_blocked
    assert blocked.phase == :reconciling
    assert blocked.policy == incompatible.policy
    assert blocked.stage_id == original.stage_id
    assert blocked.attempts == original.attempts
    assert blocked.current_attempt_id == original.current_attempt_id
    assert blocked.pending_wait == original.pending_wait
    assert blocked.definition == original.definition
    assert Evidence.identity(blocked) == Evidence.identity(incompatible)

    assert {:ok, persisted} = WorkstreamStore.fetch(state.workstreams.store, original.id)
    assert persisted == blocked
    assert {:ok, [view]} = Evidence.task(Evidence.task_key(original.task_id))
    assert view.snapshot["status"] == "policy_blocked"
    assert view.snapshot["policy"] == Jason.decode!(Jason.encode!(incompatible.policy))
    assert view.snapshot["stage_id"] == original.stage_id
    assert view.snapshot["current_attempt_id"] == original.current_attempt_id
    assert view.snapshot["pending_wait"]["id"] == original.pending_wait.id
    assert Enum.any?(view.events, &(&1["kind"] == "incompatible_service_policy"))

    assert :ok = Supervisor.stop(supervisor)
    assert :ok = GenServer.stop(evidence)
  end

  test "an active operation leaves the run, event journal, and evidence outbox untouched", context do
    write_workflow(context, :evidence_only)

    run =
      waiting_run("run-dev235-active-operation", "linear/33333333-3333-4333-8333-333333333333", [
        "LINEAR_API_KEY",
        "LINEAR_API_TOKEN",
        "FACTORY_LINEAR_SECRET_NAMES_JSON"
      ])
      |> put_in([:operations, "still-active"], %{status: :executing})

    store = start_store(context.store_path)
    assert :ok = WorkstreamStore.commit(store, run, "active-operation-wait", %{"kind" => "queued"})
    assert {:ok, pending_before} = WorkstreamStore.pending_evidence(store)

    configured_names = Enum.sort(@configured_names ++ ["FACTORY_LINEAR_SECRET_NAMES_JSON"])

    expected = %{
      expected_status: :waiting_for_answer,
      expected_stage_id: run.stage_id,
      expected_attempt_id: run.current_attempt_id,
      expected_wait_id: run.pending_wait.id
    }

    assert {:error, :run_not_quiescent} =
             PinLinearSecretNames.pin_run(store, run.id, configured_names, expected,
               operator: "DEV-235",
               service_check: fn -> :ok end
             )

    event_id =
      "operator/linear-secret-names/" <>
        (:crypto.hash(:sha256, Jason.encode!([run.id, merged_names(run.execution.secret_environment_names), "DEV-235"]))
         |> Base.encode16(case: :lower))

    assert :not_found = WorkstreamStore.event(store, event_id)
    assert {:ok, ^run} = WorkstreamStore.fetch(store, run.id)
    assert {:ok, ^pending_before} = WorkstreamStore.pending_evidence(store)
    assert :ok = GenServer.stop(store)
  end

  defp write_workflow(context, mode) do
    evidence_root = Path.join(context.root, "private-evidence")

    linear_config =
      if mode == :linear do
        """
        linear_delegation:
          organization_id: rig
          team_id: dev
          app_user_id: default-app
          oauth_client_id: default-oauth
          webhook_secret_env: LINEAR_API_TOKEN
          token_env: LINEAR_API_KEY
          store_path: #{Jason.encode!(context.store_path)}
          workspace_root: #{Jason.encode!(Path.join(context.root, "workspaces"))}
          workstream_path: #{Jason.encode!(Path.join(context.root, "software-change.yaml"))}
          agent_id: default-cloud
          rig_label: factory:rig
          cloud_agents:
            - app_user_id: high-app
              oauth_client_id: high-oauth
              webhook_secret_env: FACTORY_HIGH_WEBHOOK_SECRET
              client_secret_env: FACTORY_HIGH_CLIENT_SECRET
              agent_id: high-effort-cloud
            - app_user_id: daybreak-app
              oauth_client_id: daybreak-oauth
              webhook_secret_env: FACTORY_DAYBREAK_WEBHOOK_SECRET
              client_secret_env: FACTORY_DAYBREAK_CLIENT_SECRET
              agent_id: daybreak-cloud
        """
      else
        ""
      end

    :ok = SymphonyElixir.TestSupport.write_workflow_file!(context.workflow_path)
    [front_matter, prompt] = String.split(File.read!(context.workflow_path), "\n---\n", parts: 2)

    File.write!(
      context.workflow_path,
      front_matter <>
        "\n" <>
        linear_config <>
        """
        evidence:
          root: #{Jason.encode!(evidence_root)}
          base_url: "https://evidence.example.com"
          iap_audience: "/projects/123456789/global/backendServices/123456789"
          allowed_domains: ["rig.ai"]
        ---
        #{prompt}
        """
    )

    :ok = Workflow.set_workflow_file_path(context.workflow_path)
    if Process.whereis(WorkflowStore), do: assert(:ok = WorkflowStore.force_reload())
    assert {:ok, _} = Config.evidence()
    if mode == :linear, do: assert({:ok, _} = Config.linear_delegation())
  end

  defp configure_secret_names_environment do
    System.put_env("FACTORY_LINEAR_SECRET_NAMES_JSON", Jason.encode!(@configured_names))
  end

  defp install_inactive_systemctl(root) do
    executable = Path.join([root, "bin", "systemctl"])
    File.mkdir_p!(Path.dirname(executable))
    File.write!(executable, "#!/bin/sh\nprintf 'inactive\\n'\n")
    File.chmod!(executable, 0o755)
    Path.dirname(executable)
  end

  defp waiting_run(id, task_id, secret_names) do
    definition = %{
      name: "dev235-human-wait",
      inputs: [],
      input_values: %{"candidate" => "candidate content"},
      entry: "candidate-ready",
      agents: %{},
      definitions: %{"workstream.yaml" => %{sha256: String.duplicate("a", 64), text: "pinned workstream fixture"}},
      stages: %{
        "candidate-ready" => %{
          id: "candidate-ready",
          type: :human_wait,
          inputs: ["candidate"],
          outputs: ["answer"],
          prompt: "Review this candidate.",
          next: "complete"
        }
      }
    }

    WorkstreamRun.new(task_id, definition, "/tmp/" <> id, [branch: "cycle/dev235"], %{secret_environment_names: secret_names})
    |> Map.put(:id, id)
    |> Map.put(:artifacts, %{"candidate" => "sha256:candidate"})
    |> WorkstreamRun.start_stage()
  end

  defp with_historical_app_server_atom(run) do
    [wait_attempt] = run.attempts
    previous_attempt_id = run.id <> "/implementation/1"
    current_attempt_id = run.id <> "/" <> run.stage_id <> "/2"

    previous_attempt = %{
      id: previous_attempt_id,
      stage: "implementation",
      type: :agent,
      ordinal: 1,
      status: :completed,
      result: %{status: :error, reason: :agent_settings_override},
      gate: nil,
      gate_detail: nil
    }

    previous_operation = %{
      id: previous_attempt_id,
      attempt_id: previous_attempt_id,
      stage: "implementation",
      type: :agent,
      status: :completed,
      worker: nil,
      side_effect_id: previous_attempt_id,
      transport_retries: 0
    }

    wait_attempt = %{wait_attempt | id: current_attempt_id, ordinal: 2}

    run
    |> Map.put(:attempts, [previous_attempt, wait_attempt])
    |> Map.put(:current_attempt_id, current_attempt_id)
    |> Map.put(:operations, %{previous_attempt_id => previous_operation})
    |> put_in([:pending_wait, :id], current_attempt_id <> "/wait")
  end

  defp start_store(path) do
    assert {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    on_exit(fn -> if Process.alive?(store), do: GenServer.stop(store) end)
    store
  end

  defp start_evidence do
    assert {:ok, config} = Config.evidence()
    assert {:ok, evidence} = Evidence.start_link(config: {:ok, config})
    on_exit(fn -> if Process.alive?(evidence), do: GenServer.stop(evidence) end)
    evidence
  end

  defp invoke_helper_cli(context, args) do
    driver = Path.join(context.root, "helper-driver.exs")

    File.write!(driver, """
    Code.require_file(System.fetch_env!("DEV235_HELPER_PATH"))
    args = System.fetch_env!("DEV235_CLI_ARGS_JSON") |> Jason.decode!()
    app_server_was_cold = :code.is_loaded(SymphonyElixir.Codex.AppServer) == false

    case SymphonyElixir.Factory.PinLinearSecretNames.run_cli(args) do
      {:ok, result} ->
        IO.puts(Jason.encode!(%{
          outcome: result.outcome,
          run_id: result.run_id,
          event_id: result.event_id,
          environment_names: result.environment_names,
          app_server_was_cold: app_server_was_cold,
          app_server_loaded_after_cli: :code.is_loaded(SymphonyElixir.Codex.AppServer) != false,
          orchestrator_started: not is_nil(Process.whereis(SymphonyElixir.Orchestrator))
        }))
      {:error, reason} ->
        IO.puts(:stderr, inspect(reason))
        System.halt(1)
    end
    """)

    env = [
      {"DEV235_HELPER_PATH", helper_path()},
      {"DEV235_CLI_ARGS_JSON", Jason.encode!(args)},
      {"FACTORY_LINEAR_SECRET_NAMES_JSON", Jason.encode!(@configured_names)},
      {"PATH", System.get_env("PATH", "")}
    ]

    {output, status} =
      System.cmd(
        "mix",
        ["run", "--no-start", "--no-compile", "--no-deps-check", driver],
        cd: elixir_path(),
        env: env,
        stderr_to_stdout: true
      )

    assert status == 0, output
    output |> String.split("\n", trim: true) |> List.last() |> Jason.decode!()
  end

  defp invoke_fresh_store_reader(context, run_id, event_id) do
    reader = Path.join(context.root, "fresh-store-reader.exs")

    File.write!(reader, """
    for app <- [:yaml_elixir, :jason, :exqlite], do: {:ok, _} = Application.ensure_all_started(app)
    for module <- Application.spec(:symphony_elixir, :modules) || [], do: {:module, ^module} = Code.ensure_loaded(module)
    true = is_nil(Process.whereis(SymphonyElixir.Orchestrator))
    {:ok, store} = SymphonyElixir.WorkstreamStore.start_link(path: System.fetch_env!("DEV235_STORE_PATH"), owner: self())
    {:ok, run} = SymphonyElixir.WorkstreamStore.fetch(store, System.fetch_env!("DEV235_RUN_ID"))
    {:ok, %{run_id: run_id, event: event}} = SymphonyElixir.WorkstreamStore.event(store, System.fetch_env!("DEV235_EVENT_ID"))
    {:ok, pending} = SymphonyElixir.WorkstreamStore.pending_evidence(store)

    IO.puts(Jason.encode!(%{
      run_id: run.id,
      status: run.status,
      attempt_id: run.current_attempt_id,
      wait_id: run.pending_wait.id,
      names: run.execution.secret_environment_names,
      event_run_id: run_id,
      event: event,
      outbox_count: length(pending),
      outbox_event_id: hd(pending).event_id
    }))

    GenServer.stop(store)
    """)

    env = [
      {"DEV235_STORE_PATH", context.store_path},
      {"DEV235_RUN_ID", run_id},
      {"DEV235_EVENT_ID", event_id}
    ]

    {output, status} =
      System.cmd(
        "mix",
        ["run", "--no-start", "--no-compile", "--no-deps-check", reader],
        cd: elixir_path(),
        env: env,
        stderr_to_stdout: true
      )

    assert status == 0, output
    output |> String.split("\n", trim: true) |> List.last() |> Jason.decode!()
  end

  defp helper_path, do: Path.expand("../../../factory/deploy/pin_linear_secret_names.exs", __DIR__)
  defp elixir_path, do: Path.expand("../..", __DIR__)
  defp merged_names(existing), do: existing ++ Enum.reject(@configured_names ++ ["FACTORY_LINEAR_SECRET_NAMES_JSON"], &(&1 in existing))
  defp restore_env(name, nil), do: System.delete_env(name)
  defp restore_env(name, value), do: System.put_env(name, value)
end
