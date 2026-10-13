defmodule SymphonyElixir.WorkstreamRetryMaintenanceTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{Tracker.Issue, WorkstreamRun, WorkstreamRetryMaintenance, WorkstreamStore}

  @issue_id "linear-issue-259"
  @session_id "linear-session-259"
  @activity_id "retry-activity-259"
  @attempt_id "run-259/implement/1"
  @run_id "run-259"
  @app_user_id "linear-app-user-1"
  @oauth_client_id "linear-oauth-client-1"
  @organization_id "linear-org-1"
  @worker_control %{
    "host" => "worker-a",
    "machine_id" => String.duplicate("a", 32),
    "service_revision" => String.duplicate("a", 40),
    "release_sha256" => String.duplicate("b", 64),
    "argv" => ["/opt/rig/bin/codex", "app-server"]
  }

  setup do
    root = Path.join(System.tmp_dir!(), "workstream-retry-#{System.unique_integer([:positive])}")
    path = Path.join(root, "private/state.sqlite3")
    File.mkdir_p!(Path.dirname(path))

    config = %{
      "organization_id" => @organization_id,
      "team_id" => "linear-dev-team",
      "app_user_id" => @app_user_id,
      "oauth_client_id" => @oauth_client_id,
      "webhook_secret_env" => "LINEAR_WEBHOOK_SECRET",
      "client_secret_env" => "LINEAR_CLIENT_SECRET",
      "store_path" => path,
      "workspace_root" => Path.join(root, "workspaces"),
      "workstream_path" => Path.join(root, "software-change.yaml"),
      "agent_id" => "default-cloud",
      "rig_label" => "factory:rig",
      "worker_control" => @worker_control
    }

    context = %{root: root, path: path, config: config, run: blocked_run(config), task: linear_task(), record: rejected_record()}
    seed(context)

    on_exit(fn -> File.rm_rf!(root) end)
    context
  end

  test "plan is read-only; apply requeues the same pinned run once across a helper restart", c do
    pins = Map.take(c.run, [:policy, :definition, :execution, :workspace, :branch, :attempts, :operations])
    opts = request(c, :plan, terminal_status(c.run))

    assert {:ok, %{status: :planned, next_attempt_ordinal: 2, stage: "implement"}} = WorkstreamRetryMaintenance.run(opts)
    assert {:ok, unchanged} = fetch(c.path, @run_id)
    assert unchanged == c.run

    assert {:ok, %{status: :queued, run_id: @run_id}} = WorkstreamRetryMaintenance.run(Keyword.put(opts, :mode, :apply))
    assert {:ok, queued} = fetch(c.path, @run_id)
    assert queued.status == :ready
    assert queued.phase == :queued
    assert queued.stage_id == "implement"
    assert queued.current_attempt_id == nil
    assert Map.take(queued, Map.keys(pins)) == pins

    assert {:ok, %{status: :already_applied}} = WorkstreamRetryMaintenance.run(Keyword.put(opts, :mode, :apply))
    assert {:ok, after_restart} = fetch(c.path, @run_id)
    assert after_restart.attempts == c.run.attempts
    assert after_restart.operations == c.run.operations

    {:ok, store} = start_store(c.path)
    assert {:ok, %{run_id: @run_id, event: event}} = WorkstreamStore.event(store, "operator-retry/" <> @activity_id)
    assert event.kind == :operator_precode_retry_queued
    assert String.match?(event.helper_source_sha256, ~r/\A[0-9a-f]{64}\z/)
    assert event.attempt_id == @attempt_id
    assert event.operator == "adam"
    assert event.termination["status"] == "terminated"
    assert :ok = GenServer.stop(store)
  end

  test "an active coordinator keeps exclusive ownership and prevents a second writer", c do
    {:ok, store} = start_store(c.path)
    before = File.read!(c.path)

    assert {:error, :operator_retry_store_owned} = WorkstreamRetryMaintenance.run(request(c, :apply, terminal_status(c.run)))
    assert {:ok, unchanged} = WorkstreamStore.fetch(store, @run_id)
    assert unchanged == c.run
    assert File.read!(c.path) == before
    assert :ok = GenServer.stop(store)
  end

  test "an unrelated or malformed Linear activity is not authorization", c do
    invalid = put_in(c.record, [:event, :body], "retry another-run another-attempt")
    replace_record(c, invalid)

    assert {:error, :operator_retry_not_authorized} = WorkstreamRetryMaintenance.run(request(c, :apply, terminal_status(c.run)))
    assert {:ok, unchanged} = fetch(c.path, @run_id)
    assert unchanged.status == :blocked
  end

  test "a normal prompt with no signal or continue is accepted; auth and select are not", c do
    for signal <- [nil, "continue"] do
      record = put_in(c.record, [:event, :signal], signal)
      replace_record(c, record)

      assert {:ok, %{status: :planned}} =
               WorkstreamRetryMaintenance.run(request(c, :plan, terminal_status(c.run)))
    end

    for signal <- ["auth", "select"] do
      record = put_in(c.record, [:event, :signal], signal)
      replace_record(c, record)

      assert {:error, :operator_retry_not_authorized} =
               WorkstreamRetryMaintenance.run(request(c, :plan, terminal_status(c.run)))
    end
  end

  test "a stopped delegation cannot authorize retry even when the message and proof are valid", c do
    {:ok, store} = start_store(c.path)
    stopped = Map.put(c.task, :status, :stopped)
    assert :ok = WorkstreamStore.linear_task(store, stopped)
    assert :ok = GenServer.stop(store)

    assert {:error, :operator_retry_not_authorized} = WorkstreamRetryMaintenance.run(request(c, :apply, terminal_status(c.run)))
    assert {:ok, unchanged} = fetch(c.path, @run_id)
    assert unchanged.status == :blocked
  end

  test "a current Linear assignee mismatch is denied", c do
    opts = request(c, :apply, terminal_status(c.run))
    opts = Keyword.put(opts, :linear_opts, linear_opts("someone-else"))

    assert {:error, :operator_retry_not_authorized} = WorkstreamRetryMaintenance.run(opts)
    assert {:ok, unchanged} = fetch(c.path, @run_id)
    assert unchanged.status == :blocked
  end

  test "running, unknown and malformed worker status never make a retry eligible", c do
    for status <- [:running, :unknown, :malformed] do
      payload =
        case status do
          :running -> %{"operation_id" => @attempt_id, "status" => "running", "identity" => worker_identity(@attempt_id)}
          :unknown -> %{"operation_id" => @attempt_id, "status" => "unknown", "identity" => worker_identity(@attempt_id)}
          :malformed -> Map.put(terminal_status(c.run), "identity", %{"operation_id" => "other-operation"})
        end

      assert {:error, :worker_termination_unknown} = WorkstreamRetryMaintenance.run(request(c, :apply, payload))
      assert {:ok, unchanged} = fetch(c.path, @run_id)
      assert unchanged.status == :blocked
    end
  end

  test "a different failed stage attempt or changed policy cannot be retried", c do
    {:ok, store} = start_store(c.path)
    incompatible = %{c.run | policy: %{version: 0}}
    assert :ok = WorkstreamStore.commit(store, incompatible, "replace-run", %{kind: :fixture})
    assert :ok = GenServer.stop(store)

    assert {:error, :operator_retry_run_not_eligible} = WorkstreamRetryMaintenance.run(request(c, :apply, terminal_status(c.run)))
    assert {:ok, unchanged} = fetch(c.path, @run_id)
    assert unchanged.status == :blocked
  end

  defp seed(c) do
    {:ok, store} = start_store(c.path)
    assert :ok = WorkstreamStore.commit(store, c.run, "initial-run", %{kind: :task_queued})
    assert :ok = WorkstreamStore.linear_receive(store, c.record.id, c.record.event)
    assert :ok = WorkstreamStore.linear_commit(store, c.record, c.task)
    assert :ok = GenServer.stop(store)
  end

  defp replace_record(c, record) do
    :ok = File.rm(c.path)
    seed(%{c | record: record})
  end

  defp start_store(path), do: WorkstreamStore.start_link(path: path, owner: self())

  defp fetch(path, run_id) do
    {:ok, store} = start_store(path)
    result = WorkstreamStore.fetch(store, run_id)
    :ok = GenServer.stop(store)
    result
  end

  defp request(c, mode, payload) do
    [
      mode: mode,
      store_path: c.path,
      run_id: @run_id,
      attempt_id: @attempt_id,
      activity_id: @activity_id,
      operator: "adam",
      linear_config: c.config,
      linear_opts: linear_opts("human-owner-17"),
      worker_operation_opts: [rpc_fun: fn _config, _request -> {:ok, payload} end]
    ]
  end

  defp linear_opts(assignee_id) do
    [
      linear_issue_fetcher: fn issue_id, _config ->
        {:ok,
         %Issue{
           id: issue_id,
           identifier: "DEV-259",
           title: "Retry a pre-code attempt",
           team_id: "linear-dev-team",
           delegate_id: @app_user_id,
           assignee_id: assignee_id,
           labels: ["factory:rig"],
           state: "In Progress",
           state_type: "started"
         }}
      end,
      linear_session_fetcher: fn session_id, _config ->
        {:ok, %{"id" => session_id, "issueId" => @issue_id, "appUser" => %{"id" => @app_user_id}, "dismissedAt" => nil}}
      end
    ]
  end

  defp blocked_run(config) do
    definition = %{
      name: "software-change",
      inputs: ["task"],
      input_values: %{"task" => "DEV-259 retry test"},
      entry: "implement",
      agents: %{"default-cloud" => %{model: "gpt-6-luna", reasoning_effort: "medium", daybreak: false}},
      definitions: %{"workstream" => %{sha256: String.duplicate("d", 64)}},
      stages: %{
        "implement" => %{id: "implement", type: :agent, agent: "default-cloud", inputs: ["task"], outputs: ["candidate"], next: "validate"},
        "validate" => %{id: "validate", type: :check, outputs: ["validation"], gate: %{command: ["true"], success: :complete, failure: :blocked}}
      }
    }

    WorkstreamRun.new("linear/" <> @issue_id, definition, Path.join(config["workspace_root"], "DEV-259"),
      workspace_root: config["workspace_root"],
      branch: "cycle/dev259-retry",
      issue_id: @issue_id,
      worker_control: @worker_control
    )
    |> WorkstreamRun.start_stage()
    |> WorkstreamRun.record_worker(%{"worker" => "registered"})
    |> WorkstreamRun.finish_stage({:error, :agent_execution_failed})
    |> put_external_process()
    |> Map.put(:id, @run_id)
    |> update_attempt_id(@attempt_id)
  end

  defp put_external_process(run) do
    operation_id = run.current_attempt_id || List.last(run.attempts).id
    operation = run.operations[operation_id] |> Map.put(:external_process, worker_identity(operation_id))
    %{run | operations: Map.put(run.operations, operation_id, operation)}
  end

  defp update_attempt_id(run, new_id) do
    old_id = hd(run.attempts).id
    attempt = %{hd(run.attempts) | id: new_id}
    operation = run.operations[old_id] |> Map.put(:id, new_id) |> Map.put(:attempt_id, new_id) |> Map.put(:external_process, worker_identity(new_id))

    %{run | attempts: [attempt], operations: %{new_id => operation}}
  end

  defp worker_identity(operation_id) do
    unit = "factory-operation-#{Base.encode16(:crypto.hash(:sha256, operation_id), case: :lower)}.service"

    %{
      "kind" => "systemd-unit",
      "operation_id" => operation_id,
      "machine_id" => @worker_control["machine_id"],
      "boot_id" => String.duplicate("1", 32),
      "unit" => unit,
      "invocation_id" => String.duplicate("2", 32),
      "control_group" => "/system.slice/" <> unit,
      "request_sha256" => String.duplicate("c", 64)
    }
  end

  defp terminal_status(run) do
    operation_id = List.last(run.attempts).id
    identity = worker_identity(operation_id)

    proof = %{
      "machine_id" => identity["machine_id"],
      "boot_id" => identity["boot_id"],
      "unit" => identity["unit"],
      "invocation_id" => identity["invocation_id"],
      "control_group" => identity["control_group"],
      "cgroup_state" => "present",
      "cgroup_populated" => 0,
      "active_state" => "inactive",
      "sub_state" => "dead",
      "exec_main_code" => "exited",
      "exec_main_status" => 1,
      "main_pid" => 0,
      "observed_at" => "2026-10-13T00:00:00Z",
      "systemd_version" => 254
    }

    %{
      "operation_id" => operation_id,
      "status" => "terminated",
      "identity" => identity,
      "termination_proof" => proof,
      "exit_status" => 1,
      "exit_code" => "exited"
    }
  end

  defp linear_task do
    %{
      issue_id: @issue_id,
      run_id: @run_id,
      status: :acknowledged,
      session_id: @session_id,
      assignee_id: "human-owner-17",
      agent_id: "default-cloud",
      route: %{
        "organization_id" => @organization_id,
        "app_user_id" => @app_user_id,
        "oauth_client_id" => @oauth_client_id,
        "agent_id" => "default-cloud"
      },
      event: %{timestamp: 1_800_000_000_000},
      publications: %{},
      scope_revision: 0
    }
  end

  defp rejected_record do
    event = %{
      id: "retry-delivery-259",
      kind: :prompted,
      issue_id: @issue_id,
      session_id: @session_id,
      app_user_id: @app_user_id,
      verified_app_user_id: @app_user_id,
      oauth_client_id: @oauth_client_id,
      organization_id: @organization_id,
      timestamp: 1_800_000_001_000,
      activity_id: @activity_id,
      signal: "continue",
      body: "retry #{@run_id} #{@attempt_id}"
    }

    %{id: event.id, event: event, status: :reply_rejected}
  end
end
