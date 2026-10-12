defmodule SymphonyElixir.WorkerOperationTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{WorkerOperation, Workstream}

  @config %{
    "host" => "worker-a",
    "machine_id" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "service_revision" => String.duplicate("a", 40),
    "release_sha256" => String.duplicate("b", 64),
    "argv" => ["/opt/rig/bin/codex", "app-server"]
  }
  @operation_id "run-1/check/1"
  @workspace "/tmp/worker-operation-workspace"

  test "validates only a bounded worker config with an absolute executable" do
    assert :ok = WorkerOperation.validate_config(@config)

    assert {:error, :invalid_worker_machine_id} =
             WorkerOperation.validate_config(Map.put(@config, "machine_id", "machine-a"))

    assert {:error, :invalid_worker_control_config} =
             WorkerOperation.validate_config(Map.put(@config, "extra", true))

    assert {:error, :worker_argv_must_start_with_absolute_executable} =
             WorkerOperation.validate_config(Map.put(@config, "argv", ["codex", "app-server"]))
  end

  test "qualifies only the configured machine, revision and release" do
    assert :ok = WorkerOperation.qualify(@config, rpc_fun: fn _config, _request -> {:ok, capabilities()} end)

    no_receipt = Map.delete(capabilities(), "containment_qualified")

    assert {:error, :worker_not_qualified} =
             WorkerOperation.qualify(@config, rpc_fun: fn _config, _request -> {:ok, no_receipt} end)

    wrong_release = Map.put(capabilities(), "release_sha256", String.duplicate("c", 64))

    assert {:error, :worker_not_qualified} =
             WorkerOperation.qualify(@config, rpc_fun: fn _config, _request -> {:ok, wrong_release} end)

    old_systemd = Map.put(capabilities(), "systemd_version", 251)

    assert {:error, :worker_not_qualified} =
             WorkerOperation.qualify(@config, rpc_fun: fn _config, _request -> {:ok, old_systemd} end)

    compatible_systemd = Map.put(capabilities(), "systemd_version", 252)
    assert :ok = WorkerOperation.qualify(@config, rpc_fun: fn _config, _request -> {:ok, compatible_systemd} end)
  end

  test "registers the prepared identity before stream setup and release" do
    identity = identity()

    rpc_fun = fn config, %{"action" => action} = request ->
      case action do
        "prepare" ->
          assert request["request"]["argv"] == config["argv"]
          send(self(), :prepare)
          {:ok, identity}

        "release" ->
          send(self(), :release)
          {:ok, released_payload(identity)}
      end
    end

    stream_start_fun = fn _host, command, _opts ->
      assert command =~ "factory-operation stream "
      send(self(), :stream_open)
      {:ok, fake_port(stream_script(~s(echo '{"ok":"stream_ready"}'), "cat >/dev/null"))}
    end

    callback = fn ^identity ->
      send(self(), :registered)
      :ok
    end

    assert {:ok, port, ^identity} =
             WorkerOperation.start(@workspace, @operation_id, @config, callback,
               rpc_fun: rpc_fun,
               stream_start_fun: stream_start_fun
             )

    assert Process.info(self(), :messages) == {:messages, [:prepare, :registered, :stream_open, :release]}
    close_fake_port(port)
  end

  test "validates registration identity locally without status RPC" do
    identity = identity()

    assert {:ok, ^identity} = WorkerOperation.validate_registration_identity(identity, @operation_id, @config)

    assert {:error, :invalid_worker_identity} =
             WorkerOperation.validate_registration_identity(Map.put(identity, "unit", "other.service"), @operation_id, @config)
  end

  test "callback rejection prevents stream and requests exact-identity stop" do
    identity = identity()
    parent = self()

    rpc_fun = fn _config, %{"action" => action} = request ->
      case action do
        "prepare" ->
          {:ok, identity}

        "stop" ->
          assert request["identity"] == identity
          send(parent, :stop_requested)
          {:ok, terminal_payload(identity)}
      end
    end

    stream_start_fun = fn _host, _command, _opts ->
      send(parent, :stream_started)
      {:error, :unexpected}
    end

    assert {:error, {:worker_registration_rejected, {:callback_rejected, :durable_write_failed}}} =
             WorkerOperation.start(@workspace, @operation_id, @config, fn _identity -> {:error, :durable_write_failed} end,
               rpc_fun: rpc_fun,
               stream_start_fun: stream_start_fun
             )

    assert_receive :stop_requested
    refute_receive :stream_started
  end

  test "malformed prepared identity is stopped and remains uncertain" do
    malformed = Map.put(identity(), "unexpected", "field")
    parent = self()

    rpc_fun = fn _config, %{"action" => action} = request ->
      case action do
        "prepare" ->
          {:ok, malformed}

        "stop" ->
          assert request["identity"] == malformed
          send(parent, :stop_requested)
          {:ok, terminal_payload(identity())}
      end
    end

    assert {:uncertain, :external_termination_unknown} =
             WorkerOperation.start(@workspace, @operation_id, @config, fn _identity -> :ok end,
               rpc_fun: rpc_fun,
               stream_start_fun: fn _host, _command, _opts -> flunk("stream must not start") end
             )

    assert_receive :stop_requested
  end

  test "an ambiguous prepare error remains uncertain" do
    assert {:uncertain, :external_termination_unknown} =
             WorkerOperation.start(@workspace, @operation_id, @config, fn _identity -> :ok end,
               rpc_fun: fn _config, %{"action" => "prepare"} -> {:error, {:remote_error, "operation_unavailable"}} end,
               stream_start_fun: fn _host, _command, _opts -> flunk("stream must not start") end
             )
  end

  test "readiness mismatch stops the exact unit before returning an error" do
    identity = identity()
    parent = self()

    rpc_fun = fn _config, %{"action" => action} = request ->
      case action do
        "prepare" ->
          {:ok, identity}

        "release" ->
          flunk("a mismatched stream must not be released")

        "stop" ->
          assert request["identity"] == identity
          send(parent, :stop_requested)
          {:ok, terminal_payload(identity)}
      end
    end

    stream_start_fun = fn _host, _command, _opts ->
      {:ok, fake_port(stream_script(~s(echo '{"ok":"wrong"}'), "cat >/dev/null"))}
    end

    assert {:error, :worker_stream_ready_mismatch} =
             WorkerOperation.start(@workspace, @operation_id, @config, fn _identity -> :ok end,
               rpc_fun: rpc_fun,
               stream_start_fun: stream_start_fun
             )

    assert_receive :stop_requested
  end

  test "a lost check stream with unavailable remote status stays uncertain" do
    identity = identity()

    rpc_fun = fn _config, %{"action" => action} ->
      case action do
        "prepare" -> {:ok, identity}
        "release" -> {:ok, released_payload(identity)}
        "status" -> {:ok, %{"operation_id" => @operation_id, "status" => "unknown", "identity" => identity}}
        "stop" -> {:ok, %{"operation_id" => @operation_id, "status" => "stopping", "identity" => identity}}
      end
    end

    stream_start_fun = fn _host, _command, _opts ->
      {:ok,
       fake_port(
         stream_script(
           ~s(echo '{"ok":"stream_ready"}'; echo 'partial output'),
           "exit 255"
         )
       )}
    end

    assert {:uncertain, :external_termination_unknown} =
             WorkerOperation.run_check(["check.sh"], @workspace, 1_000, @operation_id, @config, fn _identity -> :ok end, %{},
               rpc_fun: rpc_fun,
               stream_start_fun: stream_start_fun,
               termination_timeout_ms: 0
             )
  end

  test "check evidence uses unit status and closes a stream after the terminal proof" do
    identity = identity()
    terminal = terminal_payload(identity, 7)

    rpc_fun = fn _config, %{"action" => action} = request ->
      case action do
        "prepare" ->
          ["/usr/bin/env", assignment, "check.sh"] = request["request"]["argv"]
          assert Jason.decode!(String.replace_prefix(assignment, "WORKSTREAM_INPUTS_JSON=", "")) == %{"task" => "lint"}
          {:ok, identity}

        "release" ->
          {:ok, released_payload(identity)}

        "status" ->
          {:ok, terminal}
      end
    end

    stream_start_fun = fn _host, _command, _opts ->
      {:ok, fake_port(stream_script(~s(echo '{"ok":"stream_ready"}'), "echo 'gate output'; exec sleep 5"))}
    end

    assert {:ok, %{exit_status: 7, exit_code: "exited", output: "gate output\n", truncated: true}} =
             WorkerOperation.run_check(["check.sh"], @workspace, 1_000, @operation_id, @config, fn _identity -> :ok end, %{"task" => "lint"},
               rpc_fun: rpc_fun,
               stream_start_fun: stream_start_fun
             )
  end

  test "drains late check output and waits for delayed transport exit after terminal proof" do
    assert {:ok, %{exit_status: 0, output: "{\"candidate\":true}\n", truncated: false, timed_out: false}} =
             delayed_check("printf '{\"candidate\":'; sleep 0.8; printf 'true}\\n'; sleep 0.1", 2_000)
  end

  test "delayed drain preserves output limits and rejects unsuccessful transport exit" do
    assert {:ok, %{output: output, truncated: true}} = delayed_check("head -c 65537 /dev/zero; echo", 2_000)
    assert byte_size(output) <= 65_536
    assert {:ok, %{truncated: true}} = delayed_check("echo partial; exit 255", 2_000)
  end

  defp delayed_check(script, timeout_ms) do
    identity = identity()

    rpc = fn _, %{"action" => action} ->
      case action do
        "prepare" -> {:ok, identity}
        "release" -> {:ok, released_payload(identity)}
        "status" -> {:ok, terminal_payload(identity)}
      end
    end

    WorkerOperation.run_check(["check.sh"], @workspace, timeout_ms, @operation_id, @config, fn _ -> :ok end, %{},
      rpc_fun: rpc,
      stream_start_fun: fn _, _, _ -> {:ok, fake_port(stream_script(~s(echo '{"ok":"stream_ready"}'), script))} end
    )
  end

  test "stop accepts only a complete exact terminal proof" do
    identity = identity()

    assert :unknown =
             WorkerOperation.stop(@config, identity,
               rpc_fun: fn _config, %{"action" => "stop"} ->
                 {:ok, Map.delete(terminal_payload(identity), "termination_proof")}
               end,
               termination_timeout_ms: 0
             )

    assert :terminated =
             WorkerOperation.stop(@config, identity, rpc_fun: fn _config, %{"action" => "stop"} -> {:ok, terminal_payload(identity)} end)
  end

  test "the shipped pinned candidate producer reaches remote prepare and launch" do
    command = shipped_candidate_command()
    assert byte_size(List.last(command)) == 6_994
    pin = command_sha256(command)
    assert pin == "a7bf9930b1e99665d598a30c94db43896d9056a3aaeaf28347d5a1a12a1a1ed9"
    identity = identity()
    inputs = %{"candidate" => %{"thread_id" => "implementation-thread"}}

    rpc = fn _, %{"action" => action} = request ->
      case action do
        "prepare" ->
          ["/usr/bin/env", assignment | actual] = request["request"]["argv"]
          assert actual == command
          assert Jason.decode!(String.replace_prefix(assignment, "WORKSTREAM_INPUTS_JSON=", "")) == inputs
          send(self(), :candidate_prepared)
          {:ok, identity}

        "release" ->
          {:ok, released_payload(identity)}

        "status" ->
          {:ok, terminal_payload(identity)}
      end
    end

    assert {:ok, %{exit_status: 0}} =
             WorkerOperation.run_check(command, @workspace, 1_000, @operation_id, @config, fn _ -> :ok end, inputs,
               candidate_producer_command_sha256: pin,
               rpc_fun: rpc,
               stream_start_fun: fn _, _, _ -> {:ok, fake_port(stream_script(~s(echo '{"ok":"stream_ready"}'), "exit 0"))} end
             )

    assert_receive :candidate_prepared
  end

  test "larger candidate scripts require the exact pin and never relax ordinary bounds" do
    command = shipped_candidate_command()
    pin = command_sha256(command)
    oversized = ["/usr/bin/python3", "-I", "-c", String.duplicate("#", 8_193)]
    altered = List.update_at(command, 3, &String.replace_prefix(&1, "import", "#mport"))
    opts = [rpc_fun: fn _, _ -> flunk("rejected command must not prepare a worker") end]

    for {argv, supplied_pin} <- [
          {command, nil},
          {command, String.duplicate("a", 64)},
          {altered, pin},
          {oversized, command_sha256(oversized)},
          {["check.sh", String.duplicate("x", 4_097)], pin},
          {["/usr/bin/python3", "-c", List.last(command)], pin},
          {["/usr/bin/python3", "-I", "-c", String.duplicate("#", 4_097) <> <<0>>], pin},
          {["/usr/bin/python3", "-I", "-c", String.duplicate("#", 4_097) <> <<255>>], pin}
        ] do
      assert {:error, :invalid_worker_check_command} =
               WorkerOperation.run_check(
                 argv,
                 @workspace,
                 1_000,
                 @operation_id,
                 @config,
                 fn _ -> :ok end,
                 %{"candidate_producer_command_sha256" => pin},
                 Keyword.put(opts, :candidate_producer_command_sha256, supplied_pin)
               )
    end

    assert {:error, :invalid_worker_argv} =
             WorkerOperation.validate_config(Map.put(@config, "argv", ["/usr/bin/python3", String.duplicate("x", 4_097)]))
  end

  defp shipped_candidate_command do
    path = Path.expand("../../../factory/workstreams/software-change.yaml", __DIR__)
    {:ok, definition} = Workstream.load(path, %{"task" => "remote validation regression"})
    definition.stages["local-commit"].gate.command
  end

  defp command_sha256(command), do: :crypto.hash(:sha256, Jason.encode!(command)) |> Base.encode16(case: :lower)

  test "review preparation sends identity without caller target paths and registers before release" do
    identity = identity()
    command = ["/usr/bin/python3", "-I", "-c", String.duplicate("#", 4_407)]
    preparation = review_preparation()

    rpc_fun = fn _config, %{"action" => action} = request ->
      case action do
        "prepare_review_workspace" ->
          assert request["request"] == Map.merge(preparation, %{"current_operation_id" => @operation_id, "source_workspace" => @workspace, "command_argv" => command})
          assert request["expected_release"] == Map.take(@config, ["service_revision", "release_sha256"])
          send(self(), :prepared_review)
          {:ok, identity}

        "release" ->
          assert_receive :registered_review
          {:ok, released_payload(identity)}

        "status" ->
          {:ok, terminal_payload(identity)}
      end
    end

    assert {:ok, %{exit_status: 0}} =
             WorkerOperation.run_review_prepare(
               command,
               @workspace,
               1_000,
               @operation_id,
               @config,
               fn ^identity ->
                 send(self(), :registered_review)
                 :ok
               end,
               preparation,
               rpc_fun: rpc_fun,
               stream_start_fun: fn _, _, _ -> {:ok, fake_port(stream_script(~s(echo '{"ok":"stream_ready"}'), "exit 0"))} end
             )

    assert_receive :prepared_review
    assert {:error, :invalid_worker_check_command} = WorkerOperation.run_check(command, @workspace, 1_000, @operation_id, @config, fn _ -> :ok end, %{})
  end

  test "review preparation rejects caller paths and oversized or altered command shape before RPC" do
    command = ["/usr/bin/python3", "-I", "-c", "pass"]
    opts = [rpc_fun: fn _, _ -> flunk("invalid input must not reach broker") end]

    for {argv, preparation} <- [
          {command, Map.put(review_preparation(), "workspace", "/other")},
          {command, Map.put(review_preparation(), "run_id", "another-run")},
          {command, Map.put(review_preparation(), "stable_operation_id", "run-1/other-stage/1")},
          {["/usr/bin/python3", "-c", "pass"], review_preparation()},
          {["/usr/bin/python3", "-I", "-c", String.duplicate("x", 16_385)], review_preparation()}
        ] do
      assert {:error, :invalid_review_preparation_request} = WorkerOperation.run_review_prepare(argv, @workspace, 1_000, @operation_id, @config, fn _ -> :ok end, preparation, opts)
    end
  end

  test "review preparation registration failure stops exact unit and ambiguous preparation remains unknown" do
    identity = identity()
    command = ["/usr/bin/python3", "-I", "-c", "pass"]

    rpc = fn _, %{"action" => action} = request ->
      case action do
        "prepare_review_workspace" ->
          {:ok, identity}

        "stop" ->
          assert request["identity"] == identity
          {:ok, terminal_payload(identity)}
      end
    end

    assert {:error, {:worker_registration_rejected, {:callback_rejected, :durable_write_failed}}} =
             WorkerOperation.run_review_prepare(command, @workspace, 1_000, @operation_id, @config, fn _ -> {:error, :durable_write_failed} end, review_preparation(),
               rpc_fun: rpc,
               stream_start_fun: fn _, _, _ -> flunk("unregistered operation cannot stream") end
             )

    assert {:uncertain, :external_termination_unknown} =
             WorkerOperation.run_review_prepare(command, @workspace, 1_000, @operation_id, @config, fn _ -> :ok end, review_preparation(),
               rpc_fun: fn _, _ -> {:error, {:remote_error, "operation_unavailable"}} end
             )

    assert {:error, {:worker_prepare_rejected, "unsupported_action"}} =
             WorkerOperation.run_review_prepare(command, @workspace, 1_000, @operation_id, @config, fn _ -> :ok end, review_preparation(),
               rpc_fun: fn _, _ -> {:error, {:remote_error, "unsupported_action"}} end
             )
  end

  defp review_preparation do
    %{
      "run_id" => "run-1",
      "stable_operation_id" => @operation_id,
      "identity" => %{
        "number" => 1,
        "url" => "https://github.com/RigCodeAI/rig/pull/1",
        "base_sha" => String.duplicate("a", 40),
        "head_sha" => String.duplicate("b", 40),
        "head_branch" => "cycle/factory-1",
        "base_branch" => "main",
        "rubric_sha256" => String.duplicate("c", 64),
        "cloud_thread_id" => "cloud-1",
        "candidate_context_id" => nil
      }
    }
  end

  defp capabilities do
    %{
      "protocol_version" => 1,
      "contained" => true,
      "containment_qualified" => true,
      "systemd_version" => 254,
      "machine_id" => @config["machine_id"],
      "service_revision" => @config["service_revision"],
      "release_sha256" => @config["release_sha256"]
    }
  end

  defp identity do
    unit = "factory-operation-#{Base.encode16(:crypto.hash(:sha256, @operation_id), case: :lower)}.service"

    %{
      "kind" => "systemd-unit",
      "operation_id" => @operation_id,
      "machine_id" => @config["machine_id"],
      "boot_id" => "1234567890abcdef1234567890abcdef",
      "unit" => unit,
      "invocation_id" => "1234567890abcdef1234567890abcdef",
      "control_group" => "/system.slice/#{unit}",
      "request_sha256" => String.duplicate("c", 64)
    }
  end

  defp released_payload(identity) do
    %{"operation_id" => identity["operation_id"], "status" => "released", "identity" => identity}
  end

  defp terminal_payload(identity, exit_status \\ 0) do
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
      "exec_main_status" => exit_status,
      "main_pid" => 0,
      "observed_at" => "2026-10-10T00:00:00Z",
      "systemd_version" => 254
    }

    %{
      "operation_id" => identity["operation_id"],
      "status" => "terminated",
      "identity" => identity,
      "termination_proof" => proof,
      "exit_status" => exit_status,
      "exit_code" => "exited"
    }
  end

  defp fake_port(script) do
    Port.open({:spawn_executable, ~c"/bin/sh"}, [
      :binary,
      :exit_status,
      :stderr_to_stdout,
      line: 131_072,
      args: [~c"-c", String.to_charlist(script)]
    ])
  end

  defp stream_script(handshake, after_handshake) do
    "IFS= read -r control || exit 2; #{handshake}; #{after_handshake}"
  end

  defp close_fake_port(port) do
    if Port.info(port), do: Port.close(port)
  rescue
    _ -> :ok
  end
end
