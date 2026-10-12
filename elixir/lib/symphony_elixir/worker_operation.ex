defmodule SymphonyElixir.WorkerOperation do
  @moduledoc """
  Client for the contained worker operation service.

  Every operation is prepared and identified by the worker before its durable identity is
  registered locally. The worker remains held until the stream is ready and release is
  acknowledged. SSH process exit and port closure are transport events; only the worker's
  exact systemd identity and termination proof establish termination.
  """

  alias SymphonyElixir.SSH

  @config_keys ~w(host machine_id service_revision release_sha256 argv)
  @identity_keys ~w(kind operation_id machine_id boot_id unit invocation_id control_group request_sha256)
  @proof_keys ~w(machine_id boot_id unit invocation_id control_group cgroup_state cgroup_populated active_state sub_state exec_main_code exec_main_status main_pid observed_at systemd_version)
  @operation_statuses ~w(held running released stopping terminated unknown reserved)
  @terminal_exec_codes ~w(none exited killed dumped)

  @max_argv_count 64
  @max_arg_bytes 4_096
  @max_argv_bytes 16_384
  @max_workspace_bytes 4_096
  @max_operation_id_bytes 200
  @max_rpc_bytes 131_072
  @max_output_bytes 65_536
  @rpc_timeout_ms 10_000
  @stream_ready_timeout_ms 10_000
  @termination_timeout_ms 5_000
  @termination_poll_ms 100
  @stream_status_poll_ms 250
  @stream_drain_timeout_ms 500
  @max_check_timeout_ms 86_400_000

  @doc "Validates the exact SSH worker control configuration used by this client."
  @spec validate_config(map()) :: :ok | {:error, term()}
  def validate_config(config) when is_map(config) do
    with true <- Enum.sort(Map.keys(config)) == Enum.sort(@config_keys),
         :ok <- validate_host(config["host"]),
         :ok <- validate_machine_id(config["machine_id"]),
         :ok <- validate_revision(config["service_revision"]),
         :ok <- validate_release_sha256(config["release_sha256"]),
         :ok <- validate_argv(config["argv"]) do
      :ok
    else
      false -> {:error, :invalid_worker_control_config}
      {:error, _} = error -> error
    end
  end

  def validate_config(_config), do: {:error, :invalid_worker_control_config}

  @doc "Checks that the remote worker has the required contained-operation protocol and release."
  @spec qualify(map()) :: :ok | {:error, term()}
  def qualify(config), do: qualify(config, [])

  @spec qualify(map(), keyword()) :: :ok | {:error, term()}
  def qualify(config, opts) when is_list(opts) do
    with :ok <- validate_config(config),
         {:ok, payload} <- rpc(config, %{"action" => "capabilities"}, opts),
         true <- payload["protocol_version"] == 1,
         true <- payload["contained"] == true,
         true <- payload["containment_qualified"] == true,
         true <- is_integer(payload["systemd_version"]) and payload["systemd_version"] >= 252,
         true <- payload["machine_id"] == config["machine_id"],
         true <- payload["service_revision"] == config["service_revision"],
         true <- payload["release_sha256"] == config["release_sha256"] do
      :ok
    else
      false -> {:error, :worker_not_qualified}
      {:error, _} = error -> error
    end
  end

  @doc "Prepares, durably registers, streams and releases one contained operation."
  @spec start(Path.t(), String.t(), map(), (map() -> :ok | term())) ::
          {:ok, port(), map()} | {:error, term()} | {:uncertain, term()}
  def start(workspace, operation_id, control, on_process_start),
    do: start(workspace, operation_id, control, on_process_start, [])

  @spec start(Path.t(), String.t(), map(), (map() -> :ok | term()), keyword()) ::
          {:ok, port(), map()} | {:error, term()} | {:uncertain, term()}
  def start(workspace, operation_id, control, on_process_start, opts) when is_list(opts) do
    with :ok <- validate_config(control),
         :ok <- validate_operation_id(operation_id),
         :ok <- validate_workspace(workspace),
         :ok <- validate_registration_callback(on_process_start),
         {:ok, port, identity} <- launch(workspace, operation_id, control, control["argv"], on_process_start, opts) do
      {:ok, port, identity}
    else
      {:uncertain, _reason} = uncertain -> uncertain
      {:error, _} = error -> error
    end
  rescue
    _ -> {:uncertain, :external_termination_unknown}
  catch
    _, _ -> {:uncertain, :external_termination_unknown}
  end

  @doc "Validates the prepared identity locally for a fast durable-registration callback."
  @spec validate_registration_identity(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def validate_registration_identity(identity, operation_id, config) do
    with :ok <- validate_config(config),
         :ok <- validate_operation_id(operation_id),
         {:ok, identity} <- validate_identity_shape(identity, operation_id, config) do
      {:ok, identity}
    end
  rescue
    _ -> {:error, :invalid_worker_identity}
  catch
    _, _ -> {:error, :invalid_worker_identity}
  end

  @doc "Validates a stored worker identity locally and against the current worker status."
  @spec validate_identity(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def validate_identity(identity, operation_id, config),
    do: validate_identity(identity, operation_id, config, [])

  @spec validate_identity(map(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def validate_identity(identity, operation_id, config, opts) when is_list(opts) do
    with :ok <- validate_config(config),
         :ok <- validate_operation_id(operation_id),
         {:ok, identity} <- validate_identity_shape(identity, operation_id, config),
         {:ok, payload} <- status_payload(config, identity, opts),
         true <- identity_payload_matches?(payload, identity, operation_id),
         true <- valid_status_payload?(payload) do
      if payload["status"] == "unknown" or
           (payload["status"] == "terminated" and not terminal_proof?(payload, identity)) do
        {:error, :worker_identity_unavailable}
      else
        {:ok, identity}
      end
    else
      false -> {:error, :worker_identity_unavailable}
      {:error, _} = error -> error
      :unknown -> {:error, :worker_identity_unavailable}
    end
  rescue
    _ -> {:error, :worker_identity_unavailable}
  catch
    _, _ -> {:error, :worker_identity_unavailable}
  end

  @doc "Returns the exact remote operation status, preserving unavailable status as `:unknown`."
  @spec status(map(), map()) :: {:ok, map()} | :unknown
  def status(config, identity), do: status(config, identity, [])

  @spec status(map(), map(), keyword()) :: {:ok, map()} | :unknown
  def status(config, identity, opts) when is_list(opts) do
    with :ok <- validate_config(config),
         {:ok, identity} <- validate_identity_shape(identity, identity_operation_id(identity), config),
         {:ok, payload} <- status_payload(config, identity, opts),
         true <- identity_payload_matches?(payload, identity, identity["operation_id"]),
         true <- valid_status_payload?(payload) do
      cond do
        payload["status"] == "unknown" -> :unknown
        payload["status"] == "terminated" and not terminal_proof?(payload, identity) -> :unknown
        true -> {:ok, payload}
      end
    else
      _ -> :unknown
    end
  rescue
    _ -> :unknown
  catch
    _, _ -> :unknown
  end

  @doc "Reconciles an operation without treating missing or ambiguous status as termination."
  @spec reconcile(map(), map()) :: :terminated | :running | :unknown
  def reconcile(config, identity), do: reconcile(config, identity, [])

  @spec reconcile(map(), map(), keyword()) :: :terminated | :running | :unknown
  def reconcile(config, identity, opts) when is_list(opts) do
    case status(config, identity, opts) do
      {:ok, %{"status" => "terminated"}} ->
        :terminated

      {:ok, %{"status" => status}} when status in ["held", "running", "released", "stopping"] ->
        :running

      _ ->
        :unknown
    end
  end

  @doc "Requests remote stop and returns `:terminated` only after exact cgroup-empty proof."
  @spec stop(map(), map()) :: :terminated | :unknown
  def stop(config, identity), do: stop(config, identity, [])

  @spec stop(map(), map(), keyword()) :: :terminated | :unknown
  def stop(config, identity, opts) when is_list(opts) do
    case stop_result(config, identity, opts) do
      {:terminated, _payload} -> :terminated
      :unknown -> :unknown
    end
  end

  @doc "Runs a remote check with bounded output and a deadline tied to its contained unit."
  @spec run_check([String.t()], Path.t(), pos_integer(), String.t(), map(), (map() -> :ok | term()), map()) ::
          {:ok, map()} | {:error, term()} | {:uncertain, term()}
  def run_check(command, workspace, timeout_ms, operation_id, control, on_process_start, inputs),
    do: run_check(command, workspace, timeout_ms, operation_id, control, on_process_start, inputs, [])

  @spec run_check([String.t()], Path.t(), pos_integer(), String.t(), map(), (map() -> :ok | term()), map(), keyword()) ::
          {:ok, map()} | {:error, term()} | {:uncertain, term()}
  def run_check(command, workspace, timeout_ms, operation_id, control, on_process_start, inputs, opts)
      when is_list(opts) do
    with :ok <- validate_config(control),
         :ok <- validate_check_request(command, workspace, timeout_ms, operation_id, inputs),
         :ok <- validate_registration_callback(on_process_start),
         {:ok, input_json} <- encode_inputs(inputs),
         argv <- ["/usr/bin/env", "WORKSTREAM_INPUTS_JSON=" <> input_json | command],
         {:ok, port, identity} <- launch(workspace, operation_id, control, argv, on_process_start, opts) do
      run_started_check(port, identity, control, monotonic_ms() + timeout_ms, opts)
    else
      {:uncertain, _} = uncertain -> uncertain
      {:error, _} = error -> error
    end
  rescue
    _ -> {:uncertain, :external_termination_unknown}
  catch
    _, _ -> {:uncertain, :external_termination_unknown}
  end

  @doc "Reads a bounded candidate bundle from an exact successful service-check operation; no caller path is sent."
  @spec candidate_bundle(map(), map(), map(), keyword()) :: {:ok, binary()} | {:error, term()}
  def candidate_bundle(control, identity, expected, opts \\ []) do
    with :ok <- validate_config(control),
         true <- is_map(expected) and valid_sha256?(expected["candidate_input_sha256"]) and valid_sha256?(expected["bundle_sha256"]),
         bytes = expected["bundle_bytes"],
         true <- is_integer(bytes) and bytes > 0 and bytes <= 64 * 1024 * 1024,
         {:ok, identity} <- validate_identity_shape(identity, identity_operation_id(identity), control),
         {:ok, payload} <- status(control, identity, opts),
         true <- payload["status"] == "terminated" and payload["exit_code"] == "exited" and payload["exit_status"] == 0,
         true <- terminal_proof?(payload, identity),
         deadline = monotonic_ms() + 60_000,
         {:ok, bundle} <- candidate_chunks(control, identity, expected, 0, [], deadline, opts),
         true <- byte_size(bundle) == bytes and sha256(bundle) == expected["bundle_sha256"] do
      {:ok, bundle}
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_bundle_unavailable}
    end
  rescue
    _ -> {:error, :candidate_bundle_unavailable}
  catch
    _, _ -> {:error, :candidate_bundle_unavailable}
  end

  defp candidate_chunks(control, identity, expected, offset, chunks, deadline, opts) do
    envelope = %{
      "action" => "candidate_bundle",
      "identity" => identity,
      "expected_release" => expected_release(control),
      "input_sha256" => expected["candidate_input_sha256"],
      "expected_sha256" => expected["bundle_sha256"],
      "expected_bytes" => expected["bundle_bytes"],
      "offset" => offset
    }

    remaining = deadline - monotonic_ms()

    with true <- remaining > 0,
         {:ok, reply} <- rpc(control, envelope, Keyword.put(opts, :rpc_timeout_ms, min(remaining, @rpc_timeout_ms))),
         true <- reply["operation_id"] == identity["operation_id"] and reply["candidate_input_sha256"] == expected["candidate_input_sha256"],
         true <- reply["bundle_sha256"] == expected["bundle_sha256"] and reply["bundle_bytes"] == expected["bundle_bytes"] and reply["offset"] == offset,
         encoded when is_binary(encoded) <- reply["data_base64"],
         true <- byte_size(encoded) <= 87_384,
         {:ok, data} <- Base.decode64(encoded),
         size = byte_size(data),
         true <- size > 0 and size <= 65_536 and offset + size <= expected["bundle_bytes"],
         true <- reply["eof"] == (offset + size == expected["bundle_bytes"]) do
      if reply["eof"] do
        {:ok, IO.iodata_to_binary(Enum.reverse([data | chunks]))}
      else
        candidate_chunks(control, identity, expected, offset + size, [data | chunks], deadline, opts)
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :candidate_bundle_response_mismatch}
    end
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp launch(workspace, operation_id, control, argv, on_process_start, opts) do
    with :ok <- validate_launch_argv(argv),
         {:ok, identity} <- prepare(control, workspace, operation_id, argv, opts) do
      start_prepared(control, operation_id, identity, on_process_start, opts)
    else
      {:prepare_uncertain, _reason} -> {:uncertain, :external_termination_unknown}
      {:error, _} = error -> error
    end
  end

  defp start_prepared(config, operation_id, raw_identity, on_process_start, opts) do
    case validate_identity_shape(raw_identity, operation_id, config) do
      {:ok, identity} ->
        continue_prepared(config, operation_id, identity, on_process_start, opts)

      {:error, _reason} ->
        cleanup_untrusted_prepare(config, raw_identity, opts)
        {:uncertain, :external_termination_unknown}
    end
  rescue
    _ ->
      cleanup_untrusted_prepare(config, raw_identity, opts)
      {:uncertain, :external_termination_unknown}
  catch
    _, _ ->
      cleanup_untrusted_prepare(config, raw_identity, opts)
      {:uncertain, :external_termination_unknown}
  end

  defp continue_prepared(config, operation_id, identity, on_process_start, opts) do
    case register_identity(on_process_start, identity) do
      :ok ->
        open_and_release(config, operation_id, identity, opts)

      {:error, reason} ->
        case stop_result(config, identity, opts) do
          {:terminated, _proof} -> {:error, {:worker_registration_rejected, reason}}
          :unknown -> {:uncertain, :external_termination_unknown}
        end
    end
  end

  defp open_and_release(config, operation_id, identity, opts) do
    stream_envelope = %{"identity" => identity, "expected_release" => expected_release(config)}
    stream_command = "factory-operation stream " <> encode_envelope!(stream_envelope)

    case stream_start(config["host"], stream_command, opts) do
      {:ok, port} ->
        case begin_stream(port, stream_envelope, opts) do
          :ok ->
            envelope = %{
              "action" => "release",
              "identity" => identity,
              "expected_release" => expected_release(config)
            }

            case rpc(config, envelope, opts) do
              {:ok, payload} when is_map(payload) ->
                if release_payload_matches?(payload, identity, operation_id) do
                  {:ok, port, identity}
                else
                  failed_after_prepare(config, identity, port, :release_not_confirmed, opts)
                end

              {:error, reason} ->
                failed_after_prepare(config, identity, port, {:release_failed, reason}, opts)
            end

          {:error, reason} ->
            failed_after_prepare(config, identity, port, reason, opts)
        end

      {:error, reason} ->
        case stop_result(config, identity, opts) do
          {:terminated, _proof} -> {:error, {:worker_stream_open_failed, reason}}
          :unknown -> {:uncertain, :external_termination_unknown}
        end
    end
  end

  defp begin_stream(port, stream_envelope, opts) do
    command = Jason.encode!(stream_envelope) <> "\n"

    if safe_port_command(port, command) do
      await_stream_ready(port, monotonic_ms() + Keyword.get(opts, :stream_ready_timeout_ms, @stream_ready_timeout_ms), "")
    else
      {:error, :worker_stream_command_failed}
    end
  rescue
    _ -> {:error, :worker_stream_command_failed}
  catch
    _, _ -> {:error, :worker_stream_command_failed}
  end

  defp await_stream_ready(port, deadline, partial) do
    receive do
      {^port, {:data, {:eol, line}}} ->
        decode_stream_ready(partial <> bytes(line))

      {^port, {:data, {:noeol, line}}} ->
        next = partial <> bytes(line)
        if byte_size(next) <= @max_rpc_bytes, do: await_stream_ready(port, deadline, next), else: {:error, :worker_stream_handshake_too_large}

      {^port, {:data, data}} when is_binary(data) ->
        decode_stream_ready(partial <> data)

      {^port, {:data, data}} when is_list(data) ->
        decode_stream_ready(partial <> IO.iodata_to_binary(data))

      {^port, {:exit_status, _status}} ->
        {:error, :worker_stream_closed_before_ready}

      {^port, :closed} ->
        {:error, :worker_stream_closed_before_ready}
    after
      max(deadline - monotonic_ms(), 0) -> {:error, :worker_stream_ready_timeout}
    end
  end

  defp decode_stream_ready(line) do
    with {:ok, %{"ok" => "stream_ready"} = message} <- Jason.decode(line),
         true <- map_size(message) == 1 do
      :ok
    else
      _ -> {:error, :worker_stream_ready_mismatch}
    end
  end

  defp failed_after_prepare(config, identity, port, reason, opts) do
    close_port(port)
    outcome = stop_result(config, identity, opts)

    case outcome do
      {:terminated, _proof} -> {:error, reason}
      :unknown -> {:uncertain, :external_termination_unknown}
    end
  end

  defp prepare(config, workspace, operation_id, argv, opts) do
    request = %{"operation_id" => operation_id, "argv" => argv, "workspace" => workspace}
    envelope = %{"action" => "prepare", "request" => request, "expected_release" => expected_release(config)}

    case rpc(config, envelope, opts) do
      {:ok, identity} when is_map(identity) ->
        {:ok, identity}

      {:error, {:remote_error, reason}} when reason in ["release_mismatch", "invalid_request", "unsupported_action", "permission_denied", "invalid_command"] ->
        {:error, {:worker_prepare_rejected, reason}}

      {:error, {:remote_error, _reason}} ->
        {:prepare_uncertain, :external_termination_unknown}

      {:error, :worker_rpc_request_too_large} ->
        {:error, :worker_rpc_request_too_large}

      {:error, _reason} ->
        {:prepare_uncertain, :external_termination_unknown}
    end
  end

  defp cleanup_untrusted_prepare(config, raw_identity, opts) when is_map(raw_identity) do
    rpc(config, %{"action" => "stop", "identity" => raw_identity, "expected_release" => expected_release(config)}, opts)

    :unknown
  rescue
    _ -> :unknown
  end

  defp release_payload_matches?(payload, identity, operation_id) do
    payload["operation_id"] == operation_id and payload["status"] == "released" and payload["identity"] == identity
  end

  defp status_payload(config, identity, opts) do
    envelope = %{
      "action" => "status",
      "identity" => identity,
      "expected_release" => expected_release(config)
    }

    case rpc(config, envelope, opts) do
      {:ok, payload} when is_map(payload) -> {:ok, payload}
      _ -> :unknown
    end
  end

  defp identity_payload_matches?(payload, identity, operation_id) do
    payload["operation_id"] == operation_id and payload["identity"] == identity
  end

  defp valid_status_payload?(payload) do
    payload["status"] in @operation_statuses
  end

  defp stop_result(config, identity, opts) do
    with :ok <- validate_config(config),
         operation_id when is_binary(operation_id) <- identity_operation_id(identity),
         {:ok, identity} <- validate_identity_shape(identity, operation_id, config) do
      stop_envelope = %{
        "action" => "stop",
        "identity" => identity,
        "expected_release" => expected_release(config)
      }

      stop_reply = rpc(config, stop_envelope, opts)

      case stop_reply do
        {:ok, %{"status" => "terminated"} = payload} ->
          if terminal_payload_matches?(payload, identity) do
            {:terminated, payload}
          else
            await_termination(config, identity, monotonic_ms() + termination_timeout(opts), opts)
          end

        _ ->
          await_termination(config, identity, monotonic_ms() + termination_timeout(opts), opts)
      end
    else
      _ -> :unknown
    end
  rescue
    _ -> :unknown
  catch
    _, _ -> :unknown
  end

  defp await_termination(config, identity, deadline, opts) do
    remaining = max(deadline - monotonic_ms(), 1)
    bounded_opts = Keyword.put(opts, :rpc_timeout_ms, min(Keyword.get(opts, :rpc_timeout_ms, @rpc_timeout_ms), remaining))

    case status_payload(config, identity, bounded_opts) do
      {:ok, %{"status" => "terminated"} = payload} ->
        if terminal_payload_matches?(payload, identity) do
          {:terminated, payload}
        else
          retry_termination(config, identity, deadline, opts)
        end

      _ ->
        retry_termination(config, identity, deadline, opts)
    end
  end

  defp retry_termination(config, identity, deadline, opts) do
    remaining = deadline - monotonic_ms()

    if remaining <= 0 do
      :unknown
    else
      Process.sleep(min(Keyword.get(opts, :termination_poll_ms, @termination_poll_ms), remaining))
      await_termination(config, identity, deadline, opts)
    end
  end

  defp terminal_payload_matches?(payload, identity) do
    payload["status"] == "terminated" and payload["identity"] == identity and
      payload["operation_id"] == identity["operation_id"] and terminal_proof?(payload, identity)
  end

  defp terminal_proof?(payload, identity) do
    proof = payload["termination_proof"]

    is_map(proof) and Enum.sort(Map.keys(proof)) == Enum.sort(@proof_keys) and
      Enum.all?(~w(machine_id boot_id unit invocation_id control_group), fn key -> proof[key] == identity[key] end) and
      proof["cgroup_state"] in ["present", "released"] and proof["cgroup_populated"] == 0 and proof["main_pid"] == 0 and
      is_integer(proof["systemd_version"]) and proof["systemd_version"] >= 252 and
      valid_terminal_state?(proof["active_state"], proof["sub_state"]) and
      proof["exec_main_code"] in @terminal_exec_codes and is_integer(proof["exec_main_status"]) and
      proof["exec_main_status"] >= 0 and proof["exec_main_status"] <= 255 and
      payload["exit_status"] == proof["exec_main_status"] and
      payload["exit_code"] == proof["exec_main_code"] and valid_timestamp?(proof["observed_at"])
  end

  defp valid_timestamp?(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, _datetime, _offset} -> true
      _ -> false
    end
  end

  defp valid_timestamp?(_value), do: false

  defp valid_terminal_state?("active", "exited"), do: true
  defp valid_terminal_state?("inactive", sub) when sub in ["dead", "failed"], do: true
  defp valid_terminal_state?("failed", "failed"), do: true
  defp valid_terminal_state?(_active, _sub), do: false

  defp run_started_check(port, identity, config, deadline, opts) do
    case collect_stream(port, identity, config, deadline, monotonic_ms(), "", false, opts) do
      {:terminated, payload, output, truncated} ->
        {output, truncated} = drain_stream(port, output, truncated)
        close_port(port)
        check_result(payload, output, truncated, false)

      {:closed, output, truncated, _ssh_status} ->
        case status_payload(config, identity, bounded_rpc_opts(opts, deadline)) do
          {:ok, %{"status" => "terminated"} = payload} ->
            if terminal_payload_matches?(payload, identity) do
              close_port(port)
              check_result(payload, output, truncated, false)
            else
              stop_after_check_stream(config, identity, port, :invalid_terminal_proof, opts)
            end

          {:ok, %{"status" => status}} when status in ["held", "running", "released", "stopping", "reserved"] ->
            stop_after_check_stream(config, identity, port, :worker_stream_ended_early, opts)

          _ ->
            stop_after_check_stream(config, identity, port, :worker_status_unavailable, opts)
        end

      {:timeout, output, truncated} ->
        close_port(port)

        case stop_result(config, identity, opts) do
          {:terminated, payload} ->
            check_result(payload, output, truncated, true)

          :unknown ->
            {:uncertain, :external_termination_unknown}
        end

      {:stream_error, _output, _truncated} ->
        stop_after_check_stream(config, identity, port, :worker_stream_lost, opts)
    end
  rescue
    _ ->
      close_port(port)
      _ = stop_result(config, identity, opts)
      {:uncertain, :external_termination_unknown}
  catch
    _, _ ->
      close_port(port)
      _ = stop_result(config, identity, opts)
      {:uncertain, :external_termination_unknown}
  end

  defp stop_after_check_stream(config, identity, port, reason, opts) do
    close_port(port)
    outcome = stop_result(config, identity, opts)

    case outcome do
      {:terminated, _proof} -> {:error, reason}
      :unknown -> {:uncertain, :external_termination_unknown}
    end
  end

  defp collect_stream(port, identity, config, deadline, next_status_poll, output, truncated, opts) do
    now = monotonic_ms()

    cond do
      now >= deadline ->
        {:timeout, output, truncated}

      now >= next_status_poll ->
        case status_payload(config, identity, bounded_rpc_opts(opts, deadline)) do
          {:ok, %{"status" => "terminated"} = payload} ->
            if terminal_payload_matches?(payload, identity) do
              {:terminated, payload, output, truncated}
            else
              collect_stream(
                port,
                identity,
                config,
                deadline,
                monotonic_ms() + Keyword.get(opts, :stream_status_poll_ms, @stream_status_poll_ms),
                output,
                truncated,
                opts
              )
            end

          _ ->
            collect_stream(
              port,
              identity,
              config,
              deadline,
              monotonic_ms() + Keyword.get(opts, :stream_status_poll_ms, @stream_status_poll_ms),
              output,
              truncated,
              opts
            )
        end

      true ->
        wait_ms = min(deadline, next_status_poll) - now

        receive do
          {^port, {:data, data}} ->
            {chunk, newline?} = stream_chunk(data)
            {output, truncated} = append_bounded(output, chunk <> if(newline?, do: "\n", else: ""), truncated)
            collect_stream(port, identity, config, deadline, next_status_poll, output, truncated, opts)

          {^port, {:exit_status, status}} ->
            {:closed, output, truncated, status}

          {^port, :closed} ->
            {:stream_error, output, truncated}
        after
          max(wait_ms, 0) -> collect_stream(port, identity, config, deadline, next_status_poll, output, truncated, opts)
        end
    end
  end

  defp drain_stream(port, output, truncated) do
    drain_stream(port, monotonic_ms() + @stream_drain_timeout_ms, output, truncated)
  end

  defp drain_stream(port, deadline, output, truncated) do
    if monotonic_ms() >= deadline do
      {output, true}
    else
      receive do
        {^port, {:data, data}} ->
          {chunk, newline?} = stream_chunk(data)
          {output, truncated} = append_bounded(output, chunk <> if(newline?, do: "\n", else: ""), truncated)
          drain_stream(port, deadline, output, truncated)

        {^port, {:exit_status, _status}} ->
          {output, truncated}

        {^port, :closed} ->
          {output, truncated}
      after
        max(deadline - monotonic_ms(), 0) -> {output, true}
      end
    end
  end

  defp bounded_rpc_opts(opts, deadline) do
    remaining = max(deadline - monotonic_ms(), 1)
    Keyword.put(opts, :rpc_timeout_ms, min(Keyword.get(opts, :rpc_timeout_ms, @rpc_timeout_ms), remaining))
  end

  defp stream_chunk({:eol, data}), do: {bytes(data), true}
  defp stream_chunk({:noeol, data}), do: {bytes(data), false}
  defp stream_chunk(data) when is_binary(data), do: {data, false}
  defp stream_chunk(data) when is_list(data), do: {IO.iodata_to_binary(data), false}
  defp stream_chunk(_data), do: {"", false}

  defp append_bounded(output, chunk, truncated) do
    available = max(@max_output_bytes - byte_size(output), 0)
    kept = binary_part(chunk, 0, min(available, byte_size(chunk)))
    {output <> kept, truncated or byte_size(chunk) > available}
  end

  defp check_evidence(payload, output, truncated, timed_out) do
    proof = payload["termination_proof"]
    {output, expanded} = output |> valid_utf8() |> redact_output() |> bounded_text()

    %{
      exit_status: payload["exit_status"],
      output: output,
      truncated: truncated or expanded,
      timed_out: timed_out,
      exit_code: proof["exec_main_code"]
    }
  end

  defp check_result(payload, output, truncated, timed_out) do
    evidence = check_evidence(payload, output, truncated, timed_out)

    if timed_out or evidence.exit_code == "exited" do
      {:ok, evidence}
    else
      {:error, :worker_check_did_not_exit}
    end
  end

  defp redact_output(output) do
    String.replace(output, ~r/(?:sk-|gh[pousr]_|github_pat_)[A-Za-z0-9_-]+/, "[REDACTED]")
  end

  defp bounded_text(output) when byte_size(output) <= @max_output_bytes, do: {output, false}

  defp bounded_text(output) do
    case :unicode.characters_to_binary(binary_part(output, 0, @max_output_bytes)) do
      valid when is_binary(valid) -> {valid, true}
      {:incomplete, valid, _rest} -> {valid, true}
      {:error, valid, _rest} -> {valid, true}
    end
  end

  defp valid_utf8(output) do
    case :unicode.characters_to_binary(output) do
      valid when is_binary(valid) -> valid
      {:error, valid, <<_invalid, rest::binary>>} -> valid <> "�" <> valid_utf8(rest)
      {:incomplete, valid, _rest} -> valid <> "�"
    end
  end

  defp validate_check_request(command, workspace, timeout_ms, operation_id, inputs) do
    with :ok <- validate_command(command),
         :ok <- validate_workspace(workspace),
         true <- is_integer(timeout_ms) and timeout_ms > 0 and timeout_ms <= @max_check_timeout_ms,
         :ok <- validate_operation_id(operation_id),
         true <- is_map(inputs) do
      :ok
    else
      false -> {:error, :invalid_worker_check_request}
      {:error, _} = error -> error
    end
  end

  defp encode_inputs(inputs) do
    case Jason.encode(inputs) do
      {:ok, json} when byte_size(json) <= @max_output_bytes -> {:ok, json}
      {:ok, _json} -> {:error, :worker_check_inputs_too_large}
      {:error, _} -> {:error, :invalid_worker_check_inputs}
    end
  end

  defp validate_command(command) when is_list(command) and length(command) in 1..@max_argv_count do
    if Enum.all?(command, &valid_argument?/1) and Enum.sum(Enum.map(command, &byte_size/1)) <= @max_argv_bytes do
      :ok
    else
      {:error, :invalid_worker_check_command}
    end
  end

  defp validate_command(_command), do: {:error, :invalid_worker_check_command}

  defp validate_host(host) when is_binary(host) and byte_size(host) in 1..255 do
    if String.valid?(host) and String.trim(host) == host and Regex.match?(~r/\A[A-Za-z0-9\[][A-Za-z0-9_.@:\-\[\]]*\z/, host), do: :ok, else: {:error, :invalid_worker_host}
  end

  defp validate_host(_host), do: {:error, :invalid_worker_host}

  defp validate_machine_id(machine_id) do
    if valid_invocation_id?(machine_id), do: :ok, else: {:error, :invalid_worker_machine_id}
  end

  defp validate_revision(revision) when is_binary(revision) do
    if String.valid?(revision) and Regex.match?(~r/\A[a-f0-9]{40}\z/, revision), do: :ok, else: {:error, :invalid_worker_service_revision}
  end

  defp validate_revision(_revision), do: {:error, :invalid_worker_service_revision}

  defp validate_release_sha256(sha256) when is_binary(sha256) do
    if String.valid?(sha256) and Regex.match?(~r/\A[a-f0-9]{64}\z/, sha256), do: :ok, else: {:error, :invalid_worker_release_sha256}
  end

  defp validate_release_sha256(_sha256), do: {:error, :invalid_worker_release_sha256}

  defp validate_argv(argv) when is_list(argv) and length(argv) in 1..@max_argv_count do
    first = hd(argv)

    cond do
      not (is_binary(first) and Path.type(first) == :absolute) ->
        {:error, :worker_argv_must_start_with_absolute_executable}

      not Enum.all?(argv, &valid_argument?/1) ->
        {:error, :invalid_worker_argv}

      Enum.sum(Enum.map(argv, &byte_size/1)) > @max_argv_bytes ->
        {:error, :worker_argv_too_large}

      true ->
        :ok
    end
  end

  defp validate_argv(_argv), do: {:error, :invalid_worker_argv}

  defp validate_launch_argv(argv) when is_list(argv) and length(argv) in 1..(@max_argv_count + 2)//1 do
    first = hd(argv)

    if is_binary(first) and Path.type(first) == :absolute and Enum.all?(argv, &valid_launch_argument?/1) and
         Enum.sum(Enum.map(argv, &byte_size/1)) <= @max_output_bytes + @max_argv_bytes do
      :ok
    else
      {:error, :invalid_worker_argv}
    end
  end

  defp validate_launch_argv(_argv), do: {:error, :invalid_worker_argv}

  defp valid_launch_argument?("WORKSTREAM_INPUTS_JSON=" <> value) do
    byte_size(value) <= @max_output_bytes and not String.contains?(value, <<0>>)
  end

  defp valid_launch_argument?(value), do: valid_argument?(value)

  defp valid_argument?(value) when is_binary(value) and byte_size(value) in 1..@max_arg_bytes do
    String.valid?(value) and not String.contains?(value, <<0>>)
  end

  defp valid_argument?(_value), do: false

  defp validate_operation_id(value) when is_binary(value) and byte_size(value) in 1..@max_operation_id_bytes do
    valid =
      String.valid?(value) and
        Enum.all?(String.split(value, "/"), fn part ->
          Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._-]*\z/, part) and part not in [".", ".."]
        end)

    if valid do
      :ok
    else
      {:error, :invalid_operation_id}
    end
  end

  defp validate_operation_id(_value), do: {:error, :invalid_operation_id}

  defp validate_workspace(workspace) when is_binary(workspace) and byte_size(workspace) in 1..@max_workspace_bytes do
    if String.valid?(workspace) and Path.type(workspace) == :absolute and Path.expand(workspace) == workspace and not String.contains?(workspace, <<0>>) do
      :ok
    else
      {:error, :invalid_worker_workspace}
    end
  end

  defp validate_workspace(_workspace), do: {:error, :invalid_worker_workspace}

  defp validate_registration_callback(callback) when is_function(callback, 1), do: :ok
  defp validate_registration_callback(_callback), do: {:error, :worker_registration_callback_required}

  defp register_identity(callback, identity) do
    case callback.(identity) do
      :ok -> :ok
      other -> {:error, {:callback_rejected, normalize_callback_result(other)}}
    end
  rescue
    _ -> {:error, :callback_failed}
  catch
    _, _ -> {:error, :callback_failed}
  end

  defp normalize_callback_result(:ok), do: :ok
  defp normalize_callback_result({:error, reason}) when is_atom(reason), do: reason
  defp normalize_callback_result(other) when is_atom(other), do: other
  defp normalize_callback_result(_other), do: :rejected

  defp validate_identity_shape(identity, operation_id, config) when is_map(identity) do
    with true <- Enum.sort(Map.keys(identity)) == Enum.sort(@identity_keys),
         true <- identity["kind"] == "systemd-unit",
         true <- identity["operation_id"] == operation_id,
         true <- identity["machine_id"] == config["machine_id"],
         true <- valid_boot_id?(identity["boot_id"]),
         true <- identity["unit"] == unit_name(operation_id),
         true <- valid_invocation_id?(identity["invocation_id"]),
         true <- identity["control_group"] == "/system.slice/" <> unit_name(operation_id),
         true <- valid_sha256?(identity["request_sha256"]) do
      {:ok, identity}
    else
      _ -> {:error, :invalid_worker_identity}
    end
  end

  defp validate_identity_shape(_identity, _operation_id, _config), do: {:error, :invalid_worker_identity}

  defp valid_boot_id?(value) when is_binary(value), do: String.valid?(value) and Regex.match?(~r/\A[0-9a-f]{32}\z/, value)
  defp valid_boot_id?(_value), do: false

  defp valid_sha256?(value) when is_binary(value), do: String.valid?(value) and Regex.match?(~r/\A[a-f0-9]{64}\z/, value)
  defp valid_sha256?(_value), do: false

  defp valid_invocation_id?(value) when is_binary(value), do: String.valid?(value) and Regex.match?(~r/\A[0-9a-f]{32}\z/, value)
  defp valid_invocation_id?(_value), do: false

  defp unit_name(operation_id) do
    digest = :crypto.hash(:sha256, operation_id) |> Base.encode16(case: :lower)
    "factory-operation-#{digest}.service"
  end

  defp identity_operation_id(%{"operation_id" => operation_id}), do: operation_id
  defp identity_operation_id(_identity), do: nil

  defp expected_release(config), do: %{"service_revision" => config["service_revision"], "release_sha256" => config["release_sha256"]}

  defp rpc(config, envelope, opts) do
    case Keyword.get(opts, :rpc_fun) do
      fun when is_function(fun, 2) -> normalize_rpc_hook(fun.(config, envelope))
      nil -> rpc_port(config, envelope, opts)
      _ -> {:error, :invalid_rpc_hook}
    end
  rescue
    _ -> {:error, :worker_rpc_failed}
  catch
    _, _ -> {:error, :worker_rpc_failed}
  end

  defp normalize_rpc_hook({:ok, payload}) when is_map(payload), do: {:ok, payload}
  defp normalize_rpc_hook({:error, _} = error), do: error
  defp normalize_rpc_hook(_), do: {:error, :worker_rpc_protocol_error}

  defp rpc_port(config, envelope, opts) do
    with {:ok, json} <- Jason.encode(envelope),
         true <- byte_size(json) <= @max_rpc_bytes,
         command <- "factory-operation rpc " <> Base.url_encode64(json, padding: false),
         {:ok, port} <- start_rpc_port(config["host"], command, opts) do
      deadline = monotonic_ms() + Keyword.get(opts, :rpc_timeout_ms, @rpc_timeout_ms)

      try do
        with {:ok, line} <- read_rpc_line(port, deadline) do
          close_after_rpc_result(port, line, deadline)
        end
      after
        close_port(port)
      end
    else
      false -> {:error, :worker_rpc_request_too_large}
      {:error, _} = error -> error
    end
  rescue
    _ -> {:error, :worker_rpc_failed}
  catch
    _, _ -> {:error, :worker_rpc_failed}
  end

  defp start_rpc_port(host, command, opts) do
    case Keyword.get(opts, :rpc_start_fun) do
      fun when is_function(fun, 3) -> fun.(host, command, line: @max_rpc_bytes, raw_command: true)
      nil -> SSH.start_port(host, command, line: @max_rpc_bytes, raw_command: true)
      _ -> {:error, :invalid_rpc_start_hook}
    end
  end

  defp stream_start(host, command, opts) do
    case Keyword.get(opts, :stream_start_fun) do
      fun when is_function(fun, 3) -> fun.(host, command, line: @max_rpc_bytes, raw_command: true)
      nil -> SSH.start_port(host, command, line: @max_rpc_bytes, raw_command: true)
      _ -> {:error, :invalid_stream_start_hook}
    end
  rescue
    _ -> {:error, :worker_stream_open_failed}
  catch
    _, _ -> {:error, :worker_stream_open_failed}
  end

  defp read_rpc_line(port, deadline), do: read_rpc_line(port, deadline, "")

  defp read_rpc_line(port, deadline, partial) do
    receive do
      {^port, {:data, {:eol, line}}} ->
        {:ok, partial <> bytes(line)}

      {^port, {:data, {:noeol, line}}} ->
        next = partial <> bytes(line)
        if byte_size(next) <= @max_rpc_bytes, do: read_rpc_line(port, deadline, next), else: {:error, :worker_rpc_response_too_large}

      {^port, {:data, data}} when is_binary(data) ->
        next = partial <> data
        if byte_size(next) <= @max_rpc_bytes, do: read_rpc_line(port, deadline, next), else: {:error, :worker_rpc_response_too_large}

      {^port, {:exit_status, _status}} ->
        {:error, :worker_rpc_response_missing}

      {^port, :closed} ->
        {:error, :worker_rpc_response_missing}
    after
      max(deadline - monotonic_ms(), 0) -> {:error, :worker_rpc_timeout}
    end
  end

  defp close_after_rpc_result(port, line, deadline) do
    result = await_rpc_exit(port, deadline)

    case result do
      {:ok, 0} ->
        decode_rpc_response(line)

      {:ok, _nonzero_status} ->
        case decode_rpc_response(line) do
          {:error, {:remote_error, _reason}} = error -> error
          _ -> {:error, :worker_rpc_transport_failed}
        end

      _ ->
        {:error, :worker_rpc_transport_failed}
    end
  end

  defp await_rpc_exit(port, deadline) do
    receive do
      {^port, {:exit_status, status}} -> {:ok, status}
      {^port, {:data, _extra}} -> {:error, :worker_rpc_extra_output}
      {^port, :closed} -> {:error, :worker_rpc_transport_closed}
    after
      max(deadline - monotonic_ms(), 0) -> {:error, :worker_rpc_timeout}
    end
  end

  defp decode_rpc_response(line) do
    case Jason.decode(line) do
      {:ok, %{"ok" => payload} = response} when map_size(response) == 1 and is_map(payload) ->
        {:ok, payload}

      {:ok, %{"error" => error} = response} when map_size(response) == 1 and is_binary(error) ->
        if Regex.match?(~r/\A[a-z][a-z0-9_]{0,63}\z/, error), do: {:error, {:remote_error, error}}, else: {:error, :worker_rpc_protocol_error}

      _ ->
        {:error, :worker_rpc_protocol_error}
    end
  end

  defp encode_envelope!(envelope), do: envelope |> Jason.encode!() |> Base.url_encode64(padding: false)

  defp safe_port_command(port, data) do
    Port.command(port, data, [:nosuspend])
  rescue
    _ -> false
  catch
    _, _ -> false
  end

  defp close_port(port) when is_port(port) do
    if Port.info(port) != nil, do: Port.close(port)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp close_port(_port), do: :ok

  defp bytes(value) when is_binary(value), do: value
  defp bytes(value) when is_list(value), do: IO.iodata_to_binary(value)
  defp bytes(_value), do: ""

  defp termination_timeout(opts), do: Keyword.get(opts, :termination_timeout_ms, @termination_timeout_ms)
  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end
