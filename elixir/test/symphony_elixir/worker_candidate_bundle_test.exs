defmodule SymphonyElixir.WorkerCandidateBundleTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.WorkerOperation

  @config %{
    "host" => "factory-control@worker",
    "machine_id" => String.duplicate("a", 32),
    "service_revision" => String.duplicate("b", 40),
    "release_sha256" => String.duplicate("c", 64),
    "argv" => ["/usr/bin/true"]
  }
  @id "run/local-commit/1"

  test "reads ordered bounded chunks tied to the successful operation and verifies the whole checksum" do
    identity = identity()
    data = :binary.copy("x", 70_000)
    metadata = metadata(data)
    parent = self()

    rpc = fn _config, request ->
      case request["action"] do
        "status" ->
          {:ok, terminated(identity)}

        "candidate_bundle" ->
          send(parent, {:candidate_read, request})
          offset = request["offset"]
          chunk = binary_part(data, offset, min(65_536, byte_size(data) - offset))
          {:ok, reply(identity, metadata, offset, chunk)}
      end
    end

    assert {:ok, ^data} = WorkerOperation.candidate_bundle(@config, identity, metadata, rpc_fun: rpc)
    assert_receive {:candidate_read, first}
    assert first["offset"] == 0
    refute Map.has_key?(first, "path")
    assert first["expected_release"]["service_revision"] == @config["service_revision"]
    assert_receive {:candidate_read, second}
    assert second["offset"] == 65_536
  end

  test "unknown/running/failed operations and wrong chunk identity or digest fail closed" do
    identity = identity()
    data = "candidate bundle contract bytes"
    metadata = metadata(data)

    for mutation <- [:running, :failed, :wrong_offset, :wrong_input, :wrong_digest, :corrupt_bytes, :early_eof, :oversized_chunk] do
      rpc = fn _config, request ->
        if request["action"] == "status" do
          status = terminated(identity)

          status =
            case mutation do
              :running -> %{status | "status" => "running"}
              :failed -> status |> Map.put("exit_status", 1) |> put_in(["termination_proof", "exec_main_status"], 1)
              _ -> status
            end

          {:ok, status}
        else
          response = reply(identity, metadata, 0, data)

          response =
            case mutation do
              :wrong_offset -> Map.put(response, "offset", 1)
              :wrong_input -> Map.put(response, "candidate_input_sha256", String.duplicate("f", 64))
              :wrong_digest -> Map.put(response, "bundle_sha256", String.duplicate("f", 64))
              :corrupt_bytes -> Map.put(response, "data_base64", Base.encode64(:binary.copy("z", byte_size(data))))
              :early_eof -> Map.put(response, "eof", false)
              :oversized_chunk -> Map.put(response, "data_base64", Base.encode64(:binary.copy("x", 65_537)))
            end

          {:ok, response}
        end
      end

      assert {:error, _} = WorkerOperation.candidate_bundle(@config, identity, metadata, rpc_fun: rpc)
    end
  end

  defp metadata(data), do: %{"candidate_input_sha256" => String.duplicate("d", 64), "bundle_sha256" => digest(data), "bundle_bytes" => byte_size(data)}

  defp reply(identity, metadata, offset, data) do
    %{
      "operation_id" => identity["operation_id"],
      "candidate_input_sha256" => metadata["candidate_input_sha256"],
      "bundle_sha256" => metadata["bundle_sha256"],
      "bundle_bytes" => metadata["bundle_bytes"],
      "offset" => offset,
      "data_base64" => Base.encode64(data),
      "eof" => offset + byte_size(data) == metadata["bundle_bytes"]
    }
  end

  defp identity do
    unit = "factory-operation-#{digest(@id)}.service"

    %{
      "kind" => "systemd-unit",
      "operation_id" => @id,
      "machine_id" => @config["machine_id"],
      "boot_id" => String.duplicate("e", 32),
      "unit" => unit,
      "invocation_id" => String.duplicate("f", 32),
      "control_group" => "/system.slice/" <> unit,
      "request_sha256" => String.duplicate("d", 64)
    }
  end

  defp terminated(identity) do
    proof =
      Map.take(identity, ~w(machine_id boot_id unit invocation_id control_group))
      |> Map.merge(%{
        "cgroup_state" => "present",
        "cgroup_populated" => 0,
        "active_state" => "inactive",
        "sub_state" => "dead",
        "exec_main_code" => "exited",
        "exec_main_status" => 0,
        "main_pid" => 0,
        "observed_at" => "2026-10-11T00:00:00Z",
        "systemd_version" => 254
      })

    %{"operation_id" => @id, "identity" => identity, "status" => "terminated", "exit_code" => "exited", "exit_status" => 0, "termination_proof" => proof}
  end

  defp digest(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
