defmodule SymphonyElixir.Dev243ContractTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{Validation, ValidationPolicy}

  # These private fixtures exercise the existing scalar gate contract. Their
  # test_only assertions are not a Coverage Factory schema or scorer contract.
  # No fixture is loaded by a production workstream or authenticated producer.
  @check "test-only-evaluation"

  test "six evaluation outcomes keep execution distinct from detection and safe-negative acceptance" do
    for {outcome, expected} <- [
          {:pass, :passed},
          {:miss, :failed},
          {:false_positive, :failed},
          {:incomplete, :failed},
          {:unsupported, :failed},
          {:unavailable, :failed}
        ] do
      evidence = fixture(outcome)
      gate = ValidationPolicy.gate(evidence, capability_contract())

      assert gate.verdict == expected, "unexpected capability verdict for #{outcome}"
      assert gate.evidence_id == evidence["id"]
      assert gate.rationale != []

      [check] = evidence["checks"]
      assert check["assertions"]["test_only.specimen_qualified"] == true

      if outcome in [:pass, :miss, :false_positive] do
        assert check["status"] == "completed"
        assert check["exit_status"] == 0
        assert ValidationPolicy.gate(evidence, specimen_contract()).verdict == :passed
      else
        # Even optimistic detection assertions cannot certify an unexecuted check.
        assert check["assertions"]["test_only.vulnerable_detected"] == true
        assert check["assertions"]["test_only.safe_clean"] == true
        assert ValidationPolicy.gate(evidence, specimen_contract()).verdict == :failed
      end
    end
  end

  test "the exact same completed miss satisfies specimen requirements but fails required detection" do
    evidence = fixture(:miss)
    specimen = ValidationPolicy.gate(evidence, specimen_contract())
    capability = ValidationPolicy.gate(evidence, capability_contract())

    assert specimen.verdict == :passed
    assert capability.verdict == :failed
    assert specimen.evidence_id == capability.evidence_id
    assert hd(capability.rationale) =~ "test_only.vulnerable_detected"
    assert hd(capability.rationale) =~ "did not match"
  end

  test "a completed false positive cannot be hidden by a passing detection assertion" do
    evidence = fixture(:false_positive)
    assert ValidationPolicy.gate(evidence, [required("test_only.vulnerable_detected")]).verdict == :passed

    gate = ValidationPolicy.gate(evidence, capability_contract())
    assert gate.verdict == :failed
    assert hd(gate.rationale) =~ "test_only.safe_clean"
  end

  test "absent detection or safe-negative assertions are not completed negative results" do
    for assertion <- ["test_only.specimen_qualified", "test_only.vulnerable_detected", "test_only.safe_clean"] do
      evidence = update_in(fixture(:pass), ["checks", Access.at(0), "assertions"], &Map.delete(&1, assertion))
      gate = ValidationPolicy.gate(evidence, capability_contract())

      assert gate.verdict == :failed
      assert hd(gate.rationale) =~ "missing"
      assert hd(gate.rationale) =~ assertion
    end
  end

  test "a passing synthetic manifest with a matching checksum remains unauthenticated" do
    root = Path.join(System.tmp_dir!(), "dev243-contract-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)

    evidence = fixture(:pass)
    id = sha256(evidence["id"])
    evidence = Map.put(evidence, "id", id)
    bytes = Jason.encode!(evidence)
    File.mkdir_p!(Path.join(root, id))
    File.write!(Path.join([root, id, "manifest.json"]), bytes)
    receipt = %{"id" => id, "manifest_sha256" => sha256(bytes)}

    assert ValidationPolicy.gate(evidence, capability_contract()).verdict == :passed
    assert {:error, :missing_or_unauthenticated_receipt} = Validation.read_receipt(receipt, root)
    refute File.exists?(Path.join(root, "producer.key"))
  end

  defp specimen_contract, do: [required("test_only.specimen_qualified")]

  defp capability_contract do
    specimen_contract() ++ [required("test_only.vulnerable_detected"), required("test_only.safe_clean")]
  end

  defp required(assertion), do: %{check: @check, assertion: assertion, equals: true}

  defp fixture(outcome) do
    status = if outcome in [:pass, :miss, :false_positive], do: "completed", else: to_string(outcome)

    %{
      "id" => "synthetic-dev243-#{outcome}",
      "mode" => "final",
      "complete" => true,
      "checks" => [
        %{
          "id" => @check,
          "status" => status,
          "exit_status" => 0,
          "assertions" => %{
            "test_only.specimen_qualified" => true,
            "test_only.vulnerable_detected" => outcome != :miss,
            "test_only.safe_clean" => outcome != :false_positive
          }
        }
      ]
    }
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
