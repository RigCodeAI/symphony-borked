defmodule SymphonyElixir.EvidencePublicationTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{Evidence, WorkstreamRun}

  # This local fixture exercises immutable archive identity checks. Its verified receipt
  # is synthetic and does not represent a GCS upload or a real validation execution.
  test "a verified revision remains usable after later conversation data creates an unverified revision" do
    c = fixture()
    revision = c.revision

    assert {:ok, %{revision: ^revision, archive: %{"status" => "verified"}}} =
             Evidence.publication_revision(c.run, c.attempt_id, c.validation_receipt, revision, c.server)

    :ok = Evidence.snapshot(c.evidence_run, "later-conversation", %{"kind" => "agent_message", "text" => "later text"}, c.server)
    assert {:ok, [latest]} = Evidence.task(Evidence.task_key(c.run.task_id), c.server)
    refute latest.revision == revision
    assert latest.archive["status"] == "pending"

    assert {:ok, %{revision: ^revision}} =
             Evidence.publication_revision(c.run, c.attempt_id, c.validation_receipt, revision, c.server)

    assert {:error, :validation_not_permanently_archived} =
             Evidence.publication_revision(c.run, c.attempt_id, c.validation_receipt, latest.revision, c.server)

    assert {:ok, events} = Evidence.read_artifact(Evidence.task_key(c.run.task_id), revision, "events.jsonl", c.server)
    refute events =~ "later text"
  end

  test "a mismatched archive attempt, receipt, or pinned task identity cannot qualify" do
    c = fixture()
    revision = c.revision

    assert {:error, :validation_not_permanently_archived} =
             Evidence.publication_revision(c.run, "other-attempt", c.validation_receipt, revision, c.server)

    forged_receipt = Map.put(c.validation_receipt, "manifest_sha256", String.duplicate("0", 64))

    assert {:error, :validation_not_permanently_archived} =
             Evidence.publication_revision(c.run, c.attempt_id, forged_receipt, revision, c.server)

    changed_pin = %{c.run | task_id: "DEV-237"}

    assert {:error, :validation_not_permanently_archived} =
             Evidence.publication_revision(changed_pin, c.attempt_id, c.validation_receipt, revision, c.server)

    events_path = Path.join([c.evidence_root, "bundles", revision, "events.jsonl"])
    File.write!(events_path, "corrupted archived bytes")

    assert {:error, :validation_not_permanently_archived} =
             Evidence.publication_revision(c.run, c.attempt_id, c.validation_receipt, revision, c.server)
  end

  defp fixture do
    root = Path.join(System.tmp_dir!(), "evidence-publication-#{System.unique_integer([:positive])}")
    evidence_root = Path.join(root, "evidence")
    File.mkdir_p!(root)

    config = %{
      "root" => evidence_root,
      "base_url" => "https://evidence.example.test",
      "iap_audience" => "/projects/1/global/backendServices/2",
      "allowed_domains" => ["example.test"],
      "allowed_emails" => []
    }

    {:ok, server} = Evidence.start_link(config: {:ok, config}, name: nil, receipt_uid: File.stat!(root).uid)
    attempt_id = "run-evidence-publication/validate/1"
    validation_payload = Jason.encode!(%{"synthetic_contract_fixture" => true, "note" => "not a real validation manifest"})
    validation_receipt = %{"id" => digest("synthetic validation id"), "manifest_sha256" => digest(validation_payload)}

    attempt = %{
      id: attempt_id,
      stage: "validate",
      type: :check,
      ordinal: 1,
      status: :completed,
      gate: :passed,
      result: %{evidence: %{"receipt" => validation_receipt}}
    }

    definition = %{
      inputs: [],
      input_values: %{},
      entry: "publish",
      agents: %{},
      definitions: %{"flow.yaml" => %{sha256: digest("flow"), text: "fixture"}},
      stages: %{}
    }

    run =
      WorkstreamRun.new("DEV-238", definition, root, [], %{})
      |> Map.merge(%{
        id: "run-evidence-publication",
        status: :executing,
        stage_id: "publish",
        attempts: [attempt]
      })

    evidence_run = %{run | stage_id: "validate", current_attempt_id: attempt_id}

    :ok =
      Evidence.snapshot(
        evidence_run,
        "validation-imported",
        %{"kind" => "validation_imported", "manifest_sha256" => validation_receipt["manifest_sha256"]},
        server
      )

    :ok =
      Evidence.artifact(
        %{id: run.id, stage_id: "validate", current_attempt_id: attempt_id},
        "validation/" <> attempt_id <> "/manifest.json",
        validation_payload,
        server
      )

    {:ok, [view]} = Evidence.task(Evidence.task_key(run.task_id), server)
    write_verified_receipt(evidence_root, view)

    on_exit(fn ->
      if Process.alive?(server), do: GenServer.stop(server)
      File.rm_rf(root)
    end)

    %{
      root: root,
      evidence_root: evidence_root,
      server: server,
      run: run,
      evidence_run: evidence_run,
      attempt_id: attempt_id,
      validation_receipt: validation_receipt,
      revision: view.revision
    }
  end

  defp write_verified_receipt(root, view) do
    directory = Path.join(root, "receipts")
    File.mkdir_p!(directory)

    payload = %{
      "status" => "verified",
      "revision" => view.revision,
      "run_id" => view.snapshot["id"],
      "manifest_sha256" => view.revision,
      "artifacts" => view.artifacts
    }

    path = Path.join(directory, view.revision <> ".json")
    File.write!(path, Jason.encode!(payload))
    File.chmod!(path, 0o640)
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
