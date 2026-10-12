alias SymphonyElixir.{Workstream, WorkstreamRun, ReviewWorkspace}
{:ok, definition} = Workstream.load("/workspace/factory/workstreams/software-change.yaml", %{"task" => "approved bounded first-alpha Rig demo"})
command = definition.stages["local-commit"].gate.command
sha = fn bytes -> :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower) end
pins = %{
  service_revision: Jason.decode!(File.read!("/workspace/RELEASE.json"))["service_revision"],
  review_preparation_command_sha256: ReviewWorkspace.command_sha256(),
  candidate_producer_command_sha256: sha.(Jason.encode!(command)),
  workstream_file_sha256: sha.(File.read!("/workspace/factory/workstreams/software-change.yaml")),
  default_cloud_file_sha256: sha.(File.read!("/workspace/factory/agents/default-cloud.yaml")),
  default_review_file_sha256: sha.(File.read!("/workspace/factory/agents/default-review.yaml")),
  policy: WorkstreamRun.policy()
}
File.write!("/workspace/dev240-alpha-pins.json", Jason.encode!(pins, pretty: true) <> "\n")
IO.puts("Combined alpha release pins verified: " <> Jason.encode!(pins))
