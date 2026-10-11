defmodule SymphonyElixirWeb.EvidenceController do
  @moduledoc "Small private HTML viewer for durable evidence; no websocket identity session."
  use Phoenix.Controller, formats: [:html]

  alias SymphonyElixir.Evidence

  @spec index(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def index(conn, _params) do
    case Evidence.list() do
      {:ok, tasks} ->
        entries =
          Enum.map_join(tasks, "", fn task ->
            "<li><a href=\"/tasks/" <> escape(task.key) <> "\">" <> escape(task.task_id) <> "</a> · " <> escape(task.run_id) <> " · " <> escape(task.status) <> "</li>"
          end)

        html(
          conn,
          "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Task evidence</title></head><body><h1>Task evidence</h1><p>Up to 100 retained runs. Older tasks remain available at their Linear links.</p><ul>" <>
            entries <> "</ul></body></html>"
        )

      _ ->
        conn |> put_status(503) |> html("Evidence is unavailable.")
    end
  end

  @spec show(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def show(conn, %{"task_key" => key}) do
    case Evidence.task(key) do
      {:ok, []} -> conn |> put_status(404) |> html("Evidence task not found.")
      {:ok, runs} -> html(conn, render_task(runs))
      _ -> conn |> put_status(503) |> html("Evidence is unavailable. Archival is not confirmed.")
    end
  end

  @spec artifact(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def artifact(conn, %{"task_key" => key, "revision" => revision, "name" => name}) do
    case Evidence.read_artifact(key, revision, name) do
      {:ok, payload} -> conn |> put_resp_content_type("text/plain") |> send_resp(200, payload)
      _ -> conn |> put_resp_content_type("text/plain") |> send_resp(404, "Evidence artifact unavailable.")
    end
  end

  @spec render_task([map()]) :: String.t()
  def render_task(runs) do
    "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>Task evidence</title>" <>
      "<link rel=\"stylesheet\" href=\"/dashboard.css\"></head><body><main style=\"max-width:1000px;margin:2rem auto;padding:1rem\"><h1>Task evidence</h1>" <>
      "<p>Private observable conversation and test evidence. Reload to see new events.</p>" <>
      Enum.map_join(runs, "\n", &render_run/1) <> "</main></body></html>"
  end

  defp render_run(run) do
    snapshot = run.snapshot

    "<article><h2>Run " <>
      escape(snapshot["id"]) <>
      "</h2><p>Task: " <>
      escape(snapshot["task_id"]) <>
      " · State: " <>
      escape(snapshot["status"]) <>
      " · Archive: <strong>" <>
      escape(run.archive["status"]) <>
      "</strong></p>" <>
      "<p>Evidence revision: <code>" <>
      escape(run.revision) <>
      "</code></p>" <>
      links(run) <>
      "<h3>Stages and attempts</h3>" <>
      Enum.map_join(snapshot["attempts"] || [], "", &attempt/1) <>
      "<h3>Waiting gate and questions</h3>" <>
      json(%{"pending_wait" => snapshot["pending_wait"], "questions" => snapshot["questions"]}) <>
      "<details><summary>Pinned definitions, agents and policy</summary>" <>
      json(Map.take(snapshot, ~w(definitions pinned_definitions agents policy))) <>
      "</details>" <>
      "<h3>Observable events</h3>" <>
      Enum.map_join(run.events, "", &json/1) <>
      "<h3>Artifact manifest</h3>" <> artifact_links(run) <> json(run.artifacts) <> "</article>"
  end

  defp artifact_links(run) do
    key = Evidence.task_key(run.snapshot["task_id"])

    Enum.map_join(run.artifacts, "", fn artifact ->
      "<p><a href=\"/tasks/" <> escape(key) <> "/artifacts/" <> escape(run.revision) <> "/" <> escape(artifact["name"]) <> "\">" <> escape(artifact["name"]) <> "</a></p>"
    end)
  end

  defp attempt(value) do
    "<section><h4>Stage " <>
      escape(value["stage"]) <>
      " · Attempt " <>
      escape(value["ordinal"]) <>
      "</h4>" <>
      json(Map.take(value, ~w(id type status gate gate_detail result continuation inbox_messages))) <> "</section>"
  end

  defp links(run) do
    parent = if run.parent_key, do: link(run.parent_key, "Parent task"), else: ""
    parent <> Enum.map_join(run.child_keys, "", &link(&1, "Child task"))
  end

  defp link(key, label), do: "<p><a href=\"/tasks/" <> escape(key) <> "\">" <> label <> "</a></p>"
  defp json(value), do: "<pre style=\"white-space:pre-wrap;overflow-wrap:anywhere\">" <> escape(Jason.encode!(value, pretty: true)) <> "</pre>"
  defp escape(nil), do: ""
  defp escape(value), do: value |> to_string() |> Phoenix.HTML.html_escape() |> Phoenix.HTML.safe_to_string()
end
