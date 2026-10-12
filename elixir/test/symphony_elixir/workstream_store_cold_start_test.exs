defmodule SymphonyElixir.WorkstreamStoreColdStartTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{Workstream, WorkstreamRun, WorkstreamStore}

  setup do
    root = Path.join(System.tmp_dir!(), "cold-store-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    path = Path.join(root, "state.sqlite3")
    definition_path = Path.expand("../../../factory/workstreams/software-change.yaml", __DIR__)
    assert {:ok, definition} = Workstream.load(definition_path, %{"task" => "retained task"})

    run =
      WorkstreamRun.new("task", definition, root, [branch: "cycle/factory-retained"], %{})
      |> WorkstreamRun.start_stage()
      |> WorkstreamRun.finish_stage({:error, :invalid_worker_check_command})

    event = %{kind: :created, issue_id: "issue", session_id: "linear-session"}

    # Redacted shape of Linear.Coordinator's retained delegated task.
    task = %{
      issue_id: "issue",
      run_id: run.id,
      status: :acknowledging,
      session_id: event.session_id,
      activity_id: "activity",
      assignee_id: "human",
      agent_id: "default-cloud",
      route: %{},
      app_user_id: "app",
      oauth_client_id: "oauth",
      event: event,
      owned_child_grant: nil,
      authorized_stage: nil,
      authorized_until: 0,
      scope_revision: 0,
      publications: %{},
      assignment_history: []
    }

    assert {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    assert :ok = WorkstreamStore.commit(store, run, "retained", %{kind: :created})
    assert :ok = WorkstreamStore.linear_receive(store, "delivery", event)
    assert :ok = WorkstreamStore.linear_task(store, task)
    assert {:ok, linear} = WorkstreamStore.linear_load(store)
    assert :ok = GenServer.stop(store)
    fixture = Path.join(root, "expected.etf")
    File.write!(fixture, :erlang.term_to_binary(%{runs: [run], linear: linear}))
    %{path: path, fixture: fixture}
  end

  for mode <- ["store", "image"] do
    test "cold #{mode} reads retained records without changing data or allowing unknown atoms", context do
      original = File.read!(context.path)
      args = Enum.flat_map(:code.get_path(), &["-pa", List.to_string(&1)])

      {output, status} =
        System.cmd(
          System.find_executable("elixir"),
          args ++ ["-e", cold_reader(), "--", context.path, context.fixture, unquote(mode)],
          stderr_to_stdout: true,
          env: [{"ERL_FLAGS", "+S 2:2"}]
        )

      assert status == 0, output
      assert output =~ "COLD_STORE_OK"
      assert File.read!(context.path) == original
    end
  end

  defp cold_reader do
    ~S"""
    import ExUnit.Assertions
    alias SymphonyElixir.WorkstreamStore
    [path, fixture, mode] = System.argv()
    {:ok, _} = Application.ensure_all_started(:exqlite)
    :ok = Application.load(:symphony_elixir)
    assert :code.is_loaded(SymphonyElixir.Linear.Coordinator) == false
    Code.ensure_loaded!(SymphonyElixir.WorkstreamRun)
    raw = File.read!(fixture)
    # Loading WorkstreamRun alone does not populate atoms from the other owners.
    assert_raise ArgumentError, fn -> :erlang.binary_to_term(raw, [:safe]) end

    if mode == "image" do
      assert {:ok, image} = WorkstreamStore.inspect_recovery_image(File.read!(path))
      expected = :erlang.binary_to_term(raw, [:safe])
      assert image.runs === expected.runs
      assert image.linear === expected.linear
    end

    assert {:ok, store} = WorkstreamStore.start_link(path: path, owner: self())
    assert {:ok, runs} = WorkstreamStore.load(store)
    assert {:ok, linear} = WorkstreamStore.linear_load(store)
    expected = :erlang.binary_to_term(raw, [:safe])
    assert runs === expected.runs
    assert linear === expected.linear
    assert :ok = GenServer.stop(store)

    # Database-controlled atom names must still fail safe decoding.
    unknown = "untrusted_retained_atom_" <> Integer.to_string(System.unique_integer([:positive]))
    payload = <<131, 119, byte_size(unknown)>> <> unknown
    copy = path <> ".unknown"
    File.cp!(path, copy)
    File.chmod!(copy, 0o600)
    {:ok, db} = Exqlite.Sqlite3.open(copy)
    {:ok, statement} = Exqlite.Sqlite3.prepare(db, "UPDATE runs SET payload = ?1")
    :ok = Exqlite.Sqlite3.bind(statement, [{:blob, payload}])
    :done = Exqlite.Sqlite3.step(db, statement)
    :ok = Exqlite.Sqlite3.release(db, statement)
    :ok = Exqlite.Sqlite3.close(db)
    assert {:ok, store} = WorkstreamStore.start_link(path: copy, owner: self())
    assert {:error, {:term_decode_failed, %ArgumentError{}}} = WorkstreamStore.load(store)
    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown) end
    assert :ok = GenServer.stop(store)
    IO.puts("COLD_STORE_OK")
    """
  end
end
