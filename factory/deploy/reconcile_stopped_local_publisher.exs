defmodule SymphonyElixir.Factory.ReconcileStoppedLocalPublisher do
  @moduledoc """
  Offline, service-owned termination bookkeeping for a stopped pre-intent local
  publisher. Never starts the coordinator or executes any publication operation.
  """

  alias Exqlite.Sqlite3
  alias SymphonyElixir.{Config, Evidence, WorkstreamRun, WorkstreamStore, Workflow}

  @unit "factory-coordinator.service"
  @user "factory-coordinator"
  @executor "ec8ae546aeb408243529f96a1a7048c92d4a61bbe268eee6decbccfb169f4c22"
  @legacy_executor "505e0686567846995558b576a12f5e63a1edf2cfdd0e2a285e890683a9d15179"
  @policy %{
    version: 1,
    gate_policy: "exit-status-v1",
    lifecycle_sha256: "96b3f754fae20657a74993320094ab5213735a84157f6022915099babc671545",
    child_sha256: "a598793bd67ae523b441445143c548b0ed28e9572c471196f3f7291e20b465d5",
    validation_sha256: "47a77cb57082970ce8f6ca5515b327fc49c31a68a19482fff07893d93efc9cbf"
  }
  @switches [apply: :boolean, workflow_path: :string, store_path: :string, run_id: :string, attempt_id: :string, worker_boot_id: :string, worker_pid: :string, machine_id: :string, operator: :string]

  @doc "Reconciles only the named stopped publisher under the caller's exclusive store ownership."
  @spec reconcile(GenServer.server(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def reconcile(store, expected, opts \\ []) do
    probe = Keyword.get(opts, :probe, &probe_host/1)

    with true <- WorkstreamRun.policy() == Map.put(@policy, :executor_sha256, @executor),
         {:ok, _} <- Config.evidence(),
         {:ok, run} <- WorkstreamStore.fetch(store, expected.run_id),
         :ok <- target_matches(run, expected),
         {:ok, _proof} <- probe.(expected.machine_id) do
      event_id = expected.attempt_id <> "/stopped-local-publisher-terminated-v1"

      case WorkstreamStore.event(store, event_id) do
        :not_found -> confirm(store, run, expected, event_id, probe, Keyword.get(opts, :apply, false))
        {:ok, %{run_id: id, event: event}} when id == run.id -> duplicate(run, event_id, event)
        _ -> {:error, :termination_event_conflict}
      end
    else
      false -> {:error, :unsupported_source_policy}
      :not_found -> {:error, :run_not_found}
      {:error, _} = error -> error
      _ -> {:error, :invalid_request}
    end
  rescue
    _ -> {:error, :local_publisher_reconciliation_failed}
  end

  defp target_matches(run, expected) do
    operation = run.operations[run.current_attempt_id]
    worker = operation && operation[:worker]

    valid =
      run.status == :stopped and run.phase == :stopped and run.current_attempt_id == expected.attempt_id and
        Map.delete(run.policy, :executor_sha256) == @policy and run.policy.executor_sha256 in [@executor, @legacy_executor] and
        is_map(operation) and operation.id == expected.attempt_id and operation.attempt_id == expected.attempt_id and
        operation.type == :publish and operation.stage == run.stage_id and run.definition.stages[run.stage_id].type == :publish and
        run.execution[:publisher_transport] in [nil, SymphonyElixir.PublisherGithub] and
        is_map(worker) and worker.node == "nonode@nohost" and worker.boot_id == expected.worker_boot_id and worker.pid == expected.worker_pid

    if valid, do: :ok, else: {:error, :unexpected_stopped_publisher}
  end

  defp confirm(store, run, expected, event_id, probe, apply?) do
    operation = run.operations[expected.attempt_id]
    lineage = Enum.filter(Map.values(run.operations), &(&1.side_effect_id == operation.side_effect_id))
    no_side_effect = Enum.all?(lineage, &(is_nil(&1[:publication_intent]) and is_nil(&1[:external_process])))
    attempt = Enum.find(run.attempts, &(&1.id == expected.attempt_id))

    with true <- operation.status == :executing and operation[:reconciliation] == ":validation_not_permanently_archived",
         true <- is_map(attempt) and attempt.type == :publish and attempt.stage == run.stage_id and attempt.status in [:executing, :canceled],
         true <- no_side_effect,
         {:ok, %{run_id: id, event: %{kind: :reconciliation_required}}} when id == run.id <-
           WorkstreamStore.event(store, expected.attempt_id <> "/uncertain"),
         {:ok, proof} <- probe.(expected.machine_id) do
      if apply? do
        commit_confirmation(store, run, expected, event_id, probe, proof, operation)
      else
        {:ok, %{run_id: run.id, event_id: event_id, outcome: :qualified, proof: proof}}
      end
    else
      _ -> {:error, :local_publisher_termination_not_proven}
    end
  end

  defp commit_confirmation(store, run, expected, event_id, probe, proof, operation) do
    updated = WorkstreamRun.confirm_termination(run)

    event =
      %{
        kind: :stopped_local_publisher_termination_confirmed,
        operator: expected.operator,
        proof: proof,
        worker: operation.worker,
        original_reconciliation: operation.reconciliation,
        original_sha256: digest(run),
        confirmed_sha256: digest(updated)
      }
      |> Jason.encode!()
      |> Jason.decode!()

    with true <- Evidence.identity(run) == Evidence.identity(updated),
         :ok <- WorkstreamStore.commit(store, updated, event_id, event),
         {:ok, ^updated} <- WorkstreamStore.fetch(store, run.id),
         {:ok, outbox} <- WorkstreamStore.pending_evidence(store),
         true <-
           Enum.any?(outbox, fn entry ->
             journal = Evidence.journal(updated, event)
             entry.event_id == event_id and entry.snapshot == journal.snapshot and entry.event == journal.event
           end),
         {:ok, _} <- probe.(expected.machine_id) do
      {:ok, %{run_id: run.id, event_id: event_id, outcome: :committed}}
    else
      _ -> {:error, {:commit_confirmation_failed, event_id}}
    end
  end

  defp duplicate(run, event_id, %{"kind" => "stopped_local_publisher_termination_confirmed", "confirmed_sha256" => sha}) do
    if digest(run) == sha and get_in(run, [:cancellation, :termination]) == :terminated,
      do: {:ok, %{run_id: run.id, event_id: event_id, outcome: :duplicate}},
      else: {:error, :termination_event_conflict}
  end

  defp duplicate(_run, _event_id, _event), do: {:error, :termination_event_conflict}

  @doc "Reads fresh Linux/systemd proof. A supplied JSON receipt is never used as live proof."
  @spec probe_host(String.t()) :: {:ok, map()} | {:error, atom()}
  def probe_host(expected_machine) do
    with {:unix, :linux} <- :os.type(),
         {:ok, machine} <- File.read("/etc/machine-id"),
         true <- String.trim(machine) == expected_machine,
         {uid, 0} <- System.cmd("/usr/bin/id", ["-u", @user]),
         {uid, ""} <- Integer.parse(String.trim(uid)),
         {output, 0} <- System.cmd("/usr/bin/systemctl", ["show", @unit, "--no-pager", "--property=Id,LoadState,ActiveState,SubState,MainPID,ControlGroup,User,KillMode"]),
         properties = Map.new(String.split(output, "\n", trim: true), fn line -> List.to_tuple(String.split(line, "=", parts: 2)) end),
         :ok <- stopped_unit(properties),
         :ok <- empty_cgroup(),
         {:ok, processes} <- processes(),
         {self_pid, ""} <- Integer.parse(System.pid()),
         {:ok, self_process} <- Map.fetch(processes, self_pid),
         true <- self_process.uid == uid,
         :ok <- no_survivors(processes, self_pid, uid) do
      {:ok,
       %{
         unit: @unit,
         machine_id: expected_machine,
         uid: uid,
         service: properties,
         maintenance_pid: self_pid,
         maintenance_start_ticks: self_process.start_ticks,
         cgroup_empty: true,
         other_uid_processes: []
       }}
    else
      _ -> {:error, :coordinator_termination_not_proven}
    end
  rescue
    _ -> {:error, :coordinator_termination_not_proven}
  end

  @doc false
  @spec stopped_unit(map()) :: :ok | {:error, atom()}
  def stopped_unit(properties) do
    if properties["Id"] == @unit and properties["LoadState"] == "loaded" and
         properties["ActiveState"] == "inactive" and properties["SubState"] == "dead" and
         properties["MainPID"] == "0" and properties["ControlGroup"] == "" and
         properties["User"] == @user and properties["KillMode"] == "control-group",
       do: :ok,
       else: {:error, :coordinator_not_stopped}
  end

  defp empty_cgroup do
    with {:ok, _} <- File.read("/sys/fs/cgroup/cgroup.controllers") do
      case File.read("/sys/fs/cgroup/system.slice/" <> @unit <> "/cgroup.events") do
        {:error, :enoent} -> :ok
        {:ok, text} -> if "populated 0" in String.split(text, "\n"), do: :ok, else: {:error, :cgroup_populated}
        _ -> {:error, :cgroup_unreadable}
      end
    end
  end

  defp processes do
    with {:ok, entries} <- File.ls("/proc") do
      Enum.reduce_while(entries, {:ok, %{}}, fn entry, {:ok, found} ->
        case Integer.parse(entry) do
          {pid, ""} ->
            case process(entry) do
              {:ok, value} -> {:cont, {:ok, Map.put(found, pid, value)}}
              {:error, :enoent} -> {:cont, {:ok, found}}
              _ -> {:halt, {:error, :process_inventory_unavailable}}
            end

          _ ->
            {:cont, {:ok, found}}
        end
      end)
    end
  end

  defp process(entry) do
    with {:ok, status} <- File.read("/proc/" <> entry <> "/status"),
         [_, uid] <- Regex.run(~r/^Uid:\s+(\d+)/m, status),
         {:ok, stat} <- File.read("/proc/" <> entry <> "/stat"),
         [{offset, 2} | _] <- Enum.reverse(:binary.matches(stat, ") ")) do
      fields = stat |> binary_part(offset + 2, byte_size(stat) - offset - 2) |> String.split()
      {:ok, %{uid: String.to_integer(uid), ppid: String.to_integer(Enum.at(fields, 1)), start_ticks: String.to_integer(Enum.at(fields, 19))}}
    end
  end

  @doc false
  @spec no_survivors(map(), pos_integer(), non_neg_integer()) :: :ok | {:error, atom()}
  def no_survivors(processes, own_pid, uid) do
    if Enum.all?(processes, fn {pid, process} -> process.uid != uid or descendant?(processes, pid, own_pid, MapSet.new()) end),
      do: :ok,
      else: {:error, :coordinator_uid_process_survives}
  end

  defp descendant?(_processes, pid, pid, _seen), do: true

  defp descendant?(processes, pid, ancestor, seen) do
    case Map.get(processes, pid) do
      %{ppid: ppid} when ppid > 0 ->
        not MapSet.member?(seen, pid) and descendant?(processes, ppid, ancestor, MapSet.put(seen, pid))

      _ ->
        false
    end
  end

  @spec run_cli([String.t()]) :: {:ok, map()} | {:error, term()}
  def run_cli(argv) do
    with {options, [], []} <- OptionParser.parse(argv, strict: @switches),
         options = Map.new(options),
         true <- Enum.all?(Keyword.keys(@switches) -- [:apply], &Map.has_key?(options, &1)),
         true <- Regex.match?(~r/\A[0-9a-f]{32}\z/, options.machine_id),
         true <- Regex.match?(~r/\A[A-Za-z0-9._@\/-]{1,128}\z/, options.operator),
         true <- Enum.all?([:workflow_path, :store_path], &(Path.type(options[&1]) == :absolute)),
         :ok <- dependencies(),
         true <- not Enum.any?(Application.started_applications(), &(elem(&1, 0) == :symphony_elixir)),
         {:ok, _} <- probe_host(options.machine_id),
         :ok <- Workflow.set_workflow_file_path(options.workflow_path),
         {:ok, config} <- Config.linear_delegation(),
         true <- Path.expand(config["store_path"]) == Path.expand(options.store_path),
         :ok <- existing_schema(options.store_path),
         {:ok, store} <- WorkstreamStore.start_link(path: options.store_path, owner: self()) do
      try do
        reconcile(store, options, apply: Map.get(options, :apply, false))
      after
        GenServer.stop(store)
      end
    else
      _ -> {:error, :offline_preflight_failed}
    end
  rescue
    _ -> {:error, :offline_preflight_failed}
  end

  defp dependencies do
    for app <- [:yaml_elixir, :jason, :exqlite], do: {:ok, _} = Application.ensure_all_started(app)
    _ = Application.load(:symphony_elixir)
    for module <- Application.spec(:symphony_elixir, :modules), do: {:module, _} = Code.ensure_loaded(module)
    :ok
  end

  defp existing_schema(path) do
    with {:ok, %{type: :regular, links: 1}} <- File.lstat(path),
         {:ok, db} <- Sqlite3.open(path, mode: :readonly) do
      try do
        {:ok, statement} = Sqlite3.prepare(db, "PRAGMA user_version")

        try do
          {:ok, mode} = Sqlite3.prepare(db, "PRAGMA journal_mode")

          try do
            if Sqlite3.fetch_all(db, statement) == {:ok, [[3]]} and Sqlite3.fetch_all(db, mode) == {:ok, [["delete"]]},
              do: :ok,
              else: {:error, :schema_three_delete_journal_required}
          after
            Sqlite3.release(db, mode)
          end
        after
          Sqlite3.release(db, statement)
        end
      after
        Sqlite3.close(db)
      end
    end
  end

  defp digest(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic])) |> Base.encode16(case: :lower)
end

if Mix.env() != :test do
  case SymphonyElixir.Factory.ReconcileStoppedLocalPublisher.run_cli(System.argv()) do
    {:ok, result} ->
      IO.puts(Jason.encode!(result))

    {:error, reason} ->
      IO.puts(:stderr, "Stopped local publisher reconciliation failed closed: #{inspect(reason)}")
      System.halt(1)
  end
end
