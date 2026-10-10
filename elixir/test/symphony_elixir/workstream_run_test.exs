defmodule SymphonyElixir.WorkstreamRunTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.WorkstreamRun

  defp definition(failure) do
    %{
      name: "smoke",
      inputs: ["task"],
      input_values: %{"task" => "smoke"},
      entry: "agent",
      agents: %{"worker" => %{model: "controlled", reasoning_effort: "medium", daybreak: false}},
      definitions: %{"fixture" => %{sha256: "pinned"}},
      stages: %{
        "agent" => %{id: "agent", type: :agent, outputs: ["candidate"], next: "check"},
        "check" => %{id: "check", type: :check, outputs: ["result"], gate: %{success: "wait", failure: failure}},
        "wait" => %{id: "wait", type: :human_wait, inputs: ["result"], outputs: ["answer"], prompt: "Continue?", next: :complete}
      }
    }
  end

  defp new(failure \\ :blocked), do: WorkstreamRun.new("task", definition(failure), System.tmp_dir!(), workspace_root: System.tmp_dir!(), codex_command: "pinned", branch: "cycle/test")

  defp to_check(run), do: run |> WorkstreamRun.start_stage() |> WorkstreamRun.finish_stage({:ok, %{session_id: "session", thread_id: "thread"}}) |> WorkstreamRun.start_stage()

  test "failed and malformed agent outcomes block instead of satisfying a gate" do
    for result <- [{:error, :broken}, :malformed, {:ok, "not evidence"}, {:ok, %{opaque: self()}}] do
      report = new() |> WorkstreamRun.start_stage() |> WorkstreamRun.finish_stage(result)
      assert report.status == :blocked
      assert List.last(report.attempts).result.status == :error
    end
  end

  test "checks fail closed on missing, timed-out and unknown evidence" do
    for result <- [{:ok, %{exit_status: 0}}, {:ok, %{exit_status: 0, timed_out: true}}, {:error, :timeout}, :malformed] do
      blocked = new() |> to_check() |> WorkstreamRun.finish_stage(result)
      assert blocked.status == :blocked
      assert List.last(blocked.attempts).gate == :failed
    end
  end

  test "repair budget is separate from transport retries and side-effect IDs survive transport retry" do
    run = new(%{repair: "agent", max_attempts: 1}) |> WorkstreamRun.start_stage()
    first = run.current_attempt_id
    run = WorkstreamRun.record_worker(run, %{pid: "pid", boot_id: "boot"})
    assert run.operations[first].worker.boot_id == "boot"
    run = run |> WorkstreamRun.uncertain(:unreachable) |> WorkstreamRun.retry_transport() |> WorkstreamRun.start_stage()
    assert run.transport_retries == 1
    assert run.operations[run.current_attempt_id].side_effect_id == first
    assert run.operations[run.current_attempt_id].transport_retries == 1
    run = run |> WorkstreamRun.finish_stage({:ok, %{}}) |> WorkstreamRun.start_stage() |> WorkstreamRun.finish_stage({:ok, %{exit_status: 1, timed_out: false}})
    assert run.repairs == %{"check" => 1}
    run = run |> to_check() |> WorkstreamRun.finish_stage({:ok, %{exit_status: 1, timed_out: false}})
    assert run.status == :blocked
    assert run.repairs == %{"check" => 1}
  end

  test "human wait accepts only JSON outputs bound to the current wait" do
    run = new() |> to_check() |> WorkstreamRun.finish_stage({:ok, %{exit_status: 0, timed_out: false}}) |> WorkstreamRun.start_stage()
    wait = run.pending_wait
    assert {:error, :stale_or_missing_human_wait} = WorkstreamRun.wait_answer(run, "stale", %{"answer" => true})
    assert {:error, :stale_or_missing_human_wait} = WorkstreamRun.wait_answer(run, wait.id, "text")
    assert {:error, :invalid_human_wait_outputs} = WorkstreamRun.wait_answer(run, wait.id, %{"answer" => nil})
    assert {:error, :invalid_human_wait_outputs} = WorkstreamRun.wait_answer(run, wait.id, %{"answer" => self()})
    assert {:ok, completed} = WorkstreamRun.wait_answer(run, wait.id, %{"answer" => true})
    assert completed.status == :complete
    assert completed.pending_wait == nil
    assert completed.outputs["answer"] == true
    assert List.last(completed.attempts).result.evidence.artifact_ids == wait.artifact_ids
    assert WorkstreamRun.compatible_policy?(completed)
    refute WorkstreamRun.compatible_policy?(%{completed | policy: %{}})
    report = WorkstreamRun.report(completed)
    refute Map.has_key?(report.outputs, "task")
    refute Map.has_key?(report, :execution)
    assert report.definitions == %{"fixture" => "pinned"}
  end

  test "durable clarification IDs are stable and replies resume the same stage once" do
    run = new() |> WorkstreamRun.start_stage()
    request = %{prompt: "Which module should change?", request_id: "input-7"}

    assert {:ok, run, %{id: wait_id}} = WorkstreamRun.ask_question(run, request)
    assert {:ok, same_run, %{id: ^wait_id}} = WorkstreamRun.ask_question(run, request)
    assert same_run.questions[wait_id].stage_id == "agent"
    assert same_run.questions[wait_id].artifact_ids == %{}
    assert {:error, :conflicting_question_request} = WorkstreamRun.ask_question(run, %{request | prompt: "A different prompt"})

    waiting =
      WorkstreamRun.finish_stage(run, {:waiting, %{wait_id: wait_id, thread_id: "thread-1", session_id: "session-1"}})

    assert waiting.status == :waiting_for_answer
    assert waiting.phase == :waiting_for_answer
    assert waiting.pending_wait.id == wait_id
    assert List.last(waiting.attempts).status == :waiting
    assert waiting.operations[waiting.current_attempt_id].status == :completed

    assert {:ok, resumed} = WorkstreamRun.linear_reply(waiting, "answer #{wait_id}: Change the parser.", "activity-1")
    assert resumed.status == :ready
    assert resumed.stage_id == "agent"
    assert resumed.current_attempt_id == nil
    assert resumed.questions[wait_id].reply.body == "Change the parser."

    assert resumed.continuation == %{
             stage_id: "agent",
             thread_id: "thread-1",
             session_id: "session-1",
             question_id: wait_id,
             question_prompt: "Which module should change?",
             reply: %{body: "Change the parser.", activity_id: "activity-1"},
             questions: [%{id: wait_id, prompt: "Which module should change?", reply: %{body: "Change the parser.", activity_id: "activity-1"}}],
             pending_questions: [],
             messages: [],
             delivery_attempt_id: nil
           }

    assert {:ok, duplicate} = WorkstreamRun.linear_reply(resumed, "answer #{wait_id}: Change the parser.", "activity-1")
    assert duplicate == resumed

    started = WorkstreamRun.start_stage(resumed)
    assert started.stage_id == "agent"
    assert started.attempts |> List.last() |> Map.fetch!(:continuation) |> Map.fetch!(:delivery_attempt_id) == started.current_attempt_id
    assert started.continuation.delivery_attempt_id == started.current_attempt_id
  end

  test "a reply received before the stopped receipt becomes continuation context" do
    run = WorkstreamRun.start_stage(new())
    assert {:ok, run, %{id: wait_id}} = WorkstreamRun.ask_question(run, %{prompt: "Need a choice?"})
    assert {:ok, replied} = WorkstreamRun.linear_reply(run, "Use the new path.", "activity-2")
    assert replied.status == :executing

    resumed = WorkstreamRun.finish_stage(replied, {:waiting, %{wait_id: wait_id, thread_id: "thread-2", session_id: "session-2"}})
    assert resumed.status == :ready
    assert resumed.stage_id == "agent"
    assert resumed.pending_wait == nil
    assert resumed.continuation.reply.body == "Use the new path."
    assert resumed.continuation.thread_id == "thread-2"
  end

  test "clarifications reject stale artifacts and ambiguous untargeted replies" do
    run = WorkstreamRun.start_stage(new())
    assert {:ok, run, %{id: first_id}} = WorkstreamRun.ask_question(run, %{prompt: "First choice?"})
    assert {:ok, run, %{id: second_id}} = WorkstreamRun.ask_question(run, %{prompt: "Second choice?"})
    assert first_id != second_id
    assert {:error, :ambiguous_question_reply} = WorkstreamRun.linear_reply(run, "Choose the first.", "ambiguous-activity")

    changed = %{run | artifacts: %{"patch" => %{run_id: run.id, attempt_id: run.current_attempt_id, sha256: "changed"}}}
    assert {:error, :stale_question_or_artifacts} = WorkstreamRun.question_answer(changed, first_id, "Choose the first.", "stale-activity")
  end

  test "unanswered question IDs remain valid across same-stage clarification continuation" do
    initial = WorkstreamRun.start_stage(new())
    original_attempt = initial.current_attempt_id
    assert {:ok, asked, %{id: first_id}} = WorkstreamRun.ask_question(initial, %{prompt: "First choice?"})
    assert {:ok, asked, %{id: second_id}} = WorkstreamRun.ask_question(asked, %{prompt: "Second choice?"})

    waiting =
      WorkstreamRun.finish_stage(asked, {:waiting, %{wait_id: first_id, thread_id: "thread-4", session_id: "session-4"}})

    assert {:ok, answered_first} = WorkstreamRun.linear_reply(waiting, "answer #{first_id}: First option.", "activity-4")
    continuation = WorkstreamRun.start_stage(answered_first)
    second_question = continuation.questions[second_id]

    assert continuation.stage_id == initial.stage_id
    assert continuation.current_attempt_id != original_attempt
    assert second_question.attempt_id == original_attempt
    assert second_question.active_attempt_id == continuation.current_attempt_id

    stopped =
      WorkstreamRun.finish_stage(continuation, {:waiting, %{wait_id: second_id, thread_id: "thread-5", session_id: "session-5"}})

    assert stopped.status == :waiting_for_answer
    assert stopped.pending_wait.question_origin_attempt_id == original_attempt
    stale_stage = %{stopped | stage_id: "check"}
    assert {:error, :stale_question_or_artifacts} = WorkstreamRun.question_answer(stale_stage, second_id, "Late answer.", "activity-6")
    assert {:ok, answered_second} = WorkstreamRun.linear_reply(stopped, "answer #{second_id}: Second option.", "activity-5")
    assert answered_second.questions[second_id].reply == %{body: "Second option.", activity_id: "activity-5"}
    assert {:ok, duplicate} = WorkstreamRun.linear_reply(answered_second, "answer #{second_id}: Second option.", "activity-5")
    assert duplicate == answered_second

    changed_artifacts = %{
      continuation
      | artifacts: %{"candidate" => %{run_id: continuation.id, attempt_id: continuation.current_attempt_id, sha256: "changed"}}
    }

    stale_wait = WorkstreamRun.finish_stage(changed_artifacts, {:waiting, %{wait_id: second_id, thread_id: "thread-6", session_id: "session-6"}})
    assert stale_wait.status == :blocked
    assert List.last(stale_wait.attempts).result.reason =~ "unmatched_question_wait"
  end

  test "ready-window answers are reconstructed and bound to the dispatched continuation attempt" do
    initial = WorkstreamRun.start_stage(new())
    original_attempt = initial.current_attempt_id
    assert {:ok, asked, %{id: first_id}} = WorkstreamRun.ask_question(initial, %{prompt: "First choice?"})
    assert {:ok, asked, %{id: second_id}} = WorkstreamRun.ask_question(asked, %{prompt: "Second choice?"})

    waiting =
      WorkstreamRun.finish_stage(asked, {:waiting, %{wait_id: first_id, thread_id: "thread-ready", session_id: "session-ready"}})

    assert {:ok, ready} = WorkstreamRun.linear_reply(waiting, "answer #{first_id}: First answer.", "ready-answer-1")
    assert ready.status == :ready
    assert {:ok, answered_before_start} = WorkstreamRun.linear_reply(ready, "answer #{second_id}: Second answer.", "ready-answer-2")
    assert answered_before_start.questions[second_id].attempt_id == original_attempt
    assert Enum.map(answered_before_start.continuation.questions, & &1.id) == [first_id, second_id]

    started = WorkstreamRun.start_stage(answered_before_start)
    second_question = started.questions[second_id]
    continuation = List.last(started.attempts).continuation

    assert second_question.attempt_id == original_attempt
    assert second_question.active_attempt_id == started.current_attempt_id
    second_context = Enum.find(continuation.questions, &(&1.id == second_id))
    assert second_context.reply == %{body: "Second answer.", activity_id: "ready-answer-2"}

    resumed =
      WorkstreamRun.finish_stage(started, {:waiting, %{wait_id: second_id, thread_id: "thread-ready", session_id: "session-ready"}})

    assert resumed.status == :ready
    assert resumed.pending_wait == nil
    resumed_second = Enum.find(resumed.continuation.questions, &(&1.id == second_id))
    assert resumed_second.reply == %{body: "Second answer.", activity_id: "ready-answer-2"}
  end

  test "not-applied transport retries retain question replies and assigned inbox messages" do
    run = WorkstreamRun.start_stage(new())
    assert {:ok, run, %{id: question_id}} = WorkstreamRun.ask_question(run, %{prompt: "Which approach?"})
    assert {:ok, run} = WorkstreamRun.linear_reply(run, "answer #{question_id}: Keep the API stable.", "retry-answer")
    assert {:ok, run} = WorkstreamRun.linear_reply(run, "Also update the docs.", "retry-message")

    run = WorkstreamRun.finish_stage(run, {:waiting, %{wait_id: question_id, thread_id: "thread-retry", session_id: "session-retry"}})
    run = WorkstreamRun.start_stage(run)
    original_attempt = run.current_attempt_id
    original_side_effect = run.operations[original_attempt].side_effect_id
    assert run.continuation.reply.body == "Keep the API stable."
    assert run.continuation.messages == [%{body: "Also update the docs.", activity_id: "retry-message"}]

    retried = run |> WorkstreamRun.uncertain(:unreachable) |> WorkstreamRun.retry_transport() |> WorkstreamRun.start_stage()

    assert retried.current_attempt_id != original_attempt
    assert retried.operations[retried.current_attempt_id].side_effect_id == original_side_effect
    assert retried.continuation.questions == [%{id: question_id, prompt: "Which approach?", reply: %{body: "Keep the API stable.", activity_id: "retry-answer"}}]
    assert retried.continuation.messages == [%{body: "Also update the docs.", activity_id: "retry-message"}]
    assert retried.continuation.delivery_attempt_id == retried.current_attempt_id
    assert retried.attempts |> List.last() |> Map.fetch!(:inbox_messages) == [%{body: "Also update the docs.", activity_id: "retry-message"}]
    assert retried.inbox == [%{body: "Also update the docs.", activity_id: "retry-message", delivered_attempt_id: retried.current_attempt_id}]

    assert {:ok, duplicate} = WorkstreamRun.linear_reply(retried, "Also update the docs.", "retry-message")
    assert duplicate == retried
  end

  test "inbox messages are retained until assigned to one continuation attempt" do
    run = WorkstreamRun.start_stage(new())
    assert {:ok, inboxed} = WorkstreamRun.linear_reply(run, "Please also update the README.", "activity-3")
    assert {:ok, duplicate} = WorkstreamRun.linear_reply(inboxed, "Please also update the README.", "activity-3")
    assert duplicate == inboxed

    ready = WorkstreamRun.finish_stage(inboxed, {:ok, %{thread_id: "thread-3", session_id: "session-3"}})
    assert ready.status == :ready
    assert ready.stage_id == "agent"
    assert ready.operations[inboxed.current_attempt_id].status == :completed
    assert List.last(ready.attempts).status == :completed
    assert ready.continuation.messages == [%{body: "Please also update the README.", activity_id: "activity-3"}]

    started = WorkstreamRun.start_stage(ready)
    assert started.inbox == [%{body: "Please also update the README.", activity_id: "activity-3", delivered_attempt_id: started.current_attempt_id}]
    assert started.continuation.delivery_attempt_id == started.current_attempt_id
    assert List.last(started.attempts).inbox_activity_ids == ["activity-3"]
  end

  test "approval human waits require an explicit digest-bound approval envelope" do
    definition = definition(:blocked)
    wait_stage = Map.put(definition.stages["wait"], :approval, true)
    definition = %{definition | stages: Map.put(definition.stages, "wait", wait_stage)}
    run = WorkstreamRun.new("task", definition, System.tmp_dir!(), workspace_root: System.tmp_dir!(), codex_command: "pinned", branch: "cycle/test")

    run = run |> to_check() |> WorkstreamRun.finish_stage({:ok, %{exit_status: 0, timed_out: false}}) |> WorkstreamRun.start_stage()
    wait = run.pending_wait
    assert is_binary(wait.approval_digest)
    assert {:error, :explicit_approval_required} = WorkstreamRun.wait_answer(run, wait.id, %{"answer" => true})
    assert {:error, :stale_artifact_revision} = WorkstreamRun.linear_reply(run, "approve #{wait.id} #{String.duplicate("0", 64)}", "wrong-activity")
    assert {:error, :explicit_approval_required} = WorkstreamRun.linear_reply(run, "answer #{wait.id}: {}", "wrong-target")

    stale = %{run | artifacts: Map.put(run.artifacts, "new-artifact", %{run_id: run.id, attempt_id: run.current_attempt_id, sha256: "changed"})}
    assert {:error, :stale_artifact_revision} = WorkstreamRun.linear_reply(stale, "approve #{wait.id} #{wait.approval_digest}", "stale-approval")

    assert {:ok, approved} = WorkstreamRun.linear_reply(run, "approve #{wait.id} #{wait.approval_digest}", "activity-4")
    assert approved.status == :complete

    assert approved.attempts |> List.last() |> Map.fetch!(:result) |> Map.fetch!(:evidence) |> Map.fetch!(:approval) == %{
             "approval" => true,
             "artifact_revision" => wait.approval_digest,
             "wait_id" => wait.id
           }
  end
end
