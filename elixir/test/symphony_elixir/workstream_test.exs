defmodule SymphonyElixir.WorkstreamTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Workstream

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-workstream-test-#{System.unique_integer([:positive])}"
      )

    workstream_path = Path.join([root, "workstreams", "local-demo.yml"])
    agent_path = Path.join([root, "agents", "worker.yml"])
    instruction_path = Path.join([root, "agents", "instructions", "implementation.md"])
    skill_path = Path.join([root, "agents", "skills", "review", "SKILL.md"])

    File.mkdir_p!(Path.dirname(workstream_path))
    File.mkdir_p!(Path.dirname(instruction_path))
    File.mkdir_p!(Path.dirname(skill_path))
    File.write!(workstream_path, workstream_yaml())
    File.write!(agent_path, agent_yaml())
    File.write!(instruction_path, "Implement the requested change.\n")
    File.write!(skill_path, "Review the candidate diff.\n")

    on_exit(fn -> File.rm_rf(root) end)

    context = [
      workstream_path: workstream_path,
      agent_path: agent_path,
      instruction_path: instruction_path,
      skill_path: skill_path
    ]

    {:ok, context}
  end

  test "loads the versioned workstream and pins all referenced definitions", context do
    inputs = %{"request" => "add a feature"}
    assert {:ok, workstream} = Workstream.load(context.workstream_path, inputs)

    assert workstream.version == 1
    assert workstream.name == "local-demo"
    assert workstream.inputs == ["request"]
    assert workstream.entry == "implement"
    assert workstream.input_values == inputs

    assert workstream.stages["implement"] == %{
             id: "implement",
             type: :agent,
             inputs: ["request"],
             outputs: ["patch"],
             agent: "worker",
             prompt: "Implement the requested change.",
             next: "verify"
           }

    assert workstream.stages["verify"].type == :check

    assert workstream.stages["verify"].gate == %{
             command: ["sh", "-c", "test -f patch.diff"],
             timeout_ms: 2_000,
             success: :complete,
             failure: :blocked
           }

    agent = workstream.agents["worker"]
    assert agent.name == "Local Worker"
    assert agent.model == "gpt-6.1-sol"
    assert agent.reasoning_effort == "high"
    assert agent.daybreak == false
    assert agent.approval_policy == "never"
    assert agent.sandbox == "workspace-write"

    [instruction] = agent.instructions
    [skill] = agent.skills
    assert instruction.path == context.instruction_path
    assert instruction.text == "Implement the requested change.\n"
    assert instruction.sha256 == sha256(instruction.text)
    assert skill.path == context.skill_path
    assert skill.text == "Review the candidate diff.\n"
    assert skill.sha256 == sha256(skill.text)

    assert workstream.definitions[context.workstream_path].sha256 ==
             sha256(File.read!(context.workstream_path))

    assert workstream.definitions[context.agent_path].sha256 ==
             sha256(File.read!(context.agent_path))
  end

  test "explicit authentication cannot inherit silently or accept credential modes", context do
    for authentication <- ["null", "{mode: api_key, reference: token}"] do
      File.write!(context.agent_path, agent_yaml() <> "\nauthentication: #{authentication}\n")
      assert {:error, {:unsupported_agent_authentication, _}} = Workstream.load_agent(context.agent_path)
    end

    File.write!(context.agent_path, agent_yaml() <> "\nauthentication: {mode: subscription, reference: valid-ref, secret: disallowed}\n")
    assert {:error, _} = Workstream.load_agent(context.agent_path)
  end

  test "requires all declared workstream inputs", context do
    assert {:error, {:missing_workstream_inputs, ["request"]}} =
             Workstream.load(context.workstream_path, %{})

    assert {:error, {:missing_workstream_inputs, ["request"]}} =
             Workstream.load(context.workstream_path)

    assert {:error, {:missing_workstream_inputs, ["request"]}} =
             Workstream.load(context.workstream_path, %{"request" => nil})
  end

  test "rejects atom and undeclared runtime input keys", context do
    assert {:error, {:invalid_workstream_inputs, :keys_must_be_strings}} =
             Workstream.load(context.workstream_path, %{request: "task"})

    assert {:error, {:unknown_workstream_inputs, ["extra"]}} =
             Workstream.load(context.workstream_path, %{"request" => "task", "extra" => "unused"})
  end

  test "resolves agent files and their references from each definition directory", context do
    assert {:ok, loaded} = Workstream.load(context.workstream_path, %{"request" => "task"})
    assert loaded.agents["worker"].instructions |> hd() |> Map.fetch!(:path) == context.instruction_path
    assert loaded.agents["worker"].skills |> hd() |> Map.fetch!(:path) == context.skill_path
  end

  test "pins canonical paths when a definition is reached through a symlink", context do
    alias_path = Path.join(Path.dirname(context.agent_path), "worker-link.yml")
    File.ln_s!(context.agent_path, alias_path)
    replace_in_file!(context.workstream_path, "worker: ../agents/worker.yml", "worker: ../agents/worker-link.yml")

    assert {:ok, loaded} = Workstream.load(context.workstream_path, %{"request" => "task"})
    assert Map.has_key?(loaded.definitions, context.agent_path)
    refute Map.has_key?(loaded.definitions, alias_path)
  end

  test "rejects unknown workstream fields", context do
    append_to_file!(context.workstream_path, "\nunsupported: true\n")

    assert {:error, {:invalid_definition_fields, :workstream, [], ["unsupported"]}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "rejects unknown stage agent references", context do
    replace_in_file!(context.workstream_path, "agent: worker", "agent: missing")

    assert {:error, {:unknown_stage_agent, "implement", "missing"}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "rejects missing stage transitions", context do
    replace_in_file!(context.workstream_path, "next: verify", "next: missing")

    assert {:error, {:unknown_stage_transition, "implement", "missing"}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "loads human waits and permits their normal transition to every stage type or complete", context do
    File.write!(context.workstream_path, """
    version: 1
    name: local-demo
    inputs: [request]
    agents:
      worker: ../agents/worker.yml
    entry: implement
    stages:
      - id: implement
        type: agent
        inputs: [request]
        outputs: [patch]
        agent: worker
        prompt: Implement the requested change.
        next: verify
      - id: verify
        type: check
        inputs: [patch]
        outputs: [check]
        gate:
          command: ["true"]
          timeout_ms: 2000
          success: question
          failure: blocked
      - id: question
        type: human_wait
        inputs: [patch]
        outputs: [answer]
        prompt: What should happen next?
        next: followup
      - id: followup
        type: agent
        inputs: [answer]
        outputs: [followup-result]
        agent: worker
        prompt: Apply the answer.
        next: followup-check
      - id: followup-check
        type: check
        inputs: [followup-result]
        outputs: []
        gate:
          command: ["true"]
          timeout_ms: 2000
          success: second-question
          failure: blocked
      - id: second-question
        type: human_wait
        inputs: [followup-result]
        outputs: [intermediate-answer]
        prompt: Confirm the result.
        next: third-question
      - id: third-question
        type: human_wait
        inputs: [intermediate-answer]
        outputs: [human-answer]
        prompt: Confirm one more thing.
        next: final-check
      - id: final-check
        type: check
        inputs: [human-answer]
        outputs: []
        gate:
          command: ["true"]
          timeout_ms: 2000
          success: complete
          failure: blocked
    """)

    assert {:ok, loaded} = load(context)

    assert loaded.stages["question"] == %{
             id: "question",
             type: :human_wait,
             inputs: ["patch"],
             outputs: ["answer"],
             prompt: "What should happen next?",
             next: "followup"
           }

    assert loaded.stages["second-question"].next == "third-question"
    assert loaded.stages["third-question"].next == "final-check"
    assert loaded.stages["final-check"].gate.success == :complete
  end

  test "human wait can complete directly", context do
    add_human_wait!(context, "patch", "complete", "success: complete")
    assert {:ok, loaded} = load(context)
    assert loaded.stages["question"].next == :complete
  end

  test "human wait approval is an optional boolean setting", context do
    add_human_wait!(context, "patch", "complete", "success: complete")
    replace_in_file!(context.workstream_path, "prompt: Please answer.", "prompt: Please answer.\napproval: true")
    assert {:ok, loaded} = load(context)
    assert loaded.stages["question"].approval

    replace_in_file!(context.workstream_path, "approval: true", "approval: maybe")
    assert {:error, {:invalid_human_wait_approval, "question", "maybe"}} = load(context)
  end

  test "human wait definitions accept exactly their declared fields", context do
    add_human_wait!(context, "patch", "complete", "success: complete")
    replace_in_file!(context.workstream_path, "prompt: Please answer.", "prompt: Please answer.\nextra: true")

    assert {:error, {:invalid_definition_fields, {:stage, "question"}, [], ["extra"]}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    add_human_wait!(context, "patch", "complete", "success: complete")
    replace_in_file!(context.workstream_path, "prompt: Please answer.\n", "")

    assert {:error, {:invalid_definition_fields, {:stage, "question"}, ["prompt"], []}} = load(context)
  end

  test "rejects unknown human wait transitions and inputs missing on an incoming path", context do
    add_human_wait!(context, "missing", "unknown-stage", "success: complete")
    assert {:error, {:unknown_stage_transition, "question", "unknown-stage"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    add_human_wait!(context, "missing", "complete", "success: complete")
    assert {:error, {:stage_inputs_unavailable, "question", ["missing"]}} = load(context)
  end

  test "rejects an unbounded cycle through a human wait", context do
    add_human_wait!(context, "patch", "implement", "success: complete")

    assert {:error, {:unbounded_workstream_cycle, _}} = load(context)
  end

  test "rejects stage inputs that are not available on every incoming path", context do
    replace_in_file!(context.workstream_path, "outputs: [patch]", "outputs: [other]")

    assert {:error, {:stage_inputs_unavailable, "verify", ["patch"]}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "rejects duplicate outputs and outputs that overwrite initial inputs", context do
    replace_in_file!(context.workstream_path, "outputs: [patch]", "outputs: [request]")

    assert {:error, {:stage_outputs_overwrite_inputs, ["request"]}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})

    replace_in_file!(context.workstream_path, "outputs: [request]", "outputs: [patch]")
    replace_in_file!(context.workstream_path, "outputs: []", "outputs: [patch]")

    assert {:error, {:duplicate_stage_outputs, ["patch"]}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "accepts a bounded check repair edge to its preceding agent", context do
    replace_in_file!(
      context.workstream_path,
      "      failure: blocked",
      "failure:\n  repair: implement\n  max_attempts: 2"
    )

    assert {:ok, loaded} = Workstream.load(context.workstream_path, %{"request" => "task"})

    assert loaded.stages["verify"].gate.failure == %{repair: "implement", max_attempts: 2}
  end

  test "rejects repairs outside the bounded attempt range", context do
    replace_in_file!(
      context.workstream_path,
      "      failure: blocked",
      "failure:\n  repair: implement\n  max_attempts: 4"
    )

    assert {:error, {:invalid_gate_repair_attempts, "verify", 4}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "rejects agent settings outside the local execution policy", context do
    replace_in_file!(context.agent_path, "sandbox: workspace-write", "sandbox: unrestricted")

    assert {:error, {:unsupported_agent_setting, "worker", :sandbox, "unrestricted", "workspace-write"}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "rejects skill references that do not name SKILL.md", context do
    replace_in_file!(context.agent_path, "- skills/review/SKILL.md", "- skills/review/README.md")

    assert {:error, {:invalid_skill_reference, "worker", "skills/review/README.md"}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})
  end

  test "rejects empty or malformed instruction and skill references", context do
    replace_in_file!(context.agent_path, "instructions:\n  - instructions/implementation.md", "instructions: []")
    assert {:error, {:invalid_agent_references, "worker", :instructions, :expected_nonempty_list}} = load(context)

    File.write!(context.agent_path, agent_yaml())
    replace_in_file!(context.agent_path, "skills:\n  - skills/review/SKILL.md", "skills: no")
    assert {:error, {:invalid_agent_references, "worker", :skills, :expected_list}} = load(context)

    File.write!(context.agent_path, agent_yaml())
    replace_in_file!(context.agent_path, "- instructions/implementation.md", "- /tmp/instructions.md")
    assert {:error, {:invalid_agent_reference, "worker", :instructions, "/tmp/instructions.md"}} = load(context)

    File.write!(context.agent_path, agent_yaml())
    replace_in_file!(context.agent_path, "- instructions/implementation.md", "- 1")
    assert {:error, {:invalid_agent_reference, "worker", :instructions, 1}} = load(context)

    File.write!(context.agent_path, agent_yaml())
    replace_in_file!(context.workstream_path, "worker: ../agents/worker.yml", "worker: ../agents/missing.yml")
    assert {:error, {:definition_file_error, {:agent, "worker"}, _, :enoent}} = load(context)
  end

  test "reports missing referenced files", context do
    File.rm!(context.skill_path)

    assert {:error, {:definition_file_error, {:agent_reference, "worker", :skills}, path, :enoent}} =
             Workstream.load(context.workstream_path, %{"request" => "task"})

    assert path == context.skill_path
  end

  test "reports errors when a definition path has a file as a parent", context do
    blocked_parent = context.workstream_path <> ".file"
    File.write!(blocked_parent, "not a directory")
    invalid_path = Path.join(blocked_parent, "workstream.yml")

    expected_error =
      {:error, {:definition_path_error, :workstream, invalid_path, {:path_canonicalize_failed, invalid_path, :enotdir}}}

    assert Workstream.load(invalid_path, %{"request" => "task"}) == expected_error
  end

  test "rejects malformed workstream headers and input declarations", context do
    replace_in_file!(context.workstream_path, "version: 1", "version: 2")
    assert {:error, {:unsupported_definition_version, :workstream, 2}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "name: local-demo", "name: \"\"")
    assert {:error, {:invalid_name, ""}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "inputs: [request]", "inputs: request")
    assert {:error, {:invalid_names, :workstream_inputs}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "inputs: [request]", "inputs: [request, request]")
    assert {:error, {:duplicate_names, :workstream_inputs}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "inputs: [request]", "inputs: [1]")
    assert {:error, {:invalid_names, :workstream_inputs}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "name: local-demo", "name: 7")
    assert {:error, {:invalid_name, :workstream_name, 7}} = load(context)
  end

  test "rejects required fields missing from workstream, agent, stage, and gate definitions", context do
    replace_in_file!(context.workstream_path, "name: local-demo\n", "")
    assert {:error, {:invalid_definition_fields, :workstream, ["name"], []}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.agent_path, "model: gpt-6.1-sol\n", "")
    assert {:error, {:invalid_definition_fields, {:agent, "worker"}, ["model"], []}} = load(context)

    File.write!(context.agent_path, agent_yaml())
    replace_in_file!(context.workstream_path, "prompt: Implement the requested change.\n", "")
    assert {:error, {:invalid_definition_fields, {:stage, "implement"}, ["prompt"], []}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "timeout_ms: 2000\n", "")
    assert {:error, {:invalid_definition_fields, {:gate, "verify"}, ["timeout_ms"], []}} = load(context)
  end

  test "rejects invalid agent maps, ids, and paths", context do
    replace_in_file!(context.workstream_path, "agents:\n  worker: ../agents/worker.yml", "agents: {}")
    assert {:error, {:invalid_workstream_agents, :expected_nonempty_map}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "worker: ../agents/worker.yml", "1: ../agents/worker.yml")
    assert {:error, {:invalid_agent_id, 1}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "worker: ../agents/worker.yml", "worker: /tmp/worker.yml")
    assert {:error, {:invalid_agent_path, "worker", "/tmp/worker.yml"}} = load(context)
  end

  test "rejects malformed agent and stage definitions", context do
    replace_in_file!(context.agent_path, "version: 1", "version: 2")
    assert {:error, {:unsupported_definition_version, {:agent, "worker"}, 2}} = load(context)

    File.write!(context.agent_path, agent_yaml())
    replace_in_file!(context.agent_path, "reasoning_effort: high", "reasoning_effort: extreme")
    assert {:error, {:unsupported_reasoning_effort, "worker", "extreme"}} = load(context)

    File.write!(context.agent_path, agent_yaml())
    replace_in_file!(context.agent_path, "daybreak: false", "daybreak: true")
    assert {:ok, definition} = load(context)
    assert definition.agents["worker"].daybreak == true
    assert {:error, :daybreak_execution_unverified} = SymphonyElixir.AgentReadiness.dispatch(definition.agents["worker"])

    File.write!(context.agent_path, agent_yaml())
    replace_in_file!(context.agent_path, "approval_policy: never", "approval_policy: on-request")

    assert {:error, {:unsupported_agent_setting, "worker", :approval_policy, "on-request", "never"}} =
             load(context)

    File.write!(context.agent_path, agent_yaml())
    append_to_file!(context.agent_path, "unsupported: true\n")
    assert {:error, {:invalid_definition_fields, {:agent, "worker"}, [], ["unsupported"]}} = load(context)

    File.write!(context.agent_path, agent_yaml())
    replace_in_file!(context.workstream_path, "type: agent", "type: evaluator")
    assert {:error, {:invalid_stage_type, "evaluator"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "type: agent", "")
    assert {:error, {:invalid_stage_definition, _}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())

    replace_in_file!(
      context.workstream_path,
      "prompt: Implement the requested change.\n",
      "prompt: Implement the requested change.\nextra: true\n"
    )

    assert {:error, {:invalid_definition_fields, {:stage, "implement"}, [], ["extra"]}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "- id: verify", "- id: implement")
    assert {:error, {:duplicate_stage_id, "implement"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())

    File.write!(
      context.workstream_path,
      "version: 1\nname: local-demo\ninputs: [request]\nagents:\n  worker: ../agents/worker.yml\nentry: implement\nstages: []\n"
    )

    assert {:error, {:invalid_workstream_stages, :expected_nonempty_list}} = load(context)
  end

  test "rejects invalid gate command, timeout, success, and failure values", context do
    replace_in_file!(context.workstream_path, "command: [sh, -c, \"test -f patch.diff\"]", "command: []")
    assert {:error, {:invalid_gate_command, "verify"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "timeout_ms: 2000", "timeout_ms: 0")
    assert {:error, {:invalid_positive_integer, {:gate_timeout, "verify"}, 0}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "success: complete", "success: \"\"")
    assert {:error, {:invalid_gate_success, "verify", ""}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "failure: blocked", "failure: retry")
    assert {:error, {:invalid_gate_failure, "verify", "retry"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "command: [sh, -c, \"test -f patch.diff\"]", "command: [sh, 1]")
    assert {:error, {:invalid_gate_command, "verify"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "success: complete", "success: 1")
    assert {:error, {:invalid_gate_success, "verify", 1}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())

    replace_in_file!(
      context.workstream_path,
      "failure: blocked",
      "failure: {repair: implement, max_attempts: 1, other: true}"
    )

    assert {:error, {:invalid_definition_fields, {:gate_repair, "verify"}, [], ["other"]}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "failure: blocked", "failure: {repair: implement}")
    assert {:error, {:invalid_gate_failure, "verify", %{"repair" => "implement"}}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())

    replace_in_file!(
      context.workstream_path,
      "gate:\n  command: [sh, -c, \"test -f patch.diff\"]\n  timeout_ms: 2000\n  success: complete\n  failure: blocked",
      "gate: []"
    )

    assert {:error, {:invalid_gate, "verify"}} = load(context)
  end

  test "normalizes inline candidate validation gates", context do
    replace_in_file!(
      context.workstream_path,
      "gate:\n  command: [sh, -c, \"test -f patch.diff\"]\n  timeout_ms: 2000\n  success: complete\n  failure: blocked",
      "gate:\n  evaluator: candidate_validation\n  required:\n    - check: unit-tests\n      assertion: exit_status\n      equals: 0\n    - check: focused-unittest\n      assertion: test_count\n      equals: 1\n  success: complete\n  failure: blocked"
    )

    assert {:ok, workstream} = load(context)

    assert workstream.stages["verify"].gate == %{
             evaluator: :candidate_validation,
             required: [
               %{check: "unit-tests", assertion: "exit_status", equals: 0},
               %{check: "focused-unittest", assertion: "test_count", equals: 1}
             ],
             success: :complete,
             failure: :blocked
           }
  end

  test "validates candidate validation gate fields and assertion uniqueness", context do
    replace_in_file!(
      context.workstream_path,
      "gate:\n  command: [sh, -c, \"test -f patch.diff\"]\n  timeout_ms: 2000\n  success: complete\n  failure: blocked",
      "gate:\n  evaluator: candidate_validation\n  required:\n    - check: unit-tests\n      assertion: exit_status\n  success: complete\n  failure: blocked"
    )

    assert {:error, {:invalid_validation_gate_required, "verify", {:invalid_required_assertion, 0, :expected_check_assertion_equals}}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())

    replace_in_file!(
      context.workstream_path,
      "gate:\n  command: [sh, -c, \"test -f patch.diff\"]\n  timeout_ms: 2000\n  success: complete\n  failure: blocked",
      "gate:\n  evaluator: candidate_validation\n  required:\n    - {check: unit-tests, assertion: exit_status, equals: 0}\n    - {check: unit-tests, assertion: exit_status, equals: 1}\n  success: complete\n  failure: blocked"
    )

    assert {:error, {:invalid_validation_gate_required, "verify", {:duplicate_required_assertion, "unit-tests", "exit_status"}}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())

    replace_in_file!(
      context.workstream_path,
      "gate:\n  command: [sh, -c, \"test -f patch.diff\"]\n  timeout_ms: 2000\n  success: complete\n  failure: blocked",
      "gate:\n  evaluator: candidate_validation\n  required:\n    - {check: unit-tests, assertion: exit_status, equals: 0, extra: true}\n  success: complete\n  failure: blocked"
    )

    assert {:error, {:invalid_validation_gate_required, "verify", {:invalid_required_assertion, 0, :expected_check_assertion_equals}}} = load(context)
  end

  test "rejects invalid entries, transitions, cycles, and unreachable stages", context do
    replace_in_file!(context.workstream_path, "entry: implement", "entry: missing")
    assert {:error, {:unknown_workstream_entry, "missing"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "entry: implement", "entry: verify")
    assert {:error, {:workstream_entry_must_be_agent, "verify"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "success: complete", "success: missing")
    assert {:error, {:unknown_stage_transition, "verify", "missing"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "success: complete", "success: implement")
    assert {:error, {:unbounded_workstream_cycle, _}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())
    replace_in_file!(context.workstream_path, "next: verify", "next: implement")
    assert {:error, {:agent_must_transition_to_check, "implement", "implement"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())

    replace_in_file!(
      context.workstream_path,
      "      - id: verify",
      "- id: unused\n  type: agent\n  inputs: [request]\n  outputs: []\n  agent: worker\n  prompt: Unused\n  next: verify\n- id: verify"
    )

    assert {:error, {:unreachable_stages, ["unused"]}} = load(context)
  end

  test "repairs must return from a reachable check to its ancestor agent", context do
    replace_in_file!(
      context.workstream_path,
      "      - id: verify",
      "- id: retry-only\n  type: agent\n  inputs: [request]\n  outputs: [retry-output]\n  agent: worker\n  prompt: Retry\n  next: verify\n- id: verify"
    )

    replace_in_file!(
      context.workstream_path,
      "      failure: blocked",
      "failure:\n  repair: retry-only\n  max_attempts: 1"
    )

    assert {:error, {:repair_target_must_be_ancestor, "verify", "retry-only"}} = load(context)
  end

  test "rejects unknown repair targets, check repair targets, and targets leading elsewhere", context do
    replace_in_file!(
      context.workstream_path,
      "      failure: blocked",
      "failure:\n  repair: missing\n  max_attempts: 1"
    )

    assert {:error, {:unknown_repair_stage, "verify", "missing"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())

    replace_in_file!(
      context.workstream_path,
      "      failure: blocked",
      "failure:\n  repair: verify\n  max_attempts: 1"
    )

    assert {:error, {:repair_target_must_be_agent, "verify", "verify"}} = load(context)

    File.write!(context.workstream_path, workstream_yaml())

    replace_in_file!(
      context.workstream_path,
      "      - id: verify",
      "- id: finish\n  type: check\n  inputs: []\n  outputs: []\n  gate:\n    command: [\"true\"]\n    timeout_ms: 1000\n    success: complete\n    failure: blocked\n- id: verify"
    )

    replace_in_file!(
      context.workstream_path,
      "gate:\n  command: [sh, -c, \"test -f patch.diff\"]\n  timeout_ms: 2000\n  success: complete\n  failure: blocked",
      "gate:\n  command: [sh, -c, \"test -f patch.diff\"]\n  timeout_ms: 2000\n  success: complete\n  failure:\n    repair: implement\n    max_attempts: 1"
    )

    replace_in_file!(context.workstream_path, "next: verify", "next: finish")
    assert {:error, {:repair_target_must_lead_to_check, "verify", "implement"}} = load(context)
  end

  test "rejects malformed YAML and missing workstream files", context do
    File.write!(context.workstream_path, "version: [\n")
    assert {:error, {:yaml_parse_error, :workstream, _, _}} = load(context)

    File.write!(context.workstream_path, "- not-a-map\n")
    assert {:error, {:invalid_definition, :workstream, :expected_map}} = load(context)

    assert {:error, {:definition_file_error, :workstream, _, :enoent}} =
             Workstream.load(context.workstream_path <> ".missing", %{"request" => "task"})

    assert {:error, {:invalid_workstream_path, nil}} = Workstream.load(nil)
    assert {:error, {:invalid_workstream_path, nil}} = Workstream.load(nil, %{})
    assert {:error, {:invalid_workstream_inputs, :expected_map}} = Workstream.load(context.workstream_path, nil)
  end

  defp load(context), do: Workstream.load(context.workstream_path, %{"request" => "task"})

  defp add_human_wait!(context, inputs, next, success_line) do
    replace_in_file!(context.workstream_path, success_line, "success: question")

    append_to_file!(
      context.workstream_path,
      "  - id: question\n" <>
        "    type: human_wait\n" <>
        "    inputs: [#{inputs}]\n" <>
        "    outputs: [answer]\n" <>
        "    prompt: Please answer.\n" <>
        "    next: #{next}\n"
    )
  end

  defp workstream_yaml do
    """
    version: 1
    name: local-demo
    inputs: [request]
    agents:
      worker: ../agents/worker.yml
    entry: implement
    stages:
      - id: implement
        type: agent
        inputs: [request]
        outputs: [patch]
        agent: worker
        prompt: Implement the requested change.
        next: verify
      - id: verify
        type: check
        inputs: [patch]
        outputs: []
        gate:
          command: [sh, -c, "test -f patch.diff"]
          timeout_ms: 2000
          success: complete
          failure: blocked
    """
  end

  defp agent_yaml do
    """
    version: 1
    name: Local Worker
    model: gpt-6.1-sol
    reasoning_effort: high
    daybreak: false
    approval_policy: never
    sandbox: workspace-write
    instructions:
      - instructions/implementation.md
    skills:
      - skills/review/SKILL.md
    """
  end

  defp append_to_file!(path, contents), do: File.write!(path, File.read!(path) <> contents)

  defp replace_in_file!(path, expected, replacement) do
    lines = File.read!(path) |> String.split("\n", trim: false)
    expected_lines = fixture_lines(expected) |> Enum.map(&String.trim/1)
    replacement_lines = if replacement == "", do: [], else: fixture_lines(replacement)
    last_start = length(lines) - length(expected_lines)

    start =
      if last_start < 0 do
        nil
      else
        Enum.find(0..last_start, fn index ->
          lines
          |> Enum.slice(index, length(expected_lines))
          |> Enum.map(&String.trim/1) == expected_lines
        end)
      end

    assert is_integer(start), "expected fixture to include lines #{inspect(expected_lines)}"

    indent = leading_indent(Enum.at(lines, start))
    replacement_lines = Enum.map(replacement_lines, &(indent <> &1))

    updated =
      Enum.take(lines, start) ++ replacement_lines ++ Enum.drop(lines, start + length(expected_lines))

    File.write!(path, Enum.join(updated, "\n"))
  end

  defp fixture_lines(text) do
    text
    |> String.split("\n", trim: false)
    |> drop_terminal_empty_line()
  end

  defp drop_terminal_empty_line(lines) do
    if List.last(lines) == "", do: Enum.drop(lines, -1), else: lines
  end

  defp leading_indent(line) do
    String.slice(line, 0, byte_size(line) - byte_size(String.trim_leading(line)))
  end

  defp sha256(text), do: :crypto.hash(:sha256, text) |> Base.encode16(case: :lower)
end
