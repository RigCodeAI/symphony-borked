# Durable Linear questions and replies (DEV-234)

The native delegation coordinator owns questions and replies. It records each question
against its run, stage attempt and input artifact identities before publishing a native
`elicitation`. The agent can continue independent work after `factory_question`; it calls
`factory_wait` when the answer is needed. A suspended agent releases capacity only after
its app-server operation has confirmed termination. Unknown termination stays reconciling.

## Replies

Answer in the native Linear agent conversation, using the command printed beside the
question:

```text
answer <question-id>: blue
```

An untargeted reply may answer the sole outstanding clarification. With several questions,
name one explicitly; resolving one preserves the others. Human prompt text is bounded to
16 KiB, redacted before persistence. Normal status logs use identifiers rather than reply bodies. Agent-authored
thoughts, responses, errors and elicitations cannot enter the human-prompt path.

Delivery IDs deduplicate HTTP receipts. Activity IDs deduplicate logical replies across
separate deliveries. A reply is committed to the run before the intake receipt is marked
handled; restart replays that commit safely. Replies arriving during a turn remain durable
until the safe boundary. Before a resumed stage dispatches, delegation and session ownership
are checked again. Stopped tasks retain their tombstone and cannot be restarted by replies.

On load, runs saved before this feature receive empty `questions`, `inbox` and
`activity_ids` fields and a nil `continuation` only where those fields are missing.
The store preserves all existing values, identities, pinned definitions and policy,
attempts, operations, artifacts and waits. It does not recreate a run or reset its
policy pin. A saved policy that differs from the service still blocks execution.

## Continuation and native input

A wait closes the old app-server connection. Its pending JSON-RPC request ID is never reused
on a new connection. Continuation explicitly creates a fresh conversation with the pinned
stage instructions and inputs, prior thread identity, saved question/reply or queued
messages, and the same dedicated workspace. This is recorded as context reconstruction;
it does not claim automatic resumption of an interrupted native request.

Native `item/tool/requestUserInput` requests become persistent clarifications. Execution,
file-change and permission approvals remain explicit unsupported-input blockers; answering
an ordinary question cannot grant permissions, enable Daybreak, change billing or authorize
GitHub merging.

## Approval stages

A trusted workstream can mark a `human_wait` stage with `approval: true`. It binds the wait
to a digest of all its current artifact identities. Its printed reply is:

```text
approve <wait-id> <artifact-digest>
```

An ordinary clarification, a different wait ID or an old artifact digest cannot unlock
that stage. Changed artifact identities invalidate an existing decision. Agent tools may
create clarifications only; they cannot invent approval gates or satisfy them themselves.

## Checks

From `elixir/`, using Elixir 1.19.5 / OTP 28:

```sh
mise exec -- mix compile --warnings-as-errors
mise exec -- mix test test/symphony_elixir/linear_delegation_test.exs \
  test/symphony_elixir/linear_webhook_test.exs \
  test/symphony_elixir/linear_agent_client_test.exs \
  test/symphony_elixir/workstream_run_test.exs \
  test/symphony_elixir/app_server_test.exs \
  test/symphony_elixir/workstream_runner_test.exs
```

Use a dedicated disposable Rig clone with its push URL disabled for a live test. Delegate
one harmless test issue, observe a question and independent work, restart the coordinator
while waiting, answer in its native conversation, and replay that reply. Expect one
continuation, a retained run/stage identity and no held execution slot during the wait.
Inspect the saved transcript/receipts and exact worker termination proof before cleanup.
Publication stays disabled. Live evidence is recorded separately from controlled tests.
