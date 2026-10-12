# Combined first-alpha source qualification

Exact merged source: `818c59a1a9f93479cff2048ec343c3c6745e33cb`.

The existing `factory/deploy/cloud_io.py pack` helper produced the source archive
and `RELEASE.json`; its recorded SHA was verified against the actual local bytes.
The package was extracted and copied into a local isolated toolchain container:
image `sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`,
Linux ARM64, two CPUs, 4 GiB, network disabled and no supplied credentials.

- Formatting, warnings-as-errors compilation and public specs passed.
- 172 focused review/repair/context/producer/transfer/publication/validation,
  completion/input and contained-client tests passed, zero failures, seed 240.
- 55 broker/transport tests passed, including pinned preparation and held wrapper.
- The parsed workstream command and compiled ReviewWorkspace API produced the
  command pins in [pins.json](pins.json). No policy source entry was removed.
- Container `dev240-alpha-combined` exited zero and was removed; its heavy build
  slot was released. No live worker, model, GitHub review or cloud operation ran.

Receipts: [package](package.json), [source tests](qualification.log),
[broker tests](broker-tests.log), [pin command](pins-command.exs).

This is source-package qualification. The source archive is ready for a scoped
release decision, not a proof of live alpha acceptance. Native VM build, actual
coordinator Docker validation, worker effective model/subscription readiness and
the real independent-review/same-PR-fix demo remain to be proven. Workspaces are
retained and automatic cleanup remains disabled for the first alpha.
