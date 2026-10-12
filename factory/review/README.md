# Pinned review rubric

`rubric.md` is an unchanged copy of OpenAI Codex's review rubric, distributed under
the upstream [Apache 2.0 license](https://github.com/openai/codex/blob/5fe4fc8f7cd16688b5c661d8cdb76e2a340b046d/LICENSE).
Its repository-rule attribution and exact JSON output contract are preserved.
The applicable OpenAI copyright notice is retained in [NOTICE](NOTICE).

| Identity | Value |
| --- | --- |
| Upstream source | [OpenAI Codex rubric](https://github.com/openai/codex/blob/5fe4fc8f7cd16688b5c661d8cdb76e2a340b046d/codex-rs/prompts/templates/review/rubric.md) |
| Upstream commit | `5fe4fc8f7cd16688b5c661d8cdb76e2a340b046d` |
| Git blob | `85e89cb7eeadb97fa9e4868d2b9977c87225506d` |
| SHA-256 | `ec60e7f36a1d1c2679ce095c0205ecc56f7dd8fb57707a13ef362072390f219f` |

`SymphonyElixir.ReviewResult` embeds the resource at compilation and checks both
digests. `rubric/0` returns its text and pin metadata, so packaging does not depend
on a mutable runtime file. Updating the rubric requires deliberately updating
the source, pins, schema validation and tests together. Do not fetch moving upstream
`main` during a review.

This resource is not wired into an agent definition or live workstream. The
qualified `DefaultReview` and `DaybreakReview` definitions remain unchanged.
See the [result-validation contract](../../docs/review-results.md).
