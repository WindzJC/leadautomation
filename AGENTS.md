# AGENTS.md

Permanent rules for the `lead finder automation` repo.

## Product and scope
- Preserve the current Python CLI pipeline as the source-of-truth engine.
- Do not rebuild the architecture into a new backend, queue engine, or SaaS app.
- Never silently change product scope. If scope shifts, make it explicit in code, docs, and stats.
- Keep `strict_full` as the gold-standard verifier.
- Keep any scouting or relaxed review modes separate from `strict_full` outputs and stats.

## Validation and output safety
- Do not loosen `strict_full` proof rules.
- Do not change existing internal strict audit/debug CSV schemas without an explicit task asking for it.
- Keep lead-facing exports aligned to the current business requirement and preserve their documented schema.
- Preserve `SourceURL` auditability for any exported lead row.
- Preserve existing debug and stats artifacts unless a change explicitly adds a new one.
- Keep the validator as the source of truth for strict pass/fail decisions.

## Engineering approach
- Prefer boring, reliable implementation over cleverness.
- Keep changes incremental, local, and reviewable.
- Add or update tests for every behavior change that can be tested locally.
- Do not add LLM dependencies.
- Do not add UI changes unless the task explicitly asks for dashboard/browser work.

## Agent-style extensions
- If adding an agent-like mode, layer it on top of the existing staged pipeline.
- Reuse existing harvest, validate, dedupe, and orchestration flows where possible.
- Keep relaxed/scouting outputs isolated from strict verified outputs.
- Do not let scouting modes pollute strict exports, strict counters, or strict audit artifacts.
