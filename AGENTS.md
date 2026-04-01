# AGENTS.md

- Keep responses short and direct.
- Prefer narrow, reviewable diffs over broad refactors.
- Do not redesign working architecture unless explicitly asked.
- Preserve `strict_full` behavior unless explicitly asked to change it.
- Preserve export schemas and `SourceURL` auditability.
- Do not commit generated artifacts, logs, CSVs, JSONs, or debug outputs.
- Prefer dashboard-based operation when possible.
- For `email_only`, optimize for fast name + visible public email + `SourceURL` collection without loosening other profiles.
- Run targeted tests first, then broader tests if needed.
- When changing runtime/profile behavior, verify existing profiles are not unintentionally affected.
- Summaries should be concise:
  - changed files
  - what changed
  - tests run
  - anything incomplete
