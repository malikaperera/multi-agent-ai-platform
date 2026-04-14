# Forge Workflow

Forge is the system build agent. It should stage, document, and validate work before anything becomes part of the live system.

## Current Rule

Forge-created outputs belong in the managed artifact workspace first:

```text
data/forge_artifacts/
  forge_task_<task_id>_<slug>/
    plan/
    files/
    reports/
    validation/
    handoff/
    manifest.json
```

Markdown-only reading artifacts can be created there without approval. Code, configuration, scripts, Docker changes, source documentation, and live system behavior changes still require approval.

## Promotion Flow

1. Forge stages artifacts under `data/forge_artifacts/`.
2. Roderick records the approval request or dashboard-visible workflow item.
3. Forge creates a bounded plan that names the files and risks.
4. The user approves the plan.
5. Forge applies the approved change.
6. Sentinel runs the sanity gate.
7. The user approves promotion after Sentinel passes.
8. Git commit/push and Docker refresh can happen only after the previous gates are complete.

## Sentinel Gate

Sentinel should run the relevant checks before a change is treated as promotable:

```text
python -m compileall apps shared
JSON/config sanity checks
secret leakage checks
dangerous shell pattern checks
API health check
dashboard build when dashboard files changed
container startup/log smoke check when services changed
```

## Git Status

This workspace should not pretend Git promotion exists until Git metadata and a remote policy are configured. If a `.git` directory is absent, the dashboard should show Git promotion as `not_configured`.

When Git is configured, the recommended professional flow is:

```text
Forge artifact -> approved plan -> applied patch -> Sentinel gate -> user approval -> commit -> push -> deployment refresh
```

Forge should not push, pull, or overwrite local work without explicit approval.
