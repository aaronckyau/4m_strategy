# 4M Project Guardrails

## Scope
- This file is the single source of truth for AI agents working in `C:\Users\aaron\Documents\4M`.
- Apply these rules across `Aurum_Infinity_AI`, `Aurum_Data_Fetcher`, `News_fetcher`, `News_features`, and root utility scripts.

## Product Map
- `Aurum_Infinity_AI/`: Flask web app, HTML templates, static assets, blueprints, services, admin tools.
- `Aurum_Data_Fetcher/`: market data ingestion CLI, fetchers, DB writes, scheduled scripts.
- `News_fetcher/`: standalone news cache generation and schema validation pipeline.
- `News_features/`: generated article output and manifest data.
- `data/`, `logs/`, `runtime/`: generated or runtime artifacts, not feature logic.

## Architecture Rules
- Keep web request orchestration inside Flask blueprints; move reusable business logic into `services/`.
- Keep scraping, fetching, and DB write logic inside the relevant fetcher or service layer, not in templates.
- Do not import Flask app objects into `Aurum_Data_Fetcher/` or `News_fetcher/`.
- Do not let `shared/generated data` directories become sources of truth for application logic.
- Prefer extending an existing module in the correct layer over adding duplicate helper files.
- Do not mix feature work with broad refactors unless the refactor is required to ship the change safely.

## Change Boundaries
- UI changes:
  - Allowed in `Aurum_Infinity_AI/templates/`, `Aurum_Infinity_AI/static/`, and the matching blueprint/service pair.
  - Do not place business rules directly in JS, templates, or CSS.
- Backend feature changes:
  - Prefer `blueprints/` for routing, `services/` for composition, `database.py` or module-local DB helpers for persistence.
- Data pipeline changes:
  - Keep CLI entrypoints thin.
  - Put fetch logic in `fetchers/` or dedicated module functions.
- Prompt or schema changes:
  - Update both the loader/validator code and the schema or prompt asset together.

## Forbidden Moves
- Do not commit secrets, `.env` files, DB files, or runtime browser state.
- Do not rewrite large generated JSON or CSV assets unless the task explicitly requires regenerating data.
- Do not change public route names, cache file formats, or DB schema without documenting impact in the change summary.
- Do not fix unrelated lint noise in files outside the task scope.
- Do not introduce placeholder code, `TODO`, or commented-out implementation blocks.

## Quality Gates
- Every behavior change must include at least one of:
  - updated automated tests
  - a new regression test
  - a documented reason why tests are not practical
- Before finishing, run the local QC script or equivalent commands:
  - `.\scripts\qc.ps1`
- Minimum required checks for code changes:
  - Python syntax compilation
  - `ruff` static checks
  - `pytest` for `Aurum_Infinity_AI/tests`

## Definition Of Done
- The requested behavior is implemented with minimal blast radius.
- Existing tests still pass, or failures are explained explicitly.
- New rules, schema updates, or operational impacts are documented in the change summary.
- No generated runtime artifacts are accidentally staged.

## Review Output Format
- Summarize:
  - changed files
  - impact scope
  - risks
  - validation run and result
- If blocked, state the root cause instead of only reporting symptoms.
