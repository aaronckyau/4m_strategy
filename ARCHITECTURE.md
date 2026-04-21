# 4M Architecture

## Top-Level Structure
- `Aurum_Infinity_AI/`
  - Primary Flask application.
  - Owns routes, templates, static assets, admin pages, translation handling, and site-facing services.
- `Aurum_Data_Fetcher/`
  - Batch and CLI data ingestion pipeline.
  - Owns market data fetch, financial statements, metric computation, and DB population jobs.
- `News_fetcher/`
  - External news fetch and cache publication pipeline.
  - Owns scrape, AI rewrite, schema validation, and cache publication.
- `News_features/`
  - Published article artifacts and manifest outputs.
- `data/`
  - Shared generated data snapshots. Treat as data assets, not executable logic.

## Execution Model
- Web requests enter through `Aurum_Infinity_AI/app.py`, then route through blueprints.
- Blueprints should remain transport-oriented:
  - parse request
  - call service or helper
  - return template or JSON response
- Business composition belongs in `Aurum_Infinity_AI/services/`.
- Data collection jobs enter via CLI scripts in `Aurum_Data_Fetcher/main.py` or `News_fetcher/*.py`.

## Dependency Direction
- Allowed:
  - `blueprints -> services -> database/helpers`
  - `CLI entrypoint -> fetchers/jobs/helpers`
  - `templates/static -> blueprint endpoints`
- Avoid:
  - `services -> blueprints`
  - `fetchers -> Flask app modules`
  - `templates/js -> direct business logic duplication`
  - cross-imports between `Aurum_Data_Fetcher/` and `News_fetcher/` unless there is a clear shared contract

## Stable Interfaces
- Web routes and API responses in `Aurum_Infinity_AI/blueprints/`
- Cache schema under `News_fetcher/schema/`
- DB write contracts in `Aurum_Data_Fetcher/db.py` and `Aurum_Infinity_AI/database.py`
- Stock code and translation loaders that other modules rely on

## Risk Areas
- Route registration order in `Aurum_Infinity_AI/app.py` is sensitive because catch-all stock routes exist.
- News cache generation depends on external HTML structure and AI output validation.
- Fetch pipelines write to persistent storage; schema drift or ticker normalization bugs have wide blast radius.
- Large generated assets can create noisy diffs and mask real code review issues.

## QC Strategy
- Use static analysis and syntax validation across all Python directories.
- Keep automated tests focused on stable normalization and service behavior first.
- Add regression tests whenever:
  - a route bug is fixed
  - ticker parsing changes
  - schema validation logic changes
  - cache publication rules change

## Change Checklist
- Does the change stay within the correct module boundary?
- Does it alter a stable interface?
- Does it touch generated data by accident?
- Does it need a regression test?
- Does it require a user-visible rollback note?
