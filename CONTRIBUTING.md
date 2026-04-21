# Contributing To 4M

## Working Rules
- Keep changes minimal and scoped to the requested problem.
- Separate feature work from refactor work unless the refactor is necessary to land the fix.
- Do not commit runtime output, local browser state, caches, or DB files.

## Local Setup
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r Aurum_Infinity_AI\requirements.txt
python -m pip install -r Aurum_Infinity_AI\requirements-dev.txt
```

## Required Validation
Run before opening a PR:

```powershell
.\scripts\qc.ps1
```

Equivalent commands:

```powershell
python -m compileall Aurum_Infinity_AI Aurum_Data_Fetcher News_fetcher
python -m ruff check Aurum_Infinity_AI Aurum_Data_Fetcher News_fetcher check_db.py check_db2.py
$env:PYTHONPATH = (Resolve-Path .\Aurum_Infinity_AI).Path
python -m pytest Aurum_Infinity_AI\tests -q
```

## Pull Request Expectations
- Explain what changed and why.
- List impact scope.
- List risks and rollback approach when a stable interface changes.
- Mention validation results.
- Keep PRs reviewable; avoid bundling unrelated edits.

## Test Policy
- Any behavior change should come with a test update when the code is testable.
- Bug fixes should prefer regression tests.
- If no test is added, explain why in the PR summary.
