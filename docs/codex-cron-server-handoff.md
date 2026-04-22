# Codex Handoff: Contabo VPS Cron Job Environment

This document is for another Codex session that will help create or update cron jobs for the `4m_strategy` project on the Contabo VPS.

## Goal

Create cron jobs safely for the existing production server without disrupting the live web app or other services.

The server is not a blank machine. Treat it as production.

## Server Access

- Provider: Contabo
- Host: `161.97.167.144`
- SSH user: `root`
- SSH port: `22`
- Known hostname after login: `vmi3199380`
- SSH command from Aaron's Windows machine:

```powershell
ssh -i $HOME\.ssh\id_ed25519 root@161.97.167.144
```

Do not assume password SSH is available.

## Main Repository

- Repo name: `4m_strategy`
- GitHub remote on VPS: `git@github.com:aaronckyau/4m_strategy.git`
- VPS repo path:

```bash
/root/apps/4m_strategy
```

- Production branch: `main`
- Normal repo health check:

```bash
cd /root/apps/4m_strategy
git status --short --branch
git rev-parse HEAD
git log -1 --oneline
```

The repo should normally show:

```text
## main...origin/main
```

If the repo is dirty, stop and inspect before changing cron jobs.

## Web App

- App name: Aurum Infinity AI
- App directory:

```bash
/root/apps/4m_strategy/Aurum_Infinity_AI
```

- Public URL:

```text
https://ai.4mstrategy.com
```

- Docker compose must be run from:

```bash
cd /root/apps/4m_strategy/Aurum_Infinity_AI
```

- Container service:

```text
aurum-web
```

- Host binding:

```text
127.0.0.1:5000 -> container:5000
```

- Nginx reverse proxies `ai.4mstrategy.com` to `127.0.0.1:5000`.

Do not restart all Docker services. Only operate on this app's compose project if needed.

## Existing Other Services

Do not affect these services:

- `hibor_chart`, expected around port `5010`
- `insider`, expected around port `5030`

Avoid server-wide restarts such as:

```bash
systemctl restart docker
systemctl restart nginx
```

Only use those if Aaron explicitly approves.

## Database

Production uses SQLite, not PostgreSQL.

- Host DB path:

```bash
/root/apps/4m_strategy/Aurum_Infinity_AI/runtime/aurum.db
```

- Container DB path:

```bash
/runtime/aurum.db
```

Important:

- Never overwrite `runtime/aurum.db`.
- Never replace it with an empty DB.
- `/health` can pass even when stock pages fail if the wrong DB is used.
- Stock page smoke tests are required after changes.

## Data Fetcher

The Data Fetcher is outside Docker and uses the repo-level virtual environment:

```bash
/root/apps/4m_strategy/.venv
```

The Data Fetcher and Web App share the same SQLite database.

Before writing cron jobs for data refreshes, confirm:

- The script path exists.
- The script uses the correct `DB_PATH`.
- The script can run from the intended working directory.
- The script does not create a new empty DB in a different path.
- Concurrent writes will not conflict with the live web app.

Preferred cron style:

```cron
SHELL=/bin/bash
PATH=/root/apps/4m_strategy/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Example only. Verify script path and args before enabling.
# 5 6 * * * cd /root/apps/4m_strategy && /root/apps/4m_strategy/.venv/bin/python path/to/script.py >> /root/apps/4m_strategy/logs/cron/example.log 2>&1
```

## News Fetcher / Cache Data

Known data directories:

```bash
/root/apps/4m_strategy/News_fetcher/data
/root/apps/4m_strategy/News_features
```

`News_fetcher/data` has been mounted into the Docker container previously for app access.

If a cron job updates files under `News_fetcher/data`, check whether the web app reads them live through a bind mount or only through built image contents. If in doubt, inspect:

```bash
cd /root/apps/4m_strategy/Aurum_Infinity_AI
docker compose config
```

Known deployment issue to keep in mind:

- Feature article detail pages may require `News_features` to be visible inside the container.
- If routes under `/futunn/features/...` return `404` even though files exist on host, inspect Docker volume mounts.

## Deployment Flow

If a code change is needed before cron work, use this safe deployment flow:

```bash
cd /root/apps/4m_strategy
git fetch origin main
git status --short --branch
git pull --ff-only origin main

cd /root/apps/4m_strategy/Aurum_Infinity_AI
docker compose build --no-cache
docker compose up -d
docker compose ps
docker compose logs --tail=120 aurum-web
```

Do not deploy if:

- GitHub Actions failed.
- `git status` shows unexpected local changes.
- The repo is not on `main`.
- The DB path is unclear.

## Smoke Tests

After cron changes or deployment, check:

```bash
curl -s http://127.0.0.1:5000/health
curl -I https://ai.4mstrategy.com/health
curl -I https://ai.4mstrategy.com/AAPL
curl -I https://ai.4mstrategy.com/0700.HK
curl -I https://ai.4mstrategy.com/news-radar
curl -I https://ai.4mstrategy.com/futunn
curl -I https://ai.4mstrategy.com/admin/login
curl -I https://ai.4mstrategy.com/admin/update-log
```

Expected:

- `/health`: `200 OK` with `{"db":"ok","gemini":"ok","status":"ok"}`
- `/AAPL`: `200 OK`
- `/0700.HK`: `200 OK`
- `/news-radar`: `200 OK`
- `/futunn`: `200 OK`
- `/admin/login`: `200 OK`
- `/admin/update-log`: `302` to `/admin/login` when not logged in

## Cron Job Safety Rules

Before installing or editing a cron job:

1. Show the exact command that will run.
2. Show the working directory.
3. Show the Python interpreter path.
4. Show where logs will be written.
5. Confirm whether the job writes to SQLite, JSON cache, or both.
6. Run the command manually once.
7. Check logs and smoke tests.
8. Only then install the crontab entry.

Recommended log directory:

```bash
/root/apps/4m_strategy/logs/cron
```

Create it if missing:

```bash
mkdir -p /root/apps/4m_strategy/logs/cron
```

Recommended cron inspection commands:

```bash
crontab -l
systemctl status cron --no-pager
journalctl -u cron --since "1 hour ago" --no-pager
```

On Ubuntu, cron service may be named `cron`.

## What Not To Commit

These local files have previously been treated as not suitable for GitHub unless Aaron explicitly decides otherwise:

```text
.claude/settings.local.json
runtime/
check_db.py
check_db2.py
News_fetcher/run_refresh_and_upload.ps1
```

`Aurum_Data_Fetcher/fetch_stocktwits.py` is optional:

- It is not required for Flask app startup.
- Not pushing it should not crash the website.
- It may be needed if Aaron wants VPS-side Stocktwits refresh jobs.

## Current Production Baseline

As of the last verified deployment:

- VPS repo was on `main`.
- Web app container was `healthy`.
- `/health`, `/AAPL`, `/0700.HK`, `/news-radar`, `/futunn`, and `/admin/login` returned expected responses.
- `/admin/update-log` redirected to `/admin/login` when unauthenticated, which is expected.

Before doing cron work, verify the current commit again because production may have moved since this document was created.

