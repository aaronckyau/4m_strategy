from __future__ import annotations

from datetime import datetime
from datetime import timezone
import json
import time
from pathlib import Path

from flask import Flask

from blueprints.admin import admin_bp
from blueprints.admin import routes as admin_routes


def create_app():
    app = Flask(__name__)
    app.secret_key = "test-secret"
    app.register_blueprint(admin_bp)
    return app


class TestAdminHelpers:
    def test_resolve_fetcher_dir_uses_explicit_env_path(self, tmp_path, monkeypatch):
        fetcher_dir = tmp_path / "Aurum_Data_Fetcher"
        fetcher_dir.mkdir()
        (fetcher_dir / "updater.py").write_text("print('ok')\n", encoding="utf-8")

        monkeypatch.setenv("AURUM_DATA_FETCHER_DIR", str(fetcher_dir))

        assert admin_routes._resolve_fetcher_dir() == fetcher_dir.resolve()

    def test_resolve_fetcher_dir_falls_back_to_repo_sibling(self, tmp_path, monkeypatch):
        repo_root = tmp_path / "repo"
        app_root = repo_root / "Aurum_Infinity_AI"
        fetcher_dir = repo_root / "Aurum_Data_Fetcher"
        app_root.mkdir(parents=True)
        fetcher_dir.mkdir()
        (fetcher_dir / "updater.py").write_text("print('ok')\n", encoding="utf-8")

        monkeypatch.delenv("AURUM_DATA_FETCHER_DIR", raising=False)
        monkeypatch.setattr(admin_routes, "APP_ROOT", app_root.resolve())

        assert admin_routes._resolve_fetcher_dir() == fetcher_dir.resolve()

    def test_throttle_check_blocks_second_call_temporarily(self):
        key = f"test-{time.time()}"

        first = admin_routes._throttle_check(key)
        second = admin_routes._throttle_check(key)

        assert first is None
        assert second is not None
        assert second > 0

    def test_format_duration_handles_valid_timestamps(self):
        result = admin_routes._format_duration("2026-04-21T10:00:00Z", "2026-04-21T10:01:05Z")

        assert result == "1m 5s"

    def test_tail_log_file_returns_last_lines(self, tmp_path):
        log_path = tmp_path / "job.log"
        log_path.write_text("1\n2\n3\n4\n", encoding="utf-8")

        tail = admin_routes._tail_log_file(log_path, lines=2)

        assert tail == "3\n4\n"

    def test_resolve_allowed_log_path_rejects_outside_directory(self, tmp_path, monkeypatch):
        monkeypatch.setattr(admin_routes, "UPDATE_JOB_LOG_DIR", tmp_path.resolve())
        inside = tmp_path / "run.log"
        inside.write_text("", encoding="utf-8")
        outside = tmp_path.parent / "outside.log"
        outside.write_text("", encoding="utf-8")

        assert admin_routes._resolve_allowed_log_path(str(inside)) == inside.resolve()
        assert admin_routes._resolve_allowed_log_path(str(outside)) is None

    def test_format_frequency_label_and_freshness(self):
        assert admin_routes._format_frequency_label("daily", 1440) == "每日 / 24h"
        assert admin_routes._format_frequency_label("manual", 60) == "手動"

        state, label = admin_routes._compute_dataset_freshness(None, 60, "idle")
        assert state == "failed"
        assert "沒有成功紀錄" in label

    def test_normalize_dataset_run_marks_partial_failure_as_warning(self):
        row = {
            "dataset_key": "ratios",
            "label": "TTM 比率",
            "started_at": "2026-04-21T10:00:00Z",
            "finished_at": "2026-04-21T10:02:00Z",
            "duration_seconds": None,
            "status": "done",
            "failed_items": 33,
            "records_written": 100,
            "trigger_source": "cron",
            "last_success_at": "2026-04-21T10:02:00Z",
            "freshness_sla_minutes": 1440,
            "running_timeout_minutes": 60,
            "frequency_type": "daily",
            "error_summary": None,
        }

        item = admin_routes._normalize_dataset_run(row)

        assert item["display_status"] == "done"
        assert item["freshness_state"] == "warning"
        assert "33" in item["freshness_label"]

    def test_collect_news_cache_health_reports_fresh_cache(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "futunn_cache.json"
        cache_path.write_text(
            json.dumps(
                {
                    "fetched_at": "2026-04-21 17:09 HKT",
                    "articles": [{"id": "a1"}],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        now = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return now if tz else now.replace(tzinfo=None)

        monkeypatch.setattr(admin_routes, "resolve_news_cache_path", lambda: cache_path)
        monkeypatch.setattr(admin_routes, "datetime", FixedDateTime)

        health = admin_routes._collect_news_cache_health()

        assert health["key"] == "news_cache"
        assert health["status"] == "ok"
        assert health["value"] == "1 篇"


class TestAdminRoutes:
    def test_admin_root_redirects_based_on_session(self, monkeypatch):
        app = create_app()
        client = app.test_client()

        monkeypatch.setattr(admin_routes, "verify_admin_session", lambda token: False)
        response = client.get("/admin/")
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/admin/login")

        monkeypatch.setattr(admin_routes, "verify_admin_session", lambda token: True)
        response = client.get("/admin/")
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/admin/dashboard")

    def test_admin_login_get_renders_template(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(
            admin_routes,
            "render_template",
            lambda template, **context: json.dumps({"template": template, **context}, ensure_ascii=False),
        )
        client = app.test_client()

        response = client.get("/admin/login")

        payload = json.loads(response.get_data(as_text=True))
        assert payload["template"] == "admin/login.html"
        assert payload["error"] is None

    def test_admin_login_post_success_sets_cookie(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(admin_routes, "verify_admin_password", lambda password: True)
        monkeypatch.setattr(admin_routes, "create_admin_session", lambda: "token-123")
        client = app.test_client()

        response = client.post("/admin/login", data={"password": "secret"})

        assert response.status_code == 302
        cookie_header = response.headers.get("Set-Cookie", "")
        assert "admin_token=token-123" in cookie_header

    def test_admin_login_post_failure_shows_error(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(admin_routes, "verify_admin_password", lambda password: False)
        monkeypatch.setattr(
            admin_routes,
            "render_template",
            lambda template, **context: json.dumps({"template": template, **context}, ensure_ascii=False),
        )
        client = app.test_client()

        response = client.post("/admin/login", data={"password": "wrong"})

        payload = json.loads(response.get_data(as_text=True))
        assert payload["template"] == "admin/login.html"
        assert "密碼錯誤" in payload["error"]

    def test_admin_logout_deletes_cookie_and_session(self, monkeypatch):
        app = create_app()
        deleted = []
        monkeypatch.setattr(admin_routes, "delete_admin_session", lambda token: deleted.append(token))
        client = app.test_client()
        client.set_cookie("admin_token", "abc123")

        response = client.get("/admin/logout")

        assert response.status_code == 302
        assert deleted == ["abc123"]
        assert "admin_token=;" in response.headers.get("Set-Cookie", "")

    def test_update_log_stream_redacts_disallowed_log_path(self, monkeypatch):
        app = create_app()

        class FakeQuery:
            def fetchone(self):
                return {
                    "id": 1,
                    "dataset_key": "ohlc",
                    "status": "failed",
                    "started_at": "2026-04-21T10:00:00Z",
                    "finished_at": "2026-04-21T10:05:00Z",
                    "log_path": "C:/secret/outside.log",
                    "error_summary": "boom",
                }

        class FakeConn:
            def execute(self, query, params=()):
                return FakeQuery()

            def close(self):
                return None

        monkeypatch.setattr(admin_routes, "get_db", lambda: FakeConn())
        monkeypatch.setattr(admin_routes, "_reconcile_stale_runs", lambda conn: None)
        monkeypatch.setattr(admin_routes, "_db_last_write_age_seconds", lambda conn, job_name: 12)
        monkeypatch.setattr(admin_routes, "_resolve_allowed_log_path", lambda raw: None)
        monkeypatch.setattr(admin_routes, "_log_mtime", lambda path: (_ for _ in ()).throw(AssertionError(f"unexpected path probe: {path}")) if path else None)
        with app.test_request_context("/admin/update-log/log/ohlc"):
            response = admin_routes.update_log_stream.__wrapped__("ohlc")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["log_path"] is None
        assert payload["content"] == ""
        assert payload["log_mtime_iso"] is None

    def test_update_run_status_does_not_probe_disallowed_log_path(self, monkeypatch):
        app = create_app()

        class FakeQuery:
            def fetchall(self):
                return [
                    {
                        "id": 1,
                        "dataset_key": "ohlc",
                        "status": "running",
                        "started_at": "2026-04-21T10:00:00Z",
                        "finished_at": None,
                        "total_items": 10,
                        "success_items": 2,
                        "failed_items": 0,
                        "skipped_items": 0,
                        "records_written": 20,
                        "error_summary": None,
                        "log_path": "C:/secret/outside.log",
                        "trigger_source": "admin",
                        "label": "OHLC 日線",
                    }
                ]

        class FakeConn:
            def execute(self, query, params=()):
                return FakeQuery()

            def close(self):
                return None

        monkeypatch.setattr(admin_routes, "get_db", lambda: FakeConn())
        monkeypatch.setattr(admin_routes, "_reconcile_stale_runs", lambda conn: None)
        monkeypatch.setattr(admin_routes, "_resolve_allowed_log_path", lambda raw: None)
        monkeypatch.setattr(admin_routes, "_log_mtime", lambda path: (_ for _ in ()).throw(AssertionError(f"unexpected path probe: {path}")) if path else None)
        monkeypatch.setattr(admin_routes, "_db_last_write_age_seconds", lambda conn, job_name: 30)

        with app.test_request_context("/admin/update-log/run-status"):
            response = admin_routes.update_run_status.__wrapped__()

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["success"] is True
        assert payload["is_any_running"] is True
        assert payload["active"][0]["log_age_seconds"] is None
        assert payload["active"][0]["progress_hint"] is None

    def test_run_update_job_does_not_return_log_path(self, monkeypatch):
        app = create_app()

        class FakeQuery:
            def fetchone(self):
                return {"label": "OHLC æ—¥ç·š", "manual_run_allowed": 1}

        class FakeConn:
            def execute(self, query, params=()):
                return FakeQuery()

            def close(self):
                return None

        monkeypatch.setattr(admin_routes, "get_db", lambda: FakeConn())
        monkeypatch.setattr(admin_routes, "_throttle_check", lambda key: None)
        monkeypatch.setattr(admin_routes, "_dataset_is_running", lambda conn, job_names: False)
        monkeypatch.setattr(admin_routes, "_job_is_running", lambda conn, job_names: False)
        monkeypatch.setattr(admin_routes, "_launch_updater", lambda args: (Path("C:/secret/job.log"), 4321))

        with app.test_request_context("/admin/update-log/run/ohlc", method="POST"):
            response = admin_routes.run_update_job.__wrapped__("ohlc")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["success"] is True
        assert payload["job_name"] == "ohlc"
        assert "log_path" not in payload

    def test_run_update_job_accepts_analyst_forecast(self, monkeypatch):
        app = create_app()

        class FakeQuery:
            def fetchone(self):
                return {"label": "Analyst Forecast", "manual_run_allowed": 1}

        class FakeConn:
            def execute(self, query, params=()):
                return FakeQuery()

            def close(self):
                return None

        launched = []
        monkeypatch.setattr(admin_routes, "get_db", lambda: FakeConn())
        monkeypatch.setattr(admin_routes, "_throttle_check", lambda key: None)
        monkeypatch.setattr(admin_routes, "_dataset_is_running", lambda conn, job_names: False)
        monkeypatch.setattr(admin_routes, "_job_is_running", lambda conn, job_names: False)
        monkeypatch.setattr(
            admin_routes,
            "_launch_updater",
            lambda args: launched.append(args) or (Path("C:/secret/analyst.log"), 7654),
        )

        with app.test_request_context("/admin/update-log/run/analyst_forecast", method="POST"):
            response = admin_routes.run_update_job.__wrapped__("analyst_forecast")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["success"] is True
        assert payload["job_name"] == "analyst_forecast"
        assert launched == [["--job", "analyst_forecast", "--triggered-by", "admin"]]

    def test_analyst_forecast_has_liveness_mapping(self):
        assert admin_routes._DATASET_LIVENESS_TABLES["analyst_forecast"] == (
            "analyst_price_targets",
            "fetched_at",
        )

    def test_run_update_all_does_not_return_log_path(self, monkeypatch):
        app = create_app()

        class FakeConn:
            def close(self):
                return None

        monkeypatch.setattr(admin_routes, "get_db", lambda: FakeConn())
        monkeypatch.setattr(admin_routes, "_throttle_check", lambda key: None)
        monkeypatch.setattr(admin_routes, "_job_is_running", lambda conn, job_names: False)
        monkeypatch.setattr(admin_routes, "_launch_updater", lambda args: (Path("C:/secret/all.log"), 999))

        with app.test_request_context("/admin/update-log/run-all", method="POST"):
            response = admin_routes.run_update_all.__wrapped__()

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["success"] is True
        assert "run_group_id" in payload
        assert "log_path" not in payload
