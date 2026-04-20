import json
from datetime import date, timedelta

from flask import jsonify, render_template, request

from blueprints.trending import trending_bp
from blueprints.stock.routes import get_current_lang
from database import get_db
from translations import get_translations


def load_json(value, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def parse_iso_date(value):
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def build_change_payload(current_row, previous_row):
    if not current_row:
        return None

    current_count = current_row.get("watchlist_count")
    previous_count = previous_row.get("watchlist_count") if previous_row else None
    payload = {
        "current_date": current_row.get("snapshot_date"),
        "current_watchlist_count": current_count,
        "compare_date": previous_row.get("snapshot_date") if previous_row else None,
        "compare_watchlist_count": previous_count,
        "delta": None,
        "percent_change": None,
        "has_data": previous_count is not None and current_count is not None,
    }

    if payload["has_data"]:
        payload["delta"] = current_count - previous_count
        if previous_count:
            payload["percent_change"] = ((current_count - previous_count) / previous_count) * 100

    return payload


def get_history_changes(db, symbol, latest_snapshot_date=None, latest_watchlist_count=None):
    latest_row = None
    if latest_snapshot_date:
        latest_row = {
            "snapshot_date": latest_snapshot_date,
            "watchlist_count": latest_watchlist_count,
        }
    else:
        latest_row = db.execute(
            """
            SELECT snapshot_date, watchlist_count
            FROM stocktwits_daily_snapshots
            WHERE symbol = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()

    if not latest_row:
        return {}

    latest_date = parse_iso_date(latest_row["snapshot_date"])
    if not latest_date:
        return {}

    changes = {}
    for days in (1, 7, 10):
        target_date = (latest_date - timedelta(days=days)).isoformat()
        previous_row = db.execute(
            """
            SELECT snapshot_date, watchlist_count
            FROM stocktwits_daily_snapshots
            WHERE symbol = ?
              AND snapshot_date <= ?
            ORDER BY snapshot_date DESC
            LIMIT 1
            """,
            (symbol, target_date),
        ).fetchone()
        changes[f"{days}d"] = build_change_payload(latest_row, previous_row)

    return changes


@trending_bp.route("/trending")
def trending_home():
    lang = get_current_lang()
    t = get_translations(lang)
    return render_template("trending/index.html", lang=lang, t=t)


@trending_bp.route("/api/trending")
def trending_symbols():
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT
                s.symbol,
                COALESCE(s.stocktwits_title, s.company_name) AS title,
                COALESCE(s.stocktwits_exchange, s.exchange) AS exchange,
                s.logo_url,
                ds.watchlist_count,
                ds.bullish_count,
                ds.bearish_count,
                ds.unlabeled_count,
                ds.snapshot_date,
                ds.captured_at
            FROM stocktwits_symbols s
            JOIN stocktwits_daily_snapshots ds
                ON ds.symbol = s.symbol
            JOIN (
                SELECT symbol, MAX(snapshot_date) AS latest_date
                FROM stocktwits_daily_snapshots
                GROUP BY symbol
            ) latest
                ON latest.symbol = ds.symbol
               AND latest.latest_date = ds.snapshot_date
            WHERE s.is_sp500 = 1
            ORDER BY ds.watchlist_count DESC, s.symbol ASC
            LIMIT 30
            """
        ).fetchall()

        symbols = []
        for row in rows:
            enriched = dict(row)
            enriched["history"] = get_history_changes(
                db,
                row["symbol"],
                latest_snapshot_date=row["snapshot_date"],
                latest_watchlist_count=row["watchlist_count"],
            )
            symbols.append(enriched)

        meta = db.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM stocktwits_symbols WHERE is_sp500 = 1) AS total_symbols,
                (SELECT COUNT(DISTINCT symbol) FROM stocktwits_daily_snapshots) AS synced_symbols,
                (SELECT MAX(snapshot_date) FROM stocktwits_daily_snapshots) AS latest_snapshot_date
            """
        ).fetchone()
        return jsonify({"symbols": symbols, "meta": dict(meta) if meta else {}})
    finally:
        db.close()


@trending_bp.route("/api/trending/search")
def search_symbols():
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"error": "missing_query", "detail": "Query parameter 'q' is required."}), 400

    pattern = f"%{query.upper()}%"
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT
                symbol,
                COALESCE(stocktwits_title, company_name) AS title,
                COALESCE(stocktwits_exchange, exchange) AS exchange
            FROM stocktwits_symbols
            WHERE is_sp500 = 1
              AND (
                UPPER(symbol) LIKE ?
                OR UPPER(COALESCE(stocktwits_title, company_name, '')) LIKE ?
              )
            ORDER BY
                CASE WHEN UPPER(symbol) = ? THEN 0 ELSE 1 END,
                CASE WHEN UPPER(symbol) LIKE ? THEN 0 ELSE 1 END,
                symbol ASC
            LIMIT 20
            """,
            (pattern, pattern, query.upper(), f"{query.upper()}%"),
        ).fetchall()

        results = [
            {
                "type": "symbol",
                "symbol": row["symbol"],
                "title": row["title"],
                "exchange": row["exchange"],
            }
            for row in rows
        ]
        return jsonify({"results": results})
    finally:
        db.close()


@trending_bp.route("/api/trending/stream/<symbol>")
def symbol_stream(symbol):
    normalized = symbol.upper()
    limit = request.args.get("limit", default=30, type=int)
    limit = max(1, min(limit, 30))

    db = get_db()
    try:
        symbol_row = db.execute(
            """
            SELECT
                s.symbol,
                COALESCE(s.stocktwits_title, s.company_name) AS title,
                COALESCE(s.stocktwits_exchange, s.exchange) AS exchange,
                s.stocktwits_id AS id,
                s.logo_url,
                s.stocktwits_region AS region,
                s.instrument_class,
                ds.watchlist_count,
                ds.snapshot_date,
                ds.raw_symbol_json
            FROM stocktwits_symbols s
            LEFT JOIN stocktwits_daily_snapshots ds
                ON ds.symbol = s.symbol
               AND ds.snapshot_date = (
                    SELECT MAX(snapshot_date)
                    FROM stocktwits_daily_snapshots
                    WHERE symbol = s.symbol
               )
            WHERE s.symbol = ?
            """,
            (normalized,),
        ).fetchone()

        if not symbol_row:
            return jsonify({
                "error": "symbol_not_found",
                "detail": f"Symbol {normalized} is not in the local database.",
            }), 404

        raw_symbol = load_json(symbol_row["raw_symbol_json"], {})
        merged_symbol = {
            "id": symbol_row["id"],
            "symbol": symbol_row["symbol"],
            "title": symbol_row["title"],
            "exchange": symbol_row["exchange"],
            "watchlist_count": symbol_row["watchlist_count"],
            "logo_url": symbol_row["logo_url"],
            "region": symbol_row["region"],
            "instrument_class": symbol_row["instrument_class"],
            "snapshot_date": symbol_row["snapshot_date"],
        }
        merged_symbol.update(raw_symbol)
        merged_symbol["history"] = get_history_changes(
            db,
            normalized,
            latest_snapshot_date=symbol_row["snapshot_date"],
            latest_watchlist_count=symbol_row["watchlist_count"],
        )

        message_rows = db.execute(
            """
            SELECT
                stocktwits_message_id,
                body,
                created_at,
                captured_at,
                username,
                display_name,
                avatar_url,
                sentiment,
                likes_total,
                source_title,
                source_url,
                discussion,
                raw_message_json
            FROM stocktwits_messages
            WHERE symbol = ?
            ORDER BY datetime(created_at) DESC, stocktwits_message_id DESC
            LIMIT ?
            """,
            (normalized, limit),
        ).fetchall()

        messages = []
        for row in message_rows:
            raw_message = load_json(row["raw_message_json"], {})
            if raw_message:
                messages.append(raw_message)
                continue

            messages.append(
                {
                    "id": row["stocktwits_message_id"],
                    "body": row["body"],
                    "created_at": row["created_at"],
                    "discussion": bool(row["discussion"]),
                    "user": {
                        "username": row["username"],
                        "name": row["display_name"],
                        "avatar_url": row["avatar_url"],
                    },
                    "likes": {"total": row["likes_total"]},
                    "source": {
                        "title": row["source_title"],
                        "url": row["source_url"],
                    },
                    "entities": {
                        "sentiment": {"basic": row["sentiment"]} if row["sentiment"] else None
                    },
                }
            )

        cursor = {
            "more": False,
            "since": messages[0]["id"] if messages else None,
            "max": messages[-1]["id"] if messages else None,
        }
        return jsonify({"symbol": merged_symbol, "messages": messages, "cursor": cursor})
    finally:
        db.close()
