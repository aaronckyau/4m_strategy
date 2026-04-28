"""
fetch_sec_insider.py - Fetch SEC Form 4 insider trades into data/db/insider.db.

This job intentionally writes to a separate SQLite database instead of aurum.db.
The web app reads sec_stage_trades from this DB for /insider.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from utils import log


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
load_dotenv(BASE_DIR / ".env", override=False)
load_dotenv(PROJECT_DIR / "Aurum_Infinity_AI" / ".env", override=False)

SEC_BASE_URL = "https://www.sec.gov/Archives"
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "Aaron 4M Insider Research aaron@example.com")
SEC_SOURCE = "sec_stage"
SEC_SOURCE_PRIORITY = 10
TARGET_FORMS = {"4", "4/A"}
REQUEST_SLEEP_SECONDS = float(os.getenv("SEC_REQUEST_SLEEP_SECONDS", "0.12"))
REQUEST_RETRIES = 3
FETCH_STATE_JOB = "sec_form4_stage"
DEFAULT_LOOKBACK_DAYS = 120


STAGE_SCHEMA = """
CREATE TABLE IF NOT EXISTS sec_stage_filings (
    accession_no TEXT PRIMARY KEY,
    filing_date TEXT,
    period_of_report TEXT,
    form_type TEXT,
    issuer_cik TEXT,
    issuer_symbol TEXT,
    issuer_name TEXT,
    reporting_owner_names TEXT,
    reporting_owner_ciks TEXT,
    index_url TEXT,
    xml_url TEXT,
    source TEXT NOT NULL DEFAULT 'sec_stage',
    source_priority INTEGER NOT NULL DEFAULT 10,
    raw_payload_json TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sec_stage_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    accession_no TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'sec_stage',
    source_priority INTEGER NOT NULL DEFAULT 10,
    symbol TEXT NOT NULL,
    issuer_cik TEXT,
    company_name TEXT,
    filling_date TEXT,
    transaction_date TEXT,
    reporting_name TEXT,
    reporting_owner_cik TEXT,
    type_of_owner TEXT,
    acquisition_or_disposition TEXT,
    form_type TEXT,
    transaction_code TEXT,
    transaction_type TEXT,
    securities_transacted REAL,
    price REAL,
    securities_transacted_value REAL,
    securities_owned REAL,
    shares_following_transaction REAL,
    sec_index_url TEXT,
    sec_xml_url TEXT,
    raw_payload_json TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(accession_no, source_record_id)
);

CREATE INDEX IF NOT EXISTS idx_sec_stage_trade_date ON sec_stage_trades(transaction_date);
CREATE INDEX IF NOT EXISTS idx_sec_stage_symbol ON sec_stage_trades(symbol);
CREATE INDEX IF NOT EXISTS idx_sec_stage_accession ON sec_stage_trades(accession_no);
CREATE INDEX IF NOT EXISTS idx_sec_stage_created ON sec_stage_trades(created_at);

CREATE TABLE IF NOT EXISTS sec_fetch_state (
    job_name TEXT PRIMARY KEY,
    last_completed_day TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);
"""


TRANSACTION_TYPE_MAP = {
    "P": "P-Purchase",
    "S": "S-Sale",
    "A": "A-Award",
    "M": "M-Exempt",
    "G": "G-Gift",
    "F": "F-Tax",
    "J": "J-Other",
    "C": "C-Conversion",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def default_db_path() -> Path:
    return Path(
        os.getenv("INSIDER_DB_PATH")
        or PROJECT_DIR / "data" / "db" / "insider.db"
    ).expanduser().resolve()


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(STAGE_SCHEMA)
    conn.commit()


def validate_date(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%d")
    return value


def normalize_sec_date(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    if re.fullmatch(r"\d{8}", text):
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if match:
        return match.group(1)
    return text


def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def quarter_for_day(day: date) -> int:
    return ((day.month - 1) // 3) + 1


def master_index_url(day: date) -> str:
    return f"{SEC_BASE_URL}/edgar/daily-index/{day.year}/QTR{quarter_for_day(day)}/master.{day:%Y%m%d}.idx"


def request_text(session: requests.Session, url: str) -> str:
    last_exc: Exception | None = None
    for attempt in range(REQUEST_RETRIES):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            time.sleep(REQUEST_SLEEP_SECONDS)
            return response.text
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt == REQUEST_RETRIES - 1:
                raise
            time.sleep(1.0 * (attempt + 1))
    assert last_exc is not None
    raise last_exc


def request_json(session: requests.Session, url: str) -> dict:
    last_exc: Exception | None = None
    for attempt in range(REQUEST_RETRIES):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            time.sleep(REQUEST_SLEEP_SECONDS)
            return response.json()
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt == REQUEST_RETRIES - 1:
                raise
            time.sleep(1.0 * (attempt + 1))
    assert last_exc is not None
    raise last_exc


def parse_master_index(text: str) -> list[dict]:
    lines = text.splitlines()
    try:
        start = next(
            i for i, line in enumerate(lines)
            if line.startswith("CIK|Company Name|Form Type|Date Filed|File Name")
        ) + 1
    except StopIteration:
        return []

    rows = []
    for line in lines[start:]:
        parts = line.strip().split("|")
        if len(parts) != 5:
            continue
        cik, company_name, form_type, filing_date, file_name = parts
        if form_type not in TARGET_FORMS:
            continue
        rows.append({
            "issuer_cik": cik,
            "company_name": company_name,
            "form_type": form_type,
            "filing_date": filing_date,
            "file_name": file_name,
        })
    return rows


def accession_from_filename(file_name: str) -> str:
    return file_name.rsplit("/", 1)[-1].replace(".txt", "")


def filing_directory_url(file_name: str) -> str:
    parts = file_name.split("/")
    compact_accession = parts[-1].replace(".txt", "").replace("-", "")
    directory_parts = parts[:-1] + [compact_accession]
    return f"{SEC_BASE_URL}/{'/'.join(directory_parts)}/"


def filing_index_url(file_name: str, accession_no: str) -> str:
    return f"{filing_directory_url(file_name)}{accession_no}-index.html"


def locate_xml_url(session: requests.Session, file_name: str) -> str | None:
    directory = filing_directory_url(file_name)
    index_json = request_json(session, f"{directory}index.json")
    items = index_json.get("directory", {}).get("item", [])
    xml_candidates = [
        item["name"]
        for item in items
        if item.get("name", "").lower().endswith(".xml")
        and "xsl" not in item.get("name", "").lower()
        and "schema" not in item.get("name", "").lower()
    ]
    return f"{directory}{xml_candidates[0]}" if xml_candidates else None


def text_of(node: ET.Element | None, path: str) -> str | None:
    if node is None:
        return None
    found = node.find(path)
    if found is None or found.text is None:
        return None
    return found.text.strip()


def normalize_bool(value: str | None) -> bool:
    return bool(value and value.strip().lower() in {"1", "true", "y", "yes"})


def owner_label(owner_node: ET.Element) -> dict:
    relationship = owner_node.find("reportingOwnerRelationship")
    owner_id = owner_node.find("reportingOwnerId")
    name = text_of(owner_id, "rptOwnerName") or ""
    cik = text_of(owner_id, "rptOwnerCik")

    bits: list[str] = []
    if normalize_bool(text_of(relationship, "isDirector")):
        bits.append("director")
    if normalize_bool(text_of(relationship, "isOfficer")):
        bits.append("officer")
    if normalize_bool(text_of(relationship, "isTenPercentOwner")):
        bits.append("10 percent owner")
    if normalize_bool(text_of(relationship, "isOther")):
        bits.append("other")
    officer_title = text_of(relationship, "officerTitle")
    other_text = text_of(relationship, "otherText")
    if officer_title:
        bits.append(f"title: {officer_title}")
    if other_text:
        bits.append(other_text)

    return {"name": name, "cik": cik, "type_of_owner": ", ".join(bits)}


def transaction_type_from_code(code: str | None) -> str:
    if not code:
        return ""
    return TRANSACTION_TYPE_MAP.get(code, code)


def build_source_record_id(
    accession_no: str,
    owner_cik: str | None,
    transaction_code: str | None,
    transaction_date: str | None,
    shares: float,
    price: float,
    side: str | None,
) -> str:
    fingerprint = "|".join([
        accession_no,
        owner_cik or "",
        transaction_code or "",
        transaction_date or "",
        str(shares),
        str(price),
        side or "",
    ])
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()


def parse_filing(
    xml_text: str,
    xml_url: str,
    index_url: str,
    accession_no: str,
    filing_date: str,
) -> tuple[dict, list[dict]]:
    root = ET.fromstring(xml_text)

    issuer = root.find("issuer")
    issuer_cik = text_of(issuer, "issuerCik")
    issuer_name = text_of(issuer, "issuerName") or ""
    issuer_symbol = text_of(issuer, "issuerTradingSymbol") or ""
    filing_date = normalize_sec_date(filing_date) or filing_date
    period_of_report = normalize_sec_date(text_of(root, "periodOfReport")) or filing_date
    form_type = text_of(root, "documentType") or "4"

    owners = [owner_label(owner) for owner in root.findall("reportingOwner")]
    primary_owner = owners[0] if owners else {"name": "", "cik": None, "type_of_owner": ""}

    filing_payload = {
        "accession_no": accession_no,
        "filing_date": filing_date,
        "period_of_report": period_of_report,
        "form_type": form_type,
        "issuer_cik": issuer_cik,
        "issuer_symbol": issuer_symbol,
        "issuer_name": issuer_name,
        "reporting_owner_names": json.dumps([owner["name"] for owner in owners], ensure_ascii=False),
        "reporting_owner_ciks": json.dumps([owner["cik"] for owner in owners if owner["cik"]], ensure_ascii=False),
        "index_url": index_url,
        "xml_url": xml_url,
        "source": SEC_SOURCE,
        "source_priority": SEC_SOURCE_PRIORITY,
        "raw_payload_json": None,
    }

    trades = []
    for transaction in root.findall(".//nonDerivativeTransaction"):
        transaction_date = normalize_sec_date(text_of(transaction, "transactionDate/value")) or period_of_report
        transaction_code = text_of(transaction, "transactionCoding/transactionCode")
        side = text_of(transaction, "transactionAmounts/transactionAcquiredDisposedCode/value")
        shares = float(text_of(transaction, "transactionAmounts/transactionShares/value") or 0)
        price = float(text_of(transaction, "transactionAmounts/transactionPricePerShare/value") or 0)
        shares_following = float(text_of(transaction, "postTransactionAmounts/sharesOwnedFollowingTransaction/value") or 0)

        trades.append({
            "accession_no": accession_no,
            "source_record_id": build_source_record_id(
                accession_no=accession_no,
                owner_cik=primary_owner["cik"],
                transaction_code=transaction_code,
                transaction_date=transaction_date,
                shares=shares,
                price=price,
                side=side,
            ),
            "source": SEC_SOURCE,
            "source_priority": SEC_SOURCE_PRIORITY,
            "symbol": issuer_symbol,
            "issuer_cik": issuer_cik,
            "company_name": issuer_name,
            "filling_date": filing_date,
            "transaction_date": transaction_date,
            "reporting_name": primary_owner["name"],
            "reporting_owner_cik": primary_owner["cik"],
            "type_of_owner": primary_owner["type_of_owner"],
            "acquisition_or_disposition": side,
            "form_type": form_type,
            "transaction_code": transaction_code,
            "transaction_type": transaction_type_from_code(transaction_code),
            "securities_transacted": shares,
            "price": price,
            "securities_transacted_value": round(shares * price, 2),
            "securities_owned": shares_following,
            "shares_following_transaction": shares_following,
            "sec_index_url": index_url,
            "sec_xml_url": xml_url,
            "raw_payload_json": None,
        })

    return filing_payload, trades


def upsert_filing(conn: sqlite3.Connection, filing: dict) -> None:
    conn.execute(
        """
        INSERT INTO sec_stage_filings (
            accession_no, filing_date, period_of_report, form_type,
            issuer_cik, issuer_symbol, issuer_name,
            reporting_owner_names, reporting_owner_ciks,
            index_url, xml_url, source, source_priority, raw_payload_json, updated_at
        ) VALUES (
            :accession_no, :filing_date, :period_of_report, :form_type,
            :issuer_cik, :issuer_symbol, :issuer_name,
            :reporting_owner_names, :reporting_owner_ciks,
            :index_url, :xml_url, :source, :source_priority, :raw_payload_json, datetime('now')
        )
        ON CONFLICT(accession_no) DO UPDATE SET
            filing_date = excluded.filing_date,
            period_of_report = excluded.period_of_report,
            form_type = excluded.form_type,
            issuer_cik = excluded.issuer_cik,
            issuer_symbol = excluded.issuer_symbol,
            issuer_name = excluded.issuer_name,
            reporting_owner_names = excluded.reporting_owner_names,
            reporting_owner_ciks = excluded.reporting_owner_ciks,
            index_url = excluded.index_url,
            xml_url = excluded.xml_url,
            source = excluded.source,
            source_priority = excluded.source_priority,
            raw_payload_json = excluded.raw_payload_json,
            updated_at = datetime('now')
        """,
        filing,
    )


def insert_trades(conn: sqlite3.Connection, trades: list[dict]) -> int:
    if not trades:
        return 0
    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO sec_stage_trades (
            accession_no, source_record_id, source, source_priority,
            symbol, issuer_cik, company_name, filling_date, transaction_date,
            reporting_name, reporting_owner_cik, type_of_owner,
            acquisition_or_disposition, form_type, transaction_code, transaction_type,
            securities_transacted, price, securities_transacted_value,
            securities_owned, shares_following_transaction,
            sec_index_url, sec_xml_url, raw_payload_json
        ) VALUES (
            :accession_no, :source_record_id, :source, :source_priority,
            :symbol, :issuer_cik, :company_name, :filling_date, :transaction_date,
            :reporting_name, :reporting_owner_cik, :type_of_owner,
            :acquisition_or_disposition, :form_type, :transaction_code, :transaction_type,
            :securities_transacted, :price, :securities_transacted_value,
            :securities_owned, :shares_following_transaction,
            :sec_index_url, :sec_xml_url, :raw_payload_json
        )
        """,
        trades,
    )
    return conn.total_changes - before


def get_existing_accessions(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT accession_no FROM sec_stage_filings")}


def get_resume_day(conn: sqlite3.Connection, job_name: str) -> date | None:
    row = conn.execute(
        "SELECT last_completed_day FROM sec_fetch_state WHERE job_name = ?",
        (job_name,),
    ).fetchone()
    if not row or not row[0]:
        return None
    return datetime.strptime(validate_date(row[0]), "%Y-%m-%d").date() + timedelta(days=1)


def set_resume_day(conn: sqlite3.Connection, job_name: str, day: date) -> None:
    conn.execute(
        """
        INSERT INTO sec_fetch_state (job_name, last_completed_day, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(job_name) DO UPDATE SET
            last_completed_day = excluded.last_completed_day,
            updated_at = datetime('now')
        """,
        (job_name, day.isoformat()),
    )


def resolve_start_day(conn: sqlite3.Connection, since: str | None, resume: bool, lookback_days: int) -> date:
    if since:
        return datetime.strptime(validate_date(since), "%Y-%m-%d").date()
    if resume:
        resume_day = get_resume_day(conn, FETCH_STATE_JOB)
        if resume_day:
            return resume_day
    return date.today() - timedelta(days=max(lookback_days, 1) - 1)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": SEC_USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    })
    return session


def run_fetch(
    *,
    db_path: Path,
    since: str | None = None,
    until: str | None = None,
    max_filings: int | None = None,
    resume: bool = True,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> dict:
    conn = connect_db(db_path)
    init_db(conn)
    session = build_session()

    start = resolve_start_day(conn, since=since, resume=resume, lookback_days=lookback_days)
    end = datetime.strptime(validate_date(until), "%Y-%m-%d").date() if until else date.today()
    if start > end:
        summary = {
            "total_items": 0,
            "success_items": 0,
            "failed_items": 0,
            "skipped_items": 0,
            "records_written": 0,
            "summary_text": f"Already up to date through {end.isoformat()}",
            "items": [],
        }
        conn.close()
        return summary

    existing_accessions = get_existing_accessions(conn)
    pages_seen = 0
    filings_seen = 0
    filings_inserted = 0
    trades_inserted = 0
    failed_items = 0
    skipped_items = 0
    items: list[dict] = []

    try:
        for day in daterange(start, end):
            day_started = _now_iso()
            day_filings_inserted = 0
            day_trades_inserted = 0
            day_failures = 0
            try:
                index_text = request_text(session, master_index_url(day))
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in {403, 404}:
                    skipped_items += 1
                    set_resume_day(conn, FETCH_STATE_JOB, day)
                    conn.commit()
                    items.append({
                        "item_key": day.isoformat(),
                        "item_type": "sec_index_day",
                        "status": "skipped",
                        "attempts": 1,
                        "records_written": 0,
                        "error_message": f"daily index returned {status_code}",
                        "started_at": day_started,
                        "finished_at": _now_iso(),
                    })
                    log(f"skip day {day.isoformat()} because daily index returned {status_code}")
                    continue
                failed_items += 1
                day_failures += 1
                items.append({
                    "item_key": day.isoformat(),
                    "item_type": "sec_index_day",
                    "status": "failed",
                    "attempts": REQUEST_RETRIES,
                    "records_written": 0,
                    "error_message": str(exc),
                    "started_at": day_started,
                    "finished_at": _now_iso(),
                })
                continue
            except (requests.Timeout, requests.ConnectionError) as exc:
                failed_items += 1
                day_failures += 1
                items.append({
                    "item_key": day.isoformat(),
                    "item_type": "sec_index_day",
                    "status": "failed",
                    "attempts": REQUEST_RETRIES,
                    "records_written": 0,
                    "error_message": str(exc),
                    "started_at": day_started,
                    "finished_at": _now_iso(),
                })
                continue

            pages_seen += 1
            filings = parse_master_index(index_text)
            filings_seen += len(filings)
            if not filings:
                set_resume_day(conn, FETCH_STATE_JOB, day)
                conn.commit()
                items.append({
                    "item_key": day.isoformat(),
                    "item_type": "sec_index_day",
                    "status": "success",
                    "attempts": 1,
                    "records_written": 0,
                    "error_message": None,
                    "started_at": day_started,
                    "finished_at": _now_iso(),
                })
                continue

            for filing in filings:
                if max_filings is not None and filings_inserted >= max_filings:
                    conn.commit()
                    return {
                        "total_items": filings_seen,
                        "success_items": filings_inserted,
                        "failed_items": failed_items,
                        "skipped_items": skipped_items,
                        "records_written": trades_inserted,
                        "summary_text": f"Stopped at max_filings={max_filings}; inserted {trades_inserted} trades",
                        "items": items[-1000:],
                    }

                accession_no = accession_from_filename(filing["file_name"])
                if accession_no in existing_accessions:
                    skipped_items += 1
                    continue

                index_url = filing_index_url(filing["file_name"], accession_no)
                try:
                    xml_url = locate_xml_url(session, filing["file_name"])
                    if not xml_url:
                        skipped_items += 1
                        continue
                    xml_text = request_text(session, xml_url)
                    filing_payload, trades = parse_filing(
                        xml_text=xml_text,
                        xml_url=xml_url,
                        index_url=index_url,
                        accession_no=accession_no,
                        filing_date=filing["filing_date"],
                    )
                    upsert_filing(conn, filing_payload)
                    inserted = insert_trades(conn, trades)
                    conn.commit()
                except Exception as exc:
                    failed_items += 1
                    day_failures += 1
                    items.append({
                        "item_key": accession_no,
                        "item_type": "sec_filing",
                        "status": "failed",
                        "attempts": REQUEST_RETRIES,
                        "records_written": 0,
                        "error_message": str(exc),
                        "started_at": day_started,
                        "finished_at": _now_iso(),
                    })
                    log(f"skip accession {accession_no}: {exc}")
                    continue

                existing_accessions.add(accession_no)
                filings_inserted += 1
                day_filings_inserted += 1
                trades_inserted += inserted
                day_trades_inserted += inserted
                log(f"inserted accession {accession_no} | symbol={filing_payload['issuer_symbol']} trades={inserted}")

            set_resume_day(conn, FETCH_STATE_JOB, day)
            conn.commit()
            status = "failed" if day_failures else "success"
            items.append({
                "item_key": day.isoformat(),
                "item_type": "sec_index_day",
                "status": status,
                "attempts": 1,
                "records_written": day_trades_inserted,
                "error_message": f"{day_failures} filing failures" if day_failures else None,
                "started_at": day_started,
                "finished_at": _now_iso(),
            })
            log(
                f"day {day.isoformat()} processed | filings={len(filings)} "
                f"inserted={day_filings_inserted} trades={day_trades_inserted}"
            )
    finally:
        conn.close()

    return {
        "total_items": filings_seen or pages_seen,
        "success_items": filings_inserted,
        "failed_items": failed_items,
        "skipped_items": skipped_items,
        "records_written": trades_inserted,
        "summary_text": (
            f"{pages_seen} index days, {filings_inserted} filings inserted, "
            f"{trades_inserted} trades inserted, {failed_items} failed"
        ),
        "items": items[-1000:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch SEC Form 4 insider trades into data/db/insider.db")
    parser.add_argument("--since", help="Fetch SEC Form 4 / 4-A filings since YYYY-MM-DD")
    parser.add_argument("--until", help="Fetch through YYYY-MM-DD instead of today")
    parser.add_argument("--max-filings", type=int, help="Stop after N new filings")
    parser.add_argument("--no-resume", action="store_true", help="Ignore checkpoint and use --since or lookback")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="Initial lookback when no checkpoint exists")
    parser.add_argument("--db-path", default=None, help="Override insider SQLite path")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser().resolve() if args.db_path else default_db_path()
    log("=== SEC Insider Form 4 Fetcher ===")
    log(f"DB: {db_path}")
    log(f"User-Agent: {SEC_USER_AGENT}")
    summary = run_fetch(
        db_path=db_path,
        since=args.since,
        until=args.until,
        max_filings=args.max_filings,
        resume=not args.no_resume,
        lookback_days=args.lookback_days,
    )
    print("RUN_SUMMARY_JSON:" + json.dumps(summary, ensure_ascii=False), flush=True)
    return 1 if summary.get("failed_items") else 0


if __name__ == "__main__":
    sys.exit(main())
