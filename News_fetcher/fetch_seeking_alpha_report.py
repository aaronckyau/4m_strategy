from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai

try:
    from prompt_loader import load_prompt_template
except ImportError:
    from News_fetcher.prompt_loader import load_prompt_template


BASE_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_OUTPUT_PATH = DATA_DIR / "seeking_alpha_report.json"
RSS_URL = "https://seekingalpha.com/tag/wall-st-breakfast.xml"
RAPIDAPI_HOST = "seeking-alpha.p.rapidapi.com"
RAPIDAPI_BASE_URL = f"https://{RAPIDAPI_HOST}"
GEMINI_MODEL = "gemini-3.1-flash-lite-preview"
HKT = timezone(timedelta(hours=8))
REQUEST_TIMEOUT = 30
MAX_ARTICLE_TEXT_CHARS = 5000


def log(message: str) -> None:
    sys.stdout.buffer.write(f"{message}\n".encode("utf-8", errors="replace"))
    sys.stdout.flush()


def load_env() -> None:
    for path in [
        BASE_DIR / ".env",
        WORKSPACE_DIR / ".env",
        WORKSPACE_DIR / "Aurum_Infinity_AI" / ".env",
    ]:
        if path.exists():
            load_dotenv(path, override=False)


def get_rapidapi_key() -> str:
    return (
        os.getenv("SEEKING_ALPHA_RAPIDAPI_KEY")
        or os.getenv("RAPIDAPI_KEY")
        or os.getenv("RAPID_API_KEY")
        or ""
    ).strip()


def parse_rss_datetime(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %z")
    except ValueError:
        return value
    return parsed.astimezone(HKT).isoformat()


def fetch_rss_items(max_items: int) -> list[dict[str, Any]]:
    response = requests.get(
        RSS_URL,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    root = ET.fromstring(response.content)
    items: list[dict[str, Any]] = []
    for item in root.findall("./channel/item")[:max_items]:
        categories = item.findall("category")
        symbols = [
            (category.text or "").strip()
            for category in categories
            if category.attrib.get("type") == "symbol" and (category.text or "").strip()
        ]
        guid = (item.findtext("guid") or "").strip()
        if not guid:
            continue
        items.append(
            {
                "id": guid,
                "title": (item.findtext("title") or "").strip(),
                "url": (item.findtext("link") or "").strip(),
                "published_at": parse_rss_datetime((item.findtext("pubDate") or "").strip()),
                "symbols": symbols,
            }
        )
    return items


def rapidapi_headers(api_key: str) -> dict[str, str]:
    return {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }


def clean_html_content(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html or "", "html.parser")
    for tag in soup(["script", "style", "figure", "iframe", "noscript"]):
        tag.decompose()

    for selector in [
        ".inline_ad_placeholder",
        ".before_last_paragraph-piano-placeholder",
        ".piano-placeholder",
    ]:
        for tag in soup.select(selector):
            tag.decompose()

    text = soup.get_text("\n", strip=True)
    text = html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def fetch_article_detail(api_key: str, article: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(
        f"{RAPIDAPI_BASE_URL}/articles/get-details",
        params={"id": article["id"]},
        headers=rapidapi_headers(api_key),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()

    data = payload.get("data") or {}
    attributes = data.get("attributes") or {}
    included = payload.get("included") or []
    api_symbols = [
        item.get("attributes", {}).get("name", "")
        for item in included
        if item.get("type") == "tag" and item.get("attributes", {}).get("tagKind") == "Tags::Ticker"
    ]
    symbols = unique_preserve_order([*article.get("symbols", []), *api_symbols])
    content = clean_html_content(str(attributes.get("content") or ""))

    return {
        "id": str(data.get("id") or article["id"]),
        "title": str(attributes.get("title") or article["title"]),
        "url": article["url"],
        "published_at": str(attributes.get("publishOn") or article["published_at"]),
        "symbols": symbols,
        "summary": [str(item).strip() for item in attributes.get("summary") or [] if str(item).strip()],
        "is_paywalled": bool(attributes.get("isPaywalled")),
        "content_length": len(content),
        "content_for_ai": content[:MAX_ARTICLE_TEXT_CHARS],
    }


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        cleaned = str(value).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        output.append(cleaned)
    return output


def build_gemini_payload(articles: list[dict[str, Any]]) -> str:
    payload = {
        "source": "Seeking Alpha Wall Street Breakfast",
        "rss_url": RSS_URL,
        "generated_at": datetime.now(HKT).isoformat(),
        "articles": [
            {
                "id": article["id"],
                "title": article["title"],
                "published_at": article["published_at"],
                "url": article["url"],
                "symbols": article["symbols"],
                "summary": article["summary"],
                "content": article["content_for_ai"],
            }
            for article in articles
        ],
    }
    return json.dumps(payload, ensure_ascii=False)


def parse_json_response(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
    if fenced:
        text = fenced.group(1).strip()
    return json.loads(text)


def generate_report(articles: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    if not os.getenv("GEMINI_API_KEY"):
        raise RuntimeError(
            "找不到 GEMINI_API_KEY。請先在 News_fetcher/.env、4M/.env 或 Aurum_Infinity_AI/.env 設定。"
        )

    client = genai.Client()
    prompt = load_prompt_template("seeking_alpha_report_prompt.txt")
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        config={"system_instruction": prompt},
        contents=build_gemini_payload(articles),
    )
    raw_text = response.text or ""
    return raw_text, parse_json_response(raw_text)


def write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_report(max_items: int, output_path: Path, skip_gemini: bool) -> dict[str, Any]:
    load_env()
    api_key = get_rapidapi_key()
    if not api_key:
        raise RuntimeError(
            "找不到 SEEKING_ALPHA_RAPIDAPI_KEY 或 RAPIDAPI_KEY。請先在 .env 設定 RapidAPI key。"
        )

    log("開始讀取 Seeking Alpha Wall Street Breakfast RSS...")
    rss_items = fetch_rss_items(max_items)
    log(f"RSS 解析完成，共 {len(rss_items)} 篇待抓取 detail")

    articles: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for index, item in enumerate(rss_items, start=1):
        try:
            detail = fetch_article_detail(api_key, item)
        except (requests.RequestException, ValueError, KeyError) as exc:
            failures.append({"id": item["id"], "title": item.get("title", ""), "error": str(exc)})
            log(f"[{index}/{len(rss_items)}] 跳過：{item.get('title', item['id'])} ({exc})")
            continue

        articles.append(detail)
        log(f"[{index}/{len(rss_items)}] 完成：{detail['title']} ({detail['content_length']} chars)")

    if not articles:
        raise RuntimeError("沒有成功取得任何 Seeking Alpha article detail。")

    report: dict[str, Any] | None = None
    raw_gemini_output = ""
    if not skip_gemini:
        log("開始呼叫 Gemini 生成市場熱話報告...")
        raw_gemini_output, report = generate_report(articles)
        log("Gemini 報告生成完成")

    output = {
        "generated_at": datetime.now(HKT).isoformat(),
        "source": {
            "name": "Seeking Alpha Wall Street Breakfast",
            "rss_url": RSS_URL,
            "rapidapi_endpoint": "/articles/get-details",
        },
        "meta": {
            "requested": len(rss_items),
            "fetched": len(articles),
            "failures": failures,
            "gemini_model": None if skip_gemini else GEMINI_MODEL,
            "skip_gemini": skip_gemini,
        },
        "articles": [
            {
                "id": article["id"],
                "title": article["title"],
                "url": article["url"],
                "published_at": article["published_at"],
                "symbols": article["symbols"],
                "summary": article["summary"],
                "content_length": article["content_length"],
                "is_paywalled": article["is_paywalled"],
            }
            for article in articles
        ],
        "report": report,
        "raw_gemini_output": raw_gemini_output,
    }
    write_report(output_path, output)
    log(f"已輸出：{output_path}")
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Seeking Alpha Wall Street Breakfast RSS details and generate a Gemini market report."
    )
    parser.add_argument("--max-items", type=int, default=8, help="最多處理幾篇 RSS item，預設 8。")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"輸出 JSON 路徑，預設 {DEFAULT_OUTPUT_PATH}。",
    )
    parser.add_argument(
        "--skip-gemini",
        action="store_true",
        help="只測 RSS + RapidAPI detail，不呼叫 Gemini。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    max_items = max(1, min(int(args.max_items), 20))
    build_report(max_items=max_items, output_path=args.output, skip_gemini=bool(args.skip_gemini))


if __name__ == "__main__":
    main()
