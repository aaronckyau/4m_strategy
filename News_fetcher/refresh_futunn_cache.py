from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai

try:
    from prompt_loader import load_prompt_template
    from schema_validator import allowed_sectors, load_cache_schema, validate_cache_payload
except ImportError:
    from News_fetcher.prompt_loader import load_prompt_template
    from News_fetcher.schema_validator import allowed_sectors, load_cache_schema, validate_cache_payload

BASE_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"
CACHE_PATH = DATA_DIR / "futunn_cache.json"
LIST_URL = "https://news.futunn.com/hk/main?lang=zh-hk"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
}
MAX_ARTICLES = 20
GEMINI_MODEL = "gemini-3.1-flash-lite-preview"
AI_BATCH_SIZE = 8
CACHE_SCHEMA = load_cache_schema()
ALLOWED_SECTORS = allowed_sectors(CACHE_SCHEMA)


SECTOR_LIST_TEXT = "、".join(ALLOWED_SECTORS)
REWRITE_AND_STOCK_PROMPT = load_prompt_template(
    "futunn_rewrite_prompt.txt",
    allowed_sectors=SECTOR_LIST_TEXT,
)


def log(message: str) -> None:
    sys.stdout.buffer.write(f"{message}\n".encode("utf-8", errors="replace"))
    sys.stdout.flush()


def load_env() -> None:
    candidates = [
        BASE_DIR / ".env",
        WORKSPACE_DIR / ".env",
        WORKSPACE_DIR / "Aurum_Infinity_AI" / ".env",
    ]
    for path in candidates:
        if path.exists():
            load_dotenv(path, override=False)


def fetch_html(url: str) -> str:
    response = requests.get(url, timeout=30, headers=HEADERS)
    response.raise_for_status()
    return response.text


def build_article_id(url: str, title: str) -> str:
    seed = "|".join([url, title])
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            output.append(item)
    return output


def parse_listing(html: str) -> list[str]:
    links = re.findall(r"https://news\.futunn\.com/hk/post/\d+/[^\s\"'<>?]+", html)
    return unique_preserve_order(links)


def extract_meta(soup: BeautifulSoup, key: str) -> str:
    tag = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
    if tag and tag.get("content"):
        return str(tag["content"]).strip()
    return ""


def extract_source_and_time(text: str) -> tuple[str, str]:
    normalized = re.sub(r"\s+", " ", text)
    match = re.search(r"([^\s·•]{2,30})\s*[·•]\s*(\d{1,2}:\d{2})", normalized)
    if not match:
        return "", ""
    return match.group(1).strip(), match.group(2).strip()


def extract_content(soup: BeautifulSoup, fallback: str) -> list[str]:
    candidates: list[tuple[int, str]] = []
    for tag in soup.find_all(["div", "article", "section"]):
        classes = " ".join(tag.get("class", []))
        text = tag.get_text(" ", strip=True)
        if not text:
            continue
        if "newsDetail" in classes or "trans_content" in classes:
            candidates.append((len(text), text))

    content = max(candidates, key=lambda item: item[0])[1] if candidates else fallback
    paragraphs = [part.strip() for part in re.split(r"\s{2,}", content) if part.strip()]
    if not paragraphs and content:
        paragraphs = [content]
    return paragraphs


def parse_detail(url: str) -> dict[str, Any]:
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    title = extract_meta(soup, "og:title")
    if not title and soup.title:
        title = soup.title.get_text(strip=True)

    summary = extract_meta(soup, "og:description")
    cover_image = extract_meta(soup, "og:image") or None
    source, short_time = extract_source_and_time(page_text)
    paragraphs = extract_content(soup, summary)

    return {
        "id": build_article_id(url, title or url),
        "title": title or "Untitled",
        "summary": summary,
        "summary_raw": summary,
        "paragraphs": paragraphs,
        "paragraphs_raw": paragraphs,
        "source": source,
        "source_url": url,
        "url": url,
        "time": short_time,
        "category": "富途",
        "cover_image": cover_image,
        "stock_tags": [],
        "country_tags": [],
        "sector_tags": [],
        "ai_rewrite_raw": None,
        "ai_rewrite_error": None,
    }


def is_flash_news(article: dict[str, Any]) -> bool:
    source = str(article.get("source") or "").strip().lower()
    title = str(article.get("title") or "").strip().lower()
    return source in {"快讯", "快訊"} or title.startswith("快讯") or title.startswith("快訊")


def chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def build_ai_batch_payload(batch: list[dict[str, Any]]) -> str:
    payload = {
        "items": [
            {
                "id": article["id"],
                "title": article["title"],
                "summary": article["summary_raw"],
                "paragraphs": article["paragraphs_raw"],
            }
            for article in batch
        ]
    }
    return json.dumps(payload, ensure_ascii=False)


def sanitize_ticker(value: str) -> str:
    return re.sub(r"[^A-Z]", "", str(value).upper())


def sanitize_label_list(values: Any, *, max_items: int) -> list[str]:
    if not isinstance(values, list):
        return []

    cleaned: list[str] = []
    for item in values:
        value = re.sub(r"\s+", " ", str(item)).strip()
        if not value or value in cleaned:
            continue
        cleaned.append(value)
        if len(cleaned) >= max_items:
            break
    return cleaned


def sanitize_sector_list(values: Any) -> list[str]:
    allowed = set(ALLOWED_SECTORS)
    sectors = sanitize_label_list(values, max_items=len(ALLOWED_SECTORS))
    return [sector for sector in sectors if sector in allowed]


def analyze_ai_batch(client: genai.Client, batch: list[dict[str, Any]]) -> tuple[str, dict[str, dict[str, Any]]]:
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        config={"system_instruction": REWRITE_AND_STOCK_PROMPT},
        contents=build_ai_batch_payload(batch),
    )
    raw_text = response.text
    payload = json.loads(raw_text)

    result_map: dict[str, dict[str, Any]] = {}
    for item in payload.get("items", []):
        article_id = str(item.get("id", "")).strip()
        if not article_id:
            continue

        summary = str(item.get("summary", "")).strip()
        paragraphs = [
            str(paragraph).strip()
            for paragraph in item.get("paragraphs", [])
            if str(paragraph).strip()
        ]
        stock_tags: list[str] = []
        for ticker in item.get("stock_tags", []):
            symbol = sanitize_ticker(str(ticker))
            if symbol and symbol not in stock_tags:
                stock_tags.append(symbol)

        country_tags = sanitize_label_list(
            item.get("country_tags", []),
            max_items=5,
        )
        sector_tags = sanitize_sector_list(item.get("sector_tags", []))

        result_map[article_id] = {
            "summary": summary,
            "paragraphs": paragraphs,
            "stock_tags": stock_tags[:5],
            "country_tags": country_tags,
            "sector_tags": sector_tags,
            "ai_rewrite_raw": item,
        }

    return raw_text, result_map


def save_cache(payload: dict[str, Any]) -> None:
    validate_cache_payload(payload, CACHE_SCHEMA)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_cache() -> dict[str, Any]:
    load_env()
    if not os.getenv("GEMINI_API_KEY"):
        raise RuntimeError("找不到 GEMINI_API_KEY。請先在 News_fetcher/.env、4M/.env 或 Aurum_Infinity_AI/.env 設定。")

    client = genai.Client()
    log("開始抓取富途新聞...")
    listing_html = fetch_html(LIST_URL)
    article_urls = parse_listing(listing_html)[:MAX_ARTICLES]
    log(f"列表解析完成，共 {len(article_urls)} 篇待抓取")

    articles: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    skipped_flash = 0

    payload = {
        "fetched_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "categories": ["富途"],
        "articles": [],
        "message": "",
        "meta": {
            "list_url": LIST_URL,
            "requested": len(article_urls),
            "saved": 0,
            "skipped_flash": 0,
            "failures": failures,
        },
    }
    save_cache(payload)

    for index, url in enumerate(article_urls, start=1):
        try:
            article = parse_detail(url)
            if is_flash_news(article):
                skipped_flash += 1
                log(f"[{index}/{len(article_urls)}] 排除快訊：{article['title']}")
                continue

            articles.append(article)
            log(f"[{index}/{len(article_urls)}] 完成：{article['title']}")
        except requests.RequestException as exc:
            failures.append({"url": url, "error": str(exc)})
            log(f"[{index}/{len(article_urls)}] 失敗：{url}")

        payload["articles"] = articles
        payload["fetched_at"] = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
        payload["message"] = "" if not failures else f"有 {len(failures)} 篇抓取失敗"
        payload["meta"] = {
            "list_url": LIST_URL,
            "requested": len(article_urls),
            "saved": len(articles),
            "skipped_flash": skipped_flash,
            "failures": failures,
        }
        save_cache(payload)

    batches = chunked(articles, AI_BATCH_SIZE)
    for batch_index, batch in enumerate(batches, start=1):
        log(f"[Gemini 批次 {batch_index}/{len(batches)}] 共 {len(batch)} 篇")
        try:
            raw_text, result_map = analyze_ai_batch(client, batch)
        except Exception as exc:
            log(f"  - Gemini 批次失敗：{exc}")
            for article in batch:
                article["ai_rewrite_error"] = str(exc)
        else:
            log("  - Gemini 原始輸出：")
            log(raw_text)
            for article in batch:
                result = result_map.get(article["id"])
                if not result:
                    article["ai_rewrite_error"] = "Gemini 未返回此文章結果"
                    continue

                article["summary"] = result["summary"] or article["summary"]
                article["paragraphs"] = result["paragraphs"] or article["paragraphs"]
                article["stock_tags"] = result["stock_tags"]
                article["country_tags"] = result["country_tags"]
                article["sector_tags"] = result["sector_tags"]
                article["ai_rewrite_raw"] = result["ai_rewrite_raw"]
                article["ai_rewrite_error"] = None

        payload["articles"] = articles
        payload["fetched_at"] = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
        payload["message"] = "" if not failures else f"有 {len(failures)} 篇抓取失敗"
        payload["meta"] = {
            "list_url": LIST_URL,
            "requested": len(article_urls),
            "saved": len(articles),
            "skipped_flash": skipped_flash,
            "failures": failures,
        }
        save_cache(payload)

    return payload


def main() -> None:
    payload = build_cache()
    log(f"完成，共儲存 {len(payload['articles'])} 篇：{CACHE_PATH}")


if __name__ == "__main__":
    main()
