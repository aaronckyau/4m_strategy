from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import UTC, datetime, timedelta, timezone
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
PENDING_CACHE_PATH = DATA_DIR / "futunn_cache.pending.json"
LIST_URL = "https://news.futunn.com/hk/main?lang=zh-hk"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
}
MAX_ARTICLES = 20
MAX_SAVED_ARTICLES = 100
MIN_SUCCESS_ARTICLES = 5
GEMINI_MODEL = "gemini-3.1-flash-lite-preview"
AI_BATCH_SIZE = 8
CACHE_SCHEMA = load_cache_schema()
ALLOWED_SECTORS = allowed_sectors(CACHE_SCHEMA)
WAF_TOKEN_PATTERN = re.compile(r"(eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)")
HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update(HEADERS)
HKT = timezone(timedelta(hours=8))


SECTOR_LIST_TEXT = "、".join(ALLOWED_SECTORS)
REWRITE_AND_STOCK_PROMPT = load_prompt_template(
    "futunn_rewrite_prompt.txt",
    allowed_sectors=SECTOR_LIST_TEXT,
)


def log(message: str) -> None:
    sys.stdout.buffer.write(f"{message}\n".encode("utf-8", errors="replace"))
    sys.stdout.flush()


def now_hkt_label() -> str:
    return datetime.now(HKT).strftime("%Y-%m-%d %H:%M HKT")


def load_env() -> None:
    candidates = [
        BASE_DIR / ".env",
        WORKSPACE_DIR / ".env",
        WORKSPACE_DIR / "Aurum_Infinity_AI" / ".env",
    ]
    for path in candidates:
        if path.exists():
            load_dotenv(path, override=False)


def is_waf_challenge(html: str) -> bool:
    return "<title>Document</title>" in html and "wafToken=" in html


def extract_waf_token(html: str) -> str:
    match = WAF_TOKEN_PATTERN.search(html)
    return match.group(1) if match else ""


def fetch_html(url: str) -> str:
    response = HTTP_SESSION.get(url, timeout=30)
    response.raise_for_status()
    if is_waf_challenge(response.text):
        token = extract_waf_token(response.text)
        if not token:
            raise RuntimeError("Futunn WAF challenge detected but no wafToken was found")

        HTTP_SESSION.cookies.set("wafToken", token, domain="news.futunn.com", path="/")
        response = HTTP_SESSION.get(url, timeout=30)
        response.raise_for_status()

        if is_waf_challenge(response.text):
            raise RuntimeError("Futunn WAF challenge did not clear after wafToken retry")

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
    trans_content = soup.select_one(".trans_content")
    if trans_content:
        paragraphs = [
            tag.get_text(" ", strip=True)
            for tag in trans_content.find_all("p")
            if tag.get_text(" ", strip=True)
        ]
        if paragraphs:
            return paragraphs

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
    if title == "Document" or (not summary and not paragraphs):
        raise RuntimeError(f"Futunn detail content was not available: {url}")

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


def write_cache_file(path: Path, payload: dict[str, Any]) -> None:
    validate_cache_payload(payload, CACHE_SCHEMA)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_pending_cache(payload: dict[str, Any]) -> None:
    write_cache_file(PENDING_CACHE_PATH, payload)


def publish_cache(payload: dict[str, Any]) -> None:
    write_cache_file(PENDING_CACHE_PATH, payload)
    PENDING_CACHE_PATH.replace(CACHE_PATH)
    log(f"已發布正式快取：{CACHE_PATH}")


def ensure_publishable(payload: dict[str, Any]) -> None:
    articles = payload.get("articles", [])
    if len(articles) < MIN_SUCCESS_ARTICLES:
        raise RuntimeError(
            f"Only {len(articles)} valid articles were fetched; "
            f"keeping existing cache because minimum is {MIN_SUCCESS_ARTICLES}."
        )

    bad_titles = [
        str(article.get("url", ""))
        for article in articles
        if str(article.get("title", "")).strip() == "Document"
    ]
    if bad_titles:
        raise RuntimeError(f"Refusing to publish cache with Document title articles: {bad_titles[:3]}")

    empty_content = [
        str(article.get("url", ""))
        for article in articles
        if not article.get("paragraphs") and not str(article.get("summary", "")).strip()
    ]
    if empty_content:
        raise RuntimeError(f"Refusing to publish cache with empty article content: {empty_content[:3]}")


def load_existing_articles() -> list[dict[str, Any]]:
    if not CACHE_PATH.exists():
        return []

    try:
        payload = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    articles = payload.get("articles", [])
    return [article for article in articles if isinstance(article, dict)]


def merge_with_existing_articles(new_articles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    existing_articles = load_existing_articles()
    existing_ids = {
        str(article.get("id", "")).strip()
        for article in existing_articles
        if str(article.get("id", "")).strip()
    }

    merged: list[dict[str, Any]] = []
    seen: set[str] = set()

    for article in new_articles + existing_articles:
        article_id = str(article.get("id", "")).strip()
        if not article_id or article_id in seen:
            continue
        seen.add(article_id)
        merged.append(article)

    new_count = sum(
        1
        for article in new_articles
        if str(article.get("id", "")).strip() and str(article.get("id", "")).strip() not in existing_ids
    )
    return merged[:MAX_SAVED_ARTICLES], new_count


def update_payload(
    payload: dict[str, Any],
    articles: list[dict[str, Any]],
    failures: list[dict[str, str]],
    article_urls: list[str],
    skipped_flash: int,
    *,
    saved_articles: int | None = None,
    total_articles: int | None = None,
    new_articles: int = 0,
) -> None:
    payload["articles"] = articles
    payload["fetched_at"] = now_hkt_label()
    payload["message"] = "" if not failures else f"有 {len(failures)} 篇抓取失敗"
    payload["meta"] = {
        "list_url": LIST_URL,
        "requested": len(article_urls),
        "saved": saved_articles if saved_articles is not None else len(articles),
        "total_articles": total_articles if total_articles is not None else len(articles),
        "new_articles": new_articles,
        "max_articles": MAX_SAVED_ARTICLES,
        "skipped_flash": skipped_flash,
        "failures": failures,
    }


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
        "fetched_at": now_hkt_label(),
        "categories": ["富途"],
        "articles": [],
        "message": "",
        "meta": {
            "list_url": LIST_URL,
            "requested": len(article_urls),
            "saved": 0,
            "total_articles": 0,
            "new_articles": 0,
            "max_articles": MAX_SAVED_ARTICLES,
            "skipped_flash": 0,
            "failures": failures,
        },
    }
    save_pending_cache(payload)

    for index, url in enumerate(article_urls, start=1):
        try:
            article = parse_detail(url)
            if is_flash_news(article):
                skipped_flash += 1
                log(f"[{index}/{len(article_urls)}] 排除快訊：{article['title']}")
                continue

            articles.append(article)
            log(f"[{index}/{len(article_urls)}] 完成：{article['title']}")
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            failures.append({"url": url, "error": str(exc)})
            log(f"[{index}/{len(article_urls)}] 跳過：{url} ({exc})")

        update_payload(payload, articles, failures, article_urls, skipped_flash)
        save_pending_cache(payload)

    ensure_publishable(payload)

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

        update_payload(payload, articles, failures, article_urls, skipped_flash)
        save_pending_cache(payload)

    merged_articles, new_article_count = merge_with_existing_articles(articles)
    update_payload(
        payload,
        merged_articles,
        failures,
        article_urls,
        skipped_flash,
        saved_articles=len(articles),
        total_articles=len(merged_articles),
        new_articles=new_article_count,
    )
    ensure_publishable(payload)
    publish_cache(payload)

    return payload


def main() -> None:
    payload = build_cache()
    log(f"完成，共儲存 {len(payload['articles'])} 篇：{CACHE_PATH}")


if __name__ == "__main__":
    main()
