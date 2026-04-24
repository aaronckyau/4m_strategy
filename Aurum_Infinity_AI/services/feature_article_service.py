from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


_SERVICE_DIR = Path(__file__).resolve().parent
_APP_DIR = _SERVICE_DIR.parent
_WORKSPACE_DIR = _APP_DIR.parent
_FEATURE_DIR = _WORKSPACE_DIR / "News_features"
_ARTICLE_DIR = _FEATURE_DIR / "articles"
_IMAGE_DIR = _FEATURE_DIR / "images"
_MANIFEST_PATH = _FEATURE_DIR / "manifest.json"
_ALLOWED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
_CHARSET_RE = re.compile(r"""<meta[^>]+charset=["']?\s*([a-zA-Z0-9_\-]+)""", re.IGNORECASE)
_MOJIBAKE_MARKERS = "ÃÂâ€œâ€\u00a0æåçèéêï¼"


def slugify_feature(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower())
    return slug.strip("-")


def parse_feature_tags(raw_tags: str | list[str]) -> list[str]:
    if isinstance(raw_tags, list):
        values = raw_tags
    else:
        values = re.split(r"[,，\n]+", str(raw_tags))
    tags: list[str] = []
    for item in values:
        tag = str(item).strip()
        if tag and tag not in tags:
            tags.append(tag)
    return tags


def _read_manifest_items() -> list[dict[str, Any]]:
    if not _MANIFEST_PATH.exists():
        return []

    try:
        payload = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return payload if isinstance(payload, list) else []


def _write_manifest_items(items: list[dict[str, Any]]) -> None:
    _FEATURE_DIR.mkdir(parents=True, exist_ok=True)
    _ARTICLE_DIR.mkdir(parents=True, exist_ok=True)
    _IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    _MANIFEST_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_image_extension(filename: str) -> str:
    suffix = Path(str(filename or "")).suffix.lower()
    if suffix not in _ALLOWED_IMAGE_EXTENSIONS:
        raise ValueError("圖片格式只接受 jpg、jpeg、png、webp、gif")
    return suffix


def _safe_child(base: Path, filename: str) -> Path | None:
    candidate = (base / str(filename).strip()).resolve()
    try:
        candidate.relative_to(base.resolve())
    except ValueError:
        return None
    return candidate


def _remove_file_if_possible(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except PermissionError:
        return


def _count_cjk(text: str) -> int:
    return sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")


def _mojibake_score(text: str) -> int:
    return sum(text.count(marker) for marker in _MOJIBAKE_MARKERS)


def _repair_common_mojibake(text: str) -> str:
    try:
        repaired = text.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text

    original_score = (_count_cjk(text), -_mojibake_score(text))
    repaired_score = (_count_cjk(repaired), -_mojibake_score(repaired))
    return repaired if repaired_score > original_score else text


def _ensure_utf8_meta(html: str) -> str:
    if _CHARSET_RE.search(html):
        return _CHARSET_RE.sub('<meta charset="utf-8"', html, count=1)

    head_match = re.search(r"<head[^>]*>", html, re.IGNORECASE)
    if head_match:
        insert_at = head_match.end()
        return html[:insert_at] + '\n<meta charset="utf-8">' + html[insert_at:]
    return html


def _decode_html_bytes(html_bytes: bytes) -> str:
    ascii_probe = html_bytes.decode("ascii", errors="ignore")
    encodings: list[str] = []
    meta_match = _CHARSET_RE.search(ascii_probe)
    if meta_match:
        encodings.append(meta_match.group(1).strip().lower())
    encodings.extend(["utf-8-sig", "utf-8", "cp950", "big5", "cp1252", "latin1"])

    last_error: Exception | None = None
    seen: set[str] = set()
    for encoding in encodings:
        normalized = encoding.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        try:
            text = html_bytes.decode(encoding)
            return _ensure_utf8_meta(_repair_common_mojibake(text))
        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error is not None:
        raise ValueError("HTML 檔案無法以可接受的編碼解析") from last_error
    raise ValueError("HTML 檔案內容為空或編碼無法識別")


def _normalize_feature(item: dict[str, Any]) -> dict[str, Any] | None:
    slug = slugify_feature(item.get("slug", ""))
    html_file = str(item.get("html_file", "")).strip()
    title = str(item.get("title", "")).strip()
    if not slug or not html_file or not title:
        return None

    article_path = _safe_child(_ARTICLE_DIR, html_file)
    if not article_path or not article_path.exists():
        return None

    tags = item.get("tags", [])
    if not isinstance(tags, list):
        tags = []

    image_file = str(item.get("image_file", "")).strip()
    image_path = _safe_child(_IMAGE_DIR, image_file) if image_file else None
    if image_path and not image_path.exists():
        image_path = None

    return {
        "slug": slug,
        "title": title,
        "summary": str(item.get("summary", "")).strip(),
        "date": str(item.get("date", "")).strip(),
        "tags": [str(tag).strip() for tag in tags if str(tag).strip()],
        "html_file": html_file,
        "image_file": image_file,
        "image_path": image_path,
        "source": str(item.get("source", "4M 專題")).strip() or "4M 專題",
        "path": article_path,
    }


def load_feature_articles() -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for item in _read_manifest_items():
        if not isinstance(item, dict):
            continue
        feature = _normalize_feature(item)
        if feature:
            features.append(feature)
    return features


def get_feature_article(slug: str) -> dict[str, Any] | None:
    normalized_slug = slugify_feature(slug)
    for feature in load_feature_articles():
        if feature["slug"] == normalized_slug:
            return feature
    return None


def get_feature_manifest_item(slug: str) -> dict[str, Any] | None:
    normalized_slug = slugify_feature(slug)
    for item in _read_manifest_items():
        if not isinstance(item, dict):
            continue
        if slugify_feature(item.get("slug", "")) == normalized_slug:
            payload = dict(item)
            payload["slug"] = normalized_slug
            payload["tags"] = parse_feature_tags(item.get("tags", []))
            payload["image_file"] = str(item.get("image_file", "")).strip()
            return payload
    return None


def save_feature_article(
    *,
    slug: str,
    title: str,
    summary: str,
    date: str,
    tags: list[str] | str,
    source: str,
    html_bytes: bytes | None,
    image_bytes: bytes | None = None,
    image_filename: str | None = None,
    original_slug: str | None = None,
) -> dict[str, Any]:
    normalized_slug = slugify_feature(slug)
    normalized_original_slug = slugify_feature(original_slug or normalized_slug)
    if not normalized_slug:
        raise ValueError("slug 不可為空")
    if not str(title).strip():
        raise ValueError("title 不可為空")

    items = _read_manifest_items()
    target_index = -1
    existing_item: dict[str, Any] | None = None
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        item_slug = slugify_feature(item.get("slug", ""))
        if item_slug == normalized_original_slug:
            target_index = index
            existing_item = dict(item)
        if item_slug == normalized_slug and item_slug != normalized_original_slug:
            raise ValueError("slug 已存在")

    html_file = f"{normalized_slug}.html"
    if html_bytes is None:
        if existing_item is None:
            raise ValueError("新增專題時必須上傳 HTML 檔案")
        html_file = str(existing_item.get("html_file", "")).strip() or html_file
    else:
        _ARTICLE_DIR.mkdir(parents=True, exist_ok=True)
        normalized_html = _decode_html_bytes(html_bytes)
        (_ARTICLE_DIR / html_file).write_text(normalized_html, encoding="utf-8")

    image_file = str(existing_item.get("image_file", "")).strip() if existing_item else ""
    old_image_file = image_file
    if image_bytes is not None:
        suffix = _normalize_image_extension(image_filename or "")
        image_file = f"{normalized_slug}{suffix}"
        _IMAGE_DIR.mkdir(parents=True, exist_ok=True)
        (_IMAGE_DIR / image_file).write_bytes(image_bytes)
    elif existing_item and normalized_slug != normalized_original_slug and image_file:
        old_path = _IMAGE_DIR / image_file
        suffix = Path(image_file).suffix.lower()
        if old_path.exists() and suffix in _ALLOWED_IMAGE_EXTENSIONS:
            image_file = f"{normalized_slug}{suffix}"
            new_path = _IMAGE_DIR / image_file
            if old_path != new_path:
                old_path.replace(new_path)

    record = {
        "slug": normalized_slug,
        "title": str(title).strip(),
        "summary": str(summary).strip(),
        "date": str(date).strip(),
        "tags": parse_feature_tags(tags),
        "html_file": html_file,
        "image_file": image_file,
        "source": str(source).strip() or "4M 專題",
    }

    if target_index >= 0:
        items[target_index] = record
        old_html_file = str(existing_item.get("html_file", "")).strip() if existing_item else ""
        if html_bytes is not None and old_html_file and old_html_file != html_file:
            old_path = _ARTICLE_DIR / old_html_file
            _remove_file_if_possible(old_path)
        if image_bytes is not None and old_image_file and old_image_file != image_file:
            old_image_path = _IMAGE_DIR / old_image_file
            _remove_file_if_possible(old_image_path)
    else:
        items.insert(0, record)

    _write_manifest_items(items)
    feature = get_feature_article(normalized_slug)
    if feature is None:
        raise RuntimeError("專題文章儲存後讀取失敗")
    return feature


def delete_feature_article(slug: str) -> bool:
    normalized_slug = slugify_feature(slug)
    items = _read_manifest_items()
    remaining: list[dict[str, Any]] = []
    target_html_file = ""
    target_image_file = ""

    for item in items:
        if not isinstance(item, dict):
            continue
        item_slug = slugify_feature(item.get("slug", ""))
        if item_slug == normalized_slug:
            target_html_file = str(item.get("html_file", "")).strip()
            target_image_file = str(item.get("image_file", "")).strip()
            continue
        remaining.append(item)

    if len(remaining) == len(items):
        return False

    _write_manifest_items(remaining)
    if target_html_file:
        target_path = _ARTICLE_DIR / target_html_file
        _remove_file_if_possible(target_path)
    if target_image_file:
        target_image_path = _IMAGE_DIR / target_image_file
        _remove_file_if_possible(target_image_path)
    return True
