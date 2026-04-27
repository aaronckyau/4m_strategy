from __future__ import annotations

from flask import Response, abort, render_template, request, send_file

from blueprints.news import news_bp
from blueprints.stock.routes import get_current_lang
from services.feature_article_service import get_feature_article, load_feature_articles
from services.news_service import load_news_data
from translations import get_translations


@news_bp.route("/news")
def home():
    lang = get_current_lang()
    t = get_translations(lang)
    data = load_news_data()
    meta = data.get("meta", {}) if isinstance(data.get("meta"), dict) else {}
    category = (request.args.get("category") or "").strip()
    active_tab = (request.args.get("tab") or "all").strip()
    if active_tab not in {"all", "features", "briefs"}:
        active_tab = "all"

    articles = data.get("articles", [])
    features = load_feature_articles(article_type="feature")

    if category:
        filtered = [article for article in articles if article.get("category") == category]
    else:
        filtered = articles

    return render_template(
        "news/index.html",
        lang=lang,
        t=t,
        articles=filtered,
        feature_spotlight=features[:3],
        featured=filtered[:3],
        latest=filtered[3:],
        features=features,
        active_tab=active_tab,
        active_category=category,
        total_count=len(filtered),
        feature_count=len(features),
        new_count=int(meta.get("new_articles", 0) or 0),
        max_articles=int(meta.get("max_articles", 100) or 100),
        fetched_at=data.get("fetched_at", ""),
        cache_message=data.get("message", ""),
        cache_path=data.get("cache_path"),
        cache_origin=data.get("cache_origin", "missing"),
        site_categories=data.get("categories", []),
    )


@news_bp.route("/news/features/<slug>")
def feature_detail(slug: str):
    lang = get_current_lang()
    t = get_translations(lang)
    feature = get_feature_article(slug)
    if not feature:
        abort(404)

    return render_template(
        "news/feature_detail.html",
        lang=lang,
        t=t,
        feature=feature,
    )


@news_bp.route("/news/features/<slug>/raw")
def feature_raw(slug: str):
    feature = get_feature_article(slug)
    if not feature:
        abort(404)
    return Response(
        feature["path"].read_text(encoding="utf-8"),
        content_type="text/html; charset=utf-8",
    )


@news_bp.route("/news/features/<slug>/cover")
def feature_cover(slug: str):
    feature = get_feature_article(slug)
    if not feature or not feature.get("image_path"):
        abort(404)
    return send_file(feature["image_path"])


@news_bp.route("/news/items/<article_id>")
def detail(article_id: str):
    lang = get_current_lang()
    t = get_translations(lang)
    data = load_news_data()
    article = data.get("article_map", {}).get(article_id)
    if not article:
        abort(404)

    related = [
        item
        for item in data.get("articles", [])
        if item.get("id") != article_id and item.get("category") == article.get("category")
    ][:4]

    return render_template(
        "news/detail.html",
        lang=lang,
        t=t,
        article=article,
        related=related,
        active_category=article.get("category", ""),
    )
