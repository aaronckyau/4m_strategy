from __future__ import annotations

from flask import abort, render_template, request, send_file

from blueprints.futunn import futunn_bp
from blueprints.stock.routes import get_current_lang
from services.feature_article_service import get_feature_article, load_feature_articles
from services.futunn_service import load_futunn_data
from translations import get_translations


@futunn_bp.route("/futunn")
def home():
    lang = get_current_lang()
    t = get_translations(lang)
    data = load_futunn_data()
    meta = data.get("meta", {}) if isinstance(data.get("meta"), dict) else {}
    category = (request.args.get("category") or "").strip()
    active_tab = (request.args.get("tab") or "features").strip()
    if active_tab not in {"features", "briefs"}:
        active_tab = "features"

    articles = data.get("articles", [])
    features = load_feature_articles()

    if category:
        filtered = [article for article in articles if article.get("category") == category]
    else:
        filtered = articles

    return render_template(
        "futunn/index.html",
        lang=lang,
        t=t,
        articles=filtered,
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


@futunn_bp.route("/futunn/features/<slug>")
def feature_detail(slug: str):
    lang = get_current_lang()
    t = get_translations(lang)
    feature = get_feature_article(slug)
    if not feature:
        abort(404)

    return render_template(
        "futunn/feature_detail.html",
        lang=lang,
        t=t,
        feature=feature,
    )


@futunn_bp.route("/futunn/features/<slug>/raw")
def feature_raw(slug: str):
    feature = get_feature_article(slug)
    if not feature:
        abort(404)
    return send_file(feature["path"], mimetype="text/html")


@futunn_bp.route("/futunn/news/<article_id>")
def detail(article_id: str):
    lang = get_current_lang()
    t = get_translations(lang)
    data = load_futunn_data()
    article = data.get("article_map", {}).get(article_id)
    if not article:
        abort(404)

    related = [
        item
        for item in data.get("articles", [])
        if item.get("id") != article_id and item.get("category") == article.get("category")
    ][:4]

    return render_template(
        "futunn/detail.html",
        lang=lang,
        t=t,
        article=article,
        related=related,
        active_category=article.get("category", ""),
    )
