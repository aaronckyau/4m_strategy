from __future__ import annotations

from flask import abort, render_template, request

from blueprints.futunn import futunn_bp
from blueprints.stock.routes import get_current_lang
from services.futunn_service import load_futunn_data
from translations import get_translations


@futunn_bp.route("/futunn")
def home():
    lang = get_current_lang()
    t = get_translations(lang)
    data = load_futunn_data()
    category = (request.args.get("category") or "").strip()
    articles = data.get("articles", [])

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
        active_category=category,
        total_count=len(filtered),
        fetched_at=data.get("fetched_at", ""),
        cache_message=data.get("message", ""),
        cache_path=data.get("cache_path"),
        cache_origin=data.get("cache_origin", "missing"),
        site_categories=data.get("categories", []),
    )


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
