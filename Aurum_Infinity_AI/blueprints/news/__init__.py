from flask import Blueprint

news_bp = Blueprint("news", __name__)

from blueprints.news import routes  # noqa: F401, E402
