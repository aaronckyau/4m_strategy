from flask import Blueprint

news_radar_bp = Blueprint('news_radar', __name__)

from blueprints.news_radar import routes  # noqa: F401, E402
