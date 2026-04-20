from flask import Blueprint

trending_bp = Blueprint('trending', __name__)

from blueprints.trending import routes  # noqa: F401, E402
