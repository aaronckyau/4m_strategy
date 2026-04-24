from flask import Blueprint

markets_bp = Blueprint("markets", __name__)

from blueprints.markets import routes  # noqa: E402, F401
