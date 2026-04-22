from flask import Blueprint

insider_bp = Blueprint("insider", __name__)

from blueprints.insider import routes  # noqa: E402, F401
