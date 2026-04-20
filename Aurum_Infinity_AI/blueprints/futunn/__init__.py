from flask import Blueprint

futunn_bp = Blueprint("futunn", __name__)

from blueprints.futunn import routes  # noqa: F401, E402
