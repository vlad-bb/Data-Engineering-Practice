"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""

from flask import Flask, request
from flask import typing as flask_typing
from job2.config import AUTH_TOKEN, BASE_DIR, PORT
from job2.dal.local_disk import load_to_stage

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")


app = Flask(__name__)


def validate_path(path_str: str) -> bool:
    """Basic path validation to prevent directory traversal"""
    if not isinstance(path_str, str) or not path_str:
        return False

    new_path = BASE_DIR.joinpath(path_str).resolve()

    if new_path.exists():
        return new_path.is_dir()

    if new_path.suffix:
        return False

    return True


@app.route("/", methods=["POST"])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger data logic layer

    Proposed POST body in JSON:
    {
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    stg_dir = input_data.get("stg_dir")
    raw_dir = input_data.get("raw_dir")

    if not all([validate_path(raw_dir), validate_path(stg_dir)]):
        return {
            "message": "Invalid raw_dir or stg_dir parameter",
        }, 400

    is_success, msg = load_to_stage(stg_dir=stg_dir, raw_dir=raw_dir)
    if is_success is True:
        return {
            "message": msg,
        }, 201
    else:
        return {
            "message": msg,
        }, 500


def run() -> None:
    app.run(debug=False, host="localhost", port=PORT)
