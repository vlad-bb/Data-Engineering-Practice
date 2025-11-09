"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""

import re

from flask import Flask, request
from flask import typing as flask_typing
from job1.bll.sales_api import save_sales_to_local_disk
from job1.config import AUTH_TOKEN, BASE_DIR, PORT

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")


app = Flask(__name__)


def validate_date(date_str: str) -> bool:
    """Simple date validation YYYY-MM-DD"""
    pattern = r"^\d{4}-\d{2}-\d{2}$"
    return bool(re.match(pattern, date_str))


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
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "date": "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    date = input_data.get("date")
    raw_dir = input_data.get("raw_dir")

    if validate_date(date) is False:
        return {
            "message": "Invalid date parameter, expected format YYYY-MM-DD",
        }, 400

    if validate_path(raw_dir) is False:
        return {
            "message": "Invalid raw_dir parameter",
        }, 400

    is_success, msg = save_sales_to_local_disk(date=date, raw_dir=raw_dir)
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
