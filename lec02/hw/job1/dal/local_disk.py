import json
import os
import shutil
from typing import Any, Dict, List

from job1.config import BASE_DIR


def save_to_disk(json_content: List[Dict[str, Any]], path: str, date: str) -> None:
    """
    Save JSON content to disk.
    """
    print(f"Saving data to disk at {path} for date {date}")
    target_dir_path = BASE_DIR.joinpath("file_storage", path.lstrip("/")).absolute()
    if target_dir_path.exists():
        shutil.rmtree(target_dir_path)
    os.makedirs(target_dir_path)
    target_file_path = target_dir_path.joinpath(f"sales_{date}.json")
    with open(target_file_path, "w") as f:
        json.dump(json_content, f, ensure_ascii=False, indent=4)
