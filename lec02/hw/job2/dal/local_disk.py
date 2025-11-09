import json
import os
import shutil
from pathlib import Path

from fastavro import parse_schema, writer
from job2.config import BASE_DIR


def load_to_stage(stg_dir: str, raw_dir: str) -> tuple[bool, str]:
    """
    Load data from raw to staging directory.

    :param stg_dir: staging directory path
    :param raw_dir: raw directory path
    :return: tuple indicating success status and message
    """
    try:
        print(f"Loading data from {raw_dir} to {stg_dir}")
        source_dir_path = BASE_DIR.joinpath(
            "file_storage", raw_dir.lstrip("/")
        ).absolute()
        target_dir_path = BASE_DIR.joinpath(
            "file_storage", stg_dir.lstrip("/")
        ).absolute()

        if not source_dir_path.exists():
            return False, f"Source directory {source_dir_path} does not exist."

        if target_dir_path.exists():
            shutil.rmtree(target_dir_path)
        os.makedirs(target_dir_path)

        target_file_path = target_dir_path.joinpath(
            f"sales_{stg_dir.split('/')[-1]}.avro"
        )

        save_avro(source_dir_path, target_file_path)
        return True, f"Data successfully loaded to staging directory {target_dir_path}."
    except Exception as e:
        return False, f"Error loading data to staging directory: {e}"


def save_avro(source_dir_path: Path, target_file_path: Path) -> None:
    """
    Save data from source directory to target AVRO file.

    :param source_dir_path: Path to the source directory
    :param target_file_path: Path to the target AVRO file
    """
    print(f"Saving AVRO file to {target_file_path}")
    schema = {
        "type": "record",
        "name": "Sales",
        "fields": [
            {"name": "client", "type": "string"},
            {"name": "purchase_date", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "price", "type": "int"},
        ],
    }
    parsed_schema = parse_schema(schema)
    records = []
    for file_path in source_dir_path.glob("*.json"):
        with open(file_path, "r") as f:
            data = json.load(f)
            records.extend(data)
    print(f"Total records to write: {len(records)}")
    with open(target_file_path, "wb") as out:
        writer(out, parsed_schema, records)
