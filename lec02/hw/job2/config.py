import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")
BASE_DIR = Path(__file__).resolve().parent.parent
PORT = 8082
