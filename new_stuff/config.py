import json
import os
import logging
from fastapi import HTTPException, Header
from clickhouse_handler import log_to_clickhouse_query

CONFIG_FILE = "config.json"
logger = logging.getLogger(__name__)


def load_config():
    """Load configuration from config.json."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise ValueError(f"Error loading configuration: {e}")


config = load_config()
API_TOKEN = config.get("API_TOKEN")
UPLOAD_ROOT = config.get("UPLOAD_DIR", "/tmp/uploads")
DEBUG = config.get("DEBUG", False)
CLICKHOUSE = config.get("clickhouse", {})

if not API_TOKEN or not UPLOAD_ROOT:
    raise ValueError("Missing required configurations in config.json")

# Ensure root directory exists
os.makedirs(UPLOAD_ROOT, exist_ok=True)


def get_upload_dir(session_token: str = None):
    return os.path.join(UPLOAD_ROOT, session_token) if session_token else UPLOAD_ROOT


def get_chunk_dir(session_token: str = None):
    if session_token:
        return os.path.join(UPLOAD_ROOT, session_token, "chunks")
    return os.path.join(UPLOAD_ROOT, "chunks")


def is_debug():
    return DEBUG


def verify_auth(authorization: str = Header(None)):
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=403, detail="Invalid API Token")
    return True


def log_to_clickhouse(query: str):
    """Execute a ClickHouse INSERT or UPDATE using the shared handler."""
    log_to_clickhouse_query(query)