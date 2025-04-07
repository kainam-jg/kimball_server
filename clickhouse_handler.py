import os
import json
import shutil
import logging
import requests
from typing import List, Tuple

# Load config
CONFIG_FILE = "config.json"

try:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
except Exception as e:
    raise RuntimeError(f"Failed to load config.json: {e}")

UPLOAD_DIR = config.get("UPLOAD_DIR")
CH_CONFIG = config.get("clickhouse", {})
CLICKHOUSE_DB = CH_CONFIG.get("database", "default")
CLICKHOUSE_HOST = CH_CONFIG.get("host", "localhost")
CLICKHOUSE_PORT = CH_CONFIG.get("port", 8123)
CLICKHOUSE_USER = CH_CONFIG.get("username", "default")
CLICKHOUSE_PASS = CH_CONFIG.get("password", "")
USE_SSL = config.get("SSL", False)

# Setup internal logger
logger = logging.getLogger("clickhouse_handler")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("logs/clickhouse_handler.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def get_stale_sessions() -> Tuple[List[str], str]:
    """Query ClickHouse to find sessions with expired end_time."""
    protocol = "https" if USE_SSL else "http"
    url = f"{protocol}://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    query = f"""
        SELECT DISTINCT session_token
        FROM {CLICKHOUSE_DB}.file_upload_log
        WHERE end_time IS NOT NULL
          AND end_time <= now() - INTERVAL 1 MINUTE
    """

    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASS) if CLICKHOUSE_USER or CLICKHOUSE_PASS else None

    try:
        response = requests.post(url, params={"database": CLICKHOUSE_DB}, data=query, auth=auth)
        response.raise_for_status()
        session_tokens = [line.strip() for line in response.text.split("\n") if line.strip()]
        return session_tokens, UPLOAD_DIR
    except Exception as e:
        logger.error(f"❌ Failed to query ClickHouse: {e}")
        raise

def delete_upload_dir(base_dir: str, session_token: str) -> bool:
    """Safely delete a session's upload directory."""
    path = os.path.join(base_dir, session_token)
    if os.path.isdir(path):
        try:
            shutil.rmtree(path)
            return True
        except Exception as e:
            logger.error(f"❌ Failed to delete {path}: {e}")
            return False
    else:
        logger.warning(f"⚠️ Not found or not a directory: {path}")
        return False