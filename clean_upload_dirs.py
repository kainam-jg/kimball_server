import os
import json
import shutil
import requests
import logging
from fastapi import APIRouter, HTTPException

router = APIRouter()

# Setup logging
LOG_FILE = "logs/clean_upload_dirs.log"
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger("clean_upload_dirs")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

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

@router.post("/clean_upload_dirs/")
async def clean_upload_dirs():
    if not UPLOAD_DIR:
        logger.error("UPLOAD_DIR not configured")
        raise HTTPException(status_code=500, detail="UPLOAD_DIR not configured")

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
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
        logger.info(f"Found {len(session_tokens)} session(s) to clean up:")
        for token in session_tokens:
            logger.info(f"  - {token}")
    except Exception as e:
        logger.error(f"ClickHouse query failed: {e}")
        raise HTTPException(status_code=500, detail=f"ClickHouse query failed: {e}")

    deleted = []
    for token in session_tokens:
        session_path = os.path.join(UPLOAD_DIR, token)
        if os.path.isdir(session_path):
            try:
                shutil.rmtree(session_path)
                deleted.append(token)
                logger.info(f"✅ Deleted: {session_path}")
            except Exception as e:
                logger.error(f"❌ Failed to delete {session_path}: {e}")
        else:
            logger.warning(f"⚠️ Not found or not a directory: {session_path}")

    logger.info(f"Cleanup complete. Deleted sessions: {deleted}")
    return {"deleted_sessions": deleted}
