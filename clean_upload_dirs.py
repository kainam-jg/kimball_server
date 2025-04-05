import os
import json
import shutil
import requests
from datetime import datetime, timedelta

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

if not UPLOAD_DIR:
    raise ValueError("UPLOAD_DIR must be defined in config.json")

# Build query URL and payload
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
    print(f"Found {len(session_tokens)} session(s) to clean up:")
    for token in session_tokens:
        print(f"  - {token}")
except Exception as e:
    raise RuntimeError(f"ClickHouse HTTP query failed: {e}")

# Delete directories for each session
for token in session_tokens:
    session_path = os.path.join(UPLOAD_DIR, token)
    if os.path.isdir(session_path):
        try:
            shutil.rmtree(session_path)
            print(f"✅ Deleted: {session_path}")
        except Exception as e:
            print(f"❌ Failed to delete {session_path}: {e}")
    else:
        print(f"⚠️ Not found or not a directory: {session_path}")
