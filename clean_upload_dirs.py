import os
import json
import shutil
from datetime import datetime, timedelta
from clickhouse_driver import Client

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
CLICKHOUSE_PORT = CH_CONFIG.get("port", 9000)  # Native protocol port
CLICKHOUSE_USER = CH_CONFIG.get("username", "default")
CLICKHOUSE_PASS = CH_CONFIG.get("password", "")

if not UPLOAD_DIR:
    raise ValueError("UPLOAD_DIR must be defined in config.json")

# Connect to ClickHouse using native protocol
try:
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        database=CLICKHOUSE_DB
    )
except Exception as e:
    raise RuntimeError(f"Failed to connect to ClickHouse: {e}")

# Query for session tokens with completed uploads
query = """
    SELECT DISTINCT session_token
    FROM file_upload_log
    WHERE end_time IS NOT NULL
      AND end_time <= now() - INTERVAL 1 MINUTE
"""

try:
    rows = client.execute(query)
    session_tokens = [row[0] for row in rows]
    print(f"Found {len(session_tokens)} session(s) to clean up:")
    for token in session_tokens:
        print(f"  - {token}")
except Exception as e:
    raise RuntimeError(f"Query failed: {e}")

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