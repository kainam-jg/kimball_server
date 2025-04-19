import os
import json
import shutil
import clickhouse_connect

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
CLICKHOUSE_PORT = CH_CONFIG.get("https_port", 8443)
CLICKHOUSE_USER = CH_CONFIG.get("username", "default")
CLICKHOUSE_PASS = CH_CONFIG.get("password", "")
CLICKHOUSE_CERT = CH_CONFIG.get("cert_file", "")

try:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        database=CLICKHOUSE_DB,
        secure=True,
        verify=CLICKHOUSE_CERT
    )
    
    qry = f"SELECT DISTINCT session_token FROM file_upload_log WHERE end_time IS NOT NULL AND end_time <= now() - INTERVAL 1 MINUTE"
    result = client.query(qry)
    for row in result.result_rows:
        user_dir = row[0]
        user_dir_path = os.path.join(UPLOAD_DIR, user_dir)
        if os.path.exists(user_dir_path):
            shutil.rmtree(user_dir_path)
            print(f"Deleted directory: {user_dir_path}")
        else:
            print(f"Directory not found: {user_dir_path}")
    
except Exception as e:
    print("Connection failed:", str(e))
