import json
import os
import subprocess
import logging
from fastapi import HTTPException, Header

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
CH = config.get("clickhouse", {})
CH_DB = CH.get("database", "default")
CH_HOST = CH.get("host", "localhost")
CH_PORT = CH.get("port", 8123)
CH_USER = CH.get("username", "default")
CH_PASS = CH.get("password", "")
CH_SSL = CH.get("SSL", False)
CH_SSL_CERT = CH.get("cert_file", "")

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
    """Execute a ClickHouse INSERT or UPDATE using clickhouse-client."""
    try:
        # Escape quotes inside query
        escaped_query = query.replace('"', '\\"')
        cmd = ' '.join([
            'clickhouse-client',
            f"--user {CH_USER}",
            f"--password '{CH_PASS}'",  # Note the single quotes around password
            "--port 9440",
            "--secure",
            f"--host {CH_HOST}",
            "-q",
            f'"{escaped_query}"'  # Quote the query
        ])
        logger.info(f"🛠️ Executing ClickHouse log command: {cmd}")
        process = subprocess.run(cmd, shell=True, text=True, capture_output=True)

        if process.returncode != 0:
            logger.error(f"❌ ClickHouse log failed: {process.stderr}")
        else:
            logger.info(f"✅ ClickHouse log successful. {cmd}")

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ ClickHouse logging exception: {e}")

def load_to_clickhouse(query: str):
    """Execute a ClickHouse INSERT or UPDATE using clickhouse-client."""
    try:
        # Escape quotes inside query
        #escaped_query = query.replace('"', '\\"')
        cmd = ' '.join([
            'clickhouse-client',
            f"--user {CH_USER}",
            f"--password '{CH_PASS}'",  # Note the single quotes around password
            "--port 9440",
            "--secure",
            f"--host {CH_HOST}",
            "--query={query}"
        ])
        logger.info(f"🛠️ Executing ClickHouse load command: {cmd}")
        process = subprocess.run(cmd, shell=True, text=True, capture_output=True)

        if process.returncode != 0:
            logger.error(f"❌ ClickHouse log failed: {process.stderr} {cmd}")
        else:
            logger.info(f"✅ ClickHouse log successful. {cmd}")

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ ClickHouse logging exception: {e}")
