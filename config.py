import json
import os
from fastapi import HTTPException, Header

CONFIG_FILE = "config.json"

def load_config():
    """Load configuration from config.json."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise ValueError(f"Error loading configuration: {e}")

config = load_config()
API_TOKEN = config.get("API_TOKEN")
UPLOAD_DIR = config.get("UPLOAD_DIR", "/tmp/uploads")
CHUNK_DIR = config.get("CHUNK_DIR", "/tmp/uploads/chunks")
DEBUG = config.get("DEBUG", False)  # ✅ Read debug flag from config.json

if not API_TOKEN or not UPLOAD_DIR or not CHUNK_DIR:
    raise ValueError("Missing required configurations in config.json")

# Ensure directories exist
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(CHUNK_DIR, exist_ok=True)

def get_upload_dir():
    """Return upload directory from config."""
    return UPLOAD_DIR

def get_chunk_dir():
    """Return chunk directory from config."""
    return CHUNK_DIR

def is_debug():
    """Return whether debug mode is enabled."""
    return DEBUG

def verify_auth(authorization: str = Header(None)):
    """Verify API authentication."""
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=403, detail="Invalid API Token")
    return True
