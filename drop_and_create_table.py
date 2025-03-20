import os
import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Dict
from config import verify_auth, is_debug

router = APIRouter()

# Ensure logs directory exists
LOG_FILE = "logs/drop_and_create_table.log"
os.makedirs("logs", exist_ok=True)

# Set up a simple logger
logger = logging.getLogger("drop_and_create_table")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

@router.post("/drop_and_create_table/")
async def drop_and_create_table(request: Request, auth: bool = Depends(verify_auth)):
    """
    Receive and log the JSON payload, including raw data and headers.
    """
    try:
        logger.info("🚀 Received request to drop and create tables.")

        # Log the headers
        headers = dict(request.headers)
        logger.info(f"🔍 Request Headers: {headers}")

        # Log the raw body
        try:
            raw_body = await request.body()
            logger.info(f"📥 Raw Request Body: {raw_body.decode('utf-8')}")
        except Exception as e:
            logger.error(f"❌ Failed to read raw body: {e}")

        # Attempt to parse JSON
        try:
            table_data = await request.json()
            logger.info(f"✅ Parsed JSON Payload: {table_data}")
        except Exception as e:
            logger.error(f"❌ Failed to parse JSON: {e}")
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

        # Just return a success message
        return {"message": "Received JSON successfully."}

    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
