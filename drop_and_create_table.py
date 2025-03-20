import os
import logging
from fastapi import APIRouter, Depends, HTTPException
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
async def drop_and_create_table(table_data: Dict[str, Dict], auth: bool = Depends(verify_auth)):
    """
    Receive and log the JSON payload.
    """
    try:
        logger.info("🚀 Received request to drop and create tables.")

        # Log the received JSON payload
        if table_data:
            logger.info(f"📥 Received JSON Payload: {table_data}")
        else:
            logger.error("❌ Empty JSON payload received!")
            raise HTTPException(status_code=400, detail="Empty JSON payload received.")

        # Just return a success message
        return {"message": "Received JSON successfully."}

    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
