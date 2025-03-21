import os
import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from config import verify_auth

router = APIRouter()

# ✅ Ensure logs directory exists
LOG_FILE = "logs/drop_and_create_table.log"
os.makedirs("logs", exist_ok=True)

# ✅ Set up a custom logger
logger = logging.getLogger("drop_and_create_table")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

@router.post("/drop_and_create_table/")
async def drop_and_create_table(request: Request, auth: bool = Depends(verify_auth)):
    """Receive JSON and log it without processing."""
    try:
        logger.info("🚀 Received request to drop and create tables.")
        
        # Read and log the raw request body
        raw_body = await request.body()
        logger.info(f"📥 Raw Request Body: {raw_body.decode('utf-8')}")
        
        # Try to parse the JSON and log it
        try:
            table_data = await request.json()
            logger.info(f"✅ Parsed JSON: {table_data}")
        except Exception as e:
            logger.error(f"❌ Error parsing JSON: {e}")
            raise HTTPException(status_code=400, detail="Invalid JSON format")

        return {"message": "JSON received and logged successfully"}

    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
