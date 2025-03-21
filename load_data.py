import os
import subprocess
import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from config import verify_auth, get_upload_dir

router = APIRouter()

# ✅ Ensure logs directory exists
LOG_FILE = "logs/load_data.log"
os.makedirs("logs", exist_ok=True)

# ✅ Set up a custom logger
logger = logging.getLogger("load_data")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

@router.post("/load_data/")
async def load_csv_data(request: Request, auth: bool = Depends(verify_auth)):
    """
    Loads CSV data into ClickHouse tables using clickhouse-client.
    """
    try:
        # ✅ Log raw request body
        raw_body = await request.body()
        logger.info(f"📥 Raw Request Body: {raw_body.decode('utf-8')}")

        # ✅ Parse the JSON body
        table_data = await request.json()
        logger.info(f"✅ Parsed JSON: {table_data}")

        if not table_data or "groups" not in table_data:
            logger.error("❌ Received invalid or empty JSON payload!")
            raise HTTPException(status_code=400, detail="Empty or invalid JSON payload received.")

        upload_dir = get_upload_dir()

        for group in table_data["groups"]:
            table_name = group["group"]  # ✅ Get the table name from the JSON payload
            files = group.get("files", [])

            for filename in files:
                file_path = os.path.join(upload_dir, filename)
                logger.info(f"📤 Loading {filename} into table {table_name}...")

                # ✅ Construct the load command
                load_cmd = f'clickhouse-client -q "INSERT INTO {table_name} FORMAT CSVWithNames" < "{file_path}"'
                logger.info(f"🛠️ Executing load command: {load_cmd}")

                try:
                    process = subprocess.run(load_cmd, shell=True, text=True, capture_output=True)

                    if process.returncode != 0:
                        logger.error(f"❌ Failed to load {filename}: {process.stderr}")
                        raise HTTPException(status_code=500, detail=f"Error loading {filename}: {process.stderr}")

                    logger.info(f"✅ Successfully loaded {filename} into {table_name}")

                except subprocess.CalledProcessError as e:
                    logger.error(f"❌ Command failed for {filename}: {e}")
                    raise HTTPException(status_code=500, detail=f"Command failed for {filename}: {str(e)}")

        return {"message": "✅ All CSV files loaded successfully"}

    except Exception as e:
        logger.error(f"❌ Unexpected error while loading CSVs: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
