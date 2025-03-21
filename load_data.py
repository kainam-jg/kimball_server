import os
import subprocess
import logging
from fastapi import APIRouter, Depends, HTTPException
from config import verify_auth, get_upload_dir, is_debug

router = APIRouter()

LOG_FILE = "logs/load_data.log"
os.makedirs("logs", exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@router.post("/load_data/")
async def load_csv_data(table_data: dict, auth: bool = Depends(verify_auth)):
    """
    Loads CSV data into ClickHouse tables using clickhouse-client.
    """
    try:
        # Log the raw request body
        if is_debug():
            logging.info(f"📥 Raw Request Body: {table_data}")

        if not table_data or "groups" not in table_data:
            logging.error("❌ Received invalid or empty JSON payload!")
            raise HTTPException(status_code=400, detail="Empty or invalid JSON payload received.")

        upload_dir = get_upload_dir()

        for group in table_data["groups"]:
            table_name = group["group"]  # ✅ Correctly using the group name from JSON
            files = group["files"]

            for filename in files:
                file_path = os.path.join(upload_dir, filename)

                logging.info(f"📤 Loading {filename} into table {table_name}...")

                # Construct the ClickHouse command
                load_cmd = f'clickhouse-client -q "INSERT INTO {table_name} FORMAT CSVWithNames" < "{file_path}"'
                logging.info(f"🛠️ Executing load command: {load_cmd}")

                process = subprocess.run(load_cmd, shell=True, text=True, capture_output=True)

                if process.returncode != 0:
                    logging.error(f"❌ Failed to load {filename}: {process.stderr}")
                    raise HTTPException(status_code=500, detail=f"Error loading {filename}: {process.stderr}")

                logging.info(f"✅ Successfully loaded {filename} into {table_name}")

        return {"message": "✅ All CSV files loaded successfully"}

    except Exception as e:
        logging.error(f"❌ Unexpected error while loading CSVs: {e}")
        raise HTTPException(status_code=500, detail=str(e))
