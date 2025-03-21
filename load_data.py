import os
import subprocess
import logging
from fastapi import APIRouter, Depends, HTTPException
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
async def load_csv_data(auth: bool = Depends(verify_auth)):
    """
    Loads CSV data into ClickHouse tables using clickhouse-client.
    """
    upload_dir = get_upload_dir()

    try:
        # ✅ Get list of all CSV files in upload directory
        csv_files = [f for f in os.listdir(upload_dir) if f.endswith(".csv")]

        if not csv_files:
            logger.info("❌ No CSV files found for loading.")
            return {"message": "No CSV files found for loading"}

        for filename in csv_files:
            table_name = os.path.splitext(filename)[0]  # Remove .csv extension
            file_path = os.path.join(upload_dir, filename)

            logger.info(f"📤 Loading {filename} into table {table_name}...")

            # ✅ Construct the load command
            load_cmd = f'clickhouse-client -q "INSERT INTO `{table_name}` FORMAT CSVWithNames" < "{file_path}"'
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
