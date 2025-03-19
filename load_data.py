import os
import subprocess
import logging
from fastapi import APIRouter, Depends, HTTPException
from config import verify_auth, get_upload_dir

router = APIRouter()

LOG_FILE = "logs/load_data.log"
os.makedirs("logs", exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@router.post("/load_data/")
async def load_csv_data(auth: bool = Depends(verify_auth)):
    """
    Loads CSV data into ClickHouse tables using clickhouse-client.
    """
    upload_dir = get_upload_dir()

    try:
        # Get list of all CSV files in upload directory
        csv_files = [f for f in os.listdir(upload_dir) if f.endswith(".csv")]

        if not csv_files:
            logging.info("❌ No CSV files found for loading.")
            return {"message": "No CSV files found for loading"}

        for filename in csv_files:
            table_name = os.path.splitext(filename)[0]  # Remove .csv extension
            file_path = os.path.join(upload_dir, filename)

            logging.info(f"📤 Loading {filename} into table {table_name}...")

            load_cmd = f'clickhouse-client -q "INSERT INTO `{table_name}` FORMAT CSVWithNames" < "{file_path}"'
            process = subprocess.run(load_cmd, shell=True, text=True, capture_output=True)

            if process.returncode != 0:
                logging.error(f"❌ Failed to load {filename}: {process.stderr}")
                raise HTTPException(status_code=500, detail=f"Error loading {filename}: {process.stderr}")

            logging.info(f"✅ Successfully loaded {filename} into {table_name}")

        return {"message": "✅ All CSV files loaded successfully"}

    except Exception as e:
        logging.error(f"❌ Error loading CSVs: {e}")
        raise HTTPException(status_code=500, detail=str(e))
