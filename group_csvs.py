import os
import csv
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import APIRouter, Depends, HTTPException, Query
from config import get_upload_dir, verify_auth, is_debug

router = APIRouter()

# Use the same log file as upload_handler.py
LOG_FILE = "logs/upload.log"
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger("group_csvs")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

BATCH_SIZE = 1_000_000
MAX_WORKERS = 4

def get_headers(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = tuple(next(reader))
            row_count = sum(1 for _ in reader)
        logger.info(f"✅ Extracted headers from {file_path} with {row_count} rows")
        return headers, row_count
    except Exception as e:
        logger.error(f"❌ Error reading {file_path}: {e}")
        return None, 0

@router.get("/group_csvs/")
async def group_csvs(
    session_token: str = Query(...),
    auth: bool = Depends(verify_auth)
):
    """
    Groups CSV files in the session-specific upload directory based on matching headers.
    """
    upload_dir = get_upload_dir(session_token)
    if not os.path.exists(upload_dir):
        logger.error(f"❌ Upload directory for session {session_token} not found")
        raise HTTPException(status_code=404, detail=f"Upload directory for session {session_token} not found")

    files = [f for f in os.listdir(upload_dir) if f.endswith(".csv")]

    if not files:
        logger.error("❌ No CSV files found in the session upload directory")
        raise HTTPException(status_code=404, detail="No CSV files found")

    grouped_files = defaultdict(list)
    total_row_count = defaultdict(int)

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(get_headers, os.path.join(upload_dir, file)): file
                for file in files
            }

            for future in as_completed(futures):
                file = futures[future]
                try:
                    headers, row_count = future.result()
                    if headers:
                        grouped_files[headers].append(file)
                        total_row_count[headers] += row_count
                    else:
                        logger.error(f"❌ Failed to get headers for {file}")
                except Exception as e:
                    logger.error(f"❌ Error processing file {file}: {e}")

        grouped_output = []
        for i, (headers, files) in enumerate(grouped_files.items()):
            group_name = f"filegroup_{i+1}"
            grouped_output.append({
                "group": group_name,
                "files": files,
                "headers": list(headers),
                "total_row_count": total_row_count[headers]
            })

        if is_debug():
            logger.info(f"📥 DEBUG: Grouped Output: {grouped_output}")

        logger.info("✅ CSV files grouped successfully")
        return {"groups": grouped_output}

    except Exception as e:
        logger.error(f"❌ Error during CSV grouping: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
