import os
import csv
import logging
from collections import defaultdict
from fastapi import APIRouter, Depends
from config import get_upload_dir, verify_auth, is_debug  # ✅ Import is_debug()

router = APIRouter()
LOG_FILE = "logs/group_csvs.log"

# ✅ Ensure log directory exists
os.makedirs("logs", exist_ok=True)

# ✅ Set up logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@router.get("/group_csvs/")
async def group_csvs(auth: bool = Depends(verify_auth)):
    """Groups CSV files in the upload directory based on matching headers."""
    logging.info("🚀 Received request to group CSV files.")

    upload_dir = get_upload_dir()
    files = [f for f in os.listdir(upload_dir) if f.endswith(".csv")]

    grouped_files = defaultdict(list)

    for file in files:
        file_path = os.path.join(upload_dir, file)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                headers = tuple(next(reader))  # ✅ Extract headers from the first row
                grouped_files[headers].append(file)

        except Exception as e:
            logging.error(f"❌ Error reading {file}: {e}")

    # ✅ Convert dictionary to JSON-friendly format
    grouped_output = [{"group": f"filegroup_{i+1}", "files": files, "headers": list(headers)}
                      for i, (headers, files) in enumerate(grouped_files.items())]

    if is_debug():
        logging.info(f"📤 DEBUG: Grouped CSV JSON Output: {grouped_output}")

    logging.info("✅ CSV files grouped successfully")
    return {"groups": grouped_output}
