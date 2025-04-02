import os
import csv
import logging
from collections import defaultdict
from sse_starlette.sse import EventSourceResponse
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import APIRouter, Depends, HTTPException
from config import get_upload_dir, verify_auth, is_debug

router = APIRouter()

LOG_FILE = "logs/group_csvs.log"
os.makedirs("logs", exist_ok=True)

# ✅ Set up a custom logger
logger = logging.getLogger("group_csvs")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ✅ Set batch size and number of workers
BATCH_SIZE = 1_000_000
MAX_WORKERS = 4

def get_headers(file_path):
    """Extract headers from a CSV file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = tuple(next(reader))  # Extract headers from the first row
            row_count = sum(1 for _ in reader)  # Count the remaining rows
        logger.info(f"✅ Extracted headers from {file_path} with {row_count} rows")
        return headers, row_count
    except Exception as e:
        logger.error(f"❌ Error reading {file_path}: {e}")
        return None, 0

@router.get("/group_csvs/")
async def group_csvs(auth: bool = Depends(verify_auth)):
    """
    Groups CSV files in the upload directory based on matching headers.
    Uses concurrent processing for efficiency.
    """
    upload_dir = get_upload_dir()
    files = [f for f in os.listdir(upload_dir) if f.endswith(".csv")]

    if not files:
        logger.error("❌ No CSV files found in the upload directory")
        raise HTTPException(status_code=404, detail="No CSV files found")

    grouped_files = defaultdict(list)
    total_row_count = defaultdict(int)

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(get_headers, os.path.join(upload_dir, file)): file for file in files}

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

        # ✅ Convert grouped data to JSON-friendly format
        grouped_output = []
        for i, (headers, files) in enumerate(grouped_files.items()):
            group_name = f"filegroup_{i+1}"
            grouped_output.append({
                "group": group_name,
                "files": files,
                "headers": list(headers),
                "total_row_count": total_row_count[headers]  # Add row count for each group
            })

        if is_debug():
            logger.info(f"📥 DEBUG: Grouped Output: {grouped_output}")

        logger.info("✅ CSV files grouped successfully")
        return {"groups": grouped_output}

    except Exception as e:
        logger.error(f"❌ Error during CSV grouping: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@router.get("/group_csvs_stream/")
async def group_csvs_stream(auth: bool = Depends(verify_auth)):
    """
    Streams progress updates while grouping CSVs using Server-Sent Events (SSE).
    """
    upload_dir = get_upload_dir()
    files = [f for f in os.listdir(upload_dir) if f.endswith(".csv")]

    if not files:
        # Note: You cannot yield outside the generator. Handle inside.
        async def error_event():
            yield {"event": "error", "data": "No CSV files found"}
        return EventSourceResponse(error_event)

    grouped_files = defaultdict(list)
    total_row_count = defaultdict(int)

    async def event_generator():
        yield {"event": "start", "data": f"Starting to process {len(files)} files."}

        loop = asyncio.get_event_loop()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                loop.run_in_executor(executor, get_headers, os.path.join(upload_dir, f)): f
                for f in files
            }

            for coro in asyncio.as_completed(futures):
                file = futures[coro]
                try:
                    headers, row_count = await coro
                    if headers:
                        grouped_files[headers].append(file)
                        total_row_count[headers] += row_count
                        yield {"event": "progress", "data": f"✅ Finished: {file}"}
                    else:
                        yield {"event": "progress", "data": f"❌ Failed: {file}"}
                except Exception as e:
                    yield {"event": "progress", "data": f"❌ Error: {file} - {str(e)}"}

        # Format the final JSON payload
        grouped_output = [
            {
                "group": f"filegroup_{i+1}",
                "files": files,
                "headers": list(headers),
                "total_row_count": total_row_count[headers]
            }
            for i, (headers, files) in enumerate(grouped_files.items())
        ]

        if is_debug():
            logger.info(f"📥 DEBUG: Final group output: {grouped_output}")

        yield {"event": "complete", "data": json.dumps({"groups": grouped_output})}

    return EventSourceResponse(event_generator)
