import os
import csv
import logging
from collections import defaultdict
from sse_starlette.sse import EventSourceResponse
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from fastapi import APIRouter, Depends, HTTPException, Request
from config import API_TOKEN, get_upload_dir, verify_auth, is_debug

router = APIRouter()

LOG_FILE = "logs/group_csvs.log"
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

@router.get("/group_csvs_stream/")
async def group_csvs_stream(request: Request):
    token = request.query_params.get("token")
    if token != f"Bearer {API_TOKEN}":
        async def error_event():
            yield {"event": "error", "data": "Invalid API Token"}
        return EventSourceResponse(error_event())

    upload_dir = get_upload_dir()
    files = [f for f in os.listdir(upload_dir) if f.endswith(".csv")]

    if not files:
        async def error_event():
            yield {"event": "error", "data": "No CSV files found"}
        return EventSourceResponse(error_event())

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
            logger.info(f"📥 DEBUG: Final group output:\n{json.dumps(grouped_output, indent=2)}")

        yield {"event": "complete", "data": json.dumps({"groups": grouped_output})}

        # This ensures the stream is closed cleanly
        yield {"event": "done", "data": "✅ All processing complete."}

    return EventSourceResponse(event_generator())
