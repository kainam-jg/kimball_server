from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from upload_handler import router as upload_router
from group_csvs import router as group_router
from create_and_load_tables import router as combined_router
from clean_upload_dirs import router as cleanup_router

import asyncio
import json
from fastapi.testclient import TestClient
import logging
import os

app = FastAPI(title="FastAPI CSV Processing", description="Handles CSV uploads, grouping, and analysis.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload_router, prefix="/upload", tags=["Upload"])
app.include_router(group_router, prefix="/csv", tags=["CSV Processing"])
app.include_router(combined_router, prefix="/csv", tags=["Create and Load Tables"])
app.include_router(cleanup_router, prefix="/internal", tags=["Cleanup"])

# Setup logging for main app
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("logs/main.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

@app.on_event("startup")
async def schedule_cleanup():
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
        interval = config.get("cleanup_interval_minutes", 10)
        logger.info(f"Scheduled cleanup interval set to {interval} minutes")
    except Exception as e:
        logger.error(f"Failed to read config for cleanup schedule: {e}")
        interval = 10
        logger.warning(f"Using default cleanup interval of {interval} minutes")

    client = TestClient(app)

    async def call_cleanup_loop():
        await asyncio.sleep(5)  # Give FastAPI time to finish startup
        logger.info("Starting cleanup schedule loop")
        while True:
            try:
                logger.info("🔄 Initiating scheduled cleanup...")
                response = client.post("/internal/clean_upload_dirs/")
                if response.status_code == 200:
                    result = response.json()
                    deleted = result.get("deleted_sessions", [])
                    logger.info(f"✅ Cleanup successful. Deleted {len(deleted)} sessions")
                else:
                    logger.error(f"❌ Cleanup failed: {response.status_code} - {response.text}")
            except Exception as e:
                logger.error(f"Exception during scheduled cleanup: {e}")
            await asyncio.sleep(interval * 60)

    asyncio.create_task(call_cleanup_loop())
