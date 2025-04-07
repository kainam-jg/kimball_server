from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from upload_handler import router as upload_router
from group_csvs import router as group_router
from create_and_load_tables import router as combined_router
from clean_upload_dirs import router as cleanup_router

import asyncio
import json
from fastapi.testclient import TestClient

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

# Background task to call cleanup API internally
@app.on_event("startup")
async def schedule_cleanup():
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
        interval = config.get("cleanup_interval_minutes", 10)
    except Exception as e:
        print(f"Failed to read config for cleanup schedule: {e}")
        interval = 10

    client = TestClient(app)

    async def call_cleanup_loop():
        await asyncio.sleep(5)  # Give FastAPI time to finish startup
        while True:
            try:
                print("🔄 Calling cleanup internally...")
                response = client.post("/internal/clean_upload_dirs/")
                if response.status_code == 200:
                    print("✅ Cleanup successful.")
                else:
                    print(f"❌ Cleanup failed: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"Exception during scheduled cleanup: {e}")
            await asyncio.sleep(interval * 60)

    asyncio.create_task(call_cleanup_loop())
