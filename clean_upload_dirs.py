import os
import logging
from fastapi import APIRouter, HTTPException
from clickhouse_handler import get_stale_sessions, delete_upload_dir

router = APIRouter()

# Setup logging
LOG_FILE = "logs/clean_upload_dirs.log"
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger("clean_upload_dirs")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

@router.post("/clean_upload_dirs/")
async def clean_upload_dirs():
    try:
        session_tokens, upload_dir = get_stale_sessions()
        logger.info(f"Found {len(session_tokens)} session(s) to clean up:")
        for token in session_tokens:
            logger.info(f"  - {token}")

        deleted = []
        for token in session_tokens:
            success = delete_upload_dir(upload_dir, token)
            if success:
                deleted.append(token)
                logger.info(f"✅ Deleted: {os.path.join(upload_dir, token)}")
            else:
                logger.warning(f"⚠️ Could not delete: {os.path.join(upload_dir, token)}")

        logger.info(f"Cleanup complete. Deleted sessions: {deleted}")
        return {"deleted_sessions": deleted}

    except Exception as e:
        logger.error(f"❌ Exception during cleanup: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
