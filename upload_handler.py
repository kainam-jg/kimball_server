import os
import shutil
import logging
from fastapi import APIRouter, File, UploadFile, Form, HTTPException, Depends
from config import get_upload_dir, get_chunk_dir, verify_auth, is_debug  # ✅ Import is_debug()

router = APIRouter()
LOG_FILE = "logs/upload.log"

# ✅ Ensure log directory exists
os.makedirs("logs", exist_ok=True)

# ✅ Set up logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@router.post("/upload_chunk/")
async def upload_chunk(
    file: UploadFile = File(...), 
    chunk_number: int = Form(...), 
    total_chunks: int = Form(...), 
    filename: str = Form(...),
    auth: bool = Depends(verify_auth)
):
    """Handles chunked CSV uploads."""
    chunk_path = os.path.join(get_chunk_dir(), f"{filename}.part{chunk_number}")

    with open(chunk_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    if is_debug():
        logging.info(f"📥 DEBUG: Received chunk {chunk_number}/{total_chunks} for {filename}")

    logging.info(f"✅ Uploaded chunk {chunk_number}/{total_chunks} for {filename}")
    return {"message": f"Chunk {chunk_number}/{total_chunks} uploaded successfully"}


@router.post("/finalize_upload/")
async def finalize_upload(filename: str = Form(...), total_chunks: int = Form(...), auth: bool = Depends(verify_auth)):
    """Merges uploaded CSV chunks into a single file."""
    final_path = os.path.join(get_upload_dir(), filename)

    with open(final_path, "wb") as final_file:
        for i in range(1, total_chunks + 1):
            chunk_path = os.path.join(get_chunk_dir(), f"{filename}.part{i}")
            if not os.path.exists(chunk_path):
                logging.error(f"❌ Missing chunk {i} for {filename}")
                raise HTTPException(status_code=400, detail=f"Missing chunk {i}")

            with open(chunk_path, "rb") as chunk_file:
                shutil.copyfileobj(chunk_file, final_file)

            os.remove(chunk_path)  # ✅ Remove chunk after merging

    if is_debug():
        logging.info(f"📥 DEBUG: Successfully merged {filename}")

    logging.info(f"✅ Successfully merged {filename}")
    return {"message": f"File '{filename}' successfully uploaded and merged"}
