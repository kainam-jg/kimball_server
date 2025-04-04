import os
import shutil
import uuid
import logging
from datetime import datetime
from fastapi import APIRouter, File, UploadFile, Form, HTTPException, Depends
from config import (
    get_upload_dir,
    get_chunk_dir,
    verify_auth,
    is_debug,
    log_to_clickhouse
)

router = APIRouter()
LOG_FILE = "logs/upload.log"

os.makedirs("logs", exist_ok=True)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@router.post("/initialize_upload/")
async def initialize_upload(file: UploadFile = File(...), auth: bool = Depends(verify_auth)):
    session_token = str(uuid.uuid4())
    session_base_dir = os.path.join(get_upload_dir(), session_token)
    chunks_dir = os.path.join(session_base_dir, "chunks")

    try:
        os.makedirs(chunks_dir, exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not create directories: {e}")

    file_path = os.path.join(session_base_dir, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())

    start_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    log_query = f"""
        INSERT INTO default.file_upload_log (session_token, file_name, start_time)
        VALUES ('{session_token}', '{file.filename}', toDateTime('{start_time}'))
    """
    log_to_clickhouse(log_query)

    logging.info(f"🆕 Initialized session {session_token} and saved file {file.filename}")
    return {
        "session_token": session_token,
        "filename": file.filename,
        "message": "Upload session initialized"
    }

@router.post("/upload_chunk/")
async def upload_chunk(
    file: UploadFile = File(...), 
    chunk_number: int = Form(...), 
    total_chunks: int = Form(...), 
    filename: str = Form(...),
    session_token: str = Form(...),
    auth: bool = Depends(verify_auth)
):
    chunk_path = os.path.join(get_chunk_dir(session_token), f"{filename}.part{chunk_number}")

    with open(chunk_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    if is_debug():
        logging.info(f"📥 DEBUG: Received chunk {chunk_number}/{total_chunks} for {filename} in session {session_token}")

    logging.info(f"✅ Uploaded chunk {chunk_number}/{total_chunks} for {filename} in session {session_token}")
    return {"message": f"Chunk {chunk_number}/{total_chunks} uploaded successfully"}

@router.post("/finalize_upload/")
async def finalize_upload(
    filename: str = Form(...),
    total_chunks: int = Form(...),
    session_token: str = Form(...),
    auth: bool = Depends(verify_auth)
):
    final_path = os.path.join(get_upload_dir(session_token), filename)

    with open(final_path, "wb") as final_file:
        for i in range(1, total_chunks + 1):
            chunk_path = os.path.join(get_chunk_dir(session_token), f"{filename}.part{i}")
            if not os.path.exists(chunk_path):
                logging.error(f"❌ Missing chunk {i} for {filename} in session {session_token}")
                raise HTTPException(status_code=400, detail=f"Missing chunk {i}")

            with open(chunk_path, "rb") as chunk_file:
                shutil.copyfileobj(chunk_file, final_file)

            os.remove(chunk_path)

    end_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    log_query = f"""
        ALTER TABLE default.file_upload_log
        UPDATE end_time = toDateTime('{end_time}')
        WHERE session_token = '{session_token}' AND file_name = '{filename}'
    """
    log_to_clickhouse(log_query)

    logging.info(f"✅ Successfully merged {filename} in session {session_token}")
    return {"message": f"File '{filename}' successfully uploaded and merged"}
