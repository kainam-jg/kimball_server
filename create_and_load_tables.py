import os
import logging
from datetime import datetime
from typing import List
from collections import defaultdict
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from config import verify_auth, get_upload_dir
from clickhouse_handler import run_clickhouse_query, load_csv_into_table, build_create_table_query, build_drop_table_query, build_log_update_query, log_to_clickhouse_query

router = APIRouter()

LOG_FILE = "logs/create_and_load_tables.log"
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger("create_and_load_tables")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class Group(BaseModel):
    group: str
    files: List[str]
    headers: List[str]

class TableData(BaseModel):
    session_token: str
    groups: List[Group]

@router.post("/create_and_load_tables/")
async def create_and_load_tables(data: TableData, auth: bool = Depends(verify_auth)):
    try:
        session_token = data.session_token
        upload_dir = get_upload_dir(session_token)
        logger.info(f"🚀 Starting processing for session {session_token}")

        file_to_tables = defaultdict(list)

        for group in data.groups:
            table_name = group.group
            headers = [h.lstrip("\ufeff") for h in group.headers]  # remove BOM

            try:
                run_clickhouse_query(build_drop_table_query(table_name))
                logger.info(f"🗑️ Dropped table {table_name}")

                run_clickhouse_query(build_create_table_query(table_name, headers))
                logger.info(f"🛠️ Created table {table_name} with columns: {headers}")
            except Exception as e:
                logger.error(f"❌ Error creating table {table_name}: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to create table {table_name}")

            for filename in group.files:
                file_path = os.path.join(upload_dir, filename)
                if not os.path.exists(file_path):
                    logger.error(f"❌ File not found: {file_path}")
                    raise HTTPException(status_code=404, detail=f"File not found: {filename}")

                try:
                    load_csv_into_table(file_path, table_name)
                    logger.info(f"✅ Successfully loaded {filename} into {table_name}")
                    file_to_tables[filename].append(table_name)
                except Exception as e:
                    logger.error(f"❌ Failed to load {filename}: {e}")
                    raise HTTPException(status_code=500, detail=f"Error loading {filename}: {e}")

        tables = []
        filenames = []
        end_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        for filename, table_names in file_to_tables.items():
            unique_tables = list(set(table_names))
            tables.append(unique_tables[0])
            filenames.append(filename)

        logger.info(f"{tables}")
        logger.info(f"{filenames}")

        update_query = build_log_update_query(end_time, tables, filenames, session_token)
        log_to_clickhouse_query(update_query)
        logger.info(f"🕒 Logged end_time and table_name for session {session_token} in ClickHouse log")
        return {"message": "✅ All tables created and data loaded successfully."}

    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
