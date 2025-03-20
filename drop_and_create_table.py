import os
import subprocess
import logging
from fastapi import APIRouter, Depends, HTTPException
from typing import Dict
from config import verify_auth, is_debug

router = APIRouter()

# ✅ Ensure logs directory exists
LOG_FILE = "logs/drop_and_create_table.log"
os.makedirs("logs", exist_ok=True)

# ✅ Set up a custom logger
logger = logging.getLogger("drop_and_create_table")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def remove_bom(text):
    """Remove BOM from a given text if present."""
    return text.lstrip("\ufeff")

@router.post("/drop_and_create_table/")
async def drop_and_create_table(table_data: Dict[str, Dict], auth: bool = Depends(verify_auth)):
    """Drops and recreates tables in ClickHouse based on the provided column definitions."""
    try:
        logger.info("🚀 Received request to drop and create tables.")

        # ✅ Check and log the full received JSON data
        if is_debug():
            logger.info(f"📥 DEBUG: Full JSON Payload: {table_data}")

        if not table_data:
            logger.error("❌ Received empty JSON payload!")
            raise HTTPException(status_code=400, detail="Empty JSON payload received.")

        for group in table_data.get("groups", []):
            table_name = group.get("group")
            headers = group.get("headers")

            # ✅ Validate table name and headers
            if not table_name or not headers:
                logger.error(f"❌ Missing required fields for table {table_name}: {group}")
                raise HTTPException(status_code=400, detail=f"Missing required fields for table {table_name}")

            # ✅ Clean BOM from header names
            cleaned_headers = [remove_bom(header) for header in headers]
            logger.info(f"🧹 Cleaned header names for table {table_name}: {cleaned_headers}")

            # ✅ Construct column definition string (assuming String type for all columns for simplicity)
            columns_def = ", ".join([f"`{header}` String" for header in cleaned_headers])

            # ✅ Drop Table Query
            drop_query = f"DROP TABLE IF EXISTS `{table_name}`;"

            # ✅ Create Table Query
            create_query = f"CREATE TABLE `{table_name}` ({columns_def}) ENGINE = MergeTree() ORDER BY tuple();"

            try:
                # ✅ Execute DROP TABLE
                logger.info(f"🗑️ Dropping table {table_name} with query: {drop_query}")
                subprocess.run(["clickhouse-client", "-q", drop_query], check=True, shell=False)
                logger.info(f"✅ Successfully dropped table {table_name}")

                # ✅ Execute CREATE TABLE
                logger.info(f"🛠️ Creating table {table_name} with query: {create_query}")
                subprocess.run(["clickhouse-client", "-q", create_query], check=True, shell=False)
                logger.info(f"✅ Successfully created table {table_name}")

            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Failed to execute ClickHouse command for {table_name}: {e}")
                raise HTTPException(status_code=500, detail=f"Error processing table {table_name}")

    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    logger.info("🎉 All tables successfully dropped and recreated.")
    return {"message": "Tables successfully dropped and recreated."}
