import os
import subprocess
import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List
from pydantic import BaseModel
from config import verify_auth

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

def remove_bom(text: str) -> str:
    """Remove BOM from a given text if present."""
    return text.lstrip("\ufeff")

class Group(BaseModel):
    group: str
    files: List[str]
    headers: List[str]

class TableData(BaseModel):
    groups: List[Group]

@router.post("/drop_and_create_table/")
async def drop_and_create_table(table_data: TableData, auth: bool = Depends(verify_auth)):
    """
    Drops and recreates tables in ClickHouse based on the provided column definitions.
    Expects JSON payload with table names and column definitions.
    """
    try:
        logger.info("🚀 Received request to drop and create tables.")

        # ✅ Log the full received JSON data
        logger.info(f"📥 Received JSON Payload: {table_data.dict()}")

        for group in table_data.groups:
            table_name = group.group
            headers = [remove_bom(header) for header in group.headers]

            # ✅ Log cleaned header names
            logger.info(f"🧹 Cleaned header names for table {table_name}: {headers}")

            # ✅ Construct the column definition string (defaulting to String for simplicity)
            columns_def = ", ".join([f"`{header}` String" for header in headers])

            # ✅ Generate DROP TABLE statement
            drop_query = f"DROP TABLE IF EXISTS `{table_name}`;"
            logger.info(f"🗑️ Generated DROP TABLE query: {drop_query}")

            # ✅ Generate CREATE TABLE statement
            create_query = f"CREATE TABLE `{table_name}` ({columns_def}) ENGINE = MergeTree() ORDER BY tuple();"
            logger.info(f"🛠️ Generated CREATE TABLE query: {create_query}")

            # ✅ Log the prepared commands (commented out execution for now)
            try:
                # Uncomment these lines to execute the commands
                subprocess.run(["clickhouse-client", "-q", drop_query], check=True, shell=False)
                logger.info(f"✅ Successfully dropped table {table_name}")

                subprocess.run(["clickhouse-client", "-q", create_query], check=True, shell=False)
                logger.info(f"✅ Successfully created table {table_name}")

                logger.info(f"✔️ Successfully generated ClickHouse commands for table {table_name}")

            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Failed to execute ClickHouse command for {table_name}: {e}")
                raise HTTPException(status_code=500, detail=f"Error processing table {table_name}")

    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    logger.info("🎉 All tables successfully processed (commands generated and logged).")
    return {"message": "Tables processed and commands logged successfully."}
