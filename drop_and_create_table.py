import os
import subprocess
import logging
from fastapi import APIRouter, Depends, HTTPException
from typing import Dict
from config import verify_auth, is_debug  # ✅ Import is_debug()

router = APIRouter()

# ✅ Ensure logs directory exists
LOG_FILE = "logs/drop_and_create_table.log"
os.makedirs("logs", exist_ok=True)

# ✅ Set up logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@router.post("/drop_and_create_table/")
async def drop_and_create_table(table_data: Dict[str, Dict], auth: bool = Depends(verify_auth)):
    """
    Drops and recreates tables in ClickHouse based on the provided column definitions.
    Expects JSON payload with table names and column definitions.
    """
    logging.info("🚀 Received request to drop and create tables.")

    if is_debug():
        logging.info(f"📥 DEBUG: Received JSON Payload: {table_data}")

    if not table_data:
        logging.error("❌ Received empty JSON payload!")
        raise HTTPException(status_code=400, detail="Empty JSON payload received.")

    for table_name, details in table_data.items():
        columns = details.get("columns")

        if not table_name or not columns:
            logging.error(f"❌ Missing required fields for table {table_name}: {details}")
            raise HTTPException(status_code=400, detail=f"Missing required fields for table {table_name}")

        # ✅ Construct column definition string
        columns_def = ", ".join([f"`{col}` {dtype}" for col, dtype in columns.items()])

        # ✅ Drop Table Query
        drop_query = f"DROP TABLE IF EXISTS `{table_name}`;"

        # ✅ Create Table Query
        create_query = f"CREATE TABLE `{table_name}` ({columns_def}) ENGINE = MergeTree() ORDER BY tuple();"

        try:
            # ✅ Execute DROP TABLE
            logging.info(f"🗑️ Dropping table {table_name}: {drop_query}")
            subprocess.run(["clickhouse-client", "-q", drop_query], check=True, shell=False)
            logging.info(f"✅ Successfully dropped table {table_name}")

            # ✅ Execute CREATE TABLE
            logging.info(f"🛠️ Creating table {table_name}: {create_query}")
            subprocess.run(["clickhouse-client", "-q", create_query], check=True, shell=False)
            logging.info(f"✅ Successfully created table {table_name}")

        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Failed to execute ClickHouse command for {table_name}: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing table {table_name}")

    logging.info("🎉 All tables successfully dropped and recreated.")
    return {"message": "Tables successfully dropped and recreated."}
