import os
import json
import logging
import requests
from subprocess import run, CalledProcessError

CONFIG_FILE = "config.json"
logger = logging.getLogger("clickhouse_handler")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("logs/clickhouse_handler.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

try:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
except Exception as e:
    raise RuntimeError(f"Failed to load config.json: {e}")

CH = config.get("clickhouse", {})
CH_DB = CH.get("database", "default")
CH_HOST = CH.get("host", "localhost")
CH_PORT = CH.get("port", 8123)
CH_USER = CH.get("username", "default")
CH_PASS = CH.get("password", "")
CH_SSL = CH.get("SSL", False)

SCHEME = "https" if CH_SSL else "http"
BASE_URL = f"{SCHEME}://{CH_HOST}:{CH_PORT}/"

CLIENT_FLAGS = ["clickhouse-client"]
if CH_SSL:
    CLIENT_FLAGS += ["--secure", f"--port", "9440"]
else:
    CLIENT_FLAGS += ["--port", str(CH_PORT)]
CLIENT_FLAGS += ["--host", CH_HOST]
if CH_USER:
    CLIENT_FLAGS += ["--user", CH_USER]
if CH_PASS:
    CLIENT_FLAGS += ["--password", CH_PASS]


def run_clickhouse_query(query: str):
    try:
        full_cmd = CLIENT_FLAGS + ["-q", query]
        logger.info(f"Running ClickHouse command: {' '.join(full_cmd)}")
        result = run(full_cmd, text=True, capture_output=True)
        if result.returncode != 0:
            logger.error(f"ClickHouse command failed: {result.stderr}")
            raise Exception(f"ClickHouse command failed: {result.stderr}")
    except CalledProcessError as e:
        logger.error(f"Subprocess error: {str(e)}")
        raise


def load_csv_into_table(file_path: str, table_name: str):
    load_cmd = f'"INSERT INTO {table_name} FORMAT CSVWithNames" < "{file_path}"'
    full_cmd = CLIENT_FLAGS + ["-q", load_cmd]
    logger.info(f"Loading CSV into {table_name} from {file_path}")
    result = run(" ".join(full_cmd), shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        logger.error(f"Failed to load CSV: {result.stderr}")
        raise Exception(f"CSV load failed: {result.stderr}")


def build_drop_table_query(table_name: str) -> str:
    return f"DROP TABLE IF EXISTS {table_name};"


def build_create_table_query(table_name: str, columns: list) -> str:
    columns_def = ", ".join([f"{col} String" for col in columns])
    return f"CREATE TABLE {table_name} ({columns_def}) ENGINE = MergeTree() ORDER BY tuple();"


def build_log_update_query(end_time: str, tables: list, filenames: list, session_token: str) -> str:
    return f"""
        ALTER TABLE {CH_DB}.file_upload_log 
        UPDATE 
            end_time = toDateTime('{end_time}'),
            table_names = {tables},
            file_names = {filenames}
        WHERE session_token = '{session_token}'
    """


def log_to_clickhouse_query(query: str):
    auth = (CH_USER, CH_PASS) if CH_USER or CH_PASS else None
    try:
        logger.info(f"Executing HTTP query to ClickHouse: {query}")
        response = requests.post(BASE_URL, params={"database": CH_DB}, data=query, auth=auth)
        response.raise_for_status()
        logger.info("✅ Query executed successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to execute ClickHouse query: {e}")
        raise