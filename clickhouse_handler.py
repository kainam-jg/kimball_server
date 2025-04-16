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

# First load config
try:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
except Exception as e:
    raise RuntimeError(f"Failed to load config.json: {e}")

# Set up configuration variables
CH = config.get("clickhouse", {})
CH_DB = CH.get("database", "default")
CH_HOST = CH.get("host", "localhost")
CH_PORT = CH.get("port", 8123)
CH_USER = CH.get("username", "default")
CH_PASS = CH.get("password", "")
CH_SSL = CH.get("SSL", False)
CH_SSL_CERT = CH.get("cert_file", "")
UPLOAD_DIR = config.get("UPLOAD_DIR", "")

SCHEME = "https" if CH_SSL else "http"
BASE_URL = f"{SCHEME}://{CH_HOST}:{CH_PORT}/"

CLIENT_FLAGS = ["clickhouse-client"]
if CH_USER:
    CLIENT_FLAGS += ["--user", CH_USER]
if CH_PASS:
    CLIENT_FLAGS += ["--password", f"'{CH_PASS}'"]
CLIENT_FLAGS += ["--port", "9440", "--secure"]
CLIENT_FLAGS += ["--host", CH_HOST]

# Define all functions
def run_clickhouse_query(query: str):
    try:
        full_cmd = CLIENT_FLAGS + ["-q", query]
        logger.info(f"Running ClickHouse command: {' '.join(full_cmd)}")
        result = run(full_cmd, text=True, capture_output=True)
        if result.returncode != 0:
            logger.error(f"ClickHouse command failed: {result.stderr}")
            raise Exception(f"ClickHouse command failed: {result.stderr}")
        return result
    except CalledProcessError as e:
        logger.error(f"Subprocess error: {str(e)}")
        raise

def ensure_upload_log_table_exists():
    """
    Check if file_upload_log table exists and create it if it doesn't.
    """
    check_query = f"""
        EXISTS TABLE {CH_DB}.file_upload_log
        FORMAT TabSeparated
    """
    
    try:
        result = run_clickhouse_query(check_query)
        exists = result.stdout.strip() == "1"
        
        if not exists:
            logger.info("file_upload_log table not found. Creating...")
            create_query = """
                CREATE TABLE IF NOT EXISTS file_upload_log (
                    session_token String,
                    start_time DateTime DEFAULT now(),
                    end_time Nullable(DateTime),
                    cleanup_time Nullable(DateTime),
                    table_names Array(String),
                    file_names Array(String)
                ) ENGINE = MergeTree()
                ORDER BY (session_token)
            """
            run_clickhouse_query(create_query)
            logger.info("✅ Successfully created file_upload_log table")
        else:
            logger.info("✅ file_upload_log table exists")
            
    except Exception as e:
        logger.error(f"❌ Failed to check/create file_upload_log table: {e}")
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
    """
    Build query to update log entry with completed upload information
    """
    tables_str = f"[{','.join(f"'{t}'" for t in tables)}]"
    files_str = f"[{','.join(f"'{f}'" for f in filenames)}]"
    return f"""
        ALTER TABLE {CH_DB}.file_upload_log 
        UPDATE 
            end_time = toDateTime('{end_time}'),
            table_names = {tables_str},
            file_names = {files_str}
        WHERE session_token = '{session_token}'
    """


def log_to_clickhouse_query(query: str):
    auth = (CH_USER, CH_PASS) if CH_USER or CH_PASS else None
    try:
        logger.info(f"Executing HTTP query to ClickHouse: {query}")
        verify = CH_SSL_CERT if CH_SSL_CERT else True if CH_SSL else False
        response = requests.post(
            BASE_URL, 
            params={"database": CH_DB}, 
            data=query, 
            auth=auth,
            verify=verify
        )
        response.raise_for_status()
        logger.info("✅ Query executed successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to execute ClickHouse query: {e}")
        raise


def get_stale_sessions():
    """
    Get session tokens for stale upload directories that need cleanup.
    Returns tuple of (list of session tokens, upload directory path)
    """
    if not UPLOAD_DIR:
        raise ValueError("UPLOAD_DIR not defined in config.json")

    query = f"""
        SELECT session_token
        FROM {CH_DB}.file_upload_log
        WHERE end_time IS NOT NULL
        AND cleanup_time IS NULL
        FORMAT TabSeparated
    """
    try:
        result = run_clickhouse_query(query)
        # Parse the result into list of session tokens
        tokens = [token.strip() for token in result.stdout.split('\n') if token.strip()]
        return tokens, UPLOAD_DIR
    except Exception as e:
        logger.error(f"Failed to get stale sessions: {e}")
        raise


def delete_upload_dir(upload_dir: str, session_token: str) -> bool:
    """
    Delete the upload directory for a given session token and update the cleanup status.
    Returns True if successful, False otherwise.
    """
    try:
        dir_path = os.path.join(upload_dir, session_token)
        if os.path.exists(dir_path):
            # Remove the directory and its contents
            for root, dirs, files in os.walk(dir_path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(dir_path)
            
            # Update cleanup status in ClickHouse
            update_query = f"""
                ALTER TABLE {CH_DB}.file_upload_log
                UPDATE cleanup_time = now()
                WHERE session_token = '{session_token}'
            """
            run_clickhouse_query(update_query)
            return True
    except Exception as e:
        logger.error(f"Failed to delete directory for session {session_token}: {e}")
        return False

# Finally, initialize the table
try:
    ensure_upload_log_table_exists()
except Exception as e:
    logger.error(f"Failed to initialize upload_log table: {e}")
    raise RuntimeError(f"Failed to initialize: {e}")