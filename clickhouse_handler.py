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
            create_query = '''
                CREATE TABLE IF NOT EXISTS file_upload_log (
                    "session_token" String,
                    "start_time" DateTime DEFAULT now(),
                    "end_time" Nullable(DateTime),
                    "cleanup_time" Nullable(DateTime),
                    "table_names" Array(String),
                    "file_names" Array(String)
                ) ENGINE = MergeTree()
                ORDER BY ("session_token")
            '''
            run_clickhouse_query(create_query)
            logger.info("✅ Successfully created file_upload_log table")
        else:
            logger.info("✅ file_upload_log table exists")
            
    except Exception as e:
        logger.error(f"❌ Failed to check/create file_upload_log table: {e}")
        raise 