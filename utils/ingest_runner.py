import psycopg2
from utils.logger import DBLogger
from utils.run_tracker import RunTracker
from utils.schema_detector import detect_schema
from loaders.db_loader import load_to_db

def run_ingestion(connector_func, source, connector_name, *args, option=None, table_name=None):

    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="spark@1234",
        host="localhost",
        port="5432"
    )

    tracker = RunTracker(conn)
    run_id = tracker.start_run(connector_name, source)

    logger = DBLogger(conn, run_id)

    try:
        logger.log("INFO", f"{connector_name} started")

        df = connector_func(*args)
        row_count = df.shape[0]

        logger.log("INFO", f"Fetched {row_count} rows")

        detect_schema(df)
        logger.log("INFO", "Schema detected")

        #  UPDATED LINE
        load_to_db(df, option=option, table_name=table_name)

        logger.log("INFO", "Data loaded to DB")

        tracker.end_run(run_id, "SUCCESS", row_count)

        return {"status": "SUCCESS", "run_id": run_id, "rows": row_count}

    except Exception as e:
        logger.log("ERROR", str(e))
        tracker.end_run(run_id, "FAILED", 0, str(e))

        return {"status": "FAILED", "run_id": run_id, "error": str(e)}

    finally:
        conn.close()