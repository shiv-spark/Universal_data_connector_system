from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
import psycopg2
import requests
import threading
import time
from requests.auth import HTTPBasicAuth
from typing import Optional
from utils.dag_generator import create_dag_file, delete_dag_file, list_dag_files

from connectors.csv_connector import csv_connector
from connectors.excel_connector import excel_connector
from connectors.google_sheets_connector import google_sheet_connector
from connectors.api_connector import api_connector
from utils.ingest_runner import run_ingestion
####################
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

EMAIL_SENDER   = "shivay999.ss@gmail.com"
EMAIL_PASSWORD = "nmwq hybd wlrp dwkw"
EMAIL_RECEIVER = "shivansh.s@sparkbrains.ai"
SMTP_HOST      = "smtp.gmail.com"
SMTP_PORT      = 587
def send_failure_email(dag_id: str, run_id: str, error: str = "", status: str = "failed"):
    try:
        msg = MIMEMultipart()
        msg["From"]    = EMAIL_SENDER
        msg["To"]      = EMAIL_RECEIVER
        msg["Subject"] = f"Pipeline {status.upper()}: {dag_id}"

        emoji = "✅" if status == "success" else "❌"
        body = f"""
Pipeline {status.upper()} Alert
──────────────────────────────
{emoji} DAG ID : {dag_id}
   Run ID : {run_id}
   Status : {status.upper()}
   {f'Error  : {error}' if status == "failed" else ''}

Airflow UI: http://localhost:8081
        """
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        print(f"Alert email sent for: {dag_id} — {status.upper()}")
    except Exception as e:
        print(f"Email send failed: {e}")

#################################
app = FastAPI()



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
# ─────────────────────────────────────────────
# GLOBAL CONFIG
# ─────────────────────────────────────────────

AIRFLOW_BASE = "http://localhost:8081/api/v1/dags"
AIRFLOW_AUTH = HTTPBasicAuth("admin", "admin")


DAG_MAP = {
    "csv":           "dynamic_connector_dag",   
    "excel":         "dynamic_connector_dag",
    "api":           "dynamic_connector_dag",
    "google_sheets": "dynamic_connector_dag"
}

OPTION_MAP = {
    "1": "append",
    "2": "overwrite",
    "3": "create_new"
}

# ─────────────────────────────────────────────
# DB CONFIG
# ─────────────────────────────────────────────

DB_CONFIG = {
    "host":     "localhost",
    "database": "postgres",
    "user":     "postgres",
    "password": "spark@1234",
    "port":     "5432"
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# ─────────────────────────────────────────────
# ROOT
# ─────────────────────────────────────────────

@app.get("/")
def root():
    return {"message": "SparkBrains Data Connector API Running"}


# ─────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────

def validate_inputs(option, table_name):
    if option not in ["1", "2", "3"]:
        raise HTTPException(status_code=400, detail="Invalid option. Use 1, 2, or 3")
    if not table_name:
        raise HTTPException(status_code=400, detail="table_name is required")

# ─────────────────────────────────────────────
# CSV
# ─────────────────────────────────────────────

class CSVRequest(BaseModel):
    file_path: str
    option: str
    table_name: str | None = None

@app.post("/ingest_csv")
def ingest_csv(req: CSVRequest):
    return run_ingestion(
        csv_connector,
        req.file_path,
        "CSVConnector",
        req.file_path,
        option=req.option,
        table_name=req.table_name
    )


# ─────────────────────────────────────────────
# EXCEL
# ─────────────────────────────────────────────

class ExcelRequest(BaseModel):
    file_path: str
    option: str
    table_name: str | None = None

@app.post("/ingest_excel")
def ingest_excel(req: ExcelRequest):
    skip = validate_inputs(req.option, req.table_name)
    if skip:
        return skip
    return run_ingestion(
        excel_connector,
        req.file_path,
        "ExcelConnector",
        req.file_path,
        option=req.option,
        table_name=req.table_name
    )


# ─────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────

class GoogleSheetRequest(BaseModel):
    sheet_url: str
    option: str
    table_name: str | None = None

@app.post("/ingest_google_sheet")
def ingest_google_sheet(req: GoogleSheetRequest):
    skip = validate_inputs(req.option, req.table_name)
    if skip:
        return skip
    return run_ingestion(
        google_sheet_connector,
        req.sheet_url,
        "GoogleSheetsConnector",
        req.sheet_url,
        "pandas",
        option=req.option,
        table_name=req.table_name
    )


# ─────────────────────────────────────────────
# API CONNECTOR
# ─────────────────────────────────────────────

class APIRequest(BaseModel):
    url: str
    option: str
    table_name: str | None = None

@app.post("/ingest_api")
def ingest_api(req: APIRequest):
    skip = validate_inputs(req.option, req.table_name)
    if skip:
        return skip
    return run_ingestion(
        api_connector,
        req.url,
        "APIConnector",
        req.url,
        option=req.option,
        table_name=req.table_name
    )


# ─────────────────────────────────────────────
# PIPELINE RUNS & LOGS
# ─────────────────────────────────────────────

@app.get("/runs")
def get_runs():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM pipeline_runs ORDER BY start_time DESC")
    data = cursor.fetchall()
    conn.close()
    return data


@app.get("/logs/{run_id}")
def get_logs(run_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM pipeline_logs WHERE run_id=%s ORDER BY log_time",
        (run_id,)
    )
    data = cursor.fetchall()
    conn.close()
    return data


# ─────────────────────────────────────────────
# INSERT PIPELINE LOG
# ─────────────────────────────────────────────

# def insert_pipeline_log(data):
#     conn = get_conn()
#     cur = conn.cursor()

#     query = """
#     INSERT INTO airflow_pipeline_runs (
#         dag_id, dag_run_id, connector_type, file_path,
#         sheet_url, api_url, operation, table_name,
#         status, execution_date
#     )
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """

#     cur.execute(query, (
#         data.get("dag_id"),
#         data.get("dag_run_id"),
#         data.get("connector_type"),
#         data.get("file_path") or data.get("source_path"),
#         data.get("sheet_url"),
#         data.get("api_url"),
#         data.get("operation"),
#         data.get("table_name"),
#         data.get("status"),
#         data.get("execution_date")
#     ))

#     conn.commit()
#     cur.close()
#     conn.close()

def insert_pipeline_log(data):
    conn = get_conn()
    cur = conn.cursor()
 
    query = """
    INSERT INTO airflow_pipeline_runs (
        dag_id, dag_run_id, pipeline_name,
        connector_type, file_path, folder_path,
        sheet_url, api_url, operation, table_name,
        schedule, status, execution_date, triggered_by
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
 
    cur.execute(query, (
        data.get("dag_id"),
        data.get("dag_run_id"),
        data.get("pipeline_name"),
        data.get("connector_type"),
        data.get("file_path"),
        data.get("folder_path"),
        data.get("sheet_url"),
        data.get("api_url"),
        data.get("operation"),
        data.get("table_name"),
        data.get("schedule"),
        data.get("status"),
        data.get("execution_date"),
        data.get("triggered_by", "manual"),
    ))
 
    conn.commit()
    cur.close()
    conn.close()
# ─────────────────────────────────────────────
# UPDATE STATUS IN DB
# ─────────────────────────────────────────────

def update_status_in_db(dag_run_id, status):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE airflow_pipeline_runs
        SET status = %s
        WHERE dag_run_id = %s
    """, (status, dag_run_id))

    conn.commit()
    cur.close()
    conn.close()


# ─────────────────────────────────────────────
# AUTO STATUS TRACKER — Background Thread
# ─────────────────────────────────────────────

def track_pipeline_status(dag_run_id: str, dag_id: str):
    url = f"{AIRFLOW_BASE}/{dag_id}/dagRuns/{dag_run_id}"

    print(f"🔍 Auto tracking started for: {dag_run_id}")

    max_attempts = 60
    attempt = 0

    while attempt < max_attempts:
        try:
            res = requests.get(url, auth=AIRFLOW_AUTH, timeout=10)

            if res.status_code != 200:
                print(f"⚠️ Airflow API error: {res.status_code}")
                time.sleep(5)
                attempt += 1
                continue

            data   = res.json()
            status = data.get("state")

            print(f"📊 [{dag_run_id}] Status: {status}")

            if status in ["success", "failed"]:
                update_status_in_db(dag_run_id, status.upper())
                print(f"✅ Final status '{status.upper()}' saved for: {dag_run_id}")
                send_failure_email(dag_id=dag_id, run_id=dag_run_id, status=status)
                return

            time.sleep(5)
            attempt += 1

        except Exception as e:
            print(f"❌ Tracking error: {e}")
            time.sleep(5)
            attempt += 1

    update_status_in_db(dag_run_id, "TIMEOUT")
    print(f"⏰ Tracking timeout for: {dag_run_id}")

# ─────────────────────────────────────────────
# MANUAL STATUS CHECK
# ─────────────────────────────────────────────

@app.get("/pipeline_status/{connector_type}/{dag_run_id}")
def get_status(connector_type: str, dag_run_id: str):
    dag_id = DAG_MAP.get(connector_type)
    if not dag_id:
        raise HTTPException(status_code=400, detail=f"Invalid connector_type: {connector_type}")

    url  = f"{AIRFLOW_BASE}/{dag_id}/dagRuns/{dag_run_id}"
    res  = requests.get(url, auth=AIRFLOW_AUTH)
    data = res.json()
    status = data.get("state")
    update_status_in_db(dag_run_id, status)
    return {"dag_id": dag_id, "dag_run_id": dag_run_id, "status": status}


# ─────────────────────────────────────────────
# ALL PIPELINES
# ─────────────────────────────────────────────

@app.get("/all_pipelines")
def get_all():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM airflow_pipeline_runs ORDER BY created_at DESC")
    data = cur.fetchall()
    conn.close()
    return data
 
# ── Request model ────────────────────────────────────────────────────────────

class CreatePipelineRequest(BaseModel):
    pipeline_name:   str
    connector_type:  str
    table_name:      str
    option:          str         = "1"
    after_first_run: Optional[str] = None   # option "3" required — "1" or "2"
    schedule:        str         = "*/5 * * * *"
    folder_path:     Optional[str] = None
    file_path:       Optional[str] = None
    sheet_url:       Optional[str] = None
    api_url:         Optional[str] = None
 
 

 
@app.post("/create_pipeline")
def create_pipeline(req: CreatePipelineRequest):
    result = create_dag_file(req.model_dump())
 
    if result.get("status") == "FAILED":
        raise HTTPException(status_code=400, detail=result)
 
    # ✅ Log pipeline creation to DB
    dag_id = result.get("dag_id")
    insert_pipeline_log({
        "dag_id":         dag_id,
        "dag_run_id":     f"created__{dag_id}",   # placeholder — no real run yet
        "pipeline_name":  dag_id,
        "connector_type": req.connector_type,
        "file_path":      req.file_path,
        "folder_path":    req.folder_path,
        "sheet_url":      req.sheet_url,
        "api_url":        req.api_url,
        "operation":      OPTION_MAP.get(req.option, "unknown"),
        "table_name":     req.table_name,
        "schedule":       req.schedule,
        "status":         "CREATED",
        "execution_date": None,
        "triggered_by":   "create_pipeline",
    })
 
    return result
# ── DELETE /delete_pipeline/{pipeline_name} ──────────────────────────────────
 
@app.delete("/delete_pipeline/{pipeline_name}")
def delete_pipeline(pipeline_name: str):
    """
    Existing DAG file delete karo.
    Example: DELETE /delete_pipeline/hr_data_csv
    """
    result = delete_dag_file(pipeline_name)
 
    if result.get("status") == "FAILED":
        raise HTTPException(status_code=404, detail=result)
 
    return result
 
 
# ── GET /pipelines ────────────────────────────────────────────────────────────
 
@app.get("/pipelines")
def list_pipelines():
    """
    List of all generated pipeline files.
    """
    return {
        "status":    "SUCCESS",
        "pipelines": list_dag_files()
    }

@app.get("/table/{table_name}")
def get_table_data(table_name: str):
    conn = get_conn()
    cursor = conn.cursor()

    try:
        # Table exist check
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (table_name,))
        exists = cursor.fetchone()[0]

        if not exists:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' nahi mili.")

        # Row count
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        count = cursor.fetchone()[0]

        # Poora data
        cursor.execute(f'SELECT * FROM "{table_name}"')
        rows  = cursor.fetchall()
        cols  = [desc[0] for desc in cursor.description]
        data  = [dict(zip(cols, row)) for row in rows]

        return {
            "table":      table_name,
            "row_count":  count,
            "columns":    cols,
            "data":       data
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DAG PAUSE / UNPAUSE
# ─────────────────────────────────────────────────────────────────────────────

@app.patch("/pipeline/{pipeline_name}/pause")
def pause_pipeline(pipeline_name: str):
    """
    DAG pause .
    Example: PATCH /pipeline/hr_analytics_testing/pause
    """
    dag_id = pipeline_name if pipeline_name.startswith("pipeline_") else f"pipeline_{pipeline_name}"

    url    = f"{AIRFLOW_BASE}/{dag_id}"

    res  = requests.patch(url, json={"is_paused": True}, auth=AIRFLOW_AUTH)
    data = res.json()

    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=data)

    return {
        "status":  "PAUSED",
        "dag_id":  dag_id,
        "message": f"Pipeline '{dag_id}' pause ho gayi."
    }


@app.patch("/pipeline/{pipeline_name}/unpause")
def unpause_pipeline(pipeline_name: str):
    """
    DAG unpause  (resume).
    Example: PATCH /pipeline/hr_analytics_testing/unpause
    """
    dag_id = pipeline_name if pipeline_name.startswith("pipeline_") else f"pipeline_{pipeline_name}"
    url    = f"{AIRFLOW_BASE}/{dag_id}"

    res  = requests.patch(url, json={"is_paused": False}, auth=AIRFLOW_AUTH)
    data = res.json()

    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=data)

    return {
        "status":  "ACTIVE",
        "dag_id":  dag_id,
        "message": f"Pipeline '{dag_id}' is active ."
    }


@app.get("/pipeline/{pipeline_name}/status")
def pipeline_status(pipeline_name: str):
    """
    DAG  current status  — paused / active.
    Example: GET /pipeline/hr_analytics_testing/status
    """
    dag_id = pipeline_name if pipeline_name.startswith("pipeline_") else f"pipeline_{pipeline_name}"

    url    = f"{AIRFLOW_BASE}/{dag_id}"

    res  = requests.get(url, auth=AIRFLOW_AUTH)
    data = res.json()

    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=data)

    return {
        "dag_id":    dag_id,
        "is_paused": data.get("is_paused"),
        "status":    "PAUSED" if data.get("is_paused") else "ACTIVE",
        "next_run":  data.get("next_dagrun"),
    }

##### these are helper functions for the connectors and should ideally be in their respective files, but keeping here for now to avoid merge conflicts with recent edits in connectors/google_sheets_connector.py

@app.get("/pipeline/{pipeline_name}/runs")
def get_pipeline_runs(
    pipeline_name: str,
    limit: int = 20,
    offset: int = 0,
    status: Optional[str] = None,       # filter by: success, failed, running
):
    """
    Get all historical runs for a specific pipeline.
 
    Examples:
        GET /pipeline/hr_data_csv/runs
        GET /pipeline/hr_data_csv/runs?limit=10&offset=0
        GET /pipeline/hr_data_csv/runs?status=failed
    """
    dag_id = pipeline_name if pipeline_name.startswith("pipeline_") else f"pipeline_{pipeline_name}"
 
    conn = get_conn()
    cur  = conn.cursor()
 
    try:
        # Build query with optional status filter
        base_query = """
            SELECT
                id,
                dag_id,
                dag_run_id,
                pipeline_name,
                connector_type,
                file_path,
                folder_path,
                sheet_url,
                api_url,
                operation,
                table_name,
                schedule,
                status,
                triggered_by,
                execution_date,
                created_at
            FROM airflow_pipeline_runs
            WHERE dag_id = %s
        """
        params = [dag_id]
 
        if status:
            base_query += " AND UPPER(status) = %s"
            params.append(status.upper())
 
        base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
 
        cur.execute(base_query, params)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
 
        # Total count (for pagination)
        count_query = "SELECT COUNT(*) FROM airflow_pipeline_runs WHERE dag_id = %s"
        count_params = [dag_id]
        if status:
            count_query += " AND UPPER(status) = %s"
            count_params.append(status.upper())
 
        cur.execute(count_query, count_params)
        total = cur.fetchone()[0]
 
        runs = [dict(zip(cols, row)) for row in rows]
 
        # Summary stats
        cur.execute("""
            SELECT
                COUNT(*)                                         AS total_runs,
                COUNT(*) FILTER (WHERE UPPER(status) = 'SUCCESS') AS success_count,
                COUNT(*) FILTER (WHERE UPPER(status) = 'FAILED')  AS failed_count,
                COUNT(*) FILTER (WHERE UPPER(status) = 'RUNNING') AS running_count,
                MAX(created_at)                                  AS last_run_at
            FROM airflow_pipeline_runs
            WHERE dag_id = %s
        """, [dag_id])
 
        stats_row = cur.fetchone()
        stats = {
            "total_runs":    stats_row[0],
            "success_count": stats_row[1],
            "failed_count":  stats_row[2],
            "running_count": stats_row[3],
            "last_run_at":   stats_row[4].isoformat() if stats_row[4] else None,
        }
 
        return {
            "pipeline":   dag_id,
            "stats":      stats,
            "pagination": {
                "total":   total,
                "limit":   limit,
                "offset":  offset,
                "has_more": (offset + limit) < total,
            },
            "runs": runs,
        }
 
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 
    finally:
        cur.close()
        conn.close()

import glob as _glob
 
 
def _read_log_from_filesystem(pipeline_id: str, dag_run_id: str = None) -> dict:
    """
    Fallback — read log directly from Airflow log files
    when DB has no entry yet (e.g. run still in progress).
    """
    if dag_run_id:
        # specific run
        pattern = (
            f"E:\\Universal_data_connector_system\\airflow\\logs\\"
            f"dag_id={pipeline_id}\\run_id={dag_run_id}"
            f"\\task_id=run_connector\\attempt=*.log"
        )
    else:
        # latest run — wildcard on run_id
        pattern = (
            f"E:\\Universal_data_connector_system\\airflow\\logs\\"
            f"dag_id={pipeline_id}\\run_id=*"
            f"\\task_id=run_connector\\attempt=*.log"
        )
 
    log_files = sorted(_glob.glob(pattern))
    if not log_files:
        return {"source": "filesystem", "log_content": None, "log_file_path": None}
 
    latest = log_files[-1]
    try:
        with open(latest, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        return {"source": "filesystem", "log_content": content, "log_file_path": latest}
    except Exception as e:
        return {"source": "filesystem", "log_content": f"Could not read: {e}", "log_file_path": latest}
 
 
# ─────────────────────────────────────────────
# GET /pipeline/{name}/logs
# Returns latest run's logs
# ─────────────────────────────────────────────
 
@app.get("/pipeline/{pipeline_name}/logs")
def get_pipeline_logs_latest(pipeline_name: str):
    """
    Get logs of the latest run of a pipeline.
    Tries DB first, falls back to filesystem.
 
    Example: GET /pipeline/hr_data_csv/logs
    """
    pipeline_id = (
        pipeline_name
        if pipeline_name.startswith("pipeline_")
        else f"pipeline_{pipeline_name}"
    )
 
    conn = get_conn()
    cur  = conn.cursor()
 
    try:
        cur.execute("""
            SELECT
                l.id, l.pipeline_id, l.dag_run_id, l.task_id,
                l.status, l.log_content, l.log_file_path, l.created_at
            FROM pipeline_dag_logs l
            WHERE l.pipeline_id = %s
            ORDER BY l.created_at DESC
            LIMIT 1
        """, (pipeline_id,))
 
        row = cur.fetchone()
 
        if row:
            cols = [desc[0] for desc in cur.description]
            data = dict(zip(cols, row))
            return {
                "pipeline":  pipeline_id,
                "source":    "db",
                "dag_run_id": data["dag_run_id"],
                "status":    data["status"],
                "log_file":  data["log_file_path"],
                "log":       data["log_content"],
                "logged_at": data["created_at"],
            }
 
        # fallback to filesystem
        fs = _read_log_from_filesystem(pipeline_id)
        if fs["log_content"]:
            return {
                "pipeline":  pipeline_id,
                "source":    "filesystem",
                "dag_run_id": None,
                "status":    None,
                "log_file":  fs["log_file_path"],
                "log":       fs["log_content"],
                "logged_at": None,
            }
 
        raise HTTPException(
            status_code=404,
            detail=f"No logs found for pipeline '{pipeline_id}'. Has it run yet?"
        )
 
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()
 
 
# ─────────────────────────────────────────────
# GET /pipeline/{name}/logs/{dag_run_id}
# Returns logs for a specific run
# ─────────────────────────────────────────────
 
@app.get("/pipeline/{pipeline_name}/logs/{dag_run_id:path}")
def get_pipeline_logs_by_run(pipeline_name: str, dag_run_id: str):
    """
    Get logs of a specific run by dag_run_id.
    Tries DB first, falls back to filesystem.
 
    Example: GET /pipeline/hr_data_csv/logs/run__pipeline_hr_data_csv__20260324_103000
    """
    pipeline_id = (
        pipeline_name
        if pipeline_name.startswith("pipeline_")
        else f"pipeline_{pipeline_name}"
    )
 
    conn = get_conn()
    cur  = conn.cursor()
 
    try:
        cur.execute("""
            SELECT
                l.id, l.pipeline_id, l.dag_run_id, l.task_id,
                l.status, l.log_content, l.log_file_path, l.created_at
            FROM pipeline_dag_logs l
            WHERE l.pipeline_id = %s
              AND l.dag_run_id   = %s
            ORDER BY l.created_at DESC
            LIMIT 1
        """, (pipeline_id, dag_run_id))
 
        row = cur.fetchone()
 
        if row:
            cols = [desc[0] for desc in cur.description]
            data = dict(zip(cols, row))
            return {
                "pipeline":  pipeline_id,
                "source":    "db",
                "dag_run_id": data["dag_run_id"],
                "status":    data["status"],
                "log_file":  data["log_file_path"],
                "log":       data["log_content"],
                "logged_at": data["created_at"],
            }
 
        # fallback to filesystem
        fs = _read_log_from_filesystem(pipeline_id, dag_run_id)
        if fs["log_content"]:
            return {
                "pipeline":  pipeline_id,
                "source":    "filesystem",
                "dag_run_id": dag_run_id,
                "status":    None,
                "log_file":  fs["log_file_path"],
                "log":       fs["log_content"],
                "logged_at": None,
            }
 
        raise HTTPException(
            status_code=404,
            detail=f"No logs found for run '{dag_run_id}' in pipeline '{pipeline_id}'."
        )
 
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()