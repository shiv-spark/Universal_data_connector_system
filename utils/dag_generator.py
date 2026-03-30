
import os
import re
import inspect

DAGS_FOLDER = "E:\\Universal_data_connector_system\\airflow\\dags"

VALID_CONNECTORS = {"csv", "excel", "google_sheets", "api"}
VALID_OPTIONS    = {"1", "2", "3"}


def _safe_id(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name or "pipeline"


def _clean(val):
    if not val or str(val).strip().lower() in ("string", "null", "none", ""):
        return None
    return val


def _fix_path(val):
    if val is None:
        return None
    val = val.strip().strip('"').strip("'")
    return val.replace("\\", "\\\\")


def validate_pipeline_config(config: dict) -> list:
    config["folder_path"]     = _clean(config.get("folder_path"))
    config["file_path"]       = _clean(config.get("file_path"))
    config["api_url"]         = _clean(config.get("api_url"))
    config["sheet_url"]       = _clean(config.get("sheet_url"))
    config["after_first_run"] = _clean(config.get("after_first_run"))

    errors = []
    if not config.get("pipeline_name"):
        errors.append("pipeline_name required.")

    ct = config.get("connector_type", "")
    if ct not in VALID_CONNECTORS:
        errors.append(f"connector_type '{ct}' invalid. Valid: {VALID_CONNECTORS}")

    if ct in ("csv", "excel"):
        if not config.get("folder_path") and not config.get("file_path"):
            errors.append("csv/excel: folder_path or file_path required.")

    if ct == "google_sheets" and not config.get("sheet_url"):
        errors.append("google_sheets: sheet_url required.")

    if ct == "api" and not config.get("api_url"):
        errors.append("api: api_url required.")

    if config.get("option", "1") not in VALID_OPTIONS:
        errors.append("option '1' (append), '2' (overwrite), or '3' (create new) required.")

    afr = config.get("after_first_run")
    if afr and afr not in ("1", "2"):
        errors.append("after_first_run '1' or '2' required.")

    if config.get("option") == "3" and not afr:
        errors.append("option '3': after_first_run required — '1' or '2'.")

    if not config.get("table_name"):
        errors.append("table_name required.")

    return errors


def _render_template(cfg: dict) -> str:
    pipeline_id     = _safe_id(cfg["pipeline_name"])
    connector_type  = cfg["connector_type"]
    option          = cfg.get("option", "1")
    after_first_run = cfg.get("after_first_run") or None
    table_name      = cfg["table_name"]
    schedule        = cfg.get("schedule", "*/5 * * * *")
    folder_path     = _fix_path(cfg.get("folder_path") or None)
    file_path       = _fix_path(cfg.get("file_path") or None)
    sheet_url       = cfg.get("sheet_url") or None
    api_url         = cfg.get("api_url") or None

    def q(val):
        return f'"{val}"' if val is not None else "None"

    header_lines = [
        "from airflow import DAG",
        "from airflow.operators.python import PythonOperator",
        "from datetime import datetime",
        "import os, json, requests, shutil",
        "",
        f"# AUTO-GENERATED — pipeline: {pipeline_id}",
        f"# Do not manually edit. Use /create_pipeline endpoint to regenerate.",
        "",
        f'PIPELINE_ID     = "pipeline_{pipeline_id}"',
        f'CONNECTOR_TYPE  = "{connector_type}"',
        f"FOLDER_PATH     = {q(folder_path)}",
        f"FILE_PATH       = {q(file_path)}",
        f"SHEET_URL       = {q(sheet_url)}",
        f"API_URL         = {q(api_url)}",
        f'OPTION          = "{option}"',
        f"AFTER_FIRST_RUN = {q(after_first_run)}",
        f'TABLE_NAME      = "{table_name}"',
        f'SCHEDULE        = "{schedule}"',
        "",
        'BASE_URL         = "http://host.docker.internal:8000"',
        'CONTAINER_PATH   = "/opt/airflow/user_data"',
        'WINDOWS_PATH     = "E:\\\\\\\\Universal_data_connector_system\\\\\\\\data"',
        'DATASET_BASE_WIN = "E:\\\\\\\\Universal_data_connector_system\\\\\\\\Dataset"',
        'DATASET_BASE_CON = "/opt/airflow/dataset"',
        f'DATASET_PIPELINE_CON = "/opt/airflow/dataset/pipeline_{pipeline_id}"',
        f'PIPELINE_CON_ROOT    = "/opt/airflow/user_data/pipeline_{pipeline_id}"',
        "",
        "CONNECTOR_ENDPOINT = {",
        '    "csv":           "ingest_csv",',
        '    "excel":         "ingest_excel",',
        '    "google_sheets": "ingest_google_sheet",',
        '    "api":           "ingest_api",',
        "}",
    ]

    static_body = open(
        os.path.join(os.path.dirname(__file__), "dag_static_body.py"),
        encoding="utf-8"
    ).read()

    dag_block = """

with DAG(
    dag_id            = PIPELINE_ID,
    start_date        = datetime(2024, 1, 1),
    schedule_interval = SCHEDULE,
    catchup           = False,
    tags              = ["connector", CONNECTOR_TYPE],
) as dag:
    PythonOperator(
        task_id         = "run_connector",
        python_callable = run_connector,
    )
"""
    return "\n".join(header_lines) + "\n" + static_body + dag_block


def create_dag_file(config: dict) -> dict:
    errors = validate_pipeline_config(config)
    if errors:
        return {"status": "FAILED", "errors": errors}

    pipeline_id = _safe_id(config["pipeline_name"])
    filename    = f"pipeline_{pipeline_id}.py"
    file_path   = DAGS_FOLDER.rstrip("/\\") + "\\" + filename

    if os.path.exists(file_path):
        return {
            "status": "FAILED",
            "error":  f"Pipeline '{pipeline_id}' already exists. Delete it first or rename."
        }

    content = _render_template({**config, "pipeline_name": pipeline_id})

    os.makedirs(DAGS_FOLDER, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    return {
        "status":    "SUCCESS",
        "dag_id":    f"pipeline_{pipeline_id}",
        "file_path": file_path,
        "message":   "DAG file created. Airflow will pick it up in ~30 sec."
    }


def delete_dag_file(pipeline_name: str) -> dict:
    pipeline_id = _safe_id(pipeline_name)
    file_path   = DAGS_FOLDER.rstrip("/\\") + "\\" + f"pipeline_{pipeline_id}.py"

    if not os.path.exists(file_path):
        return {"status": "FAILED", "error": f"Pipeline '{pipeline_id}' not found."}

    os.remove(file_path)
    return {"status": "SUCCESS", "message": f"pipeline_{pipeline_id}.py deleted."}


def list_dag_files() -> list:
    if not os.path.exists(DAGS_FOLDER):
        return []

    result = []
    for fname in sorted(os.listdir(DAGS_FOLDER)):
        if fname.startswith("pipeline_") and fname.endswith(".py"):
            fpath = os.path.join(DAGS_FOLDER, fname)
            result.append({
                "dag_id":    fname.replace(".py", ""),
                "file_name": fname,
                "size_kb":   round(os.path.getsize(fpath) / 1024, 1),
            })
    return result