import hashlib

def _normalize(path):
    #convert all backslash variants to forward slash
    if not path:
        return path
    return path.replace("\\\\", "/").replace("\\", "/")


def to_windows_path(path):
    n         = _normalize(path)
    n_dataset = _normalize(DATASET_BASE_CON)
    n_user    = _normalize(CONTAINER_PATH)
    if n and n.startswith(n_dataset):
        return n.replace(n_dataset, DATASET_BASE_WIN).replace("/", "\\")
    if n and n.startswith(n_user):
        return n.replace(n_user, WINDOWS_PATH).replace("/", "\\")
    return path


def to_container_path(path):
    # Windows path (E:/... E:\\... E:\\\\...) convert to container path
    if not path:
        return path
    n         = _normalize(path)
    n_dataset = _normalize(DATASET_BASE_WIN)
    n_windows = _normalize(WINDOWS_PATH)
    if n.startswith(n_dataset):
        return n.replace(n_dataset, DATASET_BASE_CON)
    if n.startswith(n_windows):
        return n.replace(n_windows, CONTAINER_PATH)
    return n


def _ensure_pipeline_folders():
    base = DATASET_PIPELINE_CON if CONNECTOR_TYPE in ("csv", "excel") else PIPELINE_CON_ROOT
    processed = os.path.join(base, "processed")
    failed    = os.path.join(base, "failed")
    os.makedirs(processed, exist_ok=True)
    os.makedirs(failed,    exist_ok=True)
    print(f"Folders ready — processed: {processed} | failed: {failed}")
    return processed, failed


# ─────────────────────────────────────────────
# HASH DEDUPLICATION — 3 naye functions
# ─────────────────────────────────────────────

def _get_file_hash(filepath):
    """File MD5 hash — same content = same hash"""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _hash_already_processed(file_hash, processed_dir):
    """
    Check if a file with the given hash has already been processed.
    """
    if not os.path.exists(processed_dir):
        return False
    for fname in os.listdir(processed_dir):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(processed_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                record = json.load(fh)
            if record.get("file_hash") == file_hash:
                print(f"Already processed (hash match): {fname} — skipping.")
                return True
        except Exception:
            continue
    return False
    if not os.path.exists(processed_dir):
        return False
    for fname in os.listdir(processed_dir):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(processed_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                record = json.load(fh)
            if record.get("file_hash") == file_hash:
                print(f"Already processed (hash match): {fname} — skipping.")
                return True
        except Exception:
            continue
    return False


def _save_file_record(filepath, file_hash, dest_folder):
    """
    check the file hash and save record in processed/ as JSON:
    to detect duplicates in future runs.
    """
    os.makedirs(dest_folder, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = os.path.basename(filepath)
    dest = os.path.join(dest_folder, f"{name}_{ts}.json")
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump({
            "original_filename": name,
            "file_hash":         file_hash,
            "processed_at":      ts,
            "pipeline":          PIPELINE_ID,
        }, fh, indent=2)
    print(f"Hash record saved: {dest}")


# ─────────────────────────────────────────────

def _move_file(src, dest_folder):
    os.makedirs(dest_folder, exist_ok=True)
    name, ext = os.path.splitext(os.path.basename(src))
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(dest_folder, f"{name}_{ts}{ext}")
    shutil.copy2(src, dest)
    os.remove(src)
    print(f"Moved: {src} -> {dest}")


def _url_already_handled(url, processed_dir, failed_dir):
        # only check processed/ — if it's already successfully processed, then skip
        # failed/ is not checked — to allow retries
    if not os.path.exists(processed_dir):
        return False
    for fname in os.listdir(processed_dir):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(processed_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                record = json.load(fh)
            if record.get("url") == url:
                print(f"URL already successfully processed — skipping: {url}")
                return True
        except Exception:
            continue
    return False


def _save_url_record(url, dest_folder, prefix="url"):
    os.makedirs(dest_folder, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(dest_folder, f"{prefix}_{ts}.json")
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump({"url": url, "timestamp": ts, "pipeline": PIPELINE_ID}, fh, indent=2)
    print(f"URL record saved: {dest}")


def _ingest_file(container_file, endpoint):
    windows_file = to_windows_path(container_file)
    payload = {"file_path": windows_file, "option": OPTION, "table_name": TABLE_NAME}
    res     = requests.post(f"{BASE_URL}/{endpoint}", json=payload, timeout=60)
    print(f"Status: {res.status_code} | Response: {res.text}")
    return res.status_code == 200 and res.json().get("status") == "SUCCESS"


def _update_option_in_dag(new_option):
    import re as _re
    with open(__file__, "r", encoding="utf-8") as fh:
        content = fh.read()
    content = _re.sub(r'OPTION\s*=\s*"3"', f'OPTION = "{new_option}"', content)
    with open(__file__, "w", encoding="utf-8") as fh:
        fh.write(content)
    print(f"OPTION updated to {new_option}")


def _send_email(status, error="", dag_run_id=None):
    import smtplib, traceback
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email import encoders

    EMAIL_SENDER   = "shivay999.ss@gmail.com"
    EMAIL_PASSWORD = "nmwq hybd wlrp dwkw"  # must be App Password, not Gmail login
    EMAIL_RECEIVER = "shivansh.s@sparkbrains.ai"

    try:
        msg = MIMEMultipart()
        msg["From"]    = EMAIL_SENDER
        msg["To"]      = EMAIL_RECEIVER
        msg["Subject"] = f"Pipeline {status.upper()}: {PIPELINE_ID}"

        emoji = "OK" if status == "success" else "FAILED"
        error_line = f"Error    : {error}" if error else ""
        body = (
            f"Pipeline {status.upper()} Alert\n"
            f"{emoji} Pipeline : {PIPELINE_ID}\n"
            f"   Connector: {CONNECTOR_TYPE}\n"
            f"   Table    : {TABLE_NAME}\n"
            f"   Status   : {status.upper()}\n"
            f"   {error_line}\n"
            f"Airflow UI: http://localhost:8081\n\n"
            f"Full logs attached."
        )
        msg.attach(MIMEText(body, "plain"))

        # Pass dag_run_id to the surviving _collect_logs
        log_content, _ = _collect_logs(dag_run_id) if dag_run_id else ("No run ID provided.", None)
        log_bytes = log_content.encode("utf-8")

        attachment = MIMEBase("application", "octet-stream")
        attachment.set_payload(log_bytes)
        encoders.encode_base64(attachment)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        attachment.add_header(
            "Content-Disposition",
            f"attachment; filename={PIPELINE_ID}_{status}_{ts}.log"
        )
        msg.attach(attachment)

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        print(f"Email sent: {status.upper()}")

    except Exception as e:
        # Print full traceback so you can see the real error in Airflow logs
        print(f"Email send failed: {e}")
        print(traceback.format_exc())


def run_connector(**context):
    print(f"Pipeline  : {PIPELINE_ID}")
    print(f"Connector : {CONNECTOR_TYPE}")
    print(f"Option    : {OPTION}")

    # Log run start
    dag_run_id = _log_run_start()

    try:
        processed_dir, failed_dir = _ensure_pipeline_folders()
        endpoint = CONNECTOR_ENDPOINT.get(CONNECTOR_TYPE)
        if not endpoint:
            raise ValueError(f"Invalid CONNECTOR_TYPE: {CONNECTOR_TYPE}")

        if CONNECTOR_TYPE in ("csv", "excel"):
            ext_filter = ".csv" if CONNECTOR_TYPE == "csv" else (".xlsx", ".xls")
            candidate_files = []

            if FOLDER_PATH:
                container_path = to_container_path(FOLDER_PATH)
                print(f"Path resolved: {container_path}")
                if not os.path.exists(container_path):
                    print(f"Path not found: {container_path} — skipping this run.")
                    _log_run_end(dag_run_id, "SKIPPED", "Path not found")
                    return
                if os.path.isfile(container_path):
                    candidate_files = [container_path]
                elif os.path.isdir(container_path):
                    files_in_dir = [f for f in os.listdir(container_path) if f.lower().endswith(ext_filter)]
                    if not files_in_dir:
                        print("No matching files found — skipping.")
                        _log_run_end(dag_run_id, "SKIPPED", "No files found in folder")
                        return
                    candidate_files = [os.path.join(container_path, f) for f in files_in_dir]
                else:
                    raise FileNotFoundError(f"Path exists but is neither file nor directory: {container_path}")

            elif FILE_PATH:
                container_file = to_container_path(FILE_PATH)
                print(f"File resolved: {container_file}")
                if not os.path.exists(container_file):
                    print(f"File not found: {container_file} — skipping.")
                    _log_run_end(dag_run_id, "SKIPPED", "File not found")
                    return
                candidate_files = [container_file]
            else:
                raise ValueError("CSV/Excel: FOLDER_PATH or FILE_PATH required.")

            # ─────────────────────────────────────────────
            #  hash check for duplicate skip
            # ─────────────────────────────────────────────
            any_failed = False
            for container_file in candidate_files:
                file_hash = _get_file_hash(container_file)

                if _hash_already_processed(file_hash, processed_dir):
                    # Same content — skip, file wahi rehne do folder mein
                    print(f"SKIP: {os.path.basename(container_file)} (same content, already in DB)")
                    continue

                print(f"Processing: {os.path.basename(container_file)}")
                if _ingest_file(container_file, endpoint):
                    _save_file_record(container_file, file_hash, processed_dir)  # hash JSON save
                    _move_file(container_file, processed_dir)                     # file move
                else:
                    print(f"Failed: {os.path.basename(container_file)} — moving to failed/")
                    _move_file(container_file, failed_dir)
                    any_failed = True
            # ─────────────────────────────────────────────

            if any_failed:
                raise Exception("One or more files failed during ingestion")

        elif CONNECTOR_TYPE == "google_sheets":
            if not SHEET_URL:
                raise ValueError("SHEET_URL required.")
            if _url_already_handled(SHEET_URL, processed_dir, failed_dir):
                _log_run_end(dag_run_id, "SKIPPED", "URL already processed")
                return
            payload = {"sheet_url": SHEET_URL, "option": OPTION, "table_name": TABLE_NAME}
            res = requests.post(f"{BASE_URL}/{endpoint}", json=payload, timeout=60)
            if res.status_code == 200 and res.json().get("status") != "FAILED":
                _save_url_record(SHEET_URL, processed_dir, prefix="sheet_processed")
            else:
                _save_url_record(SHEET_URL, failed_dir, prefix="sheet_failed")
                raise Exception(f"Google Sheets ingestion failed: {res.text}")

        elif CONNECTOR_TYPE == "api":
            if not API_URL:
                raise ValueError("API_URL required.")
            if _url_already_handled(API_URL, processed_dir, failed_dir):
                _log_run_end(dag_run_id, "SKIPPED", "URL already processed")
                return
            payload = {"url": API_URL, "option": OPTION, "table_name": TABLE_NAME}
            res = requests.post(f"{BASE_URL}/{endpoint}", json=payload, timeout=60)
            if res.status_code == 200 and res.json().get("status") != "FAILED":
                _save_url_record(API_URL, processed_dir, prefix="api_processed")
            else:
                _save_url_record(API_URL, failed_dir, prefix="api_failed")
                raise Exception(f"API ingestion failed: {res.text}")

        if OPTION == "3" and AFTER_FIRST_RUN in ("1", "2"):
            _update_option_in_dag(AFTER_FIRST_RUN)

        # Log success
        _log_run_end(dag_run_id, "SUCCESS")
        print("Pipeline completed!")
        _send_email("success", dag_run_id=dag_run_id)

    except Exception as e:
        # Log failure
        _log_run_end(dag_run_id, "FAILED", str(e))
        _send_email("failed", error=str(e), dag_run_id=dag_run_id)
        raise   # re-raise so Airflow also marks the task as failed


import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host":     "host.docker.internal",   # inside Docker container
    "database": "postgres",
    "user":     "postgres",
    "password": "spark@1234",
    "port":     "5432",
}


def _get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def _collect_logs(dag_run_id):
    """
    Read Airflow log file for this DAG run.
    Returns (log_content, log_file_path).
    """
    import glob

    pattern = (
        f"/opt/airflow/logs/dag_id={PIPELINE_ID}"
        f"/run_id=*/task_id=run_connector/attempt=*.log"
    )
    log_files = sorted(glob.glob(pattern))

    if not log_files:
        # fallback — try older Airflow log path format
        pattern_old = f"/opt/airflow/logs/{PIPELINE_ID}/run_connector/*.log"
        log_files = sorted(glob.glob(pattern_old))

    if not log_files:
        print(f"No log files found for pattern: {pattern}")
        return "No log file found.", None

    latest = log_files[-1]
    try:
        with open(latest, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        print(f"Log file read: {latest} ({len(content)} chars)")
        return content, latest
    except Exception as e:
        return f"Could not read log file: {e}", latest


def _save_log_to_db(dag_run_id, status, log_content, log_file_path):
    """
    Insert log content into pipeline_dag_logs table.
    """
    try:
        conn = _get_db_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_dag_logs (
                pipeline_id, dag_run_id, task_id,
                status, log_content, log_file_path
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            PIPELINE_ID,
            dag_run_id,
            "run_connector",
            status,
            log_content,
            log_file_path,
        ))
        conn.commit()
        cur.close()
        conn.close()
        print(f"Log saved to DB for run: {dag_run_id}")
    except Exception as e:
        print(f"Failed to save log to DB: {e}")


def _log_run_start():
    """
    Insert a RUNNING row when DAG starts.
    Returns dag_run_id string used to update status later.
    """
    dag_run_id = f"run__{PIPELINE_ID}__{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        conn = _get_db_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO airflow_pipeline_runs (
                dag_id, dag_run_id, pipeline_name,
                connector_type, folder_path, file_path,
                sheet_url, api_url, operation, table_name,
                schedule, status, execution_date, triggered_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            PIPELINE_ID,
            dag_run_id,
            PIPELINE_ID,
            CONNECTOR_TYPE,
            FOLDER_PATH,
            FILE_PATH,
            SHEET_URL,
            API_URL,
            OPTION,
            TABLE_NAME,
            SCHEDULE,
            "RUNNING",
            datetime.now().isoformat(),
            "scheduler",
        ))
        conn.commit()
        cur.close()
        conn.close()
        print(f"DB log: RUNNING — {dag_run_id}")
    except Exception as e:
        print(f"DB log failed (start): {e}")

    return dag_run_id


def _log_run_end(dag_run_id, status, error=""):
    """
    1. Update airflow_pipeline_runs status
    2. Collect log file
    3. Save log content to pipeline_dag_logs
    """
    # Step 1 — update run status
    try:
        conn = _get_db_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE airflow_pipeline_runs
            SET status = %s,
                error_message = %s
            WHERE dag_run_id = %s
        """, (status, error or None, dag_run_id))
        conn.commit()
        cur.close()
        conn.close()
        print(f"DB log: {status} — {dag_run_id}")
    except Exception as e:
        print(f"DB log failed (end): {e}")

    # Step 2+3 — collect logs and save to DB
    log_content, log_file_path = _collect_logs(dag_run_id)
    _save_log_to_db(dag_run_id, status, log_content, log_file_path)


# import hashlib

# def _normalize(path):
#     #convert all backslash variants to forward slash
#     if not path:
#         return path
#     return path.replace("\\\\", "/").replace("\\", "/")


# def to_windows_path(path):
#     n         = _normalize(path)
#     n_dataset = _normalize(DATASET_BASE_CON)
#     n_user    = _normalize(CONTAINER_PATH)
#     if n and n.startswith(n_dataset):
#         return n.replace(n_dataset, DATASET_BASE_WIN).replace("/", "\\")
#     if n and n.startswith(n_user):
#         return n.replace(n_user, WINDOWS_PATH).replace("/", "\\")
#     return path


# def to_container_path(path):
#     # Windows path (E:/... E:\\... E:\\\\...) convert to container path
#     if not path:
#         return path
#     n         = _normalize(path)
#     n_dataset = _normalize(DATASET_BASE_WIN)
#     n_windows = _normalize(WINDOWS_PATH)
#     if n.startswith(n_dataset):
#         return n.replace(n_dataset, DATASET_BASE_CON)
#     if n.startswith(n_windows):
#         return n.replace(n_windows, CONTAINER_PATH)
#     return n


# def _ensure_pipeline_folders():
#     base = DATASET_PIPELINE_CON if CONNECTOR_TYPE in ("csv", "excel") else PIPELINE_CON_ROOT
#     processed = os.path.join(base, "processed")
#     failed    = os.path.join(base, "failed")
#     os.makedirs(processed, exist_ok=True)
#     os.makedirs(failed,    exist_ok=True)
#     print(f"Folders ready — processed: {processed} | failed: {failed}")
#     return processed, failed


# def _move_file(src, dest_folder):
#     os.makedirs(dest_folder, exist_ok=True)
#     name, ext = os.path.splitext(os.path.basename(src))
#     ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
#     dest = os.path.join(dest_folder, f"{name}_{ts}{ext}")
#     shutil.copy2(src, dest)
#     os.remove(src)
#     print(f"Moved: {src} -> {dest}")


# def _url_already_handled(url, processed_dir, failed_dir):
#         # only check processed/ — if it's already successfully processed, then skip
#         # failed/ is not checked — to allow retries
#     if not os.path.exists(processed_dir):
#         return False
#     for fname in os.listdir(processed_dir):
#         if not fname.endswith(".json"):
#             continue
#         fpath = os.path.join(processed_dir, fname)
#         try:
#             with open(fpath, "r", encoding="utf-8") as fh:
#                 record = json.load(fh)
#             if record.get("url") == url:
#                 print(f"URL already successfully processed — skipping: {url}")
#                 return True
#         except Exception:
#             continue
#     return False


# def _save_url_record(url, dest_folder, prefix="url"):
#     os.makedirs(dest_folder, exist_ok=True)
#     ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
#     dest = os.path.join(dest_folder, f"{prefix}_{ts}.json")
#     with open(dest, "w", encoding="utf-8") as fh:
#         json.dump({"url": url, "timestamp": ts, "pipeline": PIPELINE_ID}, fh, indent=2)
#     print(f"URL record saved: {dest}")


# def _ingest_file(container_file, endpoint):
#     windows_file = to_windows_path(container_file)
#     payload = {"file_path": windows_file, "option": OPTION, "table_name": TABLE_NAME}
#     res     = requests.post(f"{BASE_URL}/{endpoint}", json=payload, timeout=60)
#     print(f"Status: {res.status_code} | Response: {res.text}")
#     return res.status_code == 200 and res.json().get("status") == "SUCCESS"


# def _update_option_in_dag(new_option):
#     import re as _re
#     with open(__file__, "r", encoding="utf-8") as fh:
#         content = fh.read()
#     content = _re.sub(r'OPTION\s*=\s*"3"', f'OPTION = "{new_option}"', content)
#     with open(__file__, "w", encoding="utf-8") as fh:
#         fh.write(content)
#     print(f"OPTION updated to {new_option}")

# def _send_email(status, error="", dag_run_id=None):
#     import smtplib, traceback
#     from email.mime.text import MIMEText
#     from email.mime.multipart import MIMEMultipart
#     from email.mime.base import MIMEBase
#     from email import encoders

#     EMAIL_SENDER   = "shivay999.ss@gmail.com"
#     EMAIL_PASSWORD = "nmwq hybd wlrp dwkw"  # must be App Password, not Gmail login
#     EMAIL_RECEIVER = "shivansh.s@sparkbrains.ai"

#     try:
#         msg = MIMEMultipart()
#         msg["From"]    = EMAIL_SENDER
#         msg["To"]      = EMAIL_RECEIVER
#         msg["Subject"] = f"Pipeline {status.upper()}: {PIPELINE_ID}"

#         emoji = "OK" if status == "success" else "FAILED"
#         error_line = f"Error    : {error}" if error else ""
#         body = (
#             f"Pipeline {status.upper()} Alert\n"
#             f"{emoji} Pipeline : {PIPELINE_ID}\n"
#             f"   Connector: {CONNECTOR_TYPE}\n"
#             f"   Table    : {TABLE_NAME}\n"
#             f"   Status   : {status.upper()}\n"
#             f"   {error_line}\n"
#             f"Airflow UI: http://localhost:8081\n\n"
#             f"Full logs attached."
#         )
#         msg.attach(MIMEText(body, "plain"))

#         # Pass dag_run_id to the surviving _collect_logs
#         log_content, _ = _collect_logs(dag_run_id) if dag_run_id else ("No run ID provided.", None)
#         log_bytes = log_content.encode("utf-8")

#         attachment = MIMEBase("application", "octet-stream")
#         attachment.set_payload(log_bytes)
#         encoders.encode_base64(attachment)
#         ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#         attachment.add_header(
#             "Content-Disposition",
#             f"attachment; filename={PIPELINE_ID}_{status}_{ts}.log"
#         )
#         msg.attach(attachment)

#         with smtplib.SMTP("smtp.gmail.com", 587) as server:
#             server.starttls()
#             server.login(EMAIL_SENDER, EMAIL_PASSWORD)
#             server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
#         print(f"Email sent: {status.upper()}")

#     except Exception as e:
#         # Print full traceback so you can see the real error in Airflow logs
#         print(f"Email send failed: {e}")
#         print(traceback.format_exc())

# def run_connector(**context):
#     print(f"Pipeline  : {PIPELINE_ID}")
#     print(f"Connector : {CONNECTOR_TYPE}")
#     print(f"Option    : {OPTION}")
 
#     # ✅ Log run start
#     dag_run_id = _log_run_start()
 
#     try:
#         processed_dir, failed_dir = _ensure_pipeline_folders()
#         endpoint = CONNECTOR_ENDPOINT.get(CONNECTOR_TYPE)
#         if not endpoint:
#             raise ValueError(f"Invalid CONNECTOR_TYPE: {CONNECTOR_TYPE}")
 
#         if CONNECTOR_TYPE in ("csv", "excel"):
#             ext_filter = ".csv" if CONNECTOR_TYPE == "csv" else (".xlsx", ".xls")
#             candidate_files = []
 
#             if FOLDER_PATH:
#                 container_path = to_container_path(FOLDER_PATH)
#                 print(f"Path resolved: {container_path}")
#                 if not os.path.exists(container_path):
#                     print(f"Path not found: {container_path} — skipping this run.")
#                     _log_run_end(dag_run_id, "SKIPPED", "Path not found")
#                     return
#                 if os.path.isfile(container_path):
#                     candidate_files = [container_path]
#                 elif os.path.isdir(container_path):
#                     files_in_dir = [f for f in os.listdir(container_path) if f.lower().endswith(ext_filter)]
#                     if not files_in_dir:
#                         print("No matching files found — skipping.")
#                         _log_run_end(dag_run_id, "SKIPPED", "No files found in folder")
#                         return
#                     candidate_files = [os.path.join(container_path, f) for f in files_in_dir]
#                 else:
#                     raise FileNotFoundError(f"Path exists but is neither file nor directory: {container_path}")
 
#             elif FILE_PATH:
#                 container_file = to_container_path(FILE_PATH)
#                 print(f"File resolved: {container_file}")
#                 if not os.path.exists(container_file):
#                     print(f"File not found: {container_file} — skipping.")
#                     _log_run_end(dag_run_id, "SKIPPED", "File not found")
#                     return
#                 candidate_files = [container_file]
#             else:
#                 raise ValueError("CSV/Excel: FOLDER_PATH or FILE_PATH required.")
 
#             any_failed = False
#             for container_file in candidate_files:
#                 print(f"Processing: {os.path.basename(container_file)}")
#                 if _ingest_file(container_file, endpoint):
#                     _move_file(container_file, processed_dir)
#                 else:
#                     print(f"Failed: {os.path.basename(container_file)} — moving to failed/")
#                     _move_file(container_file, failed_dir)
#                     any_failed = True
 
#             if any_failed:
#                 raise Exception("One or more files failed during ingestion")
 
#         elif CONNECTOR_TYPE == "google_sheets":
#             if not SHEET_URL:
#                 raise ValueError("SHEET_URL required.")
#             if _url_already_handled(SHEET_URL, processed_dir, failed_dir):
#                 _log_run_end(dag_run_id, "SKIPPED", "URL already processed")
#                 return
#             payload = {"sheet_url": SHEET_URL, "option": OPTION, "table_name": TABLE_NAME}
#             res = requests.post(f"{BASE_URL}/{endpoint}", json=payload, timeout=60)
#             if res.status_code == 200 and res.json().get("status") != "FAILED":
#                 _save_url_record(SHEET_URL, processed_dir, prefix="sheet_processed")
#             else:
#                 _save_url_record(SHEET_URL, failed_dir, prefix="sheet_failed")
#                 raise Exception(f"Google Sheets ingestion failed: {res.text}")
 
#         elif CONNECTOR_TYPE == "api":
#             if not API_URL:
#                 raise ValueError("API_URL required.")
#             if _url_already_handled(API_URL, processed_dir, failed_dir):
#                 _log_run_end(dag_run_id, "SKIPPED", "URL already processed")
#                 return
#             payload = {"url": API_URL, "option": OPTION, "table_name": TABLE_NAME}
#             res = requests.post(f"{BASE_URL}/{endpoint}", json=payload, timeout=60)
#             if res.status_code == 200 and res.json().get("status") != "FAILED":
#                 _save_url_record(API_URL, processed_dir, prefix="api_processed")
#             else:
#                 _save_url_record(API_URL, failed_dir, prefix="api_failed")
#                 raise Exception(f"API ingestion failed: {res.text}")
 
#         if OPTION == "3" and AFTER_FIRST_RUN in ("1", "2"):
#             _update_option_in_dag(AFTER_FIRST_RUN)
 
#         # ✅ Log success
#         _log_run_end(dag_run_id, "SUCCESS")
#         print("Pipeline completed!")
#         _send_email("success",dag_run_id=dag_run_id)
 
#     except Exception as e:
#         # ✅ Log failure
#         _log_run_end(dag_run_id, "FAILED", str(e))
#         _send_email("failed", error=str(e), dag_run_id=dag_run_id)
#         raise   # re-raise so Airflow also marks the task as failed

# import psycopg2
# from datetime import datetime
 
# DB_CONFIG = {
#     "host":     "host.docker.internal",   # inside Docker container
#     "database": "postgres",
#     "user":     "postgres",
#     "password": "spark@1234",
#     "port":     "5432",
# }
 
 
# def _get_db_conn():
#     return psycopg2.connect(**DB_CONFIG)

# def _collect_logs(dag_run_id):
#     """
#     Read Airflow log file for this DAG run.
#     Returns (log_content, log_file_path).
#     """
#     import glob
 
#     pattern = (
#         f"/opt/airflow/logs/dag_id={PIPELINE_ID}"
#         f"/run_id=*/task_id=run_connector/attempt=*.log"
#     )
#     log_files = sorted(glob.glob(pattern))
 
#     if not log_files:
#         # fallback — try older Airflow log path format
#         pattern_old = f"/opt/airflow/logs/{PIPELINE_ID}/run_connector/*.log"
#         log_files = sorted(glob.glob(pattern_old))
 
#     if not log_files:
#         print(f"No log files found for pattern: {pattern}")
#         return "No log file found.", None
 
#     latest = log_files[-1]
#     try:
#         with open(latest, "r", encoding="utf-8", errors="replace") as f:
#             content = f.read()
#         print(f"Log file read: {latest} ({len(content)} chars)")
#         return content, latest
#     except Exception as e:
#         return f"Could not read log file: {e}", latest
 
 
# def _save_log_to_db(dag_run_id, status, log_content, log_file_path):
#     """
#     Insert log content into pipeline_dag_logs table.
#     """
#     try:
#         conn = _get_db_conn()
#         cur  = conn.cursor()
#         cur.execute("""
#             INSERT INTO pipeline_dag_logs (
#                 pipeline_id, dag_run_id, task_id,
#                 status, log_content, log_file_path
#             )
#             VALUES (%s, %s, %s, %s, %s, %s)
#         """, (
#             PIPELINE_ID,
#             dag_run_id,
#             "run_connector",
#             status,
#             log_content,
#             log_file_path,
#         ))
#         conn.commit()
#         cur.close()
#         conn.close()
#         print(f"Log saved to DB for run: {dag_run_id}")
#     except Exception as e:
#         print(f"Failed to save log to DB: {e}")
 
 
# def _log_run_start():
#     """
#     Insert a RUNNING row when DAG starts.
#     Returns dag_run_id string used to update status later.
#     """
#     dag_run_id = f"run__{PIPELINE_ID}__{datetime.now().strftime('%Y%m%d_%H%M%S')}"
#     try:
#         conn = _get_db_conn()
#         cur  = conn.cursor()
#         cur.execute("""
#             INSERT INTO airflow_pipeline_runs (
#                 dag_id, dag_run_id, pipeline_name,
#                 connector_type, folder_path, file_path,
#                 sheet_url, api_url, operation, table_name,
#                 schedule, status, execution_date, triggered_by
#             )
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (
#             PIPELINE_ID,
#             dag_run_id,
#             PIPELINE_ID,
#             CONNECTOR_TYPE,
#             FOLDER_PATH,
#             FILE_PATH,
#             SHEET_URL,
#             API_URL,
#             OPTION,
#             TABLE_NAME,
#             SCHEDULE,
#             "RUNNING",
#             datetime.now().isoformat(),
#             "scheduler",
#         ))
#         conn.commit()
#         cur.close()
#         conn.close()
#         print(f"DB log: RUNNING — {dag_run_id}")
#     except Exception as e:
#         print(f"DB log failed (start): {e}")
 
#     return dag_run_id
 

# def _log_run_end(dag_run_id, status, error=""):
#     """
#     1. Update airflow_pipeline_runs status
#     2. Collect log file
#     3. Save log content to pipeline_dag_logs
#     """
#     # Step 1 — update run status
#     try:
#         conn = _get_db_conn()
#         cur  = conn.cursor()
#         cur.execute("""
#             UPDATE airflow_pipeline_runs
#             SET status = %s,
#                 error_message = %s
#             WHERE dag_run_id = %s
#         """, (status, error or None, dag_run_id))
#         conn.commit()
#         cur.close()
#         conn.close()
#         print(f"DB log: {status} — {dag_run_id}")
#     except Exception as e:
#         print(f"DB log failed (end): {e}")
 
#     # Step 2+3 — collect logs and save to DB
#     log_content, log_file_path = _collect_logs(dag_run_id)
#     _save_log_to_db(dag_run_id, status, log_content, log_file_path)


