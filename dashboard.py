
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import time

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Universal Data Connector",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
    /* Main background */
    .stApp { background-color: #0f0f1a;
            color: white; }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #13132a 0%, #0f0f1a 100%);
        border-right: 1px solid #2a2a4a;
    }
    
    /* Cards */
    .metric-card {
        background: #1a1a2e;
        border: 1px solid #2a2a4a;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        text-align: center;
    }
    .metric-val { font-size: 2rem; font-weight: 700; margin: 0; }
    .metric-lbl { font-size: 0.72rem; color: #6b6b90; letter-spacing: 0.08em; margin: 4px 0 0; }
    
    .status-badge {
        display: inline-block;
        padding: 3px 12px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.06em;
    }
    .badge-success { background: #0d2e1a; color: #27ae60; border: 1px solid #27ae60; }
    .badge-failed  { background: #2e0d0d; color: #e74c3c; border: 1px solid #e74c3c; }
    .badge-running { background: #0d1e2e; color: #3498db; border: 1px solid #3498db; }
    .badge-skipped { background: #2e2a0d; color: #f39c12; border: 1px solid #f39c12; }
    .badge-paused  { background: #2e200d; color: #e67e22; border: 1px solid #e67e22; }
    .badge-active  { background: #0d2e1a; color: #27ae60; border: 1px solid #27ae60; }
    
    .section-title {
        font-size: 1.4rem;
        font-weight: 700;
        color: #e8e8f8;
        border-left: 4px solid #6c63ff;
        padding-left: 12px;
        margin-bottom: 1rem;
    }
    
    /* Hide streamlit branding */
    #MainMenu, footer, header { visibility: hidden; }
    
    /* Inputs */
    .stTextInput input, .stSelectbox select, .stTextArea textarea {
        background: #1a1a2e !important;
        border: 1px solid #2a2a4a !important;
        color: #e8e8f8 !important;
        border-radius: 8px !important;
    }
    
    /* Buttons */
    .stButton button {
        border-radius: 8px !important;
        font-weight: 600 !important;
    }
    
    div[data-testid="stExpander"] {
        background: #1a1a2e;
        border: 1px solid #2a2a4a;
        border-radius: 10px;
    }
    
    .info-box {
        background: #0d1e3a;
        border: 1px solid #1a4a7a;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0;
        font-size: 0.85rem;
        color: #7ab8e8;
    }
    .warn-box {
        background: #2e200d;
        border: 1px solid #7a4a1a;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0;
        font-size: 0.85rem;
        color: #e8a87a;
    }
    .success-box {
        background: #0d2e1a;
        border: 1px solid #1a7a4a;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0;
        font-size: 0.85rem;
        color: #7ae8a8;
    }
    .error-box {
        background: #2e0d0d;
        border: 1px solid #7a1a1a;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0;
        font-size: 0.85rem;
        color: #e87a7a;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
if "api_base" not in st.session_state:
    st.session_state.api_base = "http://localhost:8000"

# ─────────────────────────────────────────────
# API HELPERS
# ─────────────────────────────────────────────
def api(method, endpoint, **kwargs):
    url = f"{st.session_state.api_base}{endpoint}"
    try:
        res = getattr(requests, method)(url, timeout=30, **kwargs)
        return res.status_code, res.json() if res.content else {}
    except requests.exceptions.ConnectionError:
        return 0, {"error": "Cannot connect to API. Is the app container running?"}
    except Exception as e:
        return 0, {"error": str(e)}

def badge(status):
    s = str(status).lower()
    cls = {
        "success": "badge-success", "failed": "badge-failed",
        "running": "badge-running", "skipped": "badge-skipped",
        "paused":  "badge-paused",  "active":  "badge-active",
        "created": "badge-running", "healthy": "badge-success",
        "degraded":"badge-failed",  "warning": "badge-skipped",
    }.get(s, "badge-running")
    return f'<span class="status-badge {cls}">{status.upper()}</span>'

def fmt_date(dt):
    if not dt:
        return "—"
    try:
        return datetime.fromisoformat(str(dt)).strftime("%d %b %Y, %H:%M")
    except:
        return str(dt)[:16]

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚡ Data Connector")
    api_url = st.text_input("API Base URL", value="http://localhost:8000")
    if "api_base" not in st.session_state:
        st.session_state.api_base = api_url
    elif api_url != st.session_state.api_base:
        st.session_state.api_base = api_url
        st.rerun()

    # Health check
    code, data = api("get", "/health")
    if code == 200:
        st.markdown('<div class="success-box">🟢 API Connected</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="error-box">🔴 API Unreachable</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("**Navigation**")

    page = st.radio("", [
        "📊 Dashboard",
        "➕ Create Pipeline",
        "📋 Manage Pipelines",
        "📥 Direct Ingest",
        "🗄️ Data Preview",
        "📈 Metrics",
        "📜 Logs",
        "🔗 Multi-Source Pipeline",
    ], label_visibility="collapsed")

    st.markdown("---")
    st.markdown(
        '<div style="font-size:0.7rem; color:#6b6b90; text-align:center;">'
        'SparkBrains · v1.7<br>Universal Data Connector</div>',
        unsafe_allow_html=True
    )

# ═══════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ═══════════════════════════════════════════════════════════════════
if page == "📊 Dashboard":
    st.markdown('<div class="section-title">Dashboard</div>', unsafe_allow_html=True)

    code, data = api("get", "/dashboard/summary")

    if code != 200:
        st.error(f"Could not load dashboard: {data.get('error', data)}")
        st.stop()

    metrics    = data.get("metrics", {})
    health     = data.get("system_health", "UNKNOWN")
    daily      = data.get("daily", [])
    hourly     = data.get("hourly", [])
    connectors = data.get("connectors", [])
    recent     = data.get("recent_runs", [])
    pipeline_h = data.get("pipeline_health", [])
    top_fail   = data.get("top_failing", [])
    vol_trend  = data.get("volume_trend", [])

    # ── Health + metric cards ──────────────────────────────────────
    h_color = {"HEALTHY": "#27ae60", "WARNING": "#f39c12", "DEGRADED": "#e74c3c"}.get(health, "#7f8c8d")
    col0, col1, col2, col3, col4, col5 = st.columns(6)
    with col0:
        st.markdown(f"""<div class="metric-card">
            <p class="metric-val" style="color:{h_color}">{health}</p>
            <p class="metric-lbl">SYSTEM HEALTH</p></div>""", unsafe_allow_html=True)
    with col1:
        st.markdown(f"""<div class="metric-card">
            <p class="metric-val" style="color:#e8e8f8">{metrics.get('total_runs','—')}</p>
            <p class="metric-lbl">TOTAL RUNS (24H)</p></div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class="metric-card">
            <p class="metric-val" style="color:#27ae60">{metrics.get('success','—')}</p>
            <p class="metric-lbl">SUCCESS</p></div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class="metric-card">
            <p class="metric-val" style="color:#e74c3c">{metrics.get('failed','—')}</p>
            <p class="metric-lbl">FAILED</p></div>""", unsafe_allow_html=True)
    with col4:
        sr = metrics.get('success_rate_pct', 0) or 0
        st.markdown(f"""<div class="metric-card">
            <p class="metric-val" style="color:#6c63ff">{sr}%</p>
            <p class="metric-lbl">SUCCESS RATE</p></div>""", unsafe_allow_html=True)
    with col5:
        rows = metrics.get('total_rows', 0) or 0
        st.markdown(f"""<div class="metric-card">
            <p class="metric-val" style="color:#00d4aa">{rows:,}</p>
            <p class="metric-lbl">ROWS INGESTED</p></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Charts row 1 ─────────────────────────────────────────────
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Last 7 Days — Daily Runs**")
        if daily:
            df_d = pd.DataFrame(daily)
            df_d["day"] = pd.to_datetime(df_d["day"]).dt.strftime("%d %b")
            fig = go.Figure()
            fig.add_bar(x=df_d["day"], y=df_d.get("success", []), name="Success", marker_color="#27ae60")
            fig.add_bar(x=df_d["day"], y=df_d.get("failed",  []), name="Failed",  marker_color="#e74c3c")
            fig.add_bar(x=df_d["day"], y=df_d.get("skipped", []), name="Skipped", marker_color="#f39c12")
            fig.update_layout(barmode="stack", paper_bgcolor="rgba(0,0,0,0)",
                              plot_bgcolor="rgba(0,0,0,0)", font_color="#e8e8f8",
                              height=260, margin=dict(l=0,r=0,t=10,b=0),
                              legend=dict(orientation="h", y=-0.2))
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(gridcolor="#2a2a4a")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet")

    with c2:
        st.markdown("**Connector Breakdown**")
        if connectors:
            df_c = pd.DataFrame(connectors)
            fig = px.pie(df_c, names="connector_type", values="runs",
                         color_discrete_sequence=["#6c63ff","#00d4aa","#e74c3c","#f39c12","#27ae60","#3498db"])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="#e8e8f8",
                              height=260, margin=dict(l=0,r=0,t=10,b=0),
                              legend=dict(orientation="h", y=-0.15))
            fig.update_traces(textfont_color="#ffffff")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet")

    # ── Charts row 2 ─────────────────────────────────────────────
    c3, c4 = st.columns(2)

    with c3:
        st.markdown("**30-Day Row Volume**")
        if vol_trend:
            df_v = pd.DataFrame(vol_trend)
            df_v["day"] = pd.to_datetime(df_v["day"]).dt.strftime("%d %b")
            fig = px.area(df_v, x="day", y="total_rows",
                          color_discrete_sequence=["#6c63ff"])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                              plot_bgcolor="rgba(0,0,0,0)", font_color="#e8e8f8",
                              height=240, margin=dict(l=0,r=0,t=10,b=0))
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(gridcolor="#2a2a4a")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet")

    with c4:
        st.markdown("**Hourly Activity (Last 24h)**")
        if hourly:
            df_h = pd.DataFrame(hourly)
            df_h["hour"] = pd.to_datetime(df_h["hour"]).dt.strftime("%H:%M")
            fig = go.Figure()
            fig.add_scatter(x=df_h["hour"], y=df_h.get("success",[]), name="Success",
                            line=dict(color="#27ae60"), fill="tozeroy", fillcolor="rgba(39,174,96,0.15)")
            fig.add_scatter(x=df_h["hour"], y=df_h.get("failed", []), name="Failed",
                            line=dict(color="#e74c3c"), fill="tozeroy", fillcolor="rgba(231,76,60,0.15)")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                              plot_bgcolor="rgba(0,0,0,0)", font_color="#e8e8f8",
                              height=240, margin=dict(l=0,r=0,t=10,b=0),
                              legend=dict(orientation="h", y=-0.25))
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(gridcolor="#2a2a4a")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet")

    # ── Pipeline Health Table ─────────────────────────────────────
    st.markdown("---")
    st.markdown("**Pipeline Health — All Pipelines**")
    if pipeline_h:
        df_ph = pd.DataFrame(pipeline_h)
        df_ph["last_run_at"] = df_ph["last_run_at"].apply(fmt_date)
        df_ph["success_rate"] = df_ph["success_rate"].apply(lambda x: f"{x}%" if x else "—")
        df_ph = df_ph[["pipeline_id","connector_type","total_runs","success","failed","success_rate","avg_duration","last_status","last_run_at"]]
        df_ph.columns = ["Pipeline","Connector","Total","Success","Failed","Rate","Avg Dur(s)","Last Status","Last Run"]
        st.dataframe(df_ph, use_container_width=True, hide_index=True)
    else:
        st.info("No pipeline metrics yet — run a pipeline first.")

    # ── Top Failing ───────────────────────────────────────────────
    if top_fail:
        st.markdown("---")
        st.markdown("**⚠️ Top Failing Pipelines (Last 7 Days)**")
        df_tf = pd.DataFrame(top_fail)
        df_tf["last_failed_at"] = df_tf["last_failed_at"].apply(fmt_date)
        df_tf = df_tf[["pipeline_id","fail_count","last_error","last_failed_at"]]
        df_tf.columns = ["Pipeline","Failures","Last Error","Last Failed At"]
        st.dataframe(df_tf, use_container_width=True, hide_index=True)

    # ── Recent Runs ───────────────────────────────────────────────
    st.markdown("---")
    st.markdown("**Recent Runs**")
    if recent:
        df_r = pd.DataFrame(recent)
        df_r["logged_at"] = df_r["logged_at"].apply(fmt_date)
        cols_show = [c for c in ["pipeline_id","connector_type","status","rows_inserted","duration_sec","error_message","logged_at"] if c in df_r.columns]
        st.dataframe(df_r[cols_show], use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════
# PAGE: CREATE PIPELINE
# ═══════════════════════════════════════════════════════════════════
elif page == "➕ Create Pipeline":
    st.markdown('<div class="section-title">Create Pipeline</div>', unsafe_allow_html=True)

    with st.form("create_pipeline_form"):
        st.markdown("#### Basic Config")
        col1, col2 = st.columns(2)
        with col1:
            pipeline_name  = st.text_input("Pipeline Name *", placeholder="sales_data")
            connector_type = st.selectbox("Connector Type *", ["csv","excel","google_sheets","api","postgres","s3"])
            table_name     = st.text_input("Table Name *", placeholder="tbl_sales")
        with col2:
            option   = st.selectbox("Load Option *", ["1 — Append","2 — Overwrite","3 — Create Only"])
            schedule = st.text_input("Cron Schedule", value="*/5 * * * *")
            sync_mode = st.selectbox("Sync Mode", ["full","incremental"])

        if sync_mode == "incremental":
            inc_col = st.text_input("Incremental Column *", placeholder="updated_at")
        else:
            inc_col = None

        opt_val = option.split(" ")[0]
        after_first_run = None
        if opt_val == "3":
            after_first_run = st.selectbox("After First Run", ["1 — Switch to Append","2 — Switch to Overwrite"])
            after_first_run = after_first_run.split(" ")[0]

        st.markdown("---")
        st.markdown("#### Source Config")

        # ── Connector-specific fields ──────────────────────────────
        folder_path = file_path = sheet_url = api_url = None
        src_pg_host = src_pg_db = src_pg_user = src_pg_password = src_pg_port = pg_query = None
        s3_bucket = s3_key = s3_file_type = None

        if connector_type in ("csv","excel"):
            col_a, col_b = st.columns(2)
            with col_a:
                folder_path = st.text_input("Folder Path", placeholder="/opt/airflow/dataset_win/sales")
            with col_b:
                file_path = st.text_input("File Path (single file)", placeholder="/opt/airflow/dataset_win/sales/data.csv")
            st.markdown('<div class="info-box">💡 Provide folder path or file path. If you provide a folder path, all CSV/Excel files in that folder will be processed.</div>', unsafe_allow_html=True)

        elif connector_type == "google_sheets":
            sheet_url = st.text_input("Google Sheet URL *", placeholder="https://docs.google.com/spreadsheets/d/...")
            st.markdown('<div class="info-box">💡 Sheet will be public — Share → Anyone with link → Viewer</div>', unsafe_allow_html=True)

        elif connector_type == "api":
            api_url = st.text_input("API URL *", placeholder="https://api.example.com/data")

        elif connector_type == "postgres":
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                src_pg_host = st.text_input("Host *")
                src_pg_db   = st.text_input("Database *")
            with col_b:
                src_pg_user     = st.text_input("User *")
                src_pg_password = st.text_input("Password *", type="password")
            with col_c:
                src_pg_port = st.text_input("Port", value="5432")
            pg_query = st.text_area("SQL Query *", placeholder="SELECT * FROM employees WHERE active = true")

        elif connector_type == "s3":
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                s3_bucket    = st.text_input("Bucket Name *")
            with col_b:
                s3_key       = st.text_input("Key / Prefix *", placeholder="folder/file.csv or folder/")
            with col_c:
                s3_file_type = st.selectbox("File Type", ["csv","xlsx","parquet","json"])

        submitted = st.form_submit_button("🚀 Create Pipeline", use_container_width=True, type="primary")

    if submitted:
        payload = {
            "pipeline_name":      pipeline_name,
            "connector_type":     connector_type,
            "table_name":         table_name,
            "option":             opt_val,
            "schedule":           schedule,
            "sync_mode":          sync_mode,
            "incremental_column": inc_col,
            "after_first_run":    after_first_run,
            "folder_path":        folder_path or None,
            "file_path":          file_path   or None,
            "sheet_url":          sheet_url   or None,
            "api_url":            api_url     or None,
            "src_pg_host":        src_pg_host,
            "src_pg_db":          src_pg_db,
            "src_pg_user":        src_pg_user,
            "src_pg_password":    src_pg_password,
            "src_pg_port":        src_pg_port,
            "pg_query":           pg_query,
            "s3_bucket":          s3_bucket,
            "s3_key":             s3_key,
            "s3_file_type":       s3_file_type,
        }
        code, res = api("post", "/create_pipeline", json=payload)
        if code == 200 and res.get("status") == "SUCCESS":
            st.markdown(f'<div class="success-box">✅ Pipeline created: <b>{res.get("dag_id")}</b><br>Airflow will pick it up in ~30 seconds.</div>', unsafe_allow_html=True)
            st.json(res)
        else:
            st.markdown(f'<div class="error-box">❌ Failed: {res}</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
# PAGE: MANAGE PIPELINES
# ═══════════════════════════════════════════════════════════════════
elif page == "📋 Manage Pipelines":
    st.markdown('<div class="section-title">Manage Pipelines</div>', unsafe_allow_html=True)

    col_r, col_f = st.columns([1, 3])
    with col_r:
        if st.button("🔄 Refresh"):
            st.rerun()

    code, data = api("get", "/pipelines")
    pipelines  = data.get("pipelines", []) if code == 200 else []

    if not pipelines:
        st.markdown('<div class="warn-box">No pipelines found. Create one first.</div>', unsafe_allow_html=True)
        st.stop()

    # Get status for all
    all_rows = []
    for p in pipelines:
        name    = p["dag_id"].replace("pipeline_", "")
        sc, sd  = api("get", f"/pipeline/{name}/status")
        status  = sd.get("status", "UNKNOWN") if sc == 200 else "UNKNOWN"
        next_r  = sd.get("next_run", "—")     if sc == 200 else "—"
        all_rows.append({
            "dag_id":   p["dag_id"],
            "name":     name,
            "status":   status,
            "next_run": fmt_date(next_r),
            "size_kb":  p.get("size_kb","—"),
        })

    # Summary row
    total   = len(all_rows)
    active  = sum(1 for r in all_rows if r["status"] == "ACTIVE")
    paused  = sum(1 for r in all_rows if r["status"] == "PAUSED")

    m1, m2, m3 = st.columns(3)
    m1.metric("Total", total)
    m2.metric("Active", active)
    m3.metric("Paused", paused)

    st.markdown("---")

    # Filter
    # filt = st.selectbox("Filter", ["All","Active","Paused"], horizontal=True)
    filt = st.radio(
    "Filter",
    ["All", "Active", "Paused"],
    horizontal=True,
    index=0   # 👈 default = "All"
    )
    search = st.text_input("Search pipeline name", placeholder="spark...")

    show = [r for r in all_rows
            if (filt == "All" or r["status"] == filt.upper())
            and (not search or search.lower() in r["dag_id"].lower())]

    for row in show:
        with st.container():
            c1, c2, c3, c4, c5, c6 = st.columns([3, 1.5, 1.5, 1, 1, 1])
            with c1:
                st.markdown(f"**{row['dag_id']}**")
                st.caption(f"Next run: {row['next_run']} · {row['size_kb']} KB")
            with c2:
                st.markdown(badge(row["status"]), unsafe_allow_html=True)
            with c3:
                # Schedule cron preset
                pass
            with c4:
                if row["status"] == "ACTIVE":
                    if st.button("⏸ Pause", key=f"pause_{row['name']}"):
                        c, d = api("patch", f"/pipeline/{row['name']}/pause")
                        st.success("Paused") if c == 200 else st.error(str(d))
                        time.sleep(0.5); st.rerun()
                else:
                    if st.button("▶ Resume", key=f"resume_{row['name']}"):
                        c, d = api("patch", f"/pipeline/{row['name']}/unpause")
                        st.success("Resumed") if c == 200 else st.error(str(d))
                        time.sleep(0.5); st.rerun()
            with c5:
                if st.button("📜 Logs", key=f"logs_{row['name']}"):
                    st.session_state["log_pipeline"] = row["name"]
            with c6:
                if st.button("🗑 Delete", key=f"del_{row['name']}"):
                    c, d = api("delete", f"/delete_pipeline/{row['name']}")
                    st.success("Deleted") if c == 200 else st.error(str(d))
                    time.sleep(0.5); st.rerun()
            st.markdown("---")

    # ── Pipeline run history ──────────────────────────────────────
    st.markdown("### Pipeline Run History")
    selected_pipe = st.selectbox("Select pipeline", [r["name"] for r in all_rows])
    if selected_pipe:
        hc, hd = api("get", f"/pipeline/{selected_pipe}/runs", params={"limit": 20})
        if hc == 200:
            stats = hd.get("stats", {})
            s1, s2, s3, s4 = st.columns(4)
            s1.metric("Total Runs",  stats.get("total_runs", 0))
            s2.metric("Success",     stats.get("success_count", 0))
            s3.metric("Failed",      stats.get("failed_count", 0))
            s4.metric("Last Run",    fmt_date(stats.get("last_run_at")))

            runs = hd.get("runs", [])
            if runs:
                df_runs = pd.DataFrame(runs)
                show_cols = [c for c in ["dag_run_id","status","operation","execution_date","created_at"] if c in df_runs.columns]
                df_runs["created_at"] = df_runs["created_at"].apply(fmt_date)
                st.dataframe(df_runs[show_cols], use_container_width=True, hide_index=True)
        else:
            st.error(f"Could not fetch runs: {hd}")


# ═══════════════════════════════════════════════════════════════════
# PAGE: DIRECT INGEST
# ═══════════════════════════════════════════════════════════════════
elif page == "📥 Direct Ingest":
    st.markdown('<div class="section-title">Direct Ingest</div>', unsafe_allow_html=True)
    st.markdown('<div class="info-box">💡 Direct Ingest data useful for one-time loads.</div>', unsafe_allow_html=True)

    connector = st.selectbox("Connector Type", ["csv","excel","google_sheets","api","postgres","s3"])

    with st.form("direct_ingest_form"):
        col1, col2 = st.columns(2)
        with col1:
            table_name = st.text_input("Table Name *", placeholder="tbl_sales")
            option     = st.selectbox("Load Option", ["1 — Append","2 — Overwrite","3 — Create Only"])
        with col2:
            sync_mode  = st.selectbox("Sync Mode", ["full","incremental"])
            inc_col    = st.text_input("Incremental Column", placeholder="updated_at") if sync_mode == "incremental" else None

        opt_val = option.split(" ")[0]

        st.markdown("---")
        endpoint = None
        payload  = {"option": opt_val, "table_name": table_name, "sync_mode": sync_mode, "incremental_column": inc_col}

        if connector == "csv":
            payload["file_path"] = st.text_input("File Path *", placeholder="/opt/airflow/dataset_win/data.csv")
            endpoint = "/ingest_csv"

        elif connector == "excel":
            payload["file_path"] = st.text_input("File Path *", placeholder="/opt/airflow/dataset_win/data.xlsx")
            endpoint = "/ingest_excel"

        elif connector == "google_sheets":
            payload["sheet_url"] = st.text_input("Sheet URL *")
            endpoint = "/ingest_google_sheet"

        elif connector == "api":
            payload["url"] = st.text_input("API URL *")
            endpoint = "/ingest_api"

        elif connector == "postgres":
            c1, c2, c3 = st.columns(3)
            with c1:
                payload["host"]     = st.text_input("Host *")
                payload["database"] = st.text_input("Database *")
            with c2:
                payload["user"]     = st.text_input("User *")
                payload["password"] = st.text_input("Password *", type="password")
            with c3:
                payload["port"]     = st.text_input("Port", value="5432")
            payload["query"]        = st.text_area("SQL Query *")
            endpoint = "/ingest_postgres"

        elif connector == "s3":
            c1, c2, c3 = st.columns(3)
            with c1: payload["bucket"]    = st.text_input("Bucket *")
            with c2: payload["key"]       = st.text_input("Key / Prefix *")
            with c3: payload["file_type"] = st.selectbox("File Type", ["csv","xlsx","parquet","json"])
            endpoint = "/ingest_s3"

        go = st.form_submit_button("⚡ Run Ingest Now", use_container_width=True, type="primary")

    if go and endpoint:
        with st.spinner("Ingesting data..."):
            code, res = api("post", endpoint, json=payload)
        if code == 200 and res.get("status") == "SUCCESS":
            st.markdown(f'<div class="success-box">✅ Success! {res.get("rows","—")} rows ingested. Run ID: {res.get("run_id","—")}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="error-box">❌ Failed: {res.get("error", res)}</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
# PAGE: DATA PREVIEW
# ═══════════════════════════════════════════════════════════════════
elif page == "🗄️ Data Preview":
    st.markdown('<div class="section-title">Data Preview</div>', unsafe_allow_html=True)

    col1, col2 = st.columns([3, 1])
    with col1:
        table_name = st.text_input("Table Name", placeholder="tbl_sales")
    with col2:
        limit = st.number_input("Rows per page", min_value=10, max_value=1000, value=50)

    if table_name:
        # Filters & sort
        with st.expander("🔍 Filter & Sort"):
            fc1, fc2, fc3, fc4 = st.columns(4)
            with fc1: filter_col = st.text_input("Filter Column", placeholder="status")
            with fc2: filter_val = st.text_input("Filter Value",  placeholder="active")
            with fc3: sort_by    = st.text_input("Sort By Column", placeholder="created_at")
            with fc4: order      = st.selectbox("Order", ["asc","desc"])

        # Pagination
        if "table_offset" not in st.session_state:
            st.session_state.table_offset = 0

        params = {
            "limit":      limit,
            "offset":     st.session_state.table_offset,
            "sort_by":    sort_by    if sort_by    else None,
            "order":      order,
            "filter_col": filter_col if filter_col else None,
            "filter_val": filter_val if filter_val else None,
        }
        params = {k: v for k, v in params.items() if v is not None}

        code, data = api("get", f"/table/{table_name}", params=params)

        if code == 404:
            st.markdown(f'<div class="warn-box">Table "{table_name}" not found.</div>', unsafe_allow_html=True)
        elif code != 200:
            st.markdown(f'<div class="error-box">Error: {data}</div>', unsafe_allow_html=True)
        else:
            pag   = data.get("pagination", {})
            total = pag.get("total", 0)
            pages = pag.get("pages", 1)
            page_no = pag.get("page", 1)

            # Stats
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Rows",    total)
            m2.metric("Columns",       len(data.get("columns", [])))
            m3.metric("Current Page",  page_no)
            m4.metric("Total Pages",   pages)

            # Table
            rows = data.get("data", [])
            if rows:
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True, hide_index=True)

                # Download
                csv_data = df.to_csv(index=False)
                st.download_button("⬇️ Download CSV", csv_data,
                                   file_name=f"{table_name}_page{page_no}.csv",
                                   mime="text/csv")
            else:
                st.info("No rows match your filter.")

            # Pagination controls
            pc1, pc2, pc3 = st.columns([1, 2, 1])
            with pc1:
                if st.button("◀ Previous") and st.session_state.table_offset >= limit:
                    st.session_state.table_offset -= limit
                    st.rerun()
            with pc2:
                st.markdown(f"<div style='text-align:center; padding-top:8px; color:#6b6b90;'>Page {page_no} of {pages} · {total:,} total rows</div>", unsafe_allow_html=True)
            with pc3:
                if st.button("Next ▶") and pag.get("has_more"):
                    st.session_state.table_offset += limit
                    st.rerun()


# ═══════════════════════════════════════════════════════════════════
# PAGE: METRICS
# ═══════════════════════════════════════════════════════════════════
elif page == "📈 Metrics":
    st.markdown('<div class="section-title">Pipeline Metrics</div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["All Pipelines Summary", "Single Pipeline Deep Dive"])

    with tab1:
        code, data = api("get", "/metrics/summary/all")
        if code == 200:
            summary = data.get("summary", [])
            if summary:
                df_s = pd.DataFrame(summary)
                df_s["last_run_at"] = df_s["last_run_at"].apply(fmt_date)

                # Success rate chart
                if "success_count" in df_s.columns and "total_runs" in df_s.columns:
                    df_s["success_rate"] = (df_s["success_count"] / df_s["total_runs"].replace(0,1) * 100).round(1)
                    fig = px.bar(df_s, x="pipeline_id", y="success_rate",
                                 color="connector_type", title="Success Rate by Pipeline",
                                 color_discrete_sequence=px.colors.qualitative.Bold)
                    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                                      plot_bgcolor="rgba(0,0,0,0)", font_color="#e8e8f8",
                                      height=300, margin=dict(l=0,r=0,t=30,b=0))
                    fig.update_yaxes(range=[0,100], gridcolor="#2a2a4a")
                    fig.update_xaxes(showgrid=False)
                    st.plotly_chart(fig, use_container_width=True)

                st.dataframe(df_s, use_container_width=True, hide_index=True)
            else:
                st.info("No metrics yet.")
        else:
            st.error(f"Could not load metrics: {data}")

    with tab2:
        pipeline_id = st.text_input("Pipeline ID", placeholder="pipeline_sales_data")
        n_runs = st.slider("Last N runs", 5, 50, 20)

        if pipeline_id:
            code, data = api("get", f"/metrics/{pipeline_id}", params={"limit": n_runs})
            if code == 200:
                runs = data.get("runs", [])
                if runs:
                    df_m = pd.DataFrame(runs)

                    # Duration over time
                    if "duration_sec" in df_m.columns:
                        df_m["logged_at_fmt"] = df_m["logged_at"].apply(fmt_date)
                        fig = go.Figure()
                        fig.add_scatter(x=df_m["logged_at_fmt"], y=df_m["duration_sec"],
                                        mode="lines+markers", name="Duration",
                                        line=dict(color="#6c63ff"))
                        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                                          plot_bgcolor="rgba(0,0,0,0)", font_color="#e8e8f8",
                                          height=250, margin=dict(l=0,r=0,t=10,b=0),
                                          title="Duration per Run (seconds)")
                        fig.update_xaxes(showgrid=False)
                        fig.update_yaxes(gridcolor="#2a2a4a")
                        st.plotly_chart(fig, use_container_width=True)

                    # Rows inserted
                    if "rows_inserted" in df_m.columns:
                        fig2 = px.bar(df_m, x="logged_at_fmt", y="rows_inserted",
                                      color="status", title="Rows Inserted per Run",
                                      color_discrete_map={"SUCCESS":"#27ae60","FAILED":"#e74c3c","SKIPPED":"#f39c12"})
                        fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                                           plot_bgcolor="rgba(0,0,0,0)", font_color="#e8e8f8",
                                           height=250, margin=dict(l=0,r=0,t=30,b=0))
                        fig2.update_xaxes(showgrid=False)
                        fig2.update_yaxes(gridcolor="#2a2a4a")
                        st.plotly_chart(fig2, use_container_width=True)

                    show_cols = [c for c in ["status","rows_inserted","duration_sec","match_pct","evolved_columns","error_message","logged_at"] if c in df_m.columns]
                    df_m["logged_at"] = df_m["logged_at"].apply(fmt_date)
                    st.dataframe(df_m[show_cols], use_container_width=True, hide_index=True)
                else:
                    st.info("No runs found for this pipeline.")
            else:
                st.error(f"Error: {data}")


# ═══════════════════════════════════════════════════════════════════
# PAGE: LOGS
# ═══════════════════════════════════════════════════════════════════
elif page == "📜 Logs":
    st.markdown('<div class="section-title">Pipeline Logs</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        log_pipeline = st.text_input("Pipeline Name", value=st.session_state.get("log_pipeline",""), placeholder="spark1")
    with col2:
        dag_run_id   = st.text_input("DAG Run ID (optional)", placeholder="run__pipeline_spark1__20260413...")

    if log_pipeline:
        endpoint = f"/pipeline/{log_pipeline}/logs"
        if dag_run_id:
            endpoint += f"/{dag_run_id}"

        code, data = api("get", endpoint)

        if code == 200:
            col_a, col_b, col_c = st.columns(3)
            col_a.markdown(f"**Source:** `{data.get('source','—')}`")
            col_b.markdown(badge(data.get('status','UNKNOWN')), unsafe_allow_html=True)
            col_c.markdown(f"**Logged:** {fmt_date(data.get('logged_at'))}")

            if data.get("log_file"):
                st.caption(f"Log file: `{data['log_file']}`")

            log_content = data.get("log","No log content found.")
            st.text_area("Log Output", value=log_content, height=500)

            if log_content:
                st.download_button("⬇️ Download Log", log_content,
                                   file_name=f"{log_pipeline}_{dag_run_id or 'latest'}.log",
                                   mime="text/plain")
        elif code == 404:
            st.markdown('<div class="warn-box">No logs found. Has the pipeline run yet?</div>', unsafe_allow_html=True)
        else:
            st.error(f"Error: {data}")

    # ── All pipeline runs ─────────────────────────────────────────
    st.markdown("---")
    st.markdown("### All Pipeline Runs (DB)")
    code, data = api("get", "/runs")
    if code == 200 and data:
        df_all = pd.DataFrame(data)
        if "start_time" in df_all.columns:
            df_all["start_time"] = df_all["start_time"].apply(fmt_date)
        if "end_time" in df_all.columns:
            df_all["end_time"] = df_all["end_time"].apply(fmt_date)
        st.dataframe(df_all, use_container_width=True, hide_index=True)
    else:
        st.info("No runs found.")


# ═══════════════════════════════════════════════════════════════════
# PAGE: MULTI-SOURCE PIPELINE
# ═══════════════════════════════════════════════════════════════════
elif page == "🔗 Multi-Source Pipeline":
    st.markdown('<div class="section-title">Multi-Source Pipeline</div>', unsafe_allow_html=True)
    st.markdown('<div class="info-box">💡 Merge data from multiple sources into a single table. The first source option is yours, the rest will be automatically appended.</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        ms_name  = st.text_input("Pipeline Name *", placeholder="sales_combined")
        ms_table = st.text_input("Target Table *",  placeholder="tbl_sales_all")
    with col2:
        ms_option   = st.selectbox("First Source Option", ["1 — Append","2 — Overwrite"])
        ms_schedule = st.text_input("Schedule", value="*/5 * * * *")
    with col3:
        ms_sync = st.selectbox("Sync Mode", ["full","incremental"])
        ms_inc  = st.text_input("Incremental Column") if ms_sync == "incremental" else None

    st.markdown("---")
    st.markdown("### Sources")

    if "ms_sources" not in st.session_state:
        st.session_state.ms_sources = [{"connector_type": "csv"}]

    # Add / remove sources
    if st.button("➕ Add Source"):
        st.session_state.ms_sources.append({"connector_type": "csv"})
        st.rerun()

    sources_payload = []
    for i, src in enumerate(st.session_state.ms_sources):
        with st.expander(f"Source {i+1}", expanded=True):
            col_a, col_b = st.columns([2,3])
            with col_a:
                ct = st.selectbox("Type", ["csv","excel","google_sheets","api","postgres","s3"], key=f"ms_ct_{i}")
            s = {"connector_type": ct}

            with col_b:
                if ct in ("csv","excel"):
                    s["folder_path"] = st.text_input("Folder/File Path", key=f"ms_fp_{i}", placeholder="/opt/airflow/dataset_win/...")
                elif ct == "google_sheets":
                    s["sheet_url"] = st.text_input("Sheet URL", key=f"ms_su_{i}")
                elif ct == "api":
                    s["api_url"] = st.text_input("API URL", key=f"ms_au_{i}")
                elif ct == "s3":
                    cs1, cs2, cs3 = st.columns(3)
                    with cs1: s["s3_bucket"]    = st.text_input("Bucket", key=f"ms_sb_{i}")
                    with cs2: s["s3_key"]       = st.text_input("Key",    key=f"ms_sk_{i}")
                    with cs3: s["s3_file_type"] = st.selectbox("Type", ["csv","xlsx","parquet","json"], key=f"ms_st_{i}")
                elif ct == "postgres":
                    s["src_pg_host"]     = st.text_input("Host",     key=f"ms_ph_{i}")
                    s["src_pg_db"]       = st.text_input("Database", key=f"ms_pd_{i}")
                    s["src_pg_user"]     = st.text_input("User",     key=f"ms_pu_{i}")
                    s["src_pg_password"] = st.text_input("Password", key=f"ms_pp_{i}", type="password")
                    s["pg_query"]        = st.text_area("Query",     key=f"ms_pq_{i}")

            if i > 0:
                if st.button(f"🗑 Remove Source {i+1}", key=f"ms_rm_{i}"):
                    st.session_state.ms_sources.pop(i)
                    st.rerun()

            sources_payload.append(s)

    st.markdown("---")
    if st.button("🚀 Create Multi-Source Pipeline", type="primary", use_container_width=True):
        payload = {
            "pipeline_name": ms_name,
            "table_name":    ms_table,
            "option":        ms_option.split(" ")[0],
            "schedule":      ms_schedule,
            "sync_mode":     ms_sync,
            "incremental_column": ms_inc,
            "sources":       sources_payload,
        }
        code, res = api("post", "/create_multi_pipeline", json=payload)
        if code == 200 and res.get("status") == "SUCCESS":
            st.markdown(f'<div class="success-box">✅ Multi-source pipeline created: <b>{res.get("dag_id")}</b><br>{res.get("sources_count")} sources → table "{ms_table}"</div>', unsafe_allow_html=True)
            st.json(res)
        else:
            st.markdown(f'<div class="error-box">❌ Failed: {res}</div>', unsafe_allow_html=True)