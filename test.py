import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
import networkx as nx
from streamlit_agraph import agraph, Node, Edge, Config

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Data Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# PREMIUM CSS (Databricks + Airflow Inspired)
# ─────────────────────────────────────────────
st.markdown("""
<style>
.stApp {
    background: linear-gradient(135deg, #0b0b15, #111122);
    color: #e8e8f8;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0d0d1f;
    border-right: 1px solid #222244;
}

/* Sidebar nav */
.sidebar-item {
    padding: 10px 14px;
    border-radius: 8px;
    margin-bottom: 6px;
    cursor: pointer;
    transition: 0.2s;
}
.sidebar-item:hover {
    background: #1c1c3a;
}
.active {
    background: #6c63ff !important;
    color: white !important;
}

/* Cards */
.metric-card {
    background: #151530;
    border: 1px solid #26264a;
    border-radius: 14px;
    padding: 18px;
    transition: 0.3s;
}
.metric-card:hover {
    transform: translateY(-3px);
}

/* Logs */
.log-box {
    background: #000000;
    color: #00ffcc;
    padding: 12px;
    border-radius: 10px;
    font-family: monospace;
    height: 400px;
    overflow-y: auto;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# SESSION
# ─────────────────────────────────────────────
if "api_base" not in st.session_state:
    st.session_state.api_base = "http://localhost:8000"

if "page" not in st.session_state:
    st.session_state.page = "Dashboard"

# ─────────────────────────────────────────────
# API HELPER
# ─────────────────────────────────────────────
def api(method, endpoint, **kwargs):
    try:
        url = f"{st.session_state.api_base}{endpoint}"
        res = getattr(requests, method)(url, timeout=30, **kwargs)
        return res.status_code, res.json()
    except:
        return 0, {}

# ─────────────────────────────────────────────
# SIDEBAR NAV (PREMIUM)
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ Data Platform")

    pages = [
        "Dashboard",
        "Create Pipeline",
        "Manage Pipelines",
        "DAG Viewer",
        "Logs",
    ]

    for p in pages:
        active = "active" if st.session_state.page == p else ""
        if st.markdown(f"<div class='sidebar-item {active}'>{p}</div>", unsafe_allow_html=True):
            st.session_state.page = p

# ─────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────
if st.session_state.page == "Dashboard":
    st.title("📊 Dashboard")

    code, data = api("get", "/dashboard/summary")

    col1, col2, col3, col4 = st.columns(4)

    col1.markdown(f"<div class='metric-card'>Runs<br><h2>{data.get('metrics',{}).get('total_runs',0)}</h2></div>", unsafe_allow_html=True)
    col2.markdown(f"<div class='metric-card'>Success<br><h2>{data.get('metrics',{}).get('success',0)}</h2></div>", unsafe_allow_html=True)
    col3.markdown(f"<div class='metric-card'>Failed<br><h2>{data.get('metrics',{}).get('failed',0)}</h2></div>", unsafe_allow_html=True)
    col4.markdown(f"<div class='metric-card'>Rows<br><h2>{data.get('metrics',{}).get('total_rows',0)}</h2></div>", unsafe_allow_html=True)

    st.markdown("### 📈 Trends")

    if "daily" in data:
        df = pd.DataFrame(data["daily"])
        fig = px.line(df, x="day", y="success")
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────
# CREATE PIPELINE
# ─────────────────────────────────────────────
elif st.session_state.page == "Create Pipeline":
    st.title("➕ Create Pipeline")

    with st.form("create"):
        name = st.text_input("Pipeline Name")
        table = st.text_input("Table Name")
        connector = st.selectbox("Connector", ["csv","api","postgres"])

        submit = st.form_submit_button("Create")

    if submit:
        payload = {"pipeline_name": name, "table_name": table, "connector_type": connector}
        code, res = api("post", "/create_pipeline", json=payload)
        st.success(res)

# ─────────────────────────────────────────────
# MANAGE PIPELINES
# ─────────────────────────────────────────────
elif st.session_state.page == "Manage Pipelines":
    st.title("📋 Pipelines")

    code, data = api("get", "/pipelines")

    for p in data.get("pipelines", []):
        col1, col2, col3 = st.columns([4,1,1])

        col1.write(p["dag_id"])

        if col2.button("▶ Run", key=p["dag_id"]):
            api("post", f"/pipeline/{p['dag_id']}/run")

        if col3.button("🗑 Delete", key=p["dag_id"]+"_d"):
            api("delete", f"/delete_pipeline/{p['dag_id']}")

# ─────────────────────────────────────────────
# DAG VIEWER (🔥 AIRFLOW STYLE)
# ─────────────────────────────────────────────
elif st.session_state.page == "DAG Viewer":
    st.title("🔗 DAG Viewer")

    dag_id = st.text_input("Pipeline Name")

    if dag_id:
        code, data = api("get", f"/pipeline/{dag_id}/dag")

        if code == 200:
            nodes = [Node(id=n, label=n, size=25) for n in data["nodes"]]
            edges = [Edge(source=e[0], target=e[1]) for e in data["edges"]]

            config = Config(width="100%", height=500, directed=True)

            agraph(nodes=nodes, edges=edges, config=config)

# ─────────────────────────────────────────────
# REAL-TIME LOG STREAMING 🔥
# ─────────────────────────────────────────────
elif st.session_state.page == "Logs":
    st.title("📜 Live Logs")

    pipeline = st.text_input("Pipeline Name")

    placeholder = st.empty()

    if st.button("Start Streaming"):
        while True:
            code, data = api("get", f"/pipeline/{pipeline}/logs")

            logs = data.get("log", "")

            placeholder.markdown(f"<div class='log-box'>{logs}</div>", unsafe_allow_html=True)

            time.sleep(2)