# ui.py
import streamlit as st
import pandas as pd
import requests
import json
import time
from pathlib import Path
import os
from dotenv import load_dotenv
import boto3

# ==========================================================
# 🔧 LOAD ENV VARIABLES
# ==========================================================
load_dotenv()  # Make sure your .env is in the same folder or provide full path

BACKEND_URL = os.getenv("FRONTEND_BACKEND_URL") or "http://fastapi:8000/api/v1"

DEFAULT_DB_PARAMS = {
    "dbname": os.getenv("CVAT_DB_NAME") or "cvat_annotations_db",
    "user": os.getenv("CVAT_DB_USER") or "admin",
    "password": os.getenv("CVAT_DB_PASSWORD") or "admin",
    "host": os.getenv("CVAT_DB_HOST") or "localhost",
    "port": os.getenv("CVAT_DB_PORT") or "55432",
}

DEFAULT_CVAT_HOST = os.getenv("CVAT_HOST") or "http://localhost:8080"
DEFAULT_CVAT_USER = os.getenv("CVAT_USERNAME") or "Strawhat03"
DEFAULT_CVAT_PASS = os.getenv("CVAT_PASSWORD") or "Test@123"
DEFAULT_S3_BUCKET = os.getenv("AWS_STORAGE_BUCKET_NAME") or "ava-kine-data"

# ==========================================================
# ♻️ RERUN HELPER
# ==========================================================
def rerun(force: bool = True):
    if force:
        st.cache_data.clear()
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

# ==========================================================
# 🌐 API CALL FUNCTION
# ==========================================================
def call_api(method, endpoint, json_data=None):
    url = f"{BACKEND_URL}{endpoint}"
    try:
        response = requests.request(method, url, json=json_data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        try:
            detail = e.response.json().get("detail", "Unknown error")
        except:
            detail = e.response.text
        st.error(f"API Error ({e.response.status_code}): {detail}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Connection Error: Could not connect to backend at {BACKEND_URL}.")
        st.exception(e)
        return None

# ==========================================================
# 🖥️ PAGE CONFIG
# ==========================================================
st.set_page_config(page_title="AVA-Kinetics Pipeline UI", layout="wide")
st.title("🚀 Integrated AVA-Kinetics Pipeline UI")
st.markdown("---")

# ==========================================================
# 🧭 TABS
# ==========================================================
tab_qc, tab_creator = st.tabs(["✅ QC & Dataset Generator", "🚀 CVAT Task Creator (S3)"])

# ==========================================================
# ⚙️ SIDEBAR CONFIG
# ==========================================================
st.sidebar.header("⚙️ DB Configuration (QC/Generator)")
db_params = {
    "dbname": st.sidebar.text_input("DB Name", DEFAULT_DB_PARAMS["dbname"]),
    "user": st.sidebar.text_input("DB User", DEFAULT_DB_PARAMS["user"]),
    "password": st.sidebar.text_input("DB Password", DEFAULT_DB_PARAMS["password"], type="password"),
    "host": st.sidebar.text_input("DB Host", DEFAULT_DB_PARAMS["host"]),
    "port": st.sidebar.text_input("DB Port", DEFAULT_DB_PARAMS["port"]),
}

st.sidebar.markdown("---")
st.sidebar.header("⚙️ CVAT/S3 Config (Task Creator)")
cvat_host = st.sidebar.text_input("CVAT Host URL", DEFAULT_CVAT_HOST)
cvat_user = st.sidebar.text_input("CVAT Username", DEFAULT_CVAT_USER)
cvat_pass = st.sidebar.text_input("CVAT Password", DEFAULT_CVAT_PASS, type="password")
s3_bucket_name_sidebar = st.sidebar.text_input("S3 Bucket Name", DEFAULT_S3_BUCKET)

# ==========================================================
# 🔄 HELPER FUNCTIONS
# ==========================================================
@st.cache_data(show_spinner="Fetching projects...")
def fetch_projects_api(params):
    return call_api("POST", "/qc/projects", json_data=params)

def load_pending_tasks(db_params, project_id):
    tasks = call_api("POST", f"/qc/pending_tasks/{project_id}", json_data=db_params)
    st.session_state["task_data"] = tasks or []

def list_s3_batches(bucket_name):
    """Fetch top-level batch folders from S3."""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    batches = set()
    for page in paginator.paginate(Bucket=bucket_name, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            batches.add(prefix.get("Prefix").rstrip("/"))
    return sorted(list(batches))

# ==========================================================
# 🧩 TAB 2: CVAT TASK CREATOR (S3)
# ==========================================================
with tab_creator:
    st.header("🧱 CVAT Task Creator (S3 Direct)")
    st.markdown("This tool connects to CVAT, lists clips from S3, and creates tasks for a project.")
    
    # Step 1: Select S3 bucket and fetch batch names
    st.subheader("Step 1: Select S3 Batch")
    s3_bucket_name = st.text_input("S3 Bucket Name", value=s3_bucket_name_sidebar)
    batch_name = None
    if s3_bucket_name:
        try:
            batches = list_s3_batches(s3_bucket_name)
            if batches:
                batch_name = st.selectbox("Select Batch", batches)
            else:
                st.warning("⚠️ No batches found in the bucket.")
        except Exception as e:
            st.error(f"Failed to list S3 batches: {e}")

    # Step 2: Clip selection (optional: auto fetch)
    clip_names_input = st.text_area(
        "Clip ZIP File Names (comma-separated)",
        value=st.session_state.get("s3_clip_names", "")
    )

    # Step 3: Create CVAT Project
    st.subheader("Step 2: Create CVAT Project & Tasks")
    project_name = st.text_input("New CVAT Project Name", f"S3_Project_{int(time.time())}")

    if st.button("🚀 Create Project from S3"):
        if not s3_bucket_name:
            st.error("❌ Enter S3 Bucket Name.")
        elif not batch_name:
            st.error("❌ Select a batch from S3.")
        elif not all([cvat_host, cvat_user, cvat_pass]):
            st.error("❌ Fill all CVAT credentials.")
        else:
            clip_list = [c.strip() for c in clip_names_input.split(",") if c.strip()]
            payload = {
                "project_name": project_name,
                "batch_name": batch_name,
                "s3_bucket": s3_bucket_name,
                "host": cvat_host,
                "username": cvat_user,
                "password": cvat_pass,
            }
            with st.spinner("Creating CVAT project and tasks from S3..."):
                resp = call_api("POST", "/task-creator/create_project_and_tasks_s3", json_data=payload)
                if resp:
                    st.success("✅ Project and tasks created successfully!")
                    st.json(resp)
                    st.session_state["needs_project_refresh"] = True
                    st.session_state["s3_clip_names"] = ""
                    time.sleep(0.1)
                    rerun()
