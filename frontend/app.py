import streamlit as st
import pandas as pd
import requests
import json
import time
from pathlib import Path
import os
import sys

# ==========================================================
# 🔧 CONFIGURATION
# ==========================================================
BACKEND_URL = "http://fastapi:8000/api/v1"

# ==========================================================
# ♻️ COMPATIBILITY-SAFE RERUN HELPER
# ==========================================================
def rerun(force: bool = True):
    """Streamlit rerun helper that always works across versions."""
    if force:
        st.cache_data.clear()
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

# ==========================================================
# 🌐 UTILITY FUNCTION FOR API CALLS
# ==========================================================
def call_api(method, endpoint, json_data=None):
    """Handles API calls to the FastAPI backend with correct prefix."""
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
        st.error(f"Connection Error: Could not connect to backend at {BACKEND_URL}. Is FastAPI running?")
        st.exception(e)
        return None

# ==========================================================
# 🖥️ MAIN PAGE CONFIGURATION
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
st.sidebar.header("⚙️ DB Configuration (for QC/Generator)")
db_params = {
    "dbname": st.sidebar.text_input("DB Name", "cvat_annotations_db"),
    "user": st.sidebar.text_input("DB User", "admin"),
    "password": st.sidebar.text_input("DB Password", "admin", type="password"),
    "host": st.sidebar.text_input("DB Host", "localhost"),
    "port": st.sidebar.text_input("DB Port", "55432"),
}

st.sidebar.markdown("---")
st.sidebar.header("⚙️ CVAT/S3 Config (for Task Creator)")
cvat_host = st.sidebar.text_input("CVAT Host URL", "http://localhost:8080")
cvat_user = st.sidebar.text_input("CVAT Username", "Strawhat03")
cvat_pass = st.sidebar.text_input("CVAT Password", "Test@123", type="password")
s3_bucket_name_sidebar = st.sidebar.text_input("S3 Bucket Name", "ava-kinetics-packages")

# ==========================================================
# 🔄 HELPER FUNCTIONS
# ==========================================================
@st.cache_data(show_spinner="Fetching projects...")
def fetch_projects_api(params):
    """Fetch project list (cached)."""
    return call_api("POST", "/qc/projects", json_data=params)

def load_pending_tasks(db_params, project_id):
    """Fetch tasks (not cached)."""
    try:
        tasks = call_api("POST", f"/qc/pending_tasks/{project_id}", json_data=db_params)
        st.session_state["task_data"] = tasks or []
    except Exception as e:
        st.error(f"Failed to load tasks: {e}")
        st.session_state["task_data"] = []

# ==========================================================
# 🧩 TAB 1: QC & DATASET GENERATOR
# ==========================================================
with tab_qc:
    st.header("1️⃣ Quality Control Approval")

    # Initialize session state
    st.session_state.setdefault("needs_project_refresh", False)
    st.session_state.setdefault("task_data", [])
    st.session_state.setdefault("qc_project_select_id", None)

    # Refresh cache if needed
    if st.session_state["needs_project_refresh"]:
        fetch_projects_api.clear()
        st.session_state["needs_project_refresh"] = False
        st.toast("🔁 Project list updated!")

    # Fetch projects
    projects_data = fetch_projects_api(db_params) or {}
    sorted_project_items = sorted(projects_data.items(), key=lambda item: int(item[0]), reverse=True)
    project_options = {f"{pid} - {pname}": pid for pid, pname in sorted_project_items}

    st.subheader("🔍 Project Filter")

    if not project_options:
        st.warning("⚠️ No projects found or database connection failed.")
    else:
        selected_display = st.selectbox(
            "Select Project to Review",
            options=list(project_options.keys()),
            index=0,
            key="project_select_box",
        )
        selected_project_id = project_options[selected_display]

        # Detect project change
        if selected_project_id != st.session_state["qc_project_select_id"]:
            st.session_state["qc_project_select_id"] = selected_project_id
            st.info(f"🔄 Loading tasks for Project ID {selected_project_id}...")
            load_pending_tasks(db_params, selected_project_id)
            time.sleep(0.1)
            rerun()

        current_project = st.session_state["qc_project_select_id"]

        if current_project:
            st.subheader(f"📁 Project: {selected_display}")
            st.info(f"Showing tasks in project **{current_project}** pending QC approval.")

            # Refresh button
            if st.button("🔄 Refresh Task List", key="refresh_tasks_button"):
                load_pending_tasks(db_params, current_project)
                time.sleep(0.1)
                rerun()

            # Display tasks
            pending_tasks = st.session_state.get("task_data", [])
            st.subheader("📋 Tasks Awaiting Approval")

            if not pending_tasks:
                st.success(f"No tasks pending approval in Project {current_project}.")
            else:
                st.dataframe(pd.DataFrame(pending_tasks), use_container_width=True)

                if st.button(f"✅ Approve All Pending Tasks in Project {current_project}", type="primary"):
                    with st.spinner("Approving tasks..."):
                        result = call_api("POST", f"/qc/approve_all_pending/{current_project}", json_data=db_params)
                        if result and "approved_count" in result:
                            st.success(f"Approved {result['approved_count']} tasks!")
                            load_pending_tasks(db_params, current_project)
                            time.sleep(0.1)
                            rerun()

        # Dataset generation
        st.markdown("---")
        st.header("2️⃣ Generate Final Dataset")
        manifest_path_str = st.text_input(
            "Path to Manifest File",
            "F:/ava_kinetics_dep/proposal_generation_pipeline/proposal_generation_pipeline/outputs/factory_batch_01_manifest.json",
        )
        default_output_filename = f"project_{current_project}_ava_kinetics_dataset.csv"
        output_filename = st.text_input("Output CSV Filename", default_output_filename)

        if st.button("📊 Generate Dataset"):
            manifest_path = Path(manifest_path_str)
            if not manifest_path.exists():
                st.error(f"Manifest not found: {manifest_path.resolve()}")
            else:
                with st.spinner("Generating dataset..."):
                    payload = {
                        "db_params": db_params,
                        "project_id": current_project,
                        "manifest_path": manifest_path_str,
                        "output_filename": output_filename,
                    }
                    result = call_api("POST", "/qc/generate_dataset", json_data=payload)
                    if result:
                        st.success(f"✅ Dataset generated: `{result['filename']}`")
                        if os.path.exists(output_filename):
                            with open(output_filename, "rb") as f:
                                st.download_button("📥 Download CSV", f, file_name=os.path.basename(output_filename))

# ==========================================================
# 🚀 TAB 2: CVAT TASK CREATOR (S3)
# ==========================================================
with tab_creator:
    st.header("🧱 CVAT Task Creator (S3 Direct)")
    st.markdown("This tool connects to CVAT, lists clips from S3, and creates tasks for a project.")

    # Initialize session state
    st.session_state.setdefault("s3_clip_names", "")

    st.subheader("Step 1: List Clips from S3")
    s3_bucket_name = st.text_input("S3 Bucket Name", value=s3_bucket_name_sidebar)

    if st.button("🔍 Auto-Detect Clips from S3"):
        if not s3_bucket_name:
            st.error("❌ Please enter the S3 Bucket Name.")
        else:
            with st.spinner(f"Fetching clips from '{s3_bucket_name}'..."):
                try:
                    resp = requests.post(f"{BACKEND_URL}/task-creator/list-clips", json={"s3_bucket_name": s3_bucket_name})
                    if resp.status_code == 200:
                        data = resp.json()
                        clip_list = data.get("clip_names", [])
                        if clip_list:
                            st.session_state.s3_clip_names = ", ".join(clip_list)
                            st.success(f"✅ Found {len(clip_list)} clips.")
                        else:
                            st.session_state.s3_clip_names = ""
                            st.warning(data.get("message", "No clips found in S3."))
                    else:
                        st.error(f"Error fetching clips: {resp.text}")
                except Exception as e:
                    st.error(f"❌ Exception: {e}")

    clip_names_input = st.text_area(
        "Clip ZIP File Names (comma-separated)",
        value=st.session_state.s3_clip_names
    )

    st.subheader("Step 2: Create CVAT Project & Tasks")
    project_name = st.text_input("New CVAT Project Name", f"S3_Project_{int(time.time())}")
    annotators = st.text_area("Annotators (comma-separated)", value="annotator1,annotator2")
    overlap = st.slider("Overlap Percentage", 0, 100, 20)
    org_slug = st.text_input("Organization Slug (optional)", value="").strip()

    if st.button("🚀 Create Project from S3"):
        if not s3_bucket_name:
            st.error("❌ Enter S3 Bucket Name.")
        elif not clip_names_input:
            st.error("❌ Enter list of clips.")
        elif not all([cvat_host, cvat_user, cvat_pass]):
            st.error("❌ Fill all CVAT credentials.")
        else:
            clip_list = [c.strip() for c in clip_names_input.split(",") if c.strip()]
            payload = {
                "project_name": project_name,
                "annotators": [a.strip() for a in annotators.split(",") if a.strip()],
                "overlap_percentage": overlap,
                "org_slug": org_slug if org_slug else "",
                "s3_bucket_name": s3_bucket_name,
                "clip_names": clip_list,
                "cvat_host": cvat_host,
                "cvat_user": cvat_user,
                "cvat_pass": cvat_pass
            }
            with st.spinner("Creating CVAT project and tasks from S3..."):
                try:
                    resp = requests.post(f"{BACKEND_URL}/task-creator/create-project", json=payload)
                    if resp.status_code in (200, 201):
                        st.success("✅ Project and tasks created successfully!")
                        st.json(resp.json())
                        st.balloons()
                        st.session_state["needs_project_refresh"] = True
                        time.sleep(0.1)
                        rerun()
                    else:
                        st.error(f"❌ Error creating project: {resp.text}")
                except Exception as e:
                    st.error(f"❌ Exception: {e}")
