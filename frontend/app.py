
# # # app.py
# # import streamlit as st
# # import pandas as pd
# # import requests
# # import json
# # import time
# # from pathlib import Path
# # import os
# # from dotenv import load_dotenv
# # import boto3
# # from io import BytesIO

# # # ==========================================================
# # # 🔧 LOAD ENV VARIABLES
# # # ==========================================================
# # load_dotenv()  # Make sure your .env is in the same folder or provide full path

# # BACKEND_URL = os.getenv("FRONTEND_BACKEND_URL") or "http://fastapi:8000/api/v1"

# # DEFAULT_DB_PARAMS = {
# #     "dbname": os.getenv("CVAT_DB_NAME") or "cvat_annotations_db",
# #     "user": os.getenv("CVAT_DB_USER") or "admin",
# #     "password": os.getenv("CVAT_DB_PASSWORD") or "admin",
# #     "host": os.getenv("CVAT_DB_HOST") or "localhost",
# #     "port": os.getenv("CVAT_DB_PORT") or "55432",
# # }

# # DEFAULT_CVAT_HOST = os.getenv("CVAT_HOST") or "http://localhost:8080"
# # DEFAULT_CVAT_USER = os.getenv("CVAT_USERNAME") or "Strawhat03"
# # DEFAULT_CVAT_PASS = os.getenv("CVAT_PASSWORD") or "Test@123"
# # DEFAULT_S3_BUCKET = os.getenv("AWS_STORAGE_BUCKET_NAME") or "ava-kine-data"

# # # ==========================================================
# # # ♻️ RERUN HELPER
# # # ==========================================================
# # def rerun(force: bool = True):
# #     if force:
# #         st.cache_data.clear()
# #     if hasattr(st, "rerun"):
# #         st.rerun()
# #     elif hasattr(st, "experimental_rerun"):
# #         st.experimental_rerun()

# # # ==========================================================
# # # 🌐 API CALL FUNCTION
# # # ==========================================================
# # def call_api(method, endpoint, json_data=None):
# #     url = f"{BACKEND_URL}{endpoint}"
# #     try:
# #         response = requests.request(method, url, json=json_data)
# #         response.raise_for_status()
# #         return response.json()
# #     except requests.exceptions.HTTPError as e:
# #         try:
# #             detail = e.response.json().get("detail", "Unknown error")
# #         except:
# #             detail = e.response.text
# #         st.error(f"API Error ({e.response.status_code}): {detail}")
# #         return None
# #     except requests.exceptions.RequestException as e:
# #         st.error(f"Connection Error: Could not connect to backend at {BACKEND_URL}.")
# #         st.exception(e)
# #         return None

# # # ==========================================================
# # # 🖥️ PAGE CONFIG
# # # ==========================================================
# # st.set_page_config(page_title="AVA-Kinetics Pipeline UI", layout="wide")
# # st.title("🚀 Integrated AVA-Kinetics Pipeline UI")
# # st.markdown("---")

# # # ==========================================================
# # # 🧭 TABS
# # # ==========================================================
# # tab_qc, tab_creator = st.tabs(["✅ QC & Dataset Generator", "🚀 CVAT Task Creator (S3)"])

# # # ==========================================================
# # # ⚙️ SIDEBAR CONFIG
# # # ==========================================================
# # st.sidebar.header("⚙️ DB Configuration (QC/Generator)")
# # db_params = {
# #     "dbname": st.sidebar.text_input("DB Name", DEFAULT_DB_PARAMS["dbname"]),
# #     "user": st.sidebar.text_input("DB User", DEFAULT_DB_PARAMS["user"]),
# #     "password": st.sidebar.text_input("DB Password", DEFAULT_DB_PARAMS["password"], type="password"),
# #     "host": st.sidebar.text_input("DB Host", DEFAULT_DB_PARAMS["host"]),
# #     "port": st.sidebar.text_input("DB Port", DEFAULT_DB_PARAMS["port"]),
# # }

# # st.sidebar.markdown("---")
# # st.sidebar.header("⚙️ CVAT/S3 Config (Task Creator)")
# # cvat_host = st.sidebar.text_input("CVAT Host URL", DEFAULT_CVAT_HOST)
# # cvat_user = st.sidebar.text_input("CVAT Username", DEFAULT_CVAT_USER)
# # cvat_pass = st.sidebar.text_input("CVAT Password", DEFAULT_CVAT_PASS, type="password")
# # s3_bucket_name_sidebar = st.sidebar.text_input("S3 Bucket Name", DEFAULT_S3_BUCKET)

# # # ==========================================================
# # # 🔄 HELPER FUNCTIONS
# # # ==========================================================
# # @st.cache_data(show_spinner="Fetching projects...")
# # def fetch_projects_api(params):
# #     return call_api("POST", "/qc/projects", json_data=params)

# # def load_pending_tasks(db_params, project_id):
# #     tasks = call_api("POST", f"/qc/pending_tasks/{project_id}", json_data=db_params)
# #     st.session_state["task_data"] = tasks or []

# # def list_s3_batches(bucket_name):
# #     s3 = boto3.client("s3")
# #     paginator = s3.get_paginator("list_objects_v2")
# #     batches = set()
# #     for page in paginator.paginate(Bucket=bucket_name, Delimiter="/"):
# #         for prefix in page.get("CommonPrefixes", []):
# #             batches.add(prefix.get("Prefix").rstrip("/"))
# #     return sorted(list(batches))

# # def list_s3_clips(bucket_name, batch_name):
# #     s3 = boto3.client("s3")
# #     prefix = f"{batch_name}/frames/"
# #     clips = []
# #     paginator = s3.get_paginator("list_objects_v2")
# #     for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
# #         for obj in page.get("Contents", []):
# #             if obj["Key"].endswith(".zip"):
# #                 clips.append(obj["Key"].split("/")[-1])
# #     return sorted(clips)

# # def list_s3_manifests(bucket_name, batch_name):
# #     s3 = boto3.client("s3")
# #     prefix = f"{batch_name}/manifests/"
# #     manifests = []
# #     paginator = s3.get_paginator("list_objects_v2")
# #     for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
# #         for obj in page.get("Contents", []):
# #             if obj["Key"].endswith(".json"):
# #                 manifests.append(obj["Key"].split("/")[-1])
# #     return sorted(manifests)

# # # ==========================================================
# # # 🧩 TAB 1: QC & DATASET GENERATOR
# # # ==========================================================
# # with tab_qc:
# #     st.header("1️⃣ Quality Control Approval")

# #     # -------------------------------
# #     # Step 1: Select Project
# #     # -------------------------------
# #     projects = fetch_projects_api(db_params) or {}
# #     project_options = [f"{pid}: {pname}" for pid, pname in projects.items()]
# #     selected_project_str = st.selectbox("Select Project for QC", project_options)

# #     if selected_project_str:
# #         project_id = int(selected_project_str.split(":")[0])
# #         st.session_state["qc_project_select_id"] = project_id
# #         load_pending_tasks(db_params, project_id)

# #         tasks = st.session_state.get("task_data", [])
# #         st.subheader(f"Pending Tasks ({len(tasks)})")
        
# #         if tasks:
# #             df_tasks = pd.DataFrame(tasks)
# #             st.dataframe(df_tasks)

# #             if st.button("✅ Approve All Pending Tasks"):
# #                 resp = call_api("POST", f"/qc/approve_all_pending/{project_id}", json_data=db_params)
# #                 if resp:
# #                     st.success(f"Approved {resp['approved_count']} tasks.")
# #                     load_pending_tasks(db_params, project_id)
# #         else:
# #             st.info("No pending tasks found for this project.")

# #     # -------------------------------
# #     st.markdown("---")
# #     st.header("2️⃣ Generate Final Dataset")

# #     s3_bucket_name_qc = st.text_input("S3 Bucket Name for Manifest", value=s3_bucket_name_sidebar)
# #     selected_batch_qc = st.text_input("Batch Name", "factory_batch_01")

# #     manifests = []
# #     if s3_bucket_name_qc and selected_batch_qc:
# #         try:
# #             manifests = list_s3_manifests(s3_bucket_name_qc, selected_batch_qc)
# #         except Exception as e:
# #             st.error(f"Failed to list manifests: {e}")

# #     manifest_path_str = None
# #     if manifests:
# #         manifest_choice = st.selectbox("Select Manifest", manifests)
# #         manifest_path_str = f"s3://{s3_bucket_name_qc}/{selected_batch_qc}/manifests/{manifest_choice}"

# #     default_output_filename = f"project_{st.session_state.get('qc_project_select_id', '0')}_ava_kinetics_dataset.csv"
# #     output_filename = st.text_input("Output CSV Filename", default_output_filename)

    
# #     import io

# #     if st.button("📊 Generate Dataset"):
# #         if not st.session_state.get("qc_project_select_id"):
# #             st.error("❌ Select a project first.")
# #         elif not manifest_path_str:
# #             st.error("❌ Select a manifest first.")
# #         else:
# #             payload = {
# #                 "db_params": db_params,
# #                 "project_id": st.session_state.get("qc_project_select_id"),
# #                 "manifest_path": manifest_path_str,
# #                 "output_filename": output_filename,
# #             }
# #             with st.spinner("Generating dataset..."):
# #                 try:
# #                     response = requests.post(f"{BACKEND_URL}/qc/generate_dataset", json=payload)
# #                     response.raise_for_status()
                    
# #                     # Wrap the CSV content in BytesIO for download
# #                     csv_bytes = io.BytesIO(response.content)
                    
# #                     st.success(f"✅ Dataset generated: `{output_filename}`")
# #                     st.download_button(
# #                         label="⬇️ Download Dataset CSV",
# #                         data=csv_bytes,
# #                         file_name=output_filename,
# #                         mime="text/csv"
# #                     )
# #                 except requests.exceptions.RequestException as e:
# #                     st.error(f"Failed to generate dataset: {e}")



# # # ==========================================================
# # # 🧩 TAB 2: CVAT TASK CREATOR (S3)
# # # ==========================================================
# # with tab_creator:
# #     st.header("🧱 CVAT Task Creator (S3 Direct)")
# #     st.markdown("This tool connects to CVAT, lists clips from S3, and creates tasks for a project.")
    
# #     # Step 1: Select S3 bucket and fetch batch names
# #     st.subheader("Step 1: Select S3 Batch")
# #     s3_bucket_name = st.text_input("S3 Bucket Name", value=s3_bucket_name_sidebar)

# #     batch_name = None
# #     if s3_bucket_name:
# #         try:
# #             batches = list_s3_batches(s3_bucket_name)
# #             if batches:
# #                 batch_name = st.selectbox("Select Batch", batches)
# #             else:
# #                 st.warning("⚠️ No batches found in the bucket.")
# #         except Exception as e:
# #             st.error(f"Failed to list S3 batches: {e}")

# #     # Step 2: List Clips
# #     clip_names_input = ""
# #     if batch_name:
# #         try:
# #             clips = list_s3_clips(s3_bucket_name, batch_name)
# #             if clips:
# #                 clip_names_input = st.text_area(
# #                     "Clip ZIP File Names (comma-separated)",
# #                     value=",".join(clips)
# #                 )
# #         except Exception as e:
# #             st.error(f"Failed to list clips: {e}")

# #     # Step 3: Assign Annotators
# #     st.subheader("Step 3: Assign Annotators")
# #     annotator_input = st.text_area(
# #         "Annotators (comma-separated)",
# #         value=""
# #     )
# #     annotators = [a.strip() for a in annotator_input.split(",") if a.strip()]

# #     # Step 4: Create CVAT Project & Tasks
# #     st.subheader("Step 4: Create CVAT Project & Tasks")
# #     project_name = st.text_input("New CVAT Project Name", f"S3_Project_{int(time.time())}")

# #     if st.button("🚀 Create Project from S3"):
# #         if not s3_bucket_name:
# #             st.error("❌ Enter S3 Bucket Name.")
# #         elif not batch_name:
# #             st.error("❌ Select a batch from S3.")
# #         elif not all([cvat_host, cvat_user, cvat_pass]):
# #             st.error("❌ Fill all CVAT credentials.")
# #         else:
# #             clip_list = [c.strip() for c in clip_names_input.split(",") if c.strip()]
# #             payload = {
# #                 "project_name": project_name,
# #                 "batch_name": batch_name,
# #                 "s3_bucket": s3_bucket_name,
# #                 "host": cvat_host,
# #                 "username": cvat_user,
# #                 "password": cvat_pass,
# #                 "clips": clip_list,
# #                 "annotators": annotators
# #             }
# #             with st.spinner("Creating CVAT project and tasks from S3..."):
# #                 resp = call_api("POST", "/create_project_and_tasks_s3", json_data=payload)
# #                 if resp:
# #                     st.success("✅ Project and tasks created successfully!")
# #                     st.json(resp)
# #                     time.sleep(0.5)
# #                     st.session_state["needs_project_refresh"] = True
# #                     st.session_state["s3_clip_names"] = ""
# #                     time.sleep(0.1)
# #                     rerun()









# # app.py
# import streamlit as st
# import pandas as pd
# import requests
# import json
# import time
# from pathlib import Path
# import os
# from dotenv import load_dotenv
# import boto3
# from io import BytesIO

# # ==========================================================
# # 🔧 LOAD ENV VARIABLES
# # ==========================================================
# load_dotenv()  # Make sure your .env is in the same folder or provide full path

# BACKEND_URL = os.getenv("FRONTEND_BACKEND_URL") or "http://fastapi:8000/api/v1"

# DEFAULT_DB_PARAMS = {
#     "dbname": os.getenv("CVAT_DB_NAME") or "cvat_annotations_db",
#     "user": os.getenv("CVAT_DB_USER") or "admin",
#     "password": os.getenv("CVAT_DB_PASSWORD") or "admin",
#     "host": os.getenv("CVAT_DB_HOST") or "localhost",
#     "port": os.getenv("CVAT_DB_PORT") or "55432",
# }

# DEFAULT_CVAT_HOST = os.getenv("CVAT_HOST") or "http://localhost:8080"
# DEFAULT_CVAT_USER = os.getenv("CVAT_USERNAME") or "Strawhat03"
# DEFAULT_CVAT_PASS = os.getenv("CVAT_PASSWORD") or "Test@123"
# DEFAULT_S3_BUCKET = os.getenv("AWS_STORAGE_BUCKET_NAME") or "ava-kine-data"

# # ==========================================================
# # ♻️ RERUN HELPER
# # ==========================================================
# def rerun(force: bool = True):
#     if force:
#         st.cache_data.clear()
#     if hasattr(st, "rerun"):
#         st.rerun()
#     elif hasattr(st, "experimental_rerun"):
#         st.experimental_rerun()

# # ==========================================================
# # 🌐 API CALL FUNCTION
# # ==========================================================
# def call_api(method, endpoint, json_data=None):
#     url = f"{BACKEND_URL}{endpoint}"
#     try:
#         response = requests.request(method, url, json=json_data)
#         response.raise_for_status()
#         return response.json()
#     except requests.exceptions.HTTPError as e:
#         try:
#             detail = e.response.json().get("detail", "Unknown error")
#         except:
#             detail = e.response.text
#         st.error(f"API Error ({e.response.status_code}): {detail}")
#         return None
#     except requests.exceptions.RequestException as e:
#         st.error(f"Connection Error: Could not connect to backend at {BACKEND_URL}.")
#         st.exception(e)
#         return None

# # ==========================================================
# # 🖥️ PAGE CONFIG
# # ==========================================================
# st.set_page_config(page_title="AVA-Kinetics Pipeline UI", layout="wide")
# st.title("🚀 Integrated AVA-Kinetics Pipeline UI")
# st.markdown("---")

# # ==========================================================
# # 🧭 TABS
# # ==========================================================
# tab_qc, tab_creator = st.tabs(["✅ QC & Dataset Generator", "🚀 CVAT Task Creator (S3)"])

# # ==========================================================
# # ⚙️ SIDEBAR CONFIG
# # ==========================================================
# st.sidebar.header("⚙️ DB Configuration (QC/Generator)")
# db_params = {
#     "dbname": st.sidebar.text_input("DB Name", DEFAULT_DB_PARAMS["dbname"]),
#     "user": st.sidebar.text_input("DB User", DEFAULT_DB_PARAMS["user"]),
#     "password": st.sidebar.text_input("DB Password", DEFAULT_DB_PARAMS["password"], type="password"),
#     "host": st.sidebar.text_input("DB Host", DEFAULT_DB_PARAMS["host"]),
#     "port": st.sidebar.text_input("DB Port", DEFAULT_DB_PARAMS["port"]),
# }

# st.sidebar.markdown("---")
# st.sidebar.header("⚙️ CVAT/S3 Config (Task Creator)")
# cvat_host = st.sidebar.text_input("CVAT Host URL", DEFAULT_CVAT_HOST)
# cvat_user = st.sidebar.text_input("CVAT Username", DEFAULT_CVAT_USER)
# cvat_pass = st.sidebar.text_input("CVAT Password", DEFAULT_CVAT_PASS, type="password")
# s3_bucket_name_sidebar = st.sidebar.text_input("S3 Bucket Name", DEFAULT_S3_BUCKET)

# # ==========================================================
# # 🔄 HELPER FUNCTIONS
# # ==========================================================
# @st.cache_data(show_spinner="Fetching projects...")
# def fetch_projects_api(params):
#     return call_api("POST", "/qc/projects", json_data=params)

# def load_pending_tasks(db_params, project_id):
#     tasks = call_api("POST", f"/qc/pending_tasks/{project_id}", json_data=db_params)
#     st.session_state["task_data"] = tasks or []

# def list_s3_batches(bucket_name):
#     s3 = boto3.client("s3")
#     paginator = s3.get_paginator("list_objects_v2")
#     batches = set()
#     for page in paginator.paginate(Bucket=bucket_name, Delimiter="/"):
#         for prefix in page.get("CommonPrefixes", []):
#             batches.add(prefix.get("Prefix").rstrip("/"))
#     return sorted(list(batches))

# def list_s3_clips(bucket_name, batch_name):
#     s3 = boto3.client("s3")
#     prefix = f"{batch_name}/frames/"
#     clips = []
#     paginator = s3.get_paginator("list_objects_v2")
#     for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
#         for obj in page.get("Contents", []):
#             if obj["Key"].endswith(".zip"):
#                 clips.append(obj["Key"].split("/")[-1])
#     return sorted(clips)

# def list_s3_manifests(bucket_name, batch_name):
#     s3 = boto3.client("s3")
#     prefix = f"{batch_name}/manifests/"
#     manifests = []
#     paginator = s3.get_paginator("list_objects_v2")
#     for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
#         for obj in page.get("Contents", []):
#             if obj["Key"].endswith(".json"):
#                 manifests.append(obj["Key"].split("/")[-1])
#     return sorted(manifests)

# # ==========================================================
# # 🧩 TAB 1: QC & DATASET GENERATOR
# # ==========================================================
# with tab_qc:
#     st.header("1️⃣ Quality Control Approval")

#     # Step 1: Select Project
#     projects = fetch_projects_api(db_params) or {}
#     project_options = [f"{pid}: {pname}" for pid, pname in projects.items()]
#     selected_project_str = st.selectbox("Select Project for QC", project_options)

#     if selected_project_str:
#         project_id = int(selected_project_str.split(":")[0])
#         st.session_state["qc_project_select_id"] = project_id
#         load_pending_tasks(db_params, project_id)

#         tasks = st.session_state.get("task_data", [])
#         st.subheader(f"Pending Tasks ({len(tasks)})")
        
#         if tasks:
#             df_tasks = pd.DataFrame(tasks)
#             st.dataframe(df_tasks)

#             if st.button("✅ Approve All Pending Tasks"):
#                 resp = call_api("POST", f"/qc/approve_all_pending/{project_id}", json_data=db_params)
#                 if resp:
#                     st.success(f"Approved {resp['approved_count']} tasks.")
#                     load_pending_tasks(db_params, project_id)
#         else:
#             st.info("No pending tasks found for this project.")

#     # Step 2: Generate Final Dataset
#     st.markdown("---")
#     st.header("2️⃣ Generate Final Dataset")

#     s3_bucket_name_qc = st.text_input("S3 Bucket Name for Manifest", value=s3_bucket_name_sidebar)
#     selected_batch_qc = st.text_input("Batch Name", "factory_batch_01")

#     manifests = []
#     if s3_bucket_name_qc and selected_batch_qc:
#         try:
#             manifests = list_s3_manifests(s3_bucket_name_qc, selected_batch_qc)
#         except Exception as e:
#             st.error(f"Failed to list manifests: {e}")

#     manifest_path_str = None
#     if manifests:
#         manifest_choice = st.selectbox("Select Manifest", manifests)
#         manifest_path_str = f"s3://{s3_bucket_name_qc}/{selected_batch_qc}/manifests/{manifest_choice}"

#     default_output_filename = f"project_{st.session_state.get('qc_project_select_id', '0')}_ava_kinetics_dataset.csv"
#     output_filename = st.text_input("Output CSV Filename", default_output_filename)



#     import io

#     if st.button("📊 Generate Dataset"):
#         if not st.session_state.get("qc_project_select_id"):
#             st.error("❌ Select a project first.")
#         elif not manifest_path_str:
#             st.error("❌ Select a manifest first.")
#         else:
#             payload = {
#                 "db_params": db_params,
#                 "project_id": st.session_state.get("qc_project_select_id"),
#                 "manifest_path": manifest_path_str,
#                 "output_filename": output_filename,
#             }
#             with st.spinner("Generating dataset..."):
#                 try:
#                     # Request CSV directly from backend
#                     response = requests.post(
#                         f"{BACKEND_URL}/qc/generate_dataset",
#                         json=payload,
#                         stream=True  # important for large files
#                     )
#                     response.raise_for_status()

#                     # Wrap CSV in BytesIO for download
#                     csv_bytes = io.BytesIO(response.content)
#                     csv_bytes.seek(0)

#                     st.success(f"✅ Dataset generated: `{output_filename}`")
#                     st.download_button(
#                         label="⬇️ Download Dataset CSV",
#                         data=csv_bytes,
#                         file_name=output_filename,
#                         mime="text/csv"
#                     )

#                 except requests.exceptions.RequestException as e:
#                     st.error(f"Failed to generate dataset: {e}")



# # ==========================================================
# # 🧩 TAB 2: CVAT TASK CREATOR (S3)
# # ==========================================================
# with tab_creator:
#     st.header("🧱 CVAT Task Creator (S3 Direct)")
#     st.markdown("This tool connects to CVAT, lists clips from S3, and creates tasks for a project.")
    
#     # Step 1: Select S3 bucket and fetch batch names
#     st.subheader("Step 1: Select S3 Batch")
#     s3_bucket_name = st.text_input("S3 Bucket Name", value=s3_bucket_name_sidebar)

#     batch_name = None
#     if s3_bucket_name:
#         try:
#             batches = list_s3_batches(s3_bucket_name)
#             if batches:
#                 batch_name = st.selectbox("Select Batch", batches)
#             else:
#                 st.warning("⚠️ No batches found in the bucket.")
#         except Exception as e:
#             st.error(f"Failed to list S3 batches: {e}")

#     # Step 2: List Clips
#     clip_names_input = ""
#     if batch_name:
#         try:
#             clips = list_s3_clips(s3_bucket_name, batch_name)
#             if clips:
#                 clip_names_input = st.text_area(
#                     "Clip ZIP File Names (comma-separated)",
#                     value=",".join(clips)
#                 )
#         except Exception as e:
#             st.error(f"Failed to list clips: {e}")

#     # Step 3: Assign Annotators
#     st.subheader("Step 3: Assign Annotators")
#     annotator_input = st.text_area(
#         "Annotators (comma-separated)",
#         value=""
#     )
#     annotators = [a.strip() for a in annotator_input.split(",") if a.strip()]

#     # Step 4: Create CVAT Project & Tasks
#     st.subheader("Step 4: Create CVAT Project & Tasks")
#     project_name = st.text_input("New CVAT Project Name", f"S3_Project_{int(time.time())}")

#     if st.button("🚀 Create Project from S3"):
#         if not s3_bucket_name:
#             st.error("❌ Enter S3 Bucket Name.")
#         elif not batch_name:
#             st.error("❌ Select a batch from S3.")
#         elif not all([cvat_host, cvat_user, cvat_pass]):
#             st.error("❌ Fill all CVAT credentials.")
#         else:
#             clip_list = [c.strip() for c in clip_names_input.split(",") if c.strip()]
#             payload = {
#                 "project_name": project_name,
#                 "batch_name": batch_name,
#                 "s3_bucket": s3_bucket_name,
#                 "host": cvat_host,
#                 "username": cvat_user,
#                 "password": cvat_pass,
#                 "clips": clip_list,
#                 "annotators": annotators
#             }
#             with st.spinner("Creating CVAT project and tasks from S3..."):
#                 resp = call_api("POST", "/create_project_and_tasks_s3", json_data=payload)
#                 if resp:
#                     st.success("✅ Project and tasks created successfully!")
#                     st.json(resp)
#                     time.sleep(0.5)
#                     st.session_state["needs_project_refresh"] = True
#                     st.session_state["s3_clip_names"] = ""
#                     time.sleep(0.1)
#                     rerun()





import streamlit as st
import pandas as pd
import requests
import json
import time
import os
from dotenv import load_dotenv
import boto3
import io

# ==========================================================
# 🔧 Load Environment Variables
# ==========================================================
load_dotenv()

BACKEND_URL = os.getenv("FRONTEND_BACKEND_URL") or "http://fastapi:8000/api/v1/cvat"

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
# ♻️ Helper Functions
# ==========================================================
def rerun(force: bool = True):
    if force and "fetch_projects_api" in st.session_state:
        del st.session_state["fetch_projects_api"]
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

def call_api(method, endpoint, json_data=None, expect_file=False):
    url = f"{BACKEND_URL}{endpoint}"
    try:
        response = requests.request(method, url, json=json_data, stream=expect_file)
        response.raise_for_status()
        if expect_file:
            return response
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {"message": "Success", "content": response.text}
    except requests.exceptions.HTTPError as e:
        detail = "Unknown error"
        try:
            detail = e.response.json().get("detail", e.response.text)
        except:
            detail = e.response.text
        st.error(f"API Error ({e.response.status_code}): {detail}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Connection Error: Could not connect to backend at {BACKEND_URL}.")
        st.exception(e)
        return None

@st.cache_data(show_spinner="Fetching projects...")
def fetch_projects_api(params):
    return call_api("POST", "/qc/projects", json_data=params)

def load_pending_tasks(db_params, project_id):
    tasks = call_api("POST", f"/qc/pending_tasks/{project_id}", json_data=db_params)
    st.session_state["task_data"] = tasks or []

def list_s3_batches(bucket_name):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    batches = set()
    for page in paginator.paginate(Bucket=bucket_name, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            batches.add(prefix.get("Prefix").rstrip("/"))
    return sorted(list(batches))

def list_s3_clips(bucket_name, batch_name):
    s3 = boto3.client("s3")
    prefix = f"{batch_name}/frames/"
    clips = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".zip"):
                clips.append(obj["Key"].split("/")[-1])
    return sorted(clips)

def list_s3_manifests(bucket_name, batch_name):
    s3 = boto3.client("s3")
    prefix = f"{batch_name}/manifests/"
    manifests = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                manifests.append(obj["Key"].split("/")[-1])
    return sorted(manifests)

# ==========================================================
# 🖥️ Streamlit UI Setup
# ==========================================================
st.set_page_config(page_title="AVA-Kinetics Pipeline UI", layout="wide")
st.title("🚀 Integrated AVA-Kinetics Pipeline UI")
st.markdown("---")

tab_qc, tab_creator = st.tabs(["✅ QC & Dataset Generator", "🚀 CVAT Task Creator (S3)"])

# Sidebar Config
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
# 🧾 QC & Dataset Generator Tab
# ==========================================================
with tab_qc:
    st.header("1️⃣ Quality Control Approval")
    projects = fetch_projects_api(db_params) or {}
    project_options = [f"{pid}: {pname}" for pid, pname in projects.items()]
    selected_project_str = st.selectbox("Select Project for QC", project_options)

    if selected_project_str:
        project_id = int(selected_project_str.split(":")[0])
        st.session_state["qc_project_select_id"] = project_id
        load_pending_tasks(db_params, project_id)
        tasks = st.session_state.get("task_data", [])
        st.subheader(f"Pending Tasks ({len(tasks)})")
        if tasks:
            df_tasks = pd.DataFrame(tasks)
            st.dataframe(df_tasks)
            if st.button("✅ Approve All Pending Tasks"):
                resp = call_api("POST", f"/qc/approve_all_pending/{project_id}", json_data=db_params)
                if resp:
                    st.success(f"Approved {resp['approved_count']} tasks.")
                    load_pending_tasks(db_params, project_id)
        else:
            st.info("No pending tasks found for this project.")

    st.markdown("---")
    st.header("2️⃣ Generate Final Dataset")
    s3_bucket_name_qc = st.text_input("S3 Bucket Name for Manifest", value=s3_bucket_name_sidebar, key="qc_bucket")
    selected_batch_qc = st.text_input("Batch Name", "factory_batch_01", key="qc_batch")
    manifests = []
    if s3_bucket_name_qc and selected_batch_qc:
        try:
            manifests = list_s3_manifests(s3_bucket_name_qc, selected_batch_qc)
        except Exception as e:
            st.error(f"Failed to list manifests: {e}")

    manifest_path_str = None
    if manifests:
        manifest_choice = st.selectbox("Select Manifest", manifests)
        manifest_path_str = f"s3://{s3_bucket_name_qc}/{selected_batch_qc}/manifests/{manifest_choice}"

    default_output_filename = f"project_{st.session_state.get('qc_project_select_id', '0')}_ava_kinetics_dataset.csv"
    output_filename = st.text_input("Output CSV Filename", default_output_filename)

    def generate_dataset():
        if not st.session_state.get("qc_project_select_id"):
            st.error("❌ Select a project first.")
        elif not manifest_path_str:
            st.error("❌ Select a manifest first.")
        else:
            payload = {
                "db_params": db_params,
                "project_id": st.session_state.get("qc_project_select_id"),
                "manifest_path": manifest_path_str,
                "output_filename": output_filename,
            }
            with st.spinner("Generating dataset..."):
                try:
                    response = call_api("POST", "/qc/generate_dataset", json_data=payload, expect_file=True)
                    if response is None:
                        return
                    try:
                        error_json = response.json()
                        st.error(f"❌ Download failed. Received JSON message instead of file: {error_json.get('message', 'Unknown JSON error')}")
                        return
                    except json.JSONDecodeError:
                        pass
                    csv_bytes = io.BytesIO(response.content)
                    csv_bytes.seek(0)
                    st.success(f"✅ Dataset generated: `{output_filename}`")
                    st.download_button(
                        label="⬇️ Download Dataset CSV",
                        data=csv_bytes,
                        file_name=output_filename,
                        mime="text/csv"
                    )
                except Exception as e:
                    st.error(f"Failed to process dataset response: {e}")

    if st.button("📊 Generate Dataset"):
        generate_dataset()

# ==========================================================
# 🧱 CVAT Task Creator Tab
# ==========================================================
with tab_creator:
    st.header("🧱 CVAT Task Creator (S3 Direct)")
    st.markdown("This tool connects to CVAT, lists clips from S3, and creates tasks for a project.")
    st.subheader("Step 1: Select S3 Batch")
    s3_bucket_name = st.text_input("S3 Bucket Name", value=s3_bucket_name_sidebar, key="creator_bucket")
    batch_name = None
    if s3_bucket_name:
        try:
            batches = list_s3_batches(s3_bucket_name)
            if batches:
                batch_name = st.selectbox("Select Batch", batches, key="creator_batch_select")
            else:
                st.warning("⚠️ No batches found in the bucket.")
        except Exception as e:
            st.error(f"Failed to list S3 batches: {e}")

    clip_names_input = ""
    if batch_name:
        try:
            clips = list_s3_clips(s3_bucket_name, batch_name)
            if clips:
                clip_names_input = st.text_area("Clip ZIP File Names (comma-separated)", value=",".join(clips), key="creator_clips")
        except Exception as e:
            st.error(f"Failed to list clips: {e}")

    st.subheader("Step 3: Assign Annotators")
    annotator_input = st.text_area("Annotators (comma-separated)", value="", key="creator_annotators")
    annotators = [a.strip() for a in annotator_input.split(",") if a.strip()]

    st.subheader("Step 4: Create CVAT Project & Tasks")
    project_name = st.text_input("New CVAT Project Name", f"S3_Project_{int(time.time())}", key="creator_project_name")

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
                "clips": clip_list,
                "annotators": annotators
            }
            with st.spinner("Creating CVAT project and tasks from S3..."):
                resp = call_api("POST", "/create_project_and_tasks_s3", json_data=payload)
                if resp:
                    st.success("✅ Project and tasks created successfully!")
                    st.json(resp)
                    time.sleep(2.0)
                    st.session_state["needs_project_refresh"] = True
                    st.session_state["s3_clip_names"] = ""
                    time.sleep(0.1)
                    rerun()
