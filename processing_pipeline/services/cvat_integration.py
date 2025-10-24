import requests
import os
import logging
import boto3
from typing import List, Dict, Any, Optional
from pathlib import Path
import tempfile
import time
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CVATClient:
    def __init__(self, host: str, username: str, password: str, s3_bucket: Optional[str] = None):
        self.host = host.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.token = None
        self.authenticated = self.login()
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client("s3") if s3_bucket else None

    # -------------------------
    # CVAT Authentication
    # -------------------------
    def login(self) -> bool:
        try:
            url = f"{self.host}/api/auth/login"
            resp = self.session.post(url, json={"username": self.username, "password": self.password}, timeout=30)
            resp.raise_for_status()
            self.token = resp.json()["key"]
            self.session.headers.update({"Authorization": f"Token {self.token}"})
            logger.info(f"✓ Login successful for user: {self.username}")
            return True
        except Exception as e:
            logger.error(f"Login exception: {e}")
            return False

    def _make_authenticated_request(self, method: str, url: str, **kwargs) -> requests.Response:
        if not self.authenticated:
            raise RuntimeError("Client is not authenticated.")
        kwargs.setdefault("timeout", 300)
        return self.session.request(method.upper(), url, **kwargs)

    # -------------------------
    # Project & Task Management
    # -------------------------
    def create_project(self, name: str, labels: List[Dict[str, Any]], org_slug: str = None) -> Optional[int]:
        payload = {"name": name, "labels": labels}
        if org_slug:
            payload['org'] = org_slug
        resp = self._make_authenticated_request('POST', f"{self.host}/api/projects", json=payload)
        if resp.status_code == 201:
            project_id = resp.json()["id"]
            logger.info(f"✓ Project '{name}' created with ID: {project_id}")
            return project_id
        logger.error(f"Failed to create project: {resp.status_code} - {resp.text}")
        return None

    def create_task(self, name: str, project_id: int) -> Optional[int]:
        payload = {"name": name, "project_id": project_id}
        resp = self._make_authenticated_request('POST', f"{self.host}/api/tasks", json=payload)
        if resp.status_code == 201:
            task_id = resp.json()["id"]
            logger.info(f"✓ Task '{name}' created with ID: {task_id}")
            return task_id
        logger.error(f"Failed to create task: {resp.status_code} - {resp.text}")
        return None

    # -------------------------
    # Upload Data / Annotations
    # -------------------------
    def upload_data_to_task(self, task_id: int, zip_file_path: str) -> bool:
        with open(zip_file_path, 'rb') as fh:
            files = {'client_files[0]': (os.path.basename(zip_file_path), fh, 'application/zip')}
            data = {'image_quality': '95'}
            resp = self._make_authenticated_request('POST', f"{self.host}/api/tasks/{task_id}/data", files=files, data=data)
        if resp.status_code != 202:
            logger.error(f"Data upload failed: {resp.status_code} - {resp.text}")
            return False
        
        rq_id = resp.json()['rq_id']
        while True:
            status_resp = self._make_authenticated_request("GET", f"{self.host}/api/requests/{rq_id}")
            status = status_resp.json().get("status")
            if status == "finished":
                logger.info(f"✓ Data upload for task {task_id} complete.")
                return True
            if status == "failed":
                logger.error(f"Data upload failed: {status_resp.json()}")
                return False
            time.sleep(3)

    def import_annotations(self, task_id: int, xml_file: str) -> bool:
        url = f"{self.host}/api/tasks/{task_id}/annotations?action=upload&format=CVAT%201.1"
        with open(xml_file, "rb") as fh:
            files = {"annotation_file": (os.path.basename(xml_file), fh, "application/xml")}
            resp = self._make_authenticated_request("POST", url, files=files)
        if resp.status_code not in (201, 202):
            logger.error(f"Annotation upload failed: {resp.status_code} - {resp.text}")
            return False
        return True

    def assign_user_to_task(self, task_id: int, username: str) -> bool:
        resp_user = self._make_authenticated_request('GET', f"{self.host}/api/users", params={"search": username})
        results = resp_user.json().get('results', [])
        if not results:
            logger.error(f"User '{username}' not found in CVAT.")
            return False
        user_id = results[0]['id']

        resp_jobs = self._make_authenticated_request('GET', f"{self.host}/api/jobs", params={"task_id": task_id})
        jobs = resp_jobs.json().get('results', [])
        for job in jobs:
            self._make_authenticated_request('PATCH', f"{self.host}/api/jobs/{job['id']}", json={'assignee': user_id})
        logger.info(f"✓ Assigned task {task_id} to '{username}'")
        return True

    # -------------------------
    # S3 Methods
    # -------------------------
    def list_batches_in_s3(self) -> List[str]:
        if not self.s3_client or not self.s3_bucket:
            raise RuntimeError("S3 client or bucket not configured.")
        paginator = self.s3_client.get_paginator("list_objects_v2")
        batches = set()
        for page in paginator.paginate(Bucket=self.s3_bucket, Delimiter="/"):
            for prefix in page.get("CommonPrefixes", []):
                batches.add(prefix.get("Prefix").rstrip("/"))
        return sorted(list(batches))

    def list_zip_files_in_s3(self, batch_name: str) -> List[str]:
        if not self.s3_client or not self.s3_bucket:
            raise RuntimeError("S3 client or bucket not configured.")
        prefix = f"{batch_name}/frames/"
        paginator = self.s3_client.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".zip"):
                    files.append(os.path.basename(key))
        return sorted(files)

    def download_s3_file(self, key: str, local_dir: str) -> str:
        filename = os.path.basename(key)
        local_path = os.path.join(local_dir, filename)
        self.s3_client.download_file(self.s3_bucket, key, local_path)
        return local_path

    # -------------------------
    # Task creation from S3
    # -------------------------
    # -------------------------
    # Task creation from S3 (fixed)
    # -------------------------
    def create_tasks_from_selected_s3_files(self, project_id: int, batch_name: str, zip_files: List[str], annotators: Optional[List[str]] = None) -> List[Dict]:
        """
        Create CVAT tasks from S3 files without downloading locally.
        Assumes CVAT has access to the S3 bucket (cloud storage mode).
        """
        results = []

        for idx, zip_file in enumerate(zip_files):
            base_name = Path(zip_file).stem
            annotator = annotators[idx] if annotators and idx < len(annotators) else "default"

            task_name = base_name
            task_id = self.create_task(task_name, project_id)
            if not task_id:
                continue

            # Construct S3 URLs for CVAT to pull directly
            zip_s3_url = f"s3://{self.s3_bucket}/{batch_name}/frames/{zip_file}"
            xml_s3_url = f"s3://{self.s3_bucket}/{batch_name}/annotations/{base_name}_annotations.xml"

            # Upload data (tell CVAT to use remote S3)
            data_resp = self._make_authenticated_request(
                "POST",
                f"{self.host}/api/tasks/{task_id}/data",
                json={"remote_files": [zip_s3_url], "image_quality": 95}
            )
            if data_resp.status_code != 202:
                logger.error(f"Data upload failed for task {task_id}: {data_resp.text}")
                continue

            # Import annotations (remote URL)
            annot_resp = self._make_authenticated_request(
                "POST",
                f"{self.host}/api/tasks/{task_id}/annotations?action=upload&format=CVAT%201.1",
                json={"remote_files": [xml_s3_url]}
            )
            if annot_resp.status_code not in (201, 202):
                logger.error(f"Annotation upload failed for task {task_id}: {annot_resp.text}")
                continue

            # Assign user
            self.assign_user_to_task(task_id, annotator)

            results.append({"task_id": task_id, "task_name": task_name, "annotator": annotator})
            logger.info(f"✓ Created CVAT task '{task_name}' for annotator '{annotator}'")

        return results

    def create_project_and_add_tasks_from_s3(self, project_name: str, batch_name: str, zip_files: Optional[List[str]] = None, annotators: Optional[List[str]] = None) -> Optional[Dict]:
        logger.info(f"Creating new CVAT project '{project_name}' (S3 mode)...")
        project_id = self.create_project(project_name, get_default_labels())
        if not project_id:
            return None

        if zip_files:
            results = self.create_tasks_from_selected_s3_files(project_id, batch_name, zip_files, annotators)
        else:
            results = []  # fallback empty

        return {"project_id": project_id, "tasks_created": results}


# -------------------------
# Default Labels
# -------------------------
def get_default_labels() -> List[Dict[str, Any]]:
    return [
        {
            "name": "person",
            "color": "#ff0000",
            "attributes": [
                {"name": "ppe_helmet", "mutable": True, "input_type": "select",
                 "default_value": "helmet_worn", "values": ["helmet_worn", "no_helmet", "helmet_incorrect"]},
                {"name": "ppe_vest", "mutable": True, "input_type": "select",
                 "default_value": "vest_worn", "values": ["vest_worn", "no_vest"]},
                {"name": "ppe_gloves", "mutable": True, "input_type": "select",
                 "default_value": "gloves_worn", "values": ["gloves_worn", "no_gloves"]},
                {"name": "ppe_boots", "mutable": True, "input_type": "select",
                 "default_value": "safety_boots_worn", "values": ["safety_boots_worn", "no_safety_boots"]},
                {"name": "work_activity", "mutable": True, "input_type": "select",
                 "default_value": "idle",
                 "values": ["idle", "welding", "cutting", "climbing", "lifting_materials",
                            "machine_operation", "supervising", "walking"]},
                {"name": "posture_safety", "mutable": True, "input_type": "select",
                 "default_value": "upright_normal",
                 "values": ["upright_normal", "bending", "overreaching", "unsafe_posture"]},
                {"name": "hazard_proximity", "mutable": True, "input_type": "select",
                 "default_value": "safe_zone",
                 "values": ["safe_zone", "near_hot_surface", "near_heavy_load", "near_moving_machine", "near_open_edge"]},
                {"name": "team_interaction", "mutable": True, "input_type": "select",
                 "default_value": "working_alone",
                 "values": ["working_alone", "pair_work", "small_team", "large_group", "supervisor_present"]},
            ]
        }
    ]
