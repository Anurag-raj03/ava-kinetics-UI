import argparse
import logging
import json
import xml.etree.ElementTree as ET
import io
import zipfile
import os
import sys
from typing import Dict
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Ensure CVAT client can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from processing_pipeline.services.cvat_integration import CVATClient

# --------------------- Logging ---------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --------------------- Load .env ---------------------
load_dotenv()

# --------------------- Database & CVAT Credentials ---------------------
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

CVAT_HOST = os.getenv("CVAT_HOST")
CVAT_USERNAME = os.getenv("CVAT_USER")
CVAT_PASSWORD = os.getenv("CVAT_PASS")

# --------------------- PostAnnotationService ---------------------
class PostAnnotationService:
    def __init__(self, db_params: Dict[str, str], cvat_client: CVATClient):
        self.db_params = db_params
        self.cvat_client = cvat_client
        self.conn = None

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            logger.info("✓ Successfully connected to PostgreSQL.")
            return True
        except psycopg2.OperationalError as e:
            logger.error(f"✗ Could not connect to the database: {e}")
            return False

    def close_db(self):
        if self.conn: self.conn.close()

    def _wait_for_request_completion(self, rq_id: str, timeout: int = 300):
        import time
        start_time = time.time()
        while time.time() - start_time < timeout:
            status_resp = self.cvat_client._make_authenticated_request("GET", f"{self.cvat_client.host}/api/requests/{rq_id}")
            if status_resp.status_code != 200: return None
            status_data = status_resp.json()
            if status_data.get("status") in ("finished", "failed"):
                return status_data
            time.sleep(3)
        logger.error(f"✗ Job {rq_id} timed out.")
        return None

    def export_annotations_from_task(self, task_id: int):
        try:
            url = f"{self.cvat_client.host}/api/tasks/{task_id}/dataset/export"
            params = {"format": "CVAT for images 1.1", "save_images": False}
            resp = self.cvat_client._make_authenticated_request("POST", url, params=params)
            if resp.status_code != 202:
                logger.error(f"Failed to start export for task {task_id}: {resp.status_code} - {resp.text}")
                return None

            rq_id = resp.json()['rq_id']
            logger.info(f"Started annotation export job {rq_id} for task {task_id}")
            status_data = self._wait_for_request_completion(rq_id)
            if not status_data or status_data.get("status") != "finished":
                logger.error(f"Annotation export failed or timed out. Status: {status_data}")
                return None

            result_url = status_data.get("result_url")
            if not result_url: return None
            download_resp = self.cvat_client._make_authenticated_request("GET", result_url, stream=True)
            download_resp.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(download_resp.content)) as z:
                for filename in z.namelist():
                    if filename.lower().endswith('.xml'):
                        xml_data = z.read(filename).decode('utf-8')
                        logger.info(f"✓ Successfully extracted '{filename}' for task {task_id}.")
                        return xml_data

            logger.error("Could not find annotations.xml in the exported zip file.")
            return None

        except Exception as e:
            logger.error(f"Failed to export annotations for task {task_id}: {e}")
            return None

    def process_and_store_task(self, task_id: int, provided_assignee: str):
        if not self.connect_db(): return

        try:
            logger.info(f"Processing completed task {task_id}...")
            task_details_resp = self.cvat_client._make_authenticated_request("GET", f"{self.cvat_client.host}/api/tasks/{task_id}")
            task_details = task_details_resp.json()
            assignee = provided_assignee
            project_id = task_details["project_id"]

            with self.conn.cursor() as cur:
                project_details_resp = self.cvat_client._make_authenticated_request("GET", f"{self.cvat_client.host}/api/projects/{project_id}")
                project_name = project_details_resp.json().get("name", f"Project_{project_id}")
                cur.execute(
                    "INSERT INTO projects (project_id, name) VALUES (%s, %s) ON CONFLICT (project_id) DO NOTHING;",
                    (project_id, project_name)
                )
                cur.execute(
                    """
                    INSERT INTO tasks (task_id, project_id, name, status, assignee, retrieved_at, qc_status)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                    ON CONFLICT (task_id) DO UPDATE SET status = EXCLUDED.status, assignee = EXCLUDED.assignee, retrieved_at = EXCLUDED.retrieved_at;
                    """,
                    (task_id, project_id, task_details["name"], 'completed', assignee, 'pending')
                )

            xml_data = self.export_annotations_from_task(task_id)
            if not xml_data: return
            root = ET.fromstring(xml_data)
            data_to_insert = []

            for image_tag in root.findall("image"):
                keyframe_name = image_tag.get("name")
                for person_id_counter, box_tag in enumerate(image_tag.findall("box")):
                    attributes = {attr.get("name"): attr.text for attr in box_tag.findall("attribute")}
                    data_to_insert.append((
                        task_id, keyframe_name, person_id_counter + 1,
                        float(box_tag.get("xtl")), float(box_tag.get("ytl")),
                        float(box_tag.get("xbr")), float(box_tag.get("ybr")),
                        json.dumps(attributes)
                    ))

            if not data_to_insert:
                logger.warning(f"No annotations found to parse for task {task_id}.")
                return

            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM annotations WHERE task_id = %s;", (task_id,))
                insert_query = "INSERT INTO annotations (task_id, keyframe_name, person_id, xtl, ytl, xbr, ybr, attributes) VALUES %s;"
                psycopg2.extras.execute_values(cur, insert_query, data_to_insert)
                logger.info(f"✓ Stored {cur.rowcount} annotations for task {task_id}.")
            self.conn.commit()

        except Exception as e:
            logger.error(f"Database transaction failed for task {task_id}: {e}")
            if self.conn: self.conn.rollback()
        finally:
            self.close_db()

# --------------------- CLI ---------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Sync a completed CVAT task to PostgreSQL.")
    parser.add_argument("--task-id", required=True, type=int, help="CVAT task ID to sync.")
    parser.add_argument("--assignee", required=False, type=str, default="N/A", help="Username of the annotator/assignee.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    cvat_client = CVATClient(host=CVAT_HOST, username=CVAT_USERNAME, password=CVAT_PASSWORD)
    if cvat_client.authenticated:
        service = PostAnnotationService(db_params=DB_PARAMS, cvat_client=cvat_client)
        service.process_and_store_task(task_id=args.task_id, provided_assignee=args.assignee)
