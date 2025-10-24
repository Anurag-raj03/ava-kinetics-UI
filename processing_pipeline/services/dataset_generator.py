# services/dataset_generator.py
import psycopg2
import pandas as pd
import logging
import json
import boto3
from urllib.parse import urlparse
from typing import Dict, Any
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ATTRIBUTE_DEFINITIONS = {
    "ppe_helmet": {"options": ["helmet_worn", "no_helmet", "helmet_incorrect"]},
    "ppe_vest": {"options": ["vest_worn", "no_vest"]},
    "ppe_gloves": {"options": ["gloves_worn", "no_gloves"]},
    "ppe_boots": {"options": ["safety_boots_worn", "no_safety_boots"]},
    "work_activity": {"options": ["idle", "welding", "cutting", "climbing", "lifting_materials", "machine_operation", "supervising", "walking"]},
    "posture_safety": {"options": ["upright_normal", "bending", "overreaching", "unsafe_posture"]},
    "hazard_proximity": {"options": ["safe_zone", "near_hot_surface", "near_heavy_load", "near_moving_machine", "near_open_edge"]},
    "team_interaction": {"options": ["working_alone", "pair_work", "small_team", "large_group", "supervisor_present"]},
}

def calculate_action_mapping() -> Dict[str, int]:
    mapping = {}
    cumulative_count = 0
    for attr_name in sorted(ATTRIBUTE_DEFINITIONS.keys()):
        mapping[attr_name] = cumulative_count
        cumulative_count += len(ATTRIBUTE_DEFINITIONS[attr_name]["options"])
    return mapping

class DatasetGenerator:
    def __init__(self, db_params: Dict[str, Any], manifest_path: str, project_id: int):
        self.db_params = db_params
        self.project_id = project_id
        self.manifest_path = manifest_path
        self.manifest_data = self._load_manifest(manifest_path)
        self.action_id_map = calculate_action_mapping()
        self.conn = None

    def _load_manifest(self, manifest_path: str):
        """Load manifest from local path or directly from S3 and normalize it as dict keyed by keyframe_name."""
        if manifest_path.startswith("s3://"):
            logger.info(f"📦 Loading manifest from S3: {manifest_path}")
            s3 = boto3.client("s3")
            parsed = urlparse(manifest_path)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            obj = s3.get_object(Bucket=bucket, Key=key)
            manifest = json.loads(obj["Body"].read().decode("utf-8"))
        else:
            logger.info(f"📁 Loading manifest from local path: {manifest_path}")
            with open(manifest_path, "r") as f:
                manifest = json.load(f)

        # Normalize manifest to dict keyed by keyframe_name
        if isinstance(manifest, list):
            manifest_dict = {item["keyframe_name"]: item for item in manifest}
            return manifest_dict
        elif isinstance(manifest, dict):
            return manifest
        else:
            raise ValueError("Manifest must be a dict or a list of dicts")


    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Could not connect to database: {e}")
            self.conn = None

    def close_db(self):
        if self.conn:
            self.conn.close()

    def generate_ava_csv(self, output_path: str, image_width=1280, image_height=720):
        self.connect_db()
        if not self.conn:
            return

        try:
            query = """
            SELECT a.keyframe_name, a.person_id, a.xtl, a.ytl, a.xbr, a.ybr, a.attributes
            FROM annotations a
            JOIN tasks t ON a.task_id = t.task_id
            WHERE t.qc_status = 'approved'
            AND t.project_id = %s;
            """
            df = pd.read_sql(query, self.conn, params=(self.project_id,))
            if df.empty:
                logger.warning(f"⚠️ No 'approved' annotations found for Project ID {self.project_id}.")
                return

            logger.info(f"Retrieved {len(df)} approved annotations for Project ID {self.project_id}.")

            ava_rows = []
            for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Formatting AVA CSV"):
                keyframe_name = row["keyframe_name"]
                origin_data = self.manifest_data.get(keyframe_name)
                if not origin_data:
                    logger.warning(f"Could not find '{keyframe_name}' in manifest. Skipping.")
                    continue

                video_id = origin_data["source_video"].replace(".mp4", "")
                frame_timestamp = origin_data["source_frame"]

                x1_norm = row["xtl"] / image_width
                y1_norm = row["ytl"] / image_height
                x2_norm = row["xbr"] / image_width
                y2_norm = row["ybr"] / image_height

                attributes = row["attributes"]
                person_id = row["person_id"]

                for attr_name, attr_value in attributes.items():
                    base_id = self.action_id_map.get(attr_name)
                    if base_id is None:
                        continue

                    try:
                        options_list = ATTRIBUTE_DEFINITIONS[attr_name]["options"]
                        option_index = options_list.index(attr_value)
                        final_action_id = base_id + option_index + 1
                        ava_rows.append([
                            video_id, frame_timestamp,
                            f"{x1_norm:.6f}", f"{y1_norm:.6f}",
                            f"{x2_norm:.6f}", f"{y2_norm:.6f}",
                            final_action_id, person_id
                        ])
                    except ValueError:
                        logger.warning(f"Value '{attr_value}' for '{attr_name}' not in definitions. Skipping.")

            header = ["video_id", "frame_timestamp", "x1", "y1", "x2", "y2", "action_id", "person_id"]
            ava_df = pd.DataFrame(ava_rows, columns=header)
            ava_df.sort_values(by=["video_id", "frame_timestamp", "person_id"], inplace=True)
            ava_df.to_csv(output_path, index=False)
            logger.info(f"✅ Successfully generated AVA-Kinetics dataset with {len(ava_df)} rows at: {output_path}")
        finally:
            self.close_db()
