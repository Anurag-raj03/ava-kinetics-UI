# ========================================
# services/dataset_generator.py (Final Fixed)
# ========================================
import psycopg2
import pandas as pd
import logging
import json
import boto3
import os
from urllib.parse import urlparse
from typing import Dict, Any
from tqdm import tqdm

# =============================
# Logging Configuration
# =============================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================
# Attribute Definitions
# =============================
ATTRIBUTE_DEFINITIONS = {
    "ppe_helmet": {"options": ["helmet_worn", "no_helmet", "helmet_incorrect"]},
    "ppe_vest": {"options": ["vest_worn", "no_vest"]},
    "ppe_gloves": {"options": ["gloves_worn", "no_gloves"]},
    "ppe_boots": {"options": ["safety_boots_worn", "no_safety_boots"]},
    "work_activity": {
        "options": [
            "idle", "welding", "cutting", "climbing", "lifting_materials",
            "machine_operation", "supervising", "walking"
        ]
    },
    "posture_safety": {"options": ["upright_normal", "bending", "overreaching", "unsafe_posture"]},
    "hazard_proximity": {
        "options": [
            "safe_zone", "near_hot_surface", "near_heavy_load",
            "near_moving_machine", "near_open_edge"
        ]
    },
    "team_interaction": {
        "options": ["working_alone", "pair_work", "small_team", "large_group", "supervisor_present"]
    },
}


def calculate_action_mapping() -> Dict[str, int]:
    """Map each attribute to a base action ID offset."""
    mapping = {}
    cumulative_count = 0
    for attr_name in sorted(ATTRIBUTE_DEFINITIONS.keys()):
        mapping[attr_name] = cumulative_count
        cumulative_count += len(ATTRIBUTE_DEFINITIONS[attr_name]["options"])
    return mapping


# =============================
# Dataset Generator Class
# =============================
class DatasetGenerator:
    def __init__(self, db_params: Dict[str, Any], manifest_path: str, project_id: int):
        self.db_params = db_params
        self.project_id = project_id
        self.manifest_path = manifest_path
        self.action_id_map = calculate_action_mapping()
        self.conn = None

        # Parse S3 URL
        parsed = urlparse(manifest_path)
        self.bucket = parsed.netloc
        self.manifest_key = parsed.path.lstrip("/")
        self.s3_client = boto3.client("s3")

        # Local temp manifest file
        self.local_manifest_file = f"/tmp/manifest_{project_id}.json"

        # Load manifest locally
        self.manifest_data = self._download_and_load_manifest()
        logger.info(f"📦 Loaded manifest with {len(self.manifest_data)} entries from {self.manifest_path}")

    # =============================
    # Manifest Download + Load
    # =============================
    def _download_and_load_manifest(self) -> Dict[str, Any]:
        """Download manifest from S3 → store locally → load JSON → return dict."""
        try:
            logger.info(f"📥 Downloading manifest from S3: s3://{self.bucket}/{self.manifest_key}")
            self.s3_client.download_file(self.bucket, self.manifest_key, self.local_manifest_file)

            with open(self.local_manifest_file, "r") as f:
                manifest_data = json.load(f)

            if isinstance(manifest_data, dict):
                return manifest_data

            elif isinstance(manifest_data, list):
                result = {}
                for entry in manifest_data:
                    key = (
                        entry.get("keyframe_name")
                        or entry.get("frame_name")
                        or entry.get("image_name")
                        or entry.get("file_name")
                    )
                    if key:
                        result[key] = entry
                return result

            logger.warning(f"⚠️ Unexpected manifest format: {type(manifest_data)}")
            return {}

        except Exception as e:
            logger.error(f"❌ Error downloading or loading manifest: {e}", exc_info=True)
            return {}

    # =============================
    # Database Connection
    # =============================
    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            logger.info("✅ Database connection established successfully.")
        except Exception as e:
            logger.error(f"❌ Could not connect to database: {e}")
            self.conn = None

    def close_db(self):
        if self.conn:
            self.conn.close()
            logger.info("🔒 Database connection closed.")

    # =============================
    # Main CSV Generation
    # =============================
    def generate_ava_csv(self, output_path: str, image_width=1280, image_height=720):
        """Generate AVA-Kinetics CSV from annotations + manifest."""
        self.connect_db()
        if not self.conn:
            logger.error("❌ Database connection failed. Aborting CSV generation.")
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
            logger.info(f"📊 Retrieved {len(df)} approved annotations for Project ID {self.project_id}")

            if df.empty:
                logger.warning(f"⚠️ No approved annotations found for project {self.project_id}.")
                return

            matched, missing = 0, 0
            ava_rows = []

            for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Formatting AVA CSV"):
                keyframe_name = os.path.basename(row["keyframe_name"])
                origin_data = self.manifest_data.get(keyframe_name)

                if not origin_data:
                    missing += 1
                    continue
                matched += 1

                video_id = origin_data.get("source_video", "").replace(".mp4", "")
                frame_timestamp = origin_data.get("source_frame", 0)

                x1_norm = row["xtl"] / image_width
                y1_norm = row["ytl"] / image_height
                x2_norm = row["xbr"] / image_width
                y2_norm = row["ybr"] / image_height

                attributes = row["attributes"]
                if isinstance(attributes, str):
                    try:
                        attributes = json.loads(attributes)
                    except json.JSONDecodeError:
                        continue

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
                        continue

            logger.info(f"✅ Matched frames: {matched}, Missing frames: {missing}")
            header = ["video_id", "frame_timestamp", "x1", "y1", "x2", "y2", "action_id", "person_id"]
            ava_df = pd.DataFrame(ava_rows, columns=header)

            if ava_df.empty:
                logger.warning("⚠️ Final dataset is empty. Check keyframe name matching between DB and manifest.")
                return

            ava_df.sort_values(by=["video_id", "frame_timestamp", "person_id"], inplace=True)
            ava_df.to_csv(output_path, index=False)
            logger.info(f"💾 Dataset saved successfully at: {output_path}")

        finally:
            # Always close DB + delete temp manifest
            self.close_db()
            if os.path.exists(self.local_manifest_file):
                os.remove(self.local_manifest_file)
                logger.info(f"🧹 Deleted temporary manifest file: {self.local_manifest_file}")
