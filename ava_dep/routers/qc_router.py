# services/qc_router.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict
from fastapi.responses import JSONResponse
import os
from pathlib import Path
from dotenv import load_dotenv
import sys
import logging

# =========================
# Setup logging
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# Add project root to path
# =========================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from ava_dep.db_utils import get_projects, get_pending_tasks, update_all_pending_to_approved
from processing_pipeline.services.dataset_generator import DatasetGenerator

# =========================
# Load environment variables
# =========================
load_dotenv()

DEFAULT_DB_PARAMS = {
    "dbname": os.getenv("CVAT_DB_NAME", "cvat_annotations_db"),
    "user": os.getenv("CVAT_DB_USER", "admin"),
    "password": os.getenv("CVAT_DB_PASSWORD", "admin"),
    "host": os.getenv("CVAT_DB_HOST", "localhost"),
    "port": os.getenv("CVAT_DB_PORT", "55432")
}

DEFAULT_S3_BUCKET = os.getenv("AWS_STORAGE_BUCKET_NAME", "cvat-data-uploader")

router = APIRouter()

# =========================
# Schemas
# =========================
class DBParams(BaseModel):
    dbname: str = DEFAULT_DB_PARAMS["dbname"]
    user: str = DEFAULT_DB_PARAMS["user"]
    password: str = DEFAULT_DB_PARAMS["password"]
    host: str = DEFAULT_DB_PARAMS["host"]
    port: str = DEFAULT_DB_PARAMS["port"]

class GenerateRequest(BaseModel):
    db_params: DBParams = DBParams()
    project_id: int
    manifest_path: str
    output_filename: str

# =========================
# Endpoints
# =========================

@router.post("/projects", response_model=Dict[int, str])
def get_available_projects(params: DBParams = DBParams()):
    """Fetches all projects from the database."""
    try:
        projects = get_projects(params.model_dump())
        if not projects:
            raise HTTPException(status_code=503, detail="No projects found or DB connection failed.")
        return projects
    except Exception as e:
        logger.error(f"Failed to fetch projects: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch projects: {str(e)}")

@router.post("/pending_tasks/{project_id}")
def get_tasks_for_project(project_id: int, params: DBParams = DBParams()):
    """Fetches pending tasks for a specific project."""
    try:
        tasks = get_pending_tasks(params.model_dump(), project_id)
        logger.info(f"Returning {len(tasks)} pending tasks for project {project_id}")
        return JSONResponse(
            content=tasks,
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
        )
    except Exception as e:
        logger.error(f"Failed to fetch pending tasks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch pending tasks: {str(e)}")

@router.post("/approve_all_pending/{project_id}")
def approve_pending_tasks(project_id: int, params: DBParams = DBParams()):
    """Updates all pending tasks to 'approved' for a specific project."""
    try:
        count = update_all_pending_to_approved(params.model_dump(), project_id)
        if count == 0:
            raise HTTPException(status_code=404, detail="No pending tasks found or DB connection failed.")
        return {"approved_count": count}
    except Exception as e:
        logger.error(f"Failed to approve tasks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to approve tasks: {str(e)}")

@router.post("/generate_dataset")
def generate_dataset_endpoint(request: GenerateRequest):
    """Triggers the final AVA-Kinetics dataset generation from a manifest."""
    if not request.manifest_path:
        raise HTTPException(status_code=400, detail="Manifest path is required.")

    try:
        logger.info(f"Generating dataset for project {request.project_id} from {request.manifest_path}")
        generator = DatasetGenerator(
            db_params=request.db_params.model_dump(),
            manifest_path=request.manifest_path,
            project_id=request.project_id
        )
        generator.generate_ava_csv(request.output_filename)
        logger.info(f"Dataset generated successfully: {request.output_filename}")

        return {
            "message": "Dataset generation complete.",
            "filename": request.output_filename
        }

    except Exception as e:
        logger.error(f"Dataset generation failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Dataset generation failed: {str(e)}")
