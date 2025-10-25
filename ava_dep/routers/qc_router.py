# # services/qc_router.py
# from fastapi import APIRouter, HTTPException
# from pydantic import BaseModel
# from typing import Dict
# from fastapi.responses import JSONResponse
# import os
# from pathlib import Path
# from dotenv import load_dotenv
# import sys
# import logging

# # =========================
# # Setup logging
# # =========================
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # =========================
# # Add project root to path
# # =========================
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
# from ava_dep.db_utils import get_projects, get_pending_tasks, update_all_pending_to_approved
# from processing_pipeline.services.dataset_generator import DatasetGenerator

# # =========================
# # Load environment variables
# # =========================
# load_dotenv()

# DEFAULT_DB_PARAMS = {
#     "dbname": os.getenv("CVAT_DB_NAME", "cvat_annotations_db"),
#     "user": os.getenv("CVAT_DB_USER", "admin"),
#     "password": os.getenv("CVAT_DB_PASSWORD", "admin"),
#     "host": os.getenv("CVAT_DB_HOST", "localhost"),
#     "port": os.getenv("CVAT_DB_PORT", "55432")
# }

# DEFAULT_S3_BUCKET = os.getenv("AWS_STORAGE_BUCKET_NAME", "cvat-data-uploader")

# router = APIRouter()

# # =========================
# # Schemas
# # =========================
# class DBParams(BaseModel):
#     dbname: str = DEFAULT_DB_PARAMS["dbname"]
#     user: str = DEFAULT_DB_PARAMS["user"]
#     password: str = DEFAULT_DB_PARAMS["password"]
#     host: str = DEFAULT_DB_PARAMS["host"]
#     port: str = DEFAULT_DB_PARAMS["port"]

# class GenerateRequest(BaseModel):
#     db_params: DBParams = DBParams()
#     project_id: int
#     manifest_path: str
#     output_filename: str

# # =========================
# # Endpoints
# # =========================

# @router.post("/projects", response_model=Dict[int, str])
# def get_available_projects(params: DBParams = DBParams()):
#     """Fetches all projects from the database."""
#     try:
#         projects = get_projects(params.model_dump())
#         if projects is None:
#             logger.warning("No projects found in the database.")
#             return {}  # Return empty dict instead of raising HTTPException
#         return projects
#     except Exception as e:
#         logger.error(f"Failed to fetch projects: {str(e)}", exc_info=True)
#         raise HTTPException(
#             status_code=500,
#             detail=f"Failed to fetch projects. Check DB connection and credentials. Error: {str(e)}"
#         )

# @router.post("/pending_tasks/{project_id}")
# def get_tasks_for_project(project_id: int, params: DBParams = DBParams()):
#     """Fetches pending tasks for a specific project."""
#     try:
#         tasks = get_pending_tasks(params.model_dump(), project_id)
#         logger.info(f"Returning {len(tasks)} pending tasks for project {project_id}")
#         return JSONResponse(
#             content=tasks or [],
#             headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
#         )
#     except Exception as e:
#         logger.error(f"Failed to fetch pending tasks: {str(e)}", exc_info=True)
#         raise HTTPException(
#             status_code=500,
#             detail=f"Failed to fetch pending tasks. Error: {str(e)}"
#         )

# @router.post("/approve_all_pending/{project_id}")
# def approve_pending_tasks(project_id: int, params: DBParams = DBParams()):
#     """Updates all pending tasks to 'approved' for a specific project."""
#     try:
#         count = update_all_pending_to_approved(params.model_dump(), project_id)
#         if count == 0:
#             logger.warning(f"No pending tasks found for project {project_id}")
#             return {"approved_count": 0}
#         return {"approved_count": count}
#     except Exception as e:
#         logger.error(f"Failed to approve tasks: {str(e)}", exc_info=True)
#         raise HTTPException(
#             status_code=500,
#             detail=f"Failed to approve tasks. Error: {str(e)}"
#         )

# from fastapi.responses import FileResponse

# @router.post("/generate_dataset")
# def generate_dataset_endpoint(request: GenerateRequest):
#     """Triggers the final AVA-Kinetics dataset generation from a manifest and returns the CSV file."""
#     if not request.manifest_path:
#         raise HTTPException(status_code=400, detail="Manifest path is required.")

#     try:
#         logger.info(f"Generating dataset for project {request.project_id} from {request.manifest_path}")
#         generator = DatasetGenerator(
#             db_params=request.db_params.model_dump(),
#             manifest_path=request.manifest_path,
#             project_id=request.project_id
#         )
#         # Generate the CSV file locally
#         generator.generate_ava_csv(request.output_filename)
#         logger.info(f"Dataset generated successfully: {request.output_filename}")

#         # Build absolute path
#         file_path = Path(request.output_filename).resolve()
#         if not file_path.exists():
#             logger.error(f"Generated CSV not found at: {file_path}")
#             raise HTTPException(status_code=500, detail="Generated CSV not found.")

#         # Return CSV file as a download
#         return FileResponse(
#             path=str(file_path),
#             filename=file_path.name,
#             media_type='text/csv'
#         )

#     except Exception as e:
#         logger.error(f"Dataset generation failed: {str(e)}", exc_info=True)
#         raise HTTPException(
#             status_code=500,
#             detail=f"Dataset generation failed. Error: {str(e)}"
#         )


# services/qc_router.py (Complete)
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from fastapi.responses import JSONResponse, FileResponse
import os
from pathlib import Path
from dotenv import load_dotenv
import sys
import logging
import json

# =========================
# Setup logging
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# Add project root to path
# =========================
# Assuming 'ava_dep' and 'processing_pipeline' are relative to the project root
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
        if projects is None:
            logger.warning("No projects found in the database.")
            return {}
        return projects
    except Exception as e:
        logger.error(f"Failed to fetch projects: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch projects. Check DB connection and credentials. Error: {str(e)}"
        )

@router.post("/pending_tasks/{project_id}")
def get_tasks_for_project(project_id: int, params: DBParams = DBParams()):
    """Fetches pending tasks for a specific project."""
    try:
        tasks = get_pending_tasks(params.model_dump(), project_id)
        logger.info(f"Returning {len(tasks)} pending tasks for project {project_id}")
        return JSONResponse(
            content=tasks or [],
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
        )
    except Exception as e:
        logger.error(f"Failed to fetch pending tasks: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch pending tasks. Error: {str(e)}"
        )

@router.post("/approve_all_pending/{project_id}")
def approve_pending_tasks(project_id: int, params: DBParams = DBParams()):
    """Updates all pending tasks to 'approved' for a specific project."""
    try:
        count = update_all_pending_to_approved(params.model_dump(), project_id)
        if count == 0:
            logger.warning(f"No pending tasks found for project {project_id}")
            return {"approved_count": 0}
        return {"approved_count": count}
    except Exception as e:
        logger.error(f"Failed to approve tasks: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to approve tasks. Error: {str(e)}"
        )

@router.post("/generate_dataset")
def generate_dataset_endpoint(request: GenerateRequest):
    """Triggers the final AVA-Kinetics dataset generation from a manifest and returns the CSV file."""
    if not request.manifest_path:
        raise HTTPException(status_code=400, detail="Manifest path is required.")

    try:
        logger.info(f"Generating dataset for project {request.project_id} from {request.manifest_path}")
        generator = DatasetGenerator(
            db_params=request.db_params.model_dump(),
            manifest_path=request.manifest_path,
            project_id=request.project_id
        )
        
        # 1. Generate the CSV file locally
        generator.generate_ava_csv(request.output_filename)
        logger.info(f"Dataset generated successfully: {request.output_filename}")

        # 2. Build absolute path and check
        file_path = Path(request.output_filename).resolve()
        if not file_path.exists():
            logger.error(f"Generated CSV not found at: {file_path}")
            # If the file exists but is empty, this could be the cause of the JSON message error
            # Forcing a successful JSON message if the file is missing is what caused the original error, 
            # we must raise an exception here.
            raise HTTPException(status_code=500, detail="Generated CSV not found on server.")

        # 3. Return CSV file as a download (This should be a binary response)
        # Note: We return the FileResponse. If you still get JSON, it's an environment/proxy issue.
        return FileResponse(
            path=str(file_path),
            filename=file_path.name,
            media_type='text/csv'
        )

    except Exception as e:
        logger.error(f"Dataset generation failed: {str(e)}", exc_info=True)
        # Check if the error is related to file not being returned, and if so, 
        # return a descriptive JSON error.
        raise HTTPException(
            status_code=500,
            detail=f"Dataset generation failed. Error: {str(e)}"
        )