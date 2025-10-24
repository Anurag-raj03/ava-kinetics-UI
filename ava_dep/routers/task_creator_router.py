# services/cvat_s3_router.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
import os
from typing import List
from processing_pipeline.services.cvat_integration import CVATClient

# Load .env
load_dotenv()

DEFAULT_CVAT_HOST = os.getenv("CVAT_HOST", "http://localhost:8080")
DEFAULT_CVAT_USERNAME = os.getenv("CVAT_USERNAME", "admin")
DEFAULT_CVAT_PASSWORD = os.getenv("CVAT_PASSWORD", "admin")
DEFAULT_S3_BUCKET = os.getenv("AWS_STORAGE_BUCKET_NAME", "cvat-data-uploader")

router = APIRouter()

# -----------------------
# Request Schemas
# -----------------------
class CVATS3ConfigRequest(BaseModel):
    host: str = DEFAULT_CVAT_HOST
    username: str = DEFAULT_CVAT_USERNAME
    password: str = DEFAULT_CVAT_PASSWORD
    s3_bucket: str = DEFAULT_S3_BUCKET
    project_name: str
    batch_name: str

class S3ListBatchesRequest(BaseModel):
    s3_bucket: str

class S3ListClipsRequest(BaseModel):
    s3_bucket: str
    batch_name: str

# -----------------------
# Endpoint: List batches
# -----------------------
@router.post("/list-batches")
def list_batches(request: S3ListBatchesRequest):
    """List top-level folders (batches) in the S3 bucket."""
    try:
        if not request.s3_bucket:
            raise HTTPException(status_code=400, detail="S3 bucket name is required.")

        client = CVATClient(
            host=DEFAULT_CVAT_HOST,
            username=DEFAULT_CVAT_USERNAME,
            password=DEFAULT_CVAT_PASSWORD,
            s3_bucket=request.s3_bucket
        )

        batches = client.list_batches_in_s3()
        return {"message": f"Found {len(batches)} batches.", "batches": batches}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list batches: {e}")

# -----------------------
# Endpoint: List clips
# -----------------------
@router.post("/list-clips")
def list_clips(request: S3ListClipsRequest):
    """List ZIP clips in a batch folder."""
    try:
        if not request.s3_bucket or not request.batch_name:
            raise HTTPException(status_code=400, detail="S3 bucket and batch name required.")

        client = CVATClient(
            host=DEFAULT_CVAT_HOST,
            username=DEFAULT_CVAT_USERNAME,
            password=DEFAULT_CVAT_PASSWORD,
            s3_bucket=request.s3_bucket
        )

        clip_names = client.list_zip_files_in_s3(batch_name=request.batch_name)

        return {
            "message": f"Found {len(clip_names)} clips in batch '{request.batch_name}'.",
            "clip_names": clip_names
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list clips: {e}")

# -----------------------
# Endpoint: Create project & tasks
# -----------------------
@router.post("/create_project_and_tasks_s3")
def create_cvat_project_and_tasks_s3(request: CVATS3ConfigRequest):
    """Create CVAT project and add tasks from S3."""
    try:
        client = CVATClient(
            host=request.host,
            username=request.username,
            password=request.password,
            s3_bucket=request.s3_bucket
        )

        if not client.authenticated:
            raise Exception("CVAT Authentication Failed!")

        result = client.create_project_and_add_tasks_from_s3(
            project_name=request.project_name,
            batch_name=request.batch_name
        )

        if not result or not result.get("tasks_created"):
            raise Exception("No tasks created from S3")

        return {
            "message": f"Created {len(result.get('tasks_created'))} tasks in project '{request.project_name}'",
            "tasks_created": result.get("tasks_created")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CVAT S3 integration failed: {str(e)}")
