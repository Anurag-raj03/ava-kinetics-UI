from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, List
from processing_pipeline.services.cvat_integration import CVATClient

router = APIRouter()

class CVATS3ConfigRequest(BaseModel):
    host: str
    username: str
    password: str
    s3_bucket: str
    project_name: str
    batch_name: str

@router.post("/create_project_and_tasks_s3")
def create_cvat_project_and_tasks_s3(request: CVATS3ConfigRequest):
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

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CVAT S3 integration failed: {str(e)}")
