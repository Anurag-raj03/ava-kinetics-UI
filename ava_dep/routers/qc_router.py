from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, List
from pathlib import Path
from fastapi.responses import JSONResponse 
import os
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from ava_dep.db_utils import get_projects, get_pending_tasks, update_all_pending_to_approved
from processing_pipeline.services.dataset_generator import DatasetGenerator 

router = APIRouter()

# --- Schemas ---
class DBParams(BaseModel):
    dbname: str
    user: str
    password: str
    host: str
    port: str

class GenerateRequest(BaseModel):
    db_params: DBParams
    project_id: int
    manifest_path: str
    output_filename: str

# --- Endpoints ---

@router.post("/projects", response_model=Dict[int, str])
def get_available_projects(params: DBParams):
    """Fetches all projects from the database."""
    projects = get_projects(params.model_dump())
    if not projects:
        raise HTTPException(status_code=503, detail="No projects found or DB connection failed.")
    return projects


@router.post("/pending_tasks/{project_id}")
def get_tasks_for_project(project_id: int, params: DBParams):
    """Fetches pending tasks for a specific project."""
    tasks = get_pending_tasks(params.model_dump(), project_id)
    
    # DEBUG: Log the task count to confirm FastAPI sees the fresh data
    print(f"DEBUG: FastAPI returning {len(tasks)} pending tasks for Project {project_id}.")

    # Use JSONResponse to explicitly forbid caching on the HTTP layer
    return JSONResponse(
        content=tasks,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
    )

@router.post("/approve_all_pending/{project_id}")
def approve_pending_tasks(project_id: int, params: DBParams):
    """Updates all pending tasks to 'approved' for a specific project."""
    count = update_all_pending_to_approved(params.model_dump(), project_id)
    if count == 0:
        raise HTTPException(status_code=404, detail="No pending tasks found or DB connection failed.")
    return {"approved_count": count}

@router.post("/generate_dataset")
def generate_dataset_endpoint(request: GenerateRequest):
    """Triggers the final AVA-Kinetics dataset generation."""
    try:

        generator = DatasetGenerator(
            request.db_params.model_dump(),
            request.manifest_path,
            project_id=request.project_id
        )
        generator.generate_ava_csv(request.output_filename)

        return {"message": "Dataset generation complete.", "filename": request.output_filename}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dataset generation failed: {str(e)}")
