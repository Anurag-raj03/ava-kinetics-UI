# # backend.py
# from fastapi import FastAPI
# import os
# import sys 
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
# import uvicorn
# from ava_dep.routers.qc_router import router as qc_router
# from ava_dep.routers.task_creator_router import router as cvat_router

# # Initialize FastAPI app
# app = FastAPI(
#     title="AVA-Kinetics Pipeline Backend",
#     description="API for QC, Dataset Generation, and CVAT Task Creation",
#     version="1.0.0"
# )

# app.include_router(qc_router, prefix="/api/v1/cvat/qc", tags=["Quality Control & Generator"])
# app.include_router(cvat_router, prefix="/api/v1/cvat", tags=["CVAT Task Creator"])

# @app.get("/")
# def read_root():
#     return {"message": "AVA-Kinetics Pipeline Backend is running. Check /docs for API details."}

# # Run command: uvicorn backend:app --reload --port 8000












# backend.py
from fastapi import FastAPI
import os
import sys
import uvicorn
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from ava_dep.routers.qc_router import router as qc_router
from ava_dep.routers.task_creator_router import router as cvat_router

app = FastAPI(
    title="AVA-Kinetics Pipeline Backend",
    description="API for QC, Dataset Generation, and CVAT Task Creation",
    version="1.0.0"
)

app.include_router(
    qc_router,
    prefix="/api/v1/cvat/qc",
    tags=["Quality Control & Generator"]
)

app.include_router(
    cvat_router,
    prefix="/api/v1/cvat",
    tags=["CVAT Task Creator"]
)

@app.get("/")
def read_root():
    return {"message": "AVA-Kinetics Pipeline Backend is running. Check /docs for API details."}


