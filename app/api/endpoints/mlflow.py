from fastapi import APIRouter, UploadFile, Depends, HTTPException
from fastapi.responses import JSONResponse
from app.services.mlflow_service import MLflowAPIClient
from app.models.response_models import (
    MLflowExperimentResponse,
    BaseResponse
)



from app.core.security import api_key_auth

mlflow_router = APIRouter(
    prefix="/mlflow",
    tags=["MLflow Operations"],
    dependencies=[Depends(api_key_auth)]
)
