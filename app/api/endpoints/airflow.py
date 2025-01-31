from fastapi import APIRouter, UploadFile, Depends, HTTPException
from fastapi.responses import JSONResponse
from app.services.airflow_service import AirflowAPIClient
from app.models.request_models import (
    AirflowDagDelete,
    AirflowDagTrigger
)
from app.models.response_models import (
    AirflowDagResponse,
    BaseResponse
)

from app.core.security import api_key_auth

airflow_router = APIRouter(
    prefix="/airflow",
    tags=["Airflow Operations"],
    dependencies=[Depends(api_key_auth)]
)




