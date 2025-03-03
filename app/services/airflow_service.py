import httpx
from typing import Dict, Any, Optional, List
from app.core.config import settings
from app.utils.logger import logger


class AirflowAPIClient:
    """Airflow REST API 客户端封装"""

    def __init__(self):
        self.base_url = f'http://{settings.AIRFLOW_HOST}:{settings.AIRFLOW_PORT}/api/v1'
        self.client = httpx.AsyncClient(
            auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
            timeout=30
        )
        self.logger = logger


