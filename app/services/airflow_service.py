import httpx
from typing import Dict, Any, Optional
from app.core.config import settings
from app.utils.logger import logger


class AirflowAPIClient:
    """Airflow Rest API 客户端封装"""

    def __init__(self):
        self.base_url = f'https://{settings.AIRFLOW_HOST}/{settings.AIRFLOW_PORT}/api/v1'
        self.client = httpx.AsyncClient(
            auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
            timeout=30
        )
        self.logger = logger

    async def trigger_dag(
            self,
            dag_id: str,
            conf: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Trigger Dag Running
        :param dag_id: DAG ID
        :param conf: 传递给DAG的配置参数
        :return: 触发结果
        """
        endpoint = f'/dags/{dag_id}/dagRuns'
        payloda = {
            'conf': conf or {},
            'note': 'Triggered via API'
        }

        try:
            response = await self.client.post(
                f'{self.base_url}{endpoint}',
                json=payloda
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                self.logger.error(f'DAG {dag_id} already running!')
                raise self.DagAlreadyRunningError
            self._handle_http_error(e)

    async def list_dags(
            self,
    ):
        """
        :return: All DAGs
        """
        endpoint = f'/dags'
        try:
            response = await self.client.get(
                f'{self.base_url}{endpoint}'
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self.logger.error(f'Airflow Unauthorized')
                raise self.UnauthorizedError
            self._handle_http_error(e)

    async def _handle_http_error(self, error: httpx.HTTPStatusError):
        """Handle HTTP Errors"""
        self.logger.error(
            f'Airflow API Request Fail: {error.request.url} - {error.response.status_code}'
        )
        if error.response.status_code == 404:
            raise self.DagNotFoundError("DAG not found") from error
        if error.response.status_code == 401:
            raise self.UnauthorizedError("Invalid credentials") from error

        try:
            error_detail = error.response.json().get('detail', 'Unknown error')
        except ValueError:
            error_detail = error.response.text

        raise self.APIError(f'Airflow API Error: {error_detail}') from error

    class APIError(Exception):
        """基础API异常"""

    class DagNotFoundError(APIError):
        """DAG未找到异常"""

    class UnauthorizedError(APIError):
        """认证失败异常"""

    class DagAlreadyRunningError(APIError):
        """DAG正在运行中异常"""
