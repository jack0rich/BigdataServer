import httpx
from typing import Dict, Any, Optional
from app.core.config import settings
from app.utils.logger import logger


class AirflowAPIClient:
    """Airflow REST API 客户端封装"""

    def __init__(self):
        # 使用冒号分隔主机和端口
        self.base_url = f'http://{settings.AIRFLOW_HOST}:{settings.AIRFLOW_PORT}/api/v1'
        self.client = httpx.AsyncClient(
            auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
            timeout=30
        )
        self.logger = logger

    async def trigger_dag(
        self,
        dag_id: str,
        conf: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        触发 DAG 运行
        :param dag_id: DAG ID
        :param conf: 传递给 DAG 的配置参数
        :return: 触发结果
        """
        endpoint = f'/dags/{dag_id}/dagRuns'
        payload = {
            'conf': conf or {},
            'note': 'Triggered via API'
        }
        try:
            response = await self.client.post(
                f'{self.base_url}{endpoint}',
                json=payload
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                self.logger.error(f'DAG {dag_id} already running!')
                raise self.DagAlreadyRunningError("DAG is already running!") from e
            await self._handle_http_error(e)

    async def list_dags(self) -> Dict[str, Any]:
        """
        获取所有 DAG 列表
        :return: 包含所有 DAG 的字典
        """
        endpoint = '/dags'
        try:
            response = await self.client.get(f'{self.base_url}{endpoint}')
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self.logger.error('Airflow Unauthorized')
                raise self.UnauthorizedError("Unauthorized access") from e
            await self._handle_http_error(e)

    async def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """
        获取指定 DAG 的详情
        :param dag_id: DAG ID
        :return: DAG 详情
        """
        endpoint = f'/dags/{dag_id}'
        try:
            response = await self.client.get(f'{self.base_url}{endpoint}')
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def update_dag_state(self, dag_id: str, is_paused: bool) -> Dict[str, Any]:
        """
        更新 DAG 状态（暂停或恢复）
        :param dag_id: DAG ID
        :param is_paused: True 暂停 DAG，False 恢复 DAG
        :return: 更新后的 DAG 详情
        """
        endpoint = f'/dags/{dag_id}'
        payload = {
            "is_paused": is_paused
        }
        try:
            response = await self.client.patch(
                f'{self.base_url}{endpoint}',
                json=payload
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def list_dag_runs(self, dag_id: str) -> Dict[str, Any]:
        """
        获取指定 DAG 的所有运行记录
        :param dag_id: DAG ID
        :return: 包含 DAG 运行记录的字典
        """
        endpoint = f'/dags/{dag_id}/dagRuns'
        try:
            response = await self.client.get(f'{self.base_url}{endpoint}')
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """
        获取指定 DAG 运行的详情
        :param dag_id: DAG ID
        :param dag_run_id: DAG 运行 ID
        :return: DAG 运行详情
        """
        endpoint = f'/dags/{dag_id}/dagRuns/{dag_run_id}'
        try:
            response = await self.client.get(f'{self.base_url}{endpoint}')
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def delete_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """
        删除指定的 DAG 运行
        :param dag_id: DAG ID
        :param dag_run_id: DAG 运行 ID
        :return: 删除操作的响应
        """
        endpoint = f'/dags/{dag_id}/dagRuns/{dag_run_id}'
        try:
            response = await self.client.delete(f'{self.base_url}{endpoint}')
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def _handle_http_error(self, error: httpx.HTTPStatusError):
        """
        处理 Airflow API 返回的 HTTP 错误
        :param error: HTTPStatusError 异常
        """
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
        """基础 API 异常"""

    class DagNotFoundError(APIError):
        """DAG 未找到异常"""

    class UnauthorizedError(APIError):
        """认证失败异常"""

    class DagAlreadyRunningError(APIError):
        """DAG 正在运行中异常"""

    async def close(self):
        """
        关闭 HTTP 客户端
        """
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
