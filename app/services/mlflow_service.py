import httpx
from typing import Dict, Any, Optional, List
from app.core.config import settings
from app.utils.logger import logger


class MLflowAPIClient:
    """MLflow REST API 客户端封装"""

    def __init__(self):
        # 修正配置项为MLflow专用参数
        self.base_url = f"http://{settings.MLFLOW_HOST}:{settings.MLFLOW_PORT}/api/2.0/mlflow"
        self.client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {settings.MLFLOW_TOKEN}"},
            timeout=30
        )
        self.logger = logger

    async def register_model(
            self,
            run_id: str,
            model_name: str
    ) -> Dict[str, Any]:
        """
        注册模型到模型仓库
        :param run_id: 训练运行的ID
        :param model_name: 要注册的模型名称
        :return: 注册结果
        """
        endpoint = "/model-versions/create"
        payload = {
            "name": model_name,
            "source": f"runs:/{run_id}/model",
            "run_id": run_id
        }

        try:
            response = await self.client.post(
                f"{self.base_url}{endpoint}",
                json=payload
            )
            response.raise_for_status()
            self.logger.info(f"模型注册成功: {model_name}")
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)

    async def transition_model_stage(
            self,
            model_name: str,
            version: int,
            stage: str
    ) -> Dict[str, Any]:
        """
        转换模型版本阶段
        :param model_name: 模型名称
        :param version: 模型版本号
        :param stage: 目标阶段（Staging/Production/Archived）
        :return: 操作结果
        """
        endpoint = "/model-versions/transition-stage"
        payload = {
            "name": model_name,
            "version": version,
            "stage": stage.lower()  # MLflow阶段名称需要小写
        }

        try:
            response = await self.client.post(
                f"{self.base_url}{endpoint}",
                json=payload
            )
            response.raise_for_status()
            self.logger.info(f"模型 {model_name} v{version} 已转为 {stage} 状态")
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)

    async def get_model_versions(
            self,
            model_name: str,
            filter: Optional[str] = None
    ) -> List[Dict]:
        """
        获取模型版本列表
        :param model_name: 模型名称
        :param filter: 过滤条件（例如："name='model_name'"）
        :return: 模型版本列表
        """
        endpoint = "/model-versions/search"
        payload = {
            "filter": filter or f"name='{model_name}'",
            "max_results": 100
        }

        try:
            response = await self.client.post(
                f"{self.base_url}{endpoint}",
                json=payload
            )
            response.raise_for_status()
            return response.json().get("model_versions", [])
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)

    async def create_experiment(
            self,
            experiment_name: str,
            tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        创建新实验
        :param experiment_name: 实验名称
        :param tags: 实验标签
        :return: 创建结果
        """
        endpoint = "/experiments/create"
        payload = {
            "name": experiment_name,
            "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()]
        }

        try:
            response = await self.client.post(
                f"{self.base_url}{endpoint}",
                json=payload
            )
            response.raise_for_status()
            self.logger.info(f"实验 {experiment_name} 创建成功")
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)

    def _handle_http_error(self, error: httpx.HTTPStatusError):
        """统一处理HTTP错误"""
        self.logger.error(
            f"MLflow API请求失败: {error.request.url} - {error.response.status_code}"
        )

        try:
            error_detail = error.response.json().get("error_code", "Unknown error")
            message = error.response.json().get("message", "")
        except ValueError:
            error_detail = error.response.text

        if error.response.status_code == 404:
            raise self.NotFoundError(f"Resource not found: {message}") from error
        if error.response.status_code == 401:
            raise self.UnauthorizedError("Invalid credentials") from error

        raise self.APIError(f"MLflow API Error [{error_detail}]: {message}") from error

    class APIError(Exception):
        """基础API异常"""

    class NotFoundError(APIError):
        """资源未找到异常"""

    class UnauthorizedError(APIError):
        """认证失败异常"""