import httpx
from typing import Dict, Any, Optional, List
from app.core.config import settings
from app.utils.logger import logger


class MLflowAPIClient:
    """MLflow REST API 客户端封装"""

    def __init__(self):
        self.base_url = f"http://{settings.MLFLOW_HOST}:{settings.MLFLOW_PORT}/api/2.0/mlflow"
        self.client = httpx.AsyncClient(
            timeout=30
        )
        self.logger = logger

    async def test_connection(self) -> Dict[str, Any]:
        """测试 MLflow 服务是否正常运行"""
        endpoint = "/experiments/get"
        try:
            response = await self.client.get(f"{self.base_url}{endpoint}")
            response.raise_for_status()
            return {"status": "ok", "version": response.json().get("version", "unknown")}
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)
        except httpx.RequestError as e:
            self.logger.error(f"无法连接到 MLflow: {e}")
            return {"status": "error", "message": str(e)}

    async def register_model(self, run_id: str, model_name: str) -> Dict[str, Any]:
        """注册模型到模型仓库"""
        endpoint = "/model-versions/create"
        payload = {"name": model_name, "source": f"runs:/{run_id}/model", "run_id": run_id}
        return await self._post(endpoint, payload, f"模型注册成功: {model_name}")

    async def transition_model_stage(self, model_name: str, version: int, stage: str) -> Dict[str, Any]:
        """转换模型版本阶段"""
        endpoint = "/model-versions/transition-stage"
        payload = {"name": model_name, "version": version, "stage": stage.capitalize()}
        return await self._post(endpoint, payload, f"模型 {model_name} v{version} 已转为 {stage} 状态")

    async def get_model_versions(self, model_name: str, filter: Optional[str] = None) -> List[Dict]:
        """获取模型版本列表"""
        endpoint = "/model-versions/search"
        payload = {"filter": filter or f"name='{model_name}'", "max_results": 100}
        response = await self._post(endpoint, payload)
        return response.get("model_versions", [])

    async def create_experiment(self, experiment_name: str, tags: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """创建新实验"""
        endpoint = "/experiments/create"
        payload = {"name": experiment_name, "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()]}
        return await self._post(endpoint, payload, f"实验 {experiment_name} 创建成功")

    async def delete_model(self, model_name: str) -> Dict[str, Any]:
        """删除模型（逻辑删除，仍可恢复）"""
        endpoint = "/registered-models/delete"
        payload = {"name": model_name}
        return await self._post(endpoint, payload, f"模型 {model_name} 已删除")

    async def delete_model_version(self, model_name: str, version: int) -> Dict[str, Any]:
        """删除模型版本"""
        endpoint = "/model-versions/delete"
        payload = {"name": model_name, "version": str(version)}
        return await self._post(endpoint, payload, f"模型 {model_name} 版本 {version} 已删除")

    async def get_experiment(self, experiment_id: str) -> Dict[str, Any]:
        """获取实验信息"""
        endpoint = f"/experiments/get?experiment_id={experiment_id}"
        return await self._get(endpoint)

    async def get_run(self, run_id: str) -> Dict[str, Any]:
        """获取实验运行详情"""
        endpoint = f"/runs/get?run_id={run_id}"
        return await self._get(endpoint)

    async def _post(self, endpoint: str, payload: Dict[str, Any], success_message: Optional[str] = None) -> Dict[str, Any]:
        """通用 POST 请求方法"""
        try:
            response = await self.client.post(f"{self.base_url}{endpoint}", json=payload)
            response.raise_for_status()
            if success_message:
                self.logger.info(success_message)
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)

    async def _get(self, endpoint: str) -> Dict[str, Any]:
        """通用 GET 请求方法"""
        try:
            response = await self.client.get(f"{self.base_url}{endpoint}")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)

    def _handle_http_error(self, error: httpx.HTTPStatusError):
        """统一处理HTTP错误"""
        self.logger.error(f"MLflow API请求失败: {error.request.url} - {error.response.status_code}")
        try:
            error_detail = error.response.json().get("error_code", "Unknown error")
            message = error.response.json().get("message", "")
        except ValueError:
            error_detail = error.response.text

        if error.response.status_code == 404:
            raise MLflowAPIClient.NotFoundError(f"Resource not found: {message}") from error
        if error.response.status_code == 401:
            raise MLflowAPIClient.UnauthorizedError("Invalid credentials") from error

        raise MLflowAPIClient.APIError(f"MLflow API Error [{error_detail}]: {message}") from error

    async def close(self):
        """释放 HTTP 资源"""
        await self.client.aclose()

    class APIError(Exception):
        """基础API异常"""

    class NotFoundError(APIError):
        """资源未找到异常"""

    class UnauthorizedError(APIError):
        """认证失败异常"""


if __name__ == '__main__':
    import asyncio
    c = MLflowAPIClient()
    asyncio.run(c.test_connection())
