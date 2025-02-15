import json
import time

import httpx
from typing import Dict, Any, Optional, List, Coroutine
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

    async def create_registered_model(self, model_name: str, description: Optional[str] = None) -> Dict[str, Any]:
        """
        创建已注册的模型（Registered Model）

        :param model_name: 要注册的模型名称（必填）
        :param description: 该模型的描述（可选）
        :return: 注册结果，包含 Registered Model 信息
        """
        endpoint = "/registered-models/create"
        payload = {"name": model_name}
        if description:
            payload["description"] = description

        return await self._post(endpoint, payload, f"已注册模型: {model_name}")

    async def register_model(
            self,
            run_id: str,
            model_name: str,
            source: Optional[str] = None,
            tags: Optional[Dict[str, str]] = None,
            run_link: Optional[str] = None,
            description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        注册模型到模型仓库

        :param run_id: 训练运行的ID（必填）
        :param model_name: 要注册的模型名称（必填）
        :param source: 模型存储路径（可选，默认为 MLflow 运行目录）
        :param tags: 额外的模型版本标签（可选）
        :param run_link: 训练运行的超链接（可选）
        :param description: 该模型版本的描述（可选）
        :return: 注册结果，包含模型版本信息
        """
        # 先检查 Registered Model 是否存在，不存在则创建
        try:
            await self.create_registered_model(model_name, description)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:  # 409 Conflict: 说明模型已存在
                self.logger.warning(f"模型 {model_name} 已存在，跳过注册")
            else:
                raise  # 其他错误则抛出

        endpoint = "/model-versions/create"
        payload = {
            "name": model_name,
            "source": source or f"runs:/{run_id}/model",
            "run_id": run_id
        }

        if tags:
            payload["tags"] = [{"key": k, "value": v} for k, v in tags.items()]

        if run_link:
            payload["run_link"] = run_link

        if description:
            payload["description"] = description

        return await self._post(endpoint, payload, f"模型注册成功: {model_name}")

    async def transition_model_stage(
            self,
            model_name: str,
            version: str,
            stage: str,
            archive_existing_versions: bool = False
    ) -> Dict[str, Any]:
        """
        转换模型版本阶段（如 Staging、Production、Archived）

        :param model_name: 注册的模型名称（必填）
        :param version: 目标模型版本（必填）
        :param stage: 目标阶段（Staging / Production / Archived）（必填）
        :param archive_existing_versions: 是否将同阶段的其他版本归档（默认 False）
        :return: API 返回的模型版本更新结果
        """
        endpoint = "/model-versions/transition-stage"
        payload = {
            "name": model_name,
            "version": str(version),  # 确保 version 为字符串类型
            "stage": stage.capitalize(),
            "archive_existing_versions": archive_existing_versions
        }

        return await self._post(endpoint, payload, f"模型 {model_name} v{version} 已转为 {stage} 状态")

    async def get_model_version(self, model_name: str, version: str) -> Dict[str, Any]:
        """
        获取指定模型的特定版本信息

        :param model_name: 注册的模型名称（必填）
        :param version: 目标模型版本（必填）
        :return: API 返回的模型版本信息
        """
        endpoint = f"/model-versions/get?name={model_name}&version={version}"

        return await self._get(endpoint, f"获取模型 {model_name} v{version} 信息成功")

    async def get_model_versions(self, model_name: str, filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取模型的所有版本列表

        :param model_name: 注册的模型名称（必填）
        :param filter: 过滤条件（如 "name='model_name'"），默认查询所有版本
        :return: 模型版本列表
        """
        endpoint = "/model-versions/search"
        payload = {
            "filter": filter or f"name='{model_name}'",
            "max_results": 100
        }

        response = await self._get(f"{endpoint}?filter={payload['filter']}&max_results={payload['max_results']}")

        if not response:
            self.logger.warning(f"⚠️ 获取模型版本失败: MLflow API 返回空响应")
            return []

        return response.get("model_versions", [])

    async def create_experiment(
            self,
            experiment_name: str,
            artifact_location: Optional[str] = None,
            tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        创建新实验

        :param experiment_name: 实验名称（必填）
        :param artifact_location: 存储路径（可选）
        :param tags: 实验标签（可选）
        :return: 创建结果，包含 experiment_id
        """
        endpoint = "/experiments/create"
        payload = {"name": experiment_name}

        if artifact_location:
            payload["artifact_location"] = artifact_location

        if tags:
            payload["tags"] = [{"key": k, "value": v} for k, v in tags.items()]

        try:
            response = await self._post(endpoint, payload, f"实验 {experiment_name} 创建成功")
            return response
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                error_data = e.response.json()
                if error_data.get("error_code") == "RESOURCE_ALREADY_EXISTS":
                    self.logger.warning(f"实验 {experiment_name} 已存在，跳过创建")
                    return {"status": "exists",}

    from typing import Dict, Any

    async def delete_model(self, model_name: str) -> Dict[str, Any]:
        """
        删除注册的模型（逻辑删除，仍可恢复）

        :param model_name: 要删除的模型名称（必填）
        :return: API 响应结果
        """
        endpoint = "/registered-models/delete"
        payload = {"name": model_name}

        response = await self._delete(endpoint, payload, f"✅ 模型 {model_name} 已成功删除")

        if not response:
            self.logger.warning(f"⚠️ 删除模型失败: MLflow API 返回空响应")

        return response or {}

    async def delete_model_version(self, model_name: str, version: int) -> Dict[str, Any]:
        """
        删除指定的模型版本

        :param model_name: 模型名称（必填）
        :param version: 版本号（必填）
        :return: API 响应结果
        """
        endpoint = "/model-versions/delete"
        payload = {"name": model_name, "version": str(version)}

        response = await self._delete(endpoint, payload, f"✅ 模型 {model_name} 版本 {version} 已成功删除")

        if not response:
            self.logger.warning(f"⚠️ 删除模型版本失败: MLflow API 返回空响应")

        return response or {}

    async def get_experiment(self, experiment_id: str) -> Dict[str, Any]:
        """获取实验信息"""
        endpoint = f"/experiments/get?experiment_id={experiment_id}"
        return await self._get(endpoint)

    async def get_run(self, run_id: str) -> Dict[str, Any]:
        """获取实验运行详情"""
        endpoint = f"/runs/get?run_id={run_id}"
        return await self._get(endpoint)

    async def create_run(self, experiment_id, run_name="Test Run"):
        """ 创建运行 """
        endpoint = f"/runs/create"
        payload = {"experiment_id": experiment_id, "run_name": run_name}
        return await self._post(endpoint, payload, f"运行 {experiment_id} 已创建")

    async def log_metric(self, run_id: str, key: str, value: float, step: Optional[int] = None) -> Dict[str, Any]:
        """记录单个指标"""
        endpoint = "/runs/log-metric"
        payload = {
            "run_id": run_id,
            "key": key,
            "value": value,
            "timestamp": int(time.time() * 1000)
        }
        if step is not None:
            payload["step"] = step
        return await self._post(endpoint, payload, f"记录指标 {key}: {value}")

    async def log_batch(self, run_id: str, metrics: List[Dict[str, Any]] = [], params: List[Dict[str, Any]] = [], tags: List[Dict[str, Any]] = []) -> Dict[str, Any]:
        """批量记录指标、参数和标签"""
        endpoint = "/runs/log-batch"
        payload = {"run_id": run_id, "metrics": metrics, "params": params, "tags": tags}
        return await self._post(endpoint, payload, "批量记录日志数据")

    async def log_model(self, run_id: str, model_json: str) -> Dict[str, Any]:
        """记录模型信息"""
        endpoint = "/runs/log-model"
        payload = {"run_id": run_id, "model_json": model_json}
        return await self._post(endpoint, payload, "模型信息已记录")

    async def log_inputs(self, run_id: str, datasets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """记录输入数据"""
        endpoint = "/runs/log-inputs"
        payload = {"run_id": run_id, "datasets": datasets}
        return await self._post(endpoint, payload, "输入数据已记录")

    async def log_param(self, run_id: str, key: str, value: Any) -> Dict[str, Any]:
        """记录参数"""
        endpoint = "/runs/log-parameter"
        payload = {"run_id": run_id, "key": key, "value": str(value)}
        return await self._post(endpoint, payload, f"参数 {key}: {value} 已记录")

    async def _post(self, endpoint: str, payload: Dict[str, Any], success_message: Optional[str] = None) -> Any | None:
        """通用 POST 请求方法"""
        try:
            response = await self.client.post(f"{self.base_url}{endpoint}", json=payload)
            response.raise_for_status()
            if success_message:
                self.logger.info(success_message)
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.error(f"MLflow API请求失败: {e.request.url} - {e.response.status_code}")
            self._handle_http_error(e)

    async def _get(self, endpoint: str, success_message: Optional[str] = None) -> Dict[str, Any]:
        """通用 GET 请求方法"""
        try:
            response = await self.client.get(f"{self.base_url}{endpoint}")
            response.raise_for_status()
            if success_message:
                self.logger.info(success_message)
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)

    async def _delete(self, endpoint: str, payload: Dict[str, Any], success_message: Optional[str] = None) -> Any | None:
        """通用 DELETE 请求方法，使用 request() 支持 json 参数"""
        try:
            response = await self.client.request(
                "DELETE",
                f"{self.base_url}{endpoint}",
                json=payload,  # 直接传递字典，会自动转换为 JSON
                headers={"Content-Type": "application/json"}  # 可选：明确告知服务器请求体为 JSON
            )
            response.raise_for_status()
            if success_message:
                self.logger.info(success_message)
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)

    def _handle_http_error(self, error: httpx.HTTPStatusError):
        """统一处理HTTP错误"""
        message = ''
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
