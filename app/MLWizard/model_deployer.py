import asyncio
from app.services import MLflowAPIClient
from app.utils.logger import logger

class ModelDeployer:
    """模型部署模块，负责模型注册和部署"""

    def __init__(self):
        self.mlflow_client = MLflowAPIClient()

    async def deploy(self, run_id: str, model_name: str, stage: str = "Production") -> str:
        """注册并部署模型"""
        await self.mlflow_client.register_model(run_id, model_name)
        versions = await self.mlflow_client.get_model_versions(model_name)
        latest_version = versions[-1]["version"]
        await self.mlflow_client.transition_model_stage(model_name, latest_version, stage)
        logger.info(f"✅ 模型 {model_name} v{latest_version} 已部署到 {stage}")
        return f"{model_name}:v{latest_version}"