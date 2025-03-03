import asyncio
from app.services import MLflowAPIClient
from app.utils.logger import logger

class ExperimentManager:
    """实验管理模块，负责MLflow实验的创建和管理"""

    def __init__(self):
        self.mlflow_client = MLflowAPIClient()

    async def create_experiment(self, experiment_name: str) -> str:
        """创建MLflow实验"""
        try:
            exp = await self.mlflow_client.create_experiment(experiment_name)
            experiment_id = exp["experiment_id"]
            logger.info(f"✅ 实验已创建: {experiment_name} (ID: {experiment_id})")
            return experiment_id
        except Exception as e:
            if "RESOURCE_ALREADY_EXISTS" in str(e):
                logger.warning(f"⚠️ 实验 {experiment_name} 已存在")
                experiments = await self.mlflow_client.list_experiments()
                for exp in experiments:
                    if exp["name"] == experiment_name:
                        return exp["experiment_id"]
            raise

    async def create_run(self, experiment_id: str, run_name: str = "Default Run") -> str:
        """在实验中创建运行"""
        run = await self.mlflow_client.create_run(experiment_id, run_name=run_name)
        run_id = run["run_id"]
        logger.info(f"✅ 运行已创建: {run_name} (ID: {run_id})")
        return run_id