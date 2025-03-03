import asyncio
from typing import Dict, Any, Callable
from app.services import MLflowAPIClient
from app.utils.logger import logger

class ModelTrainer:
    """模型训练模块，支持自定义训练逻辑和指标"""

    def __init__(self):
        self.mlflow_client = MLflowAPIClient()

    async def train(
        self,
        run_id: str,
        model: Any,
        data: bytes,
        params: Dict[str, Any],
        train_func: Callable[[Any, bytes, Dict[str, Any]], Any],
        metrics: Dict[str, Callable[[Any, bytes], float]] = None
    ) -> Any:
        """训练模型并记录参数和指标"""
        # 执行用户自定义训练逻辑
        trained_model = train_func(model, data, params)

        # 记录参数
        for key, value in params.items():
            await self.mlflow_client.log_param(run_id, key, value)

        # 记录用户自定义指标
        if metrics:
            for metric_name, metric_func in metrics.items():
                value = metric_func(trained_model, data)
                await self.mlflow_client.log_metric(run_id, metric_name, value)
                logger.info(f"✅ 记录指标 {metric_name}: {value}")

        logger.info("✅ 模型训练完成")
        return trained_model