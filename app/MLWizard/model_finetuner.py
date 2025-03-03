import asyncio
from typing import Dict, Any, Callable
from app.services import MLflowAPIClient
from app.services import HadoopAPIClient
from app.utils.logger import logger


class ModelFinetuner:
    """模型微调模块，支持自定义微调逻辑"""

    def __init__(self):
        self.mlflow_client = MLflowAPIClient()
        self.hadoop_client = HadoopAPIClient()

    async def finetune(
            self,
            model_name: str,
            new_data_path: str,
            params: Dict[str, Any],
            finetune_func: Callable[[Any, bytes, Dict[str, Any]], Any],
            metrics: Dict[str, Callable[[Any, bytes], float]] = None
    ) -> str:
        """微调模型并更新版本"""
        # 获取最新模型版本
        versions = await self.mlflow_client.get_model_versions(model_name)
        latest_version = versions[-1]["version"]
        model_info = await self.mlflow_client.get_model_version(model_name, latest_version)

        # 下载新数据
        new_data = await self.hadoop_client.download_file(new_data_path)

        # 加载模型并微调
        model = load_model(model_info["source"])  # 需实现load_model
        finetuned_model = finetune_func(model, new_data, params)

        # 创建新运行并注册新版本
        run_id = await self.mlflow_client.create_run("finetune_exp")  # 简化处理
        if metrics:
            for metric_name, metric_func in metrics.items():
                value = metric_func(finetuned_model, new_data)
                await self.mlflow_client.log_metric(run_id, metric_name, value)
        await self.mlflow_client.register_model(run_id, model_name)

        logger.info(f"✅ 模型 {model_name} 已微调并更新版本")
        return model_name