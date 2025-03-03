import asyncio
from typing import Any, Dict, Callable, Optional
from .data_processor import DataProcessor
from .experiment_manager import ExperimentManager
from .model_trainer import ModelTrainer
from .model_deployer import ModelDeployer
from .model_finetuner import ModelFinetuner

class MLWizard:
    """MLWizard主接口，提供低代码支持，接受用户自定义训练逻辑和指标"""

    def __init__(self):
        self.data_processor = DataProcessor()
        self.experiment_manager = ExperimentManager()
        self.model_trainer = ModelTrainer()
        self.model_deployer = ModelDeployer()
        self.model_finetuner = ModelFinetuner()

    async def run(
        self,
        experiment_name: str,
        model: Any,
        local_data: str,
        params: Dict[str, Any],
        train_func: Callable[[Any, bytes, Dict[str, Any]], Any],
        stage: str = "Production",
        preprocess_func: Optional[Callable[[bytes], bytes]] = None,
        metrics: Optional[Dict[str, Callable[[Any, bytes], float]]] = None
    ) -> str:
        """一行代码完成数据处理、训练和部署"""
        # 上传数据
        hdfs_path = await self.data_processor.upload_data("/data/input.csv", local_data)

        # 预处理数据
        if preprocess_func:
            hdfs_path = await self.data_processor.process_data(hdfs_path, preprocess_func)

        # 创建实验和运行
        experiment_id = await self.experiment_manager.create_experiment(experiment_name)
        run_id = await self.experiment_manager.create_run(experiment_id)

        # 下载数据并训练
        data = await self.data_processor.download_data(hdfs_path)
        trained_model = await self.model_trainer.train(run_id, model, data, params, train_func, metrics)

        # 部署模型
        deployed_model = await self.model_deployer.deploy(run_id, experiment_name, stage)

        return deployed_model

    async def finetune(
        self,
        model_name: str,
        new_data: str,
        params: Dict[str, Any],
        finetune_func: Callable[[Any, bytes, Dict[str, Any]], Any],
        metrics: Optional[Dict[str, Callable[[Any, bytes], float]]] = None
    ) -> str:
        """微调模型"""
        # 上传新数据
        new_hdfs_path = await self.data_processor.upload_data("/data/new_input.csv", new_data)

        # 微调模型
        updated_model = await self.model_finetuner.finetune(model_name, new_hdfs_path, params, finetune_func, metrics)

        return updated_model