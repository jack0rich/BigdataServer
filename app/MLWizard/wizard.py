from app.services_org import HadoopAPIClient, MLflowAPIClient
from abc import ABC, abstractmethod
import os


# 定义 DataProcessor 接口
class DataProcessor(ABC):
    @abstractmethod
    def load_data(self, local_path):
        """
        加载数据

        参数:
            local_path (str): 本地数据路径

        返回:
            原始数据
        """
        pass

    @abstractmethod
    def preprocess(self, raw_data):
        """
        数据预处理

        参数:
            raw_data: 原始数据

        返回:
            tuple: (X, y) 处理后的特征和标签
        """
        pass


# 定义 ModelTrainer 接口
class ModelTrainer(ABC):
    @abstractmethod
    def train(self, X, y):
        """
        训练模型

        参数:
            X: 特征数据
            y: 标签数据

        返回:
            训练好的模型
        """
        pass

    @abstractmethod
    def fine_tune(self, model, X_new, y_new):
        """
        模型微调

        参数:
            model: 已训练的模型
            X_new: 新特征数据
            y_new: 新标签数据

        返回:
            微调后的模型
        """
        pass


# MLWizard 类
class MLWizard:
    def __init__(self, experiment_name, hdfs_url, mlflow_tracking_uri, data_processor, model_trainer):
        """
        初始化 MLWizard，创建实验相关存储和跟踪实例

        参数:
            experiment_name (str): 实验名称
            hdfs_url (str): HDFS 服务地址
            mlflow_tracking_uri (str): MLflow 跟踪服务地址
            data_processor (DataProcessor): 用户自定义的数据处理对象
            model_trainer (ModelTrainer): 用户自定义的模型训练对象
        """
        self.experiment_name = experiment_name
        self.hdfs_url = hdfs_url
        self.mlflow_tracking_uri = mlflow_tracking_uri
        self.data_processor = data_processor
        self.model_trainer = model_trainer

    def run(self, data_path, fine_tune_data=None):
        """
        执行完整的机器学习流程

        参数:
            data_path (str): HDFS 数据路径
            fine_tune_data (tuple, optional): 微调数据 (X_new, y_new)，可选
        """
        # 1. 从 HDFS 下载数据并加载
        local_data_path = self._download_data(data_path)
        raw_data = self.data_processor.load_data(local_data_path)

        # 2. 数据预处理
        X, y = self.data_processor.preprocess(raw_data)

        # 3. 模型训练
        model = self.model_trainer.train(X, y)

        # 4. 可选的模型微调
        if fine_tune_data:
            X_new, y_new = fine_tune_data
            model = self.model_trainer.fine_tune(model, X_new, y_new)

        print("模型训练和微调完成")
        return model

    def _download_data(self, hdfs_path):
        """从 HDFS 下载数据到本地（模拟实现）"""
        local_path = f"temp_{os.path.basename(hdfs_path)}"
        print(f"模拟从 {hdfs_path} 下载数据到 {local_path}")
        return local_path


# 用户自定义 DataProcessor 示例
class MyDataProcessor(DataProcessor):
    def load_data(self, local_path):
        import pandas as pd
        return pd.read_csv(local_path)

    def preprocess(self, raw_data):
        from sklearn.preprocessing import StandardScaler
        X = raw_data.drop(columns=['target'])
        y = raw_data['target']
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        return X_scaled, y

