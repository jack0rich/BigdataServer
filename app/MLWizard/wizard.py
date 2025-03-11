from app.services_org import HadoopAPIClient, MLflowAPIClient
from abc import ABC, abstractmethod
import os
import tempfile
import mlflow
from mlflow.tracking import MlflowClient


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
        self.hdfs_client = HadoopAPIClient(remote_ip=hdfs_url.split('://')[1].split(':')[0])
        self.mlflow_client = MLflowAPIClient(mlflow_tracking_uri)
        self.data_processor = data_processor
        self.model_trainer = model_trainer

        # 创建实验的 STORAGE 存储路径
        self.hdfs_experiment_dir = f"/experiments/{experiment_name}"
        self.hdfs_data_dir = f"{self.hdfs_experiment_dir}/data"
        self.hdfs_client.make_directory(self.hdfs_data_dir)

        # 创建 MLflow 实验
        self.experiment_id = self.mlflow_client.create_experiment(experiment_name)

    def run(self, local_data_path, fine_tune_data=None):
        """
        执行完整的机器学习流程

        参数:
            local_data_path (str): 本地数据路径，数据将被上传到 HDFS
            fine_tune_data (tuple, optional): 微调数据 (X_new, y_new)，可选
        """
        # 1. 将本地数据上传到 HDFS
        hdfs_data_path = self._upload_data(local_data_path)

        # 2. 从 HDFS 下载数据到本地临时目录
        temp_local_path = self._download_data(hdfs_data_path)

        # 3. 数据加载和预处理
        raw_data = self.data_processor.load_data(temp_local_path)
        X, y = self.data_processor.preprocess(raw_data)

        # 4. 模型训练并记录到 MLflow
        run_id = self.mlflow_client.create_run(self.experiment_id)
        model = self.model_trainer.train(X, y)
        self.mlflow_client.log_model(model, "model")
        self.mlflow_client.update_run(run_id, "FINISHED")

        # 5. 模型注册
        model_version = self.mlflow_client.register_model(run_id, "model", self.experiment_name)
        print(f"模型已注册为 '{self.experiment_name}' 版本 {model_version.version}")

        # 6. 可选的模型微调
        if fine_tune_data:
            X_new, y_new = fine_tune_data
            fine_tune_run_id = self.mlflow_client.create_run(self.experiment_id)
            model = self.model_trainer.fine_tune(model, X_new, y_new)
            self.mlflow_client.log_model(model, "fine_tuned_model")
            self.mlflow_client.update_run(fine_tune_run_id, "FINISHED")
            fine_tuned_version = self.mlflow_client.register_model(fine_tune_run_id, "fine_tuned_model",
                                                                   self.experiment_name)
            print(f"微调后的模型已注册为版本 {fine_tuned_version.version}")

        # 7. 模型部署
        endpoint = self.mlflow_client.deploy_model(self.experiment_name, model_version.version)
        print(f"模型部署成功，端点: {endpoint}")

        return model

    def _upload_data(self, local_data_path):
        """将本地数据上传到 HDFS"""
        hdfs_path = f"{self.hdfs_data_dir}/{os.path.basename(local_data_path)}"
        self.hdfs_client.upload_file(local_data_path, hdfs_path)
        return hdfs_path

    def _download_data(self, hdfs_path):
        """从 HDFS 下载数据到本地临时目录"""
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            temp_local_path = temp_file.name
        self.hdfs_client.download_file(hdfs_path, temp_local_path)
        return temp_local_path


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


# 用户自定义 ModelTrainer 示例
class MyModelTrainer(ModelTrainer):
    def train(self, X, y):
        from sklearn.ensemble import RandomForestClassifier
        model = RandomForestClassifier()
        model.fit(X, y)
        return model

    def fine_tune(self, model, X_new, y_new):
        model.fit(X_new, y_new)  # 全量重训练
        return model


# 使用示例
if __name__ == "__main__":
    # 初始化参数
    experiment_name = "MyExperiment"
    hdfs_url = "http://localhost:9870"
    mlflow_tracking_uri = "http://localhost:5000"

    # 创建自定义实例
    data_processor = MyDataProcessor()
    model_trainer = MyModelTrainer()

    # 初始化 MLWizard
    wizard = MLWizard(
        experiment_name=experiment_name,
        hdfs_url=hdfs_url,
        mlflow_tracking_uri=mlflow_tracking_uri,
        data_processor=data_processor,
        model_trainer=model_trainer
    )

    # 运行流程
    local_data_path = "data/train.csv"  # 用户只需提供本地路径
    wizard.run(local_data_path)

