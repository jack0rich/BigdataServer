from app.services_org import HadoopAPIClient, MLflowAPIClient
from abc import ABC, abstractmethod
import os
import tempfile


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
            data_processor: 用户自定义的数据处理对象
            model_trainer: 用户自定义的模型训练对象
        """
        self._experiment_name = experiment_name
        self._hdfs_url = hdfs_url
        self._mlflow_tracking_uri = mlflow_tracking_uri
        self._hdfs_client = HadoopAPIClient(remote_ip=hdfs_url.split('://')[1].split(':')[0])
        self._mlflow_client = MLflowAPIClient(mlflow_tracking_uri)
        self._data_processor = data_processor
        self._model_trainer = model_trainer

        # 创建实验的 HDFS 存储路径
        self._hdfs_experiment_dir = f"/experiments/{experiment_name}"
        self._hdfs_data_dir = f"{self._hdfs_experiment_dir}/data"
        self._hdfs_client.make_directory(self._hdfs_data_dir)

        # 创建 MLflow 实验
        self._experiment_id = self._mlflow_client.create_experiment(experiment_name)

    def upload_data(self, local_data_path):
        """
        将本地数据上传到 HDFS

        参数:
            local_data_path (str): 本地数据路径

        返回:
            str: HDFS 数据路径
        """
        hdfs_path = f"{self._hdfs_data_dir}/{os.path.basename(local_data_path)}"
        self._hdfs_client.upload_file(local_data_path, hdfs_path)
        return hdfs_path

    def download_data(self, hdfs_path):
        """
        从 HDFS 下载数据到本地临时目录

        参数:
            hdfs_path (str): HDFS 数据路径

        返回:
            str: 本地临时数据路径
        """
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            temp_local_path = temp_file.name
        self._hdfs_client.download_file(hdfs_path, temp_local_path)
        return temp_local_path

    def process_data(self, local_path):
        """
        加载和预处理数据

        参数:
            local_path (str): 本地数据路径

        返回:
            tuple: (X, y) 处理后的特征和标签
        """
        raw_data = self._data_processor.load_data(local_path)
        X, y = self._data_processor.preprocess(raw_data)
        return X, y

    def train(self, X, y, model_name):
        """
        训练模型并注册到 MLflow 模型注册表

        参数:
            X: 特征数据
            y: 标签数据
            model_name (str): 模型名称

        返回:
            tuple: (model, model_version) 训练好的模型和注册的模型版本对象
        """
        run_id = self._mlflow_client.create_run(self._experiment_id)
        model = self._model_trainer.train(X, y)
        self._mlflow_client.log_model(model, "model")
        self._mlflow_client.update_run(run_id, "FINISHED")

        model_version = self._mlflow_client.register_model(run_id, "model", model_name)
        print(f"模型已注册为 '{model_name}' 版本 {model_version.version}")
        return model, model_version

    def fine_tune_model(self, model, X_new, y_new):
        """
        微调模型并记录到 MLflow

        返回:
            tuple: (fine_tune_run_id, fine_tuned_model)
        """
        fine_tune_run_id = self._mlflow_client.create_run(self._experiment_id)
        fine_tuned_model = self._model_trainer.fine_tune(model, X_new, y_new)
        self._mlflow_client.log_model(fine_tuned_model, "fine_tuned_model")
        self._mlflow_client.update_run(fine_tune_run_id, "FINISHED")
        return fine_tune_run_id, fine_tuned_model

    def deploy_model(self, model_name, version):
        """
        部署模型为 REST API 服务

        参数:
            model_name (str): 模型名称
            version (str): 模型版本

        返回:
            str: 服务端点地址
        """
        endpoint = self._mlflow_client.deploy_model(model_name, version)
        print(f"模型部署成功，端点: {endpoint}")
        return endpoint


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
    wizard.upload_data(local_data_path)


