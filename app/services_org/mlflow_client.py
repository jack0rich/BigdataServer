import mlflow
from mlflow.tracking import MlflowClient
from mlflow.exceptions import MlflowException
import mlflow.pyfunc
import os
import requests
import subprocess
import json
from datetime import datetime
from packaging import version



class MLflowAPIClient:
    def __init__(self, tracking_uri, registry_uri=None):
        """
        初始化 MLflow API 客户端。

        :param tracking_uri: MLflow 跟踪服务器的 URI（如 http://REMOTE_IP:5000）
        :param registry_uri: MLflow 模型注册表的 URI（可选，默认与 tracking_uri 相同）
        """
        self.tracking_uri = tracking_uri
        self.registry_uri = registry_uri if registry_uri else tracking_uri

        # 设置 MLflow 跟踪服务器
        mlflow.set_tracking_uri(self.tracking_uri)

        # 初始化 MLflow 客户端
        self.client = MlflowClient(tracking_uri=self.tracking_uri, registry_uri=self.registry_uri)

    # 辅助方法：根据名称查找实验
    def get_experiment_by_name(self, name):
        """
        根据名称查找实验。

        :param name: 实验名称
        :return: 实验对象（如果存在），否则返回 None
        """
        try:
            experiments = self.client.search_experiments(filter_string=f"name = '{name}'")
            if experiments:
                return experiments[0]
            return None
        except Exception as e:
            raise Exception(f"查找实验失败: {str(e)}")

    # 辅助方法：根据名称查找注册模型
    def get_registered_model_by_name(self, name):
        """
        根据名称查找注册模型。

        :param name: 模型名称
        :return: 模型对象（如果存在），否则返回 None
        """
        try:
            models = self.client.search_registered_models(filter_string=f"name = '{name}'")
            if models:
                return models[0]
            return None
        except Exception as e:
            raise Exception(f"查找模型失败: {str(e)}")

    # 实验管理
    def create_experiment(self, name, tags=None):
        """
        创建一个新的实验。如果实验已存在，返回已有实验的 ID。

        :param name: 实验名称
        :param tags: 实验标签（字典形式）
        :return: 实验 ID
        """
        try:
            # 检查实验是否已存在
            existing_experiment = self.get_experiment_by_name(name)
            if existing_experiment:
                print(f"实验 '{name}' 已存在，ID: {existing_experiment.experiment_id}")
                return existing_experiment.experiment_id

            # 创建新实验
            experiment_id = self.client.create_experiment(name, tags=tags)
            print(f"实验 '{name}' 已创建，ID: {experiment_id}")
            return experiment_id
        except Exception as e:
            raise Exception(f"创建实验失败: {str(e)}")

    def list_experiments(self):
        """
        列出所有实验。

        :return: 实验列表
        """
        try:
            experiments = self.client.search_experiments()
            return experiments
        except Exception as e:
            raise Exception(f"列出实验失败: {str(e)}")

    def get_experiment(self, experiment_id):
        """
        获取指定实验的详细信息。

        :param experiment_id: 实验 ID
        :return: 实验对象
        """
        try:
            experiment = self.client.get_experiment(experiment_id)
            return experiment
        except Exception as e:
            raise Exception(f"获取实验失败: {str(e)}")

    def delete_experiment(self, experiment_id):
        """
        删除指定实验。

        :param experiment_id: 实验 ID
        """
        try:
            self.client.delete_experiment(experiment_id)
            print(f"实验 ID {experiment_id} 已删除")
        except Exception as e:
            raise Exception(f"删除实验失败: {str(e)}")

    # 运行管理
    def create_run(self, experiment_id, run_name=None, tags=None):
        """
        在指定实验中创建新的运行。

        :param experiment_id: 实验 ID
        :param run_name: 运行名称（可选）
        :param tags: 运行标签（字典形式）
        :return: 运行 ID
        """
        try:
            run = self.client.create_run(experiment_id, run_name=run_name, tags=tags)
            print(f"运行已创建，ID: {run.info.run_id}")
            return run.info.run_id
        except Exception as e:
            raise Exception(f"创建运行失败: {str(e)}")

    def get_run(self, run_id):
        """
        获取指定运行的详细信息。

        :param run_id: 运行 ID
        :return: 运行对象
        """
        try:
            run = self.client.get_run(run_id)
            return run
        except Exception as e:
            raise Exception(f"获取运行失败: {str(e)}")

    def get_active_run_id(self):
        """
        获取当前活动运行的 ID。

        :return: 运行 ID
        """
        try:
            run = mlflow.active_run()
            if run:
                return run.info.run_id
            raise Exception("当前没有活动运行")
        except Exception as e:
            raise Exception(f"获取活动运行 ID 失败: {str(e)}")

    def update_run(self, run_id, status):
        """
        更新运行的状态。

        :param run_id: 运行 ID
        :param status: 运行状态（如 'FINISHED', 'FAILED'）
        :param end_time: 结束时间（可选，默认当前时间，仅在新版本支持）
        """
        try:
            mlflow_version = mlflow.__version__

            # 检查 MLflow 版本是否支持 end_time 参数
            if version.parse(mlflow_version) >= version.parse("2.0"):
                self.client.update_run(run_id, status)
            else:
                self.client.update_run(run_id, status)
            print(f"运行 ID {run_id} 已更新状态为 {status}")
        except Exception as e:
            raise Exception(f"更新运行失败: {str(e)}")

    def delete_run(self, run_id):
        """
        删除指定运行。

        :param run_id: 运行 ID
        """
        try:
            self.client.delete_run(run_id)
            print(f"运行 ID {run_id} 已删除")
        except Exception as e:
            raise Exception(f"删除运行失败: {str(e)}")

    # 参数、指标、标签管理
    def log_param(self, run_id, key, value):
        """
        记录运行的参数。

        :param run_id: 运行 ID
        :param key: 参数名称
        :param value: 参数值
        """
        try:
            self.client.log_param(run_id, key, value)
            print(f"参数 {key}={value} 已记录到运行 ID {run_id}")
        except Exception as e:
            raise Exception(f"记录参数失败: {str(e)}")

    def log_metric(self, run_id, key, value, step=None):
        """
        记录运行的指标。

        :param run_id: 运行 ID
        :param key: 指标名称
        :param value: 指标值
        :param step: 指标的步骤（可选）
        """
        try:
            self.client.log_metric(run_id, key, value, step=step)
            print(f"指标 {key}={value} 已记录到运行 ID {run_id}")
        except Exception as e:
            raise Exception(f"记录指标失败: {str(e)}")

    def set_tag(self, run_id, key, value):
        """
        设置运行的标签。

        :param run_id: 运行 ID
        :param key: 标签名称
        :param value: 标签值
        """
        try:
            self.client.set_tag(run_id, key, value)
            print(f"标签 {key}={value} 已设置到运行 ID {run_id}")
        except Exception as e:
            raise Exception(f"设置标签失败: {str(e)}")

    # 模型管理
    def register_model(self, run_id, model_path, model_name):
        """
        注册模型到模型注册表。如果模型已存在，创建新版本。

        :param run_id: 运行 ID
        :param model_path: 模型在工件中的路径
        :param model_name: 模型名称
        :return: 模型版本对象
        """
        try:
            source = f"{self.tracking_uri}/experiments/{run_id}/artifacts/{model_path}"

            # 检查模型是否已存在
            existing_model = self.get_registered_model_by_name(model_name)
            if not existing_model:
                self.client.create_registered_model(model_name)
                print(f"模型 '{model_name}' 已创建")

            # 创建新版本
            model_version = self.client.create_model_version(model_name, source, run_id)
            print(f"模型 '{model_name}' 已注册，版本: {model_version.version}")
            return model_version
        except Exception as e:
            raise Exception(f"注册模型失败: {str(e)}")

    def list_registered_models(self):
        """
        列出所有注册的模型。

        :return: 模型列表
        """
        try:
            models = self.client.search_registered_models()
            return models
        except Exception as e:
            raise Exception(f"列出模型失败: {str(e)}")

    def log_model(self, model, artifact_path="model"):
        """
        记录模型到 MLflow。

        :param model: 训练好的模型对象
        :param artifact_path: 模型存储路径（默认 "model"）
        """
        try:
            import mlflow
            if not mlflow.active_run():
                raise Exception("没有活动运行，请先调用 start_run 方法")

            # 动态检测模型类型并记录
            model_type = str(type(model))
            if 'sklearn' in model_type:
                import mlflow.sklearn
                mlflow.sklearn.log_model(model, artifact_path)
            elif 'tensorflow' in model_type or 'keras' in model_type:
                import mlflow.tensorflow
                mlflow.tensorflow.log_model(model, artifact_path)
            elif 'torch' in model_type:
                import mlflow.pytorch
                mlflow.pytorch.log_model(model, artifact_path)
            else:
                import mlflow.pyfunc
                # 将模型包装为 pyfunc 类型
                class PyFuncModel(mlflow.pyfunc.PythonModel):
                    def __init__(self, model):
                        self.model = model

                    def predict(self, context, model_input):
                        return self.model.predict(model_input)

                mlflow.pyfunc.log_model(artifact_path, python_model=PyFuncModel(model))
            print(f"模型已记录到 {artifact_path}")
        except Exception as e:
            raise Exception(f"记录模型失败: {str(e)}")

    def get_model_version(self, model_name, version):
        """
        获取指定模型版本的详细信息。

        :param model_name: 模型名称
        :param version: 模型版本号
        :return: 模型版本对象
        """
        try:
            model_version = self.client.get_model_version(model_name, version)
            return model_version
        except Exception as e:
            raise Exception(f"获取模型版本失败: {str(e)}")

    def delete_registered_model(self, model_name):
        """
        删除指定注册模型（包括所有版本）。

        :param model_name: 模型名称
        """
        try:
            self.client.delete_registered_model(model_name)
            print(f"注册模型 '{model_name}' 已删除")
        except Exception as e:
            raise Exception(f"删除注册模型失败: {str(e)}")

    def delete_model_version(self, model_name, version):
        """
        删除指定模型的某个版本。

        :param model_name: 模型名称
        :param version: 模型版本号
        """
        try:
            self.client.delete_model_version(model_name, version)
            print(f"模型 '{model_name}' 的版本 {version} 已删除")
        except Exception as e:
            raise Exception(f"删除模型版本失败: {str(e)}")

    # 模型部署
    def deploy_model(self, model_name, version, host="127.0.0.1", port=5001, background=True):
        """
        将注册的模型部署为 REST API 服务。

        :param model_name: 模型名称
        :param version: 模型版本号
        :param host: 服务监听的主机地址（默认 127.0.0.1）
        :param port: 服务监听的端口（默认 5001）
        :param background: 是否在后台运行服务（默认 True）
        :return: 服务端点地址
        """
        try:
            model_uri = f"models:/{model_name}/{version}"
            cmd = [
                "mlflow", "models", "serve",
                "-m", model_uri,
                "--host", host,
                "--port", str(port),
                "--no-conda"  # 假设不使用 Conda 环境，直接使用当前环境
            ]

            if background:
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                print(f"模型 '{model_name}' 版本 {version} 已部署为 REST API 服务，端点: http://{host}:{port}")
                return f"http://{host}:{port}"
            else:
                subprocess.run(cmd, check=True)
                print(f"模型 '{model_name}' 版本 {version} 已部署为 REST API 服务，端点: http://{host}:{port}")
                return f"http://{host}:{port}"
        except subprocess.CalledProcessError as e:
            raise Exception(f"部署模型失败: {str(e)}")
        except Exception as e:
            raise Exception(f"部署模型失败: {str(e)}")

    # 模型推理（本地）
    def predict_locally(self, model_name, version, input_data):
        """
        在本地加载模型并进行推理。

        :param model_name: 模型名称
        :param version: 模型版本号
        :param input_data: 输入数据（格式取决于模型）
        :return: 推理结果
        """
        try:
            model_uri = f"models:/{model_name}/{version}"
            model = mlflow.pyfunc.load_model(model_uri)
            prediction = model.predict(input_data)
            print(f"本地推理完成，输入: {input_data}, 输出: {prediction}")
            return prediction
        except Exception as e:
            raise Exception(f"本地推理失败: {str(e)}")

    # 模型推理（远程）
    def predict_remotely(self, endpoint, input_data):
        """
        通过部署的模型服务端点进行远程推理。

        :param endpoint: 服务端点地址（如 http://127.0.0.1:5001）
        :param input_data: 输入数据（格式取决于模型）
        :return: 推理结果
        """
        try:
            headers = {"Content-Type": "application/json"}
            # 假设输入数据是 JSON 格式，具体格式取决于模型
            response = requests.post(f"{endpoint}/invocations", json=input_data, headers=headers)
            response.raise_for_status()
            prediction = response.json()
            print(f"远程推理完成，输入: {input_data}, 输出: {prediction}")
            return prediction
        except requests.RequestException as e:
            raise Exception(f"远程推理失败: {str(e)}")


    # 工件管理
    def log_artifact(self, run_id, local_path, artifact_path=None):
        """
        上传工件到指定运行。

        :param run_id: 运行 ID
        :param local_path: 本地文件路径
        :param artifact_path: 工件在运行中的路径（可选）
        """
        try:
            self.client.log_artifact(run_id, local_path, artifact_path)
            print(f"工件 {local_path} 已上传到运行 ID {run_id}")
        except Exception as e:
            raise Exception(f"上传工件失败: {str(e)}")

    def download_artifact(self, run_id, artifact_path, local_dir):
        """
        下载运行中的工件到本地。

        :param run_id: 运行 ID
        :param artifact_path: 工件路径
        :param local_dir: 本地目标目录
        :return: 下载后的本地路径
        """
        try:
            local_path = self.client.download_artifacts(run_id, artifact_path, local_dir)
            print(f"工件 {artifact_path} 已下载到 {local_path}")
            return local_path
        except Exception as e:
            raise Exception(f"下载工件失败: {str(e)}")

    # 跟踪服务器管理
    def get_tracking_server_info(self):
        """
        获取跟踪服务器的信息（使用官方 API 路径）。

        :return: 服务器信息（JSON 格式）
        """
        try:
            response = requests.get(f"{self.tracking_uri}/api/2.0/mlflow/experiments/search")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"获取服务器信息失败: {str(e)}")


if __name__ == "__main__":
    # 测试代码
    tracking_uri = "http://127.0.0.1:5001"
    client = MLflowAPIClient(tracking_uri=tracking_uri)

    # 创建实验（测试重复名称）
    print("\n创建实验（第一次）...")
    experiment_id = client.create_experiment("TestExperiment2")

    print("\n创建实验（第二次，重复名称）...")
    experiment_id_again = client.create_experiment("TestExperiment2")

    # 创建运行
    print("\n创建运行...")
    run_id = client.create_run(experiment_id, run_name="TestRun")

    # 记录参数
    print("\n记录参数...")
    client.log_param(run_id, "learning_rate", 0.01)

    # 记录指标
    print("\n记录指标...")
    client.log_metric(run_id, "accuracy", 0.95)

    # 设置标签
    print("\n设置标签...")
    client.set_tag(run_id, "model_type", "logistic_regression")

    # 更新运行状态
    print("\n更新运行状态...")
    client.update_run(run_id, "FINISHED")

    # 上传工件
    print("\n上传工件...")
    with open("model.txt", "w") as f:
        f.write("This is a test model file.")
    client.log_artifact(run_id, "model.txt", "models")

    # 下载工件
    print("\n下载工件...")
    client.download_artifact(run_id, "models/model.txt", "./")

    # 注册模型（测试重复名称）
    print("\n注册模型（第一次）...")
    model_version = client.register_model(run_id, "models/model.txt", "TestModel")

    print("\n注册模型（第二次，重复名称）...")
    model_version_again = client.register_model(run_id, "models/model.txt", "TestModel")

    # 列出注册的模型
    print("\n列出注册的模型...")
    models = client.list_registered_models()
    for model in models:
        print(f"模型: {model.name}")

    # 获取模型版本信息
    print("\n获取模型版本信息...")
    model_version_info = client.get_model_version("TestModel", model_version.version)
    print(f"模型版本信息: {model_version_info}")

    # 删除模型版本
    print("\n删除模型版本...")
    client.delete_model_version("TestModel", model_version.version)

    # 删除注册模型
    print("\n删除注册模型...")
    client.delete_registered_model("TestModel")

    # 删除运行和实验（清理）
    print("\n删除运行...")
    client.delete_run(run_id)

    # 删除实验
    print("\n删除实验...")
    client.delete_experiment(experiment_id)

    # 获取服务器信息
    print("\n获取跟踪服务器信息...")
    server_info = client.get_tracking_server_info()
    print(f"服务器信息: {server_info}")