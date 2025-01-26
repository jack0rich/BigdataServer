# FastAPI 中转服务器开发手册

## 项目结构说明

``project_root/`` 是项目的根目录，以下是各文件夹和文件的作用：

### 1. **`app/`**

- **`main.py`**

  - FastAPI 项目入口，启动服务的核心文件。
- **`api/`**

  - **`endpoints/`**
    - 存放具体 API 路由文件：
      - `hadoop.py`：与 Hadoop 交互的接口（如数据存储）。
      - `mlflow.py`：与 MLflow 交互的接口（如模型训练和管理）。
      - `airflow.py`：与 Airflow 交互的接口（如管理工作流）。
- **`core/`**

  - **`config.py`**
    - 用于存储全局配置（如环境变量、数据库连接）。
  - **`security.py`**
    - 安全相关的配置与工具（如身份认证）。
- **`services/`**

  - 存放业务逻辑文件：
    - `hadoop_service.py`：封装与 Hadoop 服务的交互逻辑。
    - `mlflow_service.py`：封装与 MLflow 服务的交互逻辑。
    - `airflow_service.py`：封装与 Airflow 服务的交互逻辑。
- **`models/`**

  - 定义 API 的数据模型：
    - `request_models.py`：API 请求体的数据结构。
    - `response_models.py`：API 响应体的数据结构。
- **`utils/`**

  - 常用工具库：
    - `docker_client.py`：封装 Docker 客户端功能。
    - `logger.py`：封装日志记录功能。
- **`tests/`**

  - 测试文件：
    - `test_main.py`：用于测试主程序和各模块的功能。

---

### 2. **根目录文件**

- **`Dockerfile`**

  - 构建 Docker 镜像的配置文件。
- **`docker-compose.yml`**

  - 管理多个 Docker 容器的工具，简化服务部署。
- **`.env`**

  - 存储环境变量的文件，避免敏感信息硬编码到代码中。
- **`requirements.txt`**

  - 项目依赖列表，安装时运行：
    ```bash
    pip install -r requirements.txt
    ```

---

## 快速启动

1. **克隆项目并进入目录**
   ```bash
   git clone <repository_url>
   cd project_root
   ```
