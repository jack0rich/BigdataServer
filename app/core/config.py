from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    FASTAPI_ENV: str = "development"
    # 应用基本配置
    APP_NAME: str = "Big Data Proxy Server"
    VERSION: str = "0.1"
    DESCRIPTION: str = ""

    # 服务器配置
    HOST: str = "localhost"  # 监听的主机地址
    PORT: int = 8000       # 监听的端口

    # Hadoop 服务配置
    HADOOP_HOST: str = "localhost"
    HADOOP_PORT: int = 9870
    HADOOP_USER: str = 'root'

    # MLflow 服务配置
    MLFLOW_HOST: str = "localhost"
    MLFLOW_PORT: int = 5001

    # Airflow 服务配置
    AIRFLOW_HOST: str = "localhost"
    AIRFLOW_PORT: int = 7777

    AIRFLOW_USERNAME: str = 'airflow'
    AIRFLOW_PASSWORD: str = 'airflow'

    # Docker 集群管理配置
    DOCKER_API_URL: str = "http://docker-swarm-manager:2375"

    # 日志配置
    LOG_LEVEL: str = "INFO"

    # 安全配置
    API_KEY_HEADER: str = "X-API-Key"
    # 密钥轮换配置
    KEY_ROTATION_INTERVAL: int = 86400  # 24小时
    DEBUG: bool = True  # 开发环境下开启调试模式

    class Config:
        env_file = ".env"  # 加载 .env 文件中的环境变量
        env_file_encoding = "utf-8"

# 实例化配置对象
settings = Settings()

