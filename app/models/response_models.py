from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class BaseResponse(BaseModel):
    """基础响应模型"""
    success: bool = Field(..., description="操作是否成功")
    message: Optional[str] = Field(None, description="附加信息")
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="响应时间戳"
    )

class HDFSFileResponse(BaseResponse):
    """HDFS 文件操作响应"""
    hdfs_path: str = Field(..., description="文件HDFS路径")
    file_size: Optional[int] = Field(None, description="文件大小（字节）")
    block_size: Optional[int] = Field(None, description="块大小（字节）")
    replication: Optional[int] = Field(None, description="副本数")

class MLflowExperimentResponse(BaseResponse):
    """MLflow 实验操作响应"""
    experiment_id: str = Field(..., description="实验ID")
    experiment_name: str = Field(..., description="实验名称")
    artifact_location: Optional[str] = Field(None, description="实验工件存储路径")
    lifecycle_stage: str = Field(..., description="生命周期阶段，例如 'active' 或 'deleted'")

class AirflowDagResponse(BaseResponse):
    """Airflow DAG 操作响应"""
    dag_id: str = Field(..., description="DAG ID")
    execution_date: datetime = Field(..., description="DAG 执行日期")
    state: str = Field(..., description="DAG 运行状态，如 'success', 'failed', 'running'")
    run_id: Optional[str] = Field(None, description="DAG 运行ID")

class HDFSErrorResponse(BaseResponse):
    """错误响应模型"""
    error_code: str = Field(..., example="HDFS_PATH_NOT_FOUND")
    detail: str = Field(..., example="请求的HDFS路径不存在")
