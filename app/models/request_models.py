from pydantic import BaseModel, Field
from typing import Optional

class HadoopFileOperation(BaseModel):
    """Hadoop 文件操作基础请求模型"""
    hdfs_path: str = Field(
        ...,
        example="/user/data/example.txt",
        description="HDFS 完整路径"
    )
    overwrite: bool = Field(
        False,
        description="是否覆盖已存在文件"
    )

class HadoopFileUpload(HadoopFileOperation):
    """文件上传扩展模型"""
    replication: Optional[int] = Field(
        3,
        ge=1,
        le=10,
        description="文件副本数 (1-10)"
    )
    blocksize: Optional[int] = Field(
        134217728,  # 128MB
        description="HDFS 块大小（字节）"
    )

class HadoopFileDelete(BaseModel):
    """文件删除请求模型"""
    hdfs_path: str = Field(..., description="要删除的HDFS路径")
    recursive: bool = Field(
        False,
        description="是否递归删除目录"
    )

class MLflowExperimentOperation(BaseModel):
    """MLflow 实验操作基础请求模型"""
    experiment_name: str = Field(..., description="实验名称")
    artifact_location: Optional[str] = Field(None, description="实验工件存储路径")

class MLflowExperimentDelete(BaseModel):
    """MLflow 实验删除请求模型"""
    experiment_id: str = Field(..., description="要删除的实验ID")

class AirflowDagTrigger(BaseModel):
    """Airflow DAG 触发请求模型"""
    dag_id: str = Field(..., description="DAG ID")
    conf: Optional[dict] = Field(None, description="DAG 运行配置参数")

class AirflowDagDelete(BaseModel):
    """Airflow DAG 删除请求模型"""
    dag_id: str = Field(..., description="要删除的DAG ID")

