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

class HadoopFileResponse(BaseResponse):
    """Hadoop 文件操作响应"""
    hdfs_path: str = Field(..., description="文件HDFS路径")
    file_size: Optional[int] = Field(None, description="文件大小（字节）")
    block_size: Optional[int] = Field(None, description="块大小（字节）")
    replication: Optional[int] = Field(None, description="副本数")

class ErrorResponse(BaseResponse):
    """错误响应模型"""
    error_code: str = Field(..., example="HDFS_PATH_NOT_FOUND")
    detail: str = Field(..., example="请求的HDFS路径不存在")