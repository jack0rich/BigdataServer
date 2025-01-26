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
