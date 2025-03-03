import asyncio
from typing import Callable, Optional
from app.services import HadoopAPIClient
from app.utils.logger import logger

class DataProcessor:
    """数据处理模块，负责与HDFS交互并支持自定义预处理"""

    def __init__(self):
        self.hadoop_client = HadoopAPIClient()

    async def upload_data(self, hdfs_path: str, local_file: str) -> str:
        """上传本地文件到HDFS"""
        with open(local_file, "rb") as f:
            await self.hadoop_client.upload_file(hdfs_path, f.read(), overwrite=True)
        logger.info(f"✅ 数据已上传到HDFS: {hdfs_path}")
        return hdfs_path

    async def download_data(self, hdfs_path: str) -> bytes:
        """从HDFS下载数据"""
        data = await self.hadoop_client.download_file(hdfs_path)
        logger.info(f"✅ 数据已从HDFS下载: {hdfs_path}")
        return data

    async def process_data(
        self,
        hdfs_path: str,
        preprocess_func: Optional[Callable[[bytes], bytes]] = None
    ) -> str:
        """对HDFS上的数据进行自定义预处理"""
        if preprocess_func:
            data = await self.download_data(hdfs_path)
            processed_data = preprocess_func(data)
            await self.hadoop_client.upload_file(hdfs_path, processed_data, overwrite=True)
            logger.info(f"✅ 数据预处理完成: {hdfs_path}")
        return hdfs_path