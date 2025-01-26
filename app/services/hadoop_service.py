import httpx
from typing import Dict, Any
from app.core.config import settings
from app.utils.logger import logger


class HadoopAPIClient:
    """Hadoop REST API 客户端封装"""

    def __init__(self):
        self.base_url = f"https://{settings.HADOOP_NAMENODE}:{settings.HADOOP_WEB_PORT}/webhdfs/v1"
        self.client = httpx.AsyncClient()
        self.common_params = {
            "user.name": settings.HADOOP_USER,
            "op": "CREATE"  # 默认操作类型
        }

    class HDFSNotFoundError(Exception):
        """路径不存在异常"""
        pass

    class HDFSConflictError(Exception):
        """路径冲突异常"""
        pass

    async def upload_file(
            self,
            hdfs_path: str,
            file_content: bytes,
            overwrite: bool = False,
            replication: int = 3,
            blocksize: int = 134217728
    ) -> Dict[str, Any]:
        """
        通过WebHDFS REST API上传文件

        :param hdfs_path: HDFS目标路径
        :param file_content: 文件二进制内容
        :return: 包含文件元数据的字典
        """
        params = {
            **self.common_params,
            "overwrite": str(overwrite).lower(),
            "replication": replication,
            "blocksize": blocksize
        }

        try:
            # 第一步：获取文件上传地址
            create_resp = await self.client.put(
                f"{self.base_url}{hdfs_path}",
                params=params,
                follow_redirects=False
            )

            # 处理重定向（WebHDFS标准流程）
            if create_resp.status_code == 307:
                upload_url = create_resp.headers["Location"]
                upload_resp = await self.client.put(
                    upload_url,
                    content=file_content,
                    headers={"Content-Type": "application/octet-stream"}
                )
                upload_resp.raise_for_status()
            else:
                create_resp.raise_for_status()

            # 获取文件状态
            status_resp = await self.client.get(
                f"{self.base_url}{hdfs_path}",
                params={"op": "GETFILESTATUS"}
            )
            status_resp.raise_for_status()

            return self._parse_file_status(
                status_resp.json()["FileStatus"],
                hdfs_path
            )

        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def delete_path(
            self,
            hdfs_path: str,
            recursive: bool = False
    ) -> None:
        """删除HDFS路径"""
        params = {
            **self.common_params,
            "op": "DELETE",
            "recursive": str(recursive).lower()
        }

        try:
            resp = await self.client.delete(
                f"{self.base_url}{hdfs_path}",
                params=params
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    def _parse_file_status(
            self,
            status_data: Dict,
            hdfs_path: str
    ) -> Dict[str, Any]:
        """解析HDFS文件状态响应"""
        return {
            "hdfs_path": hdfs_path,
            "file_size": status_data["length"],
            "block_size": status_data["blockSize"],
            "replication": status_data["replication"]
        }

    async def _handle_http_error(self, error: httpx.HTTPStatusError):
        """统一处理HTTP错误"""
        logger.error(f"Hadoop API Error: {error}")

        if error.response.status_code == 404:
            raise self.HDFSNotFoundError("Requested path not found")
        elif error.response.status_code == 409:
            raise self.HDFSConflictError("Path already exists")
        else:
            error_msg = error.response.json().get("RemoteException", {}).get("message")
            raise Exception(f"Hadoop API Error: {error_msg}")

    async def close(self):
        """关闭HTTP客户端"""
        await self.client.aclose()