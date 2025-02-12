import httpx
from typing import Dict, Any, List, Optional
from app.core.config import settings
from app.utils.logger import logger


class HadoopAPIClient:
    """Hadoop REST API 客户端封装"""

    def __init__(self):
        self.base_url = f"http://{settings.HADOOP_HOST}:{settings.HADOOP_PORT}/webhdfs/v1"
        self.client = httpx.AsyncClient()
        self.common_params = {"user.name": settings.HADOOP_USER}

    async def check_connection(self):
        """检查 WebHDFS 连接是否可用"""
        try:
            params = {**self.common_params, "op": "GETHOMEDIRECTORY"}
            logger.info(f"🔍 测试 Hadoop 连接: {self.base_url}")
            resp = await self.client.get(self.base_url, params=params)
            resp.raise_for_status()
            logger.info(f"✅ Hadoop WebHDFS 连接成功: {resp.json()}")
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ HTTP 错误: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"❌ 无法连接到 Hadoop: {e}")
            raise

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
            blocksize: int = 134217728,  # 默认 128MB
            replication: int = 3,
            permission: str = "755",
            buffersize: int = 4096,
            noredirect: bool = False
    ) -> Dict[str, Any]:
        """
        上传文件到 HDFS
        - overwrite: 是否覆盖已有文件
        - blocksize: HDFS 块大小
        - replication: 副本数
        - permission: 文件权限
        - buffersize: 缓冲区大小
        - noredirect: 是否不自动重定向
        """
        params = {
            "op": "CREATE",
            "overwrite": str(overwrite).lower(),
            "blocksize": str(blocksize),
            "replication": str(replication),
            "permission": permission,
            "buffersize": str(buffersize),
            "noredirect": str(noredirect).lower(),
            "user.name": settings.HADOOP_USER  # 这里 NameNode 需要 user.name
        }
        try:
            print(f"创建文件请求: {self.base_url}{hdfs_path}?{params}")

            # **第一步**: 发送 CREATE 请求，获取 DataNode 的跳转地址
            create_resp = await self.client.put(
                f"{self.base_url}{hdfs_path}",
                params=params,
                follow_redirects=False
            )

            if create_resp.status_code == 307:
                upload_url = create_resp.headers.get("Location")
                if not upload_url:
                    raise Exception("Hadoop 没有返回有效的上传 URL")

                print(f"上传 URL: {upload_url}")

                # **第二步**: 发送 PUT 请求，将数据上传到 DataNode (❌ 这里不要再带 `user.name`!)
                upload_resp = await self.client.put(
                    upload_url,
                    content=file_content,
                    headers={"Content-Type": "application/octet-stream"}
                )
                upload_resp.raise_for_status()
            else:
                create_resp.raise_for_status()

            # **第三步**: 确认文件是否上传成功
            return await self.get_file_status(hdfs_path)

        except httpx.HTTPStatusError as e:
            print(f"HTTP 错误: {e.response.status_code}, {e.response.text}")
            raise

    async def download_file(self, hdfs_path: str) -> bytes:
        """从 HDFS 下载文件"""
        params = {**self.common_params, "op": "OPEN"}
        try:
            resp = await self.client.get(f"{self.base_url}{hdfs_path}", params=params, follow_redirects=True)
            resp.raise_for_status()
            return resp.content
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def delete_path(self, hdfs_path: str, recursive: bool = False) -> None:
        """删除 HDFS 路径"""
        params = {**self.common_params, "op": "DELETE", "recursive": str(recursive).lower()}
        try:
            resp = await self.client.delete(f"{self.base_url}{hdfs_path}", params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def mkdir(self, hdfs_path: str, permission: Optional[str] = None) -> None:
        """创建 HDFS 目录"""
        params = {**self.common_params, "op": "MKDIRS"}
        if permission:
            params["permission"] = permission

        try:
            resp = await self.client.put(f"{self.base_url}{hdfs_path}", params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def rename_path(self, src_path: str, dest_path: str) -> None:
        """重命名或移动 HDFS 路径"""
        params = {**self.common_params, "op": "RENAME", "destination": dest_path}
        try:
            resp = await self.client.put(f"{self.base_url}{src_path}", params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def list_dir(self, hdfs_path: str) -> List[Dict[str, Any]]:
        """列出 HDFS 目录下的所有文件和子目录"""
        params = {"op": "LISTSTATUS", "user.name": settings.HADOOP_USER}
        try:
            resp = await self.client.get(f"{self.base_url}{hdfs_path}", params=params)
            resp.raise_for_status()
            file_list = resp.json()["FileStatuses"]["FileStatus"]
            return [{"path": f"{hdfs_path}/{f['pathSuffix']}", "type": f['type'], "size": f["length"]} for f in file_list]
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    async def get_file_status(self, hdfs_path: str) -> Dict[str, Any]:
        """获取 HDFS 文件状态"""
        params = {"op": "GETFILESTATUS", "user.name": settings.HADOOP_USER}
        try:
            resp = await self.client.get(f"{self.base_url}{hdfs_path}", params=params)
            resp.raise_for_status()
            return self._parse_file_status(resp.json()["FileStatus"], hdfs_path)
        except httpx.HTTPStatusError as e:
            await self._handle_http_error(e)

    def _parse_file_status(self, status_data: Dict[str, Any], hdfs_path: str) -> Dict[str, Any]:
        """解析 HDFS 文件状态响应"""
        return {
            "hdfs_path": hdfs_path,
            "file_size": status_data["length"],
            "block_size": status_data["blockSize"],
            "replication": status_data["replication"],
            "type": status_data["type"]
        }

    async def _handle_http_error(self, error: httpx.HTTPStatusError):
        """统一处理 HTTP 错误"""
        logger.error(f"Hadoop API Error: {error}")

        try:
            error_msg = error.response.json().get("RemoteException", {}).get("message", "Unknown error")
        except Exception:
            error_msg = error.response.text  # 兜底处理

        if error.response.status_code == 404:
            raise self.HDFSNotFoundError(f"Requested path not found: {error_msg}")
        elif error.response.status_code == 409:
            raise self.HDFSConflictError(f"Path already exists: {error_msg}")
        else:
            raise Exception(f"Hadoop API Error: {error_msg}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.client.aclose()