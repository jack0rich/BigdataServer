import docker
from docker.errors import DockerException
from app.core.config import settings
import logging

class DockerClientManager:
    def __init__(self):
        self.client = None
        self.logger = logging.getLogger("app.docker")

    async def initialize(self):
        """初始化Docker客户端连接"""
        try:
            self.client = docker.DockerClient(
                base_url=settings.DOCKER_HOST,
                timeout=settings.DOCKER_TIMEOUT
            )
            self.logger.info("Docker client initialized")
            # 测试连接
            self.client.ping()
        except DockerException as e:
            self.logger.error(f"Docker connection failed: {str(e)}")
            raise

    async def close(self):
        """关闭Docker客户端"""
        if self.client:
            self.client.close()
            self.logger.info("Docker client closed")

    async def list_containers(self, all: bool = False):
        """列出所有容器"""
        try:
            return self.client.containers.list(all=all)
        except DockerException as e:
            self.logger.error(f"List containers failed: {str(e)}")
            raise

    async def restart_container(self, container_name: str):
        """重启指定容器"""
        try:
            container = self.client.containers.get(container_name)
            container.restart()
            self.logger.info(f"Container {container_name} restarted")
            return True
        except docker.errors.NotFound:
            self.logger.error(f"Container {container_name} not found")
            raise
        except DockerException as e:
            self.logger.error(f"Restart failed: {str(e)}")
            raise

    async def get_container_logs(self, container_name: str, tail: int = 100):
        """获取容器日志"""
        try:
            container = self.client.containers.get(container_name)
            return container.logs(tail=tail, follow=False).decode("utf-8")
        except docker.errors.NotFound:
            self.logger.error(f"Container {container_name} not found")
            raise
        except DockerException as e:
            self.logger.error(f"Get logs failed: {str(e)}")
            raise

# 全局Docker客户端实例
docker_client = DockerClientManager()