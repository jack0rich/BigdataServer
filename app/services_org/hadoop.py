from hdfs import InsecureClient
from app.core.config import settings
import requests
import os


class HadoopAPIClient:
    def __init__(self, remote_ip='localhost', hdfs_port=9870, yarn_port=8081, user='root'):
        """
        初始化 Hadoop API 客户端。

        :param remote_ip: 远程服务器的 IP 地址
        :param hdfs_port: NameNode 的 WebHDFS 端口（默认 9870）
        :param yarn_port: ResourceManager 的 REST API 端口（默认 8081）
        :param user: Hadoop 用户名（默认 'hadoop'）
        """
        self.hdfs_url = f'http://{remote_ip}:{hdfs_port}'
        self.yarn_url = f'http://{remote_ip}:{yarn_port}'
        self.user = user

        # 初始化 HDFS 客户端，使用 WebHDFS 接口
        self.hdfs_client = InsecureClient(self.hdfs_url, user=self.user)

    # HDFS 操作
    def list_directory(self, path):
        """
        列出 HDFS 目录下的文件和子目录。

        :param path: HDFS 路径（如 '/user/hadoop'）
        :return: 文件和目录列表
        """
        try:
            return self.hdfs_client.list(path)
        except Exception as e:
            raise Exception(f"列出目录 {path} 失败: {str(e)}")

    def upload_file(self, local_path, hdfs_path):
        """
        将本地文件上传到 HDFS。

        :param local_path: 本地文件路径
        :param hdfs_path: HDFS 目标路径
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"本地文件 {local_path} 不存在")
        try:
            self.hdfs_client.upload(hdfs_path, local_path)
            print(f"文件已上传至 {hdfs_path}")
        except Exception as e:
            raise Exception(f"上传文件失败: {str(e)}")

    def download_file(self, hdfs_path, local_path):
        """
        从 HDFS 下载文件到本地。

        :param hdfs_path: HDFS 文件路径
        :param local_path: 本地目标路径
        """
        try:
            self.hdfs_client.download(hdfs_path, local_path)
            print(f"文件已下载至 {local_path}")
        except Exception as e:
            raise Exception(f"下载文件失败: {str(e)}")

    def delete_file(self, hdfs_path, recursive=False):
        """
        删除 HDFS 上的文件或目录。

        :param hdfs_path: HDFS 路径
        :param recursive: 是否递归删除目录（默认 False）
        """
        try:
            self.hdfs_client.delete(hdfs_path, recursive=recursive)
            print(f"已删除 {hdfs_path}")
        except Exception as e:
            raise Exception(f"删除文件失败: {str(e)}")

    def make_directory(self, path):
        """
        在 HDFS 上创建目录。

        :param path: HDFS 目录路径
        """
        try:
            self.hdfs_client.makedirs(path)
            print(f"目录 {path} 已创建")
        except Exception as e:
            raise Exception(f"创建目录失败: {str(e)}")

    # YARN 操作
    def get_cluster_info(self):
        """
        获取 YARN 集群信息。

        :return: 集群信息（JSON 格式）
        """
        try:
            response = requests.get(f'{self.yarn_url}/ws/v1/cluster/info')
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"获取集群信息失败: {str(e)}")

    def list_applications(self):
        """
        列出 YARN 上的应用程序。

        :return: 应用程序列表（JSON 格式）
        """
        try:
            response = requests.get(f'{self.yarn_url}/ws/v1/cluster/apps')
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"列出应用程序失败: {str(e)}")

    def rename_path(self, src_path, dst_path):
        """
        重命名 HDFS 上的文件或目录。

        :param src_path: 源 HDFS 路径
        :param dst_path: 目标 HDFS 路径
        """
        try:
            self.hdfs_client.rename(src_path, dst_path)
            print(f"已将 {src_path} 重命名为 {dst_path}")
        except Exception as e:
            raise Exception(f"重命名失败: {str(e)}")


def run_tests():
    # 配置
    REMOTE_IP = '127.0.0.1'  # 替换为您的实际服务器 IP
    LOCAL_TEST_FILE = 'model.txt'  # 本地测试文件
    HDFS_TEST_DIR = '/user/hadoop/test_dir'
    HDFS_TEST_FILE = f'{HDFS_TEST_DIR}/test.txt'
    HDFS_RENAMED_FILE = f'{HDFS_TEST_DIR}/renamed_test.txt'
    LOCAL_DOWNLOADED_FILE = 'downloaded_test.txt'

    # 创建客户端实例
    print("正在初始化 HadoopAPIClient...")
    client = HadoopAPIClient(remote_ip=REMOTE_IP)

    # 测试 1: 创建目录
    print("\n测试 1: 创建 HDFS 目录")
    try:
        client.make_directory(HDFS_TEST_DIR)
    except Exception as e:
        print(f"测试 1 失败: {str(e)}")

    # 测试 2: 上传文件
    print("\n测试 2: 上传文件到 HDFS")
    try:
        client.upload_file(LOCAL_TEST_FILE, HDFS_TEST_FILE)
    except Exception as e:
        print(f"测试 2 失败: {str(e)}")

    # 测试 3: 列出目录
    print("\n测试 3: 列出 HDFS 目录")
    try:
        files = client.list_directory(HDFS_TEST_DIR)
        print(f"目录内容: {files}")
    except Exception as e:
        print(f"测试 3 失败: {str(e)}")

    # 测试 4: 重命名文件
    print("\n测试 4: 重命名 HDFS 文件")
    try:
        client.rename_path(HDFS_TEST_FILE, HDFS_RENAMED_FILE)
    except Exception as e:
        print(f"测试 4 失败: {str(e)}")

    # 测试 5: 下载文件
    print("\n测试 5: 从 HDFS 下载文件")
    try:
        client.download_file(HDFS_RENAMED_FILE, LOCAL_DOWNLOADED_FILE)
    except Exception as e:
        print(f"测试 5 失败: {str(e)}")

    # 测试 6: 删除文件
    print("\n测试 6: 删除 HDFS 文件")
    try:
        client.delete_file(HDFS_RENAMED_FILE)
    except Exception as e:
        print(f"测试 6 失败: {str(e)}")

    # 测试 7: 获取 YARN 集群信息
    print("\n测试 7: 获取 YARN 集群信息")
    try:
        cluster_info = client.get_cluster_info()
        print(f"集群信息: {cluster_info}")
    except Exception as e:
        print(f"测试 7 失败: {str(e)}")

    # 测试 8: 列出 YARN 应用程序
    print("\n测试 8: 列出 YARN 应用程序")
    try:
        apps = client.list_applications()
        print(f"应用程序列表: {apps}")
    except Exception as e:
        print(f"测试 8 失败: {str(e)}")

    # 清理测试环境
    print("\n清理测试环境...")
    try:
        client.delete_file(HDFS_TEST_DIR, recursive=True)
    except Exception as e:
        print(f"清理失败: {str(e)}")

if __name__ == "__main__":
    run_tests()

