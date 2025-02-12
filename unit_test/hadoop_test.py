import asyncio
import pytest
from app.services import HadoopAPIClient

@pytest.mark.asyncio()
async def test_hadoop_api():
    async with HadoopAPIClient() as client:
        test_dir = "/user/hadoop/test_dir"
        test_file = f"{test_dir}/test_file.txt"
        renamed_file = f"{test_dir}/renamed_file.txt"
        file_content = b"Hello, Hadoop!"

        # 1. 创建目录
        await client.mkdir(test_dir)
        files = await client.list_dir("/user/hadoop")
        assert any(f["path"] == test_dir for f in files), "目录创建失败"

        # 2. 上传文件
        await client.upload_file(test_file, file_content)
        file_status = await client.get_file_status(test_file)
        assert file_status["file_size"] == len(file_content), "文件上传失败"

        # 3. 下载文件
        downloaded_content = await client.download_file(test_file)
        assert downloaded_content == file_content, "文件下载失败"

        # 4. 重命名文件
        await client.rename_path(test_file, renamed_file)
        renamed_status = await client.get_file_status(renamed_file)
        assert renamed_status, "文件重命名失败"

        # 5. 删除文件
        await client.delete_path(renamed_file)
        try:
            await client.get_file_status(renamed_file)
            assert False, "文件未成功删除"
        except client.HDFSNotFoundError:
            pass  # 预期行为

        # 6. 删除目录
        await client.delete_path(test_dir, recursive=True)
        try:
            await client.get_file_status(test_dir)
            assert False, "目录未成功删除"
        except client.HDFSNotFoundError:
            pass  # 预期行为

if __name__ == "__main__":
    asyncio.run(test_hadoop_api())
