import asyncio
import pytest
from app.services import MLflowAPIClient  # 替换为你的实际模块路径

pytestmark = pytest.mark.asyncio

async def test_mlflow_client():
    client = MLflowAPIClient()

    try:
        # 🚀 **测试 1：创建实验**
        experiment_name = "test_experiment_10"
        print("\n🔹 测试 1：创建实验")
        experiment_response = await client.create_experiment(experiment_name)
        experiment_id = experiment_response["experiment_id"]
        print(f"✅ 创建实验成功: experiment_id={experiment_id}")

        # 🚀 **测试 2：创建运行**
        print("\n🔹 测试 2：创建运行")
        run_response = await client.create_run(experiment_id)
        run_id = run_response["run"]["info"]["run_id"]
        print(f"✅ 创建运行成功: run_id={run_id}")

        # 🚀 **测试 3：注册模型**
        model_name = "test_model_7"
        print("\n🔹 测试 3：注册模型")
        register_response = await client.register_model(run_id, model_name)
        print("✅ 注册模型成功:", register_response)

        # 🚀 **测试 4：获取模型版本**
        version = register_response["model_version"]["version"]
        print("\n🔹 测试 4：获取模型版本")
        model_version_info = await client.get_model_version(model_name, version)
        print("✅ 获取模型版本信息:", model_version_info)

        # 🚀 **测试 5：获取所有模型版本**
        print("\n🔹 测试 5：获取所有模型版本")
        model_versions = await client.get_model_versions(model_name)
        print("✅ 获取所有模型版本:", model_versions)

        # 🚀 **测试 6：转换模型阶段**
        print("\n🔹 测试 6：转换模型阶段")
        transition_response = await client.transition_model_stage(model_name, version, "Production")
        print("✅ 模型版本转换成功:", transition_response)

        # 🚀 **测试 7：删除模型版本**
        print("\n🔹 测试 7：删除模型版本")
        delete_version_response = await client.delete_model_version(model_name, version)
        print("✅ 删除模型版本成功:", delete_version_response)

        # 🚀 **测试 8：删除整个模型**
        print("\n🔹 测试 8：删除整个模型")
        delete_model_response = await client.delete_model(model_name)
        print("✅ 删除模型成功:", delete_model_response)

    except Exception as e:
        print(f"❌ 测试失败: {e}")

    finally:
        await client.close()

if __name__ == '__main__':
    asyncio.run(test_mlflow_client())
