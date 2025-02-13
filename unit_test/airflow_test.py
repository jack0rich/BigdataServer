import asyncio
import pytest
from app.services import AirflowAPIClient


@pytest.mark.asyncio
async def test_airflow_api():
    async with AirflowAPIClient() as client:
        dag_id = "test_dag"
        dag_conf = {"param1": "value1"}

        # 1. 创建 DAG
        await client.create_dag(dag_id, "0 12 * * *", ["test_task"])
        dags = await client.list_dags()
        assert any(d["dag_id"] == dag_id for d in dags["dags"]), "DAG 创建失败"

        # 2. 触发 DAG 运行
        dag_run = await client.trigger_dag(dag_id, conf=dag_conf)
        assert dag_run["dag_run_id"], "DAG 触发失败"

        # 3. 查询 DAG 运行状态
        dag_run_status = await client.get_dag_run(dag_id, dag_run["dag_run_id"])
        assert dag_run_status["dag_run_id"] == dag_run["dag_run_id"], "DAG 运行状态查询失败"

        # 4. 列出 DAG 运行
        dag_runs = await client.list_dag_runs(dag_id)
        assert any(run["dag_run_id"] == dag_run["dag_run_id"] for run in dag_runs["dag_runs"]), "DAG 运行未找到"

        # 5. 暂停 DAG
        await client.update_dag_state(dag_id, is_paused=True)
        dag_info = await client.get_dag(dag_id)
        assert dag_info["is_paused"] is True, "DAG 暂停失败"

        # 6. 激活 DAG
        await client.update_dag_state(dag_id, is_paused=False)
        dag_info = await client.get_dag(dag_id)
        assert dag_info["is_paused"] is False, "DAG 激活失败"

        # 7. 删除 DAG 运行
        await client.delete_dag_run(dag_id, dag_run["dag_run_id"])
        try:
            await client.get_dag_run(dag_id, dag_run["dag_run_id"])
            assert False, "DAG 运行未成功删除"
        except client.DagNotFoundError:
            pass  # 预期行为

        # 8. 删除 DAG
        await client.delete_dag(dag_id)
        try:
            await client.get_dag(dag_id)
            assert False, "DAG 未成功删除"
        except client.DagNotFoundError:
            pass  # 预期行为


if __name__ == "__main__":
    asyncio.run(test_airflow_api())
