# app/main.py
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

# 应用模块
from app.core.config import settings
from app.core.security import api_key_auth
from app.api.endpoints import (
    hadoop_router,
    mlflow_router,
    airflow_router,
    # system_router
)
from app.utils import logger
from app.utils.docker_client import docker_client


# ----------------------------
# 应用生命周期管理
# ----------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """管理应用启动/关闭事件"""
    # 启动初始化
    logger.setup_logging()
    logging.info(f"🚀 Starting Cluster Proxy Server (env: {settings.ENV})")

    try:
        await docker_client.initialize()
        logging.info("✅ Docker client initialized")
    except Exception as e:
        logging.critical(f"❌ Docker client initialization failed: {str(e)}")
        raise

    # 启动后操作
    Instrumentator().instrument(app).expose(app)
    logging.info("📊 Prometheus metrics enabled")

    yield  # 应用运行阶段

    # 关闭清理
    await docker_client.close()
    logging.info("🛑 Docker client closed")


# ----------------------------
# FastAPI 应用实例
# ----------------------------
app = FastAPI(
    title="Cluster Management Gateway",
    description="统一代理管理 Hadoop/MLflow/Airflow 集群的中转服务",
    version=settings.APP_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url=None,
    dependencies=[Depends(api_key_auth)]  # 全局API密钥验证
)

# ----------------------------
# 中间件配置
# ----------------------------
# CORS 跨域
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GZIP 压缩
app.add_middleware(
    GZipMiddleware,
    minimum_size=1024,
)


# 请求日志
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """记录所有请求的中间件"""
    start_time = datetime.now()
    client_ip = request.client.host if request.client else "unknown"

    # 跳过健康检查端点
    if request.url.path == "/health":
        return await call_next(request)

    try:
        response = await call_next(request)
        process_time = (datetime.now() - start_time).total_seconds() * 1000

        logger.access_log(
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            ip=client_ip,
            latency=f"{process_time:.2f}ms"
        )

        return response

    except Exception as e:
        logger.error_log(
            message=f"Request processing failed: {str(e)}",
            method=request.method,
            path=request.url.path,
            ip=client_ip
        )
        raise


# ----------------------------
# 全局异常处理
# ----------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """统一异常响应格式"""
    error_id = logger.error_log(
        message=f"Unhandled exception: {str(exc)}",
        method=request.method,
        path=request.url.path,
        exception=exc
    )

    return JSONResponse(
        status_code=500,
        content={
            "error": "INTERNAL_ERROR",
            "detail": "Server internal error",
            "error_id": error_id,
            "documentation": settings.DOCS_URL
        }
    )


# ----------------------------
# 路由挂载
# ----------------------------
# 核心服务代理
app.include_router(hadoop_router)
app.include_router(mlflow_router)
app.include_router(airflow_router)

# 系统管理
# app.include_router(system_router)


# ----------------------------
# 基础健康检查
# ----------------------------
@app.get("/health", include_in_schema=False)
async def health_check():
    """服务健康检查端点"""
    return {
        "status": "healthy",
        "version": settings.APP_VERSION,
        "timestamp": datetime.utcnow().isoformat()
    }


# ----------------------------
# 开发模式配置
# ----------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_config=logger.LOGGING_CONFIG,
        access_log=False  # 使用自定义访问日志
    )