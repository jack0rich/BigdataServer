import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from uuid import uuid4
from datetime import datetime
from fastapi import Request

# 日志格式
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s"


class CustomLogger:
    def __init__(self):
        self.logger = logging.getLogger("app")
        self.logger.setLevel(logging.DEBUG)

        # 控制台Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

        # 文件Handler（每天轮转）
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        file_handler = RotatingFileHandler(
            logs_dir / "app.log",
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=7,
            encoding="utf-8"
        )
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def setup_logging(self):
        """初始化日志配置"""
        pass  # 已在__init__中完成

    def access_log(self, method: str, path: str, status: int, ip: str, latency: str):
        """记录访问日志"""
        self.logger.info(
            f"ACCESS: {method} {path} - {status} | "
            f"IP: {ip} | Latency: {latency}"
        )

    def error_log(self, message: str, method: str = None, path: str = None,
                  ip: str = None, exception: Exception = None) -> str:
        """记录错误日志并返回错误ID"""
        error_id = str(uuid4())
        log_msg = (
            f"ERROR_ID: {error_id} | "
            f"Method: {method} | Path: {path} | "
            f"IP: {ip} | Message: {message}"
        )
        if exception:
            log_msg += f" | Exception: {str(exception)}"

        self.logger.error(log_msg, exc_info=exception is not None)
        return error_id


# 单例日志实例
logger = CustomLogger()

# FastAPI中间件使用的日志配置
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": LOG_FORMAT}
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard"
        }
    },
    "loggers": {
        "uvicorn.error": {"level": "INFO"},
        "uvicorn.access": {"level": "INFO", "propagate": False}
    }
}