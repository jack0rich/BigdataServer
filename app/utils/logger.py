import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from uuid import uuid4
from datetime import datetime
from fastapi import Request

# 日志格式
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s"

COLORS = {
    "DEBUG": "\033[92m",    # 绿色
    "INFO": "\033[94m",     # 蓝色
    "WARNING": "\033[93m",  # 黄色
    "ERROR": "\033[91m",    # 红色
    "CRITICAL": "\033[95m", # 紫色
    "RESET": "\033[0m",     # 重置颜色
}


class ColorFormatter(logging.Formatter):
    """自定义格式化器，为日志级别添加颜色"""

    def format(self, record):
        color = COLORS.get(record.levelname, COLORS["RESET"])
        reset = COLORS["RESET"]
        record.levelname = f"{color}{record.levelname}{reset}"  # 给 levelname 加颜色
        return super().format(record)


class CustomLogger(logging.Logger):
    def __init__(self, name: str = "app", log_dir: str = "logs"):
        super().__init__(name, logging.DEBUG)

        # 控制台Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ColorFormatter(LOG_FORMAT))  # 使用彩色格式化器
        self.addHandler(console_handler)

        # 文件Handler（每天轮转）
        logs_dir = Path(log_dir)
        logs_dir.mkdir(exist_ok=True)
        file_handler = RotatingFileHandler(
            logs_dir / "app.log",
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=7,
            encoding="utf-8"
        )
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        self.addHandler(file_handler)

    def access_log(self, method: str, path: str, status: int, ip: str, latency: str):
        """记录访问日志"""
        self.info(
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

        self.error(log_msg, exc_info=exception is not None)
        return error_id



# 单例日志实例
logger = logging.getLogger("app")

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