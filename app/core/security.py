# app/core/security.py
from typing import Annotated
from fastapi import Depends, HTTPException, status, Security
from fastapi.security import APIKeyHeader
from app.core.config import settings
from app.utils.logger import logger

# ----------------------------
# API 密钥认证配置
# ----------------------------
api_key_header = APIKeyHeader(
    name=settings.API_KEY_HEADER,
    description="API密钥认证方式",
    auto_error=False
)


class APIKeyValidator:
    """增强型API密钥验证器"""

    def __init__(self):
        self.cache = {}  # 简单缓存验证结果（生产环境建议使用Redis）

    async def validate_key(self, api_key: str) -> dict:
        """验证API密钥并返回权限信息"""
        # 优先检查缓存
        if api_key in self.cache:
            return self.cache[api_key]

        # 验证密钥有效性（示例逻辑，实际应从数据库查询）
        key_info = await self._get_key_from_source(api_key)

        if not key_info:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="无效API密钥"
            )

        # 缓存有效结果（缓存5分钟）
        self.cache[api_key] = key_info
        return key_info

    async def _get_key_from_source(self, api_key: str) -> dict:
        """从数据源获取密钥信息（示例实现）"""
        # 示例硬编码密钥，实际应从数据库或密钥管理服务获取
        valid_keys = {
            "prod-key-123": {
                "name": "生产环境密钥",
                "permissions": ["read", "write"],
                "rate_limit": 1000
            },
            "monitor-key-456": {
                "name": "监控密钥",
                "permissions": ["read"],
                "rate_limit": 100
            }
        }

        return valid_keys.get(api_key)


# 初始化验证器
api_key_validator = APIKeyValidator()


# ----------------------------
# 认证依赖项
# ----------------------------
async def api_key_auth(
        api_key: Annotated[str, Security(api_key_header)]
) -> dict:
    """统一API密钥认证依赖"""
    if not api_key:
        logger.warning("尝试访问未提供API密钥")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="缺少API密钥"
        )

    try:
        key_info = await api_key_validator.validate_key(api_key)
        logger.info(f"API密钥验证成功: {key_info['name']}")
        return key_info
    except HTTPException as e:
        logger.warning(f"无效API密钥尝试: {api_key}")
        raise e


# ----------------------------
# 权限验证装饰器
# ----------------------------
def require_permission(required_permission: str):
    """权限验证装饰器工厂"""

    def dependency(key_info: dict = Depends(api_key_auth)) -> dict:
        if required_permission not in key_info.get("permissions", []):
            logger.warning(
                f"密钥 {key_info['name']} 尝试访问需要 {required_permission} 权限的资源"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足"
            )
        return key_info

    return Depends(dependency)


# ----------------------------
# 速率限制装饰器（示例）
# ----------------------------
def rate_limited(key_info: dict = Depends(api_key_auth)):
    """示例速率限制实现"""
    # 实际应集成Redis等分布式计数器
    rate_limit = key_info.get("rate_limit", 10)
    # TODO: 实现实际速率限制逻辑
    return key_info