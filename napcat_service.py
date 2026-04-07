from __future__ import annotations

from typing import Any

try:
    from astrbot.api import logger
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)

try:
    from .models import Quote
except ImportError:  # pragma: no cover
    from models import Quote


class NapcatService:
    async def fetch_onebot_message(self, event: Any, message_id: str | None) -> dict[str, Any]:
        if not message_id or event.get_platform_name() != "aiocqhttp":
            return {}
        try:
            client = event.bot
            response = await client.api.call_action("get_msg", message_id=int(str(message_id)))
            return response or {}
        except Exception as exc:
            logger.info(f"get_msg 失败: {exc}")
            return {}

    async def resolve_user_name(self, event: Any, qq: str) -> str:
        if not qq:
            return ""
        if event.get_platform_name() != "aiocqhttp":
            return qq
        try:
            client = event.bot
            group_id = event.get_group_id()
            if group_id:
                response = await client.api.call_action(
                    "get_group_member_info",
                    group_id=int(group_id),
                    user_id=int(qq),
                    no_cache=True,
                )
                card = str(response.get("card") or "").strip()
                nickname = str(response.get("nickname") or "").strip()
                if card or nickname:
                    return card or nickname
            response = await client.api.call_action(
                "get_stranger_info",
                user_id=int(qq),
                no_cache=True,
            )
            nickname = str(response.get("nickname") or "").strip()
            if nickname:
                return nickname
        except Exception as exc:
            logger.info(f"读取 Napcat 用户信息失败，回退：{exc}")
        return qq

    async def resolve_signature_name(self, event: Any, quote: Quote, use_group_signature: bool) -> str:
        if not use_group_signature:
            return quote.name
        if event.get_platform_name() != "aiocqhttp":
            return quote.name
        group_id = str(quote.group or "").strip()
        qq = str(quote.qq or "").strip()
        if not (group_id.isdigit() and qq.isdigit()):
            return quote.name
        try:
            client = event.bot
            response = await client.api.call_action(
                "get_group_member_info",
                group_id=int(group_id),
                user_id=int(qq),
                no_cache=True,
            )
            card = str(response.get("card") or "").strip()
            nickname = str(response.get("nickname") or "").strip()
            return card or nickname or quote.name
        except Exception as exc:
            logger.info(f"读取 Napcat 群名片失败，回退：{exc}")
            return quote.name
