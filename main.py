from __future__ import annotations

from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
import astrbot.api.message_components as Comp

try:
    from astrbot.api import AstrBotConfig  # type: ignore
except Exception:  # pragma: no cover
    AstrBotConfig = dict  # type: ignore

try:
    from .constants import PLUGIN_NAME
    from .image_service import ImageService
    from .models import CommandResponse
    from .napcat_service import NapcatService
    from .quote_service import QuoteService
    from .renderer import QuoteRenderer
    from .store import QuoteRepository
    from .utils import ensure_plugin_data_dir
except ImportError:  # pragma: no cover
    from constants import PLUGIN_NAME
    from image_service import ImageService
    from models import CommandResponse
    from napcat_service import NapcatService
    from quote_service import QuoteService
    from renderer import QuoteRenderer
    from store import QuoteRepository
    from utils import ensure_plugin_data_dir


@register(
    PLUGIN_NAME,
    "Codex",
    "提交语录并生成带头像的语录图片",
    "1.6.0",
    "https://example.com/astrbot-plugin-quotes",
)
class QuotesPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self.http_client = self._create_http_client()
        self.data_root = ensure_plugin_data_dir(str(self.config.get("storage") or "").strip(), PLUGIN_NAME)
        self.repository = QuoteRepository(self.data_root)
        self.napcat_service = NapcatService()
        self.image_service = ImageService(self.http_client)
        self.renderer = QuoteRenderer(self.html_render, self.config.get("image") or {})
        self.quote_service = QuoteService(
            repository=self.repository,
            image_service=self.image_service,
            napcat_service=self.napcat_service,
            renderer=self.renderer,
            http_client=self.http_client,
            global_mode=bool(self.config.get("global_mode", False)),
            text_mode=bool((self.config.get("performance") or {}).get("text_mode", False)),
            render_cache=bool((self.config.get("performance") or {}).get("render_cache", True)),
            image_signature_use_group=bool(self.config.get("image_signature_use_group", False)),
            blacklist=self._parse_blacklist(),
        )
        self._cfg_poke_enabled = bool(self.config.get("poke_enabled", False))
        self._cfg_poke_probability = self._parse_probability(self.config.get("poke_probability", 20))
        self._cfg_poke_group_whitelist = self._parse_id_set(self.config.get("poke_group_whitelist") or [])
        self._cfg_poke_group_blacklist = self._parse_id_set(self.config.get("poke_group_blacklist") or [])
        self._pending_qid: dict[str, str] = {}
        self._last_sent_qid: dict[str, str] = {}

    async def initialize(self):
        await self.repository.migrate_legacy_data()
        await self.renderer.warmup()

    async def terminate(self):
        if self.http_client is not None:
            try:
                await self.http_client.aclose()
            except Exception:
                return

    @filter.command("上传")
    async def add_quote(self, event: AstrMessageEvent, uid: str = ""):
        response = await self.quote_service.add_quote(event, uid=uid)
        for item in self._emit_response(event, response):
            yield item

    @filter.command("语录")
    async def random_quote(self, event: AstrMessageEvent, uid: str = ""):
        response = await self.quote_service.random_quote(event, uid=uid, silent_if_empty=False)
        for item in self._emit_response(event, response):
            yield item

    @filter.command("删除", alias={"删除语录"})
    async def delete_quote(self, event: AstrMessageEvent):
        if not await self._check_delete_permission(event):
            yield event.plain_result("权限不足：你无权使用删除语录指令。")
            return
        if self.quote_service.get_reply_message_id(event) is None:
            yield event.plain_result("请先『回复机器人发送的语录』，再发送 删除。")
            return

        session_key = self._session_key(event)
        quote_id = self._last_sent_qid.get(session_key) or self._pending_qid.get(session_key)
        if not quote_id:
            yield event.plain_result("未能定位语录，请先重新发送一次随机语录再尝试删除。")
            return

        deleted = await self.quote_service.delete_quote(quote_id)
        if deleted:
            yield event.plain_result("已删除语录。")
        else:
            yield event.plain_result("未找到该语录，可能已被删除。")

    @filter.command("语录帮助")
    async def help_quote(self, event: AstrMessageEvent):
        help_text = (
            "语录插件帮助\n"
            "- 上传：先回复某人的消息，再发送“上传”（可附带图片）保存为语录。可在消息中 @某人 指定图片语录归属；不@则默认归属上传者。\n"
            "- 语录：随机发送一条语录；可用“语录 @某人”或“语录 12345678”仅随机该用户的语录；若含用户上传图片，将直接发送原图。\n"
            "- 删除：回复机器人刚发送的随机语录消息，发送“删除”或“删除语录”进行删除。\n"
            "- 存储：插件数据保存在 AstrBot 的 data/plugin_data/quotes/groups/<群号或private_xxx>/ 目录下。\n"
            "- 重复图：同一会话内重复上传相同或高度相似的图片时，会拒绝本次上传并提示“语录图片已存在”。"
        )
        yield event.plain_result(help_text)

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def random_quote_on_poke(self, event: AstrMessageEvent):
        if not self._cfg_poke_enabled:
            return
        if not self._is_poke_allowed_in_group(event.get_group_id()):
            return
        self_id = self._get_self_id(event)
        if not self_id:
            return
        try:
            segments = list(event.get_messages())
        except Exception:
            return

        has_poke_to_bot = False
        for segment in segments:
            try:
                if isinstance(segment, Comp.Poke):
                    target = self._extract_poke_target(segment)
                    if target and str(target) == str(self_id):
                        has_poke_to_bot = True
                        break
            except Exception:
                continue
        if not has_poke_to_bot:
            return

        import secrets

        if self._cfg_poke_probability <= 0:
            return
        if self._cfg_poke_probability < 100 and secrets.randbelow(100) >= self._cfg_poke_probability:
            return

        response = await self.quote_service.random_quote(event, uid="", silent_if_empty=True)
        for item in self._emit_response(event, response):
            yield item

    @filter.after_message_sent()
    async def on_after_message_sent(self, event: AstrMessageEvent):
        try:
            session_key = self._session_key(event)
            quote_id = self._pending_qid.pop(session_key, None)
            if quote_id:
                self._last_sent_qid[session_key] = quote_id
        except Exception as exc:
            logger.info(f"after_message_sent 记录失败: {exc}")

    def _emit_response(self, event: AstrMessageEvent, response: CommandResponse | None):
        if response is None or response.kind == "none":
            return
        if response.quote_id:
            self._pending_qid[self._session_key(event)] = response.quote_id
        if response.kind == "plain":
            yield event.plain_result(response.text)
            return
        if response.kind == "image_path":
            yield event.chain_result([Comp.Image.fromFileSystem(response.path)])
            return
        if response.kind == "image_url":
            yield event.image_result(response.url)

    def _session_key(self, event: AstrMessageEvent) -> str:
        return str(event.get_group_id() or event.unified_msg_origin)

    def _create_http_client(self):
        try:
            import httpx  # type: ignore

            return httpx.AsyncClient(timeout=20)
        except Exception:
            return None

    def _parse_blacklist(self) -> set[str]:
        raw = self.config.get("blacklist")
        items: set[str] = set()
        if isinstance(raw, (list, tuple)):
            for item in raw:
                value = str(item).strip()
                if value.isdigit() and len(value) >= 5:
                    items.add(value)
            return items
        for chunk in str(raw or "").replace("；", ";").replace("，", ",").splitlines():
            for item in chunk.replace(";", ",").split(","):
                value = item.strip()
                if value.isdigit() and len(value) >= 5:
                    items.add(value)
        return items

    def _parse_probability(self, value: Any) -> int:
        try:
            return max(0, min(100, int(value)))
        except (TypeError, ValueError):
            return 20

    def _parse_id_set(self, values: Any) -> set[str]:
        return {str(item).strip() for item in values if str(item).strip()}

    def _is_poke_allowed_in_group(self, group_id: str | None) -> bool:
        if not group_id:
            return True
        gid = str(group_id)
        if self._cfg_poke_group_whitelist:
            return gid in self._cfg_poke_group_whitelist
        if self._cfg_poke_group_blacklist:
            return gid not in self._cfg_poke_group_blacklist
        return True

    def _get_self_id(self, event: AstrMessageEvent) -> str:
        for getter in (
            lambda: getattr(getattr(event, "message_obj", None), "self_id", None),
            lambda: getattr(event, "self_id", None),
            lambda: (getattr(event, "raw_event", None) or {}).get("self_id") if isinstance(getattr(event, "raw_event", None), dict) else None,
        ):
            try:
                value = getter()
            except Exception:
                value = None
            if value:
                return str(value)
        return ""

    def _extract_poke_target(self, segment: Any) -> str | None:
        for field in ("qq", "target", "target_id", "user_id", "uin", "id"):
            try:
                value = getattr(segment, field, None)
            except Exception:
                value = None
            if value:
                return str(value)
        return None

    async def _check_delete_permission(self, event: AstrMessageEvent) -> bool:
        level = str(self.config.get("delete_permission") or "管理员").strip().replace(" ", "")
        if level in {"群员", "member", "普通成员"}:
            return True

        try:
            is_bot_admin = bool(getattr(event, "is_admin", None) and event.is_admin())
        except Exception:
            is_bot_admin = False

        if level in {"Bot管理员", "bot管理员", "BOT管理员", "bot_admin", "BotAdmin"}:
            return is_bot_admin

        group_id = event.get_group_id()
        if not group_id:
            return is_bot_admin

        is_group_owner = False
        is_group_admin = False
        try:
            group = await (event.get_group() if hasattr(event, "get_group") else None)
        except Exception as exc:
            logger.info(f"查询群信息失败: {exc}")
            group = None
        if group is not None:
            sender_id = str(event.get_sender_id())
            owner_id = str(getattr(group, "group_owner", "") or "")
            admin_ids = [str(item) for item in getattr(group, "group_admins", [])]
            is_group_owner = bool(owner_id and sender_id == owner_id)
            is_group_admin = sender_id in admin_ids

        if level in {"管理员", "admin"}:
            return is_group_admin or is_group_owner or is_bot_admin
        if level in {"群主", "owner"}:
            return is_group_owner or is_bot_admin
        return is_bot_admin
