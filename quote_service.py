from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    from astrbot.api import logger
    import astrbot.api.message_components as Comp
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)
    Comp = None  # type: ignore

try:
    from .models import CommandResponse, Quote
    from .napcat_service import NapcatService
    from .renderer import QuoteRenderer
    from .store import QuoteRepository
    from .utils import is_valid_qq, make_session_key, normalize_quote_text, random_id
    from .constants import DUPLICATE_IMAGE_MESSAGE
except ImportError:  # pragma: no cover
    from models import CommandResponse, Quote
    from napcat_service import NapcatService
    from renderer import QuoteRenderer
    from store import QuoteRepository
    from utils import is_valid_qq, make_session_key, normalize_quote_text, random_id
    from constants import DUPLICATE_IMAGE_MESSAGE


class QuoteService:
    def __init__(
        self,
        repository: QuoteRepository,
        image_service: Any,
        napcat_service: NapcatService,
        renderer: QuoteRenderer,
        http_client: Any | None,
        *,
        global_mode: bool,
        text_mode: bool,
        render_cache: bool,
        image_signature_use_group: bool,
        blacklist: set[str],
    ):
        self.repository = repository
        self.image_service = image_service
        self.napcat_service = napcat_service
        self.renderer = renderer
        self.http_client = http_client
        self.global_mode = global_mode
        self.text_mode = text_mode
        self.render_cache = render_cache
        self.image_signature_use_group = image_signature_use_group
        self.blacklist = blacklist

    async def add_quote(self, event: Any, uid: str = "") -> CommandResponse:
        session_key = make_session_key(event.get_group_id(), event.get_sender_id())
        reply_message_id = self.get_reply_message_id(event)
        reply_payload = await self.napcat_service.fetch_onebot_message(event, reply_message_id)
        reply_sender = reply_payload.get("sender") or {}
        reply_qq = str(reply_sender.get("user_id") or reply_sender.get("qq") or "")
        explicit_qq = uid.strip() if is_valid_qq(uid) else ""

        target_text = self.extract_plaintext_from_onebot_message(reply_payload.get("message"))
        if not target_text:
            target_text = self.extract_plaintext_from_segments(event, command_name="上传")
        if explicit_qq and target_text.startswith(explicit_qq):
            target_text = target_text[len(explicit_qq) :].strip()
        target_text = normalize_quote_text(target_text)

        images = await self.image_service.collect_images(event, reply_payload.get("message"))
        if not target_text and not images.all_images:
            return CommandResponse(kind="plain", text="未获取到被回复消息内容或图片，请确认已正确回复对方的消息或附带图片。")
        if not target_text and images.all_images:
            target_text = "[图片]"

        mention_qq = self.extract_at_qq(event) or ""
        if explicit_qq:
            target_qq = explicit_qq
        elif mention_qq:
            target_qq = mention_qq
        elif reply_qq:
            target_qq = reply_qq
        elif images.current_images:
            target_qq = str(event.get_sender_id())
        else:
            target_qq = ""

        if target_qq and target_qq in self.blacklist:
            return CommandResponse(kind="plain", text="该用户在语录黑名单中，本次语录已忽略。")

        target_name = await self.napcat_service.resolve_user_name(event, target_qq) if target_qq else ""
        if not target_name:
            target_name = target_qq or "未知用户"

        from time import time

        quote = Quote(
            id=random_id("q_"),
            qq=str(target_qq or ""),
            name=str(target_name),
            text=str(target_text),
            created_by=str(event.get_sender_id()),
            created_at=time(),
            group=session_key,
        )
        result = await self.repository.create_quote_with_images(session_key, quote, images.all_images)
        if result.duplicate:
            return CommandResponse(kind="plain", text=result.message or DUPLICATE_IMAGE_MESSAGE)

        image_count = len(images.all_images)
        if image_count:
            return CommandResponse(kind="plain", text=f"已收录 {quote.name} 的语录，并保存 {image_count} 张图片。")
        return CommandResponse(kind="plain", text=f"已收录 {quote.name} 的语录：{target_text}")

    async def random_quote(self, event: Any, uid: str = "", silent_if_empty: bool = False) -> CommandResponse | None:
        session_key = make_session_key(event.get_group_id(), event.get_sender_id())
        target_session = None if self.global_mode else session_key
        explicit_qq = uid.strip() if is_valid_qq(uid) else ""
        only_qq = explicit_qq or (self.extract_at_qq(event) or "")
        quote = await self.repository.random_quote(target_session, qq=only_qq or None)
        if quote is None:
            if silent_if_empty:
                return None
            if only_qq:
                text = "这个用户还没有语录哦~" if self.global_mode else "这个用户在本会话还没有语录哦~"
            else:
                text = "还没有语录，先用 上传 保存一条吧~" if self.global_mode else "本会话还没有语录，先用 上传 保存一条吧~"
            return CommandResponse(kind="plain", text=text)

        if quote.image_ids:
            image_path = self.resolve_random_image_path(quote)
            if image_path:
                return CommandResponse(kind="image_path", path=image_path, quote_id=quote.id)

        if self.text_mode:
            return CommandResponse(kind="plain", text=f"「{quote.text}」 — {quote.name}", quote_id=quote.id)

        store = self.repository.get_store(quote.group)
        cache_path = store.cache_path(quote.id)
        if self.render_cache and cache_path.exists():
            return CommandResponse(kind="image_path", path=str(cache_path), quote_id=quote.id)

        signature = await self.napcat_service.resolve_signature_name(
            event,
            quote,
            use_group_signature=self.image_signature_use_group,
        )
        rendered_url = await self.renderer.render_quote_image(quote, signature)
        cached = await self.cache_rendered_result(rendered_url, cache_path)
        if cached:
            return CommandResponse(kind="image_path", path=str(cache_path), quote_id=quote.id)
        return CommandResponse(kind="image_url", url=rendered_url, quote_id=quote.id)

    async def delete_quote(self, quote_id: str) -> bool:
        return await self.repository.delete_quote(quote_id)

    def resolve_random_image_path(self, quote: Quote) -> str:
        import secrets

        assets = [self.repository.find_asset(quote.group, image_id) for image_id in quote.image_ids]
        assets = [item for item in assets if item is not None]
        if not assets:
            return ""
        asset = secrets.choice(assets)
        abs_path = self.repository.root / asset.rel_path
        if abs_path.exists():
            return str(abs_path)
        return ""

    async def cache_rendered_result(self, rendered_url: str, cache_path: Path) -> bool:
        if not self.render_cache:
            return False
        try:
            if rendered_url.startswith("file://"):
                from urllib.parse import unquote, urlparse

                parsed = urlparse(rendered_url)
                local_path = Path(unquote(parsed.path))
                if local_path.exists():
                    cache_path.write_bytes(local_path.read_bytes())
                    return True
            elif rendered_url.startswith("http") and self.http_client is not None:
                response = await self.http_client.get(rendered_url)
                if getattr(response, "status_code", 200) < 400:
                    cache_path.write_bytes(bytes(response.content))
                    return True
        except Exception as exc:
            logger.info(f"渲染缓存落盘失败: {exc}")
        return False

    def extract_at_qq(self, event: Any) -> str | None:
        try:
            for segment in event.get_messages():
                if Comp is not None and isinstance(segment, Comp.At):
                    for field in ("qq", "target", "uin", "user_id", "id"):
                        value = getattr(segment, field, None)
                        if value:
                            return str(value)
        except Exception as exc:
            logger.warning(f"解析 @ 失败: {exc}")
        return None

    def get_reply_message_id(self, event: Any) -> str | None:
        try:
            for segment in event.get_messages():
                if Comp is not None and isinstance(segment, Comp.Reply):
                    value = (
                        getattr(segment, "message_id", None)
                        or getattr(segment, "id", None)
                        or getattr(segment, "reply", None)
                        or getattr(segment, "msgId", None)
                    )
                    if value:
                        return str(value)
        except Exception as exc:
            logger.warning(f"解析 Reply 段失败: {exc}")
        return None

    def extract_plaintext_from_onebot_message(self, message: Any) -> str:
        if not isinstance(message, list):
            return ""
        parts: list[str] = []
        for segment in message:
            if (segment.get("type") or "").lower() in {"text", "plain"}:
                parts.append(str((segment.get("data") or {}).get("text") or ""))
        return normalize_quote_text("".join(parts).strip())

    def extract_plaintext_from_segments(self, event: Any, command_name: str = "") -> str:
        parts: list[str] = []
        try:
            for segment in event.get_messages():
                if Comp is not None and isinstance(segment, Comp.Plain):
                    parts.append(str(getattr(segment, "text", "") or ""))
        except Exception:
            raw_text = str(getattr(event, "message_str", "") or "")
            parts = [raw_text]
        text = "".join(parts).strip()
        if command_name and text.startswith(command_name):
            text = text[len(command_name) :].strip()
        return normalize_quote_text(text)
