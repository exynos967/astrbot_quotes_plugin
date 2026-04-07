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
    from .models import ImageCollection, PreparedImage
    from .utils import prepare_image
except ImportError:  # pragma: no cover
    from models import ImageCollection, PreparedImage
    from utils import prepare_image


class ImageService:
    def __init__(self, http_client: Any | None = None):
        self.http_client = http_client

    async def collect_images(
        self,
        event: Any,
        reply_message: Any,
    ) -> ImageCollection:
        reply_images = await self._collect_from_onebot_message(event, reply_message)
        current_images = await self._collect_from_segments(event)
        return ImageCollection(reply_images=reply_images, current_images=current_images)

    async def _collect_from_onebot_message(self, event: Any, message: Any) -> list[PreparedImage]:
        if not isinstance(message, list):
            return []
        images: list[PreparedImage] = []
        for segment in message:
            try:
                if (segment.get("type") or "").lower() != "image":
                    continue
                data = segment.get("data") or {}
                prepared = await self._prepare_from_onebot_data(event, data)
                if prepared is not None:
                    images.append(prepared)
            except Exception as exc:
                logger.warning(f"处理回复图片失败: {exc}")
        return images

    async def _collect_from_segments(self, event: Any) -> list[PreparedImage]:
        images: list[PreparedImage] = []
        try:
            segments = list(event.get_messages())
        except Exception as exc:
            logger.warning(f"读取消息链失败: {exc}")
            return images

        for segment in segments:
            try:
                if Comp is None or not isinstance(segment, Comp.Image):
                    continue
                url = getattr(segment, "url", None)
                file_or_path = getattr(segment, "file", None) or getattr(segment, "path", None)
                prepared = None
                if url and str(url).startswith(("http://", "https://")):
                    prepared = await self._prepare_from_url(str(url))
                elif file_or_path:
                    prepared = await self._prepare_from_fs(str(file_or_path))
                    if prepared is None and event.get_platform_name() == "aiocqhttp":
                        local_path = await self._resolve_napcat_image_path(event, str(file_or_path))
                        if local_path:
                            prepared = await self._prepare_from_fs(local_path)
                if prepared is not None:
                    images.append(prepared)
            except Exception as exc:
                logger.warning(f"处理当前消息图片失败: {exc}")
        return images

    async def _prepare_from_onebot_data(self, event: Any, data: dict[str, Any]) -> PreparedImage | None:
        url = data.get("url") or data.get("image_url")
        if url and str(url).startswith(("http://", "https://")):
            prepared = await self._prepare_from_url(str(url))
            if prepared is not None:
                return prepared

        file_or_path = data.get("file") or data.get("path")
        if not file_or_path:
            return None

        prepared = await self._prepare_from_fs(str(file_or_path))
        if prepared is not None:
            return prepared

        local_path = await self._resolve_napcat_image_path(event, str(file_or_path))
        if local_path:
            return await self._prepare_from_fs(local_path)
        return None

    async def _prepare_from_url(self, url: str) -> PreparedImage | None:
        try:
            if self.http_client is not None:
                response = await self.http_client.get(url)
            else:
                import httpx

                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.get(url)
            if getattr(response, "status_code", 200) >= 400:
                return None
            content = bytes(response.content)
            content_type = (response.headers.get("Content-Type") or "") if getattr(response, "headers", None) else ""
            return prepare_image(content, source=url, content_type=content_type)
        except Exception as exc:
            logger.warning(f"下载图片失败: {exc}")
            return None

    async def _prepare_from_fs(self, path: str) -> PreparedImage | None:
        try:
            file_path = Path(path)
            if not file_path.exists():
                return None
            return prepare_image(file_path.read_bytes(), source=file_path.name)
        except Exception as exc:
            logger.warning(f"读取本地图片失败: {exc}")
            return None

    async def _resolve_napcat_image_path(self, event: Any, file_id: str) -> str | None:
        if event.get_platform_name() != "aiocqhttp":
            return None
        try:
            client = event.bot
            response = await client.api.call_action("get_image", file=str(file_id))
            return str(response.get("file") or response.get("path") or response.get("file_path") or "") or None
        except Exception as exc:
            logger.info(f"get_image 回退失败: {exc}")
            return None
