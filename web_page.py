"""语录管理页面（AstrBot 插件页 pages/quotes）的后端接口。

路由前缀与前端 bridge 调用一一对应：
    前端 bridge.apiGet("page/list")  ->  /astrbot_quotes_plugin/page/list
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import io
import time
from typing import Any

from pathlib import Path

from astrbot.api import logger

try:  # AstrBot 4.29+
    from astrbot.api.web import error_response, json_response, request
except Exception:  # pragma: no cover - 兼容更早的插件页机制
    try:
        from quart import jsonify, request

        def json_response(data: Any = None, *, status_code: int = 200, headers=None):
            return jsonify(data or {}), status_code

        def error_response(message: str, *, status_code: int = 400, data=None, headers=None):
            return jsonify({"status": "error", "message": message, "data": data}), status_code

    except Exception:  # pragma: no cover
        json_response = None  # type: ignore[assignment]
        error_response = None  # type: ignore[assignment]

try:
    from .constants import PLUGIN_NAME
except ImportError:  # 插件作为顶层模块加载时
    from constants import PLUGIN_NAME

try:
    from .renderer import STYLE_DEFAULTS, resolve_style
except ImportError:  # 插件作为顶层模块加载时
    from renderer import STYLE_DEFAULTS, resolve_style

PAGE_PREFIX = f"/{PLUGIN_NAME}/page"
# 面板里插件的标识名可能是注册名（quotes），也可能是插件目录名（astrbot_quotes_plugin），
# 两种前缀都注册，页面用相对 endpoint（page/list）调用，哪个命中都行。
PAGE_PREFIXES: tuple[str, ...] = tuple(
    dict.fromkeys((PAGE_PREFIX, f"/{Path(__file__).resolve().parent.name}/page"))
)
THUMB_SIZE = 200
THUMB_CACHE_LIMIT = 600
DEFAULT_PAGE_SIZE = 12
MAX_PAGE_SIZE = 60


class QuotesWebPage:
    """为插件页提供语录数据：概览、分页列表、删除。"""

    def __init__(self, plugin) -> None:
        self.plugin = plugin
        self._thumb_cache: dict[str, str] = {}

    # ------------------------------------------------------------------ 注册
    def register(self) -> None:
        context = getattr(self.plugin, "context", None)
        if context is None:
            logger.warning("[quotes.page] 插件上下文不可用，跳过管理页面接口注册。")
            return
        routes = (
            ("overview", self.api_overview, ["GET"], "语录管理页：概览"),
            ("list", self.api_list, ["GET"], "语录管理页：列表"),
            ("delete", self.api_delete, ["POST"], "语录管理页：删除"),
            ("render", self.api_render, ["POST"], "语录管理页：渲染预览"),
            ("style", self.api_style, ["GET"], "语录管理页：读取卡片样式"),
            ("style_save", self.api_style_save, ["POST"], "语录管理页：保存卡片样式"),
            ("cache_clear", self.api_cache_clear, ["POST"], "语录管理页：清空渲染缓存"),
        )
        for prefix in PAGE_PREFIXES:
            for suffix, handler, methods, desc in routes:
                context.register_web_api(f"{prefix}/{suffix}", handler, methods, desc)
        joined = ", ".join(f"{prefix}/{{...}}" for prefix in PAGE_PREFIXES)
        logger.info(f"[quotes.page] 已注册管理页面接口：{joined}")

    # ------------------------------------------------------------------ 工具
    def _repo(self, pool: str):
        if pool == "nsfw":
            return self.plugin.repository_nsfw
        return self.plugin.repository

    @staticmethod
    def _pool_param() -> str:
        raw = ""
        try:
            raw = str(request.query.get("pool", "normal") or "normal")
        except Exception:
            raw = "normal"
        return "nsfw" if raw.strip().lower() == "nsfw" else "normal"

    @staticmethod
    def _session_label(key: str) -> str:
        if key.startswith("private_"):
            return f"私聊 {key[len('private_'):]}"
        return f"群 {key}"

    @staticmethod
    def _badge(quote) -> str:
        if quote.kind == "forward":
            return "聊天记录"
        if quote.image_ids:
            return f"图{len(quote.image_ids)}"
        if quote.media_ids:
            return f"媒体{len(quote.media_ids)}"
        return "纯文字"

    def _thumb_data_url(self, repo, session_key: str, asset_id: str) -> str:
        cache_key = f"{repo.root}|{session_key}|{asset_id}"
        cached = self._thumb_cache.get(cache_key)
        if cached is not None:
            return cached
        asset = repo.find_asset(session_key, asset_id)
        if asset is None:
            return ""
        path = repo.root / asset.rel_path
        if not path.is_file():
            return ""
        url = ""
        try:
            from PIL import Image

            with Image.open(path) as image:
                image = image.convert("RGB")
                image.thumbnail((THUMB_SIZE, THUMB_SIZE))
                buffer = io.BytesIO()
                image.save(buffer, format="JPEG", quality=78)
            url = "data:image/jpeg;base64," + base64.b64encode(buffer.getvalue()).decode("ascii")
        except Exception as exc:  # noqa: BLE001 - 单张图失败不影响列表
            logger.debug(f"[quotes.page] 缩略图生成失败 {asset.file_name}: {type(exc).__name__}: {exc}")
            return ""
        if len(self._thumb_cache) >= THUMB_CACHE_LIMIT:
            self._thumb_cache.clear()
        self._thumb_cache[cache_key] = url
        return url

    def _collect_rows(self, pool: str, session: str, keyword: str) -> list[tuple[str, int, Any]]:
        repo = self._repo(pool)
        keys = repo.session_keys()
        if session and session != "all":
            keys = [item for item in keys if item == session]
        rows: list[tuple[str, int, Any]] = []
        for key in keys:
            quotes = sorted(repo.list_quotes(key), key=lambda item: (item.created_at, item.id))
            for index, quote in enumerate(quotes, 1):
                haystack = f"{quote.name} {quote.qq} {quote.text}".casefold()
                if keyword and keyword not in haystack:
                    continue
                rows.append((key, index, quote))
        rows.sort(key=lambda item: (item[2].created_at, item[2].id), reverse=True)
        return rows

    def _session_summaries(self, pool: str) -> list[dict[str, Any]]:
        repo = self._repo(pool)
        summaries: list[dict[str, Any]] = []
        for key in repo.session_keys():
            quotes = repo.list_quotes(key)
            if not quotes:
                continue
            summaries.append(
                {
                    "session": key,
                    "label": self._session_label(key),
                    "count": len(quotes),
                }
            )
        summaries.sort(key=lambda item: item["count"], reverse=True)
        return summaries

    # ------------------------------------------------------------------ 接口
    async def api_overview(self):
        pools: dict[str, Any] = {}
        for pool in ("normal", "nsfw"):
            rows = self._collect_rows(pool, "all", "")
            pools[pool] = {
                "total": len(rows),
                "sessions": self._session_summaries(pool),
            }
        return json_response(
            {
                "pools": pools,
                "generated_at": int(time.time()),
            }
        )

    async def api_list(self):
        pool = self._pool_param()
        try:
            session = str(request.query.get("session", "all") or "all").strip()
        except Exception:
            session = "all"
        try:
            keyword = str(request.query.get("query", "") or "").strip().casefold()
        except Exception:
            keyword = ""
        try:
            page = max(1, int(request.query.get("page", 1) or 1))
        except (TypeError, ValueError):
            page = 1
        try:
            page_size = int(request.query.get("page_size", DEFAULT_PAGE_SIZE) or DEFAULT_PAGE_SIZE)
        except (TypeError, ValueError):
            page_size = DEFAULT_PAGE_SIZE
        page_size = max(1, min(page_size, MAX_PAGE_SIZE))

        rows = self._collect_rows(pool, session, keyword)
        total = len(rows)
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = min(page, total_pages)
        start = (page - 1) * page_size
        chunk = rows[start : start + page_size]

        items: list[dict[str, Any]] = []
        for key, index, quote in chunk:
            thumbs: list[str] = []
            for asset_id in quote.image_ids[:4]:
                url = await asyncio.to_thread(self._thumb_data_url, self._repo(pool), key, asset_id)
                if url:
                    thumbs.append(url)
            items.append(
                {
                    "id": quote.id,
                    "session": key,
                    "session_label": self._session_label(key),
                    "index": index,
                    "qq": quote.qq,
                    "name": quote.name or quote.qq,
                    "text": (quote.text or "")[:500],
                    "created_at": quote.created_at,
                    "time_label": time.strftime("%Y-%m-%d %H:%M", time.localtime(quote.created_at)),
                    "kind": quote.kind,
                    "badge": self._badge(quote),
                    "image_count": len(quote.image_ids),
                    "media_count": len(quote.media_ids),
                    "thumbs": thumbs,
                }
            )

        return json_response(
            {
                "pool": pool,
                "session": session,
                "query": keyword,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "items": items,
            }
        )

    async def api_delete(self):
        payload: dict[str, Any] = {}
        try:
            raw = await request.json(default={})
            if isinstance(raw, dict):
                payload = raw
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"[quotes.page] 解析删除请求失败: {type(exc).__name__}: {exc}")
        pool = "nsfw" if str(payload.get("pool", "normal")).strip().lower() == "nsfw" else "normal"
        quote_id = str(payload.get("quote_id", "") or "").strip()
        if not quote_id:
            return error_response("缺少 quote_id")
        repo = self._repo(pool)
        try:
            deleted = await repo.delete_quote(quote_id)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"[quotes.page] 删除语录失败 {quote_id}: {type(exc).__name__}: {exc}")
            return error_response(f"删除失败：{type(exc).__name__}", status_code=500)
        if not deleted:
            return error_response("语录不存在或已被删除", status_code=404)
        logger.info(f"[quotes.page] 已删除语录 pool={pool} id={quote_id}")
        return json_response({"deleted": quote_id, "pool": pool})

    # --------------------------------------------------------------- 样式/渲染
    @staticmethod
    async def _json_body() -> dict[str, Any]:
        try:
            raw = await request.json(default={})
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"[quotes.page] 解析请求体失败: {type(exc).__name__}: {exc}")
            return {}
        return raw if isinstance(raw, dict) else {}

    def _current_style(self) -> dict[str, Any]:
        renderer = getattr(self.plugin, "renderer", None)
        return resolve_style(getattr(renderer, "image_config", {}) or {})

    @staticmethod
    def _quote_image_paths(repo, session_key: str, quote, limit: int = 4) -> list[str]:
        """取这条语录配图的绝对路径，交给渲染器内联进模板。"""
        paths: list[str] = []
        for asset_id in (quote.image_ids or [])[:limit]:
            asset = repo.find_asset(session_key, asset_id)
            if asset is None:
                continue
            abs_path = repo.root / asset.rel_path
            if abs_path.is_file():
                paths.append(str(abs_path))
        return paths

    def _find_quote(self, pool: str, quote_id: str):
        repo = self._repo(pool)
        for key in repo.session_keys():
            for quote in repo.list_quotes(key):
                if quote.id == quote_id:
                    return key, quote
        return None, None

    @staticmethod
    def _to_data_url(source: str, max_width: int = 1000) -> str:
        """把渲染结果（data:/http(s)/file:///本地路径）转成前端可直接展示的 data URL。"""
        text = str(source or "").strip()
        if not text:
            return ""
        raw: bytes | None = None
        if text.startswith("data:"):
            try:
                _, _, payload = text.partition(",")
                raw = base64.b64decode(payload)
            except (binascii.Error, ValueError):
                return text
        elif text.startswith(("http://", "https://")):
            try:
                import urllib.request

                req = urllib.request.Request(text, headers={"User-Agent": "AstrBot-QuotesPage/1.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - 渲染服务返回的图片地址
                    raw = resp.read()
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[quotes.page] 拉取渲染图失败 {text[:120]}: {type(exc).__name__}: {exc}")
                return ""
        else:
            path: Path | None = None
            if text.startswith("file://"):
                from urllib.parse import unquote, urlparse

                parsed = urlparse(text)
                candidate = unquote(parsed.path)
                if len(candidate) > 2 and candidate[0] == "/" and candidate[2] == ":":
                    candidate = candidate[1:]
                path = Path(candidate)
            else:
                path = Path(text)
            if path is not None and path.is_file():
                raw = path.read_bytes()
        if not raw:
            return ""
        try:
            from PIL import Image

            with Image.open(io.BytesIO(raw)) as image:
                if image.width > max_width:
                    ratio = max_width / float(image.width)
                    image = image.resize((max_width, max(1, int(image.height * ratio))))
                if image.mode not in ("RGB", "L"):
                    image = image.convert("RGB")
                buffer = io.BytesIO()
                image.save(buffer, format="JPEG", quality=86)
            raw = buffer.getvalue()
            mime = "image/jpeg"
        except Exception:  # noqa: BLE001 - 转码失败就原样返回
            mime = "image/png"
        return f"data:{mime};base64," + base64.b64encode(raw).decode("ascii")

    def _store_bg_data_url(self, data_url: str) -> str:
        """把前端上传的背景图（data URL）落盘，返回本地绝对路径。"""
        try:
            header, _, payload = data_url.partition(",")
            raw = base64.b64decode(payload)
            from PIL import Image

            with Image.open(io.BytesIO(raw)) as image:
                image = image.convert("RGB")
                if image.width > 2560:
                    ratio = 2560 / float(image.width)
                    image = image.resize((2560, max(1, int(image.height * ratio))))
                target_dir = Path(self.plugin.data_root) / "assets" / "backgrounds"
                target_dir.mkdir(parents=True, exist_ok=True)
                target = target_dir / f"bg_{int(time.time() * 1000)}.jpg"
                image.save(target, format="JPEG", quality=92)
            return str(target)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[quotes.page] 背景图保存失败: {type(exc).__name__}: {exc}")
            return ""

    async def _save_plugin_config(self, image_cfg: dict[str, Any]) -> bool:
        config = getattr(self.plugin, "config", None)
        if config is None:
            return False
        try:
            config["image"] = image_cfg
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[quotes.page] 写入配置失败: {type(exc).__name__}: {exc}")
            return False
        for name in ("save_config", "save", "save_conf"):
            save = getattr(config, name, None)
            if not callable(save):
                continue
            try:
                result = save({"image": image_cfg})
            except TypeError:
                try:
                    result = save()
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"[quotes.page] 保存配置失败({name}): {type(exc).__name__}: {exc}")
                    continue
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[quotes.page] 保存配置失败({name}): {type(exc).__name__}: {exc}")
                continue
            if asyncio.iscoroutine(result) or hasattr(result, "__await__"):
                await result
            return True
        return False

    def _clear_render_cache(self) -> int:
        removed = 0
        for pool in ("normal", "nsfw"):
            repo = self._repo(pool)
            for key in repo.session_keys():
                cache_dir = repo.get_store(key).cache_dir
                if not cache_dir.is_dir():
                    continue
                for item in cache_dir.iterdir():
                    if item.is_file():
                        try:
                            item.unlink()
                            removed += 1
                        except OSError:
                            continue
        return removed

    async def api_style(self):
        return json_response({"style": self._current_style(), "defaults": dict(STYLE_DEFAULTS)})

    async def api_style_save(self):
        payload = await self._json_body()
        raw_style = payload.get("style")
        if not isinstance(raw_style, dict):
            return error_response("缺少 style 参数")
        merged = resolve_style(self._current_style(), raw_style)
        bg = str(merged.get("bg_image") or "")
        if bg.startswith("data:"):
            saved = self._store_bg_data_url(bg)
            merged["bg_image"] = saved
            if not saved:
                return error_response("背景图保存失败，请换一张试试", status_code=500)
        image_cfg = {k: merged[k] for k in STYLE_DEFAULTS}
        ok = await self._save_plugin_config(image_cfg)
        renderer = getattr(self.plugin, "renderer", None)
        if renderer is not None:
            renderer.image_config = dict(image_cfg)
        cleared = self._clear_render_cache()
        logger.info(
            f"[quotes.page] 保存卡片样式 saved={ok} bg={bool(merged.get('bg_image'))} "
            f"font_scale={merged.get('font_scale')} cache_cleared={cleared}"
        )
        return json_response(
            {
                "style": self._current_style(),
                "saved": ok,
                "cache_cleared": cleared,
            }
        )

    async def api_render(self):
        payload = await self._json_body()
        pool = "nsfw" if str(payload.get("pool", "normal")).strip().lower() == "nsfw" else "normal"
        quote_id = str(payload.get("quote_id", "") or "").strip()
        renderer = getattr(self.plugin, "renderer", None)
        if renderer is None:
            return error_response("渲染器不可用", status_code=503)
        style_override = payload.get("style")
        style_override = style_override if isinstance(style_override, dict) else None

        if quote_id:
            session, quote = self._find_quote(pool, quote_id)
            if quote is None:
                return error_response("语录不存在或已被删除", status_code=404)
        else:
            rows = self._collect_rows(pool, "all", "")
            if not rows:
                return error_response("该池还没有语录可以预览", status_code=404)
            session, _index, quote = rows[0]

        signature = quote.name or quote.qq
        repo = self._repo(pool)
        render_images = self._quote_image_paths(repo, session, quote)
        try:
            rendered = await renderer.render_quote_image(
                quote, signature, style_override, return_url=False, images=render_images
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(f"[quotes.page] 渲染失败 {quote.id}: {type(exc).__name__}: {exc}")
            return error_response(f"渲染失败：{type(exc).__name__}", status_code=500)
        image = await asyncio.to_thread(self._to_data_url, rendered)
        if not image:
            logger.warning(f"[quotes.page] 渲染结果无法转成图片: {str(rendered)[:180]}")
            return error_response("渲染结果无法读取", status_code=500)
        logger.info(f"[quotes.page] 渲染预览 pool={pool} id={quote.id} session={session}")
        return json_response(
            {
                "quote_id": quote.id,
                "session": session,
                "name": quote.name or quote.qq,
                "text": (quote.text or "")[:200],
                "image": image,
            }
        )

    async def api_cache_clear(self):
        removed = self._clear_render_cache()
        logger.info(f"[quotes.page] 已清空渲染缓存 {removed} 个文件")
        return json_response({"removed": removed})
