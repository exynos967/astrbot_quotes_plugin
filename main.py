from __future__ import annotations

import asyncio
import json
import random
import re
import html
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
import astrbot.api.message_components as Comp

try:
    # 可用时类型提示
    from astrbot.api import AstrBotConfig  # type: ignore
except Exception:  # pragma: no cover
    AstrBotConfig = dict  # type: ignore


PLUGIN_NAME = "quotes"


@dataclass
class Quote:
    id: str
    qq: str
    name: str
    text: str
    created_by: str
    created_at: float
    images: List[str] = field(default_factory=list)  # 相对 data 目录的路径，如 "quotes/images/xxx.jpg"


class QuoteStore:
    """简易 JSON 存储，保存于插件专属数据目录（data/plugin_data/quotes/）。
    - 遵循 AstrBot 插件规范：通过 StarTools.get_data_dir 获取根目录
    - 通过 asyncio.Lock 简单串行化写入，避免并发写入冲突
    """

    def __init__(self, root_data_dir: Path, http_client: Optional[Any] = None):
        self.root = Path(root_data_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.images_dir = self.root / "images"
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.avatars_dir = self.root / "avatars"
        self.avatars_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir = self.root / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.file = self.root / "quotes.json"
        self._http = http_client
        self._lock = asyncio.Lock()
        if not self.file.exists():
            self._write({"quotes": []})
        # 内存缓存
        try:
            self._quotes: List[Dict[str, Any]] = self._read().get("quotes", [])
        except Exception:
            self._quotes = []

    def images_rel(self, filename: str, group_key: Optional[str] = None) -> str:
        """返回相对插件数据根目录的路径，按群分目录：images/<group_key>/<filename>。"""
        if group_key:
            return f"images/{group_key}/{filename}"
        return f"images/{filename}"

    def images_abs(self, filename: str, group_key: Optional[str] = None) -> Path:
        base = self.images_dir / group_key if group_key else self.images_dir
        base.mkdir(parents=True, exist_ok=True)
        return base / filename

    async def save_image_from_url(self, url: str, group_key: Optional[str] = None) -> Optional[str]:
        """下载网络图片并保存，返回相对 data 的路径。按群分目录。"""
        try:
            if self._http is None:
                # 延迟创建，保证在未注入 client 时也可工作
                import httpx
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.get(url)
                    content = resp.content
                    ct = (resp.headers.get("Content-Type") or "").lower()
            else:
                resp = await self._http.get(url)
                content = resp.content
                ct = (resp.headers.get("Content-Type") or "").lower()
            # 猜扩展名
            ext = ".jpg"
            if "png" in ct:
                ext = ".png"
            elif "webp" in ct:
                ext = ".webp"
            elif "gif" in ct:
                ext = ".gif"
            # 从 url 推断
            from urllib.parse import urlparse
            p = urlparse(url)
            name_guess = Path(p.path).name
            if "." in name_guess and len(Path(name_guess).suffix) <= 5:
                ext = Path(name_guess).suffix
            from time import time
            filename = f"{int(time()*1000)}_{random.randint(1000,9999)}{ext}"
            abs_path = self.images_abs(filename, group_key)
            abs_path.write_bytes(content)
            return self.images_rel(filename, group_key)
        except Exception as e:
            logger.warning(f"下载图片失败: {e}")
            return None

    async def save_image_from_fs(self, src: str, group_key: Optional[str] = None) -> Optional[str]:
        """从本地文件复制，返回相对 data 的路径（按群分目录）。"""
        try:
            sp = Path(src)
            if not sp.exists():
                return None
            ext = sp.suffix or ".jpg"
            from time import time
            filename = f"{int(time()*1000)}_{random.randint(1000,9999)}{ext}"
            dp = self.images_abs(filename, group_key)
            data = sp.read_bytes()
            dp.write_bytes(data)
            return self.images_rel(filename, group_key)
        except Exception as e:
            logger.warning(f"保存本地图片失败: {e}")
            return None

    def _read(self) -> Dict[str, Any]:
        if not self.file.exists():
            return {"quotes": []}
        try:
            return json.loads(self.file.read_text(encoding="utf-8"))
        except Exception:
            logger.error("quotes.json 解析失败，已回退为空列表。")
            return {"quotes": []}

    def _write(self, data: Dict[str, Any]):
        self.file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    async def add(self, q: Quote):
        async with self._lock:
            self._quotes.append(asdict(q))
            self._write({"quotes": self._quotes})

    async def random_one(self) -> Optional[Quote]:
        arr = self._quotes
        if not arr:
            return None
        obj = random.choice(arr)
        return Quote(**obj)

    async def random_one_by_qq(self, qq: str) -> Optional[Quote]:
        arr = [x for x in self._quotes if str(x.get("qq") or "") == str(qq)]
        if not arr:
            return None
        obj = random.choice(arr)
        return Quote(**obj)

    async def delete_by_id(self, qid: str) -> bool:
        async with self._lock:
            old_len = len(self._quotes)
            self._quotes = [x for x in self._quotes if str(x.get("id")) != str(qid)]
            if len(self._quotes) == old_len:
                return False
            self._write({"quotes": self._quotes})
            return True

    async def get_avatar_uri(self, qq: str, enable_cache: bool = True) -> str:
        """返回头像的 data:URI（若已缓存或成功下载）；否则返回 qlogo 远程 URL。"""
        size = 640
        remote = f"https://q1.qlogo.cn/g?b=qq&nk={qq}&s={size}"
        if not enable_cache:
            return remote
        try:
            p = self.avatars_dir / f"{qq}.png"
            if p.exists():
                data = p.read_bytes()
                b64 = base64.b64encode(data).decode("ascii")
                return f"data:image/png;base64,{b64}"
            # 下载并落盘，再返回 dataURI
            if self._http is None:
                import httpx
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.get(remote)
                    resp.raise_for_status()
                    content = resp.content
            else:
                resp = await self._http.get(remote)
                resp.raise_for_status()
                content = resp.content
            p.write_bytes(content)
            b64 = base64.b64encode(content).decode("ascii")
            return f"data:image/png;base64,{b64}"
        except Exception as e:
            logger.info(f"头像缓存失败，回退远程: {e}")
            return remote


@register(
    PLUGIN_NAME,
    "Codex",
    "提交语录并生成带头像的语录图片",
    "0.2.0",
    "https://example.com/astrbot-plugin-quotes",
)
class QuotesPlugin(Star):
    def __init__(self, context: Context, config: Optional[AstrBotConfig] = None):
        super().__init__(context)
        self.config = config or {}
        # 规范化数据目录：优先使用配置 storage；否则使用 data/plugin_data/<plugin_name>
        storage = str(self.config.get("storage") or "").strip()
        data_root = Path(storage) if storage else (Path.cwd() / "data" / "plugin_data" / PLUGIN_NAME)
        # 复用 httpx 客户端，避免频繁创建销毁
        try:
            import httpx  # type: ignore
            self.http_client = httpx.AsyncClient(timeout=20)
        except Exception:
            self.http_client = None
        self.store = QuoteStore(data_root, http_client=self.http_client)
        self.avatar_provider = (self.config.get("avatar_provider") or "qlogo").lower()
        self.img_cfg = (self.config.get("image") or {})
        self.perf_cfg = (self.config.get("performance") or {})
        self._cfg_text_mode = bool(self.perf_cfg.get("text_mode", False))
        self._cfg_render_cache = bool(self.perf_cfg.get("render_cache", True))
        # 强制显示头像，移除本地头像命中逻辑
        # 发送记录：避免在消息中暴露 qid，通过会话最近记录辅助删除
        self._pending_qid: Dict[str, str] = {}
        self._last_sent_qid: Dict[str, str] = {}

    async def initialize(self):
        """可选：异步初始化。预热渲染器，降低首条渲染延迟。"""
        try:
            minimal = "<div style=\"width:320px;height:120px;background:#000;color:#fff\">init</div>"
            await self.html_render(minimal, {}, options={"full_page": False, "clip": {"x":0,"y":0,"width":320,"height":120}})
        except Exception:
            pass

    # ============= 指令 =============
    @filter.command_group("quote")
    def quote(self):
        """语录功能命令组（/quote ...）"""
        pass

    # @quote.command("add", alias={"append"})  # disabled: use 顶层指令“上传”
    async def add_quote(self, event: AstrMessageEvent):
        """添加语录（上传）。新版用法：先『回复某人的消息』，再发送“上传”。
        会自动剔除被回复文本中的 @ 提及，避免渲染进语录图。"""
        # 1) 解析被回复消息 ID
        reply_msg_id = self._get_reply_message_id(event)
        if not reply_msg_id:
            yield event.plain_result("请先『回复某人的消息』，再发送 上传。")
            return

        # 2) 拉取被回复消息内容与发送者
        target_text: Optional[str] = None
        target_qq: Optional[str] = None
        target_name: Optional[str] = None
        ret: Dict[str, Any] = await self._fetch_onebot_msg(event, reply_msg_id)
        if ret:
            target_text = self._extract_plaintext_from_onebot_message(ret.get("message"))
            sender = ret.get("sender") or {}
            target_qq = str(sender.get("user_id") or sender.get("qq") or "") or None
            card = (sender.get("card") or "").strip()
            nickname = (sender.get("nickname") or "").strip()
            target_name = card or nickname or target_qq

        # 回退处理
        if not target_text:
            # 允许在回复的同时附带额外文本时，取去掉指令后的剩余文本
            text = (event.message_str or "").strip()
            for kw in ("上传",):
                if text.startswith(kw):
                    text = text[len(kw):].strip()
            target_text = text or None

        # 收集图片（被回复消息 + 当前消息链），按群分目录
        group_key = str(event.get_group_id() or f"private_{event.get_sender_id()}")
        images_from_reply: List[str] = []
        if event.get_platform_name() == "aiocqhttp" and ret:
            images_from_reply = await self._ingest_images_from_onebot_message(event, ret.get("message"), group_key)
        images_from_current: List[str] = await self._ingest_images_from_segments(event, group_key)
        images: List[str] = list(dict.fromkeys(images_from_reply + images_from_current))

        # 统一剔除 @提及（例如：@昵称(123456789)、@昵称、@全体成员）
        target_text = self._strip_at_tokens(target_text or "") or ""

        # 文本缺失时允许“纯图片语录”
        if not target_text and not images:
            yield event.plain_result("未获取到被回复消息内容或图片，请确认已正确回复对方的消息或附带图片。")
            return
        if not target_text and images:
            target_text = "[图片]"

        # 名称/QQ 归属规则：
        # - 如果本条消息“自己上传了图片”（images_from_current 非空）：
        #     · 若@了某人，则语录归属该@用户。
        #     · 否则，语录归属为上传者本人（event.get_sender_id()）。
        # - 否则（未上传图片，仅回复他人）：
        #     · 优先使用被回复消息的发送者（target_qq）。
        #     · 若无，则回退到本条消息的@对象。
        mention_qq = self._extract_at_qq(event)
        # 归属优先级：@指定 > 自己上传 > 被回复消息发送者
        if mention_qq:
            target_qq = str(mention_qq)
        elif images_from_current:
            target_qq = str(event.get_sender_id())
        else:
            target_qq = str(target_qq or "")

        # 最终以归属 QQ 为准统一解析展示名，避免“名不对号”
        target_name = await self._resolve_user_name(event, target_qq) if target_qq else ""
        if not target_name:
            target_name = target_qq or "未知用户"

        from time import time

        q = Quote(
            id=str(int(time()*1000)) + f"_{random.randint(1000,9999)}",
            qq=str(target_qq or ""),
            name=str(target_name),
            text=str(target_text),
            created_by=str(event.get_sender_id()),
            created_at=time(),
            images=images,
        )
        await self.store.add(q)
        if images:
            yield event.plain_result(f"已收录 {q.name} 的语录，并保存 {len(images)} 张图片。")
        else:
            yield event.plain_result(f"已收录 {q.name} 的语录：{target_text}")

    @filter.command("语录")
    async def random_quote(self, event: AstrMessageEvent):
        """随机发送一条语录：
        - 若该语录含用户上传图片，直接发送原图（不经渲染）。
        - 若不含图片，则按原逻辑渲染语录图片。
        也可用：/quote random
        """
        # 若带 @某人，则仅随机该用户的语录
        only_qq = self._extract_at_qq(event)
        q = await (self.store.random_one_by_qq(only_qq) if only_qq else self.store.random_one())
        if not q:
            if only_qq:
                yield event.plain_result("这个用户还没有语录哦~")
            else:
                yield event.plain_result("还没有语录，先用 上传 保存一条吧~")
            return
        # 在不暴露 qid 的前提下，记录待发送的 qid（会在 after_message_sent 钩子中落到 _last_sent_qid）
        self._pending_qid[self._session_key(event)] = q.id
        # 优先发送原图
        if getattr(q, "images", None):
            try:
                rel = random.choice(q.images)
                # 兼容相对/绝对路径；兼容旧数据（以 quotes/images 开头）
                p = Path(rel)
                abs_path = p if p.is_absolute() else (self.store.root / rel)
                if not abs_path.exists() and isinstance(rel, str) and rel.startswith("quotes/"):
                    # 旧存储相对路径修正：去掉前缀 quotes/
                    fixed = rel.split("/", 1)[1] if "/" in rel else rel
                    abs_path = self.store.root / fixed
                if abs_path.exists():
                    yield event.chain_result([Comp.Image.fromFileSystem(str(abs_path))])
                    return
            except Exception as e:
                logger.info(f"随机原图发送失败，回退渲染：{e}")
        # 回退：渲染语录图（支持文本模式/渲染缓存）
        if self._cfg_text_mode:
            yield event.plain_result(f"「{q.text}」 — {q.name}")
            return
        cache_path = self.store.cache_dir / f"{q.id}.png"
        if self._cfg_render_cache and cache_path.exists():
            yield event.chain_result([Comp.Image.fromFileSystem(str(cache_path))])
            return
        img_url = await self._render_quote_image(q)
        # 尝试缓存渲染结果
        try:
            if self._cfg_render_cache:
                if img_url.startswith("file://"):
                    from urllib.parse import urlparse, unquote
                    p = urlparse(img_url)
                    local_path = Path(unquote(p.path))
                    if local_path.exists():
                        cache_path.write_bytes(local_path.read_bytes())
                        yield event.chain_result([Comp.Image.fromFileSystem(str(cache_path))])
                        return
                else:
                    if self.http_client is not None and img_url.startswith("http"):
                        r = await self.http_client.get(img_url)
                        if r.status_code == 200:
                            cache_path.write_bytes(r.content)
                            yield event.chain_result([Comp.Image.fromFileSystem(str(cache_path))])
                            return
        except Exception as e:
            logger.info(f"渲染缓存落盘失败: {e}")
        # 仍然直接发 URL
        yield event.image_result(img_url)

    # @quote.command("random")  # disabled: use 顶层指令“语录”
    async def random_quote_cmd(self, event: AstrMessageEvent):
        async for res in self.random_quote(event):
            yield res

    def _qid_tag(self, qid: str) -> str:
        return f"[qid:{qid}]"

    def _extract_qid_from_onebot_message(self, message) -> Optional[str]:
        try:
            if isinstance(message, list):
                buf: List[str] = []
                for m in message:
                    t = (m.get("type") or "").lower()
                    if t in ("text", "plain"):
                        d = m.get("data") or {}
                        buf.append(str(d.get("text") or ""))
                text = "".join(buf)
                m = re.search(r"\[qid:([^\]]+)\]", text)
                if m:
                    return m.group(1)
        except Exception:
            pass
        return None

    def _session_key(self, event: AstrMessageEvent) -> str:
        return str(event.get_group_id() or event.unified_msg_origin)

    @filter.command("删除", alias={"删除语录"})
    async def delete_quote(self, event: AstrMessageEvent):
        """删除语录：请『回复机器人发送的语录』并发送“删除”来删除该语录。"""
        # 提取被回复消息 id
        reply_msg_id: Optional[str] = None
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.Reply):
                    reply_msg_id = (
                        getattr(seg, "message_id", None)
                        or getattr(seg, "id", None)
                        or getattr(seg, "reply", None)
                        or getattr(seg, "msgId", None)
                    )
                    if reply_msg_id:
                        break
        except Exception as e:
            logger.warning(f"解析 Reply 段失败: {e}")

        if not reply_msg_id:
            yield event.plain_result("请先『回复机器人发送的语录』，再发送 删除。")
            return

        # 拉取被回复消息内容，解析 [qid:...] 标记（兼容旧消息）
        qid: Optional[str] = None
        if event.get_platform_name() == "aiocqhttp":
            try:
                client = event.bot
                ret = await client.api.call_action("get_msg", message_id=int(str(reply_msg_id)))
                qid = self._extract_qid_from_onebot_message(ret.get("message"))
            except Exception as e:
                logger.info(f"读取被回复消息失败：{e}")
        # 新版消息（不带任何文本标记）回退：删除当前会话最近一次随机发送的语录
        if not qid:
            qid = self._last_sent_qid.get(self._session_key(event))
        if not qid:
            yield event.plain_result("未能定位语录，请先重新发送一次随机语录再尝试删除。")
            return

        ok = await self.store.delete_by_id(qid)
        if ok:
            yield event.plain_result("已删除语录。")
        else:
            yield event.plain_result("未找到该语录，可能已被删除。")

    @filter.after_message_sent()
    async def on_after_message_sent(self, event: AstrMessageEvent):
        try:
            key = self._session_key(event)
            qid = self._pending_qid.pop(key, None)
            if qid:
                self._last_sent_qid[key] = qid
        except Exception as e:
            logger.info(f"after_message_sent 记录失败: {e}")

    @filter.command("语录帮助")
    async def help_quote(self, event: AstrMessageEvent):
        """显示语录相关指令的使用说明。"""
        help_text = (
            "语录插件帮助\n"
            "- 上传：先回复某人的消息，再发送“上传”（可附带图片）保存为语录。可在消息中 @某人 指定图片语录归属；不@则默认归属上传者。\n"
            "- 语录：随机发送一条语录；可用“语录 @某人”仅随机该用户的语录；若含用户上传图片，将直接发送原图。\n"
            "- 删除：回复机器人刚发送的随机语录消息，发送“删除”或“删除语录”进行删除。"
        )
        yield event.plain_result(help_text)

    def _get_reply_message_id(self, event: AstrMessageEvent) -> Optional[str]:
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.Reply):
                    mid = (
                        getattr(seg, "message_id", None)
                        or getattr(seg, "id", None)
                        or getattr(seg, "reply", None)
                        or getattr(seg, "msgId", None)
                    )
                    if mid:
                        return str(mid)
        except Exception as e:
            logger.warning(f"解析 Reply 段失败: {e}")
        return None

    async def _fetch_onebot_msg(self, event: AstrMessageEvent, message_id: str) -> Dict[str, Any]:
        if event.get_platform_name() != "aiocqhttp":
            return {}
        try:
            client = event.bot
            ret = await client.api.call_action("get_msg", message_id=int(str(message_id)))
            return ret or {}
        except Exception as e:
            logger.info(f"get_msg 失败: {e}")
            return {}

    # ============= 内部方法 =============
    def _extract_at_qq(self, event: AstrMessageEvent) -> Optional[str]:
        """尽可能稳健地提取 @ 对象的 QQ：
        - 优先从消息链的 Comp.At 段读取（兼容 qq/target/uin/user_id/id 字段）。
        - 兜底：从纯文本中匹配 "At:123456" 或 "@123456"。
        """
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.At):
                    for k in ("qq", "target", "uin", "user_id", "id"):
                        v = getattr(seg, k, None)
                        if v:
                            return str(v)
        except Exception as e:
            logger.warning(f"解析 @ 失败: {e}")
        # 文本兜底匹配（适配某些平台/日志形态）
        try:
            text = (event.message_str or "").strip()
            m = re.search(r"At[:：]\s*(\d{5,})", text)
            if not m:
                m = re.search(r"@\s*(\d{5,})", text)
            if m:
                return m.group(1)
        except Exception:
            pass
        return None

    async def _resolve_user_name(self, event: AstrMessageEvent, qq: str) -> str:
        # 优先尝试平台 API（Napcat），否则退回 qq 号
        if event.get_platform_name() == "aiocqhttp":
            try:
                group_id = event.get_group_id()
                client = event.bot  # aiocqhttp client
                if group_id:
                    payloads = {"group_id": int(group_id), "user_id": int(qq), "no_cache": True}
                    ret = await client.api.call_action("get_group_member_info", **payloads)
                    # 优先群名片，其次昵称
                    card = (ret.get("card") or "").strip()
                    nickname = (ret.get("nickname") or "").strip()
                    if card or nickname:
                        return card or nickname
                else:
                    payloads = {"user_id": int(qq), "no_cache": True}
                    ret = await client.api.call_action("get_stranger_info", **payloads)
                    nickname = (ret.get("nickname") or "").strip()
                    if nickname:
                        return nickname
            except Exception as e:
                logger.info(f"读取 Napcat 用户信息失败，回退：{e}")
        return str(qq)

    def _avatar_url(self, qq: str) -> str:
        # qlogo 头像
        size = 640
        return f"https://q1.qlogo.cn/g?b=qq&nk={qq}&s={size}"

    def _extract_plaintext_from_onebot_message(self, message) -> Optional[str]:
        try:
            if isinstance(message, list):
                parts: List[str] = []
                for m in message:
                    t = m.get("type")
                    d = m.get("data") or {}
                    if t in ("text", "plain"):
                        parts.append(str(d.get("text") or ""))
                text = "".join(parts).strip()
                return text if text else None
        except Exception:
            pass
        return None

    async def _ingest_images_from_onebot_message(self, event: AstrMessageEvent, message, group_key: str) -> List[str]:
        saved: List[str] = []
        try:
            if not isinstance(message, list):
                return saved
            for m in message:
                try:
                    if (m.get("type") or "").lower() != "image":
                        continue
                    d = m.get("data") or {}
                    url = d.get("url") or d.get("image_url")
                    if url and (str(url).startswith("http://") or str(url).startswith("https://")):
                        rel = await self.store.save_image_from_url(str(url), group_key)
                        if rel:
                            saved.append(rel)
                            continue
                    file_id_or_path = d.get("file") or d.get("path")
                    if file_id_or_path:
                        # Napcat get_image: 入参 file=xxx，返回 { 'file': 'C:\\...'}
                        try:
                            client = event.bot
                            ret = await client.api.call_action("get_image", file=str(file_id_or_path))
                            local_path = ret.get("file") or ret.get("path") or ret.get("file_path")
                            if local_path:
                                rel = await self.store.save_image_from_fs(str(local_path), group_key)
                                if rel:
                                    saved.append(rel)
                                    continue
                        except Exception as e:
                            logger.info(f"get_image 回退失败: {e}")
                        # 直接尝试当作本地路径
                        rel = await self.store.save_image_from_fs(str(file_id_or_path), group_key)
                        if rel:
                            saved.append(rel)
                except Exception as e:
                    logger.warning(f"处理图片段失败: {e}")
        except Exception as e:
            logger.warning(f"解析 OneBot 消息图片失败: {e}")
        return list(dict.fromkeys(saved))  # 去重，保持顺序

    async def _ingest_images_from_segments(self, event: AstrMessageEvent, group_key: str) -> List[str]:
        saved: List[str] = []
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.Image):
                    url = getattr(seg, "url", None)
                    file_or_path = getattr(seg, "file", None) or getattr(seg, "path", None)
                    if url and (str(url).startswith("http://") or str(url).startswith("https://")):
                        rel = await self.store.save_image_from_url(str(url), group_key)
                        if rel:
                            saved.append(rel)
                    elif file_or_path:
                        # 尝试当作本地路径复制；若失败再走 Napcat get_image
                        rel = await self.store.save_image_from_fs(str(file_or_path), group_key)
                        if rel:
                            saved.append(rel)
                        elif event.get_platform_name() == "aiocqhttp":
                            try:
                                client = event.bot
                                ret = await client.api.call_action("get_image", file=str(file_or_path))
                                local_path = ret.get("file") or ret.get("path") or ret.get("file_path")
                                if local_path:
                                    rel = await self.store.save_image_from_fs(str(local_path), group_key)
                                    if rel:
                                        saved.append(rel)
                            except Exception as e:
                                logger.info(f"segments get_image 回退失败: {e}")
        except Exception as e:
            logger.warning(f"解析 Comp.Image 失败: {e}")
        return list(dict.fromkeys(saved))

    async def _render_quote_image(self, q: Quote) -> str:
        width = int(self.img_cfg.get("width", 1280))
        height = int(self.img_cfg.get("height", 427))
        bg_color = self.img_cfg.get("bg_color", "#000")
        text_color = self.img_cfg.get("text_color", "#fff")
        font_family = self.img_cfg.get("font_family", "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', 'WenQuanYi Micro Hei', Arial, sans-serif")

        # 始终使用远程 qlogo 头像（避免本地 file:// 或 dataURI 兼容性问题）
        avatar = self._avatar_url(q.qq)
        safe_text = self._strip_at_tokens(q.text)
        escaped_text = html.escape(safe_text)
        grad_width = max(200, int(width * 0.26))
        grad_left = int(width * 0.36) - int(grad_width * 0.7)

        TMPL = f"""
        <html>
        <head>
            <meta charset='utf-8' />
            <style>
                * {{ box-sizing: border-box; }}
                html, body {{ margin:0; padding:0; width:{width}px; height:{height}px; background:{bg_color}; }}
                .root {{ position:relative; width:{width}px; height:{height}px; background:{bg_color}; font-family:{font_family}; overflow:hidden; }}
                .left {{ position:absolute; left:0; top:0; width:{int(width*0.36)}px; height:{height}px; overflow:hidden; z-index:0; }}
                .left img {{ width:100%; height:100%; object-fit:cover; display:block; }}
                .left .left-shade {{ position:absolute; inset:0; background: linear-gradient(to right, rgba(0,0,0,0) 0%, rgba(0,0,0,0.28) 58%, rgba(0,0,0,0.55) 100%); }}
                .right {{ position:absolute; left:{int(width*0.36)}px; top:0; width:{int(width*0.64)}px; height:{height}px; background:{bg_color}; display:flex; align-items:center; justify-content:center; text-align:center; z-index:2; }}
                .text {{ color:{text_color}; font-size:38px; line-height:1.6; padding:0 80px; max-width:calc(100% - 160px); display:flex; align-items:center; justify-content:center; text-align:center; }}
                .signature {{ position:absolute; right:44px; bottom:28px; color:rgba(255,255,255,0.82); font-size:22px; font-weight:300; -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; text-rendering: geometricPrecision; letter-spacing:0.2px; z-index:3; }}
                .quote-mark {{ color:{text_color}; opacity:0.8; margin-right:14px; }}
                /* 大范围渐变覆盖，避免中间出现分界线 */
                .fade-overlay {{
                    position:absolute;
                    top:0; bottom:0;
                    left:{grad_left}px;
                    width:{grad_width}px;
                    pointer-events:none;
                    z-index:1;
                    background: linear-gradient(
                        to right,
                        rgba(0,0,0,0.00) 0%,
                        rgba(0,0,0,0.35) 38%,
                        rgba(0,0,0,0.70) 70%,
                        {bg_color} 100%
                    );
                }}
            </style>
        </head>
        <body>
            <div class="root">
                <div class="left"><img src="{avatar}" /><div class="left-shade"></div></div>
                <div class="right">
                    <div class="text">
                        <span class="quote-mark">「</span>
                        <div>{escaped_text}</div>
                        <span class="quote-mark">」</span>
                    </div>
                </div>
                <div class="fade-overlay"></div>
                <div class="signature">— {q.name}</div>
            </div>
        </body>
        </html>
        """
        # 渲染时强制裁剪并禁止全页，避免底部空白
        options = {
            "full_page": False,
            "omit_background": False,
            "clip": {"x": 0, "y": 0, "width": width, "height": height},
        }
        url = await self.html_render(TMPL, {}, options=options)
        return url

    async def terminate(self):
        """插件卸载/停用时回调。"""
        try:
            if getattr(self, "http_client", None):
                await self.http_client.aclose()
        except Exception:
            pass

    # ============= 语录提交中文别名 =============
    @filter.command("上传")
    async def add_quote_alias(self, event: AstrMessageEvent):
        async for res in self.add_quote(event):
            yield res

    def _strip_at_tokens(self, text: str) -> str:
        """去除 At 提及文本，例如：@昵称(123456789)、@昵称、@全体成员。
        仅影响渲染与入库文本，不改变其他消息段。"""
        if not text:
            return ""
        # 移除 @昵称(数字) 或 @昵称 以及 @全体成员（含中文括号）
        text = re.sub(r"@[^@\s（）()]+(?:[（(]\d{5,}[）)])?", "", text)
        text = text.replace("@全体成员", "")
        # 压缩多余空白
        return " ".join(text.split())


