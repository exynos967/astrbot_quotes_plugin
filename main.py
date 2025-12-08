from __future__ import annotations

import asyncio
import json
import re
import html
import secrets
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrbot.api.event import filter, AstrMessageEvent
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
    images: List[str] = field(default_factory=list)  # 相对插件数据根目录的路径，如 "images/<group>/<xxx>.jpg"
    group: str = ""  # 群聊隔离标识：群ID，或私聊为 private_<sender_id>


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
            # 使用加时间戳的不可预测随机后缀，提升随机性
            suffix = secrets.token_hex(2)
            filename = f"{int(time()*1000)}_{suffix}{ext}"
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
            suffix = secrets.token_hex(2)
            filename = f"{int(time()*1000)}_{suffix}{ext}"
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

    async def random_one(self, group_key: Optional[str] = None) -> Optional[Quote]:
        """随机返回一条语录。

        - 当 group_key 为 None 时，忽略群聊隔离键，在所有语录中随机（用于全局模式）。
        - 当 group_key 为非空字符串时，仅在该 group_key 对应会话内随机。
        """
        if group_key is None:
            arr = list(self._quotes)
        else:
            arr = [x for x in self._quotes if str(x.get("group") or "") == str(group_key)]
        if not arr:
            return None
        obj = secrets.choice(arr)
        return Quote(**obj)

    async def random_one_by_qq(self, qq: str, group_key: Optional[str] = None) -> Optional[Quote]:
        """按 QQ 过滤后随机返回一条语录。

        - 当 group_key 为 None 时，仅按 QQ 过滤（全局范围）。
        - 当 group_key 为非空字符串时，按 QQ + 会话隔离键共同过滤。
        """
        if group_key is None:
            arr = [x for x in self._quotes if str(x.get("qq") or "") == str(qq)]
        else:
            arr = [
                x
                for x in self._quotes
                if str(x.get("qq") or "") == str(qq)
                and str(x.get("group") or "") == str(group_key)
            ]
        if not arr:
            return None
        obj = secrets.choice(arr)
        return Quote(**obj)

    async def delete_by_id(self, qid: str) -> bool:
        async with self._lock:
            old_len = len(self._quotes)
            self._quotes = [x for x in self._quotes if str(x.get("id")) != str(qid)]
            if len(self._quotes) == old_len:
                return False
            self._write({"quotes": self._quotes})
            return True




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
        # 行为设置
        self._cfg_global_mode = bool(self.config.get("global_mode", False))  # True=跨群共享语录，False=按群隔离
        # 图片签名与戳一戳触发配置
        self._cfg_image_sig_use_group = bool(self.config.get("image_signature_use_group", False))
        # 戳一戳配置：开关、概率与群黑白名单（仅作用于戳 Bot 本身时的触发）
        self._cfg_poke_enabled = bool(self.config.get("poke_enabled", True))
        raw_prob = self.config.get("poke_probability", 100)
        try:
            prob = int(raw_prob)
        except (TypeError, ValueError):
            prob = 100
        # 限制在 0-100 范围内，表示百分比概率
        self._cfg_poke_probability = max(0, min(100, prob))
        # 群白名单/黑名单：统一转为字符串集合以便匹配 group_id
        self._cfg_poke_group_whitelist = {
            str(x).strip()
            for x in (self.config.get("poke_group_whitelist") or [])
            if str(x).strip()
        }
        self._cfg_poke_group_blacklist = {
            str(x).strip()
            for x in (self.config.get("poke_group_blacklist") or [])
            if str(x).strip()
        }
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
    # 指令入口
    async def add_quote(self, event: AstrMessageEvent, uid: str = ""):
        """添加语录（上传）。新版用法：
        - 方式一：先『回复某人的消息』，再发送“上传”（可附图）。
        - 方式二：不回复，直接“上传 <QQ号>”并附图，归属该 QQ。"""
        # 1) 解析被回复消息 ID（可选）
        reply_msg_id = self._get_reply_message_id(event)

        # 2) 拉取被回复消息内容与发送者
        target_text: Optional[str] = None
        target_qq: Optional[str] = None
        target_name: Optional[str] = None
        ret: Dict[str, Any] = await self._fetch_onebot_msg(event, reply_msg_id) if reply_msg_id else {}
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
        # 从命令参数 uid 覆盖（仅数字、长度>=5 即视为 QQ）
        param_qq = uid.strip() if uid and uid.strip().isdigit() and len(uid.strip()) >= 5 else ""
        mention_qq = self._extract_at_qq(event)
        # 归属优先级：参数QQ > @指定 > 自己上传 > 被回复消息发送者
        if param_qq:
            target_qq = str(param_qq)
        elif mention_qq:
            target_qq = str(mention_qq)
        elif images_from_current:
            target_qq = str(event.get_sender_id())
        else:
            target_qq = str(target_qq or "")

        # 黑名单拦截：被标记为黑名单的 QQ 不再收录语录（包括“上传 QQ号/@” 场景）
        if target_qq and self._is_blacklisted(target_qq):
            yield event.plain_result("该用户在语录黑名单中，本次语录已忽略。")
            return

        # 最终以归属 QQ 为准统一解析展示名，避免“名不对号”
        target_name = await self._resolve_user_name(event, target_qq) if target_qq else ""
        if not target_name:
            target_name = target_qq or "未知用户"

        from time import time

        # 使用时间戳 + 随机 token 生成不可预测的语录 ID
        q = Quote(
            id=str(int(time()*1000)) + f"_{secrets.token_hex(2)}",
            qq=str(target_qq or ""),
            name=str(target_name),
            text=str(target_text),
            created_by=str(event.get_sender_id()),
            created_at=time(),
            images=images,
            group=group_key,
        )
        await self.store.add(q)
        if images:
            yield event.plain_result(f"已收录 {q.name} 的语录，并保存 {len(images)} 张图片。")
        else:
            yield event.plain_result(f"已收录 {q.name} 的语录：{target_text}")

    @filter.command("语录")
    async def random_quote(self, event: AstrMessageEvent, uid: str = "", _silent_if_empty: bool = False):
        """随机发送一条语录：
        - 若该语录含用户上传图片，直接发送原图（不经渲染）。
        - 若不含图片，则按原逻辑渲染语录图片。

        支持过滤：指令前缀+语录 <QQ号> 或 指令前缀+语录 @某人。
        """
        # 当前会话的群聊隔离键（全局模式下忽略）
        group_key = str(event.get_group_id() or f"private_{event.get_sender_id()}")
        effective_group = None if self._cfg_global_mode else group_key
        # 解析参数 QQ（不做正则，只校验纯数字长度>=5），否则用 @
        explicit = uid.strip() if (uid and uid.strip().isdigit() and len(uid.strip()) >= 5) else None
        only_qq = explicit or self._extract_at_qq(event)
        q = await (self.store.random_one_by_qq(only_qq, effective_group) if only_qq else self.store.random_one(effective_group))
        if not q:
            # 戳一戳触发时允许在没有语录的情况下静默返回，不打扰群聊
            if _silent_if_empty:
                return
            if only_qq:
                yield event.plain_result("这个用户还没有语录哦~" if self._cfg_global_mode else "这个用户在本会话还没有语录哦~")
            else:
                yield event.plain_result("还没有语录，先用 上传 保存一条吧~" if self._cfg_global_mode else "本会话还没有语录，先用 上传 保存一条吧~")
            return
        # 在不暴露 qid 的前提下，记录待发送的 qid（会在 after_message_sent 钩子中落到 _last_sent_qid）
        self._pending_qid[self._session_key(event)] = q.id
        # 优先发送原图
        if getattr(q, "images", None):
            try:
                rel = secrets.choice(q.images)
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
        # 仅在需要实际渲染时解析签名文本，减少额外 API 调用
        signature = await self._resolve_signature_name(event, q)
        img_url = await self._render_quote_image(q, signature=signature)
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

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def random_quote_on_poke(self, event: AstrMessageEvent):
        """当收到“对 Bot 本身的戳一戳”消息段时，按配置随机发送一条语录。"""
        # 开关关闭则完全不处理戳一戳
        if not self._cfg_poke_enabled:
            return

        # 群黑白名单控制：仅群聊生效，私聊不受影响
        group_id = event.get_group_id()
        if not self._is_poke_allowed_in_group(group_id):
            return

        # 仅响应“戳 Bot 本身”的 Poke 段
        self_id = self._get_self_id(event)
        if not self_id:
            return

        try:
            segments = list(event.get_messages())  # type: ignore[attr-defined]
        except Exception:
            return

        has_poke_to_bot = False
        for seg in segments:
            try:
                if isinstance(seg, Comp.Poke):
                    target = self._extract_poke_target(seg)
                    if target and str(target) == str(self_id):
                        has_poke_to_bot = True
                        break
            except Exception:
                # 某些平台可能不存在 Poke 类型，忽略类型判断异常
                continue

        if not has_poke_to_bot:
            return

        # 概率控制（百分比 0-100）
        prob = self._cfg_poke_probability
        if prob <= 0:
            return
        if prob < 100:
            if secrets.randbelow(100) >= prob:
                return

        # 复用现有随机语录逻辑；当没有语录时静默返回（不发送“本群没有语录”类提示）
        async for res in self.random_quote(event, uid="", _silent_if_empty=True):
            yield res

    # 删除了未使用的旧入口与旧版 qid 标记解析，精简结构


    def _session_key(self, event: AstrMessageEvent) -> str:
        return str(event.get_group_id() or event.unified_msg_origin)

    @filter.command("删除", alias={"删除语录"})
    async def delete_quote(self, event: AstrMessageEvent):
        """删除语录：请『回复机器人发送的语录』并发送“删除”来删除该语录。"""
        # 权限检查：根据配置的 delete_permission 进行动态校验
        if not await self._check_delete_permission(event):
            yield event.plain_result("权限不足：你无权使用删除语录指令。")
            return

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

        # 兼容逻辑回退：删除当前会话最近一次随机发送的语录
        key = self._session_key(event)
        # 优先使用已确认发送成功的 qid；若因 after_message_sent 钩子异常未写入，则回退使用 pending 记录
        qid: Optional[str] = self._last_sent_qid.get(key) or self._pending_qid.get(key)
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
            "- 语录：随机发送一条语录；可用“语录 @某人”或“指令前缀+语录 12345678”仅随机该用户的语录；若含用户上传图片，将直接发送原图。\n"
            "- 删除：回复机器人刚发送的随机语录消息，发送“删除”或“删除语录”进行删除。\n"
            "- 设置：可在插件设置开启“全局模式”以跨群共享语录；关闭则各群/私聊互相隔离。"
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
    async def _check_delete_permission(self, event: AstrMessageEvent) -> bool:
        """根据配置项 delete_permission 检查当前用户是否有权限删除语录。

        支持的权限组（配置字符串）：
        - 群员：所有人可用；
        - 管理员：群管理员、群主 或 AstrBot 配置中的 Bot 管理员；
        - 群主：仅群主 或 AstrBot Bot 管理员；
        - Bot管理员：仅 AstrBot 配置中的 Bot 管理员（admins_id 列表）。

        说明：
        - Bot 管理员由 AstrBot 全局 admins_id 列表决定（event.is_admin()）。
        - QQ 群管理员/群主通过 event.get_group() 返回的 Group 对象中的
          group_admins、group_owner 字段判定，仅在群聊场景生效。
        - 在非群聊场景下，管理员/群主 权限均退化为仅允许 Bot 管理员。
        """
        lv_raw = str(self.config.get("delete_permission") or "管理员").strip()
        level = lv_raw.replace(" ", "")

        # 群员：所有人均可删除
        if level in {"群员", "member", "普通成员"}:
            return True

        # AstrBot 定义的 Bot 管理员（admins_id 列表）
        is_bot_admin = False
        try:
            is_bot_admin = bool(getattr(event, "is_admin", None) and event.is_admin())
        except Exception:
            is_bot_admin = False

        # 仅 Bot 管理员
        if level in {"Bot管理员", "bot管理员", "BOT管理员", "bot_admin", "BotAdmin"}:
            return is_bot_admin

        # 以下权限级别需要考虑群角色；若不是群聊，则退化为仅允许 Bot 管理员
        group_id = event.get_group_id()
        if not group_id:
            return is_bot_admin

        # 通过 AstrBot 的 Group 抽象判断群主/管理员
        is_group_owner = False
        is_group_admin = False
        try:
            # AiocqhttpMessageEvent.get_group() 会返回包含 owner/admin 列表的 Group
            group = await (event.get_group() if hasattr(event, "get_group") else None)  # type: ignore[call-arg]
        except Exception as e:
            logger.info(f"查询群信息失败: {e}")
            group = None

        if group is not None:
            sender_id = str(event.get_sender_id())
            try:
                owner_id = str(getattr(group, "group_owner", "") or "")
                admin_ids = [str(x) for x in getattr(group, "group_admins", [])]
            except Exception:
                owner_id = ""
                admin_ids = []
            is_group_owner = bool(owner_id and sender_id == owner_id)
            is_group_admin = bool(sender_id in admin_ids)

        # 管理员：群管理员、群主 或 Bot 管理员
        if level in {"管理员", "admin"}:
            return is_group_admin or is_group_owner or is_bot_admin
        # 群主：仅群主 或 Bot 管理员
        if level in {"群主", "owner"}:
            return is_group_owner or is_bot_admin

        # 未知配置值时，出于安全考虑仅允许 Bot 管理员
        return is_bot_admin

    def _extract_at_qq(self, event: AstrMessageEvent) -> Optional[str]:
        """从消息链 Comp.At 段提取 QQ（不做正则解析）。"""
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.At):
                    for k in ("qq", "target", "uin", "user_id", "id"):
                        v = getattr(seg, k, None)
                        if v:
                            return str(v)
        except Exception as e:
            logger.warning(f"解析 @ 失败: {e}")
        return None

    def _get_self_id(self, event: AstrMessageEvent) -> str:
        """尝试获取当前 Bot 自身的标识（例如 QQ 号），用于判断戳一戳目标是否为 Bot 本身。

        优先使用 message_obj.self_id，其次尝试事件上的常见字段，最后回退到 raw_event。
        如果多次尝试都失败，则返回空字符串。
        """
        # 1) AstrBotMessage.self_id
        try:
            msg = getattr(event, "message_obj", None)
            if msg is not None:
                v = getattr(msg, "self_id", None)
                if v:
                    return str(v)
        except Exception:
            pass

        # 2) 事件对象上的 self_id 字段
        try:
            v = getattr(event, "self_id", None)
            if v:
                return str(v)
        except Exception:
            pass

        # 3) 原始事件结构（如 OneBot/Napcat payload）
        try:
            raw = getattr(event, "raw_event", None)
            if isinstance(raw, dict):
                v = raw.get("self_id")
                if v:
                    return str(v)
        except Exception:
            pass

        return ""

    def _extract_poke_target(self, seg: Any) -> Optional[str]:
        """从 Poke 消息段中提取被戳目标，用于判断是否戳 Bot 本身。

        尝试兼容多种字段名：qq/target/target_id/user_id/uin/id。
        """
        for name in ("qq", "target", "target_id", "user_id", "uin", "id"):
            try:
                v = getattr(seg, name, None)
            except Exception:
                v = None
            if v:
                return str(v)
        return None

    def _is_poke_allowed_in_group(self, group_id: Optional[str]) -> bool:
        """根据群黑白名单配置判断当前群是否允许戳一戳触发。"""
        if not group_id:
            # 私聊场景不受群黑白名单影响
            return True
        gid = str(group_id)
        whitelist = getattr(self, "_cfg_poke_group_whitelist", set())
        blacklist = getattr(self, "_cfg_poke_group_blacklist", set())

        if whitelist:
            return gid in whitelist
        if blacklist:
            return gid not in blacklist
        return True
    def _parse_blacklist(self) -> set[str]:
        """从配置中解析语录黑名单 QQ 列表。

        配置项 blacklist 现为 list 类型（每项为一个 QQ 号字符串），但也兼容旧版文本配置：
        - 新版：blacklist = ["123456789", "987654321"]
        - 兼容：若读取到的是字符串，则按旧逻辑按行与中英文分号/逗号切分。
        仅保留纯数字且长度>=5 的条目，作为有效 QQ 号。
        """
        raw_val = self.config.get("blacklist")  # 可能是 list 或 str
        items: set[str] = set()

        # 新版 list 配置
        if isinstance(raw_val, (list, tuple)):
            for v in raw_val:
                s = str(v).strip()
                if s.isdigit() and len(s) >= 5:
                    items.add(s)
            return items

        # 兼容旧版文本配置
        raw = str(raw_val or "").strip()
        if not raw:
            return set()
        for line in raw.splitlines():
            for token in re.split(r"[;,，；]", line):
                s = token.strip()
                if s.isdigit() and len(s) >= 5:
                    items.add(s)
        return items

    def _is_blacklisted(self, qq: Optional[str]) -> bool:
        """判断给定 QQ 是否在语录黑名单中。"""
        if not qq:
            return False
        return str(qq) in self._parse_blacklist()

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

    async def _resolve_signature_name(self, event: AstrMessageEvent, q: Quote) -> str:
        """根据配置决定语录图片右下角签名文本（语录所属成员的群名片或 QQ 名称）。"""
        # 默认使用语录归属的名称
        if not getattr(self, "_cfg_image_sig_use_group", False):
            return q.name

        # 仅在 Napcat/OneBot v11 场景下尝试按群名片显示
        if event.get_platform_name() == "aiocqhttp":
            group_id_raw = str(q.group or "").strip()
            qq_raw = str(q.qq or "").strip()
            if group_id_raw.isdigit() and qq_raw.isdigit():
                try:
                    client = event.bot  # aiocqhttp client
                    payloads = {
                        "group_id": int(group_id_raw),
                        "user_id": int(qq_raw),
                        "no_cache": True,
                    }
                    ret = await client.api.call_action("get_group_member_info", **payloads)
                    card = (ret.get("card") or "").strip()
                    nickname = (ret.get("nickname") or "").strip()
                    if card or nickname:
                        # 优先群名片，其次当前群内昵称
                        return card or nickname
                except Exception as e:
                    logger.info(f"读取 Napcat 群名片失败，回退：{e}")

        # 其他平台或失败时仍使用 QQ 名称
        return q.name

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

    async def _render_quote_image(self, q: Quote, signature: Optional[str] = None) -> str:
        width = int(self.img_cfg.get("width", 1280))
        height = int(self.img_cfg.get("height", 427))
        bg_color = self.img_cfg.get("bg_color", "#000")
        text_color = self.img_cfg.get("text_color", "#fff")
        font_family = self.img_cfg.get("font_family", "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', 'WenQuanYi Micro Hei', Arial, sans-serif")

        # 始终使用远程 qlogo 头像（避免本地 file:// 或 dataURI 兼容性问题）
        avatar = self._avatar_url(q.qq)
        safe_text = self._strip_at_tokens(q.text)
        escaped_text = html.escape(safe_text)
        sig_text = (signature or q.name)
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
	                <div class="signature">— {sig_text}</div>
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
    async def add_quote_alias(self, event: AstrMessageEvent, uid: str = ""):
        async for res in self.add_quote(event, uid=uid):
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
