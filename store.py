from __future__ import annotations

import asyncio
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from astrbot.api import logger
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)

try:
    from .constants import (
        CACHE_DIRNAME,
        DUPLICATE_IMAGE_MESSAGE,
        GROUPS_DIRNAME,
        IMAGE_INDEX_FILENAME,
        IMAGES_DIRNAME,
        LEGACY_QUOTES_BAK_SUFFIX,
        MAX_SENT_RECORDS,
        MEDIA_DIRNAME,
        MEDIA_INDEX_FILENAME,
        QUOTES_FILENAME,
        SCHEMA_VERSION,
        SENT_INDEX_FILENAME,
    )
    from .models import (
        ForwardNode,
        ForwardSegment,
        ImageAsset,
        MediaAsset,
        PendingForwardNode,
        PendingQuoteSegment,
        PreparedImage,
        PreparedMedia,
        Quote,
        ImageSignature,
        SentQuoteRecord,
        QuoteSegment,
    )
    from .utils import (
        atomic_write_json,
        is_near_duplicate,
        prepare_image,
        random_id,
        read_json,
        rel_image_path,
        rel_media_path,
    )
except ImportError:  # pragma: no cover
    from constants import (
        CACHE_DIRNAME,
        DUPLICATE_IMAGE_MESSAGE,
        GROUPS_DIRNAME,
        IMAGE_INDEX_FILENAME,
        IMAGES_DIRNAME,
        LEGACY_QUOTES_BAK_SUFFIX,
        MAX_SENT_RECORDS,
        MEDIA_DIRNAME,
        MEDIA_INDEX_FILENAME,
        QUOTES_FILENAME,
        SCHEMA_VERSION,
        SENT_INDEX_FILENAME,
    )
    from models import (
        ForwardNode,
        ForwardSegment,
        ImageAsset,
        MediaAsset,
        PendingForwardNode,
        PendingQuoteSegment,
        PreparedImage,
        PreparedMedia,
        Quote,
        ImageSignature,
        SentQuoteRecord,
        QuoteSegment,
    )
    from utils import (
        atomic_write_json,
        is_near_duplicate,
        prepare_image,
        random_id,
        read_json,
        rel_image_path,
        rel_media_path,
    )


@dataclass(slots=True)
class CreateQuoteResult:
    quote: Quote | None = None
    duplicate: bool = False
    message: str = ""


class SessionStore:
    def __init__(self, plugin_root: Path, session_key: str):
        self.plugin_root = Path(plugin_root)
        self.session_key = session_key
        self.root = self.plugin_root / GROUPS_DIRNAME / session_key
        self.images_dir = self.root / IMAGES_DIRNAME
        self.media_dir = self.root / MEDIA_DIRNAME
        self.cache_dir = self.root / CACHE_DIRNAME
        self.quotes_file = self.root / QUOTES_FILENAME
        self.image_index_file = self.root / IMAGE_INDEX_FILENAME
        self.media_index_file = self.root / MEDIA_INDEX_FILENAME
        self.sent_index_file = self.root / SENT_INDEX_FILENAME
        self.lock = asyncio.Lock()
        self.root.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.media_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def load_quotes(self) -> list[Quote]:
        payload = read_json(
            self.quotes_file,
            {"schema_version": SCHEMA_VERSION, "session_key": self.session_key, "quotes": []},
        )
        return [Quote.from_dict(item) for item in (payload.get("quotes") or [])]

    def save_quotes(self, quotes: list[Quote]) -> None:
        atomic_write_json(
            self.quotes_file,
            {
                "schema_version": SCHEMA_VERSION,
                "session_key": self.session_key,
                "quotes": [item.to_dict() for item in quotes],
            },
        )

    def load_assets(self) -> list[ImageAsset]:
        payload = read_json(
            self.image_index_file,
            {"schema_version": SCHEMA_VERSION, "session_key": self.session_key, "images": []},
        )
        return [ImageAsset.from_dict(item) for item in (payload.get("images") or [])]

    def save_assets(self, assets: list[ImageAsset]) -> None:
        atomic_write_json(
            self.image_index_file,
            {
                "schema_version": SCHEMA_VERSION,
                "session_key": self.session_key,
                "images": [item.to_dict() for item in assets],
            },
        )

    def load_media_assets(self) -> list[MediaAsset]:
        payload = read_json(
            self.media_index_file,
            {"schema_version": SCHEMA_VERSION, "session_key": self.session_key, "media": []},
        )
        return [MediaAsset.from_dict(item) for item in (payload.get("media") or [])]

    def save_media_assets(self, assets: list[MediaAsset]) -> None:
        atomic_write_json(
            self.media_index_file,
            {
                "schema_version": SCHEMA_VERSION,
                "session_key": self.session_key,
                "media": [item.to_dict() for item in assets],
            },
        )

    def image_abs_path(self, file_name: str) -> Path:
        return self.images_dir / file_name

    def media_abs_path(self, file_name: str) -> Path:
        return self.media_dir / file_name

    def cache_path(self, quote_id: str) -> Path:
        return self.cache_dir / f"{quote_id}.png"

    def load_sent_records(self) -> list[SentQuoteRecord]:
        payload = read_json(
            self.sent_index_file,
            {"schema_version": SCHEMA_VERSION, "session_key": self.session_key, "sent": []},
        )
        return [SentQuoteRecord.from_dict(item) for item in (payload.get("sent") or [])]

    def save_sent_records(self, records: list[SentQuoteRecord]) -> None:
        atomic_write_json(
            self.sent_index_file,
            {
                "schema_version": SCHEMA_VERSION,
                "session_key": self.session_key,
                "sent": [item.to_dict() for item in records],
            },
        )


class QuoteRepository:
    def __init__(self, plugin_root: Path):
        self.root = Path(plugin_root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.groups_dir = self.root / GROUPS_DIRNAME
        self.groups_dir.mkdir(parents=True, exist_ok=True)
        self._stores: dict[str, SessionStore] = {}
        self._migration_lock = asyncio.Lock()

    def get_store(self, session_key: str) -> SessionStore:
        if session_key not in self._stores:
            self._stores[session_key] = SessionStore(self.root, session_key)
        return self._stores[session_key]

    def session_keys(self) -> list[str]:
        if not self.groups_dir.exists():
            return []
        return sorted(path.name for path in self.groups_dir.iterdir() if path.is_dir())

    def find_asset(self, session_key: str, asset_id: str) -> ImageAsset | None:
        for asset in self.get_store(session_key).load_assets():
            if asset.asset_id == asset_id:
                return asset
        return None

    def find_media_asset(self, session_key: str, asset_id: str) -> MediaAsset | None:
        for asset in self.get_store(session_key).load_media_assets():
            if asset.asset_id == asset_id:
                return asset
        return None

    def list_quotes(self, session_key: str) -> list[Quote]:
        return self.get_store(session_key).load_quotes()

    def get_quote(self, session_key: str, quote_id: str) -> Quote | None:
        for quote in self.get_store(session_key).load_quotes():
            if quote.id == quote_id:
                return quote
        return None

    async def record_sent_quote(
        self,
        session_key: str,
        *,
        quote_id: str,
        fingerprint: str,
        sent_at: float,
        image_signatures: list[ImageSignature] | None = None,
    ) -> None:
        if not quote_id or not fingerprint:
            return
        store = self.get_store(session_key)
        async with store.lock:
            records = [
                item
                for item in store.load_sent_records()
                if item.quote_id and item.fingerprint
            ]
            records.append(
                SentQuoteRecord(
                    quote_id=quote_id,
                    fingerprint=fingerprint,
                    sent_at=sent_at,
                    image_signatures=image_signatures or [],
                )
            )
            records = sorted(records, key=lambda item: item.sent_at)
            if len(records) > MAX_SENT_RECORDS:
                records = records[-MAX_SENT_RECORDS:]
            store.save_sent_records(records)

    def find_sent_quote_id(
        self,
        session_key: str,
        *,
        fingerprint: str,
        replied_at: float = 0.0,
    ) -> str | None:
        if not fingerprint:
            return None
        records = self.get_store(session_key).load_sent_records()
        matches = [item for item in records if item.fingerprint == fingerprint and item.quote_id]
        if not matches:
            return None
        if replied_at > 0:
            bounded = [item for item in matches if item.sent_at <= replied_at]
            if bounded:
                matches = bounded
        matches.sort(key=lambda item: item.sent_at, reverse=True)
        return matches[0].quote_id if matches else None

    def find_sent_quote_id_by_image_signature(
        self,
        session_key: str,
        *,
        image: PreparedImage,
        replied_at: float = 0.0,
    ) -> str | None:
        if image is None:
            return None
        records = self.get_store(session_key).load_sent_records()
        matches: list[SentQuoteRecord] = []
        for record in records:
            if not record.quote_id or len(record.image_signatures) != 1:
                continue
            signature = record.image_signatures[0]
            if is_near_duplicate(
                image,
                signature.sha256,
                signature.dhash,
                signature.width,
                signature.height,
            ):
                matches.append(record)

        if not matches:
            return None
        if replied_at > 0:
            bounded = [item for item in matches if item.sent_at <= replied_at]
            if bounded:
                matches = bounded
        matches.sort(key=lambda item: item.sent_at, reverse=True)
        return matches[0].quote_id if matches else None

    async def create_quote_with_segments(
        self,
        session_key: str,
        quote: Quote,
        segments: list[PendingQuoteSegment],
    ) -> CreateQuoteResult:
        store = self.get_store(session_key)
        async with store.lock:
            quotes = store.load_quotes()
            assets = store.load_assets()
            images = [segment.image for segment in segments if segment.type == "image" and segment.image is not None]
            if self._has_duplicate(images, assets):
                return CreateQuoteResult(duplicate=True, message=DUPLICATE_IMAGE_MESSAGE)

            from time import time

            created_assets: list[ImageAsset] = []
            created_files: list[Path] = []
            try:
                persisted_segments: list[QuoteSegment] = []
                for segment in segments:
                    if segment.type == "text":
                        text = str(segment.text or "").strip()
                        if text:
                            persisted_segments.append(QuoteSegment(type="text", text=text))
                        continue

                    image = segment.image
                    if segment.type != "image" or image is None:
                        continue

                    asset = self._persist_image_asset(
                        store,
                        image,
                        created_at=time(),
                        created_files=created_files,
                    )
                    created_assets.append(asset)
                    persisted_segments.append(QuoteSegment(type="image", asset_id=asset.asset_id))

                quote.kind = "standard"
                quote.forward_nodes = []
                quote.segments = persisted_segments
                quote.image_ids = [item.asset_id for item in created_assets]
                quote.media_ids = []
                if not quote.text:
                    quote.text = " ".join(
                        segment.text for segment in persisted_segments if segment.type == "text"
                    ).strip()
                quotes.append(quote)
                assets.extend(created_assets)
                store.save_assets(assets)
                store.save_quotes(quotes)
            except Exception:
                for path in created_files:
                    path.unlink(missing_ok=True)
                raise

        return CreateQuoteResult(quote=quote)

    async def create_quote_with_forward_nodes(
        self,
        session_key: str,
        quote: Quote,
        nodes: list[PendingForwardNode],
    ) -> CreateQuoteResult:
        store = self.get_store(session_key)
        async with store.lock:
            quotes = store.load_quotes()
            image_assets = store.load_assets()
            media_assets = store.load_media_assets()
            images = self._collect_pending_forward_images(nodes)
            if self._has_duplicate(images, image_assets):
                return CreateQuoteResult(duplicate=True, message=DUPLICATE_IMAGE_MESSAGE)

            from time import time

            created_image_assets: list[ImageAsset] = []
            created_media_assets: list[MediaAsset] = []
            created_files: list[Path] = []
            try:
                persisted_nodes, image_ids, media_ids = self._persist_forward_nodes(
                    store,
                    nodes,
                    created_image_assets=created_image_assets,
                    created_media_assets=created_media_assets,
                    created_files=created_files,
                    created_at=time(),
                )
                quote.kind = "forward"
                quote.segments = []
                quote.forward_nodes = persisted_nodes
                quote.image_ids = image_ids
                quote.media_ids = media_ids
                if not quote.text:
                    quote.text = self._flatten_forward_nodes(persisted_nodes)

                quotes.append(quote)
                image_assets.extend(created_image_assets)
                media_assets.extend(created_media_assets)
                store.save_assets(image_assets)
                store.save_media_assets(media_assets)
                store.save_quotes(quotes)
            except Exception:
                for path in created_files:
                    path.unlink(missing_ok=True)
                raise

        return CreateQuoteResult(quote=quote)

    async def random_quote(self, session_key: str | None = None, qq: str | None = None) -> Quote | None:
        import secrets

        candidates: list[Quote] = []
        if session_key is None:
            session_keys = self.session_keys()
        else:
            session_keys = [session_key]

        for key in session_keys:
            for quote in self.get_store(key).load_quotes():
                if qq and str(quote.qq) != str(qq):
                    continue
                candidates.append(quote)
        if not candidates:
            return None
        return secrets.choice(candidates)

    async def delete_quote(self, quote_id: str) -> bool:
        for session_key in self.session_keys():
            store = self.get_store(session_key)
            async with store.lock:
                quotes = store.load_quotes()
                target = next((item for item in quotes if item.id == quote_id), None)
                if target is None:
                    continue

                quotes = [item for item in quotes if item.id != quote_id]
                image_assets = store.load_assets()
                media_assets = store.load_media_assets()
                image_map = {item.asset_id: item for item in image_assets}
                media_map = {item.asset_id: item for item in media_assets}
                for image_id in target.image_ids:
                    asset = image_map.get(image_id)
                    if asset is not None:
                        asset.ref_count = max(0, asset.ref_count - 1)
                for media_id in target.media_ids:
                    asset = media_map.get(media_id)
                    if asset is not None:
                        asset.ref_count = max(0, asset.ref_count - 1)

                kept_image_assets: list[ImageAsset] = []
                for asset in image_assets:
                    if asset.ref_count > 0:
                        kept_image_assets.append(asset)
                        continue
                    (self.root / asset.rel_path).unlink(missing_ok=True)

                kept_media_assets: list[MediaAsset] = []
                for asset in media_assets:
                    if asset.ref_count > 0:
                        kept_media_assets.append(asset)
                        continue
                    (self.root / asset.rel_path).unlink(missing_ok=True)

                store.cache_path(quote_id).unlink(missing_ok=True)
                sent_records = [item for item in store.load_sent_records() if item.quote_id != quote_id]
                store.save_assets(kept_image_assets)
                store.save_media_assets(kept_media_assets)
                store.save_sent_records(sent_records)
                store.save_quotes(quotes)
                return True
        return False

    async def migrate_legacy_data(self) -> bool:
        legacy_quotes = self.root / QUOTES_FILENAME
        if not legacy_quotes.exists():
            return False

        async with self._migration_lock:
            if not legacy_quotes.exists():
                return False

            payload = read_json(legacy_quotes, {"quotes": []})
            legacy_quotes_list = payload.get("quotes") or []
            if not legacy_quotes_list:
                legacy_quotes.rename(legacy_quotes.with_name(legacy_quotes.name + LEGACY_QUOTES_BAK_SUFFIX))
                return False

            buckets: dict[str, dict[str, Any]] = defaultdict(
                lambda: {
                    "quotes": [],
                    "assets": [],
                    "path_map": {},
                }
            )

            for raw_quote in legacy_quotes_list:
                session_key = str(raw_quote.get("group") or "").strip()
                if not session_key:
                    created_by = str(raw_quote.get("created_by") or "unknown")
                    session_key = f"private_{created_by}"
                bucket = buckets[session_key]
                image_ids: list[str] = []
                for raw_path in raw_quote.get("images") or []:
                    normalized = self._resolve_legacy_image_path(str(raw_path))
                    if normalized is None:
                        continue
                    key = str(normalized.resolve())
                    path_map: dict[str, str] = bucket["path_map"]
                    if key in path_map:
                        asset_id = path_map[key]
                        image_ids.append(asset_id)
                        for asset in bucket["assets"]:
                            if asset.asset_id == asset_id:
                                asset.ref_count += 1
                                break
                        continue

                    file_name = self._copy_legacy_image(session_key, normalized)
                    if not file_name:
                        continue
                    copied_path = self.get_store(session_key).image_abs_path(file_name)
                    prepared = prepare_image(copied_path.read_bytes(), source=file_name)
                    asset = ImageAsset(
                        asset_id=random_id("img_"),
                        file_name=file_name,
                        rel_path=rel_image_path(session_key, file_name),
                        sha256=prepared.sha256,
                        dhash=prepared.dhash,
                        width=prepared.width,
                        height=prepared.height,
                        ref_count=1,
                        created_at=float(raw_quote.get("created_at") or 0),
                    )
                    path_map[key] = asset.asset_id
                    bucket["assets"].append(asset)
                    image_ids.append(asset.asset_id)

                bucket["quotes"].append(
                    Quote(
                        id=str(raw_quote.get("id") or random_id()),
                        qq=str(raw_quote.get("qq") or ""),
                        name=str(raw_quote.get("name") or ""),
                        text=str(raw_quote.get("text") or ""),
                        created_by=str(raw_quote.get("created_by") or ""),
                        created_at=float(raw_quote.get("created_at") or 0),
                        group=session_key,
                        image_ids=image_ids,
                    )
                )

            for session_key, bucket in buckets.items():
                store = self.get_store(session_key)
                async with store.lock:
                    quotes = store.load_quotes()
                    assets = store.load_assets()
                    quotes.extend(bucket["quotes"])
                    assets.extend(bucket["assets"])
                    store.save_assets(assets)
                    store.save_quotes(quotes)

            backup_path = legacy_quotes.with_name(legacy_quotes.name + LEGACY_QUOTES_BAK_SUFFIX)
            if backup_path.exists():
                backup_path.unlink()
            legacy_quotes.rename(backup_path)
            logger.info(f"已完成旧语录数据迁移，备份文件: {backup_path}")
            return True

    def _collect_pending_forward_images(self, nodes: list[PendingForwardNode]) -> list[PreparedImage]:
        images: list[PreparedImage] = []
        for node in nodes:
            for segment in node.segments:
                if segment.type == "image" and segment.image is not None:
                    images.append(segment.image)
                elif segment.type == "nodes" and segment.nodes:
                    images.extend(self._collect_pending_forward_images(segment.nodes))
        return images

    def _persist_forward_nodes(
        self,
        store: SessionStore,
        nodes: list[PendingForwardNode],
        *,
        created_image_assets: list[ImageAsset],
        created_media_assets: list[MediaAsset],
        created_files: list[Path],
        created_at: float,
    ) -> tuple[list[ForwardNode], list[str], list[str]]:
        persisted_nodes: list[ForwardNode] = []
        image_ids: list[str] = []
        media_ids: list[str] = []

        for node in nodes:
            persisted_segments: list[ForwardSegment] = []
            for segment in node.segments:
                if segment.type == "text":
                    text = str(segment.text or "").strip()
                    if text:
                        persisted_segments.append(ForwardSegment(type="text", text=text))
                    continue

                if segment.type == "image":
                    if segment.image is None:
                        persisted_segments.append(ForwardSegment(type="text", text="[图片]"))
                        continue
                    asset = self._persist_image_asset(
                        store,
                        segment.image,
                        created_at=created_at,
                        created_files=created_files,
                    )
                    created_image_assets.append(asset)
                    image_ids.append(asset.asset_id)
                    persisted_segments.append(ForwardSegment(type="image", asset_id=asset.asset_id))
                    continue

                if segment.type in {"record", "video", "file"}:
                    if segment.media is None:
                        persisted_segments.append(
                            ForwardSegment(type="text", text=self._placeholder_for_media(segment.type))
                        )
                        continue
                    asset = self._persist_media_asset(
                        store,
                        segment.media,
                        created_at=created_at,
                        created_files=created_files,
                    )
                    created_media_assets.append(asset)
                    media_ids.append(asset.asset_id)
                    persisted_segments.append(ForwardSegment(type=segment.type, asset_id=asset.asset_id))
                    continue

                if segment.type == "face":
                    if segment.face_id:
                        persisted_segments.append(ForwardSegment(type="face", face_id=segment.face_id))
                    continue

                if segment.type == "at":
                    if segment.qq:
                        persisted_segments.append(
                            ForwardSegment(type="at", qq=segment.qq, name=segment.name)
                        )
                    continue

                if segment.type == "nodes":
                    nested_nodes, nested_image_ids, nested_media_ids = self._persist_forward_nodes(
                        store,
                        segment.nodes,
                        created_image_assets=created_image_assets,
                        created_media_assets=created_media_assets,
                        created_files=created_files,
                        created_at=created_at,
                    )
                    if nested_nodes:
                        image_ids.extend(nested_image_ids)
                        media_ids.extend(nested_media_ids)
                        persisted_segments.append(ForwardSegment(type="nodes", nodes=nested_nodes))
                    else:
                        persisted_segments.append(ForwardSegment(type="text", text="[聊天记录]"))
                    continue

                placeholder = self._placeholder_for_unknown(segment.type)
                if placeholder:
                    persisted_segments.append(ForwardSegment(type="text", text=placeholder))

            if not persisted_segments:
                persisted_segments.append(ForwardSegment(type="text", text="[空消息]"))

            persisted_nodes.append(
                ForwardNode(
                    sender_uin=str(node.sender_uin or ""),
                    sender_name=str(node.sender_name or node.sender_uin or "未知用户"),
                    segments=persisted_segments,
                )
            )

        return persisted_nodes, image_ids, media_ids

    def _persist_image_asset(
        self,
        store: SessionStore,
        image: PreparedImage,
        *,
        created_at: float,
        created_files: list[Path],
    ) -> ImageAsset:
        file_name = f"{random_id()}{image.extension}"
        abs_path = store.image_abs_path(file_name)
        abs_path.write_bytes(image.content)
        created_files.append(abs_path)
        return ImageAsset(
            asset_id=random_id("img_"),
            file_name=file_name,
            rel_path=rel_image_path(store.session_key, file_name),
            sha256=image.sha256,
            dhash=image.dhash,
            width=image.width,
            height=image.height,
            ref_count=1,
            created_at=created_at,
        )

    def _persist_media_asset(
        self,
        store: SessionStore,
        media: PreparedMedia,
        *,
        created_at: float,
        created_files: list[Path],
    ) -> MediaAsset:
        file_name = f"{random_id()}{media.extension}"
        abs_path = store.media_abs_path(file_name)
        abs_path.write_bytes(media.content)
        created_files.append(abs_path)
        return MediaAsset(
            asset_id=random_id("media_"),
            media_type=media.media_type,
            file_name=file_name,
            rel_path=rel_media_path(store.session_key, file_name),
            display_name=media.display_name or file_name,
            ref_count=1,
            created_at=created_at,
        )

    def _flatten_forward_nodes(self, nodes: list[ForwardNode]) -> str:
        lines: list[str] = []
        for node in nodes:
            parts: list[str] = []
            for segment in node.segments:
                if segment.type == "text" and segment.text:
                    parts.append(segment.text)
                elif segment.type == "image":
                    parts.append("[图片]")
                elif segment.type == "record":
                    parts.append("[语音]")
                elif segment.type == "video":
                    parts.append("[视频]")
                elif segment.type == "file":
                    parts.append("[文件]")
                elif segment.type == "face" and segment.face_id:
                    parts.append("[表情]")
                elif segment.type == "at" and segment.qq:
                    parts.append(f"@{segment.name or segment.qq}")
                elif segment.type == "nodes":
                    parts.append("[聊天记录]")
            content = "".join(parts).strip()
            if content:
                lines.append(f"{node.sender_name or node.sender_uin or '未知用户'}：{content}")
        return "\n".join(lines).strip()

    def _has_duplicate(self, images: list[PreparedImage], existing_assets: list[ImageAsset]) -> bool:
        if not images:
            return False

        for index, current in enumerate(images):
            for previous in images[:index]:
                if is_near_duplicate(
                    current,
                    previous.sha256,
                    previous.dhash,
                    previous.width,
                    previous.height,
                ):
                    return True

            for asset in existing_assets:
                if is_near_duplicate(
                    current,
                    asset.sha256,
                    asset.dhash,
                    asset.width,
                    asset.height,
                ):
                    return True
        return False

    def _resolve_legacy_image_path(self, raw_path: str) -> Path | None:
        if not raw_path:
            return None
        path = Path(raw_path)
        if path.is_absolute() and path.exists():
            return path
        fixed = raw_path
        if fixed.startswith("quotes/"):
            fixed = fixed.split("/", 1)[1] if "/" in fixed else fixed
        candidate = self.root / fixed
        if candidate.exists():
            return candidate
        return None

    def _copy_legacy_image(self, session_key: str, source: Path) -> str | None:
        if not source.exists():
            return None
        target_store = self.get_store(session_key)
        base_name = source.name
        target = target_store.image_abs_path(base_name)
        if target.exists():
            target = target_store.image_abs_path(f"{source.stem}_{random_id()}{source.suffix}")
        shutil.copy2(source, target)
        return target.name

    def _placeholder_for_media(self, media_type: str) -> str:
        return {
            "record": "[语音]",
            "video": "[视频]",
            "file": "[文件]",
        }.get(media_type, "[附件]")

    def _placeholder_for_unknown(self, seg_type: str) -> str:
        if not seg_type:
            return ""
        return f"[{seg_type}]"
