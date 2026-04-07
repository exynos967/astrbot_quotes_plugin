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
        QUOTES_FILENAME,
        SCHEMA_VERSION,
    )
    from .models import ImageAsset, PreparedImage, Quote
    from .utils import (
        atomic_write_json,
        is_near_duplicate,
        prepare_image,
        random_id,
        read_json,
        rel_image_path,
    )
except ImportError:  # pragma: no cover
    from constants import (
        CACHE_DIRNAME,
        DUPLICATE_IMAGE_MESSAGE,
        GROUPS_DIRNAME,
        IMAGE_INDEX_FILENAME,
        IMAGES_DIRNAME,
        LEGACY_QUOTES_BAK_SUFFIX,
        QUOTES_FILENAME,
        SCHEMA_VERSION,
    )
    from models import ImageAsset, PreparedImage, Quote
    from utils import (
        atomic_write_json,
        is_near_duplicate,
        prepare_image,
        random_id,
        read_json,
        rel_image_path,
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
        self.cache_dir = self.root / CACHE_DIRNAME
        self.quotes_file = self.root / QUOTES_FILENAME
        self.image_index_file = self.root / IMAGE_INDEX_FILENAME
        self.lock = asyncio.Lock()
        self.root.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)
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

    def image_abs_path(self, file_name: str) -> Path:
        return self.images_dir / file_name

    def cache_path(self, quote_id: str) -> Path:
        return self.cache_dir / f"{quote_id}.png"


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

    async def create_quote_with_images(
        self,
        session_key: str,
        quote: Quote,
        images: list[PreparedImage],
    ) -> CreateQuoteResult:
        store = self.get_store(session_key)
        async with store.lock:
            quotes = store.load_quotes()
            assets = store.load_assets()
            if self._has_duplicate(images, assets):
                return CreateQuoteResult(duplicate=True, message=DUPLICATE_IMAGE_MESSAGE)

            from time import time

            created_assets: list[ImageAsset] = []
            created_files: list[Path] = []
            try:
                for image in images:
                    file_name = f"{random_id()}{image.extension}"
                    abs_path = store.image_abs_path(file_name)
                    abs_path.write_bytes(image.content)
                    created_files.append(abs_path)
                    created_assets.append(
                        ImageAsset(
                            asset_id=random_id("img_"),
                            file_name=file_name,
                            rel_path=rel_image_path(session_key, file_name),
                            sha256=image.sha256,
                            dhash=image.dhash,
                            width=image.width,
                            height=image.height,
                            ref_count=1,
                            created_at=time(),
                        )
                    )

                quote.image_ids = [item.asset_id for item in created_assets]
                quotes.append(quote)
                assets.extend(created_assets)
                store.save_assets(assets)
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
                assets = store.load_assets()
                asset_map = {item.asset_id: item for item in assets}
                for image_id in target.image_ids:
                    asset = asset_map.get(image_id)
                    if asset is None:
                        continue
                    asset.ref_count = max(0, asset.ref_count - 1)

                kept_assets: list[ImageAsset] = []
                for asset in assets:
                    if asset.ref_count > 0:
                        kept_assets.append(asset)
                        continue
                    abs_path = self.root / asset.rel_path
                    abs_path.unlink(missing_ok=True)

                store.cache_path(quote_id).unlink(missing_ok=True)
                store.save_assets(kept_assets)
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
