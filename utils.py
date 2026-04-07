from __future__ import annotations

import hashlib
import json
import re
import secrets
from pathlib import Path
from typing import Any

from PIL import Image

try:
    from astrbot.api import logger
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)

try:
    from astrbot.core.star.star_tools import StarTools
except Exception:  # pragma: no cover
    StarTools = None  # type: ignore

try:
    from .constants import (
        DEFAULT_ASPECT_RATIO_TOLERANCE,
        DEFAULT_DHASH_SIZE,
        DEFAULT_DHASH_THRESHOLD,
        GROUPS_DIRNAME,
        PLUGIN_NAME,
    )
    from .models import PreparedImage
except ImportError:  # pragma: no cover
    from constants import (
        DEFAULT_ASPECT_RATIO_TOLERANCE,
        DEFAULT_DHASH_SIZE,
        DEFAULT_DHASH_THRESHOLD,
        GROUPS_DIRNAME,
        PLUGIN_NAME,
    )
    from models import PreparedImage


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - 仅兜底日志
        logger.error(f"读取 JSON 失败 {path}: {exc}")
        return default


def ensure_plugin_data_dir(storage: str | None = None, plugin_name: str = PLUGIN_NAME) -> Path:
    if storage and storage.strip():
        root = Path(storage).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        return root.resolve()

    if StarTools is not None:
        try:
            return StarTools.get_data_dir(plugin_name)
        except Exception as exc:  # pragma: no cover - 兼容旧版 AstrBot
            logger.info(f"StarTools.get_data_dir 回退到 cwd 方案: {exc}")

    root = Path.cwd() / "data" / "plugin_data" / plugin_name
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def make_session_key(group_id: str | None, sender_id: str | None) -> str:
    gid = str(group_id or "").strip()
    if gid:
        return gid
    return f"private_{str(sender_id or '').strip()}"


def is_valid_qq(value: str | None) -> bool:
    value = str(value or "").strip()
    return value.isdigit() and len(value) >= 5


def normalize_quote_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"@[^@\s（）()]+(?:[（(]\d{5,}[）)])?", "", text)
    text = text.replace("@全体成员", "")
    return " ".join(text.split())


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def guess_extension(source_name: str = "", content_type: str = "") -> str:
    lowered_type = (content_type or "").lower()
    if "png" in lowered_type:
        return ".png"
    if "webp" in lowered_type:
        return ".webp"
    if "gif" in lowered_type:
        return ".gif"
    if "bmp" in lowered_type:
        return ".bmp"
    if source_name:
        suffix = Path(source_name).suffix.lower()
        if 0 < len(suffix) <= 10:
            return suffix
    return ".jpg"


def compute_dhash(content: bytes, hash_size: int = DEFAULT_DHASH_SIZE) -> tuple[str, int, int]:
    try:
        from io import BytesIO

        with Image.open(BytesIO(content)) as img:
            width, height = img.size
            if getattr(img, "is_animated", False):
                img.seek(0)
            gray = img.convert("L").resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
            pixels = list(gray.getdata())
    except Exception:
        return "", 0, 0

    bits: list[str] = []
    row_width = hash_size + 1
    for row in range(hash_size):
        row_offset = row * row_width
        for col in range(hash_size):
            left = pixels[row_offset + col]
            right = pixels[row_offset + col + 1]
            bits.append("1" if left > right else "0")

    return f"{int(''.join(bits), 2):0{hash_size * hash_size // 4}x}", width, height


def prepare_image(content: bytes, *, source: str = "", content_type: str = "") -> PreparedImage:
    extension = guess_extension(source_name=source, content_type=content_type)
    dhash, width, height = compute_dhash(content)
    return PreparedImage(
        content=content,
        extension=extension,
        sha256=sha256_bytes(content),
        source=source,
        dhash=dhash,
        width=width,
        height=height,
    )


def hamming_distance(left_hex: str, right_hex: str) -> int:
    if not left_hex or not right_hex:
        return 10**9
    return (int(left_hex, 16) ^ int(right_hex, 16)).bit_count()


def is_near_duplicate(
    left: PreparedImage,
    right_sha256: str,
    right_dhash: str,
    right_width: int,
    right_height: int,
    *,
    dhash_threshold: int = DEFAULT_DHASH_THRESHOLD,
    aspect_ratio_tolerance: float = DEFAULT_ASPECT_RATIO_TOLERANCE,
) -> bool:
    if left.sha256 and left.sha256 == right_sha256:
        return True
    if not left.dhash or not right_dhash:
        return False
    if hamming_distance(left.dhash, right_dhash) > dhash_threshold:
        return False

    left_ratio = left.aspect_ratio
    if not left_ratio or right_width <= 0 or right_height <= 0:
        return False
    right_ratio = right_width / right_height
    if right_ratio <= 0:
        return False
    ratio_diff = abs(left_ratio - right_ratio) / max(left_ratio, right_ratio)
    return ratio_diff <= aspect_ratio_tolerance


def random_id(prefix: str = "") -> str:
    from time import time

    value = f"{int(time() * 1000)}_{secrets.token_hex(4)}"
    return f"{prefix}{value}" if prefix else value


def rel_image_path(session_key: str, file_name: str) -> str:
    return f"{GROUPS_DIRNAME}/{session_key}/images/{file_name}"
