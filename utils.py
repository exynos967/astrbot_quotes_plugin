from __future__ import annotations

import hashlib
import json
import re
import secrets
from pathlib import Path
from typing import Any, Callable

from PIL import Image, ImageChops, ImageFilter, ImageOps

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
        DEFAULT_COLOR_HASH_SIZE,
        DEFAULT_COLOR_HASH_THRESHOLD,
        DEFAULT_DHASH_SIZE,
        DEFAULT_DHASH_THRESHOLD,
        DEFAULT_EXTENDED_DHASH_SIZE,
        DEFAULT_EXTENDED_DHASH_THRESHOLD,
        DEFAULT_PIXEL_MAX_BLOCK_DIFF_THRESHOLD,
        DEFAULT_PIXEL_MAX_CELL_DIFF_THRESHOLD,
        DEFAULT_PIXEL_MEAN_DIFF_THRESHOLD,
        DEFAULT_ROW_DHASH_THRESHOLD,
        GROUPS_DIRNAME,
        MEDIA_DIRNAME,
        PLUGIN_NAME,
    )
    from .models import PreparedImage, PreparedMedia
except ImportError:  # pragma: no cover
    from constants import (
        DEFAULT_ASPECT_RATIO_TOLERANCE,
        DEFAULT_COLOR_HASH_SIZE,
        DEFAULT_COLOR_HASH_THRESHOLD,
        DEFAULT_DHASH_SIZE,
        DEFAULT_DHASH_THRESHOLD,
        DEFAULT_EXTENDED_DHASH_SIZE,
        DEFAULT_EXTENDED_DHASH_THRESHOLD,
        DEFAULT_PIXEL_MAX_BLOCK_DIFF_THRESHOLD,
        DEFAULT_PIXEL_MAX_CELL_DIFF_THRESHOLD,
        DEFAULT_PIXEL_MEAN_DIFF_THRESHOLD,
        DEFAULT_ROW_DHASH_THRESHOLD,
        GROUPS_DIRNAME,
        MEDIA_DIRNAME,
        PLUGIN_NAME,
    )
    from models import PreparedImage, PreparedMedia


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


def resolve_wake_prefixes(config_obj: Any) -> tuple[str, ...]:
    default_prefixes = ("/", "#", "=", "!", "／", "！")
    if config_obj is None:
        return default_prefixes

    wake_prefix: Any = None
    if hasattr(config_obj, "get"):
        try:
            wake_prefix = config_obj.get("wake_prefix")
        except Exception:
            wake_prefix = None

    if wake_prefix is None and isinstance(config_obj, dict):
        wake_prefix = config_obj.get("wake_prefix")

    if wake_prefix is None:
        return default_prefixes

    if isinstance(wake_prefix, str):
        prefixes = [wake_prefix]
    elif isinstance(wake_prefix, (list, tuple, set)):
        prefixes = [str(item) for item in wake_prefix if str(item)]
    else:
        return default_prefixes

    merged: list[str] = []
    seen: set[str] = set()
    for prefix in [*prefixes, *default_prefixes]:
        value = str(prefix or "")
        if not value or value in seen:
            continue
        merged.append(value)
        seen.add(value)
    return tuple(merged)


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


def guess_media_extension(source_name: str = "", content_type: str = "", media_type: str = "") -> str:
    if source_name:
        suffix = Path(source_name).suffix.lower()
        if 0 < len(suffix) <= 10:
            return suffix

    lowered_type = (content_type or "").lower()
    mappings = {
        "audio/wav": ".wav",
        "audio/x-wav": ".wav",
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/ogg": ".ogg",
        "audio/amr": ".amr",
        "audio/aac": ".aac",
        "video/mp4": ".mp4",
        "video/webm": ".webm",
        "video/x-msvideo": ".avi",
        "application/pdf": ".pdf",
        "application/zip": ".zip",
        "application/json": ".json",
        "text/plain": ".txt",
    }
    for key, extension in mappings.items():
        if key in lowered_type:
            return extension

    default_map = {
        "record": ".wav",
        "video": ".mp4",
        "file": ".bin",
    }
    return default_map.get(str(media_type or "").lower(), ".bin")


def _load_normalized_image(content: bytes) -> Image.Image:
    from io import BytesIO

    with Image.open(BytesIO(content)) as img:
        if getattr(img, "is_animated", False):
            img.seek(0)
        img = ImageOps.exif_transpose(img)
        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            rgba = img.convert("RGBA")
            bg = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
            return Image.alpha_composite(bg, rgba).convert("RGB")
        return img.convert("RGB")


def _compute_h_dhash_hex(gray: Image.Image, hash_size: int) -> str:
    resized = gray.resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
    pixels = resized.tobytes()
    row_width = hash_size + 1
    value = 0
    for row in range(hash_size):
        row_offset = row * row_width
        for col in range(hash_size):
            value = (value << 1) | (1 if pixels[row_offset + col] > pixels[row_offset + col + 1] else 0)
    return f"{value:0{hash_size * hash_size // 4}x}"


def _compute_v_dhash_hex(gray: Image.Image, hash_size: int) -> str:
    resized = gray.resize((hash_size, hash_size + 1), Image.Resampling.LANCZOS)
    pixels = resized.tobytes()
    value = 0
    for row in range(hash_size):
        row_offset = row * hash_size
        next_offset = (row + 1) * hash_size
        for col in range(hash_size):
            value = (value << 1) | (1 if pixels[row_offset + col] > pixels[next_offset + col] else 0)
    return f"{value:0{hash_size * hash_size // 4}x}"


def _compute_color_hash_hex(rgb: Image.Image, hash_size: int = DEFAULT_COLOR_HASH_SIZE) -> str:
    small = rgb.resize((hash_size, hash_size), Image.Resampling.BOX)
    raw = small.tobytes()
    rg_bits = 0
    bg_bits = 0
    for idx in range(0, len(raw), 3):
        r, g, b = raw[idx], raw[idx + 1], raw[idx + 2]
        rg_bits = (rg_bits << 1) | (1 if (r - g) > 8 else 0)
        bg_bits = (bg_bits << 1) | (1 if (b - g) > 8 else 0)
    hex_len = hash_size * hash_size // 4
    return f"{rg_bits:0{hex_len}x}{bg_bits:0{hex_len}x}"


def compute_dhash(content: bytes, hash_size: int = DEFAULT_DHASH_SIZE) -> tuple[str, int, int]:
    try:
        rgb = _load_normalized_image(content)
        width, height = rgb.size
        gray = rgb.convert("L")
        base_hex = _compute_h_dhash_hex(gray, hash_size)
        if hash_size != DEFAULT_DHASH_SIZE:
            return base_hex, width, height

        ext_h = _compute_h_dhash_hex(gray, DEFAULT_EXTENDED_DHASH_SIZE)
        ext_v = _compute_v_dhash_hex(gray, DEFAULT_EXTENDED_DHASH_SIZE)
        color_hex = _compute_color_hash_hex(rgb, DEFAULT_COLOR_HASH_SIZE)
        return f"{base_hex}{ext_h}{ext_v}{color_hex}", width, height
    except Exception:
        return "", 0, 0


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


def prepare_media(
    content: bytes,
    *,
    media_type: str,
    source: str = "",
    content_type: str = "",
    display_name: str = "",
) -> PreparedMedia:
    extension = guess_media_extension(
        source_name=display_name or source,
        content_type=content_type,
        media_type=media_type,
    )
    return PreparedMedia(
        content=content,
        extension=extension,
        media_type=media_type,
        source=source,
        display_name=display_name or Path(source).name or f"{media_type}{extension}",
    )


def hamming_distance(left_hex: str, right_hex: str) -> int:
    if not left_hex or not right_hex:
        return 10**9
    common_len = min(len(left_hex), len(right_hex))
    if common_len <= 0:
        return 10**9
    return (int(left_hex[:common_len], 16) ^ int(right_hex[:common_len], 16)).bit_count()


def _is_dhash_compatible(
    left_dhash: str,
    right_dhash: str,
    *,
    dhash_threshold: int = DEFAULT_DHASH_THRESHOLD,
) -> bool:
    if not left_dhash or not right_dhash:
        return False
    base_len = DEFAULT_DHASH_SIZE * DEFAULT_DHASH_SIZE // 4
    if len(left_dhash) < base_len or len(right_dhash) < base_len:
        return hamming_distance(left_dhash, right_dhash) <= dhash_threshold

    if hamming_distance(left_dhash[:base_len], right_dhash[:base_len]) > dhash_threshold:
        return False

    ext_len = DEFAULT_EXTENDED_DHASH_SIZE * DEFAULT_EXTENDED_DHASH_SIZE // 4
    color_len = (DEFAULT_COLOR_HASH_SIZE * DEFAULT_COLOR_HASH_SIZE // 4) * 2
    full_len = base_len + ext_len * 2 + color_len
    if len(left_dhash) >= full_len and len(right_dhash) >= full_len:
        h16_left = left_dhash[base_len : base_len + ext_len]
        h16_right = right_dhash[base_len : base_len + ext_len]
        v16_left = left_dhash[base_len + ext_len : base_len + ext_len * 2]
        v16_right = right_dhash[base_len + ext_len : base_len + ext_len * 2]
        col_left = left_dhash[base_len + ext_len * 2 : full_len]
        col_right = right_dhash[base_len + ext_len * 2 : full_len]

        h16_dist = hamming_distance(h16_left, h16_right)
        v16_dist = hamming_distance(v16_left, v16_right)
        if h16_dist + v16_dist > DEFAULT_EXTENDED_DHASH_THRESHOLD:
            return False

        row_hex_step = DEFAULT_EXTENDED_DHASH_SIZE // 4
        max_row_flips = max(
            hamming_distance(h16_left[idx : idx + row_hex_step], h16_right[idx : idx + row_hex_step])
            for idx in range(0, ext_len, row_hex_step)
        )
        if max_row_flips > DEFAULT_ROW_DHASH_THRESHOLD:
            return False

        max_col_flips = max(
            hamming_distance(v16_left[idx : idx + row_hex_step], v16_right[idx : idx + row_hex_step])
            for idx in range(0, ext_len, row_hex_step)
        )
        if max_col_flips > DEFAULT_ROW_DHASH_THRESHOLD:
            return False

        if hamming_distance(col_left, col_right) > DEFAULT_COLOR_HASH_THRESHOLD:
            return False

    return True


def _crop_foreground_pair(img1: Image.Image, img2: Image.Image) -> tuple[Image.Image, Image.Image]:
    w, h = img1.size
    bg_color = img1.getpixel((0, 0))
    bg1 = (
        ImageChops.difference(img1, Image.new("RGB", (w, h), bg_color))
        .convert("L")
        .point(lambda p: 255 if p > 8 else 0)
    )
    bg2 = (
        ImageChops.difference(img2, Image.new("RGB", (w, h), bg_color))
        .convert("L")
        .point(lambda p: 255 if p > 8 else 0)
    )
    bbox = ImageChops.lighter(bg1, bg2).getbbox()
    if bbox:
        bw = bbox[2] - bbox[0]
        bh = bbox[3] - bbox[1]
        if bw >= 32 and bh >= 32 and (bw * bh) < (w * h * 0.75):
            return img1.crop(bbox), img2.crop(bbox)
    return img1, img2


def _max_pooled_diff(pooled: Image.Image) -> float:
    raw = pooled.tobytes()
    if not raw:
        return 0.0
    return max((raw[i] + raw[i + 1] + raw[i + 2]) / 3.0 for i in range(0, len(raw), 3))


def verify_image_similarity(
    left_content: bytes,
    right_content: bytes,
    *,
    max_mean_diff: float = DEFAULT_PIXEL_MEAN_DIFF_THRESHOLD,
    max_cell_diff: float = DEFAULT_PIXEL_MAX_CELL_DIFF_THRESHOLD,
    max_block_diff: float = DEFAULT_PIXEL_MAX_BLOCK_DIFF_THRESHOLD,
) -> bool:
    if not left_content or not right_content:
        return False
    try:
        img1 = _load_normalized_image(left_content)
        img2 = _load_normalized_image(right_content)
    except Exception:
        return False

    # 1. 32x32 网格均值与峰值校验，拦截全局结构或颜色差异
    g1 = img1.resize((32, 32), Image.Resampling.BOX)
    g2 = img2.resize((32, 32), Image.Resampling.BOX)
    cell_raw = ImageChops.difference(g1, g2).tobytes()
    cell_vals = [(cell_raw[i] + cell_raw[i + 1] + cell_raw[i + 2]) / 3.0 for i in range(0, len(cell_raw), 3)]
    if not cell_vals:
        return False
    if (sum(cell_vals) / len(cell_vals)) > max_mean_diff or max(cell_vals) > max_cell_diff:
        return False

    # 2. 对齐到较小分辨率后剥离纯色背景，按 8x8 像素分块做刚性对齐局部峰值校验，拦截同底色不同字的截图/表情包
    same_size = img1.size == img2.size
    min_w = min(img1.width, img2.width)
    min_h = min(img1.height, img2.height)
    scale = min(1.0, 640.0 / max(min_w, min_h))
    w = max(32, int(round(min_w * scale)))
    h = max(32, int(round(min_h * scale)))
    s1 = img1.resize((w, h), Image.Resampling.LANCZOS)
    s2 = img2.resize((w, h), Image.Resampling.LANCZOS)
    c1, c2 = _crop_foreground_pair(s1, s2)
    b1 = c1.filter(ImageFilter.GaussianBlur(radius=0.8))
    b2 = c2.filter(ImageFilter.GaussianBlur(radius=0.8))
    gw = max(8, c1.width // 8)
    gh = max(8, c1.height // 8)

    zero_shift_pooled = ImageChops.difference(b1, b2).resize((gw, gh), Image.Resampling.BOX)
    if _max_pooled_diff(zero_shift_pooled) <= max_block_diff:
        return True
    if same_size:
        return False

    best_pooled = zero_shift_pooled
    cw, ch = b1.size
    bg_pixel = b2.getpixel((0, 0))
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            if dx == 0 and dy == 0:
                continue
            shifted = Image.new("RGB", (cw, ch), bg_pixel)
            src_x0 = max(0, -dx)
            src_y0 = max(0, -dy)
            src_x1 = min(cw, cw - dx)
            src_y1 = min(ch, ch - dy)
            shifted.paste(b2.crop((src_x0, src_y0, src_x1, src_y1)), (max(0, dx), max(0, dy)))
            pooled = ImageChops.difference(b1, shifted).resize((gw, gh), Image.Resampling.BOX)
            best_pooled = ImageChops.darker(best_pooled, pooled)

    return _max_pooled_diff(best_pooled) <= max_block_diff


def is_near_duplicate(
    left: PreparedImage,
    right_sha256: str,
    right_dhash: str,
    right_width: int,
    right_height: int,
    *,
    right_content: bytes | None = None,
    right_content_loader: Callable[[], bytes | None] | None = None,
    dhash_threshold: int = DEFAULT_DHASH_THRESHOLD,
    aspect_ratio_tolerance: float = DEFAULT_ASPECT_RATIO_TOLERANCE,
) -> bool:
    if left.sha256 and left.sha256 == right_sha256:
        return True
    if not left.dhash or not right_dhash:
        return False

    left_ratio = left.aspect_ratio
    if not left_ratio or right_width <= 0 or right_height <= 0:
        return False
    right_ratio = right_width / right_height
    if right_ratio <= 0:
        return False
    ratio_diff = abs(left_ratio - right_ratio) / max(left_ratio, right_ratio)
    if ratio_diff > aspect_ratio_tolerance:
        return False

    if not _is_dhash_compatible(left.dhash, right_dhash, dhash_threshold=dhash_threshold):
        return False

    if right_content is None and right_content_loader is not None:
        right_content = right_content_loader()
    if right_content is not None and left.content:
        return verify_image_similarity(left.content, right_content)
    return True


def random_id(prefix: str = "") -> str:
    from time import time

    value = f"{int(time() * 1000)}_{secrets.token_hex(4)}"
    return f"{prefix}{value}" if prefix else value


def rel_image_path(session_key: str, file_name: str) -> str:
    return f"{GROUPS_DIRNAME}/{session_key}/images/{file_name}"


def rel_media_path(session_key: str, file_name: str) -> str:
    return f"{GROUPS_DIRNAME}/{session_key}/{MEDIA_DIRNAME}/{file_name}"


_CQ_SEGMENT_PATTERN = re.compile(r"\[CQ:([A-Za-z0-9_-]+)((?:,[^\[\]]*)?)\]")


def _cq_unescape(value: str) -> str:
    return (
        str(value)
        .replace("&#91;", "[")
        .replace("&#93;", "]")
        .replace("&#44;", ",")
        .replace("&amp;", "&")
    )


def _parse_cq_via_aiocqhttp(raw: str) -> list[dict[str, Any]] | None:
    # 借助框架自带的 aiocqhttp.message.Message 解析 CQ 码；不可用或解析为空时返回 None
    try:
        from aiocqhttp.message import Message

        segments = [
            {"type": str(segment.type).lower(), "data": dict(segment.data or {})}
            for segment in Message(raw)
            if getattr(segment, "type", None)
        ]
        return segments or None
    except Exception:
        return None


def parse_cq_message(raw: str) -> list[dict[str, Any]]:
    # 把 CQ 码字符串消息解析为 OneBot 段列表，兼容 message_format=string 的实现（如 go-cqhttp 默认）
    raw = str(raw or "")
    if not raw:
        return []
    if "[CQ:" not in raw:
        return [{"type": "text", "data": {"text": _cq_unescape(raw)}}]

    # 优先复用框架依赖 aiocqhttp 的解析，失败再回落到内置简化解析
    framework_segments = _parse_cq_via_aiocqhttp(raw)
    if framework_segments is not None:
        return framework_segments

    segments: list[dict[str, Any]] = []
    pos = 0
    for match in _CQ_SEGMENT_PATTERN.finditer(raw):
        if match.start() > pos:
            text = _cq_unescape(raw[pos : match.start()])
            if text:
                segments.append({"type": "text", "data": {"text": text}})
        data: dict[str, Any] = {}
        params = (match.group(2) or "").lstrip(",")
        for item in params.split(",") if params else []:
            key, sep, value = item.partition("=")
            if sep:
                data[key] = _cq_unescape(value)
        segments.append({"type": match.group(1).lower(), "data": data})
        pos = match.end()
    if pos < len(raw):
        text = _cq_unescape(raw[pos:])
        if text:
            segments.append({"type": "text", "data": {"text": text}})
    return segments
