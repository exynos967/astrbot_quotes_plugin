from __future__ import annotations

import base64
import binascii
import html
import re
from pathlib import Path
from typing import Any, Awaitable, Callable

try:
    from .models import Quote
    from .utils import normalize_quote_text
except ImportError:  # pragma: no cover
    from models import Quote
    from utils import normalize_quote_text


DEFAULT_FONT_FAMILY = (
    "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB', "
    "'Microsoft YaHei', 'WenQuanYi Micro Hei', Arial, sans-serif"
)

STYLE_DEFAULTS: dict[str, Any] = {
    "width": 1280,
    "height": 427,
    "bg_color": "#000",
    "text_color": "#fff",
    "font_family": DEFAULT_FONT_FAMILY,
    "font_scale": 100,
    "bg_image": "",
    "bg_image_overlay": 45,
}


def _to_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(str(value).strip()))
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


_COLOR_PATTERN = re.compile(
    r"^(?:#(?:[0-9a-fA-F]{3,4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})"
    r"|rgba?\([0-9.,%\s]{3,40}\)"
    r"|hsla?\([0-9.,%\s]{3,40}\))$"
)
_BG_IMAGE_MAX_BYTES = 8 * 1024 * 1024
_MIME_BY_SUFFIX = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
}


def _normalize_color(value: Any, default: str) -> str:
    text = str(value or "").strip()
    if not text:
        return default
    # 只接受合法的 #hex / rgb() / hsl()，避免把任意字符串拼进 CSS。
    return text if _COLOR_PATTERN.match(text) else default


def normalize_bg_image(value: Any) -> str:
    """把本地绝对路径转成 file:// URL，其余（http/data/file）原样返回。"""
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered.startswith(("http://", "https://", "data:", "file://")):
        return text
    try:
        path = Path(text)
        if path.exists():
            return path.resolve().as_uri()
    except OSError:
        return ""
    return ""


def _file_uri_to_path(uri: str) -> Path | None:
    from urllib.parse import unquote, urlparse

    try:
        parsed = urlparse(uri)
    except ValueError:
        return None
    candidate = unquote(parsed.path or "")
    # Windows: file:///F:/x -> /F:/x
    if len(candidate) > 2 and candidate[0] == "/" and candidate[2] == ":":
        candidate = candidate[1:]
    return Path(candidate) if candidate else None


def inline_bg_image(value: Any) -> str:
    """把本地背景图转成 data URL。

    渲染在远端 t2i 服务上执行，那里读不到本机的 file:// 路径，
    所以本地文件必须在拼模板前内联成 data URL，否则背景图会被静默忽略。
    """
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered.startswith(("http://", "https://", "data:")):
        return text

    path: Path | None
    if lowered.startswith("file://"):
        path = _file_uri_to_path(text)
    else:
        path = Path(text)
    if path is None or not path.is_file():
        return ""

    try:
        size = path.stat().st_size
    except OSError:
        return ""
    if size <= 0 or size > _BG_IMAGE_MAX_BYTES:
        return ""

    try:
        raw = path.read_bytes()
    except OSError:
        return ""

    mime = _MIME_BY_SUFFIX.get(path.suffix.lower(), "image/jpeg")
    try:
        return f"data:{mime};base64," + base64.b64encode(raw).decode("ascii")
    except (binascii.Error, ValueError):
        return ""


def resolve_style(base: dict[str, Any] | None, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """合并默认值 + 基础样式 + 覆盖项，产出可直接用于渲染的样式字典。"""
    merged: dict[str, Any] = dict(STYLE_DEFAULTS)
    for source in (base or {}, overrides or {}):
        if not isinstance(source, dict):
            continue
        merged.update({k: v for k, v in source.items() if v is not None})

    style = {
        "width": _to_int(merged.get("width"), 1280, 320, 4096),
        "height": _to_int(merged.get("height"), 427, 160, 4096),
        "bg_color": _normalize_color(merged.get("bg_color"), "#000"),
        "text_color": _normalize_color(merged.get("text_color"), "#fff"),
        "font_family": str(merged.get("font_family") or DEFAULT_FONT_FAMILY),
        "font_scale": _to_int(merged.get("font_scale"), 100, 50, 200),
        "bg_image": normalize_bg_image(merged.get("bg_image")),
        "bg_image_overlay": _to_int(merged.get("bg_image_overlay"), 45, 0, 95),
    }
    return style


class QuoteRenderer:
    MAX_RENDER_TEXT_LENGTH = 260
    PLAIN_FALLBACK_TEXT_LENGTH = 1000

    def __init__(
        self,
        html_render: Callable[[str, dict[str, Any], bool, dict[str, Any] | None], Awaitable[str]],
        image_config: dict[str, Any],
    ):
        self._html_render = html_render
        self.image_config = image_config or {}

    async def _render_template(
        self,
        template: str,
        data: dict[str, Any],
        options: dict[str, Any] | None = None,
        return_url: bool = True,
    ) -> str:
        return await self._html_render(
            template,
            data,
            return_url,
            options,
        )

    async def warmup(self) -> None:
        minimal = '<div style="width:320px;height:120px;background:#000;color:#fff">init</div>'
        try:
            await self._render_template(
                minimal,
                {},
                {"full_page": False, "clip": {"x": 0, "y": 0, "width": 320, "height": 120}},
            )
        except Exception:
            return

    def should_fallback_to_plain(self, quote: Quote) -> bool:
        if quote.kind != "standard":
            return False
        return len(normalize_quote_text(quote.text)) > self.PLAIN_FALLBACK_TEXT_LENGTH

    def _resolve_text_layout(self, text: str) -> dict[str, Any]:
        normalized = normalize_quote_text(text)
        if len(normalized) > self.MAX_RENDER_TEXT_LENGTH:
            clipped = normalized[: self.MAX_RENDER_TEXT_LENGTH].rstrip()
            clipped = clipped.rstrip("，。！？；：、,.!?;: ")
            normalized = f"{clipped}……" if clipped else "……"

        length = len(normalized)
        if length <= 48:
            return {
                "text": normalized,
                "font_size": 38,
                "line_height": 1.6,
                "padding_x": 80,
                "signature_size": 22,
                "quote_gap": 14,
            }
        if length <= 100:
            return {
                "text": normalized,
                "font_size": 32,
                "line_height": 1.55,
                "padding_x": 72,
                "signature_size": 20,
                "quote_gap": 12,
            }
        if length <= 180:
            return {
                "text": normalized,
                "font_size": 26,
                "line_height": 1.5,
                "padding_x": 64,
                "signature_size": 18,
                "quote_gap": 10,
            }
        return {
            "text": normalized,
            "font_size": 22,
            "line_height": 1.45,
            "padding_x": 56,
            "signature_size": 17,
            "quote_gap": 8,
        }

    async def render_quote_image(
        self,
        quote: Quote,
        signature: str,
        style: dict[str, Any] | None = None,
        return_url: bool = True,
    ) -> str:
        effective = resolve_style(self.image_config, style)
        width = effective["width"]
        height = effective["height"]
        bg_color = effective["bg_color"]
        text_color = effective["text_color"]
        font_family = effective["font_family"]
        bg_image = effective["bg_image"]
        overlay = effective["bg_image_overlay"]
        font_scale = effective["font_scale"] / 100.0
        # 本地文件要被远端渲染服务读到，只能内联成 data URL。
        bg_image_css = inline_bg_image(bg_image) if bg_image else ""

        avatar = f"https://q1.qlogo.cn/g?b=qq&nk={quote.qq}&s=640"
        layout = self._resolve_text_layout(quote.text)
        safe_text = html.escape(layout["text"])
        signature = html.escape(signature or quote.name)
        grad_width = max(200, int(width * 0.26))
        grad_left = int(width * 0.36) - int(grad_width * 0.7)
        padding_x = int(layout["padding_x"])
        signature_size = max(10, int(layout["signature_size"] * font_scale))
        font_size = max(12, int(layout["font_size"] * font_scale))
        quote_gap = int(layout["quote_gap"])
        max_text_width = max(200, int(width * 0.64) - padding_x * 2)

        if bg_image_css:
            right_background = (
                f"background-image:url('{bg_image_css}');"
                "background-size:cover;"
                "background-position:center center;"
                "background-repeat:no-repeat;"
            )
            overlay_css = (
                ".right::before { content:''; position:absolute; inset:0;"
                f" background:rgba(0,0,0,{overlay / 100:.2f}); z-index:0; }}"
            )
        else:
            right_background = ""
            overlay_css = ""

        template = f"""
        <html>
        <head>
            <meta charset='utf-8' />
            <style>
                * {{ box-sizing: border-box; }}
                html, body {{ margin:0; padding:0; width:{width}px; min-height:{height}px; height:auto; background:{bg_color}; }}
                .root {{ position:relative; width:{width}px; min-height:{height}px; height:auto; background:{bg_color}; font-family:{font_family}; overflow:hidden; }}
                .left {{ position:absolute; left:0; top:0; bottom:0; width:{int(width * 0.36)}px; overflow:hidden; z-index:0; }}
                .left img {{ width:100%; height:100%; object-fit:cover; display:block; }}
                .left .left-shade {{ position:absolute; inset:0; background: linear-gradient(to right, rgba(0,0,0,0) 0%, rgba(0,0,0,0.28) 58%, rgba(0,0,0,0.55) 100%); }}
                .right {{ position:relative; margin-left:{int(width * 0.36)}px; width:{int(width * 0.64)}px; min-height:{height}px; padding:36px 0; background:{bg_color}; {right_background} display:flex; align-items:center; justify-content:center; text-align:center; z-index:2; }}
                {overlay_css}
                .text {{
                    position:relative;
                    z-index:1;
                    color:{text_color};
                    font-size:{font_size}px;
                    line-height:{layout["line_height"]};
                    padding:0 {padding_x}px;
                    max-width:{max_text_width}px;
                    display:flex;
                    align-items:center;
                    justify-content:center;
                    gap:{quote_gap}px;
                    text-align:center;
                }}
                .text-content {{
                    display:block;
                    max-width:100%;
                    white-space:pre-wrap;
                    word-break:break-word;
                    overflow-wrap:anywhere;
                }}
                .signature {{ position:absolute; right:44px; bottom:28px; color:rgba(255,255,255,0.82); font-size:{signature_size}px; font-weight:300; z-index:3; }}
                .quote-mark {{ color:{text_color}; opacity:0.8; flex:0 0 auto; }}
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
                        <div class="text-content">{safe_text}</div>
                        <span class="quote-mark">」</span>
                    </div>
                </div>
                <div class="fade-overlay"></div>
                <div class="signature">— {signature}</div>
            </div>
        </body>
        </html>
        """
        return await self._render_template(
            template,
            {},
            {
                "full_page": True,
                "omit_background": False,
                "viewport_width": width,
                "viewport_height": height,
            },
            return_url=return_url,
        )
