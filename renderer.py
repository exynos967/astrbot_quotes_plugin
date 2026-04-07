from __future__ import annotations

import html
from typing import Any, Awaitable, Callable

try:
    from .models import Quote
    from .utils import normalize_quote_text
except ImportError:  # pragma: no cover
    from models import Quote
    from utils import normalize_quote_text


class QuoteRenderer:
    def __init__(self, html_render: Callable[[str, dict[str, Any], dict[str, Any]], Awaitable[str]], image_config: dict[str, Any]):
        self._html_render = html_render
        self.image_config = image_config or {}

    async def warmup(self) -> None:
        minimal = '<div style="width:320px;height:120px;background:#000;color:#fff">init</div>'
        try:
            await self._html_render(
                minimal,
                {},
                {"full_page": False, "clip": {"x": 0, "y": 0, "width": 320, "height": 120}},
            )
        except Exception:
            return

    async def render_quote_image(self, quote: Quote, signature: str) -> str:
        width = int(self.image_config.get("width", 1280))
        height = int(self.image_config.get("height", 427))
        bg_color = self.image_config.get("bg_color", "#000")
        text_color = self.image_config.get("text_color", "#fff")
        font_family = self.image_config.get(
            "font_family",
            "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', 'WenQuanYi Micro Hei', Arial, sans-serif",
        )
        avatar = f"https://q1.qlogo.cn/g?b=qq&nk={quote.qq}&s=640"
        safe_text = html.escape(normalize_quote_text(quote.text))
        signature = html.escape(signature or quote.name)
        grad_width = max(200, int(width * 0.26))
        grad_left = int(width * 0.36) - int(grad_width * 0.7)

        template = f"""
        <html>
        <head>
            <meta charset='utf-8' />
            <style>
                * {{ box-sizing: border-box; }}
                html, body {{ margin:0; padding:0; width:{width}px; height:{height}px; background:{bg_color}; }}
                .root {{ position:relative; width:{width}px; height:{height}px; background:{bg_color}; font-family:{font_family}; overflow:hidden; }}
                .left {{ position:absolute; left:0; top:0; width:{int(width * 0.36)}px; height:{height}px; overflow:hidden; z-index:0; }}
                .left img {{ width:100%; height:100%; object-fit:cover; display:block; }}
                .left .left-shade {{ position:absolute; inset:0; background: linear-gradient(to right, rgba(0,0,0,0) 0%, rgba(0,0,0,0.28) 58%, rgba(0,0,0,0.55) 100%); }}
                .right {{ position:absolute; left:{int(width * 0.36)}px; top:0; width:{int(width * 0.64)}px; height:{height}px; background:{bg_color}; display:flex; align-items:center; justify-content:center; text-align:center; z-index:2; }}
                .text {{ color:{text_color}; font-size:38px; line-height:1.6; padding:0 80px; max-width:calc(100% - 160px); display:flex; align-items:center; justify-content:center; text-align:center; }}
                .signature {{ position:absolute; right:44px; bottom:28px; color:rgba(255,255,255,0.82); font-size:22px; font-weight:300; z-index:3; }}
                .quote-mark {{ color:{text_color}; opacity:0.8; margin-right:14px; }}
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
                        <div>{safe_text}</div>
                        <span class="quote-mark">」</span>
                    </div>
                </div>
                <div class="fade-overlay"></div>
                <div class="signature">— {signature}</div>
            </div>
        </body>
        </html>
        """
        return await self._html_render(
            template,
            {},
            {
                "full_page": False,
                "omit_background": False,
                "clip": {"x": 0, "y": 0, "width": width, "height": height},
            },
        )
