from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class QuoteSegment:
    type: str
    text: str = ""
    asset_id: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QuoteSegment":
        return cls(
            type=str(data.get("type") or ""),
            text=str(data.get("text") or ""),
            asset_id=str(data.get("asset_id") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Quote:
    id: str
    qq: str
    name: str
    text: str
    created_by: str
    created_at: float
    group: str = ""
    image_ids: list[str] = field(default_factory=list)
    segments: list[QuoteSegment] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Quote":
        segments_raw = data.get("segments") or []
        if segments_raw:
            segments = [QuoteSegment.from_dict(item) for item in segments_raw]
        else:
            segments = []
            text = str(data.get("text") or "")
            if text:
                segments.append(QuoteSegment(type="text", text=text))
            for image_id in [str(x) for x in (data.get("image_ids") or []) if str(x)]:
                segments.append(QuoteSegment(type="image", asset_id=image_id))

        image_ids = [segment.asset_id for segment in segments if segment.type == "image" and segment.asset_id]
        if not image_ids:
            image_ids = [str(x) for x in (data.get("image_ids") or []) if str(x)]
        return cls(
            id=str(data.get("id") or ""),
            qq=str(data.get("qq") or ""),
            name=str(data.get("name") or ""),
            text=str(data.get("text") or ""),
            created_by=str(data.get("created_by") or ""),
            created_at=float(data.get("created_at") or 0),
            group=str(data.get("group") or ""),
            image_ids=image_ids,
            segments=segments,
        )

    def to_dict(self) -> dict[str, Any]:
        image_ids = [segment.asset_id for segment in self.segments if segment.type == "image" and segment.asset_id]
        if image_ids:
            self.image_ids = image_ids
        return {
            "id": self.id,
            "qq": self.qq,
            "name": self.name,
            "text": self.text,
            "created_by": self.created_by,
            "created_at": self.created_at,
            "group": self.group,
            "image_ids": self.image_ids,
            "segments": [segment.to_dict() for segment in self.segments],
        }


@dataclass(slots=True)
class ImageAsset:
    asset_id: str
    file_name: str
    rel_path: str
    sha256: str
    dhash: str = ""
    width: int = 0
    height: int = 0
    ref_count: int = 0
    created_at: float = 0.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ImageAsset":
        return cls(
            asset_id=str(data.get("asset_id") or ""),
            file_name=str(data.get("file_name") or ""),
            rel_path=str(data.get("rel_path") or ""),
            sha256=str(data.get("sha256") or ""),
            dhash=str(data.get("dhash") or ""),
            width=int(data.get("width") or 0),
            height=int(data.get("height") or 0),
            ref_count=int(data.get("ref_count") or 0),
            created_at=float(data.get("created_at") or 0),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PreparedImage:
    content: bytes
    extension: str
    sha256: str
    source: str = ""
    dhash: str = ""
    width: int = 0
    height: int = 0

    @property
    def aspect_ratio(self) -> float:
        if self.width <= 0 or self.height <= 0:
            return 0.0
        return self.width / self.height


@dataclass(slots=True)
class ImageCollection:
    reply_images: list[PreparedImage] = field(default_factory=list)
    current_images: list[PreparedImage] = field(default_factory=list)

    @property
    def all_images(self) -> list[PreparedImage]:
        return [*self.reply_images, *self.current_images]


@dataclass(slots=True)
class CommandResponse:
    kind: str
    text: str = ""
    path: str = ""
    url: str = ""
    quote_id: str = ""
    chain: list[Any] = field(default_factory=list)


@dataclass(slots=True)
class PendingQuoteSegment:
    type: str
    text: str = ""
    image: PreparedImage | None = None
