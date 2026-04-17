from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


def _is_legacy_image_placeholder_text(text: str) -> bool:
    normalized = "".join(str(text or "").strip().lower().split())
    return normalized in {
        "[图片]",
        "【图片】",
        "[image]",
        "【image】",
        "[img]",
        "【img】",
    }


def _filter_legacy_image_placeholder_segments(segments: list["QuoteSegment"]) -> list["QuoteSegment"]:
    has_image = any(segment.type == "image" and segment.asset_id for segment in segments)
    if not has_image:
        return segments
    return [
        segment
        for segment in segments
        if not (
            segment.type == "text"
            and _is_legacy_image_placeholder_text(segment.text)
        )
    ]


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
class ForwardSegment:
    type: str
    text: str = ""
    asset_id: str = ""
    qq: str = ""
    name: str = ""
    face_id: int = 0
    nodes: list["ForwardNode"] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ForwardSegment":
        nodes = [ForwardNode.from_dict(item) for item in (data.get("nodes") or [])]
        return cls(
            type=str(data.get("type") or ""),
            text=str(data.get("text") or ""),
            asset_id=str(data.get("asset_id") or ""),
            qq=str(data.get("qq") or ""),
            name=str(data.get("name") or ""),
            face_id=int(data.get("face_id") or 0),
            nodes=nodes,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "text": self.text,
            "asset_id": self.asset_id,
            "qq": self.qq,
            "name": self.name,
            "face_id": self.face_id,
            "nodes": [item.to_dict() for item in self.nodes],
        }


@dataclass(slots=True)
class ForwardNode:
    sender_uin: str = ""
    sender_name: str = ""
    segments: list[ForwardSegment] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ForwardNode":
        return cls(
            sender_uin=str(data.get("sender_uin") or ""),
            sender_name=str(data.get("sender_name") or ""),
            segments=[ForwardSegment.from_dict(item) for item in (data.get("segments") or [])],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sender_uin": self.sender_uin,
            "sender_name": self.sender_name,
            "segments": [item.to_dict() for item in self.segments],
        }


def collect_forward_asset_ids(nodes: list[ForwardNode]) -> tuple[list[str], list[str]]:
    image_ids: list[str] = []
    media_ids: list[str] = []

    def walk(current_nodes: list[ForwardNode]) -> None:
        for node in current_nodes:
            for segment in node.segments:
                if not segment.asset_id:
                    if segment.type == "nodes" and segment.nodes:
                        walk(segment.nodes)
                    continue
                if segment.type == "image":
                    image_ids.append(segment.asset_id)
                elif segment.type in {"record", "video", "file"}:
                    media_ids.append(segment.asset_id)
                elif segment.type == "nodes" and segment.nodes:
                    walk(segment.nodes)

    walk(nodes)
    return image_ids, media_ids


@dataclass(slots=True)
class Quote:
    id: str
    qq: str
    name: str
    text: str
    created_by: str
    created_at: float
    group: str = ""
    kind: str = "standard"
    image_ids: list[str] = field(default_factory=list)
    media_ids: list[str] = field(default_factory=list)
    segments: list[QuoteSegment] = field(default_factory=list)
    forward_nodes: list[ForwardNode] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Quote":
        kind = str(data.get("kind") or "")
        forward_nodes_raw = data.get("forward_nodes") or []
        forward_nodes = [ForwardNode.from_dict(item) for item in forward_nodes_raw]
        raw_text = str(data.get("text") or "")
        raw_image_ids = [str(x) for x in (data.get("image_ids") or []) if str(x)]

        segments_raw = data.get("segments") or []
        if segments_raw:
            segments = [QuoteSegment.from_dict(item) for item in segments_raw]
        else:
            segments = []
            if raw_text and not (raw_image_ids and _is_legacy_image_placeholder_text(raw_text)):
                segments.append(QuoteSegment(type="text", text=raw_text))
            for image_id in raw_image_ids:
                segments.append(QuoteSegment(type="image", asset_id=image_id))

        segments = _filter_legacy_image_placeholder_segments(segments)
        has_image_segment = any(segment.type == "image" and segment.asset_id for segment in segments)
        if (raw_image_ids or has_image_segment) and _is_legacy_image_placeholder_text(raw_text):
            raw_text = ""

        if not kind:
            kind = "forward" if forward_nodes else "standard"

        image_ids = [segment.asset_id for segment in segments if segment.type == "image" and segment.asset_id]
        media_ids = [str(x) for x in (data.get("media_ids") or []) if str(x)]
        if forward_nodes:
            forward_image_ids, forward_media_ids = collect_forward_asset_ids(forward_nodes)
            image_ids = image_ids or forward_image_ids
            if not media_ids:
                media_ids = forward_media_ids
        if not image_ids:
            image_ids = [str(x) for x in (data.get("image_ids") or []) if str(x)]

        return cls(
            id=str(data.get("id") or ""),
            qq=str(data.get("qq") or ""),
            name=str(data.get("name") or ""),
            text=raw_text,
            created_by=str(data.get("created_by") or ""),
            created_at=float(data.get("created_at") or 0),
            group=str(data.get("group") or ""),
            kind=kind,
            image_ids=image_ids,
            media_ids=media_ids,
            segments=segments,
            forward_nodes=forward_nodes,
        )

    def to_dict(self) -> dict[str, Any]:
        if self.kind == "forward":
            image_ids, media_ids = collect_forward_asset_ids(self.forward_nodes)
            self.image_ids = image_ids
            self.media_ids = media_ids
        else:
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
            "kind": self.kind,
            "image_ids": self.image_ids,
            "media_ids": self.media_ids,
            "segments": [segment.to_dict() for segment in self.segments],
            "forward_nodes": [node.to_dict() for node in self.forward_nodes],
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
class MediaAsset:
    asset_id: str
    media_type: str
    file_name: str
    rel_path: str
    display_name: str = ""
    ref_count: int = 0
    created_at: float = 0.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MediaAsset":
        return cls(
            asset_id=str(data.get("asset_id") or ""),
            media_type=str(data.get("media_type") or ""),
            file_name=str(data.get("file_name") or ""),
            rel_path=str(data.get("rel_path") or ""),
            display_name=str(data.get("display_name") or ""),
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
class PreparedMedia:
    content: bytes
    extension: str
    media_type: str
    source: str = ""
    display_name: str = ""


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
    delete_fingerprint: str = ""
    chain: list[Any] = field(default_factory=list)


@dataclass(slots=True)
class SentQuoteRecord:
    quote_id: str
    fingerprint: str
    sent_at: float = 0.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SentQuoteRecord":
        return cls(
            quote_id=str(data.get("quote_id") or ""),
            fingerprint=str(data.get("fingerprint") or ""),
            sent_at=float(data.get("sent_at") or 0),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PendingQuoteSegment:
    type: str
    text: str = ""
    image: PreparedImage | None = None


@dataclass(slots=True)
class PendingForwardSegment:
    type: str
    text: str = ""
    image: PreparedImage | None = None
    media: PreparedMedia | None = None
    qq: str = ""
    name: str = ""
    face_id: int = 0
    nodes: list["PendingForwardNode"] = field(default_factory=list)


@dataclass(slots=True)
class PendingForwardNode:
    sender_uin: str = ""
    sender_name: str = ""
    segments: list[PendingForwardSegment] = field(default_factory=list)
