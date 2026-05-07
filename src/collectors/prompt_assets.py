from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from PIL import ExifTags, Image

from src import db

DEFAULT_SUFFIXES = {".png"}
PROMPT_KEY_HINTS = {
    "prompt",
    "parameters",
    "description",
    "comment",
    "usercomment",
    "caption",
    "xmpdescription",
    "dcdescription",
}
ILLUST_ID_KEY_HINTS = {
    "illustid",
    "pixivillustid",
    "pixivid",
    "postid",
    "illust",
}
MODEL_KEY_HINTS = {
    "ckptname",
    "checkpointname",
    "modelname",
    "basemodel",
    "sdmodel",
}
LORA_KEY_HINTS = {
    "loraname",
    "loras",
    "lorastack",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", key.lower())


def _iter_metadata_pairs(value: Any, prefix: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, item in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            yield from _iter_metadata_pairs(item, next_prefix)
        return
    if isinstance(value, (list, tuple)):
        for idx, item in enumerate(value):
            next_prefix = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
            yield from _iter_metadata_pairs(item, next_prefix)
        return
    yield prefix, value


def _stringify(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _looks_like_negative_prompt(text: str) -> bool:
    lowered = text.lower()
    negative_markers = [
        "worst quality",
        "low quality",
        "bad",
        "jpeg artifacts",
        "watermark",
        "signature",
    ]
    return any(marker in lowered for marker in negative_markers)


def _extract_text_from_workflow(workflow: dict[str, Any]) -> tuple[str | None, str | None]:
    preferred_node_ids = ("275", "0")
    for node_id in preferred_node_ids:
        node = workflow.get(node_id)
        if not isinstance(node, dict):
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue
        text = inputs.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip(), f"workflow:{node_id}.inputs.text"

    candidates: list[tuple[str, str]] = []
    for node_id, node in workflow.items():
        if not isinstance(node, dict):
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue
        text = inputs.get("text")
        if isinstance(text, str):
            stripped = text.strip()
            if stripped and not _looks_like_negative_prompt(stripped):
                candidates.append((stripped, f"workflow:{node_id}.inputs.text"))

    if candidates:
        candidates.sort(key=lambda item: len(item[0]), reverse=True)
        return candidates[0]

    return None, None


def _iter_metadata_sources(metadata: dict[str, Any]) -> Iterable[tuple[str, Any]]:
    yielded_json_roots = False
    for key, value in metadata.items():
        yield key, value
        if not isinstance(value, str):
            continue
        text = value.strip()
        if not text or text[0] not in "[{":
            continue
        try:
            parsed = json.loads(text)
        except Exception:  # noqa: BLE001
            continue
        yield key, parsed
        if key == "prompt" and isinstance(parsed, dict):
            yielded_json_roots = True
    if not yielded_json_roots:
        prompt_value = metadata.get("prompt")
        if isinstance(prompt_value, dict):
            yield "prompt", prompt_value


def _looks_like_model_filename(text: str) -> bool:
    lowered = text.lower()
    return lowered.endswith((".safetensors", ".ckpt", ".pt", ".pth", ".bin"))


def _extract_model_and_loras(metadata: dict[str, Any]) -> tuple[str | None, list[str]]:
    models: list[str] = []
    loras: list[str] = []

    def add_unique(target: list[str], value: str) -> None:
        stripped = value.strip()
        if not stripped or stripped in target:
            return
        target.append(stripped)

    for source_key, source_value in _iter_metadata_sources(metadata):
        for key, value in _iter_metadata_pairs(source_value, source_key if isinstance(source_value, dict) else ""):
            if value is None:
                continue
            normalized = _normalize_key(key)
            text = str(value).strip()
            if not text:
                continue

            if normalized in MODEL_KEY_HINTS or normalized.endswith("ckptname") or normalized.endswith("checkpointname"):
                add_unique(models, text)
                continue

            if normalized in LORA_KEY_HINTS or "loraname" in normalized:
                if text.lower() != "none":
                    add_unique(loras, text)
                continue

            if _looks_like_model_filename(text):
                if "lora" in normalized or "loraname" in normalized:
                    add_unique(loras, text)
                elif any(token in normalized for token in ("ckpt", "checkpoint", "model")):
                    add_unique(models, text)

    return (models[0] if models else None, loras)


def _load_image_metadata(path: Path) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    with Image.open(path) as img:
        info = getattr(img, "info", {}) or {}
        metadata.update({str(k): _stringify(v) for k, v in info.items()})

        exif = img.getexif()
        if exif:
            for tag_id, raw_value in exif.items():
                tag_name = ExifTags.TAGS.get(tag_id, str(tag_id))
                metadata[str(tag_name)] = _stringify(raw_value)

        text_map = getattr(img, "text", None)
        if isinstance(text_map, dict):
            for key, value in text_map.items():
                metadata[str(key)] = _stringify(value)

    return metadata


def _extract_prompt_text(metadata: dict[str, Any]) -> tuple[str | None, str | None]:
    prompt_fallback: tuple[str | None, str | None] = (None, None)
    for key, value in _iter_metadata_pairs(metadata):
        if value is None:
            continue
        normalized = _normalize_key(key)
        if normalized in PROMPT_KEY_HINTS:
            text = str(value).strip()
            if not text:
                continue
            try:
                parsed = json.loads(text)
            except Exception:  # noqa: BLE001
                parsed = None
            if isinstance(parsed, dict):
                extracted, extracted_key = _extract_text_from_workflow(parsed)
                if extracted:
                    return extracted, extracted_key
            if text:
                return text, key
        if prompt_fallback == (None, None) and normalized.endswith("prompt"):
            text = str(value).strip()
            if text:
                prompt_fallback = (text, key)
    return prompt_fallback


def _extract_illust_id(metadata: dict[str, Any], path: Path) -> int | None:
    for key, value in _iter_metadata_pairs(metadata):
        if value is None:
            continue
        normalized = _normalize_key(key)
        if normalized in ILLUST_ID_KEY_HINTS:
            try:
                return int(str(value).strip())
            except ValueError:
                continue

    stem_digits = re.findall(r"(?<!\d)(\d{5,})(?!\d)", path.stem)
    if stem_digits:
        try:
            return int(stem_digits[-1])
        except ValueError:
            return None
    return None


def import_prompt_assets(
    conn,
    root_dir: str,
    account_id: str | None = None,
    suffixes: set[str] | None = None,
) -> dict[str, int]:
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(f"Prompt asset directory not found: {root}")

    effective_suffixes = {s.lower() for s in (suffixes or DEFAULT_SUFFIXES)}

    summary = {
        "seen": 0,
        "imported": 0,
        "skipped_no_account_id": 0,
        "skipped_no_prompt": 0,
        "skipped_no_illust_id": 0,
        "skipped_unsupported": 0,
        "failed": 0,
    }

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        summary["seen"] += 1
        if path.suffix.lower() not in effective_suffixes:
            summary["skipped_unsupported"] += 1
            continue

        effective_account_id = account_id
        if effective_account_id is None:
            rel_parts = path.relative_to(root).parts
            if len(rel_parts) < 2:
                summary["skipped_no_account_id"] += 1
                continue
            effective_account_id = rel_parts[0]

        try:
            metadata = _load_image_metadata(path)
            prompt_text, source_key = _extract_prompt_text(metadata)
            model_name, loras = _extract_model_and_loras(metadata)
            illust_id = _extract_illust_id(metadata, path)
            if not prompt_text:
                summary["skipped_no_prompt"] += 1
                continue
            if illust_id is None:
                summary["skipped_no_illust_id"] += 1
                continue

            db.upsert_prompt_asset(
                conn,
                {
                    "account_id": effective_account_id,
                    "illust_id": illust_id,
                    "local_path": str(path.resolve()),
                    "prompt_text": prompt_text,
                    "source_key": source_key,
                    "model_name": model_name,
                    "loras_json": json.dumps(loras, ensure_ascii=False),
                    "metadata_json": json.dumps(metadata, ensure_ascii=False),
                    "imported_at": _utc_now_iso(),
                },
            )
            summary["imported"] += 1
        except Exception:  # noqa: BLE001
            summary["failed"] += 1

    return summary
