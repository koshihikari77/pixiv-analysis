from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Iterable

from src import db

DB_PATH = os.environ.get('DB_PATH', 'data/pixiv_stats.db')
PROMPT_ROOT = Path(os.environ.get('PROMPT_ROOT', '/mnt/c/Users/inada/obsidian/base/03_projects/pixiv/akira'))
IMAGE_SUFFIXES = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tif', '.tiff'}


def _absolute_path(relative_path: str) -> str:
    path = Path(relative_path)
    if path.is_absolute():
        return str(path)
    return str((PROMPT_ROOT / path).resolve())


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROMPT_ROOT.resolve()))
    except Exception:  # noqa: BLE001
        return str(path)


def _natural_key(text: str) -> tuple[tuple[int, Any], ...]:
    key: list[tuple[int, Any]] = []
    for part in re.split(r'(\d+)', text):
        if not part:
            continue
        if part.isdigit():
            key.append((0, int(part)))
        else:
            key.append((1, part.lower()))
    return tuple(key)


def _is_image(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES


def _expand_folder(path: Path) -> list[Path]:
    if not path.exists() or not path.is_dir():
        return []
    files = [p for p in path.rglob('*') if _is_image(p)]
    files.sort(key=lambda p: _natural_key(str(p.relative_to(path))))
    return files


def _expand_range(spec: str) -> list[Path] | None:
    if '-' not in spec:
        return None
    left, right = spec.rsplit('-', 1)
    left_path = Path(left)
    right_path = Path(right)
    if not left_path.suffix or not right_path.suffix:
        return None
    if left_path.suffix.lower() not in IMAGE_SUFFIXES or right_path.suffix.lower() not in IMAGE_SUFFIXES:
        return None
    if right_path.parent != Path('.') and right_path.parent != left_path.parent:
        return None

    folder = (PROMPT_ROOT / left_path.parent).resolve()
    if not folder.exists() or not folder.is_dir():
        return []

    candidates = [p for p in folder.iterdir() if _is_image(p)]
    candidates.sort(key=lambda p: _natural_key(p.name))
    names = [p.name for p in candidates]
    try:
        start_idx = names.index(left_path.name)
        end_idx = names.index(right_path.name)
    except ValueError:
        return []
    if start_idx > end_idx:
        start_idx, end_idx = end_idx, start_idx
    return candidates[start_idx:end_idx + 1]


def _expand_spec(spec: str) -> list[Path]:
    raw = spec.strip()
    if not raw:
        return []

    if raw.endswith('/'):
        raw = raw[:-1]

    candidate = PROMPT_ROOT / raw
    if candidate.exists():
        if candidate.is_dir():
            return _expand_folder(candidate)
        if _is_image(candidate):
            return [candidate]

    range_paths = _expand_range(raw)
    if range_paths is not None:
        return range_paths

    if not candidate.suffix:
        return _expand_folder(candidate)

    if candidate.exists() and candidate.is_file():
        return [candidate]

    return []


def _parse_local_images(images: Iterable[Any]) -> list[tuple[str, dict[str, Any]]]:
    expanded: list[tuple[str, dict[str, Any]]] = []
    for item in images:
        if isinstance(item, str):
            paths = _expand_spec(item)
            if not paths:
                raise ValueError(f'local_images spec did not match any file: {item}')
            for path in paths:
                expanded.append((str(path.resolve()), {}))
            continue

        if not isinstance(item, dict):
            continue

        rel = item.get('relative_path') or item.get('path')
        if rel:
            path = Path(_absolute_path(rel))
            if not _is_image(path):
                raise ValueError(f'local_images path does not exist or is not an image: {rel}')
            expanded.append((str(path.resolve()), item))
            continue

        if item.get('range') and isinstance(item['range'], list) and len(item['range']) == 2:
            left = str(item['range'][0])
            right = str(item['range'][1])
            paths = _expand_spec(f'{left}-{right}')
            if not paths:
                raise ValueError(f'local_images range did not match any file: {left}-{right}')
            for path in paths:
                expanded.append((str(path.resolve()), item))
            continue

    return expanded


def _index_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for post in payload.get('posts', []):
        pixiv_illust_id = post.get('pixiv_illust_id')
        title = post.get('title')
        for local_path, _meta in _parse_local_images(post.get('local_images', [])):
            index[local_path] = {
                'pixiv_illust_id': pixiv_illust_id,
                'title': title,
            }
    for local_path, _meta in _parse_local_images(payload.get('unmatched_local_images', [])):
        index[local_path] = {
            'pixiv_illust_id': None,
            'title': None,
        }
    return index


def main() -> int:
    parser = argparse.ArgumentParser(description='Apply prompt-to-post link JSON to SQLite')
    parser.add_argument('--json-path', required=True, help='Path to prompt_post_links.<account>.json')
    parser.add_argument('--account-id', default=None, help='Optional account_id to restrict updates')
    args = parser.parse_args()

    json_path = Path(args.json_path)
    payload = json.loads(json_path.read_text(encoding='utf-8'))
    account_id = args.account_id or payload.get('account_id')
    clear_missing = bool(account_id)

    index = _index_payload(payload)

    conn = db.connect_db(DB_PATH)
    db.init_db(conn)

    if account_id:
        rows = conn.execute(
            "SELECT account_id, illust_id, local_path, pixiv_illust_id, title FROM prompt_assets WHERE account_id = ?",
            (account_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT account_id, illust_id, local_path, pixiv_illust_id, title FROM prompt_assets"
        ).fetchall()

    updated = 0
    cleared = 0
    for row in rows:
        desired = index.get(row['local_path'])
        if desired is None:
            if not clear_missing:
                continue
            desired = {'pixiv_illust_id': None, 'title': None}
        if row['pixiv_illust_id'] == desired['pixiv_illust_id'] and row['title'] == desired['title']:
            continue
        conn.execute(
            """
            UPDATE prompt_assets
               SET pixiv_illust_id = ?,
                   title = ?
             WHERE account_id = ?
               AND illust_id = ?
               AND local_path = ?
            """,
            (
                desired['pixiv_illust_id'],
                desired['title'],
                row['account_id'],
                row['illust_id'],
                row['local_path'],
            ),
        )
        if desired['pixiv_illust_id'] is None:
            cleared += 1
        else:
            updated += 1

    db.commit(conn)
    conn.close()
    print(f'updated {updated} prompt_asset rows, cleared {cleared}, from {json_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
