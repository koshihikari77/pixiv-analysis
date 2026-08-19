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
ASSET_ACCOUNT_ID = os.environ.get('PROMPT_ASSET_ACCOUNT_ID', 'akira')
IMAGE_SUFFIXES = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tif', '.tiff'}
EXPAND_SUFFIXES = {'.png'}


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


def _asset_path_key(path: str, prompt_root: Path, asset_account_id: str) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(prompt_root.resolve()).as_posix()
    except ValueError:
        normalized = resolved.as_posix()
        marker = f'/{asset_account_id}/'
        if marker in normalized:
            return normalized.rsplit(marker, 1)[1]
        return normalized


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
    files = [
        p for p in path.rglob('*')
        if p.is_file() and p.suffix.lower() in EXPAND_SUFFIXES
    ]
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


def _expand_spec(spec: str, allow_missing_explicit: bool = False) -> list[Path]:
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

    # Explicit image paths can be applied without mounting the image root.
    # The DB preflight in main() still rejects typos and non-imported assets.
    if allow_missing_explicit and candidate.suffix.lower() in IMAGE_SUFFIXES:
        return [candidate]

    return []


def _parse_local_images(
    images: Iterable[Any],
    allow_missing_explicit: bool = False,
) -> list[tuple[str, dict[str, Any]]]:
    expanded: list[tuple[str, dict[str, Any]]] = []
    for item in images:
        if isinstance(item, str):
            paths = _expand_spec(item, allow_missing_explicit=allow_missing_explicit)
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
            if path.suffix.lower() not in IMAGE_SUFFIXES or (
                not allow_missing_explicit and not _is_image(path)
            ):
                raise ValueError(f'local_images path is not an image: {rel}')
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


def _index_payload(
    payload: dict[str, Any],
    allow_missing_explicit: bool = False,
    include_unmatched: bool = True,
) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for post in payload.get('posts', []):
        pixiv_illust_id = post.get('pixiv_illust_id')
        title = post.get('title')
        for local_path, _meta in _parse_local_images(
            post.get('local_images', []),
            allow_missing_explicit=allow_missing_explicit,
        ):
            desired = {
                'pixiv_illust_id': pixiv_illust_id,
                'title': title,
            }
            existing = index.get(local_path)
            if existing is not None and existing != desired:
                raise ValueError(
                    f'local image is linked to multiple posts: {local_path}'
                )
            index[local_path] = desired
    for local_path, _meta in _parse_local_images(
        payload.get('unmatched_local_images', []),
        allow_missing_explicit=allow_missing_explicit,
    ):
        if local_path in index and index[local_path]['pixiv_illust_id'] is not None:
            raise ValueError(
                f'local image is both linked and unmatched: {local_path}'
            )
        if include_unmatched:
            index[local_path] = {
                'pixiv_illust_id': None,
                'title': None,
            }
    return index


def main() -> int:
    global EXPAND_SUFFIXES, PROMPT_ROOT

    parser = argparse.ArgumentParser(description='Apply prompt-to-post link JSON to SQLite')
    parser.add_argument('--json-path', required=True, help='Path to prompt_post_links.<account>.json')
    parser.add_argument(
        '--prompt-root',
        default=str(PROMPT_ROOT),
        help='Root directory used to resolve local_images paths (default: PROMPT_ROOT)',
    )
    parser.add_argument(
        '--asset-account-id',
        default=ASSET_ACCOUNT_ID,
        help='prompt_assets account_id to update (default: PROMPT_ASSET_ACCOUNT_ID or akira)',
    )
    parser.add_argument(
        '--allow-unmounted-root',
        action='store_true',
        help='Allow explicit image paths when the image root is not mounted; DB preflight still validates them',
    )
    parser.add_argument(
        '--extensions',
        default='png',
        help='Comma-separated extensions expanded by folder specs (default: png)',
    )
    parser.add_argument(
        '--clear-unmatched',
        action='store_true',
        help='Clear DB links for unmatched_local_images; disabled by default to protect other account mappings',
    )
    args = parser.parse_args()
    PROMPT_ROOT = Path(args.prompt_root)
    EXPAND_SUFFIXES = {
        f'.{ext.strip().lower().lstrip(".")}'
        for ext in args.extensions.split(',')
        if ext.strip()
    }

    json_path = Path(args.json_path)
    payload = json.loads(json_path.read_text(encoding='utf-8'))
    pixiv_account_id = payload.get('account_id')

    index = _index_payload(
        payload,
        allow_missing_explicit=args.allow_unmounted_root,
        include_unmatched=args.clear_unmatched,
    )

    conn = db.connect_db(DB_PATH)
    db.init_db(conn)

    rows = conn.execute(
        "SELECT account_id, illust_id, local_path, pixiv_illust_id, title "
        "FROM prompt_assets WHERE account_id = ?",
        (args.asset_account_id,),
    ).fetchall()

    index_by_key: dict[str, dict[str, Any]] = {}
    for local_path, desired in index.items():
        key = _asset_path_key(local_path, PROMPT_ROOT, args.asset_account_id)
        existing = index_by_key.get(key)
        if existing is not None and existing != desired:
            conn.close()
            raise ValueError(f'conflicting link targets for relative asset path: {key}')
        index_by_key[key] = desired

    rows_by_key: dict[str, Any] = {}
    for row in rows:
        key = _asset_path_key(row['local_path'], PROMPT_ROOT, args.asset_account_id)
        if key in rows_by_key:
            conn.close()
            raise ValueError(f'duplicate prompt_assets relative path: {key}')
        rows_by_key[key] = row

    missing_paths = sorted(set(index_by_key) - set(rows_by_key))
    if missing_paths:
        conn.close()
        sample = ', '.join(missing_paths[:3])
        raise ValueError(
            f'{len(missing_paths)} local_images entries are not present in prompt_assets; '
            f'check --prompt-root and imported assets. Examples: {sample}'
        )

    updated = 0
    cleared = 0
    for key, row in rows_by_key.items():
        desired = index_by_key.get(key)
        if desired is None:
            continue
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
    print(
        f'updated {updated} prompt_asset rows, cleared {cleared}, '
        f'pixiv_account={pixiv_account_id}, asset_account={args.asset_account_id}, '
        f'from {json_path}'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
