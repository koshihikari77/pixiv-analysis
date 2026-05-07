from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sqlite3

DB_PATH = os.environ.get('DB_PATH', 'data/pixiv_stats.db')
PROMPT_ROOT = Path(os.environ.get('PROMPT_ROOT', '/mnt/c/Users/inada/obsidian/base/03_projects/pixiv/akira'))
OUT_DIR = Path('data')
ACCOUNT_IDS = ['main', 'sub2']


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _relative_path(local_path: str) -> str:
    path = Path(local_path)
    try:
        return str(path.resolve().relative_to(PROMPT_ROOT.resolve()))
    except Exception:  # noqa: BLE001
        return str(path)


def _natural_key(text: str) -> tuple[tuple[int, Any], ...]:
    parts: list[tuple[int, Any]] = []
    for chunk in re.split(r'(\d+)', text):
        if not chunk:
            continue
        if chunk.isdigit():
            parts.append((0, int(chunk)))
        else:
            parts.append((1, chunk.lower()))
    return tuple(parts)


def _simplify_relative_paths(relative_paths: list[str]) -> list[str]:
    if not relative_paths:
        return []

    root = PROMPT_ROOT.resolve()
    absolute_paths: list[Path] = []
    for rel in relative_paths:
        try:
            absolute_paths.append((root / Path(rel)).resolve())
        except Exception:  # noqa: BLE001
            absolute_paths.append(Path(rel).resolve())

    image_suffixes = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tif', '.tiff'}

    def folder_images(folder: Path) -> list[Path]:
        if not folder.exists() or not folder.is_dir():
            return []
        items = [p for p in folder.rglob('*') if p.is_file() and p.suffix.lower() in image_suffixes]
        items.sort(key=lambda p: _natural_key(str(p.relative_to(folder))))
        return items

    def rel_spec(path: Path) -> str:
        try:
            return str(path.relative_to(root))
        except Exception:  # noqa: BLE001
            return str(path)

    simplified: list[str] = []
    selected_set = set(absolute_paths)

    try:
        common_folder = Path(os.path.commonpath([str(p.parent) for p in absolute_paths]))
    except ValueError:
        common_folder = Path(absolute_paths[0].parent)

    candidate_folders: list[Path] = []
    current = common_folder
    while True:
        candidate_folders.append(current)
        if current == current.parent:
            break
        if current == root:
            break
        try:
            current = current.parent
        except Exception:  # noqa: BLE001
            break

    for folder in candidate_folders:
        images = folder_images(folder)
        if images and set(images) == selected_set:
            simplified.append(rel_spec(folder))
            return simplified

    grouped: dict[Path, list[Path]] = defaultdict(list)
    for path in absolute_paths:
        grouped[path.parent].append(path)

    for folder, paths in sorted(grouped.items(), key=lambda item: _natural_key(rel_spec(item[0]))):
        existing_images = folder_images(folder)
        index_by_path = {path: idx for idx, path in enumerate(existing_images)}

        selected = [p for p in existing_images if p in set(paths)]
        selected.sort(key=lambda p: index_by_path.get(p, 10**9))

        if not selected:
            for path in sorted(paths, key=lambda p: _natural_key(p.name)):
                simplified.append(rel_spec(path))
            continue

        run: list[Path] = []
        prev_idx: int | None = None
        for path in selected:
            idx = index_by_path.get(path)
            if idx is None:
                if run:
                    first = run[0]
                    last = run[-1]
                    spec = first.name if len(run) == 1 else f"{first.name}-{last.name}"
                    simplified.append(rel_spec(folder / spec))
                    run = []
                simplified.append(rel_spec(path))
                prev_idx = None
                continue

            if prev_idx is None or idx == prev_idx + 1:
                run.append(path)
            else:
                first = run[0]
                last = run[-1]
                spec = first.name if len(run) == 1 else f"{first.name}-{last.name}"
                simplified.append(rel_spec(folder / spec))
                run = [path]
            prev_idx = idx

        if run:
            first = run[0]
            last = run[-1]
            spec = first.name if len(run) == 1 else f"{first.name}-{last.name}"
            simplified.append(rel_spec(folder / spec))

    deduped: list[str] = []
    seen: set[str] = set()
    for spec in simplified:
        if spec in seen:
            continue
        seen.add(spec)
        deduped.append(spec)
    return deduped


def _load_posts(conn: sqlite3.Connection, account_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT account_id, illust_id, title, create_date
        FROM posts
        WHERE account_id = ?
        ORDER BY create_date DESC, illust_id DESC
        """,
        (account_id,),
    ).fetchall()


def _load_assets(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT local_path, pixiv_illust_id
        FROM prompt_assets
        WHERE account_id = 'akira'
        ORDER BY COALESCE(pixiv_illust_id, -1), local_path
        """
    ).fetchall()


def _build_payload(account_id: str, posts: list[sqlite3.Row], assets: list[sqlite3.Row]) -> dict[str, Any]:
    assets_by_post: dict[int, list[str]] = defaultdict(list)
    orphan_assets: list[str] = []
    for row in assets:
        relative_path = _relative_path(row['local_path'])
        if row['pixiv_illust_id'] is None:
            orphan_assets.append(relative_path)
            continue
        assets_by_post[int(row['pixiv_illust_id'])].append(relative_path)

    out: dict[str, Any] = {
        'account_id': account_id,
        'generated_at': _utc_now_iso(),
        'posts': [],
        'unmatched_local_images': orphan_assets,
    }

    for post in posts:
        linked = assets_by_post.get(int(post['illust_id']), [])
        out['posts'].append(
            {
                'pixiv_account_id': post['account_id'],
                'pixiv_illust_id': int(post['illust_id']),
                'title': post['title'],
                'create_date': post['create_date'],
                'local_images': _simplify_relative_paths(linked),
            }
        )

    out['unmatched_local_images'] = _simplify_relative_paths(orphan_assets)
    return out


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def main() -> int:
    parser = argparse.ArgumentParser(description='Export prompt-to-post link JSONs')
    parser.add_argument('--account-id', default=None, choices=[None, *ACCOUNT_IDS], help='Export only one account file')
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    assets = _load_assets(conn)

    account_ids = [args.account_id] if args.account_id else ACCOUNT_IDS
    for account_id in account_ids:
        posts = _load_posts(conn, account_id)
        payload = _build_payload(account_id, posts, assets)
        out_path = OUT_DIR / f'prompt_post_links.{account_id}.json'
        _write_json(out_path, payload)
        print(f'wrote {out_path} with {len(payload["posts"])} posts and {len(payload["unmatched_local_images"])} unmatched images')

    if args.account_id is None:
        unmatched_only = _build_payload('unmatched', [], assets)
        _write_json(OUT_DIR / 'prompt_post_links.unmatched.json', {
            'generated_at': unmatched_only['generated_at'],
            'unmatched_local_images': unmatched_only['unmatched_local_images'],
        })
        print(f'wrote {OUT_DIR / "prompt_post_links.unmatched.json"} with {len(unmatched_only["unmatched_local_images"])} unmatched images')
    conn.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
