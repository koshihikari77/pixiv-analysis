from __future__ import annotations

from pathlib import Path

import apply_prompt_links
import link_prompt_posts


def _make_png(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'\x89PNG\r\n\x1a\n')


def test_apply_prompt_links_expands_simple_specs(tmp_path, monkeypatch):
    root = tmp_path / 'assets'
    _make_png(root / 'folder' / '0001.png')
    _make_png(root / 'folder' / '0002.png')
    _make_png(root / 'folder' / '0003.png')
    _make_png(root / 'folder' / 'nested' / '0004.png')

    monkeypatch.setattr(apply_prompt_links, 'PROMPT_ROOT', root)

    folder_paths = [Path(p).relative_to(root).as_posix() for p, _ in apply_prompt_links._parse_local_images(['folder'])]
    assert folder_paths == [
        'folder/0001.png',
        'folder/0002.png',
        'folder/0003.png',
        'folder/nested/0004.png',
    ]

    single_paths = [Path(p).relative_to(root).as_posix() for p, _ in apply_prompt_links._parse_local_images(['folder/0002.png'])]
    assert single_paths == ['folder/0002.png']

    range_paths = [Path(p).relative_to(root).as_posix() for p, _ in apply_prompt_links._parse_local_images(['folder/0001.png-0003.png'])]
    assert range_paths == ['folder/0001.png', 'folder/0002.png', 'folder/0003.png']


def test_apply_prompt_links_rejects_missing_specs(tmp_path, monkeypatch):
    root = tmp_path / 'assets'
    _make_png(root / 'folder' / '0001.png')
    monkeypatch.setattr(apply_prompt_links, 'PROMPT_ROOT', root)

    try:
        apply_prompt_links._parse_local_images(['folder/missing.png'])
    except ValueError as exc:
        assert 'did not match any file' in str(exc) or 'does not exist' in str(exc)
    else:
        raise AssertionError('expected ValueError for missing local_images spec')


def test_link_prompt_posts_simplifies_relative_paths(tmp_path, monkeypatch):
    root = tmp_path / 'assets'
    _make_png(root / 'folder' / '0001.png')
    _make_png(root / 'folder' / '0002.png')
    _make_png(root / 'folder' / '0003.png')
    _make_png(root / 'folder' / 'nested' / '0004.png')

    monkeypatch.setattr(link_prompt_posts, 'PROMPT_ROOT', root)

    assert link_prompt_posts._simplify_relative_paths([
        'folder/0001.png',
        'folder/0002.png',
        'folder/0003.png',
        'folder/nested/0004.png',
    ]) == ['folder']

    assert link_prompt_posts._simplify_relative_paths([
        'folder/0001.png',
        'folder/0002.png',
    ]) == ['folder/0001.png-0002.png']

    assert link_prompt_posts._simplify_relative_paths([
        'folder/0003.png',
    ]) == ['folder/0003.png']


def test_link_prompt_posts_joins_posts_with_attached_prompt_db(tmp_path, monkeypatch):
    import json
    import sys

    from src import db

    root = tmp_path / 'assets'
    _make_png(root / 'folder' / '0001.png')
    stats_db = tmp_path / 'pixiv_stats.db'
    prompt_db = tmp_path / 'prompt_assets.db'

    sconn = db.connect_db(str(stats_db))
    db.init_db(sconn)
    db.upsert_post(sconn, {
        'account_id': 'sub2', 'illust_id': 777, 'create_date': '2026-01-01T00:00:00+00:00',
        'tags_json': '[]', 'title': 'post',
    })
    db.commit(sconn)
    sconn.close()

    pconn = db.connect_db(str(prompt_db))
    db.init_prompt_db(pconn)
    db.upsert_prompt_asset(pconn, {
        'account_id': 'akira', 'illust_id': 1,
        'local_path': str((root / 'folder' / '0001.png').resolve()),
        'pixiv_illust_id': 777, 'metadata_json': '{}',
    })
    db.commit(pconn)
    pconn.close()

    out_dir = tmp_path / 'out'
    out_dir.mkdir()
    monkeypatch.setattr(link_prompt_posts, 'OUT_DIR', out_dir)
    monkeypatch.setattr(sys, 'argv', [
        'link_prompt_posts.py', '--account-id', 'sub2', '--prompt-root', str(root),
        '--asset-account-id', 'akira', '--db-path', str(stats_db), '--prompt-db-path', str(prompt_db),
    ])
    assert link_prompt_posts.main() == 0

    payload = json.loads((out_dir / 'prompt_post_links.sub2.json').read_text(encoding='utf-8'))
    assert payload['posts'][0]['pixiv_illust_id'] == 777
    assert payload['posts'][0]['local_images'] == ['folder']
