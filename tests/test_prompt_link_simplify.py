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
