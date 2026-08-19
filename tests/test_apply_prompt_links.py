from pathlib import Path

import pytest

import apply_prompt_links


def _image(root: Path, relative_path: str) -> None:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def test_index_payload_rejects_image_linked_to_multiple_posts(tmp_path, monkeypatch):
    monkeypatch.setattr(apply_prompt_links, "PROMPT_ROOT", tmp_path)
    _image(tmp_path, "group/one.png")
    payload = {
        "posts": [
            {"pixiv_illust_id": 1, "title": "one", "local_images": ["group/one.png"]},
            {"pixiv_illust_id": 2, "title": "two", "local_images": ["group/one.png"]},
        ]
    }

    with pytest.raises(ValueError, match="linked to multiple posts"):
        apply_prompt_links._index_payload(payload)


def test_index_payload_rejects_linked_image_left_in_unmatched(tmp_path, monkeypatch):
    monkeypatch.setattr(apply_prompt_links, "PROMPT_ROOT", tmp_path)
    _image(tmp_path, "group/one.png")
    payload = {
        "posts": [
            {"pixiv_illust_id": 1, "title": "one", "local_images": ["group/one.png"]},
        ],
        "unmatched_local_images": ["group/one.png"],
    }

    with pytest.raises(ValueError, match="both linked and unmatched"):
        apply_prompt_links._index_payload(payload)


def test_index_payload_does_not_clear_unmatched_unless_requested(tmp_path, monkeypatch):
    monkeypatch.setattr(apply_prompt_links, "PROMPT_ROOT", tmp_path)
    _image(tmp_path, "group/one.png")

    index = apply_prompt_links._index_payload(
        {"posts": [], "unmatched_local_images": ["group/one.png"]},
        include_unmatched=False,
    )

    assert index == {}


def test_index_payload_accepts_explicit_imported_path_without_mounted_root(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(apply_prompt_links, "PROMPT_ROOT", tmp_path)
    payload = {
        "posts": [
            {"pixiv_illust_id": 1, "title": "one", "local_images": ["group/one.png"]},
        ]
    }

    index = apply_prompt_links._index_payload(payload, allow_missing_explicit=True)

    assert str((tmp_path / "group/one.png").resolve()) in index


def test_asset_path_key_matches_assets_moved_between_computers(tmp_path):
    current_root = tmp_path / "pixiv" / "akira"
    current_path = current_root / "group" / "one.png"
    old_path = "/mnt/c/project/pixiv/akira/group/one.png"

    assert apply_prompt_links._asset_path_key(
        str(current_path), current_root, "akira"
    ) == apply_prompt_links._asset_path_key(old_path, current_root, "akira")
