import json
import sqlite3
import sys
from pathlib import Path

import pytest

import apply_prompt_links
from src import db


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


def test_main_writes_links_only_to_prompt_db(tmp_path, monkeypatch):
    root = tmp_path / "akira"
    _image(root, "group/one.png")
    prompt_db = tmp_path / "prompt_assets.db"
    stats_db = tmp_path / "pixiv_stats.db"
    sconn = db.connect_db(str(stats_db))
    db.init_db(sconn)
    sconn.close()
    stats_before = stats_db.read_bytes()

    conn = db.connect_db(str(prompt_db))
    db.init_prompt_db(conn)
    db.upsert_prompt_asset(
        conn,
        {
            "account_id": "akira",
            "illust_id": 1,
            "local_path": str((root / "group/one.png").resolve()),
            "prompt_text": "p",
            "metadata_json": "{}",
        },
    )
    db.commit(conn)
    conn.close()

    json_path = tmp_path / "links.json"
    json_path.write_text(
        json.dumps(
            {
                "account_id": "sub2",
                "posts": [
                    {"pixiv_illust_id": 555, "title": "t", "local_images": ["group/one.png"]}
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DB_PATH", str(stats_db))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "apply_prompt_links.py",
            "--json-path", str(json_path),
            "--prompt-root", str(root),
            "--asset-account-id", "akira",
            "--prompt-db-path", str(prompt_db),
        ],
    )

    assert apply_prompt_links.main() == 0

    check = sqlite3.connect(prompt_db)
    assert check.execute("SELECT pixiv_illust_id FROM prompt_assets").fetchone()[0] == 555
    check.close()
    assert stats_db.read_bytes() == stats_before
