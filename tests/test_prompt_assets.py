from pathlib import Path
import sqlite3

from PIL import Image, PngImagePlugin

from src import db
from src.collectors.prompt_assets import import_prompt_assets


def _make_png(path: Path, prompt: str, illust_id: int) -> None:
    img = Image.new('RGB', (2, 2), color='white')
    meta = PngImagePlugin.PngInfo()
    meta.add_text('prompt', prompt)
    meta.add_text('illust_id', str(illust_id))
    img.save(path, pnginfo=meta)


def test_import_prompt_assets_reads_png_metadata(tmp_path):
    db_path = tmp_path / 'test.db'
    conn = db.connect_db(str(db_path))
    db.init_db(conn)

    root = tmp_path / 'assets' / 'main'
    root.mkdir(parents=True)
    png_path = root / '00123.png'
    _make_png(png_path, 'a prompt from metadata', 123)

    summary = import_prompt_assets(conn, root_dir=str(tmp_path / 'assets'))
    db.commit(conn)
    conn.close()

    assert summary['imported'] == 1

    row = sqlite3.connect(db_path).execute(
        "SELECT account_id, illust_id, prompt_text, source_key FROM prompt_assets"
    ).fetchone()
    assert row[0] == 'main'
    assert row[1] == 123
    assert row[2] == 'a prompt from metadata'
    assert row[3] == 'prompt'
