from pathlib import Path
import json
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
        "SELECT account_id, illust_id, prompt_text, source_key, model_name, loras_json FROM prompt_assets"
    ).fetchone()
    assert row[0] == 'main'
    assert row[1] == 123
    assert row[2] == 'a prompt from metadata'
    assert row[3] == 'prompt'
    assert row[4] is None
    assert row[5] == '[]'


def test_import_prompt_assets_extracts_comfyui_prompt_text(tmp_path):
    db_path = tmp_path / 'test.db'
    conn = db.connect_db(str(db_path))
    db.init_db(conn)

    root = tmp_path / 'assets' / 'akira'
    root.mkdir(parents=True)
    png_path = root / '00005545.png'

    img = Image.new('RGB', (2, 2), color='white')
    meta = PngImagePlugin.PngInfo()
    workflow = {
        '10': {'inputs': {'ckpt_name': 'animagine-xl-v4.safetensors'}, 'class_type': 'CheckpointLoaderSimple'},
        '54': {
            'inputs': {
                'switch_1': 'On',
                'lora_name_1': 'Expressive_H-000001.safetensors',
                'switch_2': 'On',
                'lora_name_2': 'another_style.safetensors',
            },
            'class_type': 'CR Apply LoRA Stack',
        },
        '275': {'inputs': {'text': '1girl, black hair, masterpiece', 'clip': ['223', 0]}, 'class_type': 'PCTextEncode'},
        '276': {'inputs': {'text': '(bad), worst quality, low quality', 'clip': ['223', 1]}, 'class_type': 'PCTextEncode'},
    }
    meta.add_text('prompt', json.dumps(workflow))
    img.save(png_path, pnginfo=meta)

    summary = import_prompt_assets(conn, root_dir=str(tmp_path / 'assets'), account_id='akira')
    db.commit(conn)
    conn.close()

    assert summary['imported'] == 1

    row = sqlite3.connect(db_path).execute(
        "SELECT prompt_text, source_key, model_name, loras_json FROM prompt_assets WHERE account_id='akira' AND illust_id=5545"
    ).fetchone()
    assert row[0] == '1girl, black hair, masterpiece'
    assert row[1] == 'workflow:275.inputs.text'
    assert row[2] == 'animagine-xl-v4.safetensors'
    assert json.loads(row[3]) == ['Expressive_H-000001.safetensors', 'another_style.safetensors']
