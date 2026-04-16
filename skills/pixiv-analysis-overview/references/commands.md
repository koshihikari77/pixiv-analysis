# よく使うコマンド

## 前提

- 実行ディレクトリ: 任意
- 必要な前提: `.env`, pixiv token, `uv`

## 基本

```bash
uv run --project /mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis python /mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis/collect.py --help
```

- 用途: collector の引数を確認する

```bash
uv run --project /mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis python /mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis/collect.py --mode daily
```

- 用途: 日次収集を実行する

```bash
uv run --project /mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis streamlit run /mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis/ui/app.py
```

- 用途: UI を起動する

```bash
uv run --project /mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis pytest
```

- 用途: テストを実行する
