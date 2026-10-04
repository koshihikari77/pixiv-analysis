# よく使うコマンド

## 前提

- 実行ディレクトリ: 任意
- 必要な前提: `.env`, pixiv token, `uv`

## 最新化（正規経路: GitHub Actions）

```bash
git -C /home/inada/03_projects/pixiv-analysis pull --ff-only
```

- 用途: CI が週次 commit した DB を取り込む

```bash
gh workflow run 286307387 -R koshihikari77/pixiv-analysis   # collect-sub-weekly
# ↑ 完了を待ってから
gh workflow run 286307386 -R koshihikari77/pixiv-analysis   # collect-main-weekly
```

- 用途: 週次を待たずに今すぐ収集する。同時に起動すると後発の push が rejected になる
- `gh workflow run <file>.yml` は 404 になるので ID で指定する

## 基本（ローカル実行。`.env` が無いと `PIXIV_ACCOUNTS_JSON is required` で失敗する）

```bash
uv run --project /home/inada/03_projects/pixiv-analysis python /home/inada/03_projects/pixiv-analysis/collect.py --help
```

- 用途: collector の引数を確認する

```bash
uv run --project /home/inada/03_projects/pixiv-analysis python /home/inada/03_projects/pixiv-analysis/collect.py --mode daily
```

- 用途: 日次収集を実行する

```bash
uv run --project /home/inada/03_projects/pixiv-analysis streamlit run /home/inada/03_projects/pixiv-analysis/ui/app.py
```

- 用途: UI を起動する

```bash
uv run --project /home/inada/03_projects/pixiv-analysis pytest
```

- 用途: テストを実行する
