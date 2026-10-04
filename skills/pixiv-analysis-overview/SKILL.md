---
name: pixiv-analysis-overview
description: pixiv アカウント統計を収集して SQLite に保存し、Streamlit UI で比較・可視化したいときに使う。collector と UI の入口、重要ファイルもこの skill から辿れる。
---

# pixiv_analysis Overview

## この Repo でできること

- pixiv の投稿統計を収集して SQLite に蓄積できる
- daily と manual の収集フローを回せる
- Streamlit UI で数値を比較、可視化できる

## この Skill が向いている依頼

- pixiv の伸び方やアカウント差分を分析したい
- 投稿改善のために統計データを継続収集したい
- この repo の collector と UI の入口を確認したい場合は `references/` を読む

## この Repo の責務

- pixiv アカウント統計を収集して SQLite に保存する
- Streamlit UI で閲覧・比較できるようにする
- daily / manual 収集フローを提供する

## この Repo が責務として持たないもの

- 投稿画像の生成
- job 作成や story 作成

## 主要成果物

- `/home/inada/03_projects/pixiv-analysis/data/pixiv_stats.db`
- `/home/inada/03_projects/pixiv-analysis/src/` - collector 本体
- `/home/inada/03_projects/pixiv-analysis/ui/` - Streamlit UI

## 典型的なワークフロー

収集の正規経路は **GitHub Actions**（`collect-main-weekly` / `collect-sub-weekly`、毎週日曜 UTC 0時台）。CI が `collect.py` を回して `data/pixiv_stats.db` を commit する。ローカルに `.env` は無いので、ローカルで `collect.py` は動かない。

1. 最新化: `git pull --ff-only`（今すぐ取りたいときは `gh workflow run <id>` で手動実行。main と sub を同時に起動すると push が衝突するので、片方が終わってから次を起動する）
2. 集計: `data/pixiv_stats.db` を読み取り専用で集計する（sqlite3 CLI は無いので python の sqlite3 を使う）
3. 可視化: `ui/app.py`

注意: ローカルで DB を書き換えて commit すると、CI の週次 commit と分岐する（DB はバイナリなのでマージできない）。DB への書き込みは CI に任せる。

## 受け渡し点

- 入力: pixiv API と account 設定
- 出力: SQLite と UI 表示

## 必要に応じて読む references

- `references/key-files.md` - 初見で重要ファイルと読む順番を確認したいとき
- `references/commands.md` - 実行コマンドを確認したいとき
- `references/pitfalls.md` - token や収集モードで詰まったとき
