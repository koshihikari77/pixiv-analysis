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

- `/mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis/data/pixiv_stats.db`
- `/mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis/src/` - collector 本体
- `/mnt/c/Users/inada/obsidian/base/03_projects/pixiv_analysis/ui/` - Streamlit UI

## 典型的なワークフロー

1. `.env` を用意する
2. `collect.py` で統計を収集する
3. SQLite を更新する
4. `ui/app.py` で可視化する

## 受け渡し点

- 入力: pixiv API と account 設定
- 出力: SQLite と UI 表示

## 必要に応じて読む references

- `references/key-files.md` - 初見で重要ファイルと読む順番を確認したいとき
- `references/commands.md` - 実行コマンドを確認したいとき
- `references/pitfalls.md` - token や収集モードで詰まったとき
