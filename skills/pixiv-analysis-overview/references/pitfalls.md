# ハマりどころ

## 1. `.env` が足りない

- 症状: collector 起動時に account や DB 設定で失敗する
- 原因: `PIXIV_ACCOUNTS_JSON` などが不足している
- 対処: `README.md` の `.env` 例を埋める

## 2. token を誤って扱う

- 症状: token を commit しそうになる
- 原因: `.env` と Secrets の責務分離が曖昧
- 対処: token はローカル `.env` か CI Secrets に置く

## 3. mode の意味を見落とす

- 症状: 想定より収集量が少ない / 多い
- 原因: `daily` と `manual` の違いを把握していない
- 対処: `src/main.py` と `README.md` の収集方針を読む

## 4. prompt_assets を pixiv_stats.db に書いてしまう

- 症状: CI の週次 commit と DB が分岐してマージ不能 / wakame 全取り込みで DB が 100MB 超になり push できない
- 原因: 旧版は `prompt_assets` を `pixiv_stats.db` に書いていた
- 対処: `prompt_assets` はローカル専用 `data/prompt_assets.db` に書く（現行スクリプトの既定）。`--prompt-db-path` に `pixiv_stats.db` を渡さない。`pixiv_stats.db` 内の旧 `prompt_assets` は残置・未使用
