# 重要ファイル

## 最優先

- `/home/inada/03_projects/pixiv-analysis/README.md`
- `/home/inada/03_projects/pixiv-analysis/pyproject.toml`
- `/home/inada/03_projects/pixiv-analysis/collect.py`
- `/home/inada/03_projects/pixiv-analysis/src/main.py`
- `/home/inada/03_projects/pixiv-analysis/src/config.py`
- `/home/inada/03_projects/pixiv-analysis/ui/app.py`

## 状況別

### DB を知りたい

- `/home/inada/03_projects/pixiv-analysis/src/db.py`
- `/home/inada/03_projects/pixiv-analysis/data/pixiv_stats.db` - 統計 DB（CI commit）
- `/home/inada/03_projects/pixiv-analysis/data/prompt_assets.db` - ローカル専用 prompt DB（git 管理外）

### ローカル画像 prompt / 投稿リンクを知りたい

- `/home/inada/03_projects/pixiv-analysis/import_prompts.py` - 画像メタデータ → prompt DB
- `/home/inada/03_projects/pixiv-analysis/apply_prompt_links.py` - リンク JSON → prompt DB
- `/home/inada/03_projects/pixiv-analysis/link_prompt_posts.py` - 統計 DB + prompt DB(ATTACH) → リンク JSON 書き出し
- `/home/inada/03_projects/pixiv-analysis/src/collectors/prompt_assets.py`
- `/home/inada/03_projects/pixiv-analysis/data/prompt_post_links.{main,sub2}.json` - リンク定義の正本（git 管理）

### API 取得を知りたい

- `/home/inada/03_projects/pixiv-analysis/src/pixiv_client.py`
- `/home/inada/03_projects/pixiv-analysis/src/collectors/posts.py`
- `/home/inada/03_projects/pixiv-analysis/src/collectors/accounts.py`

### UI を知りたい

- `/home/inada/03_projects/pixiv-analysis/ui/data_access.py`
- `/home/inada/03_projects/pixiv-analysis/ui/transform.py`
