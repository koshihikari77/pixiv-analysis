# pixiv account analysis collector

複数のpixivアカウントを対象に、投稿メタ・投稿後の伸びスナップショット・フォロワー日次推移を SQLite に収集し、ローカルUIで可視化する構成です。  
収集後の SQLite (`data/pixiv_stats.db`) は GitHub Actions からコミットされ、履歴として残ります。

## Features

- 複数アカウント対応（`account_id` で分離）
- 投稿メタ収集（`illust_id`, `create_date`, `tags`, `type`, `page_count`, `x_restrict`）
- 投稿スナップショット時系列（`captured_at` + 各種カウント）
- 日次フォロワー記録（`followers`, `following`）
- `daily` / `hourly` / `manual` 実行モード
- 冪等性重視（UPSERT / INSERT OR IGNORE）
- 負荷抑制（呼び出し間隔 + ジッター、ページ数制限、詳細取得上限、429時待機）
- Streamlit UI（フォロワー推移、投稿伸び曲線、最新投稿一覧）

## Directory

```text
.
├─ .github/workflows/
│  ├─ collect_daily.yml
│  └─ collect_hourly.yml
├─ data/
│  └─ pixiv_stats.db
├─ src/
│  ├─ config.py
│  ├─ db.py
│  ├─ pixiv_client.py
│  ├─ main.py
│  └─ collectors/
│     ├─ accounts.py
│     └─ posts.py
├─ ui/
│  ├─ app.py
│  ├─ data_access.py
│  ├─ transform.py
│  └─ components.py
├─ tests/
│  ├─ test_config.py
│  ├─ test_db.py
│  ├─ test_ui_data_access.py
│  └─ test_ui_transform.py
├─ .env.example
├─ pyproject.toml
├─ collect.py
└─ requirements.txt
```

## Setup

1. `uv` で依存インストール

```bash
uv venv
uv sync --extra dev
```

2. `.env` を作成

```bash
cp .env.example .env
```

`.env` 例:

```dotenv
PIXIV_ACCOUNTS_JSON=[{"account_id":"main","pixiv_user_id":123456,"refresh_token":"YOUR_TOKEN"}]
DB_PATH=data/pixiv_stats.db
ENABLE_HOURLY=false
SNAPSHOT_RECENT_HOURS=24
USER_ILLUSTS_MAX_PAGES=3
MAX_DETAILS_PER_ACCOUNT=20
API_MIN_INTERVAL_SEC=1.0
API_JITTER_SEC=0.3
TZ=UTC
UI_DB_PATH=data/pixiv_stats.db
UI_TZ=UTC
```

補足:
- 既定で `.env` を読み込みます。
- 別ファイルを使う場合は `ENV_FILE=/path/to/your.env` を指定してください。

## Run Collector

- 日次相当の収集:

```bash
uv run python collect.py --mode daily
```

- 毎時相当の収集（`ENABLE_HOURLY=true` の時のみ実行）:

```bash
uv run python collect.py --mode hourly
```

- 手動実行（特定アカウントのみ）:

```bash
uv run python collect.py --mode manual --account-id main
```

## Run UI

```bash
uv run streamlit run ui/app.py
```

UI内容:
- Followers: 日次推移と日次増減、減少日一覧
- Post Growth: 投稿ごとの経過時間ベース成長曲線
- Latest Posts: 最新投稿と最新スナップショット一覧

## Test

```bash
uv run pytest
```

## GitHub Actions

- `collect_daily.yml`
  - 毎日1回 + 手動実行
  - `daily` モードを実行
  - DB変更時のみコミット
- `collect_hourly.yml`
  - 毎時 + 手動実行
  - `hourly` モードを実行
  - `ENABLE_HOURLY` が `false` ならスキップ
  - DB変更時のみコミット

### Required secrets

- `PIXIV_ACCOUNTS_JSON` (必須)
- `ENABLE_HOURLY` (任意。未設定時は `false`)

## Database schema

- `accounts(account_id, pixiv_user_id, updated_at)`
- `posts(account_id, illust_id, create_date, tags_json, type, page_count, x_restrict, title, updated_at)`
- `post_snapshots(account_id, illust_id, captured_at, bookmark_count, like_count, view_count, comment_count, source_mode)`
- `account_daily(account_id, date, followers, following, captured_at)`

## Notes

- refresh token はコミットせず、`.env`(ローカル) または GitHub Secrets(Actions) で管理してください。
- プライベートリポジトリでも、漏えい・誤公開・フォーク・履歴残存リスクがあるため token 直コミットは非推奨です。
- スクリプトは token をログ出力しません。例外時も token を含む文字列出力は避けてください。
- API仕様の変更が起きた場合は `src/pixiv_client.py` の抽出ロジックを更新してください。
