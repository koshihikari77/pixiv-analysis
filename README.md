# pixiv account analysis collector

複数のpixivアカウントを対象に、投稿メタ・投稿後の伸びスナップショット・フォロワー日次推移を SQLite に収集し、ローカルUIで可視化する構成です。  
収集後の SQLite (`data/pixiv_stats.db`) は GitHub Actions からコミットされ、履歴として残ります。

## Features

- 複数アカウント対応（`account_id` で分離）
- 投稿メタ収集（`illust_id`, `create_date`, `tags`, `type`, `page_count`, `x_restrict`）
- 投稿スナップショット時系列（`captured_at` + 各種カウント）
- ローカル画像メタデータからの prompt 取り込み（`prompt_assets`、ローカル専用 DB `data/prompt_assets.db`）
- 日次フォロワー記録（`followers`, `following`）
- `daily` / `manual` 実行モード
- 冪等性重視（UPSERT / INSERT OR IGNORE）
- 負荷抑制（呼び出し間隔 + ジッター、ページ数制限、詳細取得上限、429時待機）
- `daily` では投稿から60日以内の作品だけ snapshot を取得
- `main` と `sub2` は weekly の運用を想定
- Streamlit UI（フォロワー推移、投稿伸び曲線、投稿間growth比較、最新投稿一覧）

## Directory

```text
.
├─ .github/workflows/
│  ├─ collect_main_weekly.yml
│  └─ collect_sub_weekly.yml
├─ data/
│  └─ pixiv_stats.db
├─ src/
│  ├─ config.py
│  ├─ db.py
│  ├─ pixiv_client.py
│  ├─ main.py
│  └─ collectors/
│     ├─ accounts.py
│     ├─ posts.py
│     └─ prompt_assets.py
├─ ui/
│  ├─ app.py
│  ├─ data_access.py
│  ├─ transform.py
│  └─ components.py
├─ tests/
│  ├─ test_config.py
│  ├─ test_db.py
│  ├─ test_ui_data_access.py
│  ├─ test_ui_transform.py
│  └─ test_pixiv_client.py
├─ .env.example
├─ pyproject.toml
├─ collect.py
├─ import_prompts.py
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
SNAPSHOT_MAX_AGE_DAYS=60
USER_ILLUSTS_MAX_PAGES=3
MAX_DETAILS_PER_ACCOUNT=200
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

- 日次収集:

```bash
uv run python collect.py --mode daily
```

- 手動実行（特定アカウントのみ）:

```bash
uv run python collect.py --mode manual --account-id main
```

収集方針:
- `posts`: 全投稿のメタを同期
- `post_snapshots`: 投稿から `SNAPSHOT_MAX_AGE_DAYS` 日以内の作品だけ daily で取得

## Import Local Prompts

ローカル画像のメタデータに入っている prompt を `prompt_assets` テーブルに取り込めます。

### ローカル専用 prompt DB（`data/prompt_assets.db`）

`prompt_assets`（ローカル画像パス・prompt・pixiv 作品リンク）は、CI が週次 commit する
`data/pixiv_stats.db` とは**別のローカル専用 DB** に置きます。

- 既定パス: `data/prompt_assets.db`（`.gitignore` 済み。commit しない）
- 変更: 環境変数 `PROMPT_DB_PATH`、または各スクリプトの `--prompt-db-path`
- 書き込み（`import_prompts.py` / `apply_prompt_links.py`）は prompt DB だけに行い、
  `pixiv_stats.db` には一切書かない
- 読み取り（UI / `link_prompt_posts.py`）は `pixiv_stats.db` を開き、prompt DB を
  SQLite `ATTACH ... AS pdb` で結合する（`pdb.prompt_assets`）
- prompt DB が無い場合は空の `pdb.prompt_assets` を in-memory で ATTACH するので、
  UI はプロンプト列が空になるだけで落ちない
- CI（`collect.py` → `init_db`）は prompt DB に触れない。`init_db` は `prompt_assets` を作らない
- 分離の理由: ローカルで `pixiv_stats.db` に書くと CI の週次 commit と分岐してマージできず、
  wakame 全取り込みで DB が 200MB 超になり GitHub の 100MB 上限を超えるため
- 旧 `pixiv_stats.db` 内の `prompt_assets` テーブル（akira の旧パス行）は**残置・未使用**。
  CI の DB を変えないため DROP しない。UI/スクリプトはこのテーブルを参照しない

再構築手順（prompt DB は再生成可能。正本は画像メタデータとリンク JSON）:

```bash
uv run python import_prompts.py --root /home/inada/03_projects/pixiv/akira --account-id akira
uv run python import_prompts.py --root /home/inada/03_projects/pixiv/wakame --account-id wakame \
  --extensions png,jpg --derive-missing-id --include-promptless
uv run python apply_prompt_links.py --json-path data/prompt_post_links.main.json \
  --asset-account-id wakame --prompt-root /home/inada/03_projects/pixiv/wakame
uv run python apply_prompt_links.py --json-path data/prompt_post_links.sub2.json \
  --asset-account-id akira --prompt-root /home/inada/03_projects/pixiv/akira
```

アカウント対応: pixiv `main` = ローカル資産 `wakame`、pixiv `sub2` = ローカル資産 `akira`。

前提:
- 画像メタデータに `prompt` もしくは類似キーが入っていること
- `illust_id` がメタデータ内にあるか、ファイル名に数値 ID が含まれていること
- `account_id` は `--account-id` で明示するか、`<root>/<account_id>/...` の階層にすること

実行例:

```bash
uv run python import_prompts.py --root /path/to/assets --account-id main
```

`--account-id` を省略した場合は、`<root>/<account_id>/...` の先頭ディレクトリ名を使います。
画像メタデータとファイル名のどちらにも生成IDがない場合は、
`--derive-missing-id` でアカウント配下の相対パスから安定したローカルIDを生成できます。
プロンプト情報が消えている書き出し済みJPGもローカル画像として保持する場合は、
`--include-promptless` を指定します。

紐づけJSONの適用時は、Pixiv投稿側のアカウントとは別に、ローカル画像を取り込んだ
`prompt_assets.account_id` と画像rootを指定できます。

```bash
uv run python apply_prompt_links.py \
  --json-path data/prompt_post_links.sub2.json \
  --asset-account-id akira \
  --prompt-root /path/to/pixiv/akira
```

`--prompt-root` は `PROMPT_ROOT`、`--asset-account-id` は
`PROMPT_ASSET_ACCOUNT_ID` でも指定できます。
画像rootをマウントできない環境では、JSONが個別画像パスを指定済みなら
`--allow-unmounted-root` を付けて、取込済みDBパスとの照合だけで適用できます。
DBに旧PCの絶対パスが保存されていても、画像root配下の相対パスで照合されます。
フォルダ指定は既定でPNGだけを展開します。別形式も取り込んだ場合は
`--extensions png,jpg` のように指定します。
`unmatched_local_images` の既存DBリンクは通常変更しません。明示的に解除する場合だけ
`--clear-unmatched` を付けます。

仮の投稿紐づけは `data/prompt_post_links.main.json` と `data/prompt_post_links.sub2.json` に保存しています。
- `main` と `sub2` で別ファイルです
- `posts` 配下に Pixiv 投稿を全部列挙します
- 各投稿の `local_images` は手で編集します
- 未紐づけ画像は `data/prompt_post_links.unmatched.json` に分離します
- `local_images` の指定ルール:
- フォルダ指定: そのフォルダ配下の画像すべて
- 画像単体指定: その画像のみ
- 連番指定: `folder/0001.png-0003.png` のように、同一フォルダ内の並び順で範囲展開
- typo や空指定は `apply_prompt_links.py` がエラーにします

JSON のイメージ:

```json
{
  "account_id": "akira",
  "posts": [
    {
      "pixiv_account_id": "sub2",
      "pixiv_illust_id": 143554187,
      "title": "巨乳ボーイッシュな陸上部ちゃんと部室でえっち",
      "local_images": ["20260413_ボーイッシュ部室/png"]
    }
  ],
  "unmatched_local_images": []
}
```

## Run UI

```bash
uv run streamlit run ui/app.py
```

UI内容:
- Followers: 日次推移と日次増減、減少日一覧
- Post Growth: 投稿ごとの経過時間ベース成長曲線
- Growth Compare: 例 `24h` 時点の投稿間比較（metric値、時間あたり伸び、bookmark_rate）
- Latest Posts: 最新投稿と最新スナップショット一覧（タグ表示・bookmark_rate表示）

## Test

```bash
uv run --with-requirements requirements-dev.txt python -m pytest
```

## GitHub Actions

- `collect_sub_weekly.yml`
  - `sub2` 用の weekly 実行
  - `daily` モードを `--account-id sub2` で実行
  - DB変更時のみコミット
- `collect_main_weekly.yml`
  - `main` 用の weekly 実行
  - `daily` モードを `--account-id main` で実行
  - DB変更時のみコミット

### Required secrets

- `PIXIV_ACCOUNTS_JSON` (必須)

## Database schema

- `accounts(account_id, pixiv_user_id, updated_at)`
- `posts(account_id, illust_id, create_date, tags_json, type, page_count, x_restrict, title, updated_at)`
- `post_snapshots(account_id, illust_id, captured_at, bookmark_count, bookmark_rate, like_count, view_count, comment_count, source_mode)`
- `account_daily(account_id, date, followers, following, captured_at)`

`data/prompt_assets.db`（ローカル専用・git 管理外）:

- `prompt_assets(account_id, illust_id, local_path, prompt_text, source_key, model_name, loras_json, pixiv_illust_id, title, metadata_json, imported_at)`

`pixiv_stats.db` に残っている旧 `prompt_assets` は残置・未使用です。

## Notes

- refresh token はコミットせず、`.env`(ローカル) または GitHub Secrets(Actions) で管理してください。
- プライベートリポジトリでも、漏えい・誤公開・フォーク・履歴残存リスクがあるため token 直コミットは非推奨です。
- スクリプトは token をログ出力しません。例外時も token を含む文字列出力は避けてください。
- API仕様の変更が起きた場合は `src/pixiv_client.py` の抽出ロジックを更新してください。
