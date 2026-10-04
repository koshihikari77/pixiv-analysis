import argparse

from src import db
from src.collectors.prompt_assets import import_prompt_assets


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import prompt metadata from local images")
    parser.add_argument("--root", required=True, help="Root directory containing prompt image files")
    parser.add_argument(
        "--account-id",
        default=None,
        help="Optional account_id to apply to all images. If omitted, the first directory segment is used.",
    )
    parser.add_argument(
        "--extensions",
        default="png",
        help="Comma-separated file extensions to import, e.g. png or png,jpg",
    )
    parser.add_argument(
        "--prompt-db-path",
        default=db.default_prompt_db_path(),
        help="Local prompt SQLite DB path (default: PROMPT_DB_PATH or data/prompt_assets.db). "
        "Never point this at the CI-committed data/pixiv_stats.db.",
    )
    parser.add_argument(
        "--derive-missing-id",
        action="store_true",
        help="Derive a stable local ID from the account-relative path when metadata and filename have no ID",
    )
    parser.add_argument(
        "--include-promptless",
        action="store_true",
        help="Import local images even when prompt metadata is absent",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    conn = db.connect_db(args.prompt_db_path)
    db.init_prompt_db(conn)
    suffixes = {f".{ext.strip().lower().lstrip('.')}" for ext in args.extensions.split(",") if ext.strip()}
    summary = import_prompt_assets(
        conn,
        root_dir=args.root,
        account_id=args.account_id,
        suffixes=suffixes,
        derive_missing_id=args.derive_missing_id,
        include_promptless=args.include_promptless,
    )
    db.commit(conn)
    conn.close()
    print(
        "[prompts] imported={imported} seen={seen} skipped_no_prompt={skipped_no_prompt} "
        "skipped_no_illust_id={skipped_no_illust_id} skipped_no_account_id={skipped_no_account_id} "
        "skipped_unsupported={skipped_unsupported} failed={failed}".format(**summary)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
