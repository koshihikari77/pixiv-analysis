import argparse
import os

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
        "--db-path",
        default=os.environ.get("DB_PATH", "data/pixiv_stats.db"),
        help="SQLite database path",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    conn = db.connect_db(args.db_path)
    db.init_db(conn)
    summary = import_prompt_assets(conn, root_dir=args.root, account_id=args.account_id)
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
