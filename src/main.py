import argparse
from pathlib import Path

from src import db
from src.collectors.accounts import collect_account_daily
from src.collectors.posts import (
    collect_hourly_recent_snapshots,
    sync_posts_and_collect_snapshots,
)
from src.config import load_settings
from src.pixiv_client import PixivClient


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pixiv account stats collector")
    parser.add_argument(
        "--mode",
        choices=["daily", "hourly", "manual"],
        required=True,
        help="Collector mode",
    )
    parser.add_argument(
        "--account-id",
        default=None,
        help="Optional single account_id to run",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    settings = load_settings()

    selected_accounts = settings.accounts
    if args.account_id:
        selected_accounts = [a for a in settings.accounts if a.account_id == args.account_id]
        if not selected_accounts:
            raise ValueError(f"account_id not found: {args.account_id}")

    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect_db(settings.db_path)
    db.init_db(conn)

    if args.mode == "hourly" and not settings.enable_hourly:
        print("Hourly job is disabled by ENABLE_HOURLY=false. Skipping.")
        return 0

    for account in selected_accounts:
        db.upsert_account(conn, account.account_id, account.pixiv_user_id)
        client = PixivClient(
            refresh_token=account.refresh_token,
            min_interval_sec=settings.api_min_interval_sec,
            jitter_sec=settings.api_jitter_sec,
        )

        if args.mode in {"daily", "manual"}:
            collect_account_daily(
                conn=conn,
                client=client,
                account_id=account.account_id,
                pixiv_user_id=account.pixiv_user_id,
            )
            sync_posts_and_collect_snapshots(
                conn=conn,
                client=client,
                account_id=account.account_id,
                pixiv_user_id=account.pixiv_user_id,
                source_mode=args.mode,
                recent_hours=settings.snapshot_recent_hours,
                max_pages=settings.user_illusts_max_pages,
                max_details_per_account=settings.max_details_per_account,
            )
            print(f"[{account.account_id}] daily/manual collection done.")
            continue

        if args.mode == "hourly":
            snapshot_count = collect_hourly_recent_snapshots(
                conn=conn,
                client=client,
                account_id=account.account_id,
                recent_hours=settings.snapshot_recent_hours,
                source_mode="hourly",
                max_details_per_account=settings.max_details_per_account,
            )
            print(f"[{account.account_id}] hourly snapshots: {snapshot_count}")

    db.commit(conn)
    conn.close()
    return 0
