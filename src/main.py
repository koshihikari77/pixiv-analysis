import argparse
from pathlib import Path

from src import db
from src.collectors.accounts import collect_account_daily
from src.collectors.posts import sync_posts_and_collect_snapshots
from src.config import AccountModel, load_settings
from src.pixiv_client import PixivClient


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pixiv account stats collector")
    parser.add_argument(
        "--mode",
        choices=["daily", "manual"],
        required=True,
        help="Collector mode",
    )
    parser.add_argument(
        "--account-id",
        default=None,
        help="Optional single account_id to run",
    )
    parser.add_argument(
        "--pixiv-user-id",
        type=int,
        default=None,
        help=(
            "Pixiv user ID for an account not present in PIXIV_ACCOUNTS_JSON. "
            "The first configured refresh token is used for API authentication."
        ),
    )
    return parser.parse_args()


def _select_accounts(settings, account_id: str | None, pixiv_user_id: int | None):
    if not account_id:
        if pixiv_user_id is not None:
            raise ValueError("--pixiv-user-id requires --account-id")
        return settings.accounts

    selected = [a for a in settings.accounts if a.account_id == account_id]
    if selected:
        if pixiv_user_id is not None and selected[0].pixiv_user_id != pixiv_user_id:
            raise ValueError(f"pixiv_user_id does not match configured account: {account_id}")
        return selected

    if pixiv_user_id is None:
        raise ValueError(f"account_id not found: {account_id}")

    auth_account = settings.accounts[0]
    return [
        AccountModel(
            account_id=account_id,
            pixiv_user_id=pixiv_user_id,
            refresh_token=auth_account.refresh_token,
        )
    ]


def main() -> int:
    args = _parse_args()
    settings = load_settings()

    selected_accounts = _select_accounts(settings, args.account_id, args.pixiv_user_id)

    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect_db(settings.db_path)
    db.init_db(conn)

    for account in selected_accounts:
        db.upsert_account(conn, account.account_id, account.pixiv_user_id)
        client = PixivClient(
            refresh_token=account.refresh_token,
            min_interval_sec=settings.api_min_interval_sec,
            jitter_sec=settings.api_jitter_sec,
        )

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
            max_snapshot_age_days=settings.snapshot_max_age_days,
            max_pages=settings.user_illusts_max_pages,
            max_details_per_account=settings.max_details_per_account,
        )
        print(f"[{account.account_id}] {args.mode} collection done.")

    db.commit(conn)
    conn.close()
    return 0
