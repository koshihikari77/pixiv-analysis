from datetime import datetime, timezone

from src import db
from src.pixiv_client import PixivClient, extract_user_stats


def collect_account_daily(
    conn,
    client: PixivClient,
    account_id: str,
    pixiv_user_id: int,
) -> None:
    now = datetime.now(timezone.utc)
    date_str = now.date().isoformat()
    captured_at = now.replace(second=0, microsecond=0).isoformat()

    detail = client.user_detail(pixiv_user_id)
    stats = extract_user_stats(detail)
    db.upsert_account_daily(
        conn=conn,
        account_id=account_id,
        date_yyyy_mm_dd=date_str,
        followers=stats.get("followers"),
        following=stats.get("following"),
        captured_at=captured_at,
    )
