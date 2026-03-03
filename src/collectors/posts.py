import json
from datetime import datetime, timedelta, timezone

from dateutil import parser as dtparser

from src import db
from src.pixiv_client import PixivClient, extract_post_meta, extract_snapshot


def _bookmark_rate(snapshot: dict) -> float | None:
    bookmarks = snapshot.get("bookmark_count")
    views = snapshot.get("view_count")
    if bookmarks is None or views is None:
        return None
    if views <= 0:
        return None
    return float(bookmarks) / float(views)


def _to_utc_iso(raw_datetime: str) -> str:
    parsed = dtparser.isoparse(raw_datetime)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _captured_at_now() -> str:
    return datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()


def _is_within_days(create_date_iso: str, days: int) -> bool:
    create_dt = dtparser.isoparse(create_date_iso)
    if create_dt.tzinfo is None:
        create_dt = create_dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return create_dt.astimezone(timezone.utc) >= now - timedelta(days=days)


def sync_posts_and_collect_snapshots(
    conn,
    client: PixivClient,
    account_id: str,
    pixiv_user_id: int,
    source_mode: str,
    max_snapshot_age_days: int = 60,
    max_pages: int = 3,
    max_details_per_account: int = 20,
) -> None:
    known_ids = db.get_account_illust_ids(conn, account_id)
    illusts = client.list_user_illusts(pixiv_user_id, max_pages=max_pages)
    captured_at = _captured_at_now()
    detail_count = 0

    for illust in illusts:
        meta = extract_post_meta(illust)
        illust_id = meta.get("illust_id")
        raw_create_date = meta.get("create_date")

        if not illust_id or not raw_create_date:
            continue

        create_date_iso = _to_utc_iso(raw_create_date)
        db.upsert_post(
            conn,
            {
                "account_id": account_id,
                "illust_id": int(illust_id),
                "create_date": create_date_iso,
                "tags_json": json.dumps(meta.get("tags", []), ensure_ascii=False),
                "type": meta.get("type"),
                "page_count": meta.get("page_count"),
                "x_restrict": meta.get("x_restrict"),
                "title": meta.get("title"),
            },
        )

        should_collect_snapshot = _is_within_days(create_date_iso, max_snapshot_age_days)
        if not should_collect_snapshot:
            continue
        if detail_count >= max_details_per_account:
            continue

        detail = client.illust_detail(int(illust_id))
        snapshot = extract_snapshot(detail)
        db.insert_snapshot(
            conn,
            {
                "account_id": account_id,
                "illust_id": int(illust_id),
                "captured_at": captured_at,
                "bookmark_count": snapshot.get("bookmark_count"),
                "bookmark_rate": _bookmark_rate(snapshot),
                "like_count": snapshot.get("like_count"),
                "view_count": snapshot.get("view_count"),
                "comment_count": snapshot.get("comment_count"),
                "source_mode": source_mode,
            },
        )
        detail_count += 1
