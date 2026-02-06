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


def _is_recent(create_date_iso: str, hours: int) -> bool:
    create_dt = dtparser.isoparse(create_date_iso)
    if create_dt.tzinfo is None:
        create_dt = create_dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return create_dt.astimezone(timezone.utc) >= now - timedelta(hours=hours)


def sync_posts_and_collect_snapshots(
    conn,
    client: PixivClient,
    account_id: str,
    pixiv_user_id: int,
    source_mode: str,
    recent_hours: int = 24,
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

        is_new = int(illust_id) not in known_ids
        should_collect_snapshot = is_new or _is_recent(create_date_iso, recent_hours)
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


def collect_hourly_recent_snapshots(
    conn,
    client: PixivClient,
    account_id: str,
    recent_hours: int,
    source_mode: str = "hourly",
    max_details_per_account: int = 20,
) -> int:
    since_iso = (datetime.now(timezone.utc) - timedelta(hours=recent_hours)).replace(
        microsecond=0
    ).isoformat()
    recent_ids = db.get_recent_post_ids(conn, account_id, since_iso)
    if not recent_ids:
        return 0

    captured_at = _captured_at_now()
    count = 0
    for illust_id in recent_ids[:max_details_per_account]:
        detail = client.illust_detail(illust_id)
        snapshot = extract_snapshot(detail)
        db.insert_snapshot(
            conn,
            {
                "account_id": account_id,
                "illust_id": illust_id,
                "captured_at": captured_at,
                "bookmark_count": snapshot.get("bookmark_count"),
                "bookmark_rate": _bookmark_rate(snapshot),
                "like_count": snapshot.get("like_count"),
                "view_count": snapshot.get("view_count"),
                "comment_count": snapshot.get("comment_count"),
                "source_mode": source_mode,
            },
        )
        count += 1

    return count
