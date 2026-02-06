from src import db


def test_init_db_creates_required_tables(tmp_path):
    conn = db.connect_db(str(tmp_path / "test.db"))
    db.init_db(conn)

    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = {r["name"] for r in rows}

    assert "accounts" in names
    assert "posts" in names
    assert "post_snapshots" in names
    assert "account_daily" in names


def test_post_snapshot_insert_is_idempotent(tmp_path):
    conn = db.connect_db(str(tmp_path / "test.db"))
    db.init_db(conn)

    row = {
        "account_id": "main",
        "illust_id": 100,
        "captured_at": "2026-02-06T00:00:00+00:00",
        "bookmark_count": 10,
        "like_count": 20,
        "view_count": 30,
        "comment_count": 1,
        "source_mode": "daily",
    }
    db.insert_snapshot(conn, row)
    db.insert_snapshot(conn, row)
    db.commit(conn)

    count = conn.execute(
        "SELECT COUNT(*) AS c FROM post_snapshots WHERE account_id='main' AND illust_id=100"
    ).fetchone()["c"]
    assert count == 1


def test_account_daily_upsert_updates_same_day(tmp_path):
    conn = db.connect_db(str(tmp_path / "test.db"))
    db.init_db(conn)

    db.upsert_account_daily(
        conn,
        account_id="main",
        date_yyyy_mm_dd="2026-02-06",
        followers=100,
        following=50,
        captured_at="2026-02-06T01:00:00+00:00",
    )
    db.upsert_account_daily(
        conn,
        account_id="main",
        date_yyyy_mm_dd="2026-02-06",
        followers=95,
        following=50,
        captured_at="2026-02-06T02:00:00+00:00",
    )
    db.commit(conn)

    row = conn.execute(
        "SELECT followers, following FROM account_daily WHERE account_id='main' AND date='2026-02-06'"
    ).fetchone()
    assert row["followers"] == 95
    assert row["following"] == 50
