import sqlite3

from ui.data_access import (
    has_required_tables,
    load_accounts,
    load_follower_daily,
    load_post_snapshots,
    load_posts_with_latest_snapshot,
)


def _setup_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE accounts (
            account_id TEXT PRIMARY KEY,
            pixiv_user_id INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE posts (
            account_id TEXT NOT NULL,
            illust_id INTEGER NOT NULL,
            create_date TEXT NOT NULL,
            tags_json TEXT NOT NULL,
            type TEXT,
            page_count INTEGER,
            x_restrict INTEGER,
            title TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (account_id, illust_id)
        );
        CREATE TABLE post_snapshots (
            account_id TEXT NOT NULL,
            illust_id INTEGER NOT NULL,
            captured_at TEXT NOT NULL,
            bookmark_count INTEGER,
            bookmark_rate REAL,
            like_count INTEGER,
            view_count INTEGER,
            comment_count INTEGER,
            source_mode TEXT NOT NULL,
            PRIMARY KEY (account_id, illust_id, captured_at, source_mode)
        );
        CREATE TABLE account_daily (
            account_id TEXT NOT NULL,
            date TEXT NOT NULL,
            followers INTEGER,
            following INTEGER,
            captured_at TEXT NOT NULL,
            PRIMARY KEY (account_id, date)
        );
        """
    )
    conn.execute(
        "INSERT INTO accounts(account_id, pixiv_user_id, updated_at) VALUES ('main', 123, '2026-02-06T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO account_daily(account_id, date, followers, following, captured_at) VALUES ('main','2026-02-06',100,30,'2026-02-06T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO posts(account_id,illust_id,create_date,tags_json,type,page_count,x_restrict,title,updated_at) VALUES ('main',10,'2026-02-06T00:00:00+00:00','[]','illust',1,0,'t1','2026-02-06T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO post_snapshots(account_id,illust_id,captured_at,bookmark_count,bookmark_rate,like_count,view_count,comment_count,source_mode) VALUES ('main',10,'2026-02-06T01:00:00+00:00',1,0.3333,2,3,4,'daily')"
    )
    conn.commit()
    conn.close()


def test_data_access_queries(tmp_path):
    db_path = tmp_path / "ui.db"
    _setup_db(str(db_path))

    assert has_required_tables(str(db_path)) is True

    accounts = load_accounts(str(db_path))
    assert len(accounts) == 1
    assert accounts.iloc[0]["account_id"] == "main"

    followers = load_follower_daily(str(db_path), "main")
    assert len(followers) == 1
    assert int(followers.iloc[0]["followers"]) == 100

    posts = load_posts_with_latest_snapshot(str(db_path), account_id="main", limit=10)
    assert len(posts) == 1
    assert int(posts.iloc[0]["illust_id"]) == 10
    assert int(posts.iloc[0]["view_count"]) == 3

    snaps = load_post_snapshots(str(db_path), account_id="main", illust_id=10)
    assert len(snaps) == 1
    assert int(snaps.iloc[0]["bookmark_count"]) == 1
