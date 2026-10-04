import sqlite3

from src import db as core_db
from ui.data_access import (
    has_required_tables,
    prompt_db_available,
    load_accounts,
    load_follower_daily,
    load_growth_benchmark,
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
        "INSERT INTO post_snapshots(account_id,illust_id,captured_at,bookmark_count,bookmark_rate,like_count,view_count,comment_count,source_mode) VALUES ('main',10,'2026-02-06T01:00:00+00:00',1,NULL,2,4,4,'daily')"
    )
    conn.commit()
    conn.close()


def _setup_prompt_db(prompt_db_path):
    conn = core_db.connect_db(prompt_db_path)
    core_db.init_prompt_db(conn)
    core_db.upsert_prompt_asset(
        conn,
        {
            "account_id": "akira",
            "illust_id": 10,
            "local_path": "/tmp/10.png",
            "prompt_text": "a test prompt",
            "source_key": "prompt",
            "pixiv_illust_id": 10,
            "title": "t1",
            "metadata_json": "{}",
            "imported_at": "2026-02-06T01:00:00+00:00",
        },
    )
    conn.commit()
    conn.close()


def _add_legacy_prompt_table(db_path):
    """Simulate the unused legacy prompt_assets left in pixiv_stats.db."""
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE prompt_assets (
            account_id TEXT NOT NULL,
            illust_id INTEGER NOT NULL,
            local_path TEXT NOT NULL,
            prompt_text TEXT,
            source_key TEXT,
            pixiv_illust_id INTEGER,
            title TEXT,
            metadata_json TEXT NOT NULL,
            imported_at TEXT NOT NULL,
            PRIMARY KEY (account_id, illust_id, local_path)
        );
        INSERT INTO prompt_assets VALUES
            ('akira',99,'/mnt/c/old.png','legacy prompt','prompt',10,'t1','{}','2027-01-01T00:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()


def test_data_access_without_prompt_db(tmp_path):
    db_path = tmp_path / "pixiv_stats.db"
    _setup_db(str(db_path))
    _add_legacy_prompt_table(str(db_path))

    assert has_required_tables(str(db_path)) is True
    assert prompt_db_available(str(db_path)) is False

    posts = load_posts_with_latest_snapshot(str(db_path), account_id="main", limit=10)
    assert len(posts) == 1
    # legacy table in the stats DB must be ignored
    assert posts["prompt_text"].isna().all()

    snaps = load_post_snapshots(str(db_path), account_id="main", illust_id=10)
    assert len(snaps) == 1
    assert snaps["prompt_text"].isna().all()

    growth = load_growth_benchmark(
        str(db_path),
        account_id="main",
        target_hours=1.0,
        metric="bookmark_count",
        tolerance_hours=1.0,
    )
    assert len(growth) == 1
    assert not (tmp_path / "prompt_assets.db").exists()


def test_data_access_queries(tmp_path):
    db_path = tmp_path / "ui.db"
    _setup_db(str(db_path))
    _setup_prompt_db(str(tmp_path / "prompt_assets.db"))
    assert prompt_db_available(str(db_path)) is True

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
    assert int(posts.iloc[0]["view_count"]) == 4
    assert float(posts.iloc[0]["bookmark_rate"]) == 0.25
    assert posts.iloc[0]["prompt_text"] == "a test prompt"

    snaps = load_post_snapshots(str(db_path), account_id="main", illust_id=10)
    assert len(snaps) == 1
    assert int(snaps.iloc[0]["bookmark_count"]) == 1
    assert float(snaps.iloc[0]["bookmark_rate"]) == 0.25
    assert snaps.iloc[0]["prompt_text"] == "a test prompt"

    growth = load_growth_benchmark(
        str(db_path),
        account_id="main",
        target_hours=1.0,
        metric="bookmark_count",
        tolerance_hours=1.0,
    )
    assert len(growth) == 1
    assert float(growth.iloc[0]["metric_per_hour_target"]) == 1.0
