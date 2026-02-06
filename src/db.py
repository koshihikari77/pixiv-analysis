import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def connect_db(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS accounts (
            account_id TEXT PRIMARY KEY,
            pixiv_user_id INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS posts (
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
        CREATE TABLE IF NOT EXISTS post_snapshots (
            account_id TEXT NOT NULL,
            illust_id INTEGER NOT NULL,
            captured_at TEXT NOT NULL,
            bookmark_count INTEGER,
            like_count INTEGER,
            view_count INTEGER,
            comment_count INTEGER,
            source_mode TEXT NOT NULL,
            PRIMARY KEY (account_id, illust_id, captured_at, source_mode)
        );
        CREATE TABLE IF NOT EXISTS account_daily (
            account_id TEXT NOT NULL,
            date TEXT NOT NULL,
            followers INTEGER,
            following INTEGER,
            captured_at TEXT NOT NULL,
            PRIMARY KEY (account_id, date)
        );
        """
    )
    conn.commit()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def upsert_account(conn: sqlite3.Connection, account_id: str, pixiv_user_id: int) -> None:
    conn.execute(
        """
        INSERT INTO accounts(account_id, pixiv_user_id, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(account_id) DO UPDATE SET
            pixiv_user_id=excluded.pixiv_user_id,
            updated_at=excluded.updated_at
        """,
        (account_id, pixiv_user_id, utc_now_iso()),
    )


def get_account_illust_ids(conn: sqlite3.Connection, account_id: str) -> set[int]:
    rows = conn.execute(
        "SELECT illust_id FROM posts WHERE account_id = ?",
        (account_id,),
    ).fetchall()
    return {int(r["illust_id"]) for r in rows}


def upsert_post(conn: sqlite3.Connection, row: Dict) -> None:
    conn.execute(
        """
        INSERT INTO posts(
            account_id, illust_id, create_date, tags_json, type, page_count, x_restrict, title, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_id, illust_id) DO UPDATE SET
            create_date=excluded.create_date,
            tags_json=excluded.tags_json,
            type=excluded.type,
            page_count=excluded.page_count,
            x_restrict=excluded.x_restrict,
            title=excluded.title,
            updated_at=excluded.updated_at
        """,
        (
            row["account_id"],
            row["illust_id"],
            row["create_date"],
            row["tags_json"],
            row.get("type"),
            row.get("page_count"),
            row.get("x_restrict"),
            row.get("title"),
            utc_now_iso(),
        ),
    )


def insert_snapshot(conn: sqlite3.Connection, row: Dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO post_snapshots(
            account_id, illust_id, captured_at, bookmark_count, like_count, view_count, comment_count, source_mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["account_id"],
            row["illust_id"],
            row["captured_at"],
            row.get("bookmark_count"),
            row.get("like_count"),
            row.get("view_count"),
            row.get("comment_count"),
            row["source_mode"],
        ),
    )


def upsert_account_daily(
    conn: sqlite3.Connection,
    account_id: str,
    date_yyyy_mm_dd: str,
    followers: Optional[int],
    following: Optional[int],
    captured_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO account_daily(account_id, date, followers, following, captured_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(account_id, date) DO UPDATE SET
            followers=excluded.followers,
            following=excluded.following,
            captured_at=excluded.captured_at
        """,
        (account_id, date_yyyy_mm_dd, followers, following, captured_at),
    )


def get_recent_post_ids(conn: sqlite3.Connection, account_id: str, since_iso: str) -> List[int]:
    rows = conn.execute(
        """
        SELECT illust_id
        FROM posts
        WHERE account_id = ? AND create_date >= ?
        ORDER BY create_date DESC
        """,
        (account_id, since_iso),
    ).fetchall()
    return [int(r["illust_id"]) for r in rows]


def commit(conn: sqlite3.Connection) -> None:
    conn.commit()
