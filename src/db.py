import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

# prompt_assets (local image paths / prompts / pixiv links) live in a separate,
# git-ignored local DB. The CI-committed pixiv_stats.db must never receive them.
DEFAULT_PROMPT_DB_PATH = "data/prompt_assets.db"
PROMPT_DB_ALIAS = "pdb"

_PROMPT_ASSETS_COLUMNS = """
    account_id TEXT NOT NULL,
    illust_id INTEGER NOT NULL,
    local_path TEXT NOT NULL,
    prompt_text TEXT,
    source_key TEXT,
    model_name TEXT,
    loras_json TEXT,
    pixiv_illust_id INTEGER,
    title TEXT,
    metadata_json TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    PRIMARY KEY (account_id, illust_id, local_path)
"""


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
            bookmark_rate REAL,
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
    _ensure_post_snapshots_migration(conn)
    conn.commit()


def default_prompt_db_path() -> str:
    return os.environ.get("PROMPT_DB_PATH", DEFAULT_PROMPT_DB_PATH)


def init_prompt_db(conn: sqlite3.Connection) -> None:
    """Create prompt_assets in the local prompt DB (not in pixiv_stats.db)."""
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS prompt_assets ("""
        + _PROMPT_ASSETS_COLUMNS
        + """);
        """
    )
    _ensure_prompt_assets_migration(conn)
    conn.commit()


def attach_prompt_db(
    conn: sqlite3.Connection,
    prompt_db_path: Optional[str],
    alias: str = PROMPT_DB_ALIAS,
) -> bool:
    """ATTACH the local prompt DB as ``alias`` for read-side joins.

    When the file does not exist, an empty in-memory schema is attached under
    the same alias so queries referencing ``<alias>.prompt_assets`` still run.
    Returns True when a real prompt DB file was attached.
    """
    if prompt_db_path and Path(prompt_db_path).is_file():
        conn.execute(f"ATTACH DATABASE ? AS {alias}", (str(prompt_db_path),))
        has_table = conn.execute(
            f"SELECT 1 FROM {alias}.sqlite_master WHERE type='table' AND name='prompt_assets'"
        ).fetchone()
        if has_table:
            return True
        conn.execute(f"DETACH DATABASE {alias}")
    conn.execute(f"ATTACH DATABASE ':memory:' AS {alias}")
    conn.execute(
        f"""
        CREATE TABLE {alias}.prompt_assets ({_PROMPT_ASSETS_COLUMNS})
        """
    )
    return False


def _ensure_post_snapshots_migration(conn: sqlite3.Connection) -> None:
    cols = conn.execute("PRAGMA table_info(post_snapshots)").fetchall()
    col_names = {r["name"] for r in cols}
    if "bookmark_rate" not in col_names:
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN bookmark_rate REAL")


def _ensure_prompt_assets_migration(conn: sqlite3.Connection) -> None:
    cols = conn.execute("PRAGMA table_info(prompt_assets)").fetchall()
    col_names = {r["name"] for r in cols}
    if "model_name" not in col_names:
        conn.execute("ALTER TABLE prompt_assets ADD COLUMN model_name TEXT")
    if "loras_json" not in col_names:
        conn.execute("ALTER TABLE prompt_assets ADD COLUMN loras_json TEXT")
    if "pixiv_illust_id" not in col_names:
        conn.execute("ALTER TABLE prompt_assets ADD COLUMN pixiv_illust_id INTEGER")
    if "title" not in col_names:
        conn.execute("ALTER TABLE prompt_assets ADD COLUMN title TEXT")


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
            account_id, illust_id, captured_at, bookmark_count, bookmark_rate, like_count, view_count, comment_count, source_mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["account_id"],
            row["illust_id"],
            row["captured_at"],
            row.get("bookmark_count"),
            row.get("bookmark_rate"),
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


def upsert_prompt_asset(conn: sqlite3.Connection, row: Dict) -> None:
    conn.execute(
        """
        INSERT INTO prompt_assets(
            account_id, illust_id, local_path, prompt_text, source_key, model_name, loras_json, pixiv_illust_id, title, metadata_json, imported_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_id, illust_id, local_path) DO UPDATE SET
            prompt_text=excluded.prompt_text,
            source_key=excluded.source_key,
            model_name=excluded.model_name,
            loras_json=excluded.loras_json,
            pixiv_illust_id=excluded.pixiv_illust_id,
            title=excluded.title,
            metadata_json=excluded.metadata_json,
            imported_at=excluded.imported_at
        """,
        (
            row["account_id"],
            row["illust_id"],
            row["local_path"],
            row.get("prompt_text"),
            row.get("source_key"),
            row.get("model_name"),
            row.get("loras_json"),
            row.get("pixiv_illust_id"),
            row.get("title"),
            row["metadata_json"],
            row.get("imported_at", utc_now_iso()),
        ),
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
