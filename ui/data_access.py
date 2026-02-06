import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd


REQUIRED_TABLES = {"accounts", "posts", "post_snapshots", "account_daily"}


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def db_exists(db_path: str) -> bool:
    return Path(db_path).exists()


def has_required_tables(db_path: str) -> bool:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        names = {r["name"] for r in rows}
        return REQUIRED_TABLES.issubset(names)
    finally:
        conn.close()


def load_accounts(db_path: str) -> pd.DataFrame:
    conn = _connect(db_path)
    try:
        return pd.read_sql_query(
            "SELECT account_id, pixiv_user_id, updated_at FROM accounts ORDER BY account_id",
            conn,
        )
    finally:
        conn.close()


def load_follower_daily(db_path: str, account_id: str) -> pd.DataFrame:
    conn = _connect(db_path)
    try:
        if account_id == "ALL":
            query = """
            SELECT
                date,
                SUM(COALESCE(followers, 0)) AS followers,
                SUM(COALESCE(following, 0)) AS following,
                MAX(captured_at) AS captured_at,
                'ALL' AS account_id
            FROM account_daily
            GROUP BY date
            ORDER BY date
            """
            return pd.read_sql_query(query, conn)

        return pd.read_sql_query(
            """
            SELECT account_id, date, followers, following, captured_at
            FROM account_daily
            WHERE account_id = ?
            ORDER BY date
            """,
            conn,
            params=(account_id,),
        )
    finally:
        conn.close()


def load_posts_with_latest_snapshot(
    db_path: str,
    account_id: str,
    limit: int = 200,
    post_type: str = "ALL",
) -> pd.DataFrame:
    conn = _connect(db_path)
    try:
        where_parts = []
        params = []

        if account_id != "ALL":
            where_parts.append("p.account_id = ?")
            params.append(account_id)

        if post_type != "ALL":
            where_parts.append("p.type = ?")
            params.append(post_type)

        where_sql = ""
        if where_parts:
            where_sql = "WHERE " + " AND ".join(where_parts)

        query = f"""
        WITH ranked_snapshots AS (
            SELECT
                ps.*,
                ROW_NUMBER() OVER (
                    PARTITION BY ps.account_id, ps.illust_id
                    ORDER BY ps.captured_at DESC, ps.source_mode DESC
                ) AS rn
            FROM post_snapshots ps
        )
        SELECT
            p.account_id,
            p.illust_id,
            p.title,
            p.create_date,
            p.tags_json,
            p.type,
            p.page_count,
            p.x_restrict,
            rs.captured_at,
            rs.bookmark_count,
            rs.like_count,
            rs.view_count,
            rs.comment_count,
            rs.source_mode
        FROM posts p
        LEFT JOIN ranked_snapshots rs
            ON p.account_id = rs.account_id
            AND p.illust_id = rs.illust_id
            AND rs.rn = 1
        {where_sql}
        ORDER BY p.create_date DESC
        LIMIT ?
        """
        params.append(limit)
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def load_post_snapshots(
    db_path: str,
    account_id: str,
    illust_id: int,
) -> pd.DataFrame:
    conn = _connect(db_path)
    try:
        return pd.read_sql_query(
            """
            SELECT
                ps.account_id,
                ps.illust_id,
                ps.captured_at,
                ps.bookmark_count,
                ps.like_count,
                ps.view_count,
                ps.comment_count,
                ps.source_mode,
                p.create_date,
                p.title
            FROM post_snapshots ps
            JOIN posts p
              ON p.account_id = ps.account_id
             AND p.illust_id = ps.illust_id
            WHERE ps.account_id = ? AND ps.illust_id = ?
            ORDER BY ps.captured_at ASC
            """,
            conn,
            params=(account_id, illust_id),
        )
    finally:
        conn.close()
