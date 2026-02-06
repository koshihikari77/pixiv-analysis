import os

import pandas as pd
import streamlit as st

from ui.components import (
    render_follower_charts,
    render_growth_curve,
    render_latest_posts_table,
)
from ui.data_access import (
    db_exists,
    has_required_tables,
    load_accounts,
    load_follower_daily,
    load_post_snapshots,
    load_posts_with_latest_snapshot,
)
from ui.transform import add_follower_delta, mark_follower_decrease, safe_metric_series, to_elapsed_hours_curve


st.set_page_config(page_title="Pixiv Analysis UI", layout="wide")
st.title("Pixiv Account Analysis")


def _default_db_path() -> str:
    return os.environ.get("UI_DB_PATH", "data/pixiv_stats.db")


def _ui_tz() -> str:
    return os.environ.get("UI_TZ", "UTC")


with st.sidebar:
    st.header("Filters")
    db_path = st.text_input("DB Path", value=_default_db_path())

if not db_exists(db_path):
    st.error(f"DB file not found: {db_path}")
    st.stop()

if not has_required_tables(db_path):
    st.error("Required tables are missing. Run collector first.")
    st.stop()

accounts_df = load_accounts(db_path)
account_options = ["ALL"] + accounts_df["account_id"].tolist()

with st.sidebar:
    selected_account = st.selectbox("Account", options=account_options, index=0)
    days = st.selectbox("Date Range", options=[30, 90, 365, 9999], index=1)

st.subheader("Followers")
follower_df = load_follower_daily(db_path, selected_account)
follower_df = add_follower_delta(follower_df)
follower_df = mark_follower_decrease(follower_df)
if not follower_df.empty and days != 9999:
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
    follower_df = follower_df[follower_df["date"] >= cutoff]

render_follower_charts(follower_df)

if not follower_df.empty:
    decreases = follower_df[follower_df["is_decrease"]][
        ["date", "followers", "followers_delta"]
    ].copy()
    decreases["date"] = decreases["date"].dt.strftime("%Y-%m-%d")
    st.markdown("**Follower Decrease Days**")
    st.dataframe(decreases, use_container_width=True, hide_index=True)

st.divider()
st.subheader("Post Growth")

with st.sidebar:
    post_type = st.selectbox("Post Type", options=["ALL", "illust", "manga", "ugoira"], index=0)

posts_df = load_posts_with_latest_snapshot(
    db_path=db_path,
    account_id=selected_account,
    limit=300,
    post_type=post_type,
)

if posts_df.empty:
    st.info("表示できる投稿がありません。")
else:
    posts_df["label"] = posts_df.apply(
        lambda r: f"{r['account_id']} / {r['illust_id']} / {r['title'] or '(untitled)'}",
        axis=1,
    )
    selected_label = st.selectbox("Post", options=posts_df["label"].tolist(), index=0)
    selected_row = posts_df[posts_df["label"] == selected_label].iloc[0]

    metric = st.radio(
        "Metric",
        options=["bookmark_count", "like_count", "view_count", "comment_count"],
        horizontal=True,
    )

    snap_df = load_post_snapshots(
        db_path,
        account_id=selected_row["account_id"],
        illust_id=int(selected_row["illust_id"]),
    )
    curve_df = to_elapsed_hours_curve(snap_df)
    metric_df = safe_metric_series(curve_df, metric)
    render_growth_curve(metric_df, metric)

st.divider()
st.subheader("Latest Posts")

latest_display = posts_df.copy() if not posts_df.empty else pd.DataFrame()
if not latest_display.empty:
    show_cols = [
        "account_id",
        "illust_id",
        "title",
        "create_date",
        "type",
        "x_restrict",
        "bookmark_count",
        "like_count",
        "view_count",
        "comment_count",
        "captured_at",
        "source_mode",
    ]
    latest_display = latest_display[show_cols]
    latest_display["create_date"] = (
        pd.to_datetime(latest_display["create_date"], utc=True, errors="coerce")
        .dt.tz_convert(_ui_tz())
        .dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    )
    latest_display["captured_at"] = (
        pd.to_datetime(latest_display["captured_at"], utc=True, errors="coerce")
        .dt.tz_convert(_ui_tz())
        .dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    )

render_latest_posts_table(latest_display)
