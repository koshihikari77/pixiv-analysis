import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure project root is importable when launched via `streamlit run ui/app.py`.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

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
    load_growth_benchmark,
    load_post_snapshots,
    load_posts_with_latest_snapshot,
)
from ui.transform import (
    add_follower_delta,
    mark_follower_decrease,
    parse_tags_json,
    safe_metric_series,
    to_elapsed_hours_curve,
)


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
if not follower_df.empty and (follower_df["followers"].fillna(0) == 0).all():
    st.warning(
        "followers が全日0です。pixiv APIの返却値が0の可能性があります。"
        " user_detail のレスポンス確認か user_id の再確認を推奨します。"
    )

if not follower_df.empty:
    decreases = follower_df[follower_df["is_decrease"]][
        ["date", "followers", "followers_delta"]
    ].copy()
    decreases["date"] = decreases["date"].dt.strftime("%Y-%m-%d")
    st.markdown("**Follower Decrease Days**")
    st.dataframe(decreases, width="stretch", hide_index=True)

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
posts_df = parse_tags_json(posts_df)

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
st.subheader("Growth Compare (Across Illustrations)")

col1, col2, col3, col4 = st.columns(4)
with col1:
    benchmark_hours = st.number_input("Target Hours Since Post", value=24.0, min_value=1.0, step=1.0)
with col2:
    benchmark_metric = st.selectbox(
        "Benchmark Metric",
        options=["bookmark_count", "view_count", "like_count", "comment_count"],
        index=0,
    )
with col3:
    rank_by = st.selectbox(
        "Rank By",
        options=["metric_per_hour_target", "metric_value", "bookmark_rate", "target_diff_hours"],
        index=0,
    )
with col4:
    tolerance_hours = st.number_input("Tolerance (hours)", value=6.0, min_value=0.5, step=0.5)

growth_compare_df = load_growth_benchmark(
    db_path=db_path,
    account_id=selected_account,
    target_hours=float(benchmark_hours),
    metric=benchmark_metric,
    post_type=post_type,
    tolerance_hours=float(tolerance_hours),
    limit=300,
)
growth_compare_df = parse_tags_json(growth_compare_df)

if growth_compare_df.empty:
    st.info("比較用のスナップショットがありません。")
else:
    for col in [
        "bookmark_rate",
        "elapsed_hours",
        "metric_per_hour_target",
        "metric_per_hour_actual",
        "target_diff_hours",
        "metric_value",
    ]:
        if col in growth_compare_df.columns:
            growth_compare_df[col] = pd.to_numeric(growth_compare_df[col], errors="coerce")
    growth_compare_df = growth_compare_df.sort_values(rank_by, ascending=False, na_position="last")
    growth_compare_df["bookmark_rate"] = (growth_compare_df["bookmark_rate"] * 100.0).round(2)
    growth_compare_df["elapsed_hours"] = growth_compare_df["elapsed_hours"].round(2)
    growth_compare_df["metric_per_hour_target"] = growth_compare_df["metric_per_hour_target"].round(2)
    growth_compare_df["metric_per_hour_actual"] = growth_compare_df["metric_per_hour_actual"].round(2)
    growth_compare_df["target_diff_hours"] = growth_compare_df["target_diff_hours"].round(2)
    growth_compare_df["metric_value"] = growth_compare_df["metric_value"].round(2)
    show_cols = [
        "account_id",
        "illust_id",
        "title",
        "tags",
        "type",
        "elapsed_hours",
        "target_diff_hours",
        "metric_value",
        "metric_per_hour_target",
        "metric_per_hour_actual",
        "bookmark_rate",
        "bookmark_count",
        "view_count",
        "captured_at",
    ]
    st.dataframe(
        growth_compare_df[show_cols].rename(
            columns={
                "bookmark_rate": "bookmark_rate(%)",
                "metric_per_hour_target": f"{benchmark_metric}/h@{benchmark_hours:.0f}h",
                "metric_per_hour_actual": f"{benchmark_metric}/h(actual)",
            }
        ),
        width="stretch",
        hide_index=True,
    )

st.divider()
st.subheader("Latest Posts")

latest_display = posts_df.copy() if not posts_df.empty else pd.DataFrame()
if not latest_display.empty:
    show_cols = [
        "account_id",
        "illust_id",
        "title",
        "tags",
        "create_date",
        "type",
        "x_restrict",
        "bookmark_count",
        "bookmark_rate",
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
    latest_display["bookmark_rate"] = (pd.to_numeric(latest_display["bookmark_rate"], errors="coerce") * 100.0).round(2)

render_latest_posts_table(latest_display)
