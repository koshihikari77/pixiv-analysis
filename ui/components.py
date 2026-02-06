import altair as alt
import pandas as pd
import streamlit as st


def render_follower_charts(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("フォロワー日次データがありません。")
        return

    line = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("followers:Q", title="Followers"),
            tooltip=["date:T", "followers:Q", "followers_delta:Q"],
        )
        .properties(height=260)
    )

    bars = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("followers_delta:Q", title="Daily Delta"),
            color=alt.condition(
                alt.datum.followers_delta < 0,
                alt.value("#c83f3f"),
                alt.value("#2b7a4b"),
            ),
            tooltip=["date:T", "followers_delta:Q"],
        )
        .properties(height=180)
    )

    st.altair_chart(line, width="stretch")
    st.altair_chart(bars, width="stretch")


def render_growth_curve(df_metric: pd.DataFrame, metric_name: str) -> None:
    if df_metric.empty:
        st.info("選択投稿のスナップショットが不足しています。")
        return

    chart = (
        alt.Chart(df_metric)
        .mark_line(point=True)
        .encode(
            x=alt.X("elapsed_hours:Q", title="Hours Since Post"),
            y=alt.Y(f"{metric_name}:Q", title=metric_name),
            tooltip=["elapsed_hours:Q", f"{metric_name}:Q"],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, width="stretch")


def render_latest_posts_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("投稿データがありません。")
        return
    st.dataframe(df, width="stretch", hide_index=True)
