import pandas as pd


def add_follower_delta(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], utc=True, errors="coerce")
    out = out.sort_values("date")
    out["followers"] = pd.to_numeric(out["followers"], errors="coerce")
    out["followers_delta"] = out["followers"].diff()
    out["followers_delta"] = out["followers_delta"].fillna(0).astype("int64")
    return out


def mark_follower_decrease(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    if "followers_delta" not in out.columns:
        out = add_follower_delta(out)
    out["is_decrease"] = out["followers_delta"] < 0
    return out


def to_elapsed_hours_curve(df_snapshots: pd.DataFrame) -> pd.DataFrame:
    if df_snapshots.empty:
        return df_snapshots.copy()

    out = df_snapshots.copy()
    out["create_date"] = pd.to_datetime(out["create_date"], utc=True, errors="coerce")
    out["captured_at"] = pd.to_datetime(out["captured_at"], utc=True, errors="coerce")
    out = out.dropna(subset=["create_date", "captured_at"])
    out["elapsed_hours"] = (
        (out["captured_at"] - out["create_date"]).dt.total_seconds() / 3600.0
    )
    out = out[out["elapsed_hours"] >= 0]
    out = out.sort_values("elapsed_hours")
    return out


def safe_metric_series(df: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    if df.empty or metric_name not in df.columns:
        return pd.DataFrame(columns=["elapsed_hours", metric_name])

    out = df[["elapsed_hours", metric_name]].copy()
    out[metric_name] = pd.to_numeric(out[metric_name], errors="coerce")
    out = out.dropna(subset=["elapsed_hours", metric_name])
    out = out.drop_duplicates(subset=["elapsed_hours"], keep="last")
    out = out.sort_values("elapsed_hours")
    return out
