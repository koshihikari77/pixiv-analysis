import pandas as pd

from ui.transform import add_follower_delta, mark_follower_decrease, safe_metric_series, to_elapsed_hours_curve


def test_add_follower_delta_and_decrease_flag():
    df = pd.DataFrame(
        {
            "date": ["2026-02-04", "2026-02-05", "2026-02-06"],
            "followers": [100, 98, 110],
        }
    )

    out = add_follower_delta(df)
    out = mark_follower_decrease(out)

    assert out.iloc[0]["followers_delta"] == 0
    assert out.iloc[1]["followers_delta"] == -2
    assert bool(out.iloc[1]["is_decrease"]) is True
    assert out.iloc[2]["followers_delta"] == 12


def test_elapsed_hours_curve_and_metric_series():
    df = pd.DataFrame(
        {
            "create_date": ["2026-02-06T00:00:00+00:00", "2026-02-06T00:00:00+00:00"],
            "captured_at": ["2026-02-06T01:00:00+00:00", "2026-02-06T03:00:00+00:00"],
            "view_count": [10, 30],
        }
    )

    curve = to_elapsed_hours_curve(df)
    metric = safe_metric_series(curve, "view_count")

    assert list(metric["elapsed_hours"]) == [1.0, 3.0]
    assert list(metric["view_count"]) == [10, 30]
