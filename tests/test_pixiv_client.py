from src.pixiv_client import extract_user_stats


def test_extract_user_stats_prefers_profile_total_follow_users():
    payload = {
        "user": {"id": 1},
        "profile": {"total_follow_users": 123, "total_following": 45},
    }
    out = extract_user_stats(payload)
    assert out["followers"] == 123
    assert out["following"] == 45


def test_extract_user_stats_uses_fallback_keys():
    payload = {
        "user": {"followers": 10, "following": 3},
        "profile": {},
        "profile_publicity": {},
    }
    out = extract_user_stats(payload)
    assert out["followers"] == 10
    assert out["following"] == 3
