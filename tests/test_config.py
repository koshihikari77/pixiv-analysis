import pytest

from src.config import load_settings


def test_load_settings_parses_accounts_json(monkeypatch):
    monkeypatch.setenv(
        "PIXIV_ACCOUNTS_JSON",
        '[{"account_id":"main","pixiv_user_id":123,"refresh_token":"token"}]',
    )
    monkeypatch.setenv("DB_PATH", "data/test.db")
    monkeypatch.setenv("ENABLE_HOURLY", "true")
    monkeypatch.setenv("SNAPSHOT_RECENT_HOURS", "12")
    monkeypatch.setenv("USER_ILLUSTS_MAX_PAGES", "2")
    monkeypatch.setenv("MAX_DETAILS_PER_ACCOUNT", "15")
    monkeypatch.setenv("API_MIN_INTERVAL_SEC", "1.1")
    monkeypatch.setenv("API_JITTER_SEC", "0.2")
    monkeypatch.setenv("TZ", "UTC")

    settings = load_settings()

    assert settings.db_path == "data/test.db"
    assert settings.enable_hourly is True
    assert settings.snapshot_recent_hours == 12
    assert settings.user_illusts_max_pages == 2
    assert settings.max_details_per_account == 15
    assert settings.api_min_interval_sec == 1.1
    assert settings.api_jitter_sec == 0.2
    assert len(settings.accounts) == 1
    assert settings.accounts[0].account_id == "main"
    assert settings.accounts[0].pixiv_user_id == 123


def test_load_settings_reads_dotenv(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        'PIXIV_ACCOUNTS_JSON=[{"account_id":"sub1","pixiv_user_id":456,"refresh_token":"token2"}]\n',
        encoding="utf-8",
    )
    monkeypatch.delenv("PIXIV_ACCOUNTS_JSON", raising=False)
    monkeypatch.setenv("ENV_FILE", str(env_file))

    settings = load_settings()

    assert len(settings.accounts) == 1
    assert settings.accounts[0].account_id == "sub1"
    assert settings.accounts[0].pixiv_user_id == 456


def test_load_settings_requires_accounts_json(monkeypatch):
    monkeypatch.delenv("PIXIV_ACCOUNTS_JSON", raising=False)
    monkeypatch.setenv("ENV_FILE", "")

    with pytest.raises(ValueError):
        load_settings()
