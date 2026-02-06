import json
import os
from dataclasses import dataclass
from typing import List, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, RootModel, ValidationError


class AccountModel(BaseModel):
    account_id: str
    pixiv_user_id: int
    refresh_token: str


class AccountsPayload(RootModel[List[AccountModel]]):
    pass


@dataclass(frozen=True)
class Settings:
    accounts: List[AccountModel]
    db_path: str
    enable_hourly: bool
    snapshot_recent_hours: int
    user_illusts_max_pages: int
    max_details_per_account: int
    api_min_interval_sec: float
    api_jitter_sec: float
    tz: str


def _parse_bool(raw: Optional[str], default: bool = False) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    env_file = os.environ.get("ENV_FILE", ".env")
    load_dotenv(dotenv_path=env_file, override=False)

    raw_accounts = os.environ.get("PIXIV_ACCOUNTS_JSON", "").strip()
    if not raw_accounts:
        raise ValueError("PIXIV_ACCOUNTS_JSON is required.")

    try:
        parsed = json.loads(raw_accounts)
        payload = AccountsPayload.model_validate(parsed)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise ValueError("PIXIV_ACCOUNTS_JSON is invalid JSON payload.") from exc

    db_path = os.environ.get("DB_PATH", "data/pixiv_stats.db")
    enable_hourly = _parse_bool(os.environ.get("ENABLE_HOURLY"), default=False)
    snapshot_recent_hours = int(os.environ.get("SNAPSHOT_RECENT_HOURS", "24"))
    user_illusts_max_pages = int(os.environ.get("USER_ILLUSTS_MAX_PAGES", "3"))
    max_details_per_account = int(os.environ.get("MAX_DETAILS_PER_ACCOUNT", "20"))
    api_min_interval_sec = float(os.environ.get("API_MIN_INTERVAL_SEC", "1.0"))
    api_jitter_sec = float(os.environ.get("API_JITTER_SEC", "0.3"))
    tz = os.environ.get("TZ", "UTC")

    return Settings(
        accounts=payload.root,
        db_path=db_path,
        enable_hourly=enable_hourly,
        snapshot_recent_hours=snapshot_recent_hours,
        user_illusts_max_pages=user_illusts_max_pages,
        max_details_per_account=max_details_per_account,
        api_min_interval_sec=api_min_interval_sec,
        api_jitter_sec=api_jitter_sec,
        tz=tz,
    )
