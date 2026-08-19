from types import SimpleNamespace

import pytest

from src.config import AccountModel
from src.main import _select_accounts


def _settings():
    return SimpleNamespace(
        accounts=[
            AccountModel(
                account_id="main",
                pixiv_user_id=123,
                refresh_token="shared-token",
            )
        ]
    )


def test_select_accounts_uses_shared_token_for_explicit_user_id():
    selected = _select_accounts(_settings(), "sub2", 456)

    assert len(selected) == 1
    assert selected[0].account_id == "sub2"
    assert selected[0].pixiv_user_id == 456
    assert selected[0].refresh_token == "shared-token"


def test_select_accounts_still_rejects_unknown_account_without_user_id():
    with pytest.raises(ValueError, match="account_id not found: sub2"):
        _select_accounts(_settings(), "sub2", None)


def test_select_accounts_rejects_mismatched_configured_user_id():
    with pytest.raises(ValueError, match="pixiv_user_id does not match"):
        _select_accounts(_settings(), "main", 999)
