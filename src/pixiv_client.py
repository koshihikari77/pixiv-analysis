import random
import time
from typing import Any, Dict, List, Optional

from pixivpy3 import AppPixivAPI


def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


class PixivClient:
    def __init__(
        self,
        refresh_token: str,
        min_interval_sec: float = 1.0,
        jitter_sec: float = 0.3,
        max_attempts: int = 4,
    ):
        self.api = AppPixivAPI()
        self.api.auth(refresh_token=refresh_token)
        self.min_interval_sec = min_interval_sec
        self.jitter_sec = max(0.0, jitter_sec)
        self.max_attempts = max(1, max_attempts)
        self._last_called = 0.0

    def _throttle(self) -> None:
        now = time.time()
        elapsed = now - self._last_called
        base_wait = max(0.0, self.min_interval_sec - elapsed)
        jitter_wait = random.uniform(0.0, self.jitter_sec) if self.jitter_sec > 0 else 0.0
        total_wait = base_wait + jitter_wait
        if total_wait > 0:
            time.sleep(total_wait)
        self._last_called = time.time()

    def _extract_response(self, exc: Exception):
        return getattr(exc, "response", None)

    def _extract_retry_after(self, exc: Exception) -> Optional[float]:
        response = self._extract_response(exc)
        if response is None:
            return None
        headers = getattr(response, "headers", {}) or {}
        value = headers.get("Retry-After")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _should_retry(self, exc: Exception) -> bool:
        response = self._extract_response(exc)
        if response is None:
            return True
        status = getattr(response, "status_code", None)
        if status is None:
            return True
        return status == 429 or status >= 500

    def _compute_backoff(self, exc: Exception, attempt: int) -> float:
        retry_after = self._extract_retry_after(exc)
        if retry_after is not None:
            return max(retry_after, 0.5)
        # Exponential backoff: 0.5, 1, 2, 4 ... (upper bounded)
        return min(8.0, 0.5 * (2 ** (attempt - 1)))

    def _call_api(self, method, *args, **kwargs):
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            try:
                return method(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if not self._should_retry(exc) or attempt == self.max_attempts:
                    raise
                time.sleep(self._compute_backoff(exc, attempt))
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Unexpected API call state")

    def user_detail(self, user_id: int):
        return self._call_api(self.api.user_detail, user_id)

    def illust_detail(self, illust_id: int):
        return self._call_api(self.api.illust_detail, illust_id)

    def user_illusts_page(self, user_id: int, offset: Optional[int] = None):
        if offset is None:
            return self._call_api(self.api.user_illusts, user_id)
        return self._call_api(self.api.user_illusts, user_id, offset=offset)

    def list_user_illusts(self, user_id: int, max_pages: int = 3) -> List[Any]:
        results: List[Any] = []
        offset: Optional[int] = None

        for _ in range(max_pages):
            page = self.user_illusts_page(user_id, offset=offset)
            illusts = _safe_get(page, "illusts", [])
            results.extend(illusts)

            next_url = _safe_get(page, "next_url")
            if not next_url:
                break
            parsed = self.api.parse_qs(next_url)
            next_offset = parsed.get("offset")
            if not next_offset:
                break
            if isinstance(next_offset, list):
                next_offset = next_offset[0]
            offset = int(next_offset)

        return results


def extract_user_stats(user_detail_response: Any) -> Dict[str, Optional[int]]:
    user = _safe_get(user_detail_response, "user", {})
    profile = _safe_get(user_detail_response, "profile", {})
    profile_publicity = _safe_get(user_detail_response, "profile_publicity", {})

    follower_candidates = [
        _safe_get(profile, "total_follow_users"),
        _safe_get(user, "total_follow_users"),
        _safe_get(profile, "followers"),
        _safe_get(user, "followers"),
        _safe_get(profile_publicity, "total_follow_users"),
    ]
    following_candidates = [
        _safe_get(user, "total_following"),
        _safe_get(profile, "total_following"),
        _safe_get(user, "following"),
        _safe_get(profile, "following"),
        _safe_get(profile_publicity, "total_following"),
    ]

    followers = next((v for v in follower_candidates if isinstance(v, int)), None)
    following = next((v for v in following_candidates if isinstance(v, int)), None)

    return {"followers": followers, "following": following}


def extract_post_meta(illust: Any) -> Dict[str, Any]:
    tags = _safe_get(illust, "tags", [])
    tag_names: List[str] = []
    for tag in tags or []:
        name = _safe_get(tag, "name")
        if name:
            tag_names.append(name)

    return {
        "illust_id": _safe_get(illust, "id"),
        "create_date": _safe_get(illust, "create_date"),
        "tags": tag_names,
        "type": _safe_get(illust, "type"),
        "page_count": _safe_get(illust, "page_count"),
        "x_restrict": _safe_get(illust, "x_restrict"),
        "title": _safe_get(illust, "title"),
    }


def extract_snapshot(detail_response: Any) -> Dict[str, Optional[int]]:
    illust = _safe_get(detail_response, "illust", {})
    bookmark_count = _safe_get(illust, "total_bookmarks")
    like_count = _safe_get(illust, "like_count")
    view_count = _safe_get(illust, "total_view")
    comment_count = _safe_get(illust, "total_comments")

    return {
        "bookmark_count": bookmark_count,
        "like_count": like_count,
        "view_count": view_count,
        "comment_count": comment_count,
    }
