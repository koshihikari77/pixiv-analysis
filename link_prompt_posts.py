from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import sqlite3

from src.collectors.prompt_assets import _extract_prompt_text

DB_PATH = os.environ.get('DB_PATH', 'data/pixiv_stats.db')
OUT_PATH = Path('data/prompt_post_links.json')


@dataclass
class LinkCandidate:
    source_group: str
    sample_local_path: str
    sample_prompt_excerpt: str
    prompt_asset_count: int
    candidate_pixiv_account_id: str | None
    candidate_pixiv_illust_id: int | None
    candidate_title: str | None
    confidence: float
    reason: str
    needs_review: bool = True


MANUAL_CANDIDATES: dict[str, dict[str, Any]] = {
    '20260329_盗撮風': {
        'candidate_pixiv_account_id': 'main',
        'candidate_pixiv_illust_id': 142934742,
        'candidate_title': 'プティ夜見星川盗撮風',
        'confidence': 0.99,
        'reason': 'folder name exactly matches post title keyword',
    },
    '20260411_あきら競泳水着': {
        'candidate_pixiv_account_id': 'sub2',
        'candidate_pixiv_illust_id': 141341672,
        'candidate_title': '競泳水着引っ張り',
        'confidence': 0.92,
        'reason': 'folder theme matches competition swimsuit post',
    },
    '20260413_あきらビキニ': {
        'candidate_pixiv_account_id': 'sub2',
        'candidate_pixiv_illust_id': 143499698,
        'candidate_title': 'ビキニで日焼け跡が恥ずかしくなる水泳部ちゃん',
        'confidence': 0.91,
        'reason': 'folder theme matches bikini/swimsuit post',
    },
    '20260413_ボーイッシュ部室': {
        'candidate_pixiv_account_id': 'sub2',
        'candidate_pixiv_illust_id': 143554187,
        'candidate_title': '巨乳ボーイッシュな陸上部ちゃんと部室でえっち',
        'confidence': 0.94,
        'reason': 'folder theme matches boish / clubroom post',
    },
    '20260412_ボーイッシュ': {
        'candidate_pixiv_account_id': 'sub2',
        'candidate_pixiv_illust_id': 143486478,
        'candidate_title': 'ボーイッシュ巨乳陸上部',
        'confidence': 0.9,
        'reason': 'folder theme matches boish post title',
    },
    '20260411_あいりギャル': {
        'candidate_pixiv_account_id': 'sub2',
        'candidate_pixiv_illust_id': 143356464,
        'candidate_title': '桃瀬あいり  ギャルjk',
        'confidence': 0.88,
        'reason': 'folder name matches character/name and gyaru theme',
    },
    '20260409_あいり紹介': {
        'candidate_pixiv_account_id': 'sub2',
        'candidate_pixiv_illust_id': 143356464,
        'candidate_title': '桃瀬あいり  ギャルjk',
        'confidence': 0.71,
        'reason': 'same character as あいりギャル; lowest-risk candidate',
    },
    '20260408_学校エロ': {
        'candidate_pixiv_account_id': 'sub2',
        'candidate_pixiv_illust_id': 143283021,
        'candidate_title': '水泳部ちゃんと放課後えっち',
        'confidence': 0.62,
        'reason': 'school theme with after-school close match',
    },
    '20260410_すず保健室': {
        'candidate_pixiv_account_id': 'sub2',
        'candidate_pixiv_illust_id': 143283021,
        'candidate_title': '水泳部ちゃんと放課後えっち',
        'confidence': 0.34,
        'reason': 'same school-time bucket; weak fallback only',
    },
    '20260407_あきら色々': {
        'candidate_pixiv_account_id': None,
        'candidate_pixiv_illust_id': None,
        'candidate_title': None,
        'confidence': 0.0,
        'reason': 'too broad; needs manual review',
    },
    '20260407_ランダムキャラ': {
        'candidate_pixiv_account_id': None,
        'candidate_pixiv_illust_id': None,
        'candidate_title': None,
        'confidence': 0.0,
        'reason': 'too broad; needs manual review',
    },
    '20260217_性上位': {
        'candidate_pixiv_account_id': None,
        'candidate_pixiv_illust_id': None,
        'candidate_title': None,
        'confidence': 0.0,
        'reason': 'no direct post title match; needs manual review',
    },
    '20260219_効果音': {
        'candidate_pixiv_account_id': None,
        'candidate_pixiv_illust_id': None,
        'candidate_title': None,
        'confidence': 0.0,
        'reason': 'no direct post title match; needs manual review',
    },
    '20260415_セクシー': {
        'candidate_pixiv_account_id': None,
        'candidate_pixiv_illust_id': None,
        'candidate_title': None,
        'confidence': 0.0,
        'reason': 'no direct post title match; needs manual review',
    },
    '20260415_検証': {
        'candidate_pixiv_account_id': None,
        'candidate_pixiv_illust_id': None,
        'candidate_title': None,
        'confidence': 0.0,
        'reason': 'no direct post title match; needs manual review',
    },
    '20260408_プレイカタログ': {
        'candidate_pixiv_account_id': None,
        'candidate_pixiv_illust_id': None,
        'candidate_title': None,
        'confidence': 0.0,
        'reason': 'no direct post title match; needs manual review',
    },
    '20260409_grid_search': {
        'candidate_pixiv_account_id': None,
        'candidate_pixiv_illust_id': None,
        'candidate_title': None,
        'confidence': 0.0,
        'reason': 'no direct post title match; needs manual review',
    },
    'old': {
        'candidate_pixiv_account_id': None,
        'candidate_pixiv_illust_id': None,
        'candidate_title': None,
        'confidence': 0.0,
        'reason': 'legacy folder; needs manual review',
    },
}


def _group_key(path: str) -> str:
    parts = Path(path).parts
    if 'akira' in parts:
        idx = parts.index('akira')
        if len(parts) > idx + 1:
            return parts[idx + 1]
    return Path(path).parts[-3] if len(Path(path).parts) >= 3 else Path(path).parent.name


def main() -> int:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT local_path, metadata_json FROM prompt_assets WHERE account_id='akira' ORDER BY local_path"
    ).fetchall()
    conn.close()

    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        grouped[_group_key(row['local_path'])].append(row)

    out: list[dict[str, Any]] = []
    for group, items in sorted(grouped.items()):
        sample = items[0]
        meta = json.loads(sample['metadata_json'])
        prompt_text, _ = _extract_prompt_text(meta)
        prompt_excerpt = (prompt_text or '')[:220]
        manual = MANUAL_CANDIDATES.get(group, {})
        out.append(
            asdict(
                LinkCandidate(
                    source_group=group,
                    sample_local_path=sample['local_path'],
                    sample_prompt_excerpt=prompt_excerpt,
                    prompt_asset_count=len(items),
                    candidate_pixiv_account_id=manual.get('candidate_pixiv_account_id'),
                    candidate_pixiv_illust_id=manual.get('candidate_pixiv_illust_id'),
                    candidate_title=manual.get('candidate_title'),
                    confidence=float(manual.get('confidence', 0.0)),
                    reason=manual.get('reason', 'manual review needed'),
                    needs_review=manual.get('candidate_pixiv_illust_id') is None,
                )
            )
        )

    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'wrote {OUT_PATH} with {len(out)} groups')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
