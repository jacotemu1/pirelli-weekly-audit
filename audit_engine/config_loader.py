from __future__ import annotations

from pathlib import Path

import yaml

from .models import FitmentCase, Site, SitePage


def load_sites(config_path: str | Path) -> list[Site]:
    data = yaml.safe_load(Path(config_path).read_text(encoding='utf-8'))
    sites: list[Site] = []
    for item in data.get('sites', []):
        pages_data = item.get('pages') or [{'type': 'home', 'url': item['base_url']}]
        pages = [SitePage(type=p.get('type', 'internal'), url=p['url']) for p in pages_data if p.get('url')]
        allowed_prefixes = item.get('allowed_prefixes') or [item['base_url']]
        sites.append(
            Site(
                code=item['code'],
                country=item['country'],
                region=item['region'],
                language=item['language'],
                base_url=item['base_url'],
                pages=pages,
                allowed_prefixes=allowed_prefixes,
            )
        )
    return sites


def load_fitment_cases(config_path: str | Path) -> dict[str, FitmentCase]:
    path = Path(config_path)
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding='utf-8')) or {}
    cases: dict[str, FitmentCase] = {}
    for item in data.get('cases', []):
        code = str(item.get('site_code', '')).strip().upper()
        if not code:
            continue
        cases[code] = FitmentCase(
            site_code=code,
            market_url=str(item.get('market_url') or '').strip(),
            expected_types=[str(v).strip().lower() for v in (item.get('expected_types') or []) if str(v).strip()],
        )
    return cases
