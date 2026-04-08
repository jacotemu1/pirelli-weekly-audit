from __future__ import annotations

from pathlib import Path

import yaml

from .models import FitmentCase, Site, SitePage


def _safe_yaml_load(path: Path) -> dict:
    raw = yaml.safe_load(path.read_text(encoding='utf-8')) or {}
    if not isinstance(raw, dict):
        raise ValueError(f'Config YAML non valido: root non dict ({path})')
    return raw


def load_sites(config_path: str | Path) -> list[Site]:
    data = _safe_yaml_load(Path(config_path))
    sites: list[Site] = []
    for item in data.get('sites', []):
        if not isinstance(item, dict):
            continue
        pages_data = item.get('pages') or [{'type': 'home', 'url': item['base_url']}]
        pages = [SitePage(type=p.get('type', 'internal'), url=p['url']) for p in pages_data if isinstance(p, dict) and p.get('url')]
        allowed_prefixes = [str(v).strip() for v in (item.get('allowed_prefixes') or [item['base_url']]) if str(v).strip()]
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
    data = _safe_yaml_load(path)
    cases: dict[str, FitmentCase] = {}
    for item in data.get('cases', []):
        if not isinstance(item, dict):
            continue
        code = str(item.get('site_code', '')).strip().upper()
        if not code:
            continue
        cases[code] = FitmentCase(
            site_code=code,
            market_url=str(item.get('market_url') or '').strip(),
            expected_types=[str(v).strip().lower() for v in (item.get('expected_types') or []) if str(v).strip()],
        )
    return cases
