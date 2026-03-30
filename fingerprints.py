from __future__ import annotations

from pathlib import Path
import yaml

from .models import Site, SitePage


def load_sites(config_path: str | Path) -> list[Site]:
    data = yaml.safe_load(Path(config_path).read_text(encoding='utf-8'))
    sites: list[Site] = []
    for item in data['sites']:
        pages = [SitePage(type=p['type'], url=p['url']) for p in item['pages']]
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
