from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SitePage:
    type: str
    url: str


@dataclass
class Site:
    code: str
    country: str
    region: str
    language: str
    base_url: str
    pages: list[SitePage]
    allowed_prefixes: list[str] = field(default_factory=list)


@dataclass
class PageResult:
    site_code: str
    country: str
    region: str
    language: str
    url: str
    page_type: str
    final_url: str
    status_code: int | None
    title: str
    h1: str
    h2_count: int
    canonical: str
    html: str
    text: str
    links: list[str]
    meta_description: str
    crawl_depth: int = 0
    discovered_from: str = ''
    errors: list[str] = field(default_factory=list)


@dataclass
class Finding:
    site_code: str
    country: str
    region: str
    url: str
    page_type: str
    category: str
    severity: str
    title: str
    explanation_it: str
    impact_it: str
    suggested_fix_it: str
    evidence_tecnica: str
    confidence: str
    fingerprint: str
    crawl_depth: int = 0
    discovered_from: str = ''
    data: dict[str, Any] = field(default_factory=dict)
