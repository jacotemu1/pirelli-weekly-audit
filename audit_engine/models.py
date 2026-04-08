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
class FitmentCase:
    site_code: str
    market_url: str
    expected_types: list[str] = field(default_factory=list)


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
    screenshot_path: str = ''
    errors: list[str] = field(default_factory=list)
    template_type: str = 'generic'
    journey: str = 'generic'
    coverage_confidence: str = 'Media'
    evidence_type: str = 'dom'


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
    description: str
    impact: str
    suggested_fix: str
    fingerprint: str
    evidence_tecnica: str = ''
    confidence: str = 'Media'
    discovered_from: str = ''
    crawl_depth: int = 0
    fitment_tipo: str = ''
    fitment_step: str = ''
    screenshot_path: str = ''
    template_type: str = 'generic'
    journey: str = 'generic'
    evidence_type: str = 'dom'
    coverage_confidence: str = 'Media'
    observed: str = ''
    expected: str = ''
    business_impact: str = ''
    repro_steps: str = ''
    data: dict[str, Any] = field(default_factory=dict)
