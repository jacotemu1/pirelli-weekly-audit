from __future__ import annotations

import asyncio
import os
import re
from collections import deque
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse, urldefrag

from bs4 import BeautifulSoup
from playwright.async_api import Browser, BrowserContext, async_playwright

from .models import PageResult, Site

IGNORE_EXTENSIONS = (
    '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.pdf', '.zip', '.xml', '.json', '.mp4', '.webm', '.css', '.js',
    '.ico', '.woff', '.woff2', '.ttf'
)
TRACKING_QUERY_KEYS = {'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'gclid', 'fbclid'}
MAX_PAGES_PER_SITE = int(os.getenv('PIRELLI_MAX_PAGES_PER_SITE', '220'))
MAX_CRAWL_DEPTH = int(os.getenv('PIRELLI_MAX_CRAWL_DEPTH', '5'))
IGNORED_PATTERNS = ('/search?', '/ricerca?', '/?search=', '/login', '/signin', '/account', '/cart', '/checkout')
IMPORTANT_HINTS = ('catalog', 'catalogo', 'catalogue', 'dealer', 'rivenditor', 'distribut', 'faq', 'technology', 'tecnologia', 'elect', 'season', 'winter', 'summer', 'all-season')



def _normalize_url(url: str) -> str:
    clean, _ = urldefrag(url)
    parsed = urlparse(clean)
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() not in TRACKING_QUERY_KEYS]
    normalized = parsed._replace(query=urlencode(query, doseq=True))
    rebuilt = urlunparse(normalized)
    if rebuilt.endswith('/') and normalized.path not in ('/', ''):
        rebuilt = rebuilt.rstrip('/')
    return rebuilt



def _market_prefixes(site: Site) -> list[tuple[str, str]]:
    prefixes: list[tuple[str, str]] = []
    for raw in (site.allowed_prefixes or [site.base_url]):
        parsed = urlparse(raw)
        prefixes.append((parsed.netloc, parsed.path.rstrip('/')))
    return prefixes



def _same_market(site: Site, url: str) -> bool:
    try:
        target = urlparse(url)
    except Exception:
        return False
    if target.scheme not in ('http', 'https'):
        return False
    lower = target.path.lower()
    if lower.endswith(IGNORE_EXTENSIONS) or any(pattern in url.lower() for pattern in IGNORED_PATTERNS):
        return False
    for netloc, path_prefix in _market_prefixes(site):
        if target.netloc != netloc:
            continue
        if not path_prefix:
            return True
        if target.path.rstrip('/').startswith(path_prefix):
            return True
    return False



def _guess_page_type(url: str) -> str:
    lower = url.lower()
    if 'dealer' in lower or 'rivenditor' in lower or 'distribuidor' in lower or 'haendler' in lower or 'dealer-locator' in lower:
        return 'dealer'
    if 'catalog' in lower or 'catalogo' in lower or 'katalog' in lower or 'catalogue' in lower:
        return 'catalogue'
    if 'faq' in lower:
        return 'faq'
    if 'elect' in lower or 'technology' in lower or 'tecnologia' in lower or 'wissenswertes' in lower:
        return 'technology'
    if 'homepage' in lower or lower.endswith('/home') or 'anasayfa' in lower:
        return 'home'
    return 'internal'



def _priority(url: str) -> tuple[int, int, str]:
    lower = url.lower()
    important = 0 if any(h in lower for h in IMPORTANT_HINTS) else 1
    slash_depth = lower.count('/')
    return (important, slash_depth, lower)



def _extract_links_from_html(base_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, 'lxml')
    candidates: set[str] = set()
    attrs = [('a', 'href'), ('link', 'href'), ('area', 'href'), ('iframe', 'src')]
    for tag, attr in attrs:
        for el in soup.find_all(tag):
            value = el.get(attr)
            if not value:
                continue
            value = str(value).strip()
            if value.startswith('#') or value.startswith('javascript:') or value.startswith('mailto:') or value.startswith('tel:'):
                continue
            candidates.add(_normalize_url(urljoin(base_url, value)))

    for el in soup.find_all(attrs={'data-href': True}):
        value = str(el.get('data-href', '')).strip()
        if value:
            candidates.add(_normalize_url(urljoin(base_url, value)))

    # very light fallback for embedded JSON/html fragments containing absolute or root-relative URLs
    for match in re.findall(r'"(https?://[^"\s]+|/[^"\s]+)"', html):
        candidates.add(_normalize_url(urljoin(base_url, match)))

    return sorted(candidates)


async def _fetch_page(context: BrowserContext, site: Site, page_type: str, url: str, crawl_depth: int = 0, discovered_from: str = '') -> PageResult:
    page = await context.new_page()
    errors: list[str] = []
    response = None
    try:
        response = await page.goto(url, wait_until='domcontentloaded', timeout=45000)
        try:
            await page.wait_for_load_state('networkidle', timeout=8000)
        except Exception:
            pass
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1000)
    except Exception as exc:  # noqa: BLE001
        errors.append(f'navigation_error: {exc}')

    final_url = page.url or url
    html = ''
    title = ''
    text = ''
    h1 = ''
    h2_count = 0
    canonical = ''
    meta_description = ''
    links: list[str] = []

    try:
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        title = (soup.title.get_text(' ', strip=True) if soup.title else '')
        h1_el = soup.find('h1')
        h1 = h1_el.get_text(' ', strip=True) if h1_el else ''
        h2_count = len(soup.find_all('h2'))
        canonical_el = soup.find('link', attrs={'rel': lambda v: v and 'canonical' in v})
        canonical = canonical_el.get('href', '') if canonical_el else ''
        meta_el = soup.find('meta', attrs={'name': 'description'})
        meta_description = meta_el.get('content', '') if meta_el else ''
        text = soup.get_text('\n', strip=True)
        links = _extract_links_from_html(final_url, html)
    except Exception as exc:  # noqa: BLE001
        errors.append(f'parse_error: {exc}')

    status_code = response.status if response else None
    await page.close()
    return PageResult(
        site_code=site.code,
        country=site.country,
        region=site.region,
        language=site.language,
        url=url,
        page_type=page_type,
        final_url=final_url,
        status_code=status_code,
        title=title,
        h1=h1,
        h2_count=h2_count,
        canonical=canonical,
        html=html,
        text=text,
        links=links,
        meta_description=meta_description,
        crawl_depth=crawl_depth,
        discovered_from=discovered_from,
        errors=errors,
    )


async def _crawl_site(context: BrowserContext, site: Site) -> list[PageResult]:
    results: list[PageResult] = []
    visited: set[str] = set()
    queued: set[str] = set()
    queue: deque[tuple[str, str, int, str]] = deque()

    for pg in site.pages:
        seed = _normalize_url(pg.url)
        queue.append((seed, pg.type, 0, 'seed_homepage'))
        queued.add(seed)

    while queue and len(visited) < MAX_PAGES_PER_SITE:
        # prioritize likely important paths first
        queue = deque(sorted(queue, key=lambda item: _priority(item[0])))
        url, page_type, depth, discovered_from = queue.popleft()
        normalized = _normalize_url(url)
        if normalized in visited or not _same_market(site, normalized):
            continue
        visited.add(normalized)
        result = await _fetch_page(context, site, page_type, normalized, crawl_depth=depth, discovered_from=discovered_from)
        results.append(result)

        if depth >= MAX_CRAWL_DEPTH:
            continue
        if result.status_code and result.status_code >= 400:
            continue

        for link in result.links:
            normalized_link = _normalize_url(link)
            if normalized_link in visited or normalized_link in queued:
                continue
            if not _same_market(site, normalized_link):
                continue
            queue.append((normalized_link, _guess_page_type(normalized_link), depth + 1, result.final_url or result.url))
            queued.add(normalized_link)

    return results


async def crawl_sites(sites: list[Site]) -> list[PageResult]:
    results: list[PageResult] = []
    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=True)
        sem = asyncio.Semaphore(2)

        async def run_one(site: Site) -> None:
            async with sem:
                site_results = await _crawl_site(context, site)
                results.extend(site_results)

        await asyncio.gather(*[run_one(site) for site in sites])
        await context.close()
        await browser.close()
    return results
