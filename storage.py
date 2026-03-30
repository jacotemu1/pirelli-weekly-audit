from __future__ import annotations

import asyncio
from collections import deque
from urllib.parse import urljoin, urlparse, urldefrag

from bs4 import BeautifulSoup
from playwright.async_api import Browser, BrowserContext, async_playwright

from .models import PageResult, Site

IGNORE_EXTENSIONS = (
    '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.pdf', '.zip', '.xml', '.json', '.mp4', '.webm', '.css', '.js',
)
MAX_PAGES_PER_SITE = 80
MAX_CRAWL_DEPTH = 3


def _normalize_url(url: str) -> str:
    clean, _ = urldefrag(url)
    return clean.rstrip('/') if clean.endswith('/') else clean


def _same_market(site: Site, url: str) -> bool:
    try:
        base = urlparse(site.base_url)
        target = urlparse(url)
    except Exception:
        return False
    if target.scheme not in ('http', 'https'):
        return False
    if base.netloc != target.netloc:
        return False
    if not target.path.startswith(base.path):
        return False
    lower = target.path.lower()
    return not lower.endswith(IGNORE_EXTENSIONS)


def _guess_page_type(url: str) -> str:
    lower = url.lower()
    if 'dealer' in lower or 'rivenditor' in lower or 'distribuidor' in lower or 'haendler' in lower:
        return 'dealer'
    if 'catalog' in lower or 'catalogo' in lower or 'katalog' in lower or 'catalogue' in lower:
        return 'catalogue'
    if 'faq' in lower:
        return 'faq'
    if 'elect' in lower or 'technology' in lower or 'tecnologia' in lower:
        return 'technology'
    if 'homepage' in lower or lower.endswith('/home') or 'anasayfa' in lower:
        return 'home'
    return 'internal'


async def _fetch_page(context: BrowserContext, site: Site, page_type: str, url: str, crawl_depth: int = 0, discovered_from: str = '') -> PageResult:
    page = await context.new_page()
    errors: list[str] = []
    response = None
    try:
        response = await page.goto(url, wait_until='domcontentloaded', timeout=45000)
        await page.wait_for_timeout(1500)
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
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:') or href.startswith('tel:'):
                continue
            absolute = _normalize_url(urljoin(final_url, href))
            links.append(absolute)
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
    queue: deque[tuple[str, str, int, str]] = deque()

    # Seed with configured pages; homepages can expand the crawl.
    for pg in site.pages:
        queue.append((_normalize_url(pg.url), pg.type, 0, 'config'))

    while queue and len(visited) < MAX_PAGES_PER_SITE:
        url, page_type, depth, discovered_from = queue.popleft()
        normalized = _normalize_url(url)
        if normalized in visited:
            continue
        if not _same_market(site, normalized):
            continue
        visited.add(normalized)
        result = await _fetch_page(context, site, page_type, normalized, crawl_depth=depth, discovered_from=discovered_from)
        results.append(result)

        should_expand = depth < MAX_CRAWL_DEPTH and page_type in {'home', 'internal', 'catalogue', 'faq', 'technology'}
        if not should_expand:
            continue

        for link in result.links:
            normalized_link = _normalize_url(link)
            if normalized_link in visited:
                continue
            if not _same_market(site, normalized_link):
                continue
            queue.append((normalized_link, _guess_page_type(normalized_link), depth + 1, result.final_url or result.url))

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
