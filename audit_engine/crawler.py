from __future__ import annotations

import asyncio
import hashlib
import os
import re
from collections import deque
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse, urldefrag

from bs4 import BeautifulSoup
from playwright.async_api import Browser, BrowserContext, async_playwright

from .models import PageResult, Site

IGNORE_EXTENSIONS = (
    '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.pdf', '.zip', '.xml', '.json', '.mp4', '.webm', '.css', '.js',
    '.ico', '.woff', '.woff2', '.ttf', '.eot'
)
TRACKING_QUERY_KEYS = {'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'gclid', 'fbclid'}
IGNORED_PATTERNS = ('/search?', '/ricerca?', '/?search=', '/login', '/signin', '/account', '/cart', '/checkout')
IMPORTANT_HINTS = ('catalog', 'catalogo', 'catalogue', 'dealer', 'rivenditor', 'distribut', 'faq', 'technology', 'tecnologia', 'elect', 'promo', 'event', 'winter', 'summer', 'all-season')
IGNORED_DOMAINS = ('facebook.com', 'instagram.com', 'linkedin.com', 'youtube.com', 'tiktok.com', 'twitter.com', 'x.com')
MAX_PAGES_PER_SITE = int(os.getenv('PIRELLI_MAX_PAGES_PER_SITE', '420'))
MAX_CRAWL_DEPTH = int(os.getenv('PIRELLI_MAX_CRAWL_DEPTH', '7'))
CRAWL_MARKET_BUDGET_SEC = int(os.getenv('PIRELLI_CRAWL_MARKET_BUDGET_SEC', '120'))
CRAWL_TOTAL_BUDGET_SEC = int(os.getenv('PIRELLI_CRAWL_TOTAL_BUDGET_SEC', '1800'))
CRAWL_HOME_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_HOME_TIMEOUT_SEC', '35'))
CRAWL_PAGE_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_PAGE_TIMEOUT_SEC', '30'))
CRAWL_GOTO_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_GOTO_TIMEOUT_SEC', '20'))
CRAWL_DOM_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_DOM_TIMEOUT_SEC', '8'))
CRAWL_HTML_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_HTML_TIMEOUT_SEC', '12'))
CRAWL_LINKS_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_LINKS_TIMEOUT_SEC', '8'))
CRAWL_HEARTBEAT_SEC = int(os.getenv('PIRELLI_CRAWL_HEARTBEAT_SEC', '15'))
ONCLICK_URL_PATTERN = re.compile(r"""(?:location\.href|window\.open)\(['"]([^'"]+)['"]\)""")
INLINE_URL_PATTERN = re.compile(r"""["'](https?://[^"'\s]+|/[^"'\s]+)["']""")


def _crawl_log(message: str) -> None:
    print(f'[CRAWL] {message}', flush=True)


def _detect_home_entrypoints(page: PageResult) -> dict[str, bool]:
    text = f'{page.title} {page.h1} {page.text}'.lower()
    links_blob = ' '.join(page.links).lower()
    blob = f'{text} {links_blob}'
    return {
        'product_search': any(k in blob for k in ('find your tyre', 'trova il pneumatico', 'vehicle', 'veicolo', 'size', 'misura', 'plate', 'targa')),
        'dealer': any(k in blob for k in ('dealer', 'rivenditor', 'locator', 'store')),
        'editorial': any(k in blob for k in ('technology', 'tecnologia', 'faq', 'guide', 'news', 'service')),
        'product_family': any(k in blob for k in ('p zero', 'cinturato', 'scorpion', 'diablo')),
        'use_case': any(k in blob for k in ('suv', 'ev', 'winter', 'all season')),
    }


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
    if any(domain in target.netloc.lower() for domain in IGNORED_DOMAINS):
        return False
    if lower.endswith(IGNORE_EXTENSIONS) or any(pattern in url.lower() for pattern in IGNORED_PATTERNS):
        return False
    if '/api/' in lower or lower.startswith('/api') or '/graphql' in lower:
        return False
    if re.search(r'/dealer-locator/.+/(\d{8,}|[a-z]{2}\d{8,})$', lower):
        # Avoid deep dealer-detail endpoints often unstable / geo-specific.
        return False
    for netloc, path_prefix in _market_prefixes(site):
        if target.netloc != netloc:
            continue
        if not path_prefix:
            return True
        if target.path.rstrip('/').lower().startswith(path_prefix.lower()):
            return True
    return False


def _guess_page_type(url: str) -> str:
    lower = url.lower()
    if any(k in lower for k in ('dealer', 'rivenditor', 'distribuidor', 'haendler', 'dealer-locator')):
        return 'dealer'
    if any(k in lower for k in ('catalog', 'catalogo', 'katalog', 'catalogue')):
        return 'catalogue'
    if 'faq' in lower:
        return 'faq'
    if any(k in lower for k in ('elect', 'technology', 'tecnologia', 'wissenswertes')):
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
    for tag, attr in (('a', 'href'), ('link', 'href'), ('area', 'href'), ('iframe', 'src'), ('form', 'action')):
        for el in soup.find_all(tag):
            value = el.get(attr)
            if not value:
                continue
            value = str(value).strip()
            if value.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                continue
            candidates.add(_normalize_url(urljoin(base_url, value)))

    for attr_name in ('data-href', 'data-url', 'data-link', 'data-target'):
        for el in soup.find_all(attrs={attr_name: True}):
            value = str(el.get(attr_name, '')).strip()
            if value:
                candidates.add(_normalize_url(urljoin(base_url, value)))

    for onclick_match in ONCLICK_URL_PATTERN.findall(html):
        candidates.add(_normalize_url(urljoin(base_url, onclick_match)))

    for match in INLINE_URL_PATTERN.findall(html):
        candidates.add(_normalize_url(urljoin(base_url, match)))
    return sorted(candidates)


async def _fetch_page(
    context: BrowserContext,
    site: Site,
    page_type: str,
    url: str,
    crawl_depth: int = 0,
    discovered_from: str = '',
    page_timeout_sec: int = CRAWL_PAGE_TIMEOUT_SEC,
    stage_state: dict[str, str] | None = None,
) -> PageResult:
    page = await context.new_page()
    errors: list[str] = []
    response = None
    _crawl_log(f'page goto start: {site.code} url={url}')
    try:
        if stage_state is not None:
            stage_state['stage'] = 'page_goto'
        response = await asyncio.wait_for(
            page.goto(url, wait_until='domcontentloaded', timeout=page_timeout_sec * 1000),
            timeout=min(CRAWL_GOTO_TIMEOUT_SEC, page_timeout_sec),
        )
        _crawl_log(f'page goto done: {site.code} url={url} status={response.status if response else None}')
    except Exception as exc:  # noqa: BLE001
        errors.append(f'page_goto_timeout_or_error: {exc}')
        _crawl_log(f'market error: {site.code} stage=page_goto error={exc}')

    try:
        if stage_state is not None:
            stage_state['stage'] = 'page_domcontentloaded'
        await asyncio.wait_for(page.wait_for_load_state('domcontentloaded'), timeout=CRAWL_DOM_TIMEOUT_SEC)
        _crawl_log(f'domcontentloaded reached: {site.code} url={url}')
    except Exception as exc:  # noqa: BLE001
        errors.append(f'page_domcontentloaded_timeout_or_error: {exc}')
        _crawl_log(f'market error: {site.code} stage=page_domcontentloaded error={exc}')

    final_url = page.url or url
    html = ''
    title = ''
    text = ''
    h1 = ''
    h2_count = 0
    canonical = ''
    meta_description = ''
    links: list[str] = []
    screenshot_path = ''

    try:
        if stage_state is not None:
            stage_state['stage'] = 'page_extract_html'
        html = await asyncio.wait_for(page.content(), timeout=CRAWL_HTML_TIMEOUT_SEC)
        _crawl_log(f'html extracted: {site.code} size={len(html)}')
        soup = BeautifulSoup(html, 'lxml')
        if stage_state is not None:
            stage_state['stage'] = 'page_read_title'
        title = soup.title.get_text(' ', strip=True) if soup.title else ''
        _crawl_log(f'title read: {site.code} value={title[:80]}')
        h1_el = soup.find('h1')
        if stage_state is not None:
            stage_state['stage'] = 'page_read_h1'
        h1 = h1_el.get_text(' ', strip=True) if h1_el else ''
        _crawl_log(f'h1 read: {site.code} value={h1[:80]}')
        h2_count = len(soup.find_all('h2'))
        canonical_el = soup.find('link', attrs={'rel': lambda v: v and 'canonical' in v})
        canonical = canonical_el.get('href', '') if canonical_el else ''
        meta_el = soup.find('meta', attrs={'name': 'description'})
        meta_description = meta_el.get('content', '') if meta_el else ''
        text = soup.get_text('\n', strip=True)
        try:
            if stage_state is not None:
                stage_state['stage'] = 'page_extract_links'
            links = await asyncio.wait_for(asyncio.to_thread(_extract_links_from_html, final_url, html), timeout=CRAWL_LINKS_TIMEOUT_SEC)
        except Exception as exc:  # noqa: BLE001
            errors.append(f'link_extract_error: {exc}')
            links = []

        # Include only real links found in rendered DOM (no synthetic URL building).
        try:
            dom_links = await page.eval_on_selector_all(
                'a[href], link[href], area[href], iframe[src], form[action], [data-href], [data-url], [data-link], [data-target]',
                """(els) => els
                    .map((el) => el.getAttribute('href') || el.getAttribute('src') || el.getAttribute('action')
                        || el.getAttribute('data-href') || el.getAttribute('data-url')
                        || el.getAttribute('data-link') || el.getAttribute('data-target'))
                    .filter(Boolean)""",
            )
            for raw in dom_links:
                raw_str = str(raw).strip()
                if not raw_str or raw_str.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                    continue
                links.append(_normalize_url(urljoin(final_url, raw_str)))
            links = sorted(set(links))
        except Exception as exc:  # noqa: BLE001
            errors.append(f'dom_link_extract_error: {exc}')
        _crawl_log(f'links extracted from page: {site.code} count={len(links)}')
    except Exception as exc:  # noqa: BLE001
        errors.append(f'page_extract_html_timeout_or_error: {exc}')
        stage_name = (stage_state or {}).get('stage', 'page_extract_html')
        _crawl_log(f'market error: {site.code} stage={stage_name} error={exc}')

    status_code = response.status if response else None
    try:
        should_capture = bool(errors) or (status_code is not None and status_code >= 400)
        if should_capture:
            shot_dir = Path('outputs/screenshots/crawl')
            shot_dir.mkdir(parents=True, exist_ok=True)
            token = hashlib.md5(f'{site.code}|{url}'.encode('utf-8')).hexdigest()[:12]
            shot_file = shot_dir / f'{site.code}_{token}.png'
            await page.screenshot(path=str(shot_file), full_page=True)
            screenshot_path = str(shot_file)
    except Exception as exc:  # noqa: BLE001
        errors.append(f'screenshot_error: {exc}')
    if stage_state is not None:
        stage_state['stage'] = 'page_finalize'
    _crawl_log(f'page visit done: {site.code} depth={crawl_depth} url={final_url}')
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
        crawl_depth=max(0, crawl_depth),
        discovered_from=(discovered_from or ('seed_homepage' if crawl_depth == 0 else url)),
        screenshot_path=screenshot_path,
        errors=errors,
    )


async def _crawl_site(context: BrowserContext, site: Site, stage_state: dict[str, str] | None = None) -> list[PageResult]:
    results: list[PageResult] = []
    visited: set[str] = set()
    queued: set[str] = set()
    queue: deque[tuple[str, str, int, str]] = deque()

    homepage_seed = next((p.url for p in site.pages if (p.type or '').lower() == 'home' and p.url), site.base_url)
    seed = _normalize_url(homepage_seed)
    queue.append((seed, 'home', 0, 'seed_homepage'))
    queued.add(seed)
    start_ts = asyncio.get_running_loop().time()
    last_heartbeat = start_ts

    while queue and len(visited) < MAX_PAGES_PER_SITE:
        now = asyncio.get_running_loop().time()
        if now - last_heartbeat >= CRAWL_HEARTBEAT_SEC:
            _crawl_log(
                f'heartbeat: {site.code} queue={len(queue)} visited={len(visited)} '
                f'elapsed={int(now-start_ts)}s stage={(stage_state or {}).get("stage", "queue_processing")}'
            )
            last_heartbeat = now

        if stage_state is not None:
            stage_state['stage'] = 'queue_processing'
        queue = deque(sorted(queue, key=lambda item: _priority(item[0])))
        url, page_type, depth, discovered_from = queue.popleft()
        normalized = _normalize_url(url)
        if normalized in visited or not _same_market(site, normalized):
            continue

        _crawl_log(f'page visit start: {site.code} depth={depth} url={normalized}')
        visited.add(normalized)
        try:
            if stage_state is not None:
                stage_state['stage'] = 'opening_home' if depth == 0 else 'visiting_page'
            if depth == 0:
                _crawl_log(f'homepage start: {site.code} url={normalized}')
            result = await asyncio.wait_for(
                _fetch_page(
                    context,
                    site,
                    page_type,
                    normalized,
                    crawl_depth=depth,
                    discovered_from=discovered_from,
                    page_timeout_sec=CRAWL_HOME_TIMEOUT_SEC if depth == 0 else CRAWL_PAGE_TIMEOUT_SEC,
                    stage_state=stage_state,
                ),
                timeout=(CRAWL_HOME_TIMEOUT_SEC if depth == 0 else CRAWL_PAGE_TIMEOUT_SEC) + CRAWL_HTML_TIMEOUT_SEC + 10,
            )
            if depth == 0:
                _crawl_log(f'homepage response: {site.code} status={result.status_code}')
                _crawl_log(f'homepage minimal extraction done: {site.code}')
        except Exception as exc:  # noqa: BLE001
            # Hard-fail protection: record the page as errored and continue crawl.
            stage = (stage_state or {}).get('stage', 'other')
            if depth == 0:
                _crawl_log(f'homepage timeout: {site.code} stage={stage}')
            _crawl_log(f'market error: {site.code} stage={stage} error={exc}')
            results.append(
                PageResult(
                    site_code=site.code,
                    country=site.country,
                    region=site.region,
                    language=site.language,
                    url=normalized,
                    page_type=page_type,
                    final_url=normalized,
                    status_code=None,
                    title='',
                    h1='',
                    h2_count=0,
                    canonical='',
                    html='',
                    text='',
                    links=[],
                    meta_description='',
                    crawl_depth=depth,
                    discovered_from=discovered_from,
                    errors=[f'fetch_unhandled_error: {exc}'],
                )
            )
            continue
        if stage_state is not None:
            stage_state['stage'] = 'fetching_html'
        results.append(result)
        _crawl_log(f'page visit done: {site.code} status={result.status_code} title={result.title[:80]}')
        _crawl_log(f'html fetched: {site.code} size={len(result.html or "")}')
        _crawl_log(f'links extracted: {site.code} count={len(result.links)}')

        if depth == 0:
            if stage_state is not None:
                stage_state['stage'] = 'detecting_entrypoints'
            ep = _detect_home_entrypoints(result)
            _crawl_log(
                f'expected entrypoints seen: {site.code} '
                f'product_search={"yes" if ep["product_search"] else "no"} '
                f'dealer={"yes" if ep["dealer"] else "no"} '
                f'editorial={"yes" if ep["editorial"] else "no"} '
                f'product_family={"yes" if ep["product_family"] else "no"} '
                f'use_case={"yes" if ep["use_case"] else "no"}'
            )

        if depth >= MAX_CRAWL_DEPTH:
            continue
        if result.status_code and result.status_code >= 400:
            continue

        new_links = 0
        if stage_state is not None:
            stage_state['stage'] = 'queue_seeding'
        for link in result.links:
            normalized_link = _normalize_url(link)
            if normalized_link in visited or normalized_link in queued:
                continue
            if not _same_market(site, normalized_link):
                continue
            parent = result.final_url or result.url or normalized
            queue.append((normalized_link, _guess_page_type(normalized_link), depth + 1, parent))
            queued.add(normalized_link)
            new_links += 1
        if depth == 0:
            _crawl_log(f'queue seeded: {site.code} size={len(queue)}')
        if new_links == 0 and queue:
            _crawl_log(f'queue processing no new links: {site.code} queue={len(queue)} visited={len(visited)}')

    return results


async def crawl_sites(sites: list[Site]) -> list[PageResult]:
    results: list[PageResult] = []
    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=True)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + CRAWL_TOTAL_BUDGET_SEC
        for index, site in enumerate(sites, start=1):
            remaining = deadline - loop.time()
            if remaining <= 0:
                _crawl_log('global timeout reached, stopping remaining markets')
                break
            _crawl_log(f'market start: {site.code} ({index}/{len(sites)})')
            market_timeout = min(CRAWL_MARKET_BUDGET_SEC, remaining)
            market_pages_before = len(results)
            stage_state = {'stage': 'other'}
            try:
                site_results = await asyncio.wait_for(_crawl_site(context, site, stage_state=stage_state), timeout=market_timeout)
                for page_idx, page_result in enumerate(site_results, start=1):
                    _crawl_log(
                        f'market page: {site.code} count={page_idx} depth={page_result.crawl_depth} '
                        f'url={page_result.final_url or page_result.url}'
                    )
                results.extend(site_results)
                _crawl_log(f'market done: {site.code} pages={len(site_results)}')
            except asyncio.TimeoutError:
                _crawl_log(f'market timeout: {site.code} stage={stage_state.get("stage", "other")}')
                results.append(
                    PageResult(
                        site_code=site.code,
                        country=site.country,
                        region=site.region,
                        language=site.language,
                        url=site.base_url,
                        page_type='home',
                        final_url=site.base_url,
                        status_code=None,
                        title='',
                        h1='',
                        h2_count=0,
                        canonical='',
                        html='',
                        text='',
                        links=[],
                        meta_description='',
                        crawl_depth=0,
                        discovered_from='seed_homepage',
                        errors=[f'market_timeout: exceeded_{market_timeout:.1f}s; stage={stage_state.get("stage", "other")}'],
                    )
                )
            except Exception as exc:  # noqa: BLE001
                _crawl_log(f'market error: {site.code} stage={stage_state.get("stage", "other")} error={exc}')
                # Prevent one market failure from aborting the whole weekly run.
                results.append(
                    PageResult(
                        site_code=site.code,
                        country=site.country,
                        region=site.region,
                        language=site.language,
                        url=site.base_url,
                        page_type='home',
                        final_url=site.base_url,
                        status_code=None,
                        title='',
                        h1='',
                        h2_count=0,
                        canonical='',
                        html='',
                        text='',
                        links=[],
                        meta_description='',
                        crawl_depth=0,
                        discovered_from='seed_homepage',
                        errors=[f'site_crawl_unhandled_error: {exc}'],
                    )
                )
            finally:
                market_pages_after = len(results)
                if market_pages_after == market_pages_before:
                    _crawl_log(f'market done: {site.code} pages=0')
        await context.close()
        await browser.close()
    return results
