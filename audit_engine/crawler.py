from __future__ import annotations

import asyncio
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import Browser, BrowserContext, async_playwright

from .models import PageResult, Site


async def _fetch_page(context: BrowserContext, site: Site, page_type: str, url: str) -> PageResult:
    page = await context.new_page()
    errors: list[str] = []
    response = None
    try:
        response = await page.goto(url, wait_until='domcontentloaded', timeout=45000)
        await page.wait_for_timeout(2000)
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
            if href.startswith('#') or href.startswith('javascript:'):
                continue
            links.append(urljoin(final_url, href))
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
        errors=errors,
    )


async def crawl_sites(sites: list[Site]) -> list[PageResult]:
    results: list[PageResult] = []
    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=True)
        sem = asyncio.Semaphore(4)

        async def run_one(site: Site, page_type: str, url: str) -> None:
            async with sem:
                results.append(await _fetch_page(context, site, page_type, url))

        tasks = [run_one(site, pg.type, pg.url) for site in sites for pg in site.pages]
        await asyncio.gather(*tasks)
        await context.close()
        await browser.close()
    return results
