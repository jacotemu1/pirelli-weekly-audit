from __future__ import annotations

import asyncio
import hashlib
import os
import re
from collections import Counter, defaultdict, deque
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse, urldefrag

from bs4 import BeautifulSoup
from playwright.async_api import Browser, BrowserContext, async_playwright

from .fingerprints import make_fingerprint
from .models import Finding, PageResult, Site

IGNORE_EXTENSIONS = (
    '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.pdf', '.zip', '.xml', '.json', '.mp4', '.webm', '.css', '.js',
    '.ico', '.woff', '.woff2', '.ttf', '.eot'
)
TRACKING_QUERY_KEYS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'gclid', 'fbclid', '_ga', '_gl',
    'analytics_cookie', 'marketing_cookie', 'mc_cid', 'mc_eid', 'cmpid', 'icid', 'srsltid', 'sc_channel',
}
NOISE_QUERY_PREFIXES = ('utm_', 'ga_', 'pk_', 'mtm_', 'cmp_', 'trk_')
IGNORED_PATTERNS = ('/search?', '/ricerca?', '/?search=', '/login', '/signin', '/account', '/cart', '/checkout')
IMPORTANT_HINTS = ('catalog', 'catalogo', 'catalogue', 'dealer', 'rivenditor', 'distribut', 'faq', 'technology', 'tecnologia', 'elect', 'promo', 'offer', 'service', 'winter', 'summer', 'all-season', 'fitment', 'tyres')
IGNORED_DOMAINS = ('facebook.com', 'instagram.com', 'linkedin.com', 'youtube.com', 'tiktok.com', 'twitter.com', 'x.com')
MAX_PAGES_PER_SITE = int(os.getenv('PIRELLI_MAX_PAGES_PER_SITE', '220'))
MAX_CRAWL_DEPTH = int(os.getenv('PIRELLI_MAX_CRAWL_DEPTH', '4'))
CRAWL_MARKET_BUDGET_SEC = int(os.getenv('PIRELLI_CRAWL_MARKET_BUDGET_SEC', '120'))
CRAWL_TOTAL_BUDGET_SEC = int(os.getenv('PIRELLI_CRAWL_TOTAL_BUDGET_SEC', '1800'))
CRAWL_HOME_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_HOME_TIMEOUT_SEC', '35'))
CRAWL_PAGE_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_PAGE_TIMEOUT_SEC', '30'))
CRAWL_GOTO_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_GOTO_TIMEOUT_SEC', '20'))
CRAWL_DOM_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_DOM_TIMEOUT_SEC', '8'))
CRAWL_HTML_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_HTML_TIMEOUT_SEC', '12'))
CRAWL_LINKS_TIMEOUT_SEC = int(os.getenv('PIRELLI_CRAWL_LINKS_TIMEOUT_SEC', '8'))
CRAWL_HEARTBEAT_SEC = int(os.getenv('PIRELLI_CRAWL_HEARTBEAT_SEC', '15'))
MAX_ENQUEUE_PER_PAGE = int(os.getenv('PIRELLI_MAX_ENQUEUE_PER_PAGE', '45'))
MAX_TEMPLATE_REPRESENTATIVES = int(os.getenv('PIRELLI_MAX_TEMPLATE_REPRESENTATIVES', '3'))
MAX_DEALER_LINKS_PER_PAGE = int(os.getenv('PIRELLI_MAX_DEALER_LINKS_PER_PAGE', '25'))
MAX_LINKS_FROM_MASSIVE_PAGE = int(os.getenv('PIRELLI_MAX_LINKS_FROM_MASSIVE_PAGE', '120'))
ONCLICK_URL_PATTERN = re.compile(r"""(?:location\.href|window\.open)\(['\"]([^'\"]+)['\"]\)""")
INLINE_URL_PATTERN = re.compile(r"""["'](https?://[^"'\s]+|/[^"'\s]+)["']""")


def _log(prefix: str, message: str) -> None:
    print(f'[{prefix}] {message}', flush=True)


def _crawl_log(message: str) -> None:
    _log('CRAWL', message)


def _normalize_url(url: str) -> str:
    clean, _ = urldefrag((url or '').strip())
    clean = clean.replace('\\\\', '/').replace('\\', '/')
    clean = re.sub(r'/+', '/', clean.replace('://', '§§'))
    clean = clean.replace('§§', '://')
    parsed = urlparse(clean)
    filtered_query: list[tuple[str, str]] = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=False):
        lk = k.lower().strip()
        if lk in TRACKING_QUERY_KEYS or any(lk.startswith(p) for p in NOISE_QUERY_PREFIXES):
            continue
        if 'cookie' in lk and lk.endswith('cookie'):
            continue
        filtered_query.append((lk, v.strip()))
    normalized = parsed._replace(query=urlencode(filtered_query, doseq=True), fragment='')
    rebuilt = urlunparse(normalized)
    rebuilt = rebuilt.rstrip('\\')
    if rebuilt.endswith('/') and normalized.path not in ('/', ''):
        rebuilt = rebuilt.rstrip('/')
    return rebuilt


def _classify_template(url: str, title: str, text: str, status_code: int | None) -> tuple[str, str]:
    lower_url = (url or '').lower()
    blob = f'{title} {text}'.lower()
    if status_code in {404, 410} or '404' in blob or 'not found' in blob or 'pagina non trovata' in blob:
        return 'error_page', 'error_resolution'
    if re.search(r'^https?://[^/]+/?$', lower_url):
        return 'homepage', 'entry'
    if any(k in lower_url for k in ('catalog', 'catalogo', 'tyres', 'pneumatici')) and any(k in lower_url for k in ('/car', '/suv', '/moto', '/van', '/truck', '/electric')):
        return 'catalog_listing', 'browse_products'
    if any(k in lower_url for k in ('/tyres/', '/pneumatici/', '/products/', '/prodotto/', '/pzero', '/cinturato', '/scorpion', '/winter')):
        return 'product_detail', 'product_research'
    if any(k in lower_url for k in ('fitment', 'vehicle', 'trova', 'configurator', 'carrello-virtuale')):
        return ('fitment_step', 'fitment') if any(k in lower_url for k in ('step', 'result', 'results')) else ('fitment_entry', 'fitment')
    if any(k in lower_url for k in ('dealer', 'rivenditor', 'locator', 'store-locator', 'trova-rivenditore')):
        return ('dealer_detail', 'dealer_locator') if re.search(r'/\d{4,}', lower_url) else ('dealer_locator', 'dealer_locator')
    if 'faq' in lower_url:
        return 'faq', 'learn'
    if any(k in lower_url for k in ('technology', 'tecnologia', 'elect', 'newsroom')):
        return 'technology', 'learn'
    if any(k in lower_url for k in ('promo', 'offer', 'service', 'campaign')):
        return 'promotion', 'conversion'
    if any(k in lower_url for k in ('suv', 'ev', 'winter', 'summer', 'all-season', 'all season')):
        return 'use_case', 'conversion'
    return 'content_page', 'learn'


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
    for netloc, path_prefix in _market_prefixes(site):
        if target.netloc != netloc:
            continue
        if not path_prefix:
            return True
        if target.path.rstrip('/').lower().startswith(path_prefix.lower()):
            return True
    return False


def _priority(url: str, template_type: str, depth: int) -> tuple[int, int, int, str]:
    lower = url.lower()
    template_rank = {
        'homepage': 0,
        'catalog_listing': 1,
        'product_detail': 1,
        'fitment_entry': 1,
        'fitment_step': 2,
        'dealer_locator': 2,
        'technology': 3,
        'faq': 3,
        'promotion': 3,
        'use_case': 3,
        'content_page': 4,
        'error_page': 5,
    }.get(template_type, 4)
    important = 0 if any(h in lower for h in IMPORTANT_HINTS) else 1
    return (template_rank, important, depth, lower)


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
    try:
        if stage_state is not None:
            stage_state['stage'] = 'page_goto'
        response = await asyncio.wait_for(page.goto(url, wait_until='domcontentloaded', timeout=page_timeout_sec * 1000), timeout=min(CRAWL_GOTO_TIMEOUT_SEC, page_timeout_sec))
    except Exception as exc:  # noqa: BLE001
        errors.append(f'page_goto_timeout_or_error: {exc}')

    try:
        if stage_state is not None:
            stage_state['stage'] = 'page_domcontentloaded'
        await asyncio.wait_for(page.wait_for_load_state('domcontentloaded'), timeout=CRAWL_DOM_TIMEOUT_SEC)
    except Exception as exc:  # noqa: BLE001
        errors.append(f'page_domcontentloaded_timeout_or_error: {exc}')

    final_url = _normalize_url(page.url or url)
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
        soup = BeautifulSoup(html, 'lxml')
        title = soup.title.get_text(' ', strip=True) if soup.title else ''
        h1_el = soup.find('h1')
        h1 = h1_el.get_text(' ', strip=True) if h1_el else ''
        h2_count = len(soup.find_all('h2'))
        canonical_el = soup.find('link', attrs={'rel': lambda v: v and 'canonical' in v})
        canonical = canonical_el.get('href', '') if canonical_el else ''
        meta_el = soup.find('meta', attrs={'name': 'description'})
        meta_description = meta_el.get('content', '') if meta_el else ''
        text = soup.get_text('\n', strip=True)
        links = await asyncio.wait_for(asyncio.to_thread(_extract_links_from_html, final_url, html), timeout=CRAWL_LINKS_TIMEOUT_SEC)
        if len(links) > MAX_LINKS_FROM_MASSIVE_PAGE:
            links = links[:MAX_LINKS_FROM_MASSIVE_PAGE]
            errors.append(f'link_explosion_capped:{MAX_LINKS_FROM_MASSIVE_PAGE}')
    except Exception as exc:  # noqa: BLE001
        errors.append(f'page_extract_html_timeout_or_error: {exc}')

    status_code = response.status if response else None
    template_type, journey = _classify_template(final_url, title, text[:2500], status_code)
    coverage_confidence = 'Alta' if html and not errors else 'Bassa' if errors else 'Media'

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
        template_type=template_type,
        journey=journey,
        coverage_confidence=coverage_confidence,
        evidence_type='screenshot+dom' if screenshot_path else 'dom',
    )


async def _crawl_site(context: BrowserContext, site: Site, stage_state: dict[str, str] | None = None) -> list[PageResult]:
    results: list[PageResult] = []
    visited: set[str] = set()
    queued: set[str] = set()
    queue: deque[tuple[str, str, int, str, str]] = deque()
    template_seen: Counter[str] = Counter()
    source_noise_counter: Counter[str] = Counter()

    homepage_seed = next((p.url for p in site.pages if (p.type or '').lower() == 'home' and p.url), site.base_url)
    seed = _normalize_url(homepage_seed)
    queue.append((seed, 'home', 0, 'seed_homepage', 'homepage'))
    queued.add(seed)
    _log('DISCOVERY', f'{site.code}: seeded homepage {seed}')

    start_ts = asyncio.get_running_loop().time()
    last_heartbeat = start_ts

    while queue and len(visited) < MAX_PAGES_PER_SITE:
        now = asyncio.get_running_loop().time()
        if now - last_heartbeat >= CRAWL_HEARTBEAT_SEC:
            _crawl_log(f'heartbeat: {site.code} queue={len(queue)} visited={len(visited)} elapsed={int(now-start_ts)}s')
            last_heartbeat = now

        queue = deque(sorted(queue, key=lambda item: _priority(item[0], item[4], item[2])))
        url, page_type, depth, discovered_from, expected_template = queue.popleft()
        normalized = _normalize_url(url)

        if normalized in visited or not _same_market(site, normalized):
            continue
        if template_seen[expected_template] >= MAX_TEMPLATE_REPRESENTATIVES and depth > 1:
            _log('AUDIT', f'skipped duplicate template: site={site.code} template={expected_template} url={normalized}')
            continue

        visited.add(normalized)
        result = await _fetch_page(
            context,
            site,
            page_type,
            normalized,
            crawl_depth=depth,
            discovered_from=discovered_from,
            page_timeout_sec=CRAWL_HOME_TIMEOUT_SEC if depth == 0 else CRAWL_PAGE_TIMEOUT_SEC,
            stage_state=stage_state,
        )
        template_seen[result.template_type] += 1
        results.append(result)
        _log('TEMPLATE', f'classified site={site.code} template={result.template_type} journey={result.journey} depth={depth} url={result.final_url}')

        if depth >= MAX_CRAWL_DEPTH or (result.status_code and result.status_code >= 400):
            continue

        added = 0
        dealer_added = 0
        for raw_link in result.links:
            link = _normalize_url(raw_link)
            if link in visited or link in queued or not _same_market(site, link):
                continue
            if '/dealer' in link.lower() and dealer_added >= MAX_DEALER_LINKS_PER_PAGE:
                _log('AUDIT', f'capped dealer list expansion: site={site.code} source={result.final_url}')
                break
            t_type, _ = _classify_template(link, '', '', None)
            if template_seen[t_type] >= MAX_TEMPLATE_REPRESENTATIVES and depth >= 1:
                continue
            if any(k in link.lower() for k in ('analytics_cookie', 'marketing_cookie')):
                source_noise_counter['cookie_noise'] += 1
                continue
            queue.append((link, page_type, depth + 1, result.final_url or result.url, t_type))
            queued.add(link)
            added += 1
            if '/dealer' in link.lower():
                dealer_added += 1
            if added >= MAX_ENQUEUE_PER_PAGE:
                _log('AUDIT', f'queue cap reached: site={site.code} url={result.final_url} cap={MAX_ENQUEUE_PER_PAGE}')
                break

        if added == 0 and len(result.links) > 20:
            _log('AUDIT', f'page generated mainly duplicate/noise links: site={site.code} url={result.final_url}')

    _log('RUN', f'coverage summary site={site.code} pages={len(results)} templates={dict(template_seen)} noise={dict(source_noise_counter)}')
    return results


async def crawl_sites(sites: list[Site]) -> list[PageResult]:
    results: list[PageResult] = []
    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=True)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + CRAWL_TOTAL_BUDGET_SEC
        for site in sites:
            remaining = deadline - loop.time()
            if remaining <= 0:
                _crawl_log('global timeout reached, stopping remaining markets')
                break
            try:
                site_results = await asyncio.wait_for(_crawl_site(context, site), timeout=min(CRAWL_MARKET_BUDGET_SEC, remaining))
                results.extend(site_results)
            except Exception as exc:  # noqa: BLE001
                _crawl_log(f'market error: {site.code} error={exc}')
        await context.close()
        await browser.close()
    return results


def _mk_finding(page: PageResult, *, category: str, severity: str, title: str, observed: str, expected: str, impact: str, steps: str, evidence: str, confidence: str = 'Media') -> Finding:
    return Finding(
        site_code=page.site_code,
        country=page.country,
        region=page.region,
        url=page.final_url or page.url,
        page_type=page.page_type,
        category=category,
        severity=severity,
        title=title,
        description=f'Osservato: {observed} | Atteso: {expected}',
        impact=impact,
        suggested_fix='Allineare il template e aggiungere regressione test automatizzati su journey critici.',
        fingerprint=make_fingerprint(page.site_code, page.final_url or page.url, title, observed),
        evidence_tecnica=evidence,
        confidence=confidence,
        discovered_from=page.discovered_from,
        crawl_depth=page.crawl_depth,
        screenshot_path=page.screenshot_path,
        template_type=page.template_type,
        journey=page.journey,
        evidence_type=page.evidence_type,
        coverage_confidence=page.coverage_confidence,
        observed=observed,
        expected=expected,
        business_impact=impact,
        repro_steps=steps,
        data={
            'template_type': page.template_type,
            'journey': page.journey,
            'evidence_type': page.evidence_type,
            'coverage_confidence': page.coverage_confidence,
            'observed': observed,
            'expected': expected,
            'repro_steps': steps,
            'screenshot_path': page.screenshot_path,
        },
    )


def run_quality_audit(pages: list[PageResult]) -> list[Finding]:
    findings: list[Finding] = []
    by_site_template: defaultdict[tuple[str, str], list[PageResult]] = defaultdict(list)
    for page in pages:
        by_site_template[(page.site_code, page.template_type)].append(page)

    for page in pages:
        _log('AUDIT', f'rule check start site={page.site_code} template={page.template_type} url={page.final_url or page.url}')
        text = (page.text or '').lower()
        if page.errors:
            findings.append(_mk_finding(
                page,
                category='technical',
                severity='Alta',
                title='Copertura incompleta: pagina letta con errori tecnici',
                observed='Rendering/crawl incompleto con errori runtime o timeout.',
                expected='Pagina letta completamente prima di emettere audit contenutistico.',
                impact='I risultati SEO/UX su questa pagina possono essere parziali e ridurre la credibilità del report.',
                steps=f'1) Aprire {page.final_url or page.url} 2) Verificare timeout/errori in console/network 3) Rieseguire audit.',
                evidence='; '.join(page.errors),
                confidence='Alta',
            ))
        if page.template_type in {'catalog_listing', 'fitment_step', 'dealer_locator'} and len(page.links) < 3:
            findings.append(_mk_finding(page, category='functional', severity='Media', title='Template funzionale con interazioni deboli', observed='Template atteso con navigazione/filtro ma pochi link/interazioni trovate.', expected='Presenza di elementi interattivi sufficienti per proseguire il journey.', impact='Rischio di funnel bloccato o incompleto.', steps='1) Aprire pagina 2) Usare filtri/CTA principali 3) Verificare che il contenuto cambi.', evidence=f'links_count={len(page.links)} template={page.template_type}'))
        if page.template_type == 'homepage' and not re.search(r'(dealer|rivenditor|find your tyre|trova)', text):
            findings.append(_mk_finding(page, category='ux', severity='Media', title='Homepage con entrypoint funnel poco chiaro', observed='Non emergono segnali testuali chiari verso fitment/dealer above the fold.', expected='CTA primarie visibili per trovare pneumatico e rivenditore.', impact='Calo conversione e aumento abbandono in ingresso.', steps='1) Aprire homepage 2) Verificare CTA principali entro primo viewport.', evidence='missing_keywords=dealer|fitment'))
        if page.template_type == 'product_detail' and (not page.h1 or len(page.text.split()) < 120):
            findings.append(_mk_finding(page, category='seo', severity='Media', title='PDP con contenuto debole o incompleto', observed='PDP con H1 assente o testo molto scarno.', expected='PDP con naming chiaro, value proposition e dettagli tecnici minimi.', impact='Riduce fiducia e capacità di confronto prodotto.', steps='1) Aprire PDP 2) Verificare titolo/H1 e blocchi descrittivi.', evidence=f'h1={bool(page.h1)} text_words={len(page.text.split())}'))
        if page.template_type == 'error_page' and page.status_code in {200, 301, 302}:
            findings.append(_mk_finding(page, category='seo', severity='Alta', title='Error page potenzialmente indicizzabile', observed='Pagina errore con status HTTP non coerente.', expected='Error page con status 404/410 e title esplicito.', impact='Segnali SEO incoerenti e crawling inefficiente.', steps='1) Aprire URL errore 2) Verificare status HTTP e meta title.', evidence=f'status_code={page.status_code} title={page.title[:80]}', confidence='Alta'))
        if 'loading' in text and len(page.text.split()) < 30:
            findings.append(_mk_finding(page, category='performance', severity='Media', title='Loader persistente con contenuto tardivo', observed='Pagina mostra pattern da loader con poco contenuto utile.', expected='Contenuto principale visibile in tempi brevi.', impact='Aumenta bounce rate e percezione di lentezza.', steps='1) Aprire pagina 2) Cronometrare comparsa contenuto principale.', evidence='loader_keyword_detected'))

    for (site_code, template), pages_group in by_site_template.items():
        if len(pages_group) >= 3:
            titles = [p.title.strip().lower() for p in pages_group if p.title.strip()]
            if len(titles) >= 3 and len(set(titles)) == 1:
                p = pages_group[0]
                findings.append(_mk_finding(p, category='content', severity='Media', title='Contenuto duplicato tra pagine dello stesso template', observed=f'Template {template} con title ripetuto su più URL.', expected='Title e contenuti differenziati per intento pagina.', impact='Confusione utente e cannibalizzazione SEO.', steps='1) Confrontare 3 URL dello stesso template 2) Validare differenze editoriali.', evidence=f'site={site_code} template={template} duplicated_title={titles[0]}'))

    unique: dict[str, Finding] = {}
    for finding in findings:
        unique[finding.fingerprint] = finding
        _log('AUDIT', f'finding created site={finding.site_code} category={finding.category} severity={finding.severity} title={finding.title}')
    return list(unique.values())
