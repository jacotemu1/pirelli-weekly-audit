from __future__ import annotations

import asyncio
import os
import re

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .fingerprints import make_fingerprint
from .models import Finding, FitmentCase, Site

FITMENT_LABELS = {
    'vehicle': ['vehicle', 'veicolo', 'auto', 'car'],
    'size': ['size', 'misura', 'dimension'],
    'plate': ['plate', 'targa', 'license'],
}
LOCALIZATION_MISMATCH_MARKERS = ('where are you?', 'search by plate', 'find your dealer')
COOKIE_BUTTON_HINTS = (
    'accept', 'agree', 'ok', 'consent', 'allow all',
    'accetta', 'accetto', 'accettare', 'consenti',
    'j\'accepte', 'aceptar', 'zustimmen'
)
OVERLAY_CLOSE_HINTS = ('close', 'chiudi', 'dismiss', 'x')
BLOCKING_OVERLAY_SELECTORS = (
    '[id*="cookie" i]',
    '[class*="cookie" i]',
    '[id*="consent" i]',
    '[class*="consent" i]',
    '[id*="overlay" i]',
    '[class*="overlay" i]',
    '[role="dialog"]',
)
FITMENT_NAV_TIMEOUT_MS = int(os.getenv('PIRELLI_FITMENT_NAV_TIMEOUT_MS', '25000'))
FITMENT_ENTRY_TIMEOUT_MS = int(os.getenv('PIRELLI_FITMENT_ENTRY_TIMEOUT_MS', '3500'))
FITMENT_CLICK_TIMEOUT_MS = int(os.getenv('PIRELLI_FITMENT_CLICK_TIMEOUT_MS', '4500'))
FITMENT_POST_CLICK_WAIT_MS = int(os.getenv('PIRELLI_FITMENT_POST_CLICK_WAIT_MS', '1200'))
FITMENT_MARKET_BUDGET_SEC = int(os.getenv('PIRELLI_FITMENT_MARKET_BUDGET_SEC', '50'))
FITMENT_JOURNEY_TIMEOUT_SEC = int(os.getenv('PIRELLI_FITMENT_JOURNEY_TIMEOUT_SEC', '22'))
FITMENT_GLOBAL_BUDGET_SEC = int(os.getenv('PIRELLI_FITMENT_GLOBAL_BUDGET_SEC', os.getenv('PIRELLI_FITMENT_TOTAL_BUDGET_SEC', '900')))


def _fitment_log(message: str) -> None:
    print(f'[FITMENT] {message}', flush=True)


def _mk_finding(
    site: Site,
    url: str,
    severity: str,
    title: str,
    description: str,
    impact: str,
    fix: str,
    evidence: str,
    confidence: str,
    fitment_tipo: str,
    fitment_step: str,
) -> Finding:
    return Finding(
        site_code=site.code,
        country=site.country,
        region=site.region,
        url=url,
        page_type='fitment',
        category='Fitment',
        severity=severity,
        title=title,
        description=description,
        impact=impact,
        suggested_fix=fix,
        fingerprint=make_fingerprint(site.code, url, title, f'{fitment_tipo}|{fitment_step}|{description}'),
        evidence_tecnica=evidence,
        confidence=confidence,
        discovered_from='fitment_browser_journey',
        crawl_depth=0,
        fitment_tipo=fitment_tipo,
        fitment_step=fitment_step,
        data={
            'evidenza_tecnica': evidence,
            'confidence': confidence,
            'fitment_tipo': fitment_tipo,
            'fitment_step': fitment_step,
            'discovered_from': 'fitment_browser_journey',
            'crawl_depth': 0,
        },
    )


async def _find_fitment_entry(page: Page, fitment_type: str) -> str | None:
    labels = FITMENT_LABELS.get(fitment_type, [fitment_type])
    for label in labels:
        locator = page.locator(f'a:has-text("{label}"), button:has-text("{label}"), [role="button"]:has-text("{label}")').first
        try:
            if await asyncio.wait_for(locator.count(), timeout=FITMENT_ENTRY_TIMEOUT_MS / 1000) > 0:
                return label
        except Exception:
            continue
    return None


async def _dismiss_cookie_overlay(page: Page) -> None:
    # best effort: do not fail test if banner is not present
    try:
        for hint in COOKIE_BUTTON_HINTS:
            btn = page.locator(
                f'button:has-text("{hint}"), [role="button"]:has-text("{hint}"), '
                f'input[type="button"][value*="{hint}"], input[type="submit"][value*="{hint}"]'
            ).first
            if await btn.count() > 0:
                try:
                    await btn.click(timeout=2000)
                    await page.wait_for_timeout(400)
                    return
                except Exception:
                    pass

        # common consent/overlay close targets
        for hint in OVERLAY_CLOSE_HINTS:
            close_btn = page.locator(
                f'button[aria-label*="{hint}" i], button:has-text("{hint}"), '
                f'[role="button"][aria-label*="{hint}" i]'
            ).first
            if await close_btn.count() > 0:
                try:
                    await close_btn.click(timeout=1500)
                    await page.wait_for_timeout(300)
                except Exception:
                    pass
    except Exception:
        pass


async def _safe_click(page: Page, locator, timeout_ms: int = FITMENT_CLICK_TIMEOUT_MS) -> tuple[bool, str]:
    try:
        await locator.scroll_into_view_if_needed(timeout=1500)
    except Exception:
        pass

    try:
        await locator.click(timeout=timeout_ms)
        await page.wait_for_timeout(min(timeout_ms, FITMENT_POST_CLICK_WAIT_MS))
        return True, 'standard_click'
    except Exception as first_exc:  # noqa: BLE001
        await _dismiss_cookie_overlay(page)
        try:
            await locator.click(timeout=max(2500, timeout_ms - 1000))
            await page.wait_for_timeout(min(timeout_ms, FITMENT_POST_CLICK_WAIT_MS))
            return True, 'retry_click'
        except Exception as second_exc:  # noqa: BLE001
            try:
                await locator.click(timeout=2200, force=True)
                await page.wait_for_timeout(min(timeout_ms, FITMENT_POST_CLICK_WAIT_MS))
                return True, 'force_click'
            except Exception as third_exc:  # noqa: BLE001
                return False, f'click1={first_exc}; click2={second_exc}; click3={third_exc}'


async def _find_blocking_overlay(page: Page) -> str:
    for selector in BLOCKING_OVERLAY_SELECTORS:
        try:
            loc = page.locator(selector).first
            if await loc.count() > 0 and await loc.is_visible():
                return selector
        except Exception:
            continue
    return ''


async def _check_fitment_type(site: Site, page: Page, case: FitmentCase, fitment_type: str) -> list[Finding]:
    findings: list[Finding] = []
    await _dismiss_cookie_overlay(page)
    entry_label = await _find_fitment_entry(page, fitment_type)
    current_url = page.url or case.market_url

    if not entry_label:
        findings.append(
            _mk_finding(
                site,
                current_url,
                'Media',
                'Ingresso fitment non trovato',
                'Non è stato trovato nel DOM un ingresso chiaro al percorso fitment richiesto.',
                'L’utente potrebbe non riuscire ad avviare la ricerca pneumatici in modo guidato.',
                'Verificare visibilità del componente fitment, CTA e localizzazione etichette in homepage.',
                f'fitment_type={fitment_type}; expected_labels={FITMENT_LABELS.get(fitment_type, [fitment_type])}',
                'Media',
                fitment_type,
                'entrypoint',
            )
        )
        return findings

    entry = page.locator(f'a:has-text("{entry_label}"), button:has-text("{entry_label}"), [role="button"]:has-text("{entry_label}")').first
    click_ok, click_mode = await _safe_click(page, entry, timeout_ms=FITMENT_CLICK_TIMEOUT_MS)
    if not click_ok:
        blocking_overlay = await _find_blocking_overlay(page)
        findings.append(
            _mk_finding(
                site,
                current_url,
                'Alta',
                'CTA fitment non cliccabile',
                'Il punto di ingresso fitment è visibile ma non è cliccabile in modo stabile.',
                'Blocca il funnel di selezione pneumatici e riduce conversione.',
                'Verificare overlay/cookie banner, z-index e listener JS della CTA fitment.',
                f'click_error={click_mode}; fitment_type={fitment_type}; label={entry_label}; blocking_overlay={blocking_overlay}',
                'Alta',
                fitment_type,
                'click_entry',
            )
        )
        return findings

    html = await page.content()
    page_text = html.lower()
    has_spinner = bool(re.search(r'loading|spinner|caricamento', page_text))
    has_no_results = bool(re.search(r'no results|nessun risultato|nessun prodotto', page_text))
    has_next_step = bool(re.search(r'select|seleziona|choose|marca|modello|misura|diametro', page_text))
    has_explanation = bool(re.search(r'suggerimento|consiglio|try|prova|modifica', page_text))

    if has_spinner and not has_next_step and not has_no_results:
        findings.append(
            _mk_finding(
                site,
                page.url or current_url,
                'Alta',
                'Fitment bloccato su caricamento',
                'Dopo il click, il journey fitment sembra restare in loading senza avanzare.',
                'L’utente non riesce a completare la scelta pneumatici e abbandona il funnel.',
                'Controllare chiamate API fitment, timeout frontend e gestione errori asincroni.',
                f'fitment_type={fitment_type}; spinner_detected=true; url={page.url}',
                'Media',
                fitment_type,
                'post_click_loading',
            )
        )
    if has_spinner and not has_next_step:
        findings.append(
            _mk_finding(
                site,
                page.url or current_url,
                'Media',
                'Spinner fitment non termina',
                'Il widget fitment mostra loading persistente senza avanzare al passo successivo.',
                'Il journey può restare bloccato e aumentare l’abbandono utente.',
                'Introdurre timeout frontend e fallback UX quando le API fitment non rispondono.',
                f'fitment_type={fitment_type}; spinner=true; next_step={has_next_step}; no_results={has_no_results}',
                'Media',
                fitment_type,
                'spinner_timeout',
            )
        )

    if has_no_results and not has_explanation:
        findings.append(
            _mk_finding(
                site,
                page.url or current_url,
                'Media',
                'No-results senza spiegazione utile',
                'Il journey fitment mostra assenza risultati ma senza indicazioni operative per l’utente.',
                'Aumenta l’abbandono del funnel e riduce conversione.',
                'Aggiungere messaggi guidati con alternative: misura vicina, reset filtri o percorso dealer.',
                f'fitment_type={fitment_type}; no_results=true; explanation=false',
                'Media',
                fitment_type,
                'no_results_message',
            )
        )
    if not has_next_step and not has_no_results and not has_spinner:
        findings.append(
            _mk_finding(
                site,
                page.url or current_url,
                'Media',
                'Risultati fitment non visualizzati',
                'Dopo l’ingresso fitment non sono stati rilevati né step utili né risultati visibili.',
                'Il funnel risulta ambiguo e rischia di interrompersi senza feedback chiaro.',
                'Verificare rendering del widget e messaggi di stato per gli step intermedi.',
                f'fitment_type={fitment_type}; next_step={has_next_step}; no_results={has_no_results}; spinner={has_spinner}',
                'Media',
                fitment_type,
                'results_not_visible',
            )
        )

    if site.language != 'en':
        for marker in LOCALIZATION_MISMATCH_MARKERS:
            if marker in page_text:
                findings.append(
                    _mk_finding(
                        site,
                        page.url or current_url,
                        'Media',
                        'Placeholder fitment non localizzato',
                        'Nel widget fitment compaiono stringhe inglesi in un mercato non inglese.',
                        'Riduce fiducia e comprensione del percorso di scelta pneumatici.',
                        'Localizzare testi del widget fitment, inclusi placeholder e messaggi di errore.',
                        f'fitment_type={fitment_type}; marker={marker}',
                        'Alta',
                        fitment_type,
                        'localization',
                    )
                )
                break

    # Dropdown popolamento minimo (quando presenti)
    select_count = await page.locator('select').count()
    if select_count > 0:
        first_select = page.locator('select').first
        option_count = await first_select.locator('option').count()
        if option_count <= 1:
            findings.append(
                _mk_finding(
                    site,
                    page.url or current_url,
                    'Alta',
                    'Dropdown fitment non popolato',
                    'Il primo step del fitment presenta dropdown senza opzioni utili.',
                    'L’utente non può procedere nella selezione e abbandona il funnel.',
                    'Verificare API/source dati del fitment e fallback lato frontend.',
                    f'fitment_type={fitment_type}; select_options={option_count}',
                    'Alta',
                    fitment_type,
                    'dropdown_population',
                )
            )

    # CTA finale verso catalogo/dealer irraggiungibile
    final_cta = page.locator(
        'a:has-text("catalog"), a:has-text("catalogue"), a:has-text("dealer"), '
        'a:has-text("rivend"), a:has-text("distributor"), a:has-text("shop")'
    ).first
    try:
        if await final_cta.count() > 0:
            href = await final_cta.get_attribute('href')
            if href:
                target_url = page.url.split('/')[0] + '//' + page.url.split('/')[2] if page.url else current_url
                if href.startswith('/'):
                    target_url = target_url + href
                elif href.startswith('http'):
                    target_url = href
                resp = await page.request.get(target_url, timeout=10000)
                if resp.status >= 400:
                    findings.append(
                        _mk_finding(
                            site,
                            page.url or current_url,
                            'Alta',
                            'CTA finale fitment irraggiungibile',
                            'Il link finale del fitment verso catalogo/dealer restituisce errore HTTP.',
                            'Interrompe il funnel nella fase più vicina alla conversione.',
                            'Correggere URL/redirect della CTA finale e aggiungere monitor automatico link fitment.',
                            f'fitment_type={fitment_type}; cta_url={target_url}; status={resp.status}',
                            'Alta',
                            fitment_type,
                            'final_cta',
                        )
                    )
    except Exception as exc:  # noqa: BLE001
        findings.append(
            _mk_finding(
                site,
                page.url or current_url,
                'Media',
                'Errore tecnico nel controllo CTA finale fitment',
                'Il controllo automatico della CTA finale fitment non è riuscito a completarsi.',
                'Riduce osservabilità su un punto chiave del funnel.',
                'Verificare selettori CTA e stabilità delle chiamate lato client/API.',
                f'fitment_type={fitment_type}; final_cta_check_error={exc}',
                'Bassa',
                fitment_type,
                'final_cta_check',
            )
        )

    return findings


async def run_fitment_checks(sites: list[Site], cases: dict[str, FitmentCase]) -> list[Finding]:
    findings: list[Finding] = []
    if not cases:
        return findings

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=True)
        context: BrowserContext = await browser.new_context(ignore_https_errors=True)
        loop = asyncio.get_running_loop()
        global_deadline = loop.time() + FITMENT_GLOBAL_BUDGET_SEC

        async def _run_market(site: Site, case: FitmentCase) -> None:
            page = await context.new_page()
            try:
                await page.goto(case.market_url, wait_until='domcontentloaded', timeout=FITMENT_NAV_TIMEOUT_MS)
                await page.wait_for_timeout(800)
                for fitment_type in case.expected_types[:2]:
                    _fitment_log(f'journey start: {site.code} {fitment_type}')
                    try:
                        journey_findings = await asyncio.wait_for(
                            _check_fitment_type(site, page, case, fitment_type),
                            timeout=FITMENT_JOURNEY_TIMEOUT_SEC,
                        )
                        findings.extend(journey_findings)
                    except asyncio.TimeoutError:
                        _fitment_log(f'journey timeout: {site.code} {fitment_type}')
                        findings.append(
                            _mk_finding(
                                site,
                                page.url or case.market_url,
                                'Media',
                                'Timeout journey fitment',
                                'Il journey fitment ha superato il timeout operativo previsto.',
                                'Riduce la copertura del controllo automatico su un flusso chiave.',
                                'Verificare performance JS/API e stabilità del widget nel mercato.',
                                f'fitment_type={fitment_type}; timeout_sec={FITMENT_JOURNEY_TIMEOUT_SEC}',
                                'Alta',
                                fitment_type,
                                'journey_timeout',
                            )
                        )
                    except Exception as journey_exc:  # noqa: BLE001
                        findings.append(
                            _mk_finding(
                                site,
                                page.url or case.market_url,
                                'Media',
                                'Errore tecnico nel runner fitment',
                                'Il runner del journey fitment ha generato un errore non bloccante.',
                                'Riduce la copertura del controllo, ma la run globale continua.',
                                'Verificare selettori, overlay e disponibilità elementi fitment nel mercato.',
                                f'fitment_type={fitment_type}; journey_error={journey_exc}',
                                'Media',
                                fitment_type,
                                'journey_error',
                            )
                        )
            finally:
                await page.close()

        for index, site in enumerate(sites, start=1):
            case = cases.get(site.code.upper())
            if not case or not case.market_url or not case.expected_types:
                continue

            remaining_global = global_deadline - loop.time()
            if remaining_global <= 0:
                _fitment_log('global budget exceeded, stopping remaining fitment tasks')
                break

            market_timeout = min(FITMENT_MARKET_BUDGET_SEC, remaining_global)
            _fitment_log(f'market start: {site.code} ({index}/{len(sites)})')
            try:
                await asyncio.wait_for(_run_market(site, case), timeout=market_timeout)
            except asyncio.TimeoutError:
                _fitment_log(f'market timeout: {site.code}')
                findings.append(
                    _mk_finding(
                        site,
                        case.market_url,
                        'Media',
                        'Fitment interrotto per timeout complessivo',
                        'Il budget massimo di tempo per il fitment del mercato è stato superato.',
                        'La copertura fitment del mercato risulta parziale nella run corrente.',
                        'Ridurre complessità journey o aumentare budget solo se necessario.',
                        f'market_budget_sec={FITMENT_MARKET_BUDGET_SEC}; site={site.code}',
                        'Alta',
                        'generic',
                        'market_budget',
                    )
                )
            except Exception as exc:  # noqa: BLE001
                findings.append(
                    _mk_finding(
                        site,
                        case.market_url,
                        'Alta',
                        'Errore tecnico durante test fitment',
                        'Il test browser-based fitment non è riuscito a completarsi.',
                        'Riduce la copertura dei journey interattivi ad alto impatto business.',
                        'Verificare disponibilità pagina, blocchi bot e stabilità dei componenti fitment.',
                        f'fitment_runner_error={exc}',
                        'Media',
                        'generic',
                        'runner',
                    )
                )
            finally:
                market_fitment_findings = len([f for f in findings if f.site_code == site.code and f.page_type == 'fitment'])
                _fitment_log(f'market done: {site.code} findings={market_fitment_findings}')

        await context.close()
        await browser.close()

    return findings
