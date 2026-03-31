from __future__ import annotations

import asyncio
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
            if await locator.count() > 0:
                return label
        except Exception:
            continue
    return None


async def _check_fitment_type(site: Site, page: Page, case: FitmentCase, fitment_type: str) -> list[Finding]:
    findings: list[Finding] = []
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
    try:
        await entry.click(timeout=5000)
        await page.wait_for_timeout(1200)
    except Exception as exc:  # noqa: BLE001
        findings.append(
            _mk_finding(
                site,
                current_url,
                'Alta',
                'CTA fitment non cliccabile',
                'Il punto di ingresso fitment è visibile ma non è cliccabile in modo stabile.',
                'Blocca il funnel di selezione pneumatici e riduce conversione.',
                'Verificare overlay, z-index, listener JS e comportamento responsive della CTA fitment.',
                f'click_error={exc}; fitment_type={fitment_type}; label={entry_label}',
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

        sem = asyncio.Semaphore(2)

        async def run_one(site: Site) -> None:
            case = cases.get(site.code.upper())
            if not case or not case.market_url or not case.expected_types:
                return
            async with sem:
                page = await context.new_page()
                try:
                    await page.goto(case.market_url, wait_until='domcontentloaded', timeout=45000)
                    await page.wait_for_timeout(1200)
                    for fitment_type in case.expected_types[:2]:
                        findings.extend(await _check_fitment_type(site, page, case, fitment_type))
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
                    await page.close()

        await asyncio.gather(*[run_one(site) for site in sites])
        await context.close()
        await browser.close()

    return findings
