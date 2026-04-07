from __future__ import annotations

import re
from datetime import date
from urllib.parse import urlparse

from .fingerprints import make_fingerprint
from .models import Finding, PageResult

CURRENT_YEAR = date.today().year
NON_ENGLISH = {'it', 'de', 'fr', 'es', 'pt', 'zh', 'ja', 'tr', 'nl'}
FOREIGN_LANGUAGE_MARKERS = {
    'es': ['where are you?', 'servizi disponibili'],
    'pt': ['where are you?', 'vehículo'],
    'it': ['where are you?'],
    'de': ['where are you?'],
    'fr': ['where are you?'],
    'ja': ['where are you?', 'discover more'],
    'tr': ['where are you?', 'all terrain'],
    'nl': ['where are you?', 'servizi disponibili'],
}


def _finding(
    page: PageResult,
    category: str,
    severity: str,
    title: str,
    description: str,
    impact: str,
    suggested_fix: str,
    evidence: str,
    confidence: str = 'Media',
) -> Finding:
    return Finding(
        site_code=page.site_code,
        country=page.country,
        region=page.region,
        url=page.final_url or page.url,
        page_type=page.page_type,
        category=category,
        severity=severity,
        title=title,
        description=description,
        impact=impact,
        suggested_fix=suggested_fix,
        fingerprint=make_fingerprint(page.site_code, page.final_url or page.url, title, description),
        evidence_tecnica=evidence,
        confidence=confidence,
        discovered_from=page.discovered_from,
        crawl_depth=page.crawl_depth,
        data={
            'evidenza_tecnica': evidence,
            'confidence': confidence,
            'discovered_from': page.discovered_from,
            'crawl_depth': page.crawl_depth,
        },
    )


def run_rules(pages: list[PageResult]) -> list[Finding]:
    findings: list[Finding] = []
    title_index: dict[tuple[str, str], int] = {}
    h1_index: dict[tuple[str, str], int] = {}

    for page in pages:
        source_label = page.discovered_from or 'seed_homepage'

        if page.status_code is None or page.status_code >= 400:
            confidence = 'Alta' if page.crawl_depth > 0 else 'Media'
            severity = 'Media' if page.page_type == 'dealer' and page.crawl_depth > 0 else 'Alta'
            findings.append(
                _finding(
                    page,
                    'technical',
                    severity,
                    'Pagina non accessibile o con errore HTTP',
                    'La pagina restituisce errore HTTP oppure non è stata caricata correttamente durante il crawl.',
                    'Un utente può interrompere il percorso e perdere fiducia nel sito.',
                    'Verificare redirect, disponibilità reale della pagina e regole di sicurezza lato CDN/firewall.',
                    f'status_code={page.status_code}; source={source_label}',
                    confidence,
                )
            )

        if not page.title:
            findings.append(
                _finding(
                    page,
                    'seo',
                    'Media',
                    'Title mancante',
                    'La pagina non espone un tag <title> utile ai motori di ricerca.',
                    'Riduce la visibilità SEO e la chiarezza del risultato in SERP.',
                    'Aggiungere un title univoco, descrittivo e localizzato.',
                    'title vuoto o assente nel markup',
                    'Alta',
                )
            )
        else:
            title_index[(page.site_code, page.title.strip().lower())] = title_index.get((page.site_code, page.title.strip().lower()), 0) + 1

        if not page.h1:
            findings.append(
                _finding(
                    page,
                    'seo',
                    'Media',
                    'H1 mancante',
                    'La pagina non espone un heading H1 leggibile.',
                    'Peggiora la comprensione del contenuto da parte di utenti e motori di ricerca.',
                    'Inserire un H1 unico e coerente con il tema della pagina.',
                    'h1 non rilevato',
                    'Alta',
                )
            )
        else:
            h1_index[(page.site_code, page.h1.strip().lower())] = h1_index.get((page.site_code, page.h1.strip().lower()), 0) + 1

        if re.search(r'<h[1-6][^>]*>\s*</h[1-6]>', page.html, re.IGNORECASE):
            findings.append(
                _finding(
                    page,
                    'accessibility',
                    'Media',
                    'Heading vuoto nel markup',
                    'È presente almeno un heading senza contenuto testuale.',
                    'Riduce accessibilità, comprensione semantica e qualità SEO.',
                    'Pulire il template eliminando heading placeholder o vuoti.',
                    'regex heading vuoto trovata nel codice HTML',
                    'Alta',
                )
            )

        if re.search(r'\bundefined\b', page.text, re.IGNORECASE) or 'undefined' in (page.final_url or '').lower():
            findings.append(
                _finding(
                    page,
                    'technical',
                    'Alta',
                    'Placeholder tecnico visibile (undefined)',
                    'La pagina mostra token tecnici non risolti visibili all’utente.',
                    'Percezione di bassa qualità e possibile blocco nel funnel.',
                    'Correggere template/data binding e aggiungere test anti-placeholder in CI.',
                    'stringa "undefined" rilevata in testo o URL finale',
                    'Alta',
                )
            )

        if page.language in NON_ENGLISH and re.search(r'where are you\?', page.text, re.IGNORECASE):
            findings.append(
                _finding(
                    page,
                    'content',
                    'Media',
                    'Testo non localizzato nel dealer flow',
                    'Sono presenti stringhe inglesi in un mercato non inglese.',
                    'Riduce fiducia e chiarezza durante la ricerca rivenditore.',
                    'Localizzare testi, placeholder e messaggi di errore del componente dealer.',
                    'rilevata stringa "where are you?" in mercato non EN',
                    'Alta',
                )
            )

        old_years = sorted({int(y) for y in re.findall(r'\b(20\d{2})\b', page.text) if int(y) < CURRENT_YEAR})
        if old_years and page.page_type == 'home':
            findings.append(
                _finding(
                    page,
                    'content',
                    'Media',
                    'Riferimenti ad anni passati in homepage',
                    f'In homepage compaiono riferimenti potenzialmente obsoleti: {", ".join(map(str, old_years))}.',
                    'Comunicazione meno efficace e rischio percezione di contenuti non aggiornati.',
                    'Verificare se i riferimenti sono voluti o se è necessario aggiornare i contenuti editoriali.',
                    f'anni individuati: {old_years}',
                )
            )

        for marker in FOREIGN_LANGUAGE_MARKERS.get(page.language, []):
            if marker.lower() in page.text.lower():
                findings.append(
                    _finding(
                        page,
                        'content',
                        'Media',
                        'Possibile leakage linguistico',
                        'La pagina mostra una stringa tipica di un’altra localizzazione.',
                        'Incoerenza di brand e possibile confusione lato utente.',
                        'Controllare fallback lingua, traduzioni e contenuti condivisi nel CMS.',
                        f'marker rilevato: {marker}',
                    )
                )
                break

        if page.canonical and page.final_url and urlparse(page.canonical).path != urlparse(page.final_url).path:
            findings.append(
                _finding(
                    page,
                    'seo',
                    'Bassa',
                    'Canonical non allineato alla pagina finale',
                    'Il canonical punta a un path diverso rispetto all’URL finale.',
                    'Possibili segnali SEO incoerenti e perdita di rilevanza.',
                    'Verificare logica canonical, redirect e varianti locale/mercato.',
                    f'canonical={page.canonical}; final_url={page.final_url}',
                )
            )

        cta_count = len(re.findall(r'\b(discover more|scopri di più|mehr erfahren|découvrir|descubre más|ontdek meer|daha fazla)\b', page.text, re.IGNORECASE))
        if cta_count >= 4 and page.page_type == 'home':
            findings.append(
                _finding(
                    page,
                    'cro',
                    'Media',
                    'CTA generiche molto ripetute',
                    f'Rilevate {cta_count} call-to-action molto simili nella stessa pagina.',
                    'Riduce chiarezza del percorso e la capacità di conversione.',
                    'Differenziare le CTA in base all’obiettivo utente (acquisto, confronto, dealer, assistenza).',
                    f'cta_count={cta_count}',
                )
            )

        if page.errors:
            findings.append(
                _finding(
                    page,
                    'technical',
                    'Bassa',
                    'Errori tecnici durante il crawl',
                    'Durante il rendering/crawl sono stati registrati errori tecnici.',
                    'Possibile instabilità pagina o fragilità dell’esperienza utente.',
                    'Analizzare log applicativi/CDN e risolvere timeout o errori JS intermittenti.',
                    '; '.join(page.errors),
                )
            )

    for page in pages:
        title_key = (page.site_code, page.title.strip().lower()) if page.title else None
        if title_key and title_index.get(title_key, 0) > 1:
            findings.append(
                _finding(
                    page,
                    'seo',
                    'Bassa',
                    'Title duplicato nel mercato',
                    'Più pagine condividono lo stesso title all’interno dello stesso mercato.',
                    'Diminuisce la differenziazione SEO tra pagine diverse.',
                    'Rendere i title univoci, includendo tema pagina e segmento.',
                    f'title duplicato: {page.title}',
                )
            )
        h1_key = (page.site_code, page.h1.strip().lower()) if page.h1 else None
        if h1_key and h1_index.get(h1_key, 0) > 1:
            findings.append(
                _finding(
                    page,
                    'ux',
                    'Bassa',
                    'H1 duplicato nel mercato',
                    'Lo stesso H1 compare su più pagine del mercato.',
                    'Riduce chiarezza informativa e orientamento utente.',
                    'Differenziare gli H1 per template e intento di ricerca.',
                    f'h1 duplicato: {page.h1}',
                )
            )

    return dedupe_findings(findings)


def dedupe_findings(findings: list[Finding]) -> list[Finding]:
    seen: set[str] = set()
    unique: list[Finding] = []
    for finding in findings:
        if finding.fingerprint in seen:
            continue
        seen.add(finding.fingerprint)
        unique.append(finding)
    return unique
