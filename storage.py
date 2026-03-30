from __future__ import annotations

import re
from datetime import date
from urllib.parse import urlparse

from .fingerprints import make_fingerprint
from .models import Finding, PageResult

CURRENT_YEAR = date.today().year
NON_ENGLISH = {'it', 'de', 'fr', 'es', 'pt', 'zh', 'ja', 'tr', 'nl'}
FOREIGN_LANGUAGE_MARKERS = {
    'es': ['pesquisar', 'onde', 'where are you?', 'servizi disponibili'],
    'pt': ['where are you?', 'vehículo'],
    'it': ['where are you?'],
    'de': ['where are you?'],
    'fr': ['where are you?'],
    'ja': ['where are you?', 'discover more'],
    'tr': ['where are you?', 'all terrain'],
    'nl': ['where are you?', 'servizi disponibili'],
}
SEVERITY_RANK = {'Critica': 4, 'Alta': 3, 'Media': 2, 'Bassa': 1}


def _excerpt(text: str, needle: str, width: int = 90) -> str:
    lower = text.lower()
    idx = lower.find(needle.lower())
    if idx == -1:
        return needle
    start = max(0, idx - width // 2)
    end = min(len(text), idx + len(needle) + width // 2)
    return text[start:end].replace('\n', ' ').strip()


def _finding(
    page: PageResult,
    category: str,
    severity: str,
    title: str,
    explanation_it: str,
    impact_it: str,
    suggested_fix_it: str,
    evidence_tecnica: str,
    confidence: str,
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
        explanation_it=explanation_it,
        impact_it=impact_it,
        suggested_fix_it=suggested_fix_it,
        evidence_tecnica=evidence_tecnica,
        confidence=confidence,
        crawl_depth=page.crawl_depth,
        discovered_from=page.discovered_from,
        fingerprint=make_fingerprint(page.site_code, page.final_url or page.url, title, explanation_it),
    )


def run_rules(pages: list[PageResult]) -> list[Finding]:
    findings: list[Finding] = []
    title_index: dict[tuple[str, str], int] = {}
    h1_index: dict[tuple[str, str], int] = {}

    for page in pages:
        if page.status_code is None or page.status_code >= 400:
            confidence = 'Alta' if page.discovered_from and page.discovered_from != 'seed_homepage' else 'Media'
            title = 'Pagina linkata ma non accessibile' if confidence == 'Alta' else 'Pagina seed non accessibile'
            explanation = 'Una pagina del sito restituisce un errore HTTP oppure non è stata caricata correttamente durante il crawl.'
            impact = 'L’utente può incontrare una pagina non disponibile, interrompere il percorso oppure percepire bassa affidabilità del sito.'
            fix = 'Verificare status code, redirect, URL reali presenti nel menu/homepage e possibili blocchi bot o firewall.'
            evidence = f'Status code: {page.status_code}; URL: {page.final_url or page.url}; scoperta da: {page.discovered_from or "n/d"}'
            findings.append(_finding(page, 'Tecnica', 'Alta', title, explanation, impact, fix, evidence, confidence))

        if not page.title:
            findings.append(_finding(
                page, 'SEO', 'Media', 'Title mancante',
                'La pagina non espone un tag <title> leggibile.',
                'Riduce chiarezza per l’utente e qualità SEO nelle SERP.',
                'Aggiungere un title univoco, descrittivo e localizzato.',
                'Tag <title> assente o vuoto.', 'Alta'
            ))
        else:
            title_key = (page.site_code, page.title.strip().lower())
            title_index[title_key] = title_index.get(title_key, 0) + 1

        if not page.h1:
            findings.append(_finding(
                page, 'SEO', 'Media', 'H1 mancante',
                'La pagina non espone un H1 leggibile.',
                'Riduce la gerarchia semantica, la comprensione del contenuto e la solidità SEO della pagina.',
                'Aggiungere un H1 univoco e coerente con il topic della pagina.',
                'Nessun H1 rilevato nel markup.', 'Alta'
            ))
        else:
            h1_key = (page.site_code, page.h1.strip().lower())
            h1_index[h1_key] = h1_index.get(h1_key, 0) + 1

        if re.search(r'\bundefined\b', page.text, re.IGNORECASE) or 'undefined' in (page.final_url or '').lower():
            findings.append(_finding(
                page, 'Tecnica', 'Alta', 'Placeholder tecnico visibile',
                'La pagina mostra il token tecnico “undefined”, segnale di rendering incompleto o data binding non valorizzato.',
                'Riduce fiducia, qualità percepita e credibilità del contenuto, soprattutto in pagine ad alta intenzione.',
                'Correggere il template e introdurre test automatici anti-placeholder in produzione.',
                _excerpt(page.text or page.final_url, 'undefined'), 'Alta'
            ))

        if page.language in NON_ENGLISH and re.search(r'where are you\?', page.text, re.IGNORECASE):
            findings.append(_finding(
                page, 'Contenuti', 'Media', 'Placeholder dealer non localizzato',
                'Nel ramo locale compare ancora la stringa inglese “Where are you?” all’interno del dealer flow.',
                'Riduce coerenza linguistica, può creare attrito e abbassa la qualità percepita del mercato locale.',
                'Localizzare il componente dealer, placeholder, label e messaggi di errore.',
                _excerpt(page.text, 'Where are you?'), 'Alta'
            ))

        if re.search(r'<h[1-6][^>]*>\s*</h[1-6]>', page.html, re.IGNORECASE):
            findings.append(_finding(
                page, 'SEO', 'Media', 'Heading vuoto nel markup',
                'Nel markup è presente almeno un heading senza contenuto testuale.',
                'Indebolisce la struttura semantica della pagina per SEO e accessibilità.',
                'Rimuovere heading placeholder o valorizzarli correttamente nel template.',
                'Rilevato heading vuoto nel sorgente HTML.', 'Media'
            ))

        if page.canonical and page.final_url and urlparse(page.canonical).path != urlparse(page.final_url).path:
            findings.append(_finding(
                page, 'SEO', 'Bassa', 'Canonical diverso dal final URL',
                'Il canonical della pagina non coincide con il percorso finale servito al crawler.',
                'Potrebbe essere corretto, ma è un segnale da verificare per evitare incoerenze SEO tra canonical e redirect.',
                'Controllare la logica di canonical, redirect e localizzazione del ramo.',
                f'Canonical: {page.canonical} | Final URL: {page.final_url}', 'Media'
            ))

        old_years = re.findall(r'\b(20\d{2})\b', page.text)
        stale_years = sorted({int(y) for y in old_years if int(y) < CURRENT_YEAR})
        if stale_years and page.page_type == 'home':
            findings.append(_finding(
                page, 'Contenuti', 'Media', 'Anno passato visibile in homepage',
                'In homepage compaiono riferimenti ad anni precedenti che potrebbero appartenere a promo o contenuti non più aggiornati.',
                'Può generare percezione di sito non presidiato, contenuti scaduti o informazioni non più rilevanti.',
                'Verificare se i riferimenti sono evergreen oppure contenuti da aggiornare o rimuovere.',
                f'Anni trovati: {", ".join(map(str, stale_years))}', 'Media'
            ))

        markers = FOREIGN_LANGUAGE_MARKERS.get(page.language, [])
        for marker in markers:
            if marker.lower() in page.text.lower():
                findings.append(_finding(
                    page, 'Contenuti', 'Media', 'Possibile leakage di altra lingua',
                    'Nel ramo locale è presente una stringa che sembra appartenere a un’altra lingua o a un template shared non tradotto.',
                    'Riduce la qualità percepita del mercato locale e può minare fiducia e comprensione.',
                    'Revisionare le stringhe shared del template e il QA di localizzazione.',
                    _excerpt(page.text, marker), 'Media'
                ))
                break

        broken_pattern = [link for link in page.links if 'undefined' in link.lower()]
        if broken_pattern:
            findings.append(_finding(
                page, 'Tecnica', 'Critica', 'URL malformato nei link interni',
                'La pagina contiene almeno un link interno con token tecnico “undefined”.',
                'Rottura diretta del funnel e rischio di errore su percorsi importanti come dealer o prodotto.',
                'Correggere la generazione dei link e introdurre un link checker automatico in CI.',
                f'Link trovati: {broken_pattern[:3]}', 'Alta'
            ))

        if page.errors:
            findings.append(_finding(
                page, 'Tecnica', 'Bassa', 'Errori durante il crawling',
                'Durante il fetch o il parsing si sono verificati errori tecnici non bloccanti.',
                'La pagina potrebbe essere fragile, servire contenuto incompleto o dipendere troppo da JS.',
                'Verificare eventuali blocchi, timeout o dipendenze front-end critiche.',
                '; '.join(page.errors), 'Media'
            ))

    for page in pages:
        if page.title and title_index.get((page.site_code, page.title.strip().lower()), 0) > 1:
            findings.append(_finding(
                page, 'SEO', 'Bassa', 'Title duplicato nel market',
                'Lo stesso title compare su più pagine dello stesso mercato.',
                'Riduce unicità SEO e chiarezza nelle SERP.',
                'Rendere il title più specifico per template o contenuto.',
                page.title, 'Media'
            ))
        if page.h1 and h1_index.get((page.site_code, page.h1.strip().lower()), 0) > 1:
            findings.append(_finding(
                page, 'SEO', 'Bassa', 'H1 duplicato nel market',
                'Lo stesso H1 compare su più pagine dello stesso mercato.',
                'Riduce differenziazione tra pagine e chiarezza semantica.',
                'Diversificare l’H1 in base al contenuto della pagina.',
                page.h1, 'Media'
            ))

    unique: list[Finding] = []
    seen: set[str] = set()
    for finding in sorted(findings, key=lambda f: (-SEVERITY_RANK.get(f.severity, 0), f.site_code, f.title, f.url)):
        if finding.fingerprint in seen:
            continue
        seen.add(finding.fingerprint)
        unique.append(finding)
    return unique
