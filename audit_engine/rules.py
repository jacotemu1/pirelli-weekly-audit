from __future__ import annotations

import re
from datetime import date
from urllib.parse import urlparse

from .fingerprints import make_fingerprint
from .models import Finding, PageResult

CURRENT_YEAR = date.today().year
NON_ENGLISH = {'it', 'de', 'fr', 'es', 'pt', 'zh', 'ja', 'tr', 'nl'}
FOREIGN_LANGUAGE_MARKERS = {
    'es': ['pesquisar', 'onde', 'veiculo', 'where are you?', 'servizi disponibili'],
    'pt': ['where are you?', 'vehículo'],
    'it': ['where are you?'],
    'de': ['where are you?'],
    'fr': ['where are you?'],
    'ja': ['where are you?', 'discover more'],
    'tr': ['where are you?', 'all terrain'],
    'nl': ['where are you?', 'servizi disponibili'],
}


def _finding(page: PageResult, category: str, severity: str, title: str, description: str, impact: str, suggested_fix: str) -> Finding:
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
    )


def run_rules(pages: list[PageResult]) -> list[Finding]:
    findings: list[Finding] = []
    title_index: dict[tuple[str, str], int] = {}
    h1_index: dict[tuple[str, str], int] = {}

    for page in pages:
        if page.status_code is None or page.status_code >= 400:
            findings.append(_finding(page, 'technical', 'Alta', 'Pagina non accessibile o errore HTTP', f'Status code: {page.status_code}; errors: {page.errors}', 'La pagina non è disponibile o non è analizzabile correttamente.', 'Verificare availability, redirect e blocchi bot/firewall.'))

        if not page.title:
            findings.append(_finding(page, 'seo', 'Media', 'Title mancante', 'La pagina non espone un <title> leggibile.', 'Peggiora SEO e chiarezza del contenuto.', 'Aggiungere un title univoco e localizzato.'))
        else:
            title_key = (page.site_code, page.title.strip().lower())
            title_index[title_key] = title_index.get(title_key, 0) + 1

        if not page.h1:
            findings.append(_finding(page, 'seo', 'Media', 'H1 mancante', 'La pagina non espone un H1 leggibile.', 'Riduce chiarezza semantica e orientamento utente.', 'Aggiungere un H1 univoco e coerente con il topic della pagina.'))
        else:
            h1_key = (page.site_code, page.h1.strip().lower())
            h1_index[h1_key] = h1_index.get(h1_key, 0) + 1

        if re.search(r'\bundefined\b', page.text, re.IGNORECASE) or 'undefined' in page.final_url.lower():
            findings.append(_finding(page, 'technical', 'Alta', 'Placeholder tecnico visibile', 'La pagina mostra il token "undefined" nel testo o nell’URL.', 'Riduce fiducia e segnala un problema di rendering o data binding.', 'Correggere il template e aggiungere test automatici anti-placeholder.'))

        if page.language in NON_ENGLISH and re.search(r'where are you\?', page.text, re.IGNORECASE):
            findings.append(_finding(page, 'content', 'Media', 'Placeholder dealer non localizzato', 'È presente la stringa "Where are you?" su un ramo non inglese.', 'Incoerenza locale e possibile attrito nel dealer flow.', 'Localizzare il componente dealer e i suoi messaggi di input/errore.'))

        if re.search(r'<h[1-6][^>]*>\s*</h[1-6]>', page.html, re.IGNORECASE):
            findings.append(_finding(page, 'seo', 'Media', 'Heading vuoto nel markup', 'Nel markup è presente almeno un heading senza testo.', 'Semantica debole per SEO e accessibilità.', 'Pulire il template e rimuovere heading placeholder o vuoti.'))

        if page.canonical and page.final_url and urlparse(page.canonical).path != urlparse(page.final_url).path:
            findings.append(_finding(page, 'seo', 'Bassa', 'Canonical diverso dal final URL', f'Canonical: {page.canonical} / Final URL: {page.final_url}', 'Potrebbe essere corretto, ma va verificato per evitare segnali SEO incoerenti.', 'Controllare canonical, redirect e logica locale del ramo.'))

        old_years = re.findall(r'\b(20\d{2})\b', page.text)
        stale_years = sorted({int(y) for y in old_years if int(y) < CURRENT_YEAR})
        if stale_years and page.page_type == 'home':
            findings.append(_finding(page, 'content', 'Media', 'Anno passato visibile in homepage', f'In homepage compaiono anni passati: {", ".join(map(str, stale_years))}.', 'Possibile contenuto promo/evento non aggiornato.', 'Verificare se i riferimenti sono editoriali evergreen o contenuti scaduti da rimuovere.'))

        # simple cross-language leakage checks
        markers = FOREIGN_LANGUAGE_MARKERS.get(page.language, [])
        for marker in markers:
            if marker.lower() in page.text.lower():
                findings.append(_finding(page, 'content', 'Media', 'Possibile leakage di altra lingua', f'Rilevata stringa sospetta per il mercato: "{marker}".', 'Riduce coerenza linguistica e qualità percepita.', 'Revisionare le stringhe shared del template e i contenuti localizzati.'))
                break

        broken_pattern = [link for link in page.links if 'undefined' in link.lower()]
        if broken_pattern:
            findings.append(_finding(page, 'technical', 'Critica', 'URL malformato nei link interni', f'Trovati link con "undefined": {broken_pattern[:3]}', 'Rottura diretta del funnel o errore di generazione URL.', 'Correggere la generazione dei link e introdurre un link checker automatico.'))

        if page.errors:
            findings.append(_finding(page, 'technical', 'Bassa', 'Errori durante il crawling', '; '.join(page.errors), 'Possibile pagina fragile o difficile da renderizzare.', 'Verificare eventuali blocchi, timeout o dipendenze JS.'))

    for page in pages:
        if page.title and title_index.get((page.site_code, page.title.strip().lower()), 0) > 1:
            findings.append(_finding(page, 'seo', 'Bassa', 'Title duplicato nel mercato', f'Il title "{page.title}" compare più volte nello stesso ramo.', 'Può indebolire chiarezza SEO interna.', 'Differenziare i title delle pagine chiave.'))
        if page.h1 and h1_index.get((page.site_code, page.h1.strip().lower()), 0) > 1:
            findings.append(_finding(page, 'seo', 'Bassa', 'H1 duplicato nel mercato', f'L’H1 "{page.h1}" compare più volte nello stesso ramo.', 'Riduce precisione semantica tra pagine.', 'Rendere gli H1 più specifici per template.'))

        # duplicate CTA heuristic
        cta_count = len(re.findall(r'\b(discover more|scopri di più|mehr erfahren|mehr entdecken|ontdek meer|découvrir|descubre más)\b', page.text, re.IGNORECASE))
        if cta_count >= 4 and page.page_type == 'home':
            findings.append(_finding(page, 'ux', 'Media', 'CTA generiche molto ripetute', f'Rilevate {cta_count} CTA generiche simili nella pagina.', 'Information scent debole e minore chiarezza del funnel.', 'Rendere le CTA più specifiche per task e outcome.'))

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
