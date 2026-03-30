# Pirelli Weekly Audit MVP

Motore Python per eseguire un audit settimanale dei siti Pirelli Car e generare:

- crawl di homepage e pagine chiave
- controlli automatici UX / SEO / tecnica / localizzazione
- storico SQLite
- confronto con la run precedente
- export Excel
- summary Markdown

## Cosa fa

Il job:

1. legge i 19 mercati dal file `config/sites.yaml`
2. apre le pagine chiave con Playwright
3. estrae title, H1, canonical, meta description, link e testo
4. applica una prima batteria di regole automatiche
5. salva pagine e findings in SQLite
6. confronta la run corrente con la precedente
7. genera `outputs/*.xlsx` e `outputs/*.md`

## Regole incluse nel MVP

- pagina non accessibile / errore HTTP
- title mancante
- H1 mancante
- heading vuoto nel markup
- placeholder `undefined`
- placeholder dealer `Where are you?` nei mercati non EN
- canonical diverso dal final URL
- anni passati visibili in homepage
- leakage di altre lingue per mercato
- URL malformati con `undefined`
- errori di crawling
- CTA generiche molto ripetute
- title/H1 duplicati nel mercato

## Struttura

```text
pirelli_weekly_audit_mvp/
  config/sites.yaml
  audit_engine/
    config_loader.py
    crawler.py
    fingerprints.py
    models.py
    reporting.py
    rules.py
    storage.py
  outputs/
  .github/workflows/weekly_audit.yml
  main.py
  requirements.txt
```

## Setup locale

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
python main.py
```

I file generati finiranno nella cartella `outputs/`.

## Esecuzione settimanale con GitHub Actions

Il workflow incluso esegue il job ogni lunedì alle 06:00 UTC e salva gli artefatti della run.

Per abilitarlo:

1. crea un repository GitHub
2. carica questi file
3. abilita GitHub Actions
4. opzionale: modifica il cron in `.github/workflows/weekly_audit.yml`

## Estensioni consigliate

- screenshot automatici per finding
- check broken link veri via HEAD/GET
- deduplica più avanzata dei fingerprint
- summary AI via API
- invio mail o Slack
- dashboard web sopra SQLite/Postgres/Supabase

## Note

- il motore è pronto da usare come MVP
- alcune URL chiave potrebbero cambiare nel tempo: aggiorna `config/sites.yaml` quando serve
- alcuni mercati possono bloccare il fetch automatico: il sistema registrerà l’errore nel report
