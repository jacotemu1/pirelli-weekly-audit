# Pirelli Weekly Audit MVP V3

Questa versione corregge i due limiti principali emersi nelle prime run:

## 1. Crawl reale dalla homepage
- parte dalle homepage reali dei mercati
- segue i link interni realmente presenti nel ramo/locale
- non inventa più URL dealer o catalogue per deduzione
- salva `crawl_depth` e `discovered_from` per capire da dove arriva ogni pagina
- supporta mercati multi-prefix come Switzerland e UAE/GCC tramite `allowed_prefixes`

## 2. Findings più leggibili per business
Nel foglio `Findings` trovi colonne orientate all’uso pratico:
- `titolo_bug`
- `spiegazione_bug_it`
- `impatto_utenti_business`
- `fix_consigliato_it`
- `evidenza_tecnica`
- `confidence`
- `pagina_trovata_da`
- `crawl_depth`

## 3. Miglioramento del diff settimanale
Il database storico ora usa `audit_history_v3.db` ed è pensato per essere persistito tra run.
Per avere un vero `new / resolved / persistent` in GitHub Actions è consigliato aggiornare anche il workflow per conservare `outputs/audit_history_v3.db` tra le esecuzioni.

## Output
- `Summary` → KPI run + riepilogo per market
- `Pages` → tutte le pagine visitate con profondità e pagina sorgente
- `Findings` → issue spiegate in italiano e con contesto tecnico
- `Coverage` → panoramica di copertura del crawl
- `Weekly Diff` → new / resolved / persistent

## Nota
Il crawler non è infinito: segue tutti i link interni raggiungibili dalle homepage entro limiti di sicurezza.
Le soglie di default sono configurabili via env:
- `PIRELLI_MAX_PAGES_PER_SITE` (default 140)
- `PIRELLI_MAX_CRAWL_DEPTH` (default 4)
