# Pirelli Weekly Audit MVP V2

Questa versione migliora il prototipo iniziale in due punti chiave:

1. **Crawl esteso dal seed homepage**
   - parte dalle homepage/config pages del mercato
   - segue automaticamente i link interni dello stesso ramo/locale
   - salva profondità di crawl e pagina di origine (`discovered_from`)
   - usa limiti di sicurezza configurati nel codice (`MAX_PAGES_PER_SITE`, `MAX_CRAWL_DEPTH`)

2. **Excel più leggibile in italiano**
   - colonna `titolo_bug`
   - colonna `spiegazione_bug_it`
   - colonna `impatto_utenti_business`
   - colonna `fix_consigliato_it`
   - meno duplicazione tra descrizione, impatto e fix

## Output

- `Summary` → overview run e score per market
- `Pages` → tutte le pagine visitate, con `crawl_depth` e `discovered_from`
- `Findings` → issue spiegate in italiano
- `Weekly Diff` → new / resolved / persistent

## Limiti attuali

- Il crawler segue **tutte le pagine interne raggiungibili dalla homepage fino ai limiti di sicurezza**, non un crawl infinito dell'intero dominio.
- Alcuni mercati possono bloccare il fetch automatico o servire contenuti diversi a un browser headless.
- Le regole sono ancora template/rule-based, non un vero giudizio UX umano.
