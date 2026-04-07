## Cosa devi fare tu (checklist veloce)

1. **Carica questi file nel tuo repo GitHub** (sovrascrivendo quelli esistenti):
   - `main.py`
   - `requirements.txt`
   - `README.md`
   - cartella `audit_engine/`
   - cartella `config/`
   - `.github/workflows/weekly_audit.yml`
   - `WEEKLY_WORKFLOW_TO_PASTE.txt`

2. **Controlla il workflow**:
   - deve esistere `.github/workflows/weekly_audit.yml`;
   - se vuoi fare update manuale, usa `WEEKLY_WORKFLOW_TO_PASTE.txt`.

3. **Fai commit e push** su GitHub.

4. **Avvia il job**:
   - GitHub → **Actions** → **Pirelli Weekly Audit** → **Run workflow**.

5. **Quando termina, scarica l’Excel** dagli artifact e verifica almeno questi fogli:
   - `00_Sintesi`
   - `01_Priorita`
   - `02_Diff_settimanale`
   - `10_Bug_Tutti`
   - `17_Bug_Fitment`
   - `90_Pagine_Crawlate`
   - `91_Coverage`
   - `Build Info` (controlla il `build_version`).

---

## Se qualcosa non gira

- Se il workflow fallisce subito, controlla prima `requirements.txt` e il log step `Install dependencies`.
- Se falliscono i test fitment, controlla `config/fitment_test_cases.yaml`.
- Se non trovi nuovi bug nel report, controlla in `Build Info` che data/build siano aggiornati e che il crawl abbia effettivamente visitato pagine (`90_Pagine_Crawlate`).
