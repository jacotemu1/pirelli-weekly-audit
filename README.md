1. Estrai questo zip.
2. Nel repo GitHub carica e sovrascrivi:
   - main.py
   - requirements.txt
   - README.md
   - cartella audit_engine
   - cartella config
   - WEEKLY_WORKFLOW_TO_PASTE.txt
   - COME_AGGIORNARE_GITHUB.txt
3. Verifica che esista `.github/workflows/weekly_audit.yml` (già incluso in questo pacchetto).
4. In alternativa, se vuoi aggiornare il workflow manualmente, apri `.github/workflows/weekly_audit.yml` e incolla il contenuto di `.github/workflows/WEEKLY_WORKFLOW_TO_PASTE.txt`.
5. Commit changes
6. Vai su Actions > Pirelli Weekly Audit > Run workflow
7. Nel nuovo Excel controlla:
   - fogli 00_Sintesi, 01_Priorita, 10_Bug_Tutti, 11_Bug_Codice_Tecnica, 12_Bug_SEO, 13_Bug_UX_UI, 14_Bug_Contenuti_Localizzazione, 15_Bug_Accessibilita, 16_Bug_CRO
   - Build Info con build_version V4_20260331
