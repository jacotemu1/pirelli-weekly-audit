1) Estrai lo zip.
2) Apri la cartella `pirelli_weekly_audit_mvp_v3_fixed`.
3) Carica e sovrascrivi su GitHub SOLO:
   - main.py
   - requirements.txt
   - README.md
   - audit_engine/
   - config/
4) Fai commit.
5) Apri `.github/workflows/weekly_audit.yml` sul repo. Se vuoi, sostituisci il contenuto con quello presente nel file `WEEKLY_WORKFLOW_TO_PASTE.txt`.
6) Vai su Actions > Pirelli Weekly Audit > Run workflow.
7) Nel nuovo Excel verifica che esistano il foglio `Build Info` e `Summary!A1 = Pirelli Weekly Audit V3_FIXED_20260331`.
