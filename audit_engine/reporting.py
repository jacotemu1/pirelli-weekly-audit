from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from .models import Finding, PageResult

SEVERITY_RANK = {'Critica': 4, 'Alta': 3, 'Media': 2, 'Bassa': 1}


def _auto_width(ws) -> None:
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                max_len = max(max_len, len(str(cell.value or '')))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max_len + 2, 40)


def build_excel(output_path: str | Path, pages: list[PageResult], findings: list[Finding], diff: dict[str, set[str]], run_date: str) -> None:
    wb = Workbook()
    wb.remove(wb.active)

    pages_df = pd.DataFrame([
        {
            'site_code': p.site_code,
            'country': p.country,
            'region': p.region,
            'language': p.language,
            'page_type': p.page_type,
            'url': p.url,
            'final_url': p.final_url,
            'status_code': p.status_code,
            'title': p.title,
            'h1': p.h1,
            'h2_count': p.h2_count,
            'canonical': p.canonical,
            'meta_description': p.meta_description,
            'errors': ' | '.join(p.errors),
        }
        for p in pages
    ])
    findings_df = pd.DataFrame([
        {
            'site_code': f.site_code,
            'country': f.country,
            'region': f.region,
            'page_type': f.page_type,
            'url': f.url,
            'category': f.category,
            'severity': f.severity,
            'title': f.title,
            'description': f.description,
            'impact': f.impact,
            'suggested_fix': f.suggested_fix,
            'fingerprint': f.fingerprint,
            'diff_status': 'new' if f.fingerprint in diff['new'] else 'persistent' if f.fingerprint in diff['persistent'] else '',
        }
        for f in findings
    ])

    summary_rows = []
    grouped = findings_df.groupby(['site_code', 'country'], dropna=False) if not findings_df.empty else []
    counts_by_site = Counter()
    high_by_site = Counter()
    crit_by_site = Counter()
    for f in findings:
        counts_by_site[f.site_code] += 1
        if f.severity == 'Alta':
            high_by_site[f.site_code] += 1
        if f.severity == 'Critica':
            crit_by_site[f.site_code] += 1
    site_pages = Counter(p.site_code for p in pages)
    site_regions = {p.site_code: p.region for p in pages}
    site_countries = {p.site_code: p.country for p in pages}
    for site_code in sorted(site_pages):
        total = counts_by_site[site_code]
        score = max(0, 100 - (crit_by_site[site_code] * 25 + high_by_site[site_code] * 12 + max(total - crit_by_site[site_code] - high_by_site[site_code], 0) * 4))
        summary_rows.append({
            'site_code': site_code,
            'country': site_countries.get(site_code, ''),
            'region': site_regions.get(site_code, ''),
            'pages_checked': site_pages[site_code],
            'findings_total': total,
            'critical': crit_by_site[site_code],
            'high': high_by_site[site_code],
            'quality_score_estimate': score,
        })
    summary_df = pd.DataFrame(summary_rows).sort_values(['quality_score_estimate', 'findings_total'], ascending=[False, True]) if summary_rows else pd.DataFrame()

    meta_ws = wb.create_sheet('Summary')
    meta_ws['A1'] = 'Pirelli Weekly Audit'
    meta_ws['A1'].font = Font(bold=True, size=14)
    meta_ws['A2'] = 'Run date'
    meta_ws['B2'] = run_date
    meta_ws['A3'] = 'Pages checked'
    meta_ws['B3'] = len(pages)
    meta_ws['A4'] = 'Findings total'
    meta_ws['B4'] = len(findings)
    meta_ws['A5'] = 'New findings vs previous run'
    meta_ws['B5'] = len(diff['new'])
    meta_ws['A6'] = 'Resolved findings vs previous run'
    meta_ws['B6'] = len(diff['resolved'])
    meta_ws['A8'] = 'Per-site summary'
    meta_ws['A8'].font = Font(bold=True)
    if not summary_df.empty:
        for row_idx, row in enumerate([summary_df.columns.tolist()] + summary_df.values.tolist(), start=9):
            for col_idx, value in enumerate(row, start=1):
                cell = meta_ws.cell(row=row_idx, column=col_idx, value=value)
                if row_idx == 9:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill('solid', fgColor='D9EAD3')
    _auto_width(meta_ws)

    for name, df in [('Pages', pages_df), ('Findings', findings_df)]:
        ws = wb.create_sheet(name)
        if df.empty:
            ws['A1'] = 'No data'
        else:
            for row_idx, row in enumerate([df.columns.tolist()] + df.values.tolist(), start=1):
                for col_idx, value in enumerate(row, start=1):
                    cell = ws.cell(row=row_idx, column=col_idx, value=value)
                    if row_idx == 1:
                        cell.font = Font(bold=True)
                        cell.fill = PatternFill('solid', fgColor='D9EAD3')
                    cell.alignment = Alignment(vertical='top', wrap_text=True)
        _auto_width(ws)

    diff_ws = wb.create_sheet('Weekly Diff')
    diff_ws.append(['type', 'count'])
    diff_ws.append(['new', len(diff['new'])])
    diff_ws.append(['resolved', len(diff['resolved'])])
    diff_ws.append(['persistent', len(diff['persistent'])])
    for cell in diff_ws[1]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill('solid', fgColor='D9EAD3')
    _auto_width(diff_ws)

    wb.save(output_path)


def build_markdown_summary(output_path: str | Path, pages: list[PageResult], findings: list[Finding], diff: dict[str, set[str]], run_date: str) -> None:
    by_site: dict[str, list[Finding]] = defaultdict(list)
    country_map = {}
    for f in findings:
        by_site[f.site_code].append(f)
        country_map[f.site_code] = f.country
    for p in pages:
        country_map[p.site_code] = p.country

    lines = [
        '# Pirelli Weekly Audit Summary',
        '',
        f'- Run date: {run_date}',
        f'- Pages checked: {len(pages)}',
        f'- Findings total: {len(findings)}',
        f'- New findings: {len(diff["new"])}',
        f'- Resolved findings: {len(diff["resolved"])}',
        '',
        '## Highlights',
        '',
    ]
    if findings:
        severity_sorted = sorted(findings, key=lambda f: (-SEVERITY_RANK.get(f.severity, 0), f.country, f.title))[:10]
        for f in severity_sorted:
            lines.append(f'- **{f.country}** — {f.severity} — {f.title}: {f.description}')
    else:
        lines.append('- Nessuna issue rilevata.')

    lines.extend(['', '## Per market', ''])
    for site_code in sorted(country_map):
        country = country_map[site_code]
        site_findings = by_site.get(site_code, [])
        counter = Counter(f.severity for f in site_findings)
        lines.append(f'### {country} ({site_code})')
        lines.append(f'- Findings: {len(site_findings)}')
        lines.append(f'- Critiche: {counter.get("Critica", 0)} | Alte: {counter.get("Alta", 0)} | Medie: {counter.get("Media", 0)} | Basse: {counter.get("Bassa", 0)}')
        for f in sorted(site_findings, key=lambda x: -SEVERITY_RANK.get(x.severity, 0))[:5]:
            lines.append(f'  - {f.severity} — {f.title}: {f.description}')
        lines.append('')

    Path(output_path).write_text('\n'.join(lines), encoding='utf-8')
