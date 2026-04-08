from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path

from audit_engine.config_loader import load_fitment_cases, load_sites
from audit_engine.crawler import crawl_sites, run_quality_audit
from audit_engine.fitment import run_fitment_checks
from audit_engine.reporting import build_excel, build_markdown_summary
from audit_engine.rules import run_rules
from audit_engine.storage import Storage


BUILD_VERSION = 'V5_20260331'
FITMENT_TOTAL_BUDGET_SEC = int(os.getenv('PIRELLI_FITMENT_TOTAL_BUDGET_SEC', '1200'))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Pirelli weekly audit MVP V5')
    parser.add_argument('--config', default='config/sites.yaml')
    parser.add_argument('--fitment-config', default='config/fitment_test_cases.yaml')
    parser.add_argument('--db', default='outputs/audit_history_v4.db')
    parser.add_argument('--output-dir', default='outputs')
    parser.add_argument('--skip-fitment', action='store_true')
    return parser.parse_args()



def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f'[RUN] start markets_config={args.config} fitment_enabled={not args.skip_fitment}', flush=True)
    print('[RUN] loading sites config', flush=True)
    sites = load_sites(args.config)
    print(f'[RUN] sites loaded count={len(sites)}', flush=True)
    print('[RUN] loading fitment config', flush=True)
    fitment_cases = {} if args.skip_fitment else load_fitment_cases(args.fitment_config)
    print(f'[RUN] fitment config loaded count={len(fitment_cases)}', flush=True)
    storage = Storage(args.db)
    started = datetime.now(timezone.utc)
    run_date = started.date().isoformat()
    run_id = storage.create_run(run_date=run_date, started_at=started.isoformat())

    try:
        print('[RUN] crawl start', flush=True)
        pages = asyncio.run(crawl_sites(sites))
        crawled_markets = len({p.site_code for p in pages})
        print(f'[RUN] crawl done markets={crawled_markets} pages={len(pages)}', flush=True)
        print('[TEMPLATE] selection start', flush=True)
        template_summary = {}
        for p in pages:
            key = f"{p.site_code}:{getattr(p, 'template_type', 'generic')}"
            template_summary[key] = template_summary.get(key, 0) + 1
        print(f'[TEMPLATE] selection done templates={len(template_summary)}', flush=True)

        findings = run_rules(pages)
        audit_findings = run_quality_audit(pages)
        findings.extend(audit_findings)
        print(f'[AUDIT] rules done base_findings={len(findings)-len(audit_findings)} quality_findings={len(audit_findings)} total={len(findings)}', flush=True)
        if fitment_cases:
            print(f'[RUN] fitment start markets_with_cases={len(fitment_cases)}', flush=True)
            try:
                fitment_findings = asyncio.run(
                    asyncio.wait_for(
                        run_fitment_checks(sites, fitment_cases),
                        timeout=FITMENT_TOTAL_BUDGET_SEC,
                    )
                )
                findings.extend(fitment_findings)
                print(f'[RUN] fitment done findings={len(fitment_findings)}', flush=True)
            except asyncio.TimeoutError:
                print(f'[RUN] fitment total budget exceeded ({FITMENT_TOTAL_BUDGET_SEC}s), continuing without blocking run', flush=True)
            except Exception as fitment_exc:  # noqa: BLE001
                print(f'[RUN] fitment failed-soft: {fitment_exc}', flush=True)
        else:
            print('[RUN] fitment skipped: no cases loaded', flush=True)

        screenshot_by_url = {p.final_url or p.url: getattr(p, 'screenshot_path', '') for p in pages}
        screenshot_by_url.update({p.url: getattr(p, 'screenshot_path', '') for p in pages})
        for f in findings:
            if not getattr(f, 'screenshot_path', ''):
                f.screenshot_path = screenshot_by_url.get(f.url, '')
            if isinstance(f.data, dict) and f.screenshot_path and not f.data.get('screenshot_path'):
                f.data['screenshot_path'] = f.screenshot_path

        storage.save_pages(run_id, pages)
        storage.save_findings(run_id, findings)
        diff = storage.diff_findings(run_id)

        stamp = started.strftime('%Y%m%d_%H%M%S')
        excel_path = output_dir / f'pirelli_weekly_audit_{stamp}.xlsx'
        md_path = output_dir / f'pirelli_weekly_summary_{stamp}.md'
        print('[RUN] report generation start', flush=True)
        build_excel(excel_path, pages, findings, diff, run_date)
        build_markdown_summary(md_path, pages, findings, diff, run_date)
        print('[RUN] report generation done', flush=True)

        finished = datetime.now(timezone.utc)
        storage.finish_run(
            run_id,
            finished_at=finished.isoformat(),
            status='completed',
            notes=f'Build {BUILD_VERSION}; Excel: {excel_path.name}; Summary: {md_path.name}',
        )

        print(f'Build: {BUILD_VERSION}')
        print(f'Run completed: {run_id}')
        print(f'Pages checked: {len(pages)}')
        print(f'Findings: {len(findings)}')
        print(f'Excel: {excel_path}')
        print(f'Summary: {md_path}')
        return 0
    except Exception as exc:  # noqa: BLE001
        finished = datetime.now(timezone.utc)
        storage.finish_run(run_id, finished_at=finished.isoformat(), status='failed', notes=str(exc))
        raise


if __name__ == '__main__':
    raise SystemExit(main())
