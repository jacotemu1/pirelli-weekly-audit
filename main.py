from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path

from audit_engine.config_loader import load_sites
from audit_engine.crawler import crawl_sites
from audit_engine.reporting import build_excel, build_markdown_summary
from audit_engine.rules import run_rules
from audit_engine.storage import Storage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Pirelli weekly audit MVP V3')
    parser.add_argument('--config', default='config/sites.yaml')
    parser.add_argument('--db', default='outputs/audit_history_v3.db')
    parser.add_argument('--output-dir', default='outputs')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sites = load_sites(args.config)
    storage = Storage(args.db)
    started = datetime.now(timezone.utc)
    run_date = started.date().isoformat()
    run_id = storage.create_run(run_date=run_date, started_at=started.isoformat())

    try:
        pages = asyncio.run(crawl_sites(sites))
        findings = run_rules(pages)
        storage.save_pages(run_id, pages)
        storage.save_findings(run_id, findings)
        diff = storage.diff_findings(run_id)

        stamp = started.strftime('%Y%m%d_%H%M%S')
        excel_path = output_dir / f'pirelli_weekly_audit_{stamp}.xlsx'
        md_path = output_dir / f'pirelli_weekly_summary_{stamp}.md'
        build_excel(excel_path, pages, findings, diff, run_date)
        build_markdown_summary(md_path, pages, findings, diff, run_date)

        finished = datetime.now(timezone.utc)
        storage.finish_run(
            run_id,
            finished_at=finished.isoformat(),
            status='completed',
            notes=f'Excel: {excel_path.name}; Summary: {md_path.name}',
        )

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
