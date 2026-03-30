from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from .models import Finding, PageResult

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    site_code TEXT NOT NULL,
    country TEXT NOT NULL,
    region TEXT NOT NULL,
    language TEXT NOT NULL,
    url TEXT NOT NULL,
    page_type TEXT NOT NULL,
    final_url TEXT,
    status_code INTEGER,
    title TEXT,
    h1 TEXT,
    h2_count INTEGER,
    canonical TEXT,
    meta_description TEXT,
    crawl_depth INTEGER DEFAULT 0,
    discovered_from TEXT,
    errors TEXT,
    FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS findings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    site_code TEXT NOT NULL,
    country TEXT NOT NULL,
    region TEXT NOT NULL,
    url TEXT NOT NULL,
    page_type TEXT NOT NULL,
    category TEXT NOT NULL,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    explanation_it TEXT NOT NULL,
    impact_it TEXT NOT NULL,
    suggested_fix_it TEXT NOT NULL,
    evidence_tecnica TEXT NOT NULL,
    confidence TEXT NOT NULL,
    crawl_depth INTEGER DEFAULT 0,
    discovered_from TEXT,
    fingerprint TEXT NOT NULL,
    UNIQUE(run_id, fingerprint),
    FOREIGN KEY(run_id) REFERENCES runs(id)
);
"""

PAGE_COLUMNS = {
    'crawl_depth': 'ALTER TABLE pages ADD COLUMN crawl_depth INTEGER DEFAULT 0',
    'discovered_from': 'ALTER TABLE pages ADD COLUMN discovered_from TEXT',
}
FINDING_COLUMNS = {
    'explanation_it': 'ALTER TABLE findings ADD COLUMN explanation_it TEXT DEFAULT ""',
    'impact_it': 'ALTER TABLE findings ADD COLUMN impact_it TEXT DEFAULT ""',
    'suggested_fix_it': 'ALTER TABLE findings ADD COLUMN suggested_fix_it TEXT DEFAULT ""',
    'evidence_tecnica': 'ALTER TABLE findings ADD COLUMN evidence_tecnica TEXT DEFAULT ""',
    'confidence': 'ALTER TABLE findings ADD COLUMN confidence TEXT DEFAULT "Media"',
    'crawl_depth': 'ALTER TABLE findings ADD COLUMN crawl_depth INTEGER DEFAULT 0',
    'discovered_from': 'ALTER TABLE findings ADD COLUMN discovered_from TEXT',
}


class Storage:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute('PRAGMA foreign_keys = ON')
        self.conn.executescript(SCHEMA)
        self._migrate_if_needed()
        self.conn.commit()

    def _column_names(self, table: str) -> set[str]:
        cur = self.conn.execute(f'PRAGMA table_info({table})')
        return {row[1] for row in cur.fetchall()}

    def _migrate_if_needed(self) -> None:
        page_cols = self._column_names('pages')
        for col, ddl in PAGE_COLUMNS.items():
            if col not in page_cols:
                self.conn.execute(ddl)
        finding_cols = self._column_names('findings')
        for col, ddl in FINDING_COLUMNS.items():
            if col not in finding_cols:
                self.conn.execute(ddl)

    def create_run(self, run_date: str, started_at: str) -> int:
        cur = self.conn.cursor()
        cur.execute('INSERT INTO runs(run_date, started_at, status) VALUES (?, ?, ?)', (run_date, started_at, 'running'))
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, finished_at: str, status: str = 'completed', notes: str = '') -> None:
        self.conn.execute('UPDATE runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?', (finished_at, status, notes, run_id))
        self.conn.commit()

    def save_pages(self, run_id: int, pages: Iterable[PageResult]) -> None:
        self.conn.executemany(
            '''INSERT INTO pages(run_id, site_code, country, region, language, url, page_type, final_url, status_code, title, h1, h2_count, canonical, meta_description, crawl_depth, discovered_from, errors)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            [(
                run_id, p.site_code, p.country, p.region, p.language, p.url, p.page_type, p.final_url, p.status_code,
                p.title, p.h1, p.h2_count, p.canonical, p.meta_description, p.crawl_depth, p.discovered_from, ' | '.join(p.errors)
            ) for p in pages]
        )
        self.conn.commit()

    def save_findings(self, run_id: int, findings: Iterable[Finding]) -> None:
        self.conn.executemany(
            '''INSERT OR IGNORE INTO findings(run_id, site_code, country, region, url, page_type, category, severity, title, explanation_it, impact_it, suggested_fix_it, evidence_tecnica, confidence, crawl_depth, discovered_from, fingerprint)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            [(
                run_id, f.site_code, f.country, f.region, f.url, f.page_type, f.category, f.severity,
                f.title, f.explanation_it, f.impact_it, f.suggested_fix_it, f.evidence_tecnica, f.confidence, f.crawl_depth, f.discovered_from, f.fingerprint
            ) for f in findings]
        )
        self.conn.commit()

    def previous_run_id(self, current_run_id: int) -> int | None:
        cur = self.conn.cursor()
        cur.execute('SELECT id FROM runs WHERE id < ? AND status = ? ORDER BY id DESC LIMIT 1', (current_run_id, 'completed'))
        row = cur.fetchone()
        return int(row[0]) if row else None

    def diff_findings(self, current_run_id: int) -> dict[str, set[str]]:
        prev_run = self.previous_run_id(current_run_id)
        cur = self.conn.cursor()
        cur.execute('SELECT fingerprint FROM findings WHERE run_id = ?', (current_run_id,))
        current = {row[0] for row in cur.fetchall()}
        if prev_run is None:
            return {'new': current, 'resolved': set(), 'persistent': set()}
        cur.execute('SELECT fingerprint FROM findings WHERE run_id = ?', (prev_run,))
        previous = {row[0] for row in cur.fetchall()}
        return {
            'new': current - previous,
            'resolved': previous - current,
            'persistent': current & previous,
        }
