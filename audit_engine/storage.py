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
    discovered_from TEXT DEFAULT '',
    errors TEXT,
    template_type TEXT DEFAULT 'generic',
    journey TEXT DEFAULT 'generic',
    coverage_confidence TEXT DEFAULT 'Media',
    evidence_type TEXT DEFAULT 'dom',
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
    description TEXT NOT NULL,
    impact TEXT NOT NULL,
    suggested_fix TEXT NOT NULL,
    evidence_tecnica TEXT DEFAULT '',
    confidence TEXT DEFAULT 'Media',
    discovered_from TEXT DEFAULT '',
    crawl_depth INTEGER DEFAULT 0,
    fitment_tipo TEXT DEFAULT '',
    fitment_step TEXT DEFAULT '',
    template_type TEXT DEFAULT 'generic',
    journey TEXT DEFAULT 'generic',
    evidence_type TEXT DEFAULT 'dom',
    coverage_confidence TEXT DEFAULT 'Media',
    observed TEXT DEFAULT '',
    expected TEXT DEFAULT '',
    business_impact TEXT DEFAULT '',
    repro_steps TEXT DEFAULT '',
    fingerprint TEXT NOT NULL,
    UNIQUE(run_id, fingerprint),
    FOREIGN KEY(run_id) REFERENCES runs(id)
);
"""


class Storage:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute('PRAGMA foreign_keys = ON')
        self.conn.executescript(SCHEMA)
        self._migrate_schema()
        self.conn.commit()

    def _migrate_schema(self) -> None:
        self._ensure_column('pages', 'crawl_depth', 'INTEGER DEFAULT 0')
        self._ensure_column('pages', 'discovered_from', "TEXT DEFAULT ''")
        self._ensure_column('pages', 'template_type', "TEXT DEFAULT 'generic'")
        self._ensure_column('pages', 'journey', "TEXT DEFAULT 'generic'")
        self._ensure_column('pages', 'coverage_confidence', "TEXT DEFAULT 'Media'")
        self._ensure_column('pages', 'evidence_type', "TEXT DEFAULT 'dom'")
        self._ensure_column('findings', 'evidence_tecnica', "TEXT DEFAULT ''")
        self._ensure_column('findings', 'confidence', "TEXT DEFAULT 'Media'")
        self._ensure_column('findings', 'discovered_from', "TEXT DEFAULT ''")
        self._ensure_column('findings', 'crawl_depth', 'INTEGER DEFAULT 0')
        self._ensure_column('findings', 'fitment_tipo', "TEXT DEFAULT ''")
        self._ensure_column('findings', 'fitment_step', "TEXT DEFAULT ''")
        self._ensure_column('findings', 'template_type', "TEXT DEFAULT 'generic'")
        self._ensure_column('findings', 'journey', "TEXT DEFAULT 'generic'")
        self._ensure_column('findings', 'evidence_type', "TEXT DEFAULT 'dom'")
        self._ensure_column('findings', 'coverage_confidence', "TEXT DEFAULT 'Media'")
        self._ensure_column('findings', 'observed', "TEXT DEFAULT ''")
        self._ensure_column('findings', 'expected', "TEXT DEFAULT ''")
        self._ensure_column('findings', 'business_impact', "TEXT DEFAULT ''")
        self._ensure_column('findings', 'repro_steps', "TEXT DEFAULT ''")

    def _ensure_column(self, table: str, column: str, ddl: str) -> None:
        cur = self.conn.cursor()
        cur.execute(f'PRAGMA table_info({table})')
        existing = {row[1] for row in cur.fetchall()}
        if column not in existing:
            cur.execute(f'ALTER TABLE {table} ADD COLUMN {column} {ddl}')

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
            '''INSERT INTO pages(run_id, site_code, country, region, language, url, page_type, final_url, status_code, title, h1, h2_count, canonical, meta_description, crawl_depth, discovered_from, errors, template_type, journey, coverage_confidence, evidence_type)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            [(
                run_id,
                p.site_code,
                p.country,
                p.region,
                p.language,
                p.url,
                p.page_type,
                p.final_url,
                p.status_code,
                p.title,
                p.h1,
                p.h2_count,
                p.canonical,
                p.meta_description,
                p.crawl_depth,
                p.discovered_from,
                ' | '.join(p.errors),
                getattr(p, 'template_type', 'generic'),
                getattr(p, 'journey', 'generic'),
                getattr(p, 'coverage_confidence', 'Media'),
                getattr(p, 'evidence_type', 'dom'),
            ) for p in pages]
        )
        self.conn.commit()

    def save_findings(self, run_id: int, findings: Iterable[Finding]) -> None:
        self.conn.executemany(
            '''INSERT OR IGNORE INTO findings(run_id, site_code, country, region, url, page_type, category, severity, title, description, impact, suggested_fix, evidence_tecnica, confidence, discovered_from, crawl_depth, fitment_tipo, fitment_step, template_type, journey, evidence_type, coverage_confidence, observed, expected, business_impact, repro_steps, fingerprint)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            [(
                run_id,
                f.site_code,
                f.country,
                f.region,
                f.url,
                f.page_type,
                f.category,
                f.severity,
                f.title,
                f.description,
                f.impact,
                f.suggested_fix,
                f.evidence_tecnica,
                f.confidence,
                f.discovered_from,
                f.crawl_depth,
                f.fitment_tipo,
                f.fitment_step,
                getattr(f, 'template_type', 'generic'),
                getattr(f, 'journey', 'generic'),
                getattr(f, 'evidence_type', 'dom'),
                getattr(f, 'coverage_confidence', 'Media'),
                getattr(f, 'observed', ''),
                getattr(f, 'expected', ''),
                getattr(f, 'business_impact', ''),
                getattr(f, 'repro_steps', ''),
                f.fingerprint,
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
