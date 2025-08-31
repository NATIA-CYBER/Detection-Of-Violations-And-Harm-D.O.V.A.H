# src/ingest/kev_fetch.py
from __future__ import annotations
import csv
import io
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

KEV_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"

@dataclass
class KEVConfig:
    db_url: Optional[str] = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    out_csv: Path = Path("data/security/kev.csv")
    create_table: bool = True
    load_db: bool = False  # default OFF; enable with --db

class KEVFetcher:
    def __init__(self, cfg: KEVConfig):
        self.cfg = cfg
        self.engine: Optional[Engine] = None
        if self.cfg.db_url and self.cfg.load_db:
            try:
                self.engine = create_engine(self.cfg.db_url)
            except Exception as e:
                print(f"[kev] WARN: could not init DB engine ({e}); continuing without DB")
                self.engine = None

    def _ensure_dirs(self) -> None:
        self.cfg.out_csv.parent.mkdir(parents=True, exist_ok=True)

    def fetch_to_csv(self) -> Path:
        """Download KEV JSON and write a denormalized CSV."""
        self._ensure_dirs()
        print(f"[kev] downloading: {KEV_URL}")
        r = requests.get(KEV_URL, timeout=60)
        r.raise_for_status()
        data = r.json()
        vulns = data.get("vulnerabilities", [])
        
        # Use a string buffer to write CSV
        s_buf = io.StringIO()
        fieldnames = ["cveID", "vendorProject", "product", "vulnerabilityName", "dateAdded", "shortDescription", "requiredAction", "dueDate", "notes"]
        writer = csv.DictWriter(s_buf, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(vulns)
        
        # Write to final file
        with self.cfg.out_csv.open("w", encoding="utf-8", newline="") as out:
            out.write(s_buf.getvalue())
        
        print(f"[kev] wrote CSV → {self.cfg.out_csv} ({len(vulns)} rows)")
        return self.cfg.out_csv

    def _create_table_if_needed(self) -> None:
        if not self.engine or not self.cfg.create_table:
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS kev (
            cve_id TEXT PRIMARY KEY,
            vendor_project TEXT,
            product TEXT,
            vulnerability_name TEXT,
            date_added DATE,
            description TEXT,
            action TEXT,
            due_date DATE,
            notes TEXT
        );
        """
        with self.engine.begin() as cx:
            cx.exec_driver_sql(ddl)

    def _iter_rows(self, csv_path: Path) -> Iterable[dict]:
        with csv_path.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                yield row

    def load_csv_to_db(self, csv_path: Path) -> int:
        if not self.engine:
            print("[kev] DB load skipped (no engine)")
            return 0
        self._create_table_if_needed()
        sql = text("""
            INSERT INTO kev (cve_id, vendor_project, product, vulnerability_name, date_added, description, action, due_date, notes)
            VALUES (:cveID, :vendorProject, :product, :vulnerabilityName, :dateAdded, :shortDescription, :requiredAction, :dueDate, :notes)
            ON CONFLICT (cve_id) DO UPDATE SET
                vendor_project=EXCLUDED.vendor_project, product=EXCLUDED.product, vulnerability_name=EXCLUDED.vulnerability_name,
                date_added=EXCLUDED.date_added, description=EXCLUDED.description, action=EXCLUDED.action,
                due_date=EXCLUDED.due_date, notes=EXCLUDED.notes
        """)
        n = 0
        with self.engine.begin() as cx:
            for row in self._iter_rows(csv_path):
                cx.execute(sql, row)
                n += 1
        print(f"[kev] upserted {n} rows into kev")
        return n

def parse_args(argv) -> KEVConfig:
    import argparse
    ap = argparse.ArgumentParser(description="Fetch KEV JSON, save as CSV (and optionally load to Postgres).")
    ap.add_argument("--out", default="data/security/kev.csv", help="Where to write CSV")
    ap.add_argument("--db", action="store_true", help="Also load into Postgres (uses DATABASE_URL)")
    ap.add_argument("--no-create", action="store_true", help="Do not create table automatically")
    ap.add_argument("--db-url", default=None, help="Override DATABASE_URL/POSTGRES_URL")
    args = ap.parse_args(argv)
    return KEVConfig(
        db_url=args.db_url or os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL"),
        out_csv=Path(args.out),
        create_table=not args.no_create,
        load_db=bool(args.db),
    )

def main() -> int:
    cfg = parse_args(sys.argv[1:])
    fetcher = KEVFetcher(cfg)
    csv_path = fetcher.fetch_to_csv()
    if cfg.load_db:
        fetcher.load_csv_to_db(csv_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
