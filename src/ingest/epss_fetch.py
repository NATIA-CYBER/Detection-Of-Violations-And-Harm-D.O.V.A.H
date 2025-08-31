# src/ingest/epss_fetch.py
from __future__ import annotations
import csv
import gzip
import io
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

EPSS_GZ_URL = "https://epss.cyentia.com/epss_scores-current.csv.gz"

@dataclass
class EPSSConfig:
    db_url: Optional[str] = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    out_csv: Path = Path("data/security/epss_scores.csv")
    create_table: bool = True
    load_db: bool = False  # default OFF; enable with --db

class EPSSFetcher:
    def __init__(self, cfg: EPSSConfig):
        self.cfg = cfg
        self.engine: Optional[Engine] = None
        if self.cfg.db_url and self.cfg.load_db:
            try:
                self.engine = create_engine(self.cfg.db_url)
            except Exception as e:
                print(f"[epss] WARN: could not init DB engine ({e}); continuing without DB")
                self.engine = None

    def _ensure_dirs(self) -> None:
        self.cfg.out_csv.parent.mkdir(parents=True, exist_ok=True)

    def fetch_to_csv(self) -> Path:
        """Download EPSS .csv.gz and write plain CSV."""
        self._ensure_dirs()
        print(f"[epss] downloading: {EPSS_GZ_URL}")
        r = requests.get(EPSS_GZ_URL, timeout=60)
        r.raise_for_status()
        buf = io.BytesIO(r.content)
        with gzip.open(buf, "rt", encoding="utf-8", newline="") as gz, \
             self.cfg.out_csv.open("w", encoding="utf-8", newline="") as out:
            out.write(gz.read())
        print(f"[epss] wrote CSV → {self.cfg.out_csv}")
        return self.cfg.out_csv

    def _create_table_if_needed(self) -> None:
        if not self.engine or not self.cfg.create_table:
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS epss_scores (
            cve TEXT PRIMARY KEY,
            epss DOUBLE PRECISION,
            percentile DOUBLE PRECISION,
            date DATE
        );
        """
        with self.engine.begin() as cx:
            cx.exec_driver_sql(ddl)

    def _iter_rows(self, csv_path: Path) -> Iterable[tuple]:
        with csv_path.open("r", encoding="utf-8", newline="") as fh:
            r = csv.DictReader(fh)
            # EPSS header usually: cve,epss,percentile,date
            for row in r:
                cve = row.get("cve")
                if not cve:
                    continue
                try:
                    epss = float(row.get("epss", "") or "0")
                except Exception:
                    epss = None
                try:
                    pct = float(row.get("percentile", "") or "0")
                except Exception:
                    pct = None
                dt = (row.get("date") or None)
                yield (cve, epss, pct, dt)

    def load_csv_to_db(self, csv_path: Path) -> int:
        if not self.engine:
            print("[epss] DB load skipped (no engine)")
            return 0
        self._create_table_if_needed()
        sql = "INSERT INTO epss_scores (cve, epss, percentile, date) VALUES (:cve,:epss,:pct,:dt) " \
              "ON CONFLICT (cve) DO UPDATE SET epss=EXCLUDED.epss, percentile=EXCLUDED.percentile, date=EXCLUDED.date"
        n = 0
        with self.engine.begin() as cx:
            for cve, epss, pct, dt in self._iter_rows(csv_path):
                cx.execute(text(sql), {"cve": cve, "epss": epss, "pct": pct, "dt": dt})
                n += 1
        print(f"[epss] upserted {n} rows into epss_scores")
        return n

def parse_args(argv) -> EPSSConfig:
    import argparse
    ap = argparse.ArgumentParser(description="Fetch EPSS CSV (and optionally load to Postgres).")
    ap.add_argument("--out", default="data/security/epss_scores.csv", help="Where to write CSV")
    ap.add_argument("--db", action="store_true", help="Also load into Postgres (uses DATABASE_URL)")
    ap.add_argument("--no-create", action="store_true", help="Do not create table automatically")
    ap.add_argument("--db-url", default=None, help="Override DATABASE_URL/POSTGRES_URL")
    args = ap.parse_args(argv)
    return EPSSConfig(
        db_url=args.db_url or os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL"),
        out_csv=Path(args.out),
        create_table=not args.no_create,
        load_db=bool(args.db),
    )

def main() -> int:
    cfg = parse_args(sys.argv[1:])
    fetcher = EPSSFetcher(cfg)
    csv_path = fetcher.fetch_to_csv()
    if cfg.load_db:
        fetcher.load_csv_to_db(csv_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
