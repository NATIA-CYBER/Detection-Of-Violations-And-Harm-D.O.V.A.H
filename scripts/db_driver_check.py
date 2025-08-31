#!/usr/bin/env python
"""
DB driver health check for DOVAH.

What it does:
  - Prints Python/OS, SQLAlchemy, NumPy/Pandas versions (quick sanity)
  - Checks BOTH psycopg2 and psycopg (v3) presence + versions
  - Verifies psycopg2 exposes 'paramstyle' (your earlier error)
  - Reads DATABASE_URL (or POSTGRES_URL) and tries a simple "SELECT 1"
    - Works for 'sqlite://', 'postgresql+psycop2://', 'postgresql+psycopg://'

Exit code is non-zero on failure.
"""
from __future__ import annotations

import os, sys, platform, importlib
from typing import Optional

def _imp(name: str):
    try:
        m = importlib.import_module(name)
        return m, None
    except Exception as e:
        return None, e

def _ver(mod) -> Optional[str]:
    for attr in ("__version__", "VERSION", "version"):
        if hasattr(mod, attr):
            v = getattr(mod, attr)
            try:
                return ".".join(map(str, v)) if isinstance(v, (tuple, list)) else str(v)
            except Exception:
                return str(v)
    return None

def main() -> int:
    bad = False
    print("=== Runtime ===")
    print("python       :", sys.version.split()[0])
    print("platform     :", platform.platform())

    sa, e = _imp("sqlalchemy")
    print("sqlalchemy   :", _ver(sa) if sa else f"NOT INSTALLED ({e})")
    np, _ = _imp("numpy")
    print("numpy        :", _ver(np) if np else "NOT INSTALLED")
    pd, _ = _imp("pandas")
    print("pandas       :", _ver(pd) if pd else "NOT INSTALLED")

    print("\n=== Drivers ===")
    pg2, e2 = _imp("psycopg2")
    if pg2:
        pv = _ver(pg2)
        paramstyle = getattr(pg2, "paramstyle", None)
        print(f"psycopg2     : {pv} (paramstyle={paramstyle!r})")
        if paramstyle is None:
            print("!! psycopg2 BROKEN: missing 'paramstyle' (reinstall 'psycopg2-binary<3')")
            bad = True
    else:
        print(f"psycopg2     : NOT INSTALLED ({e2})")

    pg3, e3 = _imp("psycopg")
    if pg3:
        print(f"psycopg(v3)  : {_ver(pg3)}")
    else:
        print(f"psycopg(v3)  : NOT INSTALLED ({e3})")

    print("\n=== URL test ===")
    url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or "sqlite://"
    print("DATABASE_URL :", url)

    # Try simple connect
    try:
        from sqlalchemy import create_engine, text
        eng = create_engine(url)
        with eng.connect() as conn:
            ok = conn.execute(text("SELECT 1")).scalar_one()
        print(f"connect+SELECT 1 → OK ({ok})")
    except Exception as e:
        print(f"DB CONNECT FAILED: {e}")
        bad = True

    return 1 if bad else 0

if __name__ == "__main__":
    raise SystemExit(main())
