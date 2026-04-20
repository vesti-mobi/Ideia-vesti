"""
Orquestrador: roda todos os fetchers na ordem certa e depois build_html.py.

Ordem:
  1. fetch_fabric.py  -> companies_data.json + dashboard_full_data.js (COMPANIES_DATA)
  2. fetch_sheets.py  -> sheets_data.json (NPS + CSAT Oraculo)
  3. fetch_users.py   -> users_data.json (precisa de sheets_data.json pra filtrar)
  4. fetch_hubspot.py -> hubspot_data.json (CSAT Plataforma) [opcional se faltar scope]
  5. merge_data.py    -> injeta todos os consts no dashboard_full_data.js
  6. build_html.py    -> index.html
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
STEPS = [
    ("fetch_fabric.py", True),
    ("fetch_sheets.py", True),
    ("fetch_users.py", True),
    ("fetch_hubspot.py", False),  # opcional: nao bloqueia se falhar
    ("fetch_churn_vestipago.py", True),
    ("fetch_onlog.py", False),  # opcional: nao bloqueia se falhar
    ("merge_data.py", True),
    ("build_html.py", True),
]


def run(script: str, required: bool) -> bool:
    print(f"\n===> {script}")
    r = subprocess.run(["py", script], cwd=ROOT)
    if r.returncode != 0:
        if required:
            print(f"[update_all] {script} falhou (required). Abortando.", file=sys.stderr)
            sys.exit(r.returncode)
        print(f"[update_all] {script} falhou (opcional). Continuando.", file=sys.stderr)
        return False
    return True


def main() -> None:
    for script, required in STEPS:
        if not (ROOT / script).exists():
            msg = f"[update_all] {script} nao existe"
            if required:
                print(msg + " (required). Abortando.", file=sys.stderr)
                sys.exit(1)
            print(msg + " (opcional). Pulando.")
            continue
        run(script, required)
    print("\n[update_all] done.")


if __name__ == "__main__":
    main()
