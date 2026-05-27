# -*- coding: utf-8 -*-
"""Reaplica os patches da planilha do Diogo em cima do onlog_data.json fresco.

Problema: o cron diario (painelcs.yml) roda fetch_onlog.py que sobrescreve
onlog_data.json com dados crus do Fabric, perdendo os patches (data de
postagem, valorPostagem, statusOnlog) que o ingest_diogo_onlog.py aplicou.
Resultado: pedidos como "Sara e Mari - Kelly Gomes Tomelin 10" voltavam pra
data Mongo (orderDate, ex: 2026-04-30) ao inves da data de postagem real
(2026-05-05) e sumiam da quinzena correta no painel.

Este script roda DEPOIS de fetch_onlog.py e ANTES de merge_data.py e
restaura o estado patcheado lendo os snapshots em onlog_diff_*.json.
"""
import glob
import json
from pathlib import Path

from ingest_diogo_onlog import patch_onlog_data

ROOT = Path(__file__).parent
ONLOG_JSON = ROOT / "onlog_data.json"

if not ONLOG_JSON.exists():
    print("AVISO: onlog_data.json nao existe — pulando reapply")
    raise SystemExit(0)

onlog_data = json.loads(ONLOG_JSON.read_text(encoding="utf-8"))
total_upd = 0
diffs = sorted(glob.glob(str(ROOT / "onlog_diff_*.json")))
print(f"[reapply] {len(diffs)} snapshot(s) de quinzena para reaplicar")
for fn in diffs:
    d = json.loads(Path(fn).read_text(encoding="utf-8"))
    snap = (d.get("_planilhaSnapshot") or {}).get("planilha") or {}
    qz = d.get("quinzena") or {}
    de, ate = qz.get("de", ""), qz.get("ate", "")
    if not snap or not de or not ate:
        print(f"  - {Path(fn).name}: sem snapshot/quinzena, pulando")
        continue
    n_upd, n_skip = patch_onlog_data(onlog_data, snap, de, ate)
    total_upd += n_upd
    print(f"  - {Path(fn).name}: {n_upd} patcheados, {n_skip} sem match no Fabric")

ONLOG_JSON.write_text(json.dumps(onlog_data, ensure_ascii=False), encoding="utf-8")
print(f"[reapply] OK: {total_upd} pedidos restaurados em onlog_data.json")
