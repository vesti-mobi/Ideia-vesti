"""
Backfill unico da aba CP FLUXO: re-insere pedidos de fluxo (nao-antecipados)
que a API dropou (sumiram do dados.js atual mas existiam num snapshot bom),
sem tocar na aba Antecipacao.

Uso:
  DADOS_CUR=dados.js  SNAP=dados_0701.js  CANC=cancelados_valores.js  python _backfill_fluxo.py
Reusa _assemble() do fetch_data.py pra regenerar pagamentos/resumo corretamente.
"""
import json, os, sys
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from fetch_data import _assemble  # mesma logica que gera pagamentos/resumo

CUR  = os.environ.get("DADOS_CUR", str(HERE / "dados.js"))
SNAP = os.environ.get("SNAP",      str(HERE / "dados_0701.js"))
CANC = os.environ.get("CANC",      str(HERE / "cancelados.js"))

def load(fn):
    t = Path(fn).read_text(encoding="utf-8").strip()
    t = t[t.index("=") + 1:].rstrip().rstrip(";")
    return json.loads(t)

def is_test(n):
    s = (n or "").lower().strip()
    return s in ("andressa vesti", "andressa - teste")

cur  = load(CUR)
snap = load(SNAP)
try:
    canc_ids = set(load(CANC).get("valores", {}).keys())
except Exception:
    canc_ids = set()

cur_ids = set(p["orderId"] for p in cur.get("pedidos", []) if p.get("orderId"))

# Pedidos de FLUXO (nao antec) presentes no snapshot bom mas ausentes hoje,
# nao cancelados, nao-teste. (Antecipacao NAO e re-inserida.)
readd = [p for p in snap.get("pedidos", [])
         if p.get("orderId") and p["orderId"] not in cur_ids
         and p["orderId"] not in canc_ids
         and not p.get("antecipacaoEnabled")
         and not is_test(p.get("nomeFantasia"))]

def fluxo_total(data, dmin="2026-07-10"):
    return round(sum((p.get("grossValue") or 0) for p in data["pagamentos"]
                     if not p["isAntecipacao"] and not is_test(p.get("nomeFantasia"))
                     and (p.get("payDate") or "")[:10] >= dmin), 2)

def antec_total(data, dmin="2026-07-10"):
    return round(sum((p.get("grossValue") or 0) for p in data["pagamentos"]
                     if p["isAntecipacao"] and not is_test(p.get("nomeFantasia"))
                     and (p.get("payDate") or "")[:10] >= dmin), 2)

print(f"pedidos re-inseridos (fluxo): {len(readd)}")
print(f"ANTES  fluxo(de 10/07)=R$ {fluxo_total(cur):,.2f}  antec(de 10/07)=R$ {antec_total(cur):,.2f}")
print(f"ANTES  nPedidos={len(cur['pedidos'])}  nPagamentos={len(cur['pagamentos'])}")

# Une pedidos e re-monta pagamentos/resumo com a MESMA logica do pipeline
merged_pedidos = list(cur["pedidos"]) + readd
new_data = _assemble(merged_pedidos)

print(f"DEPOIS fluxo(de 10/07)=R$ {fluxo_total(new_data):,.2f}  antec(de 10/07)=R$ {antec_total(new_data):,.2f}")
print(f"DEPOIS nPedidos={len(new_data['pedidos'])}  nPagamentos={len(new_data['pagamentos'])}")

# guarda-corpo: antecipacao NAO pode mudar
assert antec_total(new_data) == antec_total(cur), "ANTEC MUDOU — abortando"

out = os.environ.get("OUT", CUR)
Path(out).write_text("window.DADOS = " + json.dumps(new_data, ensure_ascii=False) + ";\n",
                     encoding="utf-8")
print(f"[write] {out}")
