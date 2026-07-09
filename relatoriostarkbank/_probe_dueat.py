"""
Probe GENTIL da API orders-paylinks: descobrir se da pra recuperar a cauda de
fluxo dos pedidos antigos (pagos antes de ~15/04) filtrando por VENCIMENTO da
parcela (payment.receivables.dueAt) em vez de payment.paidAt.

Faz poucas requisicoes, limit pequeno, sempre pagina 1. NAO pagina fundo (nada
a ver com o 500 da paginacao). So imprime totalDocs e amostra pra decidir o fix.
"""
import json, os, sys, urllib.parse, urllib.request

API = "https://apivesti.vesti.mobi/order/v1/report/orders-paylinks"
TOKEN = os.environ.get("VESTI_ORDER_TOKEN", "").strip()
if not TOKEN:
    print("ERRO: VESTI_ORDER_TOKEN ausente", file=sys.stderr); sys.exit(1)
H = {"accept": "application/json", "Authorization": "Bearer " + TOKEN}


def get(params):
    url = API + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(urllib.request.Request(url, headers=H), timeout=120) as r:
        return json.loads(r.read().decode("utf-8"))


def show(label, params):
    print(f"\n{'='*70}\n[{label}]\n  params: {params}")
    try:
        d = get(params)
    except Exception as e:
        print(f"  ERRO: {type(e).__name__}: {e}")
        return
    if not isinstance(d, dict):
        print(f"  resposta nao-dict: {str(d)[:200]}"); return
    print(f"  totalDocs={d.get('totalDocs')}  totalPages={d.get('totalPages')}  page={d.get('page')}")
    data = d.get("data", []) or []
    print(f"  itens nesta pagina: {len(data)}")
    for o in data[:4]:
        pay = o.get("payment") or {}
        recs = pay.get("receivables") or []
        dues = [(rc.get("installment"), (rc.get("dueAt") or "")[:10], rc.get("status"),
                 rc.get("antecipationValue")) for rc in recs]
        print(f"   - #{o.get('orderNumber')} paidAt={(pay.get('paidAt') or '')[:10]} "
              f"nRec={len(recs)}")
        print(f"        parcelas(inst,due,status,antec): {dues[:12]}")


BASE = {
    "filter[isClosed]": "true",
    "filter[payment.isPaid]": "true",
    "filter[payment.method]": "CREDIT_CARD",
    "filter[payment.transaction.provider]": "STARKBANK",
    "select": "orderNumber,companyId,domainId,customer,payment",
    "limit": 10,
    "page": 1,
}

# A) baseline: filtro atual por paidAt num mes recente que SABEMOS ter dados (jun)
show("A baseline paidAt jun/2026 (deve ter dados)", dict(BASE,
     **{"filter[payment.paidAt][$gte]": "2026-06-01T00:00:00",
        "filter[payment.paidAt][$lte]": "2026-06-05T00:00:00"}))

# B) reconfirma: paidAt em fev/2026 (esperado 0 = API nao retem tao antigo)
show("B paidAt fev/2026 (reconfirma retencao)", dict(BASE,
     **{"filter[payment.paidAt][$gte]": "2026-02-01T00:00:00",
        "filter[payment.paidAt][$lte]": "2026-02-28T00:00:00"}))

# C) SEM paidAt, filtra por VENCIMENTO da parcela em ago/2026
show("C receivables.dueAt ago/2026 (sem paidAt)", dict(BASE,
     **{"filter[payment.receivables.dueAt][$gte]": "2026-08-01T00:00:00",
        "filter[payment.receivables.dueAt][$lte]": "2026-08-31T00:00:00"}))

# D) variante de nome do campo (as vezes e' 'receivables.dueAt' sem 'payment.')
show("D receivables.dueAt (sem prefixo payment)", dict(BASE,
     **{"filter[receivables.dueAt][$gte]": "2026-08-01T00:00:00",
        "filter[receivables.dueAt][$lte]": "2026-08-31T00:00:00"}))

# E) controle: mesma base SEM nenhum filtro de data (pra ver totalDocs geral e
#    se C/D mudaram algo vs este total => filtro respeitado)
show("E base sem filtro de data (controle totalDocs)", dict(BASE))

print("\n\nLEITURA:")
print("- Se C (ou D) devolve pedidos com paidAt ANTES de 2026-04 => da pra puxar a cauda antiga por dueAt. FIX limpo.")
print("- Se C/D totalDocs == E totalDocs => filtro de dueAt IGNORADO (nao suportado).")
print("- Se B tem totalDocs>0 => a API afinal retem fev; retencao nao e a causa (improvavel).")
