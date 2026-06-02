"""Debug descartavel: confere a workspace RB Store na API Vesti/Stark.

Responde DUAS perguntas:
  1) A workspace existe/esta ativa? (aparece em /workspaces)
  2) Ela tem purchases? E a paginacao completou ou foi interrompida por erro?

Nao escreve nada — so loga. Reusa as funcoes de fetch_invoices.py.
"""
import os
import sys

from fetch_invoices import list_workspaces, list_purchases

ALVO = "rb store"  # nome (lower) que procuramos


def main() -> None:
    token = os.environ.get("VESTIAPI_TOKEN", "").strip()
    if not token:
        print("[erro] VESTIAPI_TOKEN ausente", file=sys.stderr)
        sys.exit(1)

    wss = list_workspaces(token)
    print(f"[api] {len(wss)} workspaces ativas (apos filtro de teste)")

    # acha RB Store (match exato e por substring, pra pegar variacoes de nome)
    achados = [w for w in wss if ALVO in (w.get("name") or "").strip().lower()]
    if not achados:
        print(f"[resultado] NENHUMA workspace com '{ALVO}' no nome.")
        # mostra nomes parecidos pra ajudar
        parecidas = [w.get("name") for w in wss if "rb" in (w.get("name") or "").lower()
                     or "bispo" in (w.get("name") or "").lower()]
        print(f"[dica] workspaces com 'rb'/'bispo' no nome: {parecidas}")
        return

    for w in achados:
        ws_id = str(w.get("id") or w.get("workspaceId") or "")
        nome = w.get("name")
        print(f"\n[workspace] nome='{nome}' id={ws_id}")
        print(f"[workspace] payload bruto: {w}")

        purchases, ok = list_purchases(ws_id, token)
        print(f"[purchases] total coletado: {len(purchases)}")
        print(f"[purchases] paginacao completa? {'SIM' if ok else 'NAO (interrompida por erro)'}")

        # resumo dos primeiros pedidos pra ver se ha movimento real
        for p in purchases[:5]:
            print(f"  - purchaseId={p.get('purchaseId') or p.get('id')} "
                  f"status={p.get('status')} amount={p.get('amount')} "
                  f"created={p.get('created') or p.get('createdAt')}")


if __name__ == "__main__":
    main()
