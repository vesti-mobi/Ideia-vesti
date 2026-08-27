"""
PainelElisa - coleta do log de AMBIENTE (ligado/desligado) -> ambiente_elisa.json.

Fonte: planilha "Dominios Bloqueados Automacao" (Google Sheets, dona diego@vesti.mobi),
alimentada pelo workflow n8n "Bloqueio e Desbloqueio" (id 8arVGfRb408xr4xH):
  - ramo bloqueio  (Schedule 07:00 UTC, fatura =11 dias vencida) -> Salva Bloqueados
  - ramo desbloqueio (Webhook Iugu de fatura paga)               -> Salva Desbloqueados
  - 2 formTriggers manuais "Desligar/Religar Dominio"

Colunas: Dominio | Ligado? (Sim/Nao) | Update | Nome | Canal

LIMITACAO IMPORTANTE (nao e' bug deste script):
  A planilha e' append-OR-UPDATE por dominio -- guarda o ESTADO ATUAL, nao um log
  de eventos. Em 13/08/2026: 1.065 linhas, 1.065 dominios distintos, ZERO repetido.
  Logo so' da' pra saber a ULTIMA transicao de cada dominio ("religado em 29/07"),
  nunca quantas vezes religou nem religamentos anteriores. Uma marca contribui com
  no maximo 1 evento de reativacao. Para ter historico de verdade, o n8n teria que
  gravar em modo append -- decisao da Laura, workflow nao foi tocado.

Acesso: a service account do pipeline (829232163598-compute@developer.gserviceaccount.com)
precisa de permissao de LEITOR na planilha. Enquanto nao tiver, cai no snapshot
`ambiente_bloqueios.csv` versionado no repo (exportado manualmente do Drive).

Rodar:  py fetch_ambiente.py
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent
OUT = ROOT / "ambiente_elisa.json"
SNAPSHOT = ROOT / "ambiente_bloqueios.csv"

SHEET_ID = "1N0Hs3GsjrhGtAzQ0J-6AFgJ0QzYu7-LDu57hy9Tc7N8"
SHEET_RANGE = "A1:E100000"
_SA_FALLBACK = r"C:\Users\Laura\Downloads\vesti-data-499015-7ea468dae45e.json"

_MESES = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def _parse_update(s: str) -> str:
    """'Thu Jul 23 2026 14:01:54 GMT+0000 (...)' -> '2026-07-23'. Aceita ISO tambem."""
    s = (s or "").strip()
    if not s:
        return ""
    m = re.search(r"\b([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{4})\b", s)
    if m and m.group(1) in _MESES:
        try:
            return date(int(m.group(3)), _MESES[m.group(1)], int(m.group(2))).isoformat()
        except ValueError:
            return ""
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else ""


def _ler_do_sheets() -> list[list[str]] | None:
    """Le a planilha ao vivo. Devolve None se a SA nao tiver acesso (403) ou faltar lib."""
    try:
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account
    except ImportError:
        print("[ambiente] google-auth nao instalado -> usando snapshot.", flush=True)
        return None

    key = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or _SA_FALLBACK
    if not Path(key).exists():
        print(f"[ambiente] credencial nao encontrada ({key}) -> usando snapshot.", flush=True)
        return None
    try:
        cred = service_account.Credentials.from_service_account_file(
            key, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        cred.refresh(Request())
        url = (f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}"
               f"/values/{SHEET_RANGE}")
        req = urllib.request.Request(url, headers={"Authorization": "Bearer " + cred.token})
        with urllib.request.urlopen(req, timeout=60) as resp:
            valores = json.loads(resp.read().decode("utf-8")).get("values", [])
        print(f"[ambiente] planilha lida ao vivo ({len(valores)} linhas).", flush=True)
        return valores
    except Exception as e:
        msg = str(e)
        if "403" in msg:
            print("[ambiente] SA SEM ACESSO a planilha (403). Compartilhe como Leitor com "
                  "829232163598-compute@developer.gserviceaccount.com. Usando snapshot.",
                  flush=True)
        else:
            print(f"[ambiente] falha ao ler planilha ({type(e).__name__}: {msg[:120]}) "
                  f"-> usando snapshot.", flush=True)
        return None


def _ler_do_snapshot() -> list[list[str]]:
    if not SNAPSHOT.exists():
        print(f"[ambiente] ERRO: {SNAPSHOT.name} nao existe e a planilha nao pode ser lida.",
              file=sys.stderr, flush=True)
        return []
    with SNAPSHOT.open(encoding="utf-8-sig", newline="") as fh:
        linhas = list(csv.reader(fh))
    print(f"[ambiente] snapshot {SNAPSHOT.name} ({len(linhas)} linhas).", flush=True)
    return linhas


def _datas_pagas() -> dict[str, set[str]]:
    """Datas de fatura paga por dominio (pagamentos_elisa.json, do fetch_elisa_bq)."""
    p = ROOT / "pagamentos_elisa.json"
    if not p.exists():
        print("[ambiente] AVISO: pagamentos_elisa.json nao existe -> nao da' pra separar "
              "religamento real de 'pagou fatura'. Rode fetch_elisa_bq.py antes.",
              file=sys.stderr, flush=True)
        return {}
    dados = json.loads(p.read_text(encoding="utf-8"))
    return {d: set(v.get("datas") or []) for d, v in (dados.get("dominios") or {}).items()}


def _casa_com_pagamento(quando: str, datas: set[str]) -> bool:
    """True se `quando` coincide (+-1 dia) com alguma fatura paga do dominio.

    Motivo: o ramo de desbloqueio do n8n dispara no webhook de FATURA PAGA e chama
    unblock para todo mundo, bloqueada ou nao. Verificado em 13/08/2026: 710 dos 807
    dominios com Ligado?=Sim tem Update igual a' data da ultima fatura paga. Entao
    "Sim" que casa com pagamento NAO e' religamento -- e' so' a marca pagando.
    O que sobra (form manual "Religar Dominio", fallback por CNPJ) e' religamento real.
    """
    if not quando or not datas:
        return False
    d = date.fromisoformat(quando)
    for delta in (-1, 0, 1):
        if (d + timedelta(days=delta)).isoformat() in datas:
            return True
    return False


def coletar() -> dict[str, dict]:
    linhas = _ler_do_sheets() or _ler_do_snapshot()
    pagas = _datas_pagas()
    out: dict[str, dict] = {}
    ligados = desligados = 0
    for row in linhas:
        if not row:
            continue
        dom = (row[0] or "").strip()
        if not dom.isdigit():          # header e linhas vazias
            continue
        ligado_txt = (row[1] if len(row) > 1 else "").strip().lower()
        ligado = ligado_txt.startswith("s")          # "Sim" / "Nao"
        quando = _parse_update(row[2] if len(row) > 2 else "")
        # append-or-update: se o mesmo dominio repetir, o registro mais NOVO vence
        anterior = out.get(dom)
        if anterior and anterior["update"] >= quando:
            continue
        out[dom] = {
            "ligado": ligado,
            "update": quando,
            # regra 4: religamento de verdade = "Sim" que NAO casa com fatura paga
            "religamentoReal": bool(ligado and quando
                                    and not _casa_com_pagamento(quando, pagas.get(dom, set()))),
            "nomePlanilha": (row[3] if len(row) > 3 else "").strip(),
            "canalPlanilha": (row[4] if len(row) > 4 else "").strip(),
        }
    reais = 0
    for v in out.values():
        if v["ligado"]:
            ligados += 1
            reais += bool(v["religamentoReal"])
        else:
            desligados += 1
    print(f"[ambiente] {len(out)} dominios ({ligados} ligados, {desligados} desligados); "
          f"{reais} religamentos reais (os outros {ligados - reais} 'Sim' sao so' fatura paga)",
          flush=True)
    return out


def main():
    dados = coletar()
    OUT.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT.name} ({len(dados)} dominios)")


if __name__ == "__main__":
    main()
