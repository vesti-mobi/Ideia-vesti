"""
Extracao Loan Tape - aba "Duplicata, Cartao, Judicial, CP".

Reproduz as 12 colunas do template (CÓPIA - Loan Tape (vop)) puxando os
recebiveis VestiPago/StarkBank direto do Fabric (dbo.mongodb_pedidos_recebiveis),
1 linha por parcela. Recorte: parcelas liquidadas com receivables.paidAt >= 2025-06-01.

Colunas (mapeamento do template):
  1 Data da Aquisicao          = receivables.paidAt
  2 Valor de Aquisicao         = receivables.grossValue
  3 Data de Vencimento         = receivables.dueAt
  4 Valor Nominal              = vestiPagoValue + antifraudValue + antecipationValue
  5 Data do Pagamento Recebido = receivables.paidAt
  6 Valor do Pagamento Recebido= receivables.netValue
  7 Tipo de Ativo              = "Recebivel" (constante)
  8 Cedente                    = NULL (sem fonte)
  9 Sacado                     = companyId
 10 Tipo de Pagamento          = payment.method
 11 UF do Sacado               = NULL (sem fonte)
 12 Produto                    = NULL (sem fonte)

Auth: reaproveita fetch_data.connect() (az CLI local ou FABRIC_REFRESH_TOKEN no CI).
Saida: loan_tape_duplicata.csv (UTF-8 com BOM, separador ';').
"""
from __future__ import annotations

import csv
import sys
import time
from pathlib import Path

import pyodbc

import fetch_data

ROOT = Path(__file__).parent
OUT_CSV = ROOT / "loan_tape_duplicata.csv"
DATA_CORTE = "2025-06-01"

HEADER = [
    "Data da Aquisição",
    "Valor de Aquisição",
    "Data de Vencimento",
    "Valor Nominal",
    "Data do Pagamento Recebido",
    "Valor do Pagamento Recebido",
    "Tipo de Ativo",
    "Cedente",
    "Sacado",
    "Tipo de Pagamento",
    "UF do Sacado",
    "Produto",
]

# Fonte: MongoDB_Pedidos_Geral (1 linha por pedido, historico completo desde 2023).
# Os recebiveis VestiPago vivem sob provider IUGU (2023->2026) e STARKBANK (2026->;
# migracao de processador). A tabela mongodb_pedidos_recebiveis (1 linha/parcela)
# so tem janela recente (~2 meses) e so STARKBANK, por isso NAO e usada aqui — usar
# Pedidos_Geral garante o historico inteiro com grao uniforme (1 recebivel/pedido).
# Filtro pela Data da Aquisicao (receivables.paidAt, convertido p/ BRT) >= corte.
SQL = f"""
WITH base AS (
    SELECT
        DATEADD(HOUR, -3, TRY_CAST(p.payment_receivables_paidAt AS DATETIME2)) AS data_aquisicao,
        TRY_CAST(p.payment_receivables_grossValue AS FLOAT)                    AS valor_aquisicao,
        DATEADD(HOUR, -3, TRY_CAST(p.payment_receivables_dueAt AS DATETIME2))  AS data_vencimento,
        ( COALESCE(TRY_CAST(p.payment_receivables_vestiPagoValue   AS FLOAT), 0)
        + COALESCE(TRY_CAST(p.payment_receivables_antifraudValue   AS FLOAT), 0)
        + COALESCE(TRY_CAST(p.payment_receivables_antecipationValue AS FLOAT), 0) ) AS valor_nominal,
        TRY_CAST(p.payment_receivables_netValue AS FLOAT)                      AS valor_pgto_recebido,
        p.companyId                                                            AS sacado,
        p.payment_method                                                       AS tipo_pagamento
    FROM dbo.MongoDB_Pedidos_Geral p
    WHERE p.payment_transaction_provider IN ('IUGU', 'Iugu', 'STARKBANK')
      AND p.payment_receivables_grossValue IS NOT NULL
)
SELECT
    data_aquisicao        AS [Data da Aquisição],
    valor_aquisicao       AS [Valor de Aquisição],
    data_vencimento       AS [Data de Vencimento],
    valor_nominal         AS [Valor Nominal],
    data_aquisicao        AS [Data do Pagamento Recebido],
    valor_pgto_recebido   AS [Valor do Pagamento Recebido],
    CAST('Recebivel' AS NVARCHAR(20)) AS [Tipo de Ativo],
    CAST(NULL AS NVARCHAR(60))        AS [Cedente],
    sacado                AS [Sacado],
    tipo_pagamento        AS [Tipo de Pagamento],
    CAST(NULL AS NVARCHAR(2))         AS [UF do Sacado],
    CAST(NULL AS NVARCHAR(60))        AS [Produto]
FROM base
WHERE data_aquisicao >= '{DATA_CORTE}'
ORDER BY data_aquisicao
"""

TRANSIENT = {"08S01", "08001", "08003", "HYT00", "HYT01", "40001", "40197", "40613"}


def _fmt(v):
    if v is None:
        return ""
    if hasattr(v, "isoformat"):
        return v.isoformat(sep=" ")
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def fetch_rows():
    last = None
    for tentativa in range(1, 5):
        try:
            conn = fetch_data.connect()
            cur = conn.cursor()
            print(f"[query] executando (corte {DATA_CORTE}) ...", flush=True)
            cur.execute(SQL)
            rows = cur.fetchall()
            conn.close()
            return rows
        except pyodbc.Error as e:
            last = e
            st = e.args[0] if e.args else ""
            if tentativa >= 4 or (st not in TRANSIENT and "communication link" not in str(e).lower()):
                raise
            espera = 15 * tentativa
            print(f"[query] falha transitoria {st}; tentativa {tentativa}/4, retry em {espera}s",
                  file=sys.stderr, flush=True)
            time.sleep(espera)
    raise last


def main():
    rows = fetch_rows()
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(HEADER)
        for r in rows:
            w.writerow([_fmt(v) for v in r])
    print(f"[ok] {OUT_CSV.name}: {len(rows)} linhas", flush=True)


if __name__ == "__main__":
    main()
