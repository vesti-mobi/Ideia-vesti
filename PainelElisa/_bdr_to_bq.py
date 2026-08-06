"""
BDR (rebates de antecipacao BDR Asset) -> BigQuery `vesti-data-499015.vestilake_BI.bdr`.

Substitui o notebook Fabric "DadosVestipagoBDR.ipynb", que fazia:
  API BDR (janela recente) + tabela Delta `bdr` (historico) -> overwrite em `bdr`.

Aqui a mesma logica, com o BQ como destino:
  - `--seed-fabric`  : le SELECT * FROM bdr no warehouse VestiHouse (Fabric) e
                       carrega o historico inteiro no BQ (WRITE_TRUNCATE).
                       Roda UMA vez, na migracao. Exige `az login`.
                       ATENCAO: a capacidade Fabric esta PAUSADA (erro 24800) e a
                       subscription Azure aparece como Disabled -> na pratica a
                       migracao de 06/08/2026 usou `--seed-csv`.
  - `--seed-csv PATH`: mesma carga inicial, porem lendo o historico de um CSV
                       exportado do proprio modelo do Power BI (DAX via ADOMD).
                       Foi o caminho usado na migracao, com o Fabric fora do ar.
  - (padrao)         : busca a API do BDR (devolve janela rolante de ~2 meses),
                       descarta o menor dt_ref (dia parcial, igual ao notebook)
                       e faz DELETE do intervalo + INSERT no BQ (idempotente).

A API NAO devolve historico: hoje ela cobre ~2 meses. Por isso o historico
so existe via seed do Fabric; depois do seed o BQ passa a ser a fonte da verdade.

Rodar:
    py _bdr_to_bq.py --seed-fabric      # 1x, migracao (precisa de az login)
    py _bdr_to_bq.py                    # diario/incremental

Credencial BQ: GOOGLE_APPLICATION_CREDENTIALS (SA key); fallback local abaixo.
"""
from __future__ import annotations

import argparse
import json
import os
import struct
import subprocess
import sys

import pandas as pd
import requests
from google.cloud import bigquery

# ---------------------------------------------------------------- config -----
PROJECT = "vesti-data-499015"
DATASET = "vestilake_BI"
TABLE = "bdr"
FQTN = f"{PROJECT}.{DATASET}.{TABLE}"

_SA_FALLBACK = r"C:\Users\Laura\Downloads\vesti-data-499015-7ea468dae45e.json"

BDR_AUTH_URL = "https://api.services.bdrasset.com.br/api/auth"
BDR_REBATES_URL = "http://api.services.bdrasset.com.br/api/v15/antecipacoes/comercial/rebates"
# `or` (nao default do get): no GH Actions um secret inexistente vira string vazia
BDR_USER = os.environ.get("BDR_USER") or "mourad@vesti.com.br"
BDR_PASS = os.environ.get("BDR_PASS") or "123"

FABRIC_SERVER = (
    "7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4"
    ".datawarehouse.fabric.microsoft.com"
)
FABRIC_DB = "VestiHouse"
SQL_COPT_SS_ACCESS_TOKEN = 1256

DATE_COLS = ["dt_ref", "dt_pagamento", "dt_vencimento", "dt_pagamento_rebate"]
NUM_COLS = [
    "valor_onerado", "valor_presente", "valor_juros", "taxa_juros",
    "taxa_contabil", "rebate", "pct_rebate_juros", "rebate_financeiro",
]
STR_COLS = ["id_pedido", "company_id"]
COLS = DATE_COLS + NUM_COLS + STR_COLS

SCHEMA = (
    [bigquery.SchemaField(c, "DATE") for c in DATE_COLS]
    + [bigquery.SchemaField(c, "FLOAT64") for c in NUM_COLS]
    + [bigquery.SchemaField(c, "STRING") for c in STR_COLS]
)


# ------------------------------------------------------------- utilidades ----
def bq_client() -> bigquery.Client:
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists(_SA_FALLBACK):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _SA_FALLBACK
    return bigquery.Client(project=PROJECT)


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Tipa e ordena as colunas no contrato da tabela do BQ."""
    for c in COLS:
        if c not in df.columns:
            df[c] = None
    for c in DATE_COLS:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in STR_COLS:
        df[c] = df[c].astype("string")
    return df[COLS]


# ------------------------------------------------------------------ fontes ---
def fetch_api() -> pd.DataFrame:
    print("Autenticando na API do BDR...")
    tok = requests.get(BDR_AUTH_URL, auth=(BDR_USER, BDR_PASS), timeout=60).json().get("token")
    if not tok:
        sys.exit("ERRO: API do BDR nao devolveu token.")

    resp = requests.post(
        BDR_REBATES_URL,
        headers={"authToken": tok, "Content-Type": "application/json"},
        json={},
        timeout=300,
    )
    # a API emite NaN/Infinity crus, que nao sao JSON valido
    txt = resp.text
    for a, b in (
        (":NaN,", ":null,"), (":NaN}", ":null}"),
        (":Infinity,", ":null,"), (":Infinity}", ":null}"),
        (":-Infinity,", ":null,"), (":-Infinity}", ":null}"),
    ):
        txt = txt.replace(a, b)
    df = pd.DataFrame(json.loads(txt).get("data", []))
    print(f"API: {len(df)} linhas ({df['dt_ref'].min()} -> {df['dt_ref'].max()})")
    return normalize(df)


def fetch_fabric() -> pd.DataFrame:
    """Historico completo da tabela Delta `bdr` no warehouse VestiHouse."""
    import pyodbc

    out = subprocess.run(
        ["az", "account", "get-access-token", "--resource",
         "https://database.windows.net/", "-o", "json"],
        capture_output=True, text=True, shell=True,
    )
    if out.returncode != 0:
        sys.exit(f"ERRO: rode `az login` primeiro.\n{out.stderr.strip()[:400]}")
    tok = json.loads(out.stdout)["accessToken"].encode("utf-16-le")
    tok = struct.pack("<i", len(tok)) + tok

    cn = pyodbc.connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={FABRIC_SERVER},1433;Database={FABRIC_DB};"
        "Encrypt=yes;TrustServerCertificate=no;",
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: tok},
        timeout=300,
    )
    print("Lendo bdr do Fabric...")
    df = pd.read_sql(f"SELECT {', '.join(COLS)} FROM bdr", cn)
    cn.close()
    print(f"Fabric: {len(df)} linhas")
    return normalize(df)


# ------------------------------------------------------------------ cargas ---
def load(client: bigquery.Client, df: pd.DataFrame, truncate: bool) -> None:
    cfg = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_TRUNCATE" if truncate else "WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(field="dt_ref"),
        clustering_fields=["company_id"],
    )
    client.load_table_from_dataframe(df, FQTN, job_config=cfg).result()
    tbl = client.get_table(FQTN)
    print(f"OK -> {FQTN}: {tbl.num_rows} linhas no total")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed-fabric", action="store_true",
                    help="carga inicial do historico vindo do Fabric (WRITE_TRUNCATE)")
    ap.add_argument("--seed-csv", metavar="PATH",
                    help="carga inicial lendo o historico de um CSV (export do modelo PBI)")
    args = ap.parse_args()

    client = bq_client()

    if args.seed_fabric or args.seed_csv:
        if args.seed_csv:
            hist = normalize(pd.read_csv(args.seed_csv))
            print(f"CSV: {len(hist)} linhas")
        else:
            hist = fetch_fabric()
        api = fetch_api()
        # API manda no intervalo que ela cobre; Fabric preenche o que veio antes
        corte = api["dt_ref"].min()
        hist = hist[hist["dt_ref"] < corte]
        df = pd.concat([hist, api], ignore_index=True)
        print(f"Seed: {len(df)} linhas ({df['dt_ref'].min()} -> {df['dt_ref'].max()})")
        load(client, df, truncate=True)
        return

    df = fetch_api()
    # menor dt_ref da janela costuma vir parcial -> descarta (igual ao notebook)
    df = df[df["dt_ref"] > df["dt_ref"].min()]
    ini, fim = df["dt_ref"].min(), df["dt_ref"].max()
    print(f"Regravando {ini} -> {fim} ({len(df)} linhas)")
    client.query(
        f"DELETE FROM `{FQTN}` WHERE dt_ref BETWEEN @ini AND @fim",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("ini", "DATE", ini),
            bigquery.ScalarQueryParameter("fim", "DATE", fim),
        ]),
    ).result()
    load(client, df, truncate=False)


if __name__ == "__main__":
    main()
