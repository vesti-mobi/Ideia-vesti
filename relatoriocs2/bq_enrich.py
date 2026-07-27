# -*- coding: utf-8 -*-
"""
Enriquecimento do relatoriocs2 via BigQuery (roda 1x/dia junto do fetch_data.py).

Faz duas coisas:
  1. ABA 1 - a data de entrada (e os dias derivados dela) vem do cadastro real
     no BQ, nao do que esta digitado na planilha.
  2. ABA 3 - o GMV das empresas vem do BQ em tres periodos, todo dia.

O UNIVERSO DE MARCAS E O DA PLANILHA (decisao da Laura, 27/07/2026): este modulo
NUNCA cria linha. Loja nova so entra no painel quando a CS a coloca na planilha
das 3 CS — o BQ enriquece quem ja esta la, e nada mais. Ate 27/07 o modulo
adicionava toda loja nova dos ultimos 30 dias; isso foi removido de proposito.

DECISOES QUE VALEM A PENA SABER (medidas no BQ em 24/07/2026):

  * Periodo 1 e jul-dez/2025, NAO jun-dez/2025: o lake nao tem nada antes de
    2025-07. Junho/2025 simplesmente nao existe pra ser somado.

  * TETO_PEDIDO existe porque ha lixo na origem: um unico pedido de
    R$ 123.740.005.562 em jun/2026 inflava o mes de ~107 mi para ~123 BI.
    O corte e 1 milhao (e nao os 50 mil que o PainelElisa usa) pra nao jogar
    fora 328 pedidos de atacado legitimos entre 50k e 1M — que sao exatamente
    o publico deste ranking.

  * Empresa que nao casa por nome com nenhum dominio fica SEM GMV (null), nunca
    com zero: zero seria afirmar que ela nao vendeu, e o que houve foi so nao
    ter achado o cadastro.

  * Marca que nao casa por nome MANTEM a data de entrada da planilha. Nao casar
    nao prova que a data esta errada — so que nao achamos o cadastro. Mesmo
    principio do GMV e da coluna Tino.

  * Linhas com origem="bq" criadas antes de 27/07/2026 continuam sendo lidas do
    build anterior pelo fetch_data.py; elas ja estao no painel e podem ter sido
    preenchidas pelo time. O que nao acontece mais e a criacao de novas.

Credencial: GOOGLE_APPLICATION_CREDENTIALS (arquivo) ou GCP_SA_KEY (json inline).
Sem credencial o modulo nao quebra o fetcher: devolve os dados como estavam.
"""
from __future__ import annotations

import datetime
import json
import os
import re
import sys
import tempfile
import unicodedata

PROJECT = "vesti-data-499015"
DATASET = "vestilake_BI"
DS = f"`{PROJECT}.{DATASET}`"

TETO_PEDIDO = 1_000_000        # acima disso e lixo de origem, nao venda

# A data de entrada da aba 1 vem do BQ? Desligado de proposito — veja o porque
# em _aba1_entrada(): `entrada` (passagem de bastao) e `created_at` (cadastro do
# dominio) sao grandezas diferentes, e trocar quebra o alerta de 45/60 dias.
ENTRADA_DO_BQ = False

P1_INI, P1_FIM = "2025-07-01", "2025-12-31"   # piso do lake: nao existe jun/2025
P2_INI, P2_FIM = "2026-01-01", "2026-06-30"
P3_INI = "2026-07-01"
P3_FIM = "2026-12-31"


def norm(s) -> str:
    s = unicodedata.normalize("NFD", str(s or "").lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]", "", s)


def _client():
    """Client do BQ, ou None se nao houver credencial (o painel segue sem BQ)."""
    try:
        from google.cloud import bigquery
    except ImportError:
        print("[bq] google-cloud-bigquery nao instalado — pulando enriquecimento", flush=True)
        return None

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raw = os.environ.get("GCP_SA_KEY")
        if raw:
            f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
            f.write(raw)
            f.close()
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("[bq] sem GCP_SA_KEY / GOOGLE_APPLICATION_CREDENTIALS — pulando enriquecimento", flush=True)
        return None
    try:
        return bigquery.Client(project=PROJECT)
    except Exception as e:  # credencial invalida nao pode derrubar o painel
        print(f"[bq] nao consegui autenticar ({e}) — pulando enriquecimento", flush=True)
        return None


def _rows(client, sql, titulo):
    print(f"[bq] {titulo}...", flush=True)
    return [dict(r) for r in client.query(sql).result()]


SQL_LOJAS = f"""
SELECT CAST(d.ID AS STRING) id, d.name,
       DATE(CAST(d.created_at AS TIMESTAMP)) criada,
       a.name anjo
FROM {DS}.odbc_domains d
LEFT JOIN {DS}.odbc_angels a ON CAST(a.id AS STRING) = CAST(d.angel_id AS STRING)
WHERE LOWER(CAST(d.modulos AS STRING)) LIKE '%vendas%'
  AND d.name IS NOT NULL
  AND LOWER(d.name) NOT LIKE '%teste%'
"""

SQL_GMV = f"""
SELECT id,
  ROUND(SUM(CASE WHEN dia BETWEEN '{P1_INI}' AND '{P1_FIM}' THEN v END), 2) p1,
  ROUND(SUM(CASE WHEN dia BETWEEN '{P2_INI}' AND '{P2_FIM}' THEN v END), 2) p2,
  ROUND(SUM(CASE WHEN dia BETWEEN '{P3_INI}' AND '{P3_FIM}' THEN v END), 2) p3
FROM (
  SELECT domainId id,
         DATE(CAST(settings_createdAt AS TIMESTAMP)) dia,
         SAFE_CAST(summary_total AS FLOAT64) v
  FROM {DS}.MongoDB_Pedidos_Geral
  WHERE SAFE_CAST(summary_total AS FLOAT64) > 0
    AND SAFE_CAST(summary_total AS FLOAT64) < {TETO_PEDIDO}
    AND CAST(settings_createdAt AS TIMESTAMP) >= '{P1_INI}'
)
GROUP BY id
"""


def _indice_lojas(lojas, gmv):
    """norm(nome) -> loja. Nome repetido entre lojas e ambiguo: fica a que mais
    vendeu (a outra costuma ser cadastro morto homonimo)."""
    total = {g["id"]: (g.get("p1") or 0) + (g.get("p2") or 0) + (g.get("p3") or 0) for g in gmv}
    idx = {}
    for l in lojas:
        k = norm(l["name"])
        if not k:
            continue
        atual = idx.get(k)
        if atual is None or total.get(l["id"], 0) > total.get(atual["id"], 0):
            idx[k] = l
    return idx


def enriquecer(aba1, aba3, prev):
    """Devolve (aba1, aba3, meta). Nunca levanta excecao: se o BQ falhar, os
    dados voltam como entraram."""
    meta = {"ok": False, "p3_dias": _dias_p3(), "periodos": {
        "p1": [P1_INI, P1_FIM], "p2": [P2_INI, P2_FIM], "p3": [P3_INI, P3_FIM]}}

    client = _client()
    if client is None:
        return aba1, aba3, meta

    try:
        lojas = _rows(client, SQL_LOJAS, "lojas (dominios com modulo de vendas)")
        gmv = _rows(client, SQL_GMV, "GMV por loja nos 3 periodos")
    except Exception as e:
        print(f"[bq] consulta falhou ({e}) — mantendo os dados como estavam", flush=True)
        return aba1, aba3, meta

    idx = _indice_lojas(lojas, gmv)
    gmv_por_id = {g["id"]: g for g in gmv}
    print(f"[bq] {len(lojas)} lojas, {len(idx)} nomes unicos, {len(gmv)} com GMV", flush=True)

    aba1 = _aba1_entrada(aba1, idx)
    aba3 = _aba3_com_gmv(aba3, idx, gmv_por_id)
    meta["ok"] = True
    return aba1, aba3, meta


def _dias_p3():
    ini = datetime.date.fromisoformat(P3_INI)
    fim = min(datetime.date.today(), datetime.date.fromisoformat(P3_FIM))
    return max(1, (fim - ini).days + 1)


def _aba1_entrada(aba1, idx):
    """Anexa a data de cadastro do dominio (odbc_domains.created_at) como
    `cadastro_bq`, SEM tocar em `entrada`/`dias`.

    Por que nao sobrescreve (medido em 27/07/2026): as duas datas nao sao a
    mesma coisa. `entrada` na planilha e quando a marca entrou na Passagem de
    Bastao; `created_at` e quando o dominio foi criado na Vesti. Murano entrou
    no processo em jan/2026 e e cliente desde 2016 — 9,4 anos de diferenca.
    Trocar uma pela outra faria `dias` virar idade de cadastro e jogaria 22
    marcas para dentro do alerta de 45/60 dias sem reuniao, que e exatamente o
    quadro "marcas para chamar" que o time usa. Dos 28 casos divergentes, 17
    diferem por 1 dia — esses sao fuso (UTC x BRT), nao semantica.

    Para ligar a sobrescrita, ENTRADA_DO_BQ = True (e revisar o alerta antes).
    """
    hoje = datetime.date.today()
    aba1 = [dict(r) for r in aba1]
    casou = difere = 0
    for r in aba1:
        loja = idx.get(norm(r.get("marca")))
        if not loja or not loja.get("criada"):
            continue          # sem cadastro achado: a planilha continua valendo
        casou += 1
        cad = loja["criada"].isoformat()
        r["cadastro_bq"] = cad
        if r.get("entrada") != cad:
            difere += 1
        if ENTRADA_DO_BQ:
            r["entrada"] = cad
            r["dias"] = (hoje - loja["criada"]).days
    modo = "sobrescrevendo entrada" if ENTRADA_DO_BQ else "so anexando cadastro_bq"
    print(f"[bq] aba1: {casou}/{len(aba1)} casaram com o cadastro "
          f"({difere} com data diferente da planilha) — {modo}", flush=True)
    return aba1


def _aba3_com_gmv(aba3, idx, gmv_por_id):
    """GMV dos 3 periodos em toda linha que casar por nome. Quem nao casa fica
    com GMV None (nunca zero). Nao cria linha: o universo e o da planilha."""
    aba3 = [dict(r) for r in aba3]
    casou = 0
    for r in aba3:
        loja = idx.get(norm(r.get("empresa")))
        if not loja:
            r["gmv_p1"] = r["gmv_p2"] = r["gmv_p3"] = None
            r["bq_id"] = None
            continue
        g = gmv_por_id.get(loja["id"], {})
        r["bq_id"] = loja["id"]
        r["gmv_p1"] = g.get("p1")
        r["gmv_p2"] = g.get("p2")
        r["gmv_p3"] = g.get("p3")
        casou += 1
    print(f"[bq] aba3: {casou}/{len(aba3)} com GMV do BQ", flush=True)
    return aba3


if __name__ == "__main__":   # execucao solta = so mostra o que faria
    here = os.path.dirname(os.path.abspath(__file__))
    p = os.path.join(here, "dashboard_data.js")
    txt = open(p, encoding="utf-8").read()
    prev = json.loads(txt[txt.find("{"): txt.rfind("}") + 1])
    a1, a3, meta = enriquecer(prev.get("aba1", []), prev.get("aba3", []), prev)
    print(json.dumps({
        "ok": meta["ok"], "p3_dias": meta["p3_dias"],
        "aba1": len(a1), "aba3": len(a3),
        "aba3_com_gmv": sum(1 for r in a3 if r.get("gmv_p2") is not None),
        "aba1_entrada_do_bq": sum(1 for r in a1 if r.get("entrada_planilha")),
        "entrada_divergente": [
            {"marca": r["marca"], "planilha": r["entrada_planilha"], "bq": r["entrada"]}
            for r in a1 if r.get("entrada_planilha") and r["entrada_planilha"] != r["entrada"]
        ][:10],
    }, ensure_ascii=False, indent=1))
