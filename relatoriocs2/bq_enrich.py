# -*- coding: utf-8 -*-
"""
Enriquecimento do relatoriocs2 via BigQuery (roda 1x/dia junto do fetch_data.py).

Faz quatro coisas:
  1. ABA 1 - toda LOJA nova (dominio com modulo de vendas) cadastrada a partir
     de DATA_CORTE_NOVAS vira uma linha em branco na Passagem de Bastao.
  2. ABA 1 - a coluna "25+ / Ult. marco" (data25) e a data em que a marca bateu
     o 25o PEDIDO PAGO acumulado, medida no BQ.
  3. ABA 3 - o GMV das empresas vem do BQ em tres periodos, todo dia.
  4. ABA 3 - loja nova tambem entra aqui, com o CS do anjo quando ele e um dos
     tres CS do ranking.

DECISOES QUE VALEM A PENA SABER (medidas no BQ em 24 e 27/07/2026):

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

  * Linha adicionada aqui NUNCA e removida depois. As linhas geradas ficam
    marcadas com origem="bq" e sao relidas do dashboard_data.js anterior a cada
    execucao. Sem isso, mudar o criterio de entrada (como aconteceu em 28/07,
    da janela movel de 30 dias para a data fixa) apagaria linhas ja no painel,
    levando junto o que o CS tivesse preenchido nelas.

  * data25 = 25o pedido PAGO acumulado, e vem SO do BQ (28/07/2026). Medido em
    27/07 contra as 19 marcas que tinham a data preenchida na planilha: a data
    digitada vem SEMPRE depois da medida, com lag mediano de 19 dias (34 se
    contar todos os status) — a planilha registrava quando a CS percebeu o
    marco, nao quando ele aconteceu. Como a coluna deixou de ser preenchida a
    mao, o valor da planilha nao e mais herdado: fica arquivado em
    data25_planilha e a celula nasce vazia ate o BQ medir o marco. Dois erros
    de ano sumiram sozinhos com isso (Baubaki 2026-11-03, Elas charmosa
    2026-11-18, ambas datas no futuro); antes ja tinha aparecido o mesmo com
    Dona Charme, 2026-11-11 quando o marco real foi 2025-11-05.

    Excecao: quem NAO casa por nome com o BQ tambem fica vazio. E o unico ponto
    do modulo onde ausencia de match apaga dado — vale aqui porque a alternativa
    e exibir para sempre um numero que ninguem mais atualiza. Para corrigir um
    caso especifico, edite a celula no painel: o overlay vence sobre o BQ.

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
MARCO_PEDIDOS = 25             # o "25+" da aba 1

# Loja com cadastro a partir daqui vira linha nova nas abas 1 e 3. E uma data
# FIXA, nao uma janela movel (era "ultimos 30 dias" ate 28/07/2026): o roster
# pedido pela Laura e "as marcas da planilha + as cadastradas a partir de
# 24/07". Com janela movel o corte andava sozinho todo dia e o painel passava a
# depender de quando rodou. As linhas que entraram antes desta data pela regra
# antiga continuam no painel — foram mantidas de proposito (decisao de
# 28/07/2026); a data vale daqui pra frente.
DATA_CORTE_NOVAS = "2026-07-24"

# A data de entrada da aba 1 vem do BQ? Desligado de proposito — veja o porque
# em _aba1(): `entrada` (passagem de bastao) e `created_at` (cadastro do
# dominio) sao grandezas diferentes, e trocar quebra o alerta de 45/60 dias.
ENTRADA_DO_BQ = False

P1_INI, P1_FIM = "2025-07-01", "2025-12-31"   # piso do lake: nao existe jun/2025
P2_INI, P2_FIM = "2026-01-01", "2026-06-30"
P3_INI = "2026-07-01"
P3_FIM = "2026-12-31"

# anjo (odbc_angels.name) -> como o CS aparece em cada aba
CS_ABA1 = {
    "luana coutinho": "Luana",
    "thamiris ribeiro": "Thamiris",
    "gabriella busto": "Gabriella Busto",
    "elisa marques": "Elisa",
    "alexia oliveira": "Alexia",
    "tatiane ayres": "Tatiane",
}
CS_ABA3 = {
    "gabriella busto": "Busto",
    "luana coutinho": "Luana",
    "thamiris ribeiro": "Thamiris",
}


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


SQL_MARCO25 = f"""
SELECT id, MIN(CASE WHEN n = {MARCO_PEDIDOS} THEN dia END) marco, MAX(n) pedidos
FROM (
  SELECT domainId id,
         DATE(CAST(settings_createdAt AS TIMESTAMP)) dia,
         ROW_NUMBER() OVER (PARTITION BY domainId
                            ORDER BY CAST(settings_createdAt AS TIMESTAMP)) n
  FROM {DS}.MongoDB_Pedidos_Geral
  WHERE settings_createdAt IS NOT NULL
    AND payment_isPaid = 'True'
)
GROUP BY id
"""


def _cs_de(anjo, mapa):
    return mapa.get(norm_nome_anjo(anjo), "")


def norm_nome_anjo(a):
    return re.sub(r"\s+", " ", str(a or "").strip().lower())


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
        marco = _rows(client, SQL_MARCO25, f"data do {MARCO_PEDIDOS}o pedido pago")
    except Exception as e:
        print(f"[bq] consulta falhou ({e}) — mantendo os dados como estavam", flush=True)
        return aba1, aba3, meta

    idx = _indice_lojas(lojas, gmv)
    gmv_por_id = {g["id"]: g for g in gmv}
    marco_por_id = {m["id"]: m for m in marco}
    print(f"[bq] {len(lojas)} lojas, {len(idx)} nomes unicos, {len(gmv)} com GMV", flush=True)

    corte = datetime.date.fromisoformat(DATA_CORTE_NOVAS)
    novas = [l for l in lojas if l.get("criada") and l["criada"] >= corte]
    print(f"[bq] {len(novas)} loja(s) com cadastro a partir de {corte}", flush=True)

    aba1 = _aba1(aba1, novas, prev, idx, marco_por_id)
    aba3 = _aba3_com_gmv(aba3, novas, prev, idx, gmv_por_id)
    meta["ok"] = True
    meta["marco_pedidos"] = MARCO_PEDIDOS
    return aba1, aba3, meta


def _dias_p3():
    ini = datetime.date.fromisoformat(P3_INI)
    fim = min(datetime.date.today(), datetime.date.fromisoformat(P3_FIM))
    return max(1, (fim - ini).days + 1)


def _aba1(aba1, novas, prev, idx, marco_por_id):
    """Enriquece a aba 1 e acrescenta uma linha em branco por loja nova.

    Enriquecimento:
      * data25 <- data do 25o pedido pago, so do BQ. A da planilha e arquivada
        em data25_planilha e nao aparece mais no painel (a coluna deixou de ser
        preenchida a mao). Sem marco medido, a celula fica vazia.
      * cadastro_bq <- created_at do dominio, em quem casa por nome. NAO
        substitui `entrada` e nao e exibido no painel (decisao de 28/07/2026);
        fica no dado para consulta. Ver abaixo o porque de nao substituir.

    Linhas ja geradas antes (origem=bq no dashboard anterior) sao preservadas,
    inclusive as 8 que entraram com cadastro anterior a DATA_CORTE_NOVAS.

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

    # ORDEM IMPORTA: cria/repoe as linhas ANTES de enriquecer, senao a linha de
    # loja nova (que volta do build anterior a cada dia) nunca passaria pelo
    # calculo do marco — entraria sem 25 pedidos e ficaria sem data pra sempre.
    aba1, add, mantidas = _aba1_novas(aba1, novas, prev)

    casou = com_marco = limpas = 0
    for r in aba1:
        # A coluna 25+ passou a ser 100% do BQ (decisao da Laura, 28/07/2026:
        # "a coluna nao vai mais ser preenchida manualmente"). O que a CS
        # digitou fica arquivado em data25_planilha e some do painel; sem marco
        # medido a celula fica vazia, inclusive em quem nao casa por nome —
        # nao ha de onde tirar a data, e o valor herdado so envelhecia. Correcao
        # pontual continua possivel: o overlay de edicao vence sobre este campo.
        # `not in` e nao `not r.get(...)`: a chave e criada uma unica vez, mesmo
        # valendo None. Se testasse o valor, a linha voltaria a ser "arquivavel"
        # todo dia — e no dia em que o BQ nao devolvesse o marco, a data que ele
        # mesmo mediu seria arquivada como se a CS a tivesse digitado.
        if "data25_planilha" not in r:
            r["data25_planilha"] = r.get("data25")
            if r["data25_planilha"]:
                limpas += 1   # so conta arquivamento de verdade: depois que a
                              # chave existe, o que se apaga aqui e a data que o
                              # proprio BQ escreveu na execucao anterior
        r["data25"] = None

        loja = idx.get(norm(r.get("marca")))
        if not loja:
            continue          # sem cadastro achado: o resto da planilha vale
        casou += 1
        if loja.get("criada"):
            r["cadastro_bq"] = loja["criada"].isoformat()
            if ENTRADA_DO_BQ:
                r["entrada"] = loja["criada"].isoformat()
                r["dias"] = (hoje - loja["criada"]).days
        m = marco_por_id.get(loja["id"], {})
        if m.get("marco"):
            r["data25"] = m["marco"].isoformat()
            com_marco += 1
        if m.get("pedidos") is not None:
            # sem 25 pedidos ainda: sem data, mas registra quantos ja tem
            r["pedidos_pagos"] = m.get("pedidos")

    arquivo = f", {limpas} data(s) da planilha arquivada(s)" if limpas else ""
    print(f"[bq] aba1: {casou} casaram com o cadastro, {com_marco} com data de "
          f"{MARCO_PEDIDOS}o pedido pago{arquivo}; +{add} loja(s) nova(s) "
          f"(mantidas {mantidas} de execucoes anteriores)", flush=True)
    return aba1


def _aba1_novas(aba1, novas, prev):
    """Acrescenta linha em branco por loja nova; repoe as geradas antes.

    Nao preenche data25/pedidos_pagos: quem faz isso e o loop de enriquecimento
    em _aba1(), que roda depois e passa por TODAS as linhas — inclusive estas.
    """
    vistos = {norm(r.get("marca")) for r in aba1}
    antigas_bq = [r for r in prev.get("aba1", []) if r.get("origem") == "bq"]
    for r in antigas_bq:
        if norm(r.get("marca")) not in vistos:
            aba1.append(dict(r))
            vistos.add(norm(r.get("marca")))

    hoje = datetime.date.today()
    add = 0
    for l in novas:
        nome = str(l["name"]).strip()
        if not nome or norm(nome) in vistos:
            continue
        vistos.add(norm(nome))
        aba1.append({
            "marca": nome,
            "entrada": l["criada"].isoformat(),
            "dias": (hoje - l["criada"]).days,
            "implementador": "",
            "cs": _cs_de(l.get("anjo"), CS_ABA1),
            "data25": None,
            "cadastro_bq": l["criada"].isoformat(),
            "obs_orig": "",
            "status": "sem_reuniao",
            "origem": "bq",
        })
        add += 1
    return aba1, add, len(antigas_bq)


def _aba3_com_gmv(aba3, novas, prev, idx, gmv_por_id):
    """GMV dos 3 periodos em toda linha que casar por nome; loja nova entra no
    fim. Quem nao casa fica com GMV None (nunca zero)."""
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

    vistos = {norm(r.get("empresa")) for r in aba3}
    antigas_bq = [r for r in prev.get("aba3", []) if r.get("origem") == "bq"]
    for r in antigas_bq:
        if norm(r.get("empresa")) not in vistos:
            loja = idx.get(norm(r.get("empresa")))
            g = gmv_por_id.get(loja["id"], {}) if loja else {}
            r = dict(r)
            r["gmv_p1"], r["gmv_p2"], r["gmv_p3"] = g.get("p1"), g.get("p2"), g.get("p3")
            aba3.append(r)
            vistos.add(norm(r.get("empresa")))

    add = 0
    for l in novas:
        nome = str(l["name"]).strip()
        if not nome or norm(nome) in vistos:
            continue
        vistos.add(norm(nome))
        g = gmv_por_id.get(l["id"], {})
        aba3.append({
            "cs_tab": _cs_de(l.get("anjo"), CS_ABA3),
            "empresa": nome,
            "color": "",
            "cs": _cs_de(l.get("anjo"), CS_ABA1),
            "plano": "",
            "gmv_p1": g.get("p1"), "gmv_p2": g.get("p2"), "gmv_p3": g.get("p3"),
            "bq_id": l["id"],
            "mensalidade": None, "tino": None, "vestipago": None,
            "obs_orig": "", "origem": "bq",
        })
        add += 1
    print(f"[bq] aba3: {casou}/{len(aba3)} com GMV do BQ, +{add} empresa(s) nova(s)", flush=True)
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
        "novas_aba1": [r["marca"] for r in a1 if r.get("origem") == "bq"][:10],
        "aba1_com_data25_do_bq": sum(1 for r in a1 if r.get("data25_planilha") or
                                     (r.get("data25") and r.get("pedidos_pagos"))),
        "data25_corrigida": [
            {"marca": r["marca"], "planilha": r["data25_planilha"], "bq": r["data25"]}
            for r in a1 if r.get("data25_planilha") and r["data25_planilha"] != r["data25"]
        ][:8],
    }, ensure_ascii=False, indent=1))
