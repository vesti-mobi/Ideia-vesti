"""
Lê os JSONs gerados por fetch_elisa.py e produz dashboard_data.js com a
constante DADOS consumida pelo frontend.

Calcula primeiras 5 / primeiras 25 vendas:
  - Primeiro mes-calendario em que a marca fez >= N vendas pagas DENTRO do
    proprio mes. A contagem zera na virada do mes enquanto nao bater N.
  - Registra tambem a DATA exata em que bateu (data5Vendas / data25Vendas).

Alertas (vermelho):
  - Cadastrou pedido ha > 30 dias mas ainda nao teve nenhuma venda
  - Acumulou < 5 vendas ate hoje apos > 60 dias do 1o cadastro de pedido
  - VP inativo
"""
from __future__ import annotations

import json
import re
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent


def _load(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))


def _parse_iso_date(s: str | None) -> date | None:
    if not s: return None
    try:
        return datetime.fromisoformat(s.replace("Z", "")).date()
    except ValueError:
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            return None


def _primeiras_n(vendas_por_dia: dict[str, int], n: int) -> tuple[str | None, str | None]:
    """Primeiro mes-calendario em que a marca bateu n vendas pagas DENTRO do
    proprio mes, e a data exata em que bateu.

    Regra (definida pela Laura em 12/08/2026): a contagem comeca no mes de
    entrada e ZERA a cada virada de mes enquanto nao bater n. Assim que bate,
    marca a data e a marca nao e' mais reavaliada (o acumulado depois disso
    segue em qtdVendasMes, mas nao muda esse marco).

    Antes era acumulado desde sempre -- uma marca que fizesse 3 vendas/mes por
    9 meses aparecia como "25 primeiras vendas", o que nao e' o que o time quer.
    """
    por_mes: dict[str, list[tuple[str, int]]] = {}
    for dia, q in vendas_por_dia.items():
        if dia:
            por_mes.setdefault(dia[:7], []).append((dia, int(q or 0)))
    for mes in sorted(por_mes):
        acc = 0
        for dia, q in sorted(por_mes[mes]):
            acc += q
            if acc >= n:
                return mes, dia
    return None, None


# Regra 3: a 1a fatura paga da marca veio tao depois da entrada que so' pode ser
# retorno. GUARDA_PISO evita o falso positivo obvio -- o espelho iugu_invoices so'
# comeca em 2025-01-01, entao TODA marca anterior a isso teria a "1a fatura tardia".
RETORNO_MIN_DIAS = 365
GUARDA_PISO_DIAS = 180


def _dias(a: str, b: str) -> int | None:
    """dias de a ate b (ISO), ou None se alguma data faltar/for invalida."""
    da, db = _parse_iso_date(a), _parse_iso_date(b)
    return (db - da).days if da and db else None


def _norm_marca(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def _tamanho_da_marca(emp: dict, por_cnpj: dict, por_nome: dict) -> dict:
    """Casa a marca com a planilha de tamanho: CNPJ primeiro, nome como queda.

    O CNPJ e' o unico casamento confiavel -- a planilha das vendedoras chama a
    marca de "Chik Bijux" e a base chama de "Lusby Salas suyo".
    """
    cnpj = re.sub(r"\D", "", emp.get("cnpj") or "")
    t = por_cnpj.get(cnpj) if cnpj else None
    casou_por = "cnpj" if t else ""
    if not t:
        t = por_nome.get(_norm_marca(emp.get("name")))
        casou_por = "nome" if t else ""
    if not t:
        return {}
    return {
        # nome casa 1:N -- "NEW MIRE AVIAMENTOS" pegou 2 dominios de CNPJs
        # diferentes na 1a rodada. O painel mostra isso pra ninguem tratar
        # casamento por nome como se fosse certeza.
        "casouPorTamanho": casou_por,
        "unidadesNoTeto": bool(t.get("unidadesNoTeto")),
        "seguidoresDesconhecidos": bool(t.get("seguidoresDesconhecidos")),
        # apify | manual (preenchido na aba Histórico) | planilha
        "origemSeguidores": t.get("origemSeguidores") or "",
        "seguidores": t.get("seguidores"),
        "instagram": t.get("instagram") or "",
        "instagramVerificado": bool(t.get("verificado")),
        "temLojaFisica": t.get("temLojaFisica") or "",
        "qtdUnidades": t.get("qtdUnidades") or 0,
        "lojasEncontradas": t.get("enderecoObs") or "",
        "porte": t.get("porte") or "Indefinido",
        "planoPlanilha": t.get("plano") or "",
        "mensalidadePlanilha": t.get("mensalidade") or 0,
        "dataColetaTamanho": t.get("dataColeta") or "",
    }


def _eventos_ambiente(emp: dict, amb: dict, pag: dict, piso: str,
                      reativ_eventos: list[dict]) -> list[dict]:
    """Eventos de ATIVACAO/REATIVACAO da marca, do mais novo pro mais antigo.

    Regra combinada fechada com a Laura em 13/08/2026. Nenhum sinal sozinho cobria
    a lista das vendedoras, entao sao 4 deteccoes complementares -- mas o painel
    mostra so' 3 rotulos, porque a Laura pediu (13/08) que "voltou a pagar" e
    "religado fora de fatura" aparecam juntos como AMBIENTE RELIGADO: para ela as
    duas coisas sao o mesmo evento de negocio (o ambiente voltou a funcionar), o
    que muda e' so' como a Vesti ficou sabendo. A frase do `detalhe` preserva a
    diferenca linha a linha.

      criacao     - dominio criado = ambiente ATIVADO. Pega Martina Franca e o
                    dominio novo do Surf Center (cliente que volta com cadastro novo).
      retorno     - 1a fatura paga >365d depois da entrada, com guarda do piso do
                    espelho. Pega Lunar Fitwear (entrou em 2023, 1a fatura em 11/08/2026).
      religamento - junta duas deteccoes:
                    a) passou >=46 dias sem pagar e voltou (reativacao_elisa.json,
                       criterio de 12/08) -- Rery, Reve Brand, Chik Bijux, Julia Plus;
                    b) "Ligado?=Sim" na planilha do n8n cuja data NAO casa com fatura
                       paga (ver _casa_com_pagamento em fetch_ambiente.py) -- Surf
                       Center antigo, religado no form manual em 29/07/2026.

    Fica de fora: marca que a vendedora classifica como reativacao mas que pagou em
    dia e nunca foi desligada (caso Gabifit, maior atraso 40 dias) -- isso e'
    classificacao comercial, nao existe no dado.

    `desligamento` entra so' como contexto para a aba (Ligado?=Nao e' confiavel: o
    ramo agendado do n8n so' bloqueia quem tem fatura 11 dias vencida).
    """
    out = []
    entrada = (emp.get("dataEntrada") or "")[:10]

    if entrada:                                                          # 2. criacao
        out.append({"tipo": "ativacao", "origem": "criacao", "data": entrada,
                    "mes": entrada[:7], "detalhe": "ambiente criado"})

    # religamento (a): voltou a pagar depois de pular ciclo
    for ev in reativ_eventos or []:
        quando = (ev.get("voltou") or "")[:10]
        if quando:
            out.append({"tipo": "reativacao", "origem": "religamento", "data": quando,
                        "mes": quando[:7], "dias": ev.get("dias") or 0,
                        "detalhe": f"voltou a pagar apos {ev.get('dias') or 0} dias"})

    primeira = (pag or {}).get("primeira") or ""                         # 3. retorno
    if primeira and entrada:
        desde_entrada = _dias(entrada, primeira)
        depois_do_piso = _dias(piso, primeira) if piso else None
        if (desde_entrada is not None and desde_entrada > RETORNO_MIN_DIAS
                and depois_do_piso is not None and depois_do_piso >= GUARDA_PISO_DIAS):
            out.append({"tipo": "reativacao", "origem": "retorno", "data": primeira,
                        "mes": primeira[:7], "dias": desde_entrada,
                        "detalhe": f"1a fatura {desde_entrada} dias apos a entrada"})

    # religamento (b): religado sem pagamento na data -- form manual do n8n ou CNPJ
    if amb and amb.get("religamentoReal") and amb.get("update"):
        quando = amb["update"][:10]
        if quando != entrada:
            out.append({"tipo": "reativacao", "origem": "religamento", "data": quando,
                        "mes": quando[:7], "detalhe": "religado sem pagamento na data"})

    if amb and amb.get("ligado") is False and amb.get("update"):         # contexto
        quando = amb["update"][:10]
        out.append({"tipo": "desligamento", "origem": "bloqueio", "data": quando,
                    "mes": quando[:7], "detalhe": "ambiente bloqueado"})

    # mesma origem + mesmo dia = um evento so' (ex: religamento que ja entrou por pagamento)
    vistos, unicos = set(), []
    for ev in sorted(out, key=lambda e: e["data"], reverse=True):
        chave = (ev["tipo"], ev["data"])
        if chave in vistos:
            continue
        vistos.add(chave)
        unicos.append(ev)
    return unicos


def _alertas(emp: dict, cad: dict, vp: dict, gmv_emp: dict) -> list[str]:
    out = []
    hoje = date.today()
    primeiro_ped = _parse_iso_date(cad.get("primeiroPedidoCadastrado"))
    primeira_venda = _parse_iso_date((gmv_emp or {}).get("primeiraVenda"))
    if primeiro_ped and not primeira_venda and (hoje - primeiro_ped).days > 30:
        out.append(f"Cadastrou pedidos ha {(hoje-primeiro_ped).days}d e ainda nao vendeu")
    if primeira_venda and (hoje - primeira_venda).days > 60:
        total_vendas = sum((gmv_emp or {}).get("qtdVendasMes", {}).values())
        if total_vendas < 5:
            out.append(f"<5 vendas apos {(hoje-primeira_venda).days}d da 1a venda")
    if not vp.get("temVPAtivo"):
        out.append("VP inativo (sem pedidos nos ultimos 30-60d)")
    if not vp.get("temFreteAtivo"):
        out.append("Sem frete ativo")
    if not cad.get("qtProdutos"):
        out.append("Nenhum produto cadastrado")
    return out


def main():
    empresas  = _load(ROOT / "companies_elisa.json")
    gmv       = _load(ROOT / "gmv_elisa.json")
    cadastros = _load(ROOT / "cadastros_elisa.json")
    vp        = _load(ROOT / "vestipago_elisa.json")
    reativ_p  = ROOT / "reativacao_elisa.json"
    reativ    = _load(reativ_p) if reativ_p.exists() else {}
    links_p   = ROOT / "links_elisa.json"
    links     = _load(links_p) if links_p.exists() else {}
    amb_p     = ROOT / "ambiente_elisa.json"
    ambiente  = _load(amb_p) if amb_p.exists() else {}
    tam_p     = ROOT / "tamanho_marca.json"
    tamanho   = _load(tam_p) if tam_p.exists() else []
    tam_cnpj, tam_nome = {}, {}
    for t in tamanho:
        if t.get("cnpj"):
            tam_cnpj[t["cnpj"]] = t
        if t.get("marca"):
            tam_nome[_norm_marca(t["marca"])] = t
    inad_p    = ROOT / "inadimplentes_elisa.json"
    inad      = _load(inad_p) if inad_p.exists() else {"reguaDias": 15, "dominios": {}}
    inad_dom  = inad.get("dominios") or {}
    inad_regua = int(inad.get("reguaDias") or 15)
    pag_p     = ROOT / "pagamentos_elisa.json"
    pagtos    = _load(pag_p) if pag_p.exists() else {"piso": "", "dominios": {}}
    pag_piso  = pagtos.get("piso") or ""
    pag_dom   = pagtos.get("dominios") or {}

    enriched = []
    meses_set: set[str] = set()
    semanas_set: set[str] = set()

    # GMV/pedidos/links so existem no grao de DOMINIO. Como odbc_companies traz
    # 1 linha por CNPJ, um dominio com filiais gerava N linhas com o MESMO GMV
    # (Diamantes Lingerie aparecia 79x, Alcance 15x) -- inflando KPIs e rankings.
    # Fica so a matriz; as filiais viram contagem + lista de nomes nela.
    filiais_por_dom: dict[str, list[str]] = {}
    for e in empresas:
        if not e.get("isMatriz"):
            filiais_por_dom.setdefault(e["domain_id"], []).append(e.get("name") or "")
    matrizes = [e for e in empresas if e.get("isMatriz")]
    if len(matrizes) != len(empresas):
        print(f"[dedup] {len(empresas)} linhas -> {len(matrizes)} marcas "
              f"({len(empresas)-len(matrizes)} filiais agregadas na matriz)")

    for e in matrizes:
        dom = e["domain_id"]
        g = gmv["empresas"].get(dom, {})
        c = cadastros.get(dom, {})
        v = vp.get(dom, {"temVPAtivo": False, "temFreteAtivo": False})
        meses_set.update(g.get("mensal", {}).keys())
        semanas_set.update(g.get("semanal", {}).keys())
        qtd_vendas_mes = g.get("qtdVendasMes", {})
        qtd_vendas_pagas_mes = g.get("qtdVendasPagasMes", {})
        vendas_pagas_dia = g.get("vendasPagasPorDia", {})
        mes5, data5   = _primeiras_n(vendas_pagas_dia, 5)
        mes25, data25 = _primeiras_n(vendas_pagas_dia, 25)
        filiais = filiais_por_dom.get(dom, [])
        enriched.append({
            **e,
            "qtdFiliais": len(filiais),
            "filiais": filiais,
            "qtProdutos": c.get("qtProdutos", 0),
            "produtosPorMes": c.get("produtosPorMes", {}),
            "primeiroMesCadastro": c.get("primeiroMes", ""),
            "qtProdutos1oMes": c.get("qtProdutos1oMes", 0),
            "primeiroCadastroProduto": c.get("primeiroCadastroProduto", ""),
            "primeiroPedidoCadastrado": c.get("primeiroPedidoCadastrado", ""),
            "primeiraVenda": g.get("primeiraVenda", ""),
            "primeiraVendaPaga": g.get("primeiraVendaPaga", ""),
            "qtdVendasMes": qtd_vendas_mes,
            "qtdVendasPagasMes": qtd_vendas_pagas_mes,
            # mes5/25Vendas usam SOMENTE pedidos pagos (payment_paidAt preenchido)
            # e a contagem zera a cada mes ate bater (ver _primeiras_n)
            "mes5Vendas":  mes5,
            "mes25Vendas": mes25,
            "data5Vendas":  data5 or "",
            "data25Vendas": data25 or "",
            "mensal":   g.get("mensal", {}),
            "semanal":  g.get("semanal", {}),
            "temVPAtivo":     v.get("temVPAtivo", False),
            "temPixAtivo":    v.get("temPixAtivo", False),
            "temCartaoAtivo": v.get("temCartaoAtivo", False),
            "temFreteAtivo":  v.get("temFreteAtivo", False),
            "alertas": _alertas(e, c, v, g),
            "reativacoesPorMes": (reativ.get(dom) or {}).get("reativacoesPorMes", {}),
            "totalReativ": (reativ.get(dom) or {}).get("totalReativ", 0),
            # cada retorno: {mes, ultimoPag, voltou, dias sem pagar}
            "reativEventos": (reativ.get(dom) or {}).get("eventos", []),
            "maiorAusencia": (reativ.get(dom) or {}).get("maiorAusencia", 0),
            "ultimaVolta": (reativ.get(dom) or {}).get("ultimaVolta", ""),
            # Aba Reativacoes: [{tipo, origem, data, mes, detalhe}] -- regra das 4 origens
            "ambienteEventos": _eventos_ambiente(
                e, ambiente.get(dom) or {}, pag_dom.get(dom) or {}, pag_piso,
                (reativ.get(dom) or {}).get("eventos", [])),
            # Tamanho da marca (tamanho_marca.py): seguidores + loja fisica
            **_tamanho_da_marca(e, tam_cnpj, tam_nome),
            "ambienteLigado": (ambiente.get(dom) or {}).get("ligado", None),
            "ambienteUpdate": (ambiente.get(dom) or {}).get("update", ""),
            # Inadimplencia (faturas vencidas e em aberto na Iugu). A regua de
            # dias fica no front -- aqui vai o atraso cru do vencimento MAIS
            # ANTIGO em aberto. Ver SQL_INADIMPLENTES em fetch_elisa_bq.py.
            "faturasVencidas": (inad_dom.get(dom) or {}).get("qtFaturas", 0),
            "valorEmAberto": (inad_dom.get(dom) or {}).get("valorEmAberto", 0.0),
            "diasAtraso": (inad_dom.get(dom) or {}).get("diasAtraso", 0),
            "vencimentoMaisAntigo": (inad_dom.get(dom) or {}).get("vencimentoMaisAntigo", ""),
            "faturasAbertas": (inad_dom.get(dom) or {}).get("faturas", []),
            # subcontas da Iugu de onde vem a divida (sao 15 contas no espelho)
            "subcontasIugu": (inad_dom.get(dom) or {}).get("subcontas", []),
            "linksCompartilhados": (links.get(dom) or {}).get("linksCompartilhados", 0),
            "cliquesTotal": (links.get(dom) or {}).get("cliquesTotal", 0),
            "cliquesPorMes": (links.get(dom) or {}).get("cliquesPorMes", {}),
            "linksPorMes": (links.get(dom) or {}).get("linksPorMes", {}),
            "influenciadores": (links.get(dom) or {}).get("influenciadores", []),
        })

    dados = {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "empresas": enriched,
        "mesesList": sorted(meses_set),
        "semanasList": sorted(semanas_set),
        "reguaInadimplencia": inad_regua,
        # faturas vencidas que nao casaram com nenhuma marca do painel
        "inadSemDominio": inad.get("semDominio") or {"qtFaturas": 0, "valor": 0.0},
        "pendentes": ["Reativacao", "Link compartilhado", "Clicks no link"],
    }
    out = ROOT / "dashboard_data.js"
    out.write_text("const DADOS = " + json.dumps(dados, ensure_ascii=False) + ";\n", encoding="utf-8")
    print(f"[write] {out.name} ({len(enriched)} marcas, {len(meses_set)} meses, {len(semanas_set)} semanas)")


if __name__ == "__main__":
    main()
