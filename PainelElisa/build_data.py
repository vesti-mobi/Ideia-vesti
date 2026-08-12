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
            # cada fatura paga em atraso: {mes, venc, voltou, dias}
            "reativEventos": (reativ.get(dom) or {}).get("eventos", []),
            "maiorAtraso": (reativ.get(dom) or {}).get("maiorAtraso", 0),
            "ultimaVolta": (reativ.get(dom) or {}).get("ultimaVolta", ""),
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
        "pendentes": ["Reativacao", "Link compartilhado", "Clicks no link"],
    }
    out = ROOT / "dashboard_data.js"
    out.write_text("const DADOS = " + json.dumps(dados, ensure_ascii=False) + ";\n", encoding="utf-8")
    print(f"[write] {out.name} ({len(enriched)} marcas, {len(meses_set)} meses, {len(semanas_set)} semanas)")


if __name__ == "__main__":
    main()
