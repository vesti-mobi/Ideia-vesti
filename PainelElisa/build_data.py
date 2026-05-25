"""
Lê os JSONs gerados por fetch_elisa.py e produz dashboard_data.js com a
constante DADOS consumida pelo frontend.

Calcula primeiras 5 / primeiras 25 vendas:
  - Para cada marca, encontrar o mes em que ela acumulou (cumulativo) >= 5 vendas
    e >= 25 vendas pela primeira vez.
  - Marcas com >= N vendas em UM unico mes ja entram naquele mes.

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


def _primeiras_n(meses_qtd: dict[str, int], n: int) -> str | None:
    """Retorna o mes (YYYY-MM) em que o acumulado de vendas atinge n."""
    acc = 0
    for mes in sorted(meses_qtd.keys()):
        acc += meses_qtd[mes]
        if acc >= n:
            return mes
    return None


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

    for e in empresas:
        dom = e["domain_id"]
        g = gmv["empresas"].get(dom, {})
        c = cadastros.get(dom, {})
        v = vp.get(dom, {"temVPAtivo": False, "temFreteAtivo": False})
        meses_set.update(g.get("mensal", {}).keys())
        semanas_set.update(g.get("semanal", {}).keys())
        qtd_vendas_mes = g.get("qtdVendasMes", {})
        enriched.append({
            **e,
            "qtProdutos": c.get("qtProdutos", 0),
            "produtosPorMes": c.get("produtosPorMes", {}),
            "primeiroMesCadastro": c.get("primeiroMes", ""),
            "qtProdutos1oMes": c.get("qtProdutos1oMes", 0),
            "primeiroCadastroProduto": c.get("primeiroCadastroProduto", ""),
            "primeiroPedidoCadastrado": c.get("primeiroPedidoCadastrado", ""),
            "primeiraVenda": g.get("primeiraVenda", ""),
            "qtdVendasMes": qtd_vendas_mes,
            "mes5Vendas":  _primeiras_n(qtd_vendas_mes, 5),
            "mes25Vendas": _primeiras_n(qtd_vendas_mes, 25),
            "mensal":   g.get("mensal", {}),
            "semanal":  g.get("semanal", {}),
            "temVPAtivo":     v.get("temVPAtivo", False),
            "temFreteAtivo":  v.get("temFreteAtivo", False),
            "alertas": _alertas(e, c, v, g),
            "reativacoesPorMes": (reativ.get(dom) or {}).get("reativacoesPorMes", {}),
            "totalReativ": (reativ.get(dom) or {}).get("totalReativ", 0),
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
