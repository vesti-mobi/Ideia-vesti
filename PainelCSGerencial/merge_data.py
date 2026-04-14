"""
Junta todos os JSONs produzidos pelos fetchers e gera dashboard_full_data.js
com os consts que o index.html consome.

Consts gerados:
  COMPANIES_DATA       -> companies_data.json  (fetch_fabric)
  NPS_DATA             -> sheets_data.json.nps (fetch_sheets)
  CSAT_ORACULO_DATA    -> sheets_data.json.csat_oraculo (fetch_sheets)
  CSAT_PLATAFORMA_DATA -> hubspot_data.json (fetch_hubspot) [pode ser []]
  VP_DATA              -> ../NovosVestiPago/dados.js (node build.js) [pode ser {}]
  USERS_DATA           -> users_data.json (fetch_users)
  CS_NAMES             -> derivado de COMPANIES_DATA.anjo + NPS_DATA.anjo
"""

import json
import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
OUT = ROOT / "dashboard_full_data.js"


def load(name: str, default):
    p = ROOT / name
    if not p.exists():
        print(f"[merge] {name} nao existe, usando default {default!r}")
        return default
    return json.loads(p.read_text(encoding="utf-8"))


def build_nps(sheets: dict) -> list[dict]:
    """Formato esperado pelo dashboard JS: {empresa, nota, data, anjo, comentario, dominio,
    nome, telefone, telefone_norm}"""
    out = []
    for r in sheets.get("nps", []):
        out.append({
            "empresa": r.get("empresa", ""),
            "nota": r.get("nota"),
            "data": r.get("data", ""),
            "anjo": r.get("anjo") or None,
            "comentario": r.get("comentario") or None,
            "dominio": int(r["dominio"]) if str(r.get("dominio", "")).isdigit() else r.get("dominio"),
            "nome": r.get("nome", ""),
            "telefone": r.get("telefone", ""),
            "telefone_norm": r.get("telefone_norm", ""),
        })
    return out


def build_csat_oraculo(sheets: dict) -> list[dict]:
    out = []
    for r in sheets.get("csat_oraculo", []):
        out.append({
            "empresa": r.get("empresa", ""),
            "nota": r.get("nota"),
            "mes": r.get("mes", ""),
            "observacao": r.get("observacao", ""),
        })
    return out


import re
from datetime import datetime, timezone


def _extract_num(s) -> float | None:
    """Extrai o primeiro numero de strings tipo '5 (muito satisfeito)' ou '4'."""
    if s is None:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)", str(s))
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None


def _epoch_ms_to_date(ms) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        return ""


def build_csat_plataforma(hubspot: list) -> list[dict]:
    out = []
    skipped_teste = 0
    for sub in hubspot or []:
        if not isinstance(sub, dict):
            continue
        vals = sub.get("values", {})
        empresa = vals.get("nome_da_marca") or vals.get("empresa") or vals.get("company") or ""
        if "teste" in empresa.lower():
            skipped_teste += 1
            continue
        nota_impl = _extract_num(vals.get("nota_implementacao"))
        nota_pessoa = _extract_num(vals.get("nota_implementador"))
        # nota principal = nota_implementacao (CSAT do produto); fallback pessoa
        nota = nota_impl if nota_impl is not None else nota_pessoa
        data_iso = _epoch_ms_to_date(sub.get("submitted_at"))
        out.append({
            "empresa": empresa,
            "nota": nota,
            "data": data_iso,
            "comentario": vals.get("comentario_integracao") or "",
            "nome_respondente": (vals.get("firstname") or "").strip(),
            "email_respondente": vals.get("email") or "",
            "implementador": vals.get("nome_implementador") or "",
            "nota_implementador": nota_pessoa,
            "nota_implementacao": nota_impl,
        })
    if skipped_teste:
        print(f"[csat-plat] {skipped_teste} submissions de teste filtradas")
    return out


def derive_cs_names(companies: list, nps: list) -> list[str]:
    s = set()
    for c in companies:
        a = (c.get("anjo") or "").strip()
        if a:
            s.add(a)
    for r in nps:
        a = (r.get("anjo") or "").strip() if r.get("anjo") else ""
        if a:
            s.add(a)
    return sorted(s)


def load_vp() -> dict:
    """Le NovosVestiPago/dados.js (window.DADOS = {...};) e retorna o dict."""
    p = ROOT.parent / "NovosVestiPago" / "dados.js"
    if not p.exists():
        print(f"[merge] {p.name} nao existe, VP_DATA vazio")
        return {}
    txt = p.read_text(encoding="utf-8").strip()
    if txt.startswith("window.DADOS ="):
        txt = txt[len("window.DADOS ="):].strip()
    if txt.endswith(";"):
        txt = txt[:-1]
    try:
        return json.loads(txt)
    except json.JSONDecodeError as e:
        print(f"[merge] falha ao parsear dados.js do NovosVestiPago: {e}")
        return {}


def main() -> None:
    companies = load("companies_data.json", [])
    sheets = load("sheets_data.json", {"nps": [], "csat_oraculo": []})
    users = load("users_data.json", [])
    hubspot = load("hubspot_data.json", [])
    vp = load_vp()

    nps = build_nps(sheets)
    csat_oraculo = build_csat_oraculo(sheets)
    csat_plataforma = build_csat_plataforma(hubspot)
    csat_plataforma_monthly = sheets.get("csat_plataforma_monthly", []) if isinstance(sheets, dict) else []
    cs_names = derive_cs_names(companies, nps)

    def dump(name: str, data) -> str:
        return f"const {name} = {json.dumps(data, ensure_ascii=False)};\n"

    content = (
        dump("NPS_DATA", nps)
        + dump("CSAT_ORACULO_DATA", csat_oraculo)
        + dump("CSAT_PLATAFORMA_DATA", csat_plataforma)
        + dump("CSAT_PLATAFORMA_MONTHLY", csat_plataforma_monthly)
        + dump("COMPANIES_DATA", companies)
        + dump("USERS_DATA", users)
        + dump("CS_NAMES", cs_names)
        + dump("VP_DATA", vp)
    )
    OUT.write_text(content, encoding="utf-8")
    print(f"[merge] {OUT.name} escrito")
    print(f"  NPS_DATA:             {len(nps)}")
    print(f"  CSAT_ORACULO_DATA:    {len(csat_oraculo)}")
    print(f"  CSAT_PLATAFORMA_DATA: {len(csat_plataforma)}")
    print(f"  CSAT_PLATAFORMA_MONTHLY: {len(csat_plataforma_monthly)}")
    print(f"  COMPANIES_DATA:       {len(companies)}")
    print(f"  USERS_DATA:           {len(users)}")
    print(f"  CS_NAMES:             {len(cs_names)}")
    print(f"  VP_DATA.clientes:     {len(vp.get('clientes', [])) if isinstance(vp, dict) else 0}")


if __name__ == "__main__":
    main()
