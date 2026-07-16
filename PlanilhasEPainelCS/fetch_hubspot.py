"""
Gera real_data.js a partir do HubSpot:
- Reunioes (meetings) feitas pelos owners Luana, Thamiris e Gabriella,
  agrupadas por semana ISO (segunda-feira).
- Negocios fechados: deals closed-won associados a cada empresa que teve
  reuniao; o deal e contado na semana da reuniao mais recente daquela
  empresa+CS anterior ao closedate do deal.

Token: HUBSPOT_TOKEN em ../PainelCSGerencial/.env.local (ou env var).
"""

import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
OUT_JS = ROOT / "real_data.js"
OUT_JSON = ROOT / "hubspot_raw.json"
ENV = ROOT.parent / "PainelCSGerencial" / ".env.local"
BASE = "https://api.hubapi.com"

CS_NAMES = ["Luana", "Thamiris", "Gabriella"]


def load_token() -> str:
    if ENV.exists():
        for line in ENV.read_text(encoding="utf-8").splitlines():
            if line.startswith("HUBSPOT_TOKEN="):
                return line.split("=", 1)[1].strip()
    tok = os.environ.get("HUBSPOT_TOKEN", "").strip()
    if not tok:
        print("ERRO: HUBSPOT_TOKEN nao encontrado.", file=sys.stderr)
        sys.exit(1)
    return tok


def http(method: str, path: str, token: str, body: dict | None = None) -> dict:
    url = path if path.startswith("http") else BASE + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    last_err: Exception | None = None
    # Retry/backoff: no Windows a 2a conexao TLS do processo as vezes e
    # resetada pelo host/AV (WinError 10054) mesmo com token valido.
    for attempt in range(6):
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            # 429/5xx sao transitorios; demais erros HTTP sao fatais
            if e.code == 429 or 500 <= e.code < 600:
                last_err = e
                wait = min(2 ** attempt, 30)
                print(f"[hubspot] {method} {path} -> {e.code}, retry em {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            msg = e.read().decode("utf-8", errors="replace")
            print(f"[hubspot] {method} {path} -> {e.code}: {msg}", file=sys.stderr)
            sys.exit(2)
        except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
            last_err = e
            wait = min(2 ** attempt, 30)
            print(f"[hubspot] {method} {path} conexao falhou ({e}); retry em {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
    print(f"[hubspot] {method} {path} falhou apos retries: {last_err}", file=sys.stderr)
    sys.exit(2)


def resolve_owners(token: str) -> dict[str, str]:
    """Retorna {owner_id: nome_curto} para Luana/Thamiris/Gabriella."""
    after = None
    owners: dict[str, str] = {}
    while True:
        q = "?limit=100"
        if after:
            q += f"&after={after}"
        body = http("GET", f"/crm/v3/owners{q}", token)
        for o in body.get("results", []):
            first = (o.get("firstName") or "").strip()
            last = (o.get("lastName") or "").strip()
            full = f"{first} {last}".strip()
            for cs in CS_NAMES:
                if first.lower().startswith(cs.lower()) or cs.lower() in full.lower():
                    owners[str(o.get("id"))] = cs
        after = (body.get("paging") or {}).get("next", {}).get("after")
        if not after:
            break
    missing = [cs for cs in CS_NAMES if cs not in owners.values()]
    if missing:
        print(f"AVISO: owners nao encontrados para: {missing}", file=sys.stderr)
    print(f"[owners] {owners}")
    return owners


def search_paged(token: str, object_type: str, payload: dict) -> list[dict]:
    """Itera /search com paginacao por 'after'."""
    out: list[dict] = []
    after = None
    while True:
        p = dict(payload)
        if after:
            p["after"] = after
        body = http("POST", f"/crm/v3/objects/{object_type}/search", token, p)
        out.extend(body.get("results", []))
        after = (body.get("paging") or {}).get("next", {}).get("after")
        if not after:
            break
        # HubSpot search limita a 10k resultados; protege loop
        if len(out) >= 10000:
            break
    return out


def fetch_meetings(token: str, owner_ids: list[str]) -> list[dict]:
    """Reunioes dos owners-alvo, com empresa associada."""
    payload = {
        "limit": 100,
        "sorts": [{"propertyName": "hs_meeting_start_time", "direction": "DESCENDING"}],
        "properties": ["hs_meeting_start_time", "hubspot_owner_id", "hs_meeting_title"],
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "hubspot_owner_id", "operator": "IN", "values": owner_ids},
                    {"propertyName": "hs_meeting_start_time", "operator": "HAS_PROPERTY"},
                ]
            }
        ],
    }
    rows = search_paged(token, "meetings", payload)
    print(f"[meetings] {len(rows)} encontradas")
    # Busca empresa associada em lote
    ids = [r["id"] for r in rows]
    assoc_map: dict[str, str | None] = {}
    for i in range(0, len(ids), 100):
        chunk = ids[i : i + 100]
        body = http(
            "POST",
            "/crm/v4/associations/meetings/companies/batch/read",
            token,
            {"inputs": [{"id": x} for x in chunk]},
        )
        for r in body.get("results", []):
            mid = str(r.get("from", {}).get("id"))
            tos = r.get("to") or []
            assoc_map[mid] = str(tos[0]["toObjectId"]) if tos else None
    for r in rows:
        r["_company_id"] = assoc_map.get(r["id"])
    return rows


def fetch_closed_won_deals(token: str) -> list[dict]:
    payload = {
        "limit": 100,
        "properties": ["dealstage", "closedate", "hs_is_closed_won", "dealname"],
        "sorts": [{"propertyName": "closedate", "direction": "DESCENDING"}],
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"},
                    {"propertyName": "closedate", "operator": "HAS_PROPERTY"},
                ]
            }
        ],
    }
    rows = search_paged(token, "deals", payload)
    print(f"[deals] {len(rows)} closed-won")
    ids = [r["id"] for r in rows]
    assoc_map: dict[str, str | None] = {}
    for i in range(0, len(ids), 100):
        chunk = ids[i : i + 100]
        body = http(
            "POST",
            "/crm/v4/associations/deals/companies/batch/read",
            token,
            {"inputs": [{"id": x} for x in chunk]},
        )
        for r in body.get("results", []):
            did = str(r.get("from", {}).get("id"))
            tos = r.get("to") or []
            assoc_map[did] = str(tos[0]["toObjectId"]) if tos else None
    for r in rows:
        r["_company_id"] = assoc_map.get(r["id"])
    return rows


def iso_monday(dt: datetime) -> str:
    """Retorna a segunda-feira (ISO) da semana de dt como yyyy-mm-dd."""
    d = dt.astimezone(timezone.utc).date()
    return (d - timedelta(days=d.weekday())).isoformat()


def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        # HubSpot retorna ISO 8601 com Z
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def main() -> None:
    token = load_token()
    owners = resolve_owners(token)
    if not owners:
        print("ERRO: nenhum owner alvo encontrado no HubSpot.", file=sys.stderr)
        sys.exit(3)

    meetings = fetch_meetings(token, list(owners.keys()))
    deals = fetch_closed_won_deals(token)

    # Coleta semanas existentes nas reunioes
    weeks: set[str] = set()
    # reunioes[cs][week] = count
    reu: dict[str, dict[str, int]] = {cs: {} for cs in CS_NAMES}
    # indexa reunioes por (cs, company_id) ordenadas por data ASC -> [(dt, week)]
    idx: dict[tuple[str, str], list[tuple[datetime, str]]] = {}

    for m in meetings:
        props = m.get("properties", {})
        dt = parse_dt(props.get("hs_meeting_start_time"))
        if not dt:
            continue
        cs = owners.get(str(props.get("hubspot_owner_id")))
        if not cs:
            continue
        wk = iso_monday(dt)
        weeks.add(wk)
        reu[cs][wk] = reu[cs].get(wk, 0) + 1
        cid = m.get("_company_id")
        if cid:
            idx.setdefault((cs, cid), []).append((dt, wk))

    for k in idx:
        idx[k].sort(key=lambda x: x[0])

    neg: dict[str, dict[str, int]] = {cs: {} for cs in CS_NAMES}
    deals_atribuidos = 0
    deals_sem_reuniao = 0
    for d in deals:
        cid = d.get("_company_id")
        if not cid:
            continue
        props = d.get("properties", {})
        close_dt = parse_dt(props.get("closedate"))
        if not close_dt:
            continue
        # Acha a CS+reuniao mais recente daquela empresa antes do closedate
        candidato: tuple[datetime, str, str] | None = None
        for cs in CS_NAMES:
            lst = idx.get((cs, cid))
            if not lst:
                continue
            # Maior dt <= close_dt
            for dt, wk in reversed(lst):
                if dt <= close_dt:
                    if not candidato or dt > candidato[0]:
                        candidato = (dt, wk, cs)
                    break
        if not candidato:
            deals_sem_reuniao += 1
            continue
        _, wk, cs = candidato
        neg[cs][wk] = neg[cs].get(wk, 0) + 1
        weeks.add(wk)
        deals_atribuidos += 1

    print(f"[deals] atribuidos={deals_atribuidos} sem_reuniao_anterior={deals_sem_reuniao}")

    weeks_sorted = sorted(weeks)
    data = {
        "weeks": weeks_sorted,
        "reunioes": {cs: [reu[cs].get(w, 0) for w in weeks_sorted] for cs in CS_NAMES},
        "negocios": {cs: [neg[cs].get(w, 0) for w in weeks_sorted] for cs in CS_NAMES},
    }

    js = "// Gerado por fetch_hubspot.py - fonte: HubSpot\nvar DATA = " + json.dumps(
        data, ensure_ascii=False, indent=2
    ) + ";\n"
    OUT_JS.write_text(js, encoding="utf-8")
    OUT_JSON.write_text(
        json.dumps(
            {"owners": owners, "n_meetings": len(meetings), "n_deals_won": len(deals)},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[write] {OUT_JS.name} ({len(weeks_sorted)} semanas)")


if __name__ == "__main__":
    main()
