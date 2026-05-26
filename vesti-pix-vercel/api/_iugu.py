"""Helpers compartilhados entre as funções serverless."""
import json
import os
import re

import requests

BASE_URL = "https://api.iugu.com/v1"

FREQ_LABEL = {
    ("weeks", 1): "Semanal",
    ("months", 1): "Mensal",
    ("months", 3): "Trimestral",
    ("months", 6): "Semestral",
    ("months", 12): "Anual",
    ("years", 1): "Anual",
}

FREQ_TO_AUTOMATIC_PIX = {
    ("weeks", 1): "weekly",
    ("months", 1): "monthly",
    ("months", 3): "quarterly",
    ("months", 6): "semiannually",
    ("months", 12): "yearly",
    ("years", 1): "yearly",
}


def token_para_parceiro(parceiro):
    """Lê o token Iugu do parceiro a partir da env var IUGU_TOKEN_<PARCEIRO>."""
    if not parceiro or not re.match(r"^[a-zA-Z0-9_-]+$", parceiro):
        return None
    key = f"IUGU_TOKEN_{parceiro.upper().replace('-', '_')}"
    return os.environ.get(key)


def buscar_plano(token, identifier):
    r = requests.get(
        f"{BASE_URL}/plans/identifier/{identifier}",
        auth=(token, ""),
        timeout=30,
    )
    if r.status_code >= 400:
        return None, r
    return r.json(), r


def freq_label(plano):
    key = (plano.get("interval_type"), plano.get("interval") or 1)
    return FREQ_LABEL.get(key, f"{plano.get('interval')} {plano.get('interval_type')}")


def freq_automatic_pix(plano):
    key = (plano.get("interval_type"), plano.get("interval") or 1)
    return FREQ_TO_AUTOMATIC_PIX.get(key, "monthly")


def valor_cents_do_plano(plano):
    if plano.get("value_cents"):
        return plano["value_cents"]
    prices = plano.get("prices") or []
    if prices:
        return prices[0].get("value_cents") or 0
    return 0


def resposta_json(payload, status=200):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json; charset=utf-8",
            "Cache-Control": "no-store",
        },
        "body": json.dumps(payload, ensure_ascii=False),
    }
