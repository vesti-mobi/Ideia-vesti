"""GET /api/plano?parceiro=X&plano=Y → devolve dados do plano cadastrado na Iugu."""
import json
import os
import re
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

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


def token_para_parceiro(parceiro):
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


def valor_cents_do_plano(plano):
    if plano.get("value_cents"):
        return plano["value_cents"]
    prices = plano.get("prices") or []
    if prices:
        return prices[0].get("value_cents") or 0
    return 0


def _send(self, status, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    self.send_response(status)
    self.send_header("Content-Type", "application/json; charset=utf-8")
    self.send_header("Cache-Control", "no-store")
    self.send_header("Access-Control-Allow-Origin", "*")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        qs = parse_qs(urlparse(self.path).query)
        parceiro = (qs.get("parceiro") or [""])[0].strip()
        plano = (qs.get("plano") or [""])[0].strip()

        if not parceiro or not plano:
            return _send(self, 400, {"erro": "Parâmetros 'parceiro' e 'plano' são obrigatórios."})

        token = token_para_parceiro(parceiro)
        if not token:
            return _send(self, 404, {"erro": f"Parceiro '{parceiro}' não configurado."})

        try:
            dados, _ = buscar_plano(token, plano)
        except Exception as e:
            return _send(self, 502, {"erro": f"Falha ao consultar Iugu: {e}"})

        if not dados:
            return _send(self, 404, {"erro": f"Plano '{plano}' não encontrado na conta do parceiro."})

        return _send(
            self,
            200,
            {
                "parceiro": parceiro,
                "identifier": dados.get("identifier"),
                "nome": dados.get("name"),
                "valor_cents": valor_cents_do_plano(dados),
                "frequencia": freq_label(dados),
                "interval": dados.get("interval"),
                "interval_type": dados.get("interval_type"),
            },
        )
