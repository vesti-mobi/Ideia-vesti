"""GET /api/plano?parceiro=X&plano=Y → devolve dados do plano cadastrado na Iugu."""
import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from _iugu import (
    buscar_plano,
    freq_label,
    token_para_parceiro,
    valor_cents_do_plano,
)


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
            dados, resp = buscar_plano(token, plano)
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
