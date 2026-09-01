"""GET /api/plano?parceiro=X&plano=Y → devolve dados do plano cadastrado na Iugu."""
import json
import os
import re
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

import requests

BASE_URL = "https://api.iugu.com/v1"

# Trava de valor POR PLANO. O preco real continua vindo da Iugu; este mapa e a
# guarda: se o plano nao estiver exatamente no valor esperado, o link recusa em vez
# de gerar um Pix de valor inesperado (foi o que barrou o plano rascunho de R$0).
# Identifier fora do mapa e recusado — link publico nao gera cobranca arbitraria.
# Para mudar um preco: alterar o plano na Iugu E este mapa (aqui e em gerar-pix.py).
VALORES_PERMITIDOS = {
    # Plano "Starter.1" (R$499/mes). A Iugu gerou identifier diferente em cada
    # subconta porque "starter.1" ja estava em uso na Starter.
    "starter.27": 49900,    # subconta starter
    "starter.1": 49900,     # subconta uemtel
    "vesti_starter": 42900, # plano anterior; mantido para links ja enviados
    "vesti_teste_5": 500,   # plano de teste da recorrencia; semanal
}

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


PAGINA_PLANOS = 100   # tamanho de pagina da Iugu em /plans
MAX_PLANOS = 500      # teto de seguranca da varredura


def _plano_na_listagem(token, identifier):
    """Varre /plans comparando o identifier exato. Usado quando a busca direta falha."""
    start = 0
    while start < MAX_PLANOS:
        r = requests.get(
            f"{BASE_URL}/plans",
            auth=(token, ""),
            params={"limit": PAGINA_PLANOS, "start": start},
            timeout=30,
        )
        if r.status_code >= 400:
            return None
        items = (r.json() or {}).get("items") or []
        for p in items:
            if p.get("identifier") == identifier:
                # Recarrega pelo id (UUID, sem ponto) para ter o mesmo payload
                # completo que /plans/identifier devolveria. Se falhar, o item da
                # listagem ja traz nome, valor e intervalo — serve.
                rid = requests.get(
                    f"{BASE_URL}/plans/{p.get('id')}", auth=(token, ""), timeout=30
                )
                return rid.json() if rid.status_code < 400 else p
        if len(items) < PAGINA_PLANOS:
            return None
        start += PAGINA_PLANOS
    return None


def buscar_plano(token, identifier):
    """Busca o plano pelo identifier, tolerando identifier com ponto.

    A Iugu roteia /plans/identifier/<id> como Rails: o trecho depois do ponto vira
    formato de resposta. Pedir 'starter.1' consulta 'starter' — 404 ou, pior, devolve
    OUTRO plano. Por isso a resposta direta so vale se o identifier voltar identico ao
    pedido; caso contrario cai para a listagem, que compara o identifier exato.
    """
    r = requests.get(
        f"{BASE_URL}/plans/identifier/{identifier}",
        auth=(token, ""),
        timeout=30,
    )
    if r.status_code < 400:
        dados = r.json() or {}
        if dados.get("identifier") == identifier:
            return dados, r
    elif r.status_code in (401, 403):
        return None, r   # token recusado: a listagem falharia igual

    dados = _plano_na_listagem(token, identifier)
    if dados:
        return dados, r
    return None, r


def erro_da_iugu(r):
    """Traduz a falha da Iugu sem mascarar token vencido de plano inexistente."""
    if r is not None and r.status_code in (401, 403):
        return 502, "Token da Iugu recusado (401). O token do parceiro precisa ser renovado."
    return 404, None


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
            dados, r = buscar_plano(token, plano)
        except Exception as e:
            return _send(self, 502, {"erro": f"Falha ao consultar Iugu: {e}"})

        if not dados:
            status, detalhe = erro_da_iugu(r)
            return _send(self, status, {
                "erro": detalhe or f"Plano '{plano}' não encontrado na conta do parceiro."
            })

        esperado = VALORES_PERMITIDOS.get(dados.get("identifier") or plano)
        if esperado is None:
            return _send(self, 422, {
                "erro": f"Plano '{plano}' não está liberado para cobrança por este link."
            })

        valor = valor_cents_do_plano(dados)
        if valor != esperado:
            return _send(self, 422, {
                "erro": (
                    f"Plano '{plano}' está com valor R${valor / 100:.2f} na Iugu, "
                    f"mas este link espera R${esperado / 100:.2f}. "
                    "Ajuste o preço do plano na Iugu ou atualize o link."
                )
            })

        return _send(
            self,
            200,
            {
                "parceiro": parceiro,
                "identifier": dados.get("identifier"),
                "nome": dados.get("name"),
                "valor_cents": esperado,
                "frequencia": freq_label(dados),
                "interval": dados.get("interval"),
                "interval_type": dados.get("interval_type"),
                # Diagnostico: se o plano nao aceitar pix, a assinatura e recusada com
                # "payable_with sao incompativeis com o plano".
                "payable_with": dados.get("payable_with"),
            },
        )
