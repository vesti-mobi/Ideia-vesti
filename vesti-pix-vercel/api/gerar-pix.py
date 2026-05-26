"""POST /api/gerar-pix → cria customer (se preciso) + subscription com automatic_pix jornada 3."""
import json
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler

import requests

from _iugu import (
    BASE_URL,
    buscar_plano,
    freq_automatic_pix,
    token_para_parceiro,
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


def _digits(s):
    return "".join(ch for ch in (s or "") if ch.isdigit())


def _buscar_customer_por_cpf(token, cpf):
    r = requests.get(
        f"{BASE_URL}/customers",
        auth=(token, ""),
        params={"query": cpf, "limit": 20},
        timeout=30,
    )
    if r.status_code >= 400:
        return None
    for c in r.json().get("items") or []:
        if _digits(c.get("cpf_cnpj")) == cpf:
            return c.get("id")
    return None


def _criar_customer(token, nome, email, cpf):
    r = requests.post(
        f"{BASE_URL}/customers",
        auth=(token, ""),
        json={"name": nome, "email": email, "cpf_cnpj": cpf},
        timeout=30,
    )
    if r.status_code >= 400:
        return None, r
    return r.json().get("id"), r


def _criar_assinatura(token, customer_id, plano, frequencia_ap, contract_number):
    payload = {
        "customer_id": customer_id,
        "plan_identifier": plano,
        "only_on_charge_success": False,
        "payable_with": "pix",
        "automatic_pix": {
            "journey": 3,
            "frequency": frequencia_ap,
            "recurrence_beginning": (date.today() + timedelta(days=1)).isoformat(),
            "contract_number": contract_number[:35],
        },
    }
    r = requests.post(
        f"{BASE_URL}/subscriptions",
        auth=(token, ""),
        json=payload,
        timeout=30,
    )
    return r, payload


def _consultar_fatura(token, invoice_id):
    r = requests.get(
        f"{BASE_URL}/invoices/{invoice_id}",
        auth=(token, ""),
        timeout=30,
    )
    if r.status_code >= 400:
        return None
    return r.json()


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return _send(self, 400, {"erro": "JSON inválido."})

        parceiro = (body.get("parceiro") or "").strip()
        plano_id = (body.get("plano") or "").strip()
        nome = (body.get("nome") or "").strip()
        email = (body.get("email") or "").strip()
        cpf = _digits(body.get("cpf"))

        if not (parceiro and plano_id and nome and email and cpf):
            return _send(self, 400, {"erro": "Preencha parceiro, plano, nome, email e CPF."})

        token = token_para_parceiro(parceiro)
        if not token:
            return _send(self, 404, {"erro": f"Parceiro '{parceiro}' não configurado."})

        try:
            plano, _ = buscar_plano(token, plano_id)
        except Exception as e:
            return _send(self, 502, {"erro": f"Falha ao consultar plano: {e}"})
        if not plano:
            return _send(self, 404, {"erro": f"Plano '{plano_id}' não encontrado."})

        try:
            customer_id = _buscar_customer_por_cpf(token, cpf)
            if not customer_id:
                customer_id, r_new = _criar_customer(token, nome, email, cpf)
                if not customer_id:
                    return _send(self, 502, {
                        "erro": "Não foi possível criar cliente na Iugu.",
                        "detalhe": r_new.text if r_new is not None else None,
                    })
        except requests.RequestException as e:
            return _send(self, 502, {"erro": f"Erro de rede ao consultar/criar cliente: {e}"})

        try:
            r_sub, _ = _criar_assinatura(
                token,
                customer_id,
                plano_id,
                freq_automatic_pix(plano),
                f"CTR-{cpf}",
            )
        except requests.RequestException as e:
            return _send(self, 502, {"erro": f"Erro de rede ao criar assinatura: {e}"})

        if r_sub.status_code >= 400:
            try:
                detalhe = r_sub.json()
            except Exception:
                detalhe = r_sub.text
            return _send(self, 502, {"erro": "Erro ao criar assinatura.", "detalhe": detalhe})

        sub = r_sub.json()
        invoice_id = (
            (sub.get("recent_invoices") or [{}])[0].get("id")
            or sub.get("active_invoice_id")
        )

        invoice = _consultar_fatura(token, invoice_id) if invoice_id else None
        pix = (invoice or {}).get("pix") or {}

        return _send(self, 200, {
            "customer_id": customer_id,
            "subscription_id": sub.get("id"),
            "invoice_id": (invoice or {}).get("id") or invoice_id,
            "status": (invoice or {}).get("status"),
            "valor_cents": (invoice or {}).get("total_cents"),
            "qrcode": pix.get("qrcode"),
            "qrcode_text": pix.get("qrcode_text"),
            "secure_url": (invoice or {}).get("secure_url"),
        })
