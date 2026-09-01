"""POST /api/gerar-pix → cria customer (se preciso) + subscription com automatic_pix jornada 3."""
import calendar
import json
import os
import re
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler

import requests

BASE_URL = "https://api.iugu.com/v1"

# Trava de valor por plano: ver comentario em plano.py. Mudou o preco -> muda nos dois.
VALORES_PERMITIDOS = {
    # Plano "Starter.1" (R$499/mes). A Iugu gerou identifier diferente em cada
    # subconta porque "starter.1" ja estava em uso na Starter.
    "starter.27": 49900,    # subconta starter
    "starter.1": 49900,     # subconta uemtel
    "vesti_starter": 42900, # plano anterior; mantido para links ja enviados
    "vesti_teste_5": 500,   # plano de teste da recorrencia; semanal
}

# Valores literais aceitos pela Iugu no automatic_pix.frequency:
# weekly | monthly | quarterly | semiannual | annual.
# Antes estava "semiannually" e "yearly", que nao existem — so nao quebrou porque
# o plano vendido hoje e mensal. A frequencia tem que bater com o interval do plano.
FREQ_TO_AUTOMATIC_PIX = {
    ("weeks", 1): "weekly",
    ("months", 1): "monthly",
    ("months", 3): "quarterly",
    ("months", 6): "semiannual",
    ("months", 12): "annual",
    ("years", 1): "annual",
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


def freq_automatic_pix(plano):
    key = (plano.get("interval_type"), plano.get("interval") or 1)
    return FREQ_TO_AUTOMATIC_PIX.get(key, "monthly")


def _mesmo_dia_em_meses(d, meses):
    """d + N meses, encurtando o dia quando o mes de destino for mais curto (31/01 -> 28/02)."""
    ano = d.year + (d.month - 1 + meses) // 12
    mes = (d.month - 1 + meses) % 12 + 1
    return date(ano, mes, min(d.day, calendar.monthrange(ano, mes)[1]))


def inicio_recorrencia(hoje, plano):
    """Data da primeira cobranca automatica: um ciclo depois da fatura inicial.

    Sem expires_at, a Iugu vence a primeira fatura HOJE e ancora o ciclo nesse dia
    ("Define a data da primeira cobranca. As cobrancas seguintes sao calculadas
    automaticamente a partir dessa data"). Mandando hoje+1 dia como inicio da
    recorrencia, o banco registrava a autorizacao para o dia seguinte e ficava um dia
    a frente do ciclo — fatura no dia 12, autorizacao no dia 13. Alinhando aqui, o que
    o cliente ve no app do banco bate com o dia em que a fatura nasce.

    A doc exige apenas que seja data futura; com plano mensal ou maior isso e sempre
    verdade, e para plano semanal cai 7 dias a frente.
    """
    n = plano.get("interval") or 1
    tipo = plano.get("interval_type")
    if tipo == "weeks":
        return hoje + timedelta(weeks=n)
    if tipo == "years":
        return _mesmo_dia_em_meses(hoje, 12 * n)
    return _mesmo_dia_em_meses(hoje, n)


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


def _telefone(whatsapp):
    """Separa DDD do numero, como a Iugu espera (phone_prefix + phone)."""
    d = _digits(whatsapp)
    if len(d) >= 10:
        return d[:2], d[2:]
    return None, d or None


def _criar_customer(token, nome, email, doc, marca, razao, whatsapp):
    # Convencao ja usada nas subcontas: o name do cliente e "Marca = RAZAO SOCIAL".
    # Mantida para o cadastro novo nao destoar do que ja existe la.
    nome_iugu = f"{marca} = {razao}" if marca and razao else (marca or razao or nome)
    ddd, numero = _telefone(whatsapp)
    payload = {
        "name": nome_iugu,
        "email": email,
        "cpf_cnpj": doc,
        "custom_variables": [
            {"name": "marca", "value": marca},
            {"name": "razao_social", "value": razao},
            {"name": "responsavel", "value": nome},
            {"name": "whatsapp", "value": whatsapp},
        ],
    }
    if numero:
        payload["phone"] = numero
    if ddd:
        payload["phone_prefix"] = ddd

    r = requests.post(f"{BASE_URL}/customers", auth=(token, ""), json=payload, timeout=30)
    if r.status_code >= 400:
        return None, r
    return r.json().get("id"), r


def _criar_assinatura(token, customer_id, plano, frequencia_ap, contract_number, inicio):
    # NAO enviar expires_at. Na Iugu esse campo e a data de expiracao da assinatura
    # E a data da proxima cobranca — mandando hoje+5 anos, a primeira fatura vencia
    # em 2031 e a recorrencia mensal nunca comecava. O comentario antigo dizia o
    # contrario e estava errado: o fluxo do Streamlit, o unico ja comprovado em
    # producao, tambem nao envia expires_at. Iugu assume a data corrente (cobranca
    # imediata, que e o esperado na jornada 3) e avanca sozinha a cada ciclo.
    # only_on_charge_success e only_charge_on_due_date sao documentados como
    # INCOMPATIVEIS com automatic_pix (a Iugu responde 400). Mandavamos o primeiro
    # como False, que ja e o default — omitir nao muda comportamento e tira o risco.
    payload = {
        "customer_id": customer_id,
        "plan_identifier": plano,
        "payable_with": "pix",
        "automatic_pix": {
            "journey": 3,
            "frequency": frequencia_ap,
            "recurrence_beginning": inicio.isoformat(),
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
        marca = (body.get("marca") or "").strip()
        razao = (body.get("razao_social") or "").strip()
        whatsapp = (body.get("whatsapp") or "").strip()
        cpf = _digits(body.get("cpf") or body.get("documento"))

        if not (parceiro and plano_id and nome and email and cpf and marca and razao and whatsapp):
            return _send(self, 400, {
                "erro": "Preencha marca, razão social, CNPJ/CPF, responsável, e-mail e WhatsApp."
            })

        # A clientela e quase toda PJ: aceita CNPJ (14) e CPF (11), e barra o resto
        # antes de criar cadastro torto na Iugu.
        if len(cpf) not in (11, 14):
            return _send(self, 400, {
                "erro": "CNPJ deve ter 14 dígitos e CPF 11. Confira o número digitado."
            })

        if len(_digits(whatsapp)) < 10:
            return _send(self, 400, {"erro": "WhatsApp deve ter DDD + número."})

        token = token_para_parceiro(parceiro)
        if not token:
            return _send(self, 404, {"erro": f"Parceiro '{parceiro}' não configurado."})

        try:
            plano, r_plano = buscar_plano(token, plano_id)
        except Exception as e:
            return _send(self, 502, {"erro": f"Falha ao consultar plano: {e}"})
        if not plano:
            if r_plano is not None and r_plano.status_code in (401, 403):
                return _send(self, 502, {
                    "erro": "Token da Iugu recusado (401). O token do parceiro precisa ser renovado."
                })
            return _send(self, 404, {"erro": f"Plano '{plano_id}' não encontrado."})

        # Trava de valor: substitui a antiga guarda de "R$0,00". Continua barrando o
        # plano rascunho de R$0 (caso Viviane Fortunato), e agora tambem qualquer
        # divergencia de preco — o link so gera Pix de R$429,00.
        esperado = VALORES_PERMITIDOS.get(plano.get("identifier") or plano_id)
        if esperado is None:
            return _send(self, 422, {
                "erro": f"Plano '{plano_id}' não está liberado para cobrança por este link."
            })

        valor = valor_cents_do_plano(plano)
        if valor != esperado:
            return _send(self, 422, {
                "erro": (
                    f"Plano '{plano_id}' está com valor R${valor / 100:.2f} na Iugu, "
                    f"mas este link espera R${esperado / 100:.2f}. "
                    "Use o identifier correto (ex: 'starter.1') ou ajuste o preço do plano."
                )
            })

        try:
            customer_id = _buscar_customer_por_cpf(token, cpf)
            if not customer_id:
                customer_id, r_new = _criar_customer(
                    token, nome, email, cpf, marca, razao, whatsapp
                )
                if not customer_id:
                    detalhe_cli = r_new.text if r_new is not None else None
                    print(f"[gerar-pix] cliente recusado: {str(detalhe_cli)[:900]}")
                    return _send(self, 502, {
                        "erro": "Não foi possível criar cliente na Iugu.",
                        "detalhe": detalhe_cli,
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
                inicio_recorrencia(date.today(), plano),
            )
        except requests.RequestException as e:
            return _send(self, 502, {"erro": f"Erro de rede ao criar assinatura: {e}"})

        if r_sub.status_code >= 400:
            try:
                detalhe = r_sub.json()
            except Exception:
                detalhe = r_sub.text
            # Vai para os logs da Vercel: sem isso, uma recusa da Iugu chega no
            # navegador como "erro ao criar assinatura" e nao da para diagnosticar.
            print(f"[gerar-pix] assinatura recusada ({r_sub.status_code}): {str(detalhe)[:900]}")
            return _send(self, 502, {
                "erro": "Erro ao criar assinatura.",
                "detalhe": detalhe,
                "status_iugu": r_sub.status_code,
            })

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
