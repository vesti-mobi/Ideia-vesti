"""Busca summary_total e dados do pedido cancelado por orderId.

Gera cancelados_valores.js consumido por cancelados.html.

Fonte: BigQuery `vesti-data-499015.vestilake_BI.MongoDB_Pedidos_Geral`
(migrado do Fabric/VestiHouse em 19/08/2026 — a capacidade Fabric esta pausada
desde ~15/07 e cada run gastava ~13min em 6 tentativas so pra falhar, deixando
o arquivo congelado. O BQ responde a mesma consulta em ~1,3s e esta fresco:
espelho do Mongo com lag de horas, nao de dias).
"""
import io, json, os, re, sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from google.cloud import bigquery
except ImportError:
    print('ERRO: google-cloud-bigquery nao instalado', file=sys.stderr); sys.exit(1)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
ROOT = Path(__file__).parent
INV = ROOT / 'invoices.js'
OUT = ROOT / 'cancelados_valores.js'

BQ_PROJECT = os.environ.get('BQ_PROJECT', 'vesti-data-499015')
BQ_DATASET = os.environ.get('BQ_DATASET', 'vestilake_BI')
TABELA = f'`{BQ_PROJECT}.{BQ_DATASET}.MongoDB_Pedidos_Geral`'

# No lake tudo chega como STRING (o job de ingestao achata o documento do Mongo
# e converte todo campo pra texto), por isso os SAFE_CAST abaixo.
SQL = f"""
    SELECT _id,
        MAX(orderNumber) AS order_number,
        MAX(SAFE_CAST(summary_total AS FLOAT64)) AS summary_total,
        MAX(payment_transaction_installments) AS installments,
        MAX(customer_name) AS customer_name,
        MAX(customer_doc) AS customer_doc,
        MAX(DATE(CAST(settings_createdAt AS TIMESTAMP), 'America/Sao_Paulo')) AS order_date
    FROM {TABELA}
    WHERE _id IN UNNEST(@ids)
    GROUP BY _id
"""


def _fetch_valores(oids) -> dict:
    cli = bigquery.Client(project=BQ_PROJECT)
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter('ids', 'STRING', oids)
    ])
    # Sem lotes: o BQ aceita o array inteiro como parametro (o limite de ~1000
    # do "IN (...)" era do SQL Server).
    valores = {}
    for r in cli.query(SQL, job_config=cfg).result():
        valores[r['_id']] = {
            'order_number': r['order_number'],
            'summary_total': float(r['summary_total']) if r['summary_total'] is not None else None,
            'installments': r['installments'],
            'customer_name': r['customer_name'],
            'customer_doc': r['customer_doc'],
            'order_date': r['order_date'].isoformat() if r['order_date'] else '',
        }
    return valores


def main():
    txt = INV.read_text(encoding='utf-8')
    m = re.search(r'window\.INVOICES\s*=\s*(\{.*\})\s*;?\s*$', txt, re.DOTALL)
    inv = json.loads(m.group(1))
    voided = [f for f in inv['faturas'] if (f.get('purchase') or {}).get('status') == 'voided']
    oids = sorted({f['orderId'] for f in voided if f.get('orderId')})
    print(f'[invoices] {len(voided)} cancelados, {len(oids)} orderIds unicos')

    if not oids:
        OUT.write_text('window.CANCELADOS_VALORES = {};\n', encoding='utf-8'); return

    valores = _fetch_valores(oids)

    # Rede de seguranca: se o BQ devolver muito menos do que ja tinhamos, e
    # sinal de problema na fonte — melhor manter o arquivo atual do que
    # publicar um cancelados_valores.js pela metade.
    if OUT.exists():
        try:
            antigo = json.loads(re.search(r'window\.CANCELADOS_VALORES\s*=\s*(\{.*\})\s*;?\s*$',
                                          OUT.read_text(encoding='utf-8'), re.DOTALL).group(1))
            n_antigo = len(antigo.get('valores') or {})
        except Exception:
            n_antigo = 0
        if n_antigo and len(valores) < n_antigo * 0.8:
            print(f'ERRO: so {len(valores)} pedidos vieram do BQ contra {n_antigo} do arquivo atual; '
                  'nao vou sobrescrever', file=sys.stderr)
            sys.exit(1)

    out = {'geradoEm': datetime.now(timezone.utc).isoformat(), 'valores': valores}
    OUT.write_text('window.CANCELADOS_VALORES = ' + json.dumps(out, ensure_ascii=False) + ';\n', encoding='utf-8')
    print(f'[write] {OUT.name} — {len(valores)} pedidos com valor / {len(oids)} buscados')


if __name__ == '__main__':
    main()
