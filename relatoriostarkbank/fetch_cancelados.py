"""Busca summary_total e dados do pedido cancelado no Fabric por orderId.

Gera cancelados_valores.js consumido por cancelados.html.
"""
import io, json, os, re, struct, subprocess, sys, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path
try:
    import pyodbc
except ImportError:
    print('ERRO: pyodbc nao instalado', file=sys.stderr); sys.exit(1)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
ROOT = Path(__file__).parent
INV = ROOT / 'invoices.js'
OUT = ROOT / 'cancelados_valores.js'

SQL_SERVER = '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com'
SQL_DATABASE = 'VestiHouse'
DRIVER = '{ODBC Driver 18 for SQL Server}'
SQL_COPT_SS_ACCESS_TOKEN = 1256


def _refresh_token_access():
    refresh = os.environ.get('FABRIC_REFRESH_TOKEN','').strip()
    tenant = os.environ.get('FABRIC_TENANT_ID','').strip()
    client = os.environ.get('FABRIC_CLIENT_ID','').strip() or '04b07795-8ddb-461a-bbee-02f9e1bf7b46'
    if not refresh or not tenant: return None
    body = urllib.parse.urlencode({'client_id':client,'scope':'https://database.windows.net/.default offline_access','grant_type':'refresh_token','refresh_token':refresh}).encode()
    req = urllib.request.Request(f'https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token', data=body, headers={'Content-Type':'application/x-www-form-urlencoded'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode()).get('access_token')


def _az_token_struct():
    try:
        out = subprocess.run(['az','account','get-access-token','--resource','https://database.windows.net/','--query','accessToken','-o','tsv'], capture_output=True, text=True, check=True, shell=sys.platform.startswith('win'))
        token = out.stdout.strip()
        if not token: return None
        enc = token.encode('utf-16-le')
        return struct.pack('=i', len(enc)) + enc
    except Exception:
        return None


def connect():
    base = f'Driver={DRIVER};Server={SQL_SERVER},1433;Database={SQL_DATABASE};Encrypt=yes;TrustServerCertificate=no;'
    ts = _az_token_struct()
    if ts:
        print('[auth] az CLI'); return pyodbc.connect(base, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: ts})
    raw = _refresh_token_access()
    if raw:
        print('[auth] refresh token')
        enc = raw.encode('utf-16-le')
        return pyodbc.connect(base, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: struct.pack('=i', len(enc)) + enc})
    print('ERRO: sem auth', file=sys.stderr); sys.exit(1)


def _fetch_valores(oids, max_attempts: int = 6) -> dict:
    delay = 30
    for attempt in range(1, max_attempts + 1):
        valores = {}
        try:
            with connect() as conn:
                cur = conn.cursor()
                for i in range(0, len(oids), 500):
                    chunk = oids[i:i+500]
                    placeholders = ','.join('?'*len(chunk))
                    sql = f"""
                        SELECT _id,
                            MAX(orderNumber) AS order_number,
                            MAX(summary_total) AS summary_total,
                            MAX(payment_transaction_installments) AS installments,
                            MAX(customer_name) AS customer_name,
                            MAX(customer_doc) AS customer_doc,
                            MAX(CONVERT(DATE, DATEADD(HOUR,-3,TRY_CAST(settings_createdAt_TIMESTAMP AS DATETIME2)))) AS order_date
                        FROM dbo.MongoDB_Pedidos_Geral
                        WHERE _id IN ({placeholders})
                        GROUP BY _id
                    """
                    cur.execute(sql, chunk)
                    cols = [d[0] for d in cur.description]
                    for r in cur.fetchall():
                        d = dict(zip(cols, r))
                        oid = d.pop('_id')
                        d['order_date'] = d['order_date'].isoformat() if d['order_date'] else ''
                        d['summary_total'] = float(d['summary_total']) if d['summary_total'] is not None else None
                        valores[oid] = d
                    print(f'  lote {i//500+1}: {len(valores)}/{len(oids)}')
            return valores
        except pyodbc.Error as e:
            sqlstate = e.args[0] if e.args else ''
            throttle = '24801' in str(e)
            if attempt == max_attempts:
                print(f'[fabric] falha definitiva apos {attempt} tentativas: {e}', file=sys.stderr)
                raise
            motivo = 'capacidade estourada (24801)' if throttle else f'erro {sqlstate}'
            print(f'[fabric] {motivo} (tentativa {attempt}/{max_attempts}); '
                  f'aguardando {delay}s e reconectando...', file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * 2, 300)
    raise RuntimeError('unreachable')


def main():
    txt = INV.read_text(encoding='utf-8')
    m = re.search(r'window\.INVOICES\s*=\s*(\{.*\})\s*;?\s*$', txt, re.DOTALL)
    inv = json.loads(m.group(1))
    voided = [f for f in inv['faturas'] if (f.get('purchase') or {}).get('status') == 'voided']
    oids = sorted({f['orderId'] for f in voided if f.get('orderId')})
    print(f'[invoices] {len(voided)} cancelados, {len(oids)} orderIds unicos')

    if not oids:
        OUT.write_text('window.CANCELADOS_VALORES = {};\n', encoding='utf-8'); return

    # Query em lotes (IN aceita ~1000). O endpoint SQL do Fabric derruba o link
    # de forma intermitente (08S01); reabrimos a conexao e tentamos de novo em
    # vez de matar o run (que impediria o commit do dados.js ja atualizado).
    valores = _fetch_valores(oids)

    out = {'geradoEm': datetime.now(timezone.utc).isoformat(), 'valores': valores}
    OUT.write_text('window.CANCELADOS_VALORES = ' + json.dumps(out, ensure_ascii=False) + ';\n', encoding='utf-8')
    print(f'[write] {OUT.name} — {len(valores)} pedidos com valor / {len(oids)} buscados')


if __name__ == '__main__':
    main()
