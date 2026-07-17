"""
Atualiza os snapshots tino_clientes.json e marcas_gabi.json que o build-cloud-bq.js lê.
Roda LOCAL (precisa das credenciais do Google Sheets e do xlsx da Gabi — nenhum dos dois
existe no runner do GitHub Actions, por isso os snapshots ficam commitados na pasta).

Uso:  python _refresh_tino_gabi.py
Requer:  google-api-python-client, google-auth, openpyxl
  - ../PainelCSGerencial/google_sa.json  (SA painel-cs-sheets-reader@csatenps, com acesso de leitor à planilha)
  - C:\\Users\\Laura\\Downloads\\Marcas CS Gabi.xlsx
"""
import json, unicodedata, os
import openpyxl
from google.oauth2 import service_account
from googleapiclient.discovery import build

DIR = os.path.dirname(os.path.abspath(__file__))
SA_JSON = os.path.join(DIR, '..', 'PainelCSGerencial', 'google_sa.json')
GABI_XLSX = r'C:\Users\Laura\Downloads\Marcas CS Gabi.xlsx'
TINO_SHEET_ID = '1JKt8CnRGPPm7CqYh3JhGOJXeWt02kQi9uDovJOEHBVg'
TINO_RANGE = 'Clientes Ativos Tino'


def norm(s):
    s = (s or '').strip().lower()
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def refresh_tino():
    creds = service_account.Credentials.from_service_account_file(
        SA_JSON, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    svc = build('sheets', 'v4', credentials=creds)
    vals = svc.spreadsheets().values().get(
        spreadsheetId=TINO_SHEET_ID, range=TINO_RANGE).execute().get('values', [])
    hidx = next((i for i, r in enumerate(vals)
                 if r and any((c or '').strip().lower() == 'cliente' for c in r)), 0)
    clientes = [{'nome': r[0].strip(), 'status': (r[2].strip() if len(r) > 2 else '')}
                for r in vals[hidx + 1:] if r and r[0].strip()]
    json.dump({'_fonte': 'Google Sheets Clientes Tino ' + TINO_SHEET_ID, 'clientes': clientes},
              open(os.path.join(DIR, 'tino_clientes.json'), 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print('tino_clientes.json:', len(clientes), 'marcas')


def refresh_gabi():
    wb = openpyxl.load_workbook(GABI_XLSX, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    hdr = [(c or '').strip() for c in rows[0]]
    col = {name: i for i, name in enumerate(hdr)}

    def g(r, name):
        i = col.get(name)
        return (str(r[i]).strip() if i is not None and i < len(r) and r[i] is not None else '')

    marcas = []
    for r in rows[1:]:
        nome = g(r, 'MARCAS GABI')
        if not nome:
            continue
        marcas.append({'nome': nome, 'integracao': g(r, 'INTEGRAÇÃO'), 'filial': g(r, 'FILIAL'),
                       'vestipago': g(r, 'VESTIPAGO'), 'oraculo': g(r, 'ORÁCULO / ASSISTENTE')})
    json.dump({'_fonte': 'Marcas CS Gabi.xlsx', 'marcas': marcas},
              open(os.path.join(DIR, 'marcas_gabi.json'), 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print('marcas_gabi.json:', len(marcas), 'marcas')


if __name__ == '__main__':
    refresh_tino()
    refresh_gabi()
