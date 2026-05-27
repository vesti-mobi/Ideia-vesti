# -*- coding: utf-8 -*-
"""Sync da aba 'Contas dos clientes' (Google Sheet) para os 3 mapas do painel.

Fonte:  https://docs.google.com/spreadsheets/d/1uOnHsppvseKyFZsFbUYv9gX1XxHrtr-OjLv3_I7Us1s/  aba gid=1082358844
Destino: pix_marcas.js, cnpj_marcas.js, razao_marcas.js (mesma pasta deste script)

Comportamento: para cada marca presente na planilha, sobrescreve PIX/CNPJ/razao
no JS correspondente. Marcas que NAO estao na planilha sao MANTIDAS como
estao (fallback). Nada e apagado.

Auth:
  - Local: define GOOGLE_SHEETS_SA_JSON_FILE=path/para/sa.json
  - GH Actions: define GOOGLE_SHEETS_SA_JSON com o conteudo do JSON
"""
import json, os, re, sys, unicodedata
from pathlib import Path

DIR = Path(__file__).parent
SHEET_ID = '1uOnHsppvseKyFZsFbUYv9gX1XxHrtr-OjLv3_I7Us1s'
WORKSHEET_GID = 1082358844

# ----------------------- auth -----------------------
import gspread
from google.oauth2.service_account import Credentials

sa_json_str = os.environ.get('GOOGLE_SHEETS_SA_JSON')
sa_json_file = os.environ.get('GOOGLE_SHEETS_SA_JSON_FILE')
if sa_json_str:
    creds_info = json.loads(sa_json_str)
elif sa_json_file:
    creds_info = json.loads(Path(sa_json_file).read_text(encoding='utf-8'))
else:
    sys.exit('Defina GOOGLE_SHEETS_SA_JSON (conteudo) ou GOOGLE_SHEETS_SA_JSON_FILE (path)')

creds = Credentials.from_service_account_info(
    creds_info,
    scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'],
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)

ws = next((w for w in sh.worksheets() if w.id == WORKSHEET_GID), None)
if ws is None:
    sys.exit(f'aba gid={WORKSHEET_GID} nao encontrada')

rows = ws.get_all_records()  # primeira linha = cabecalhos
print(f'Aba "{ws.title}": {len(rows)} linhas')
if not rows:
    sys.exit('planilha vazia')

# ----------------------- helpers -----------------------
def norm(s):
    s = str(s or '').lower().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def find_header(sample, *aliases):
    aliases_n = [norm(a) for a in aliases]
    for k in sample.keys():
        if norm(k) in aliases_n:
            return k
    return None

sample = rows[0]
k_empresa = find_header(sample, 'empresa', 'marca', 'nome')
k_pix     = find_header(sample, 'chave pix', 'pix', 'chave_pix')
k_cnpj    = find_header(sample, 'cnpj')  # pega a primeira coluna chamada CNPJ
k_razao   = find_header(sample, 'razao social', 'razão social', 'razao')

missing = [n for n, k in [('empresa', k_empresa), ('chave pix', k_pix),
                            ('cnpj', k_cnpj), ('razao social', k_razao)] if k is None]
if missing:
    sys.exit(f'Cabecalhos faltando: {missing}. Encontrados: {list(sample.keys())}')
print(f'Colunas: empresa={k_empresa!r} pix={k_pix!r} cnpj={k_cnpj!r} razao={k_razao!r}')

# ----------------------- coleta da planilha -----------------------
sheet_pix, sheet_cnpj, sheet_razao = {}, {}, {}
skipped = 0
for r in rows:
    nm = str(r.get(k_empresa, '') or '').strip()
    if not nm:
        skipped += 1
        continue
    nn = norm(nm)
    pix   = str(r.get(k_pix, '')   or '').strip()
    cnpj  = str(r.get(k_cnpj, '')  or '').strip()
    razao = str(r.get(k_razao, '') or '').strip()
    if pix:   sheet_pix[nn]   = pix
    if cnpj:  sheet_cnpj[nn]  = cnpj
    if razao: sheet_razao[nn] = razao
print(f'Da planilha: {len(sheet_pix)} PIX, {len(sheet_cnpj)} CNPJ, {len(sheet_razao)} razao social (linhas vazias: {skipped})')

# ----------------------- merge nos .js -----------------------
def parse_raw_block(text):
    """Acha 'var raw = { ... }' (matching brace) e retorna (pairs, start, end)
    onde start..end abrange o objeto inclusive as chaves."""
    m = re.search(r'var\s+raw\s*=\s*\{', text)
    if not m:
        raise ValueError('"var raw = {" nao encontrado')
    start = m.end() - 1  # posicao do '{'
    depth = 0
    end = None
    for i in range(start, len(text)):
        if text[i] == '{':
            depth += 1
        elif text[i] == '}':
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        raise ValueError('chave de fechamento nao encontrada')
    body = text[start:end + 1]
    pairs = re.findall(r'"((?:[^"\\]|\\.)*)"\s*:\s*"((?:[^"\\]|\\.)*)"', body)
    return list(pairs), start, end

def render_block(items, indent='        '):
    if not items:
        return '{\n' + indent + '}'
    w = max(len(k) for k, _ in items) + 2
    lines = []
    for k, v in items:
        pad = ' ' * (w - len(k))
        v_esc = v.replace('\\', '\\\\').replace('"', '\\"')
        lines.append(f'{indent}"{k}":{pad}"{v_esc}",')
    body = '\n'.join(lines)
    if body.endswith(','):
        body = body[:-1]
    return '{\n' + body + '\n    }'

def update_js(path, sheet_map, label):
    text = path.read_text(encoding='utf-8')
    pairs, start, end = parse_raw_block(text)
    # dedup mantendo ordem; ultimo valor vence (mesma logica do JS)
    by_key = {}
    order = []
    for k, v in pairs:
        nk = norm(k)
        if nk not in by_key:
            order.append(nk)
        by_key[nk] = [k, v]
    added = updated = unchanged = 0
    for nk, val in sheet_map.items():
        if nk in by_key:
            if by_key[nk][1] != val:
                by_key[nk][1] = val
                updated += 1
            else:
                unchanged += 1
        else:
            by_key[nk] = [nk, val]
            order.append(nk)
            added += 1
    new_items = [(by_key[k][0], by_key[k][1]) for k in order]
    new_block = render_block(new_items)
    new_text = text[:start] + new_block + text[end + 1:]
    if new_text == text:
        print(f'  {label}: sem mudancas ({len(new_items)} entradas, ={unchanged} iguais)')
        return
    path.write_text(new_text, encoding='utf-8')
    print(f'  {label}: +{added} novas, ~{updated} atualizadas, ={unchanged} iguais (total {len(new_items)})')

print('\nMerge nos .js:')
update_js(DIR / 'pix_marcas.js',   sheet_pix,   'PIX  ')
update_js(DIR / 'cnpj_marcas.js',  sheet_cnpj,  'CNPJ ')
update_js(DIR / 'razao_marcas.js', sheet_razao, 'RAZAO')
print('\nOK')
