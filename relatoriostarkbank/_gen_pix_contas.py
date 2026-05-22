# -*- coding: utf-8 -*-
# Gera pix_marcas.js para o painel.
# FONTE PRIMARIA: aba "Contas" da planilha Google (snapshot abaixo).
# FALLBACK: planilha "PIX Starkbank (1).xlsx" (mesma logica do _gen_pix.py).
# Match: CNPJ -> cnpj_marcas.js (nomeFantasia) + nome da planilha.
# Uso:  py -3 _gen_pix_contas.py
import pandas as pd, re, json, unicodedata

BASE = r"C:/Users/Laura/Projetos/Ideia-vesti/relatoriostarkbank"
XL   = r"C:/Users/Laura/Downloads/PIX Starkbank (1).xlsx"

# ---- aba "Contas" (empresa, chave Pix, CNPJ) ----
# Quando "chave Pix" vem vazia mas ha CNPJ, o CNPJ E a chave.
CONTAS = [
    # Sincronizado da aba "contas" do "CC Starkbank - Walid (1).xlsx".
    ("Alcance Jeans Nova", "pessoal@alcancejeans.com", "34411241000145"),
    ("Anne Blanc", "43865993000177", ""),
    ("Arary", "13495868000151", ""),
    ("Barraca do Willinha", "35322340000113", ""),
    ("Be Free", "09121974000106", ""),
    ("BegKids", "44870711000192", ""),
    ("Bella Donna", "(11)93399-0994", "32805399000174"),
    ("Carmen Jeans", "41107278000140", ""),
    ("Carmila", "08482963000180", ""),
    ("CARONLY", "08127619442", ""),
    ("Charisma Filial", "11759407000113", ""),
    ("Coinage", "54493334000173", ""),
    # "Deslum..." na antecipacao = mesma empresa (CNPJ 38098914000100).
    ("Deslum...", "48067277842", "38098914000100"),
    ("Enfasy Jeans", "2a7fa437-21c9-4254-bff8-f9e1ab4ad1e8", "22534629000154"),
    ("Erilluz Jeans", "32796225000192", ""),
    ("Evian", "(11)99567-1787", "49238112000174"),
    ("Ezee", "moda.ezee4@gmail.com", "28314192000120"),
    ("Gissary", "52337840000148", ""),
    ("Groovy", "financeiro@groovyforever.com.br", "16873897000106"),
    ("Imporio Fitness", "38280852000152", ""),
    # Incentive Moda: chave PIX trocada para o e-mail (pedido da Laura).
    ("Incentive Moda", "incentivemoda@gmail.com", "29780242000127"),
    ("IZZAT JEANS", "05616094000141", ""),
    ("Jilem Modas", "45860676000193", ""),
    ("KAESSI", "24091573000136", ""),
    ("Kalli", "49345891000107", ""),
    ("Kauly", "fabriciopais@yahoo.com.br", "18625427000140"),
    ("Kelly Rodrigues", "54697378000115", ""),
    ("Lesto", "58418377000145", ""),
    ("Life Activewear", "52602995000164", ""),
    ("Mafia Fitness", "41549988000120", ""),
    ("Maria Chica", "54211529000183", ""),
    ("Maria Lima Santa Cruz", "21721042000434", ""),
    ("MissMel", "17974887000111", ""),
    ("Nasmah", "42339602000118", ""),
    ("Nicky Atacado", "44839916000105", ""),
    ("Nono Modas", "34329403000109", ""),
    ("Nova Versao Roupas", "10808886000158", ""),
    ("Ohvely", "23103066000102", ""),
    ("OXIGENIO MODAS", "54911296000121", ""),
    ("PatachosN", "38544161000119", ""),
    ("Petit Enfant", "60741324000102", ""),
    ("Planet Charm", "45676252000173", ""),
    ("Pury", "41662997000122", ""),
    ("RCR Clothing Original", "22924965000103", ""),
    ("Sedanbi", "39922297000188", ""),
    ("Sedanbi Filial", "49591774000123", ""),
    ("SN Acessorios", "60801292000193", ""),
    ("Stefani", "(11)94635-4066", "09500916000185"),
    ("Tee Fashion", "24459412000152", ""),
    ("The Lion Confeccoes", "zoesportsoficial@gmail.com", "41426603000137"),
    ("TTNG Varejo", "12292902000128", ""),
    ("Vistamy", "24680354000192", ""),
    ("VN11", "4007594d-a545-43cc-9357-277dd4c030e1", "33882279000133"),
    ("Yunire", "e.yunire@gmail.com", "45375658000116"),
    ("Zero Um Confeccoes", "47516113000108", ""),
    ("Zeros Confec", "45180025000152", ""),
]


def norm(s):
    s = str(s or "").lower().strip()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def digits(s):
    return re.sub(r"\D", "", str(s or ""))


def fmt_chave(chave):
    """Mesma normalizacao de chave do _gen_pix.py."""
    chave = str(chave or "").strip()
    if not chave:
        return ""
    if re.fullmatch(r"\(\d{2}\)\s?\d{4,5}-?\d{4}", chave):  # telefone formatado
        return "+55" + digits(chave)
    if re.fullmatch(r"\d+", chave):
        if len(chave) == 11:
            return chave  # CPF (chave PIX crua; telefone vem formatado c/ DDD)
        if len(chave) == 10:
            return "+55" + chave
        if len(chave) == 13:
            return chave.zfill(14)
    return chave


# ---- 1) cnpj_marcas.js: nomeFantasia(norm) -> cnpj ; cnpj -> [nomes] ----
txt = open(BASE + "/cnpj_marcas.js", encoding="utf-8").read()
raw = txt[txt.index("{") + 1: txt.index("};")]
cnpj_by_name, names_by_cnpj = {}, {}
for m in re.finditer(r'"([^"]+)"\s*:\s*"([^"]+)"', raw):
    nm, cj = norm(m.group(1)), digits(m.group(2))
    cnpj_by_name[nm] = cj
    names_by_cnpj.setdefault(cj, []).append(nm)

# ---- 2) Contas (PRIMARIA): cnpj/nome -> chave ----
contas_by_cnpj, contas_by_marca = {}, {}
for empresa, chave_col, cnpj_col in CONTAS:
    cj = digits(cnpj_col)
    # So a chave PIX explicita da aba Contas conta. Sem chave preenchida
    # NAO usa o CNPJ como chave — cai pro fallback xlsx; se tb nao tiver
    # la, a marca fica sem chave (em branco).
    chave = fmt_chave(chave_col)
    if not chave:
        continue
    if cj:
        contas_by_cnpj[cj] = chave
    contas_by_marca[norm(empresa)] = chave

# ---- 3) xlsx (FALLBACK): cnpj/nome -> chave ----
df = pd.read_excel(XL, header=0, dtype=str)
df.columns = [str(c).strip() for c in df.columns]
xl_by_cnpj, xl_by_marca = {}, {}
for _, r in df.iterrows():
    marca = str(r.get("Marca", "")).strip()
    cj = digits(r.get("CNPJ"))
    if cj and "." not in str(r.get("CNPJ")) and 11 < len(cj) < 14:
        cj = cj.zfill(14)
    chave = r.get("Chave PIX")
    chave = "" if (chave is None or (isinstance(chave, float) and pd.isna(chave))
                   or str(chave).strip().lower() == "nan") else str(chave).strip()
    if not chave:
        continue
    chave = fmt_chave(chave)
    if cj:
        xl_by_cnpj[cj] = chave
    if marca:
        xl_by_marca[norm(marca)] = chave


def resolve(nome_norm):
    """Contas (nome -> cnpj) tem prioridade; xlsx e fallback."""
    cj = cnpj_by_name.get(nome_norm, "")
    if nome_norm in contas_by_marca:
        return contas_by_marca[nome_norm]
    if cj and cj in contas_by_cnpj:
        return contas_by_cnpj[cj]
    if cj and cj in xl_by_cnpj:
        return xl_by_cnpj[cj]
    if nome_norm in xl_by_marca:
        return xl_by_marca[nome_norm]
    return ""


# ---- 4) alvos: nomeFantasia do cnpj_marcas.js + antec do dados.js ----
dtxt = open(BASE + "/dados.js", encoding="utf-8").read()
dtxt = dtxt[dtxt.index("{"):].strip()
DADOS, _ = json.JSONDecoder().raw_decode(dtxt)


def is_test(n):
    n = norm(n)
    return n.startswith("andressa - teste") or n == "andressa vesti" or "teste" in n


alvos = {}  # norm(nome) -> nome original (preferindo nomeFantasia do dashboard)
for cj, nomes in names_by_cnpj.items():
    for nm in nomes:
        alvos.setdefault(nm, nm)
for p in DADOS.get("pedidos", []):
    nf = p.get("nomeFantasia", "")
    if nf and not is_test(nf):
        alvos.setdefault(norm(nf), nf)
for empresa, _, _ in CONTAS:
    alvos.setdefault(norm(empresa), empresa)

seen = {}
for nm in alvos:
    c = resolve(nm)
    if c:
        seen[nm] = (c, alvos[nm])

# ---- 5) escreve pix_marcas.js ----
items = sorted(seen.items())
w = max(len(n) for n, _ in items) + 2
body = "\n".join('        "%s":%s"%s",' % (n, " " * (w - len(n)), c)
                  for n, (c, _) in items)
if body.endswith(","):
    body = body[:-1]

out = '''// Chaves PIX das marcas. FONTE PRIMARIA: aba "Contas" da planilha Google.
// FALLBACK: planilha "PIX Starkbank (1).xlsx". Atualize re-rodando _gen_pix_contas.py.
// Match feito pelo nomeFantasia (lowercase, trim, sem acento) ou CNPJ.
window.PIX_MARCAS = (function(){
    var raw = {
%s
    };
    function norm(s){
        return String(s||"").toLowerCase().trim()
            .normalize("NFD").replace(/[\\u0300-\\u036f]/g,"");
    }
    var map = {};
    for (var k in raw) map[norm(k)] = raw[k];
    return {
        get: function(nome){ return map[norm(nome)] || ""; },
        norm: norm
    };
})();
''' % body
open(BASE + "/pix_marcas.js", "w", encoding="utf-8").write(out)

# ---- relatorio ----
print("pix_marcas.js gerado:", len(items), "marcas")
n_contas = sum(1 for nm in seen if nm in contas_by_marca
               or cnpj_by_name.get(nm, "") in contas_by_cnpj)
print("  via Contas (primaria):", n_contas)
print("  via xlsx (fallback)  :", len(items) - n_contas)

antec_sem = []
for p in DADOS.get("pedidos", []):
    if p.get("antecipacaoEnabled"):
        nf = p.get("nomeFantasia", "")
        if nf and not is_test(nf) and norm(nf) not in seen:
            antec_sem.append(nf)
antec_sem = sorted(set(antec_sem))
print("\n=== Marcas com ANTECIPACAO ainda SEM chave (Contas+xlsx) ===")
for nf in antec_sem:
    print(" -", nf, " CNPJ:", cnpj_by_name.get(norm(nf), "(sem cnpj)"))
print("Total:", len(antec_sem))
