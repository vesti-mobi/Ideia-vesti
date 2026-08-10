// ---------------------------------------------------------------------------
// Gera o `dados.js` do painel de metricas do Catalogo IA.
//
// A telemetria do Catalogo IA nao vive no BigQuery nem em planilha: ela e escrita
// direto no Redis pelo proprio app (Upstash, o mesmo que guarda contas, arquetipos
// e o estilo aprendido). Este script LE esse Redis e congela um retrato em JSON —
// o painel e estatico, entao ninguem precisa expor o Redis para a web.
//
// Uso:
//   node fetch_metricas.js            -> escreve dados.js nesta pasta
//   node fetch_metricas.js --ver      -> so imprime o resumo, nao escreve
//
// Credenciais (nao ficam no repo):
//   REDIS_URL     -> ProjetoCatalogo/versao_auto/.env.local (ou variavel de ambiente)
//   VESTI_BRANDS  -> ProjetoCatalogo/.env  (so para saber o NOME de cada marca;
//                    nenhuma APIKEY entra no dados.js)
//
// As regras aqui sao as MESMAS do /admin do app (aba "Uso da plataforma"). Se os
// dois divergirem sobre a mesma marca, o painel perde a credibilidade:
//   - "ativa"  = 2+ produtos publicados nos ultimos 7 dias
//   - "parada" = passou de 7 dias sem nenhum toque
//   - produto conta UMA vez (conjunto por integration_id): reenviar o mesmo
//     produto ate aprovar o texto continua sendo 1 cadastro
//   - o numero pode vir do aprendizado de estilo quando e maior que o medido —
//     nesse caso vai marcado como estimado (o painel mostra "≈"), porque a
//     telemetria so existe desde 24/07/2026 e antes disso havia uso real.
// ---------------------------------------------------------------------------
const fs = require('fs');
const path = require('path');

const RAIZ_APP = path.join(__dirname, '..', 'ProjetoCatalogo');

function carregarEnv(arquivo) {
  let txt;
  try { txt = fs.readFileSync(arquivo, 'utf8'); } catch (_) { return false; }
  for (const linha of txt.split(/\r?\n/)) {
    const m = linha.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)$/);
    if (!m) continue;
    let v = m[2].trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
    if (!process.env[m[1]]) process.env[m[1]] = v;
  }
  return true;
}
carregarEnv(path.join(RAIZ_APP, 'versao_auto', '.env.local'));   // REDIS_URL
carregarEnv(path.join(RAIZ_APP, '.env'));                        // VESTI_BRANDS

const soVer = process.argv.includes('--ver');
if (!process.env.REDIS_URL) {
  console.error('ERRO: REDIS_URL ausente. Esperado em ProjetoCatalogo/versao_auto/.env.local');
  process.exit(1);
}
const Redis = require(path.join(RAIZ_APP, 'versao_auto', 'node_modules', 'ioredis'));

// ---------- datas (fuso de Brasilia, igual ao app) ----------
const TZ_BR = 'America/Sao_Paulo';
const _fmtDiaBR = new Intl.DateTimeFormat('en-CA', { timeZone: TZ_BR, year: 'numeric', month: '2-digit', day: '2-digit' });
const diaBR = (d) => _fmtDiaBR.format(d || new Date());
const _ancora = (dia) => { const [a, m, d] = String(dia).split('-').map(Number); return new Date(Date.UTC(a, m - 1, d, 12)); };
const somaDias = (dia, n) => { const t = _ancora(dia); t.setUTCDate(t.getUTCDate() + n); return t.toISOString().slice(0, 10); };
const segundaDe = (dia) => somaDias(dia, -(((_ancora(dia).getUTCDay() + 6) % 7)));
const ultimosDias = (n, ate) => Array.from({ length: n }, (_, i) => somaDias(ate || diaBR(), -i));
const ultimasSemanas = (n, ate) => Array.from({ length: n }, (_, i) => somaDias(segundaDe(ate || diaBR()), -7 * i)).reverse();

const ATIVA_MIN_PRODUTOS = 2;
const PARADA_DIAS = 7;
const SEMANAS = 12;

const normSlug = (s) => String(s || '').normalize('NFD').toLowerCase()
  .replace(/[̀-ͯ]/g, '').replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');

const diasDesde = (iso) => {
  const t = iso ? Date.parse(iso) : NaN;
  if (!Number.isFinite(t)) return null;
  return Math.floor((Date.now() - t) / 86400000);
};

(async () => {
  const r = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: 3 });

  // ---------- quais marcas existem: env + liberadas no /admin - removidas ----------
  const lerJson = async (chave) => {
    try { const raw = await r.get(chave); const a = raw ? JSON.parse(raw) : []; return Array.isArray(a) ? a : []; }
    catch (_) { return []; }
  };
  const porSlug = new Map();
  for (const m of JSON.parse(process.env.VESTI_BRANDS || '[]')) porSlug.set(normSlug(m.slug || m.nome), { slug: normSlug(m.slug || m.nome), nome: m.nome || m.slug });
  for (const m of await lerJson('marcas:registry')) porSlug.set(normSlug(m.slug || m.nome), { slug: normSlug(m.slug || m.nome), nome: m.nome || m.slug });
  for (const m of await lerJson('marcas:removidas')) porSlug.delete(normSlug(m.slug || m.nome));
  const marcas = [...porSlug.values()].filter((m) => m.slug);
  const slugs = marcas.map((m) => m.slug);

  // ---------- contas (para "tem conta" e para a curva de cadastros) ----------
  const contas = [];
  for (const k of await r.keys('user:*')) {
    try { contas.push(JSON.parse(await r.get(k))); } catch (_) {}
  }
  const contasPorSlug = {};
  for (const c of contas) {
    for (const s of (c.marcas || [])) {
      const slug = normSlug(s);
      (contasPorSlug[slug] = contasPorSlug[slug] || []).push(c);
    }
  }

  const medindoDesde = await r.get('uso:_inicio');
  const hoje = diaBR();
  const dias7 = ultimosDias(7, hoje);
  const semanas = ultimasSemanas(SEMANAS, hoje);

  // ---------- por marca ----------
  const linhas = [];
  for (const m of marcas) {
    const h = await r.hgetall(`uso:${m.slug}`) || {};
    const medidos = await r.scard(`uso:${m.slug}:produtos`);

    // Piso historico: o aprendizado de estilo guarda o id de cada produto que a marca
    // aprovou. E subestimado de proposito (so conta produto editado), mas cobre o uso
    // anterior a telemetria — sem ele a Petit apareceria como "nunca usou".
    let estimados = 0, estiloAtualizado = '';
    try {
      const e = JSON.parse((await r.get(`estilo:${m.slug}`)) || 'null');
      if (e) {
        const ids = new Set();
        for (const reg of [...(e.ativas || []), ...(e.candidatas || []), ...(e.bloqueadas || []), ...(e.regras || [])]) {
          (reg.produtos || []).forEach((id) => ids.add(String(id)));
        }
        estimados = ids.size;
        estiloAtualizado = e.atualizado || '';
      }
    } catch (_) {}

    // Produtos dos ultimos 7 dias: uniao dos conjuntos diarios (nao a soma — o mesmo
    // produto tocado em dois dias e um produto so).
    const idsSemana = new Set();
    for (const d of dias7) {
      for (const id of await r.smembers(`uso:${m.slug}:d:${d}`)) idsSemana.add(id);
    }

    const ultimo = [h.ultimo || '', estiloAtualizado].sort().pop() || '';
    const produtos = Math.max(medidos, estimados);
    let arquetipo = '';
    try {
      const a = await r.get(`arq:${m.slug}`);
      if (a) { const o = a.startsWith('{') ? JSON.parse(a) : { a }; arquetipo = o.a || o.arquetipo || ''; }
    } catch (_) {}

    linhas.push({
      slug: m.slug,
      nome: m.nome,
      arquetipo,
      tem_conta: (contasPorSlug[m.slug] || []).length > 0,
      email: h.ultimo_email || ((contasPorSlug[m.slug] || [])[0] || {}).email || '',
      produtos,
      estimado: estimados > medidos || (!h.ultimo && !!estiloAtualizado),
      produtos_7d: idsSemana.size,
      ativa: idsSemana.size >= ATIVA_MIN_PRODUTOS,
      acessos: Number(h.acesso || 0),
      buscas: Number(h.busca || 0),
      descricoes: Number(h.descricao || 0),
      envios: Number(h.envio || 0),
      primeiro: h.primeiro || '',
      ultimo,
      dias: diasDesde(ultimo),
    });
  }
  linhas.sort((a, b) => b.produtos - a.produtos || b.produtos_7d - a.produtos_7d || String(a.nome).localeCompare(String(b.nome)));

  // ---------- serie semanal ----------
  const cadastros = contas.map((c) => String(c.criado_em || '').slice(0, 10)).filter(Boolean);
  const serie = [];
  for (const inicio of semanas) {
    const fim = somaDias(inicio, 6);
    const toques = await r.hgetall(`uso:w:${inicio}`) || {};
    let produtos = 0, ativas = 0, usaram = 0;
    for (const s of slugs) {
      const n = await r.scard(`uso:${s}:w:${inicio}`);
      produtos += n;
      if (n >= ATIVA_MIN_PRODUTOS) ativas++;
      if (n > 0 || Number(toques[s] || 0) > 0) usaram++;
    }
    serie.push({
      inicio, fim, produtos, ativas, usaram,
      cadastros: cadastros.filter((d) => d <= fim).length,                     // acumulado
      novos: cadastros.filter((d) => d >= inicio && d <= fim).length,
    });
  }

  const dados = {
    gerado_em: new Date().toISOString(),
    hoje,
    medindo_desde: medindoDesde || '',
    criterio: { produtos_semana: ATIVA_MIN_PRODUTOS, dias_parada: PARADA_DIAS, janela_dias: 7, semanas: SEMANAS },
    kpis: {
      liberadas: linhas.length,
      ativas: linhas.filter((l) => l.ativa).length,
      usando: linhas.filter((l) => l.ultimo).length,
      nunca_usaram: linhas.filter((l) => !l.ultimo).length,
      paradas: linhas.filter((l) => l.dias !== null && l.dias > PARADA_DIAS).length,
      com_conta: linhas.filter((l) => l.tem_conta).length,
      contas: contas.length,
      produtos_total: linhas.reduce((s, l) => s + l.produtos, 0),
      produtos_7d: linhas.reduce((s, l) => s + l.produtos_7d, 0),
      descricoes_total: linhas.reduce((s, l) => s + l.descricoes, 0),
      total_estimado: linhas.some((l) => l.estimado && l.produtos),
    },
    marcas: linhas,
    semanas: serie,
  };

  console.log(`medindo desde ${dados.medindo_desde || '(nao definido)'} | ${dados.kpis.liberadas} marcas liberadas | ${dados.kpis.usando} usaram | ${dados.kpis.produtos_total} produtos publicados`);
  for (const l of linhas) {
    console.log(`  ${l.nome.padEnd(22)} produtos:${String(l.produtos).padStart(4)}${l.estimado ? '~' : ' '} 7d:${String(l.produtos_7d).padStart(3)}  descricoes:${String(l.descricoes).padStart(4)}  envios:${String(l.envios).padStart(4)}  ultimo:${(l.ultimo || '-').slice(0, 10)}`);
  }

  if (!soVer) {
    const saida = path.join(__dirname, 'dados.js');
    fs.writeFileSync(saida, 'window.DADOS = ' + JSON.stringify(dados, null, 2) + ';\n', 'utf8');
    console.log(`\ndados.js escrito (${(fs.statSync(saida).size / 1024).toFixed(1)} KB)`);
  }

  await r.quit();
})().catch((e) => { console.error('ERRO:', e.message); process.exit(1); });
