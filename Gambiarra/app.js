// PainelElisa — frontend com tabs + graficos
const fmtBRL = (n) => Number(n||0).toLocaleString("pt-BR",{style:"currency",currency:"BRL",maximumFractionDigits:0});
const fmtInt = (n) => Number(n||0).toLocaleString("pt-BR");
const $ = (id) => document.getElementById(id);
// R$ curto pros numeros desenhados na barra (R$ 12,3 mil nao vira borrao)
const fmtBRLCurto = (n) => {
  const v = Number(n) || 0;
  return v >= 1000 ? "R$ " + (v/1000).toLocaleString("pt-BR",{maximumFractionDigits:1}) + "k"
                   : "R$ " + v.toLocaleString("pt-BR",{maximumFractionDigits:0});
};

// state.chaves = lista de periodos selecionados (multi-selecao). 1 item = comportamento antigo.
const CANAIS = ["Starter","Vesti","Uemtel","Atta","Parceiros"];
const state = { periodo:"mensal", chaves:[], cs:"todas", canais:new Set(CANAIS), empresa:"todas", tab:"home", cadMes:"todos", upgMin:300000, soCoorte:true, reativOrigem:"todas", reativDrill:null, portesSel:new Set(),
  inadDias:0 };
const D = (typeof DADOS !== "undefined") ? DADOS : { empresas:[], mesesList:[], semanasList:[], anosList:[] };
// parceiros que NAO entram no ranking "TOP 10 Parceiros" (viram canal proprio)
const PARC_EXCL = new Set(["atta","attasoft","onix","uemtel","vesti","varejo vesti"]);
const isUemtel = (e) => (e.partner_raw||"").toLowerCase() === "uemtel";
const isAtta   = (e) => ["atta","attasoft"].includes((e.partner_raw||"").toLowerCase());
const isVesti  = (e) => ["vesti","varejo vesti"].includes((e.partner_raw||"").toLowerCase());
const COLORS = ["#6C5CE7","#00B894","#F39C12","#E17055","#0984E3","#FD79A8","#00CEC9","#A29BFE","#D63031","#74B9FF","#FFB94A","#55EFC4"];
const charts = {}; // canvas id -> Chart instance
// Regua das TAGS de inadimplencia (pedido da Laura): 1 a 10 dias de atraso = alerta
// (amarelo); 11 dias ou mais = bloqueado (vermelho). O atraso vem do vencimento MAIS
// ANTIGO em aberto (ver build_inadimplencia em fetch_elisa_bq.py).
const INAD_LIMITE_ALERTA = 10;

// ---------- helpers ----------
function canalDe(e) {
  if (e.starter_interno) return "Starter";
  if (isVesti(e)) return "Vesti";
  if (isUemtel(e)) return "Uemtel";
  if (isAtta(e)) return "Atta";
  return "Parceiros";
}
// o JSON traz canal so como Starter/Parceiros; aqui vira o canal real (usado nas tabelas)
function normalizaCanal() { for (const e of D.empresas) e.canal = canalDe(e); }

// Piso de dados: o BQ so tem pedidos a partir do 1o mes de mesesList (jul/2025).
// Para marcas que entraram ANTES disso a contagem das 5/25 nao comeca no mes de
// entrada delas (nao existe o historico), entao o marco nao significa "primeiras
// N vendas" -- significa so "um mes em que fez N". Marcamos essas p/ poder filtrar.
const PISO_DADOS = () => (D.mesesList && D.mesesList.length) ? D.mesesList[0] : "";
function marcaCoorte() {
  const piso = PISO_DADOS();
  for (const e of D.empresas) {
    e.marcoConfiavel = !!piso && (e.dataEntrada || "").slice(0,7) >= piso;
  }
}
function empresasFiltradas() {
  return D.empresas.filter(e => {
    if (state.cs !== "todas" && e.cs !== state.cs) return false;
    if (!state.canais.has(canalDe(e))) return false;
    if (state.empresa !== "todas" && e.name !== state.empresa) return false;
    return true;
  });
}
function inadStatus(dias) {
  const d = Number(dias) || 0;
  if (d <= 0) return null;
  return d <= INAD_LIMITE_ALERTA ? "alerta" : "bloqueado";
}

function inadBadge(dias) {
  const st = inadStatus(dias);
  if (!st) return "—";
  const d = Number(dias) || 0;
  return st === "alerta"
    ? `<span class="pill pill-inad-alerta">⚠️ Alerta · ${d}d</span>`
    : `<span class="pill pill-inad-bloq">⛔ Bloqueado · ${d}d</span>`;
}

function vpBadge(e) {
  if (e.temPixAtivo && e.temCartaoAtivo) return `<span class="pill pill-ambos">PIX + Cartão</span>`;
  if (e.temPixAtivo) return `<span class="pill pill-pix">só PIX</span>`;
  if (e.temCartaoAtivo) return `<span class="pill pill-cartao">só Cartão</span>`;
  return `<span class="pill pill-nenhum">sem VP</span>`;
}

// agrega e.mensal por ano -> e.anual ({"2026": {...}}) e popula D.anosList
function buildAnual() {
  const anos = new Set();
  for (const e of D.empresas) {
    const anual = {};
    for (const [m, b] of Object.entries(e.mensal || {})) {
      const ano = m.slice(0, 4);
      anos.add(ano);
      const a = anual[ano] || (anual[ano] = {valPix:0,valCartao:0,valTotal:0,qtPix:0,qtCartao:0,qtTotal:0});
      a.valPix += b.valPix||0; a.valCartao += b.valCartao||0; a.valTotal += b.valTotal||0;
      a.qtPix += b.qtPix||0; a.qtCartao += b.qtCartao||0; a.qtTotal += b.qtTotal||0;
    }
    e.anual = anual;
  }
  D.anosList = Array.from(anos).sort();
}

// soma os buckets de TODOS os periodos selecionados (state.chaves)
function bucket(e) {
  const fonte = (state.periodo === "mensal" ? e.mensal : state.periodo === "anual" ? e.anual : e.semanal) || {};
  const acc = {valPix:0,valCartao:0,valTotal:0,qtPix:0,qtCartao:0,qtTotal:0};
  for (const k of state.chaves) {
    const b = fonte[k];
    if (!b) continue;
    acc.valPix += b.valPix||0; acc.valCartao += b.valCartao||0; acc.valTotal += b.valTotal||0;
    acc.qtPix += b.qtPix||0; acc.qtCartao += b.qtCartao||0; acc.qtTotal += b.qtTotal||0;
  }
  return acc;
}
// conjunto de meses "YYYY-MM" cobertos pela selecao (mensal: a propria chave; semanal: o mes da semana)
function mesesSelecionados() {
  const s = new Set();
  for (const k of state.chaves) s.add(state.periodo === "semanal" ? (k||"").slice(0,7) : k);
  return s;
}
// rotulo amigavel da selecao (1 = a chave; N = "N períodos (min … max)")
function chaveLabel() {
  if (!state.chaves.length) return "—";
  if (state.chaves.length === 1) return state.chaves[0];
  const s = [...state.chaves].sort();
  return `${s.length} períodos (${s[0]} … ${s[s.length-1]})`;
}
const mesAtualChave = () => chaveLabel();
// true se o mês "YYYY-MM" cai em algum período selecionado (no anual, compara o ano)
const mesMatch = (m) => {
  if (!m) return false;
  if (state.periodo === "anual") return state.chaves.some(k => m.slice(0,4) === k);
  return mesesSelecionados().has(m);
};
// nº de ativações + reativações da empresa nos períodos selecionados.
// Fonte = ambienteEventos (regra das 4 origens, ver _eventos_ambiente em
// build_data.py). O critério antigo era só o gap de pagamento e deixava de fora
// marca sem cobrança na Iugu e cliente que volta com cadastro novo.
function reativNoPeriodo(e) {
  let s = 0;
  for (const ev of (e.ambienteEventos || [])) {
    if (ev.tipo === "desligamento") continue;
    if (mesMatch(ev.mes)) s++;
  }
  return s;
}

// Numeros desenhados na barra — pedido da Laura (17/08): ver os valores sem passar
// o mouse. O Chart.js 4 nao tem datalabels nativo e o painel nao carrega plugin de
// CDN alem dele, entao desenhamos a mao. Em barra EMPILHADA sai o valor de cada
// faixa dentro dela (so quando cabe, senao vira borrao) e o total da coluna logo
// acima. Passa-se no config do grafico (`plugins:[valoresNaBarra]`), entao vale so
// onde for pedido. Respeita a legenda: dataset escondido nao conta no total.
// Com UM dataset so' (barra simples) o de dentro sairia igual ao de cima -- nesse
// caso desenha so' o de cima. Para formatar (R$, %), passar em
// options.plugins.valoresNaBarra.fmt.
const valoresNaBarra = {
  id: "valoresNaBarra",
  afterDatasetsDraw(chart) {
    const ctx = chart.ctx;
    const opt = (chart.options.plugins && chart.options.plugins.valoresNaBarra) || {};
    const fmt = typeof opt.fmt === "function" ? opt.fmt : (v => v);
    const visiveis = chart.data.datasets.filter(
      (_, di) => !chart.isDatasetVisible || chart.isDatasetVisible(di)).length;
    const totais = [], topoY = [], topoX = [];
    ctx.save();
    ctx.textAlign = "center";
    ctx.font = "600 11px system-ui, -apple-system, 'Segoe UI', sans-serif";
    chart.data.datasets.forEach((ds, di) => {
      // isDatasetVisible cobre os dois jeitos de esconder (clique na legenda e
      // `hidden` no dataset); meta.hidden sozinho vem null no primeiro caso.
      if (chart.isDatasetVisible && !chart.isDatasetVisible(di)) return;
      const meta = chart.getDatasetMeta(di);
      if (meta.hidden) return;
      meta.data.forEach((barra, i) => {
        const v = Number(ds.data[i]) || 0;
        if (!v) return;
        totais[i] = (totais[i] || 0) + v;
        if (topoY[i] === undefined || barra.y < topoY[i]) { topoY[i] = barra.y; topoX[i] = barra.x; }
        if (visiveis < 2) return;   // barra simples: so' o numero de cima
        const base = barra.base === undefined ? barra.y : barra.base;
        if (Math.abs(base - barra.y) < 14) return;   // faixa fina: o numero nao cabe
        ctx.fillStyle = "#fff";
        ctx.textBaseline = "middle";
        ctx.fillText(fmt(v), barra.x, (barra.y + base) / 2);
      });
    });
    ctx.fillStyle = Chart.defaults.color || "#666";
    ctx.textBaseline = "bottom";
    totais.forEach((t, i) => {
      if (!t || topoY[i] === undefined) return;
      // barra mais alta encosta no topo do canvas: o numero sairia cortado --
      // nesse caso escreve DENTRO da barra, logo abaixo do topo.
      if (topoY[i] < 14) {
        ctx.save();
        ctx.fillStyle = "#fff"; ctx.textBaseline = "top";
        ctx.fillText(fmt(t), topoX[i], topoY[i] + 4);
        ctx.restore();
        return;
      }
      ctx.fillText(fmt(t), topoX[i], topoY[i] - 4);
    });
    ctx.restore();
  },
};

function destroyChart(id) { if (charts[id]) { charts[id].destroy(); delete charts[id]; } }
// Grafico sem nenhum dado desenha eixos vazios e parece "quebrado". Escreve o
// motivo no meio da area do grafico.
// NAO mexer em canvas.width/height na mao: sem o Chart.js no controle o canvas
// perde o style que o destroy() removeu, vira 1200x600 no layout e ESTOURA o
// .cw (altura fixa de 260px) -- foi o que deixou os graficos brancos e os cards
// gigantes em 27/08 ao desmarcar todos os canais. Aqui e' um Chart de verdade,
// sem dados e sem eixos, com o recado desenhado por plugin.
function chartVazio(id, msg) {
  makeChart(id, {
    type: "bar",
    data: { labels: [], datasets: [] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: {display: false}, tooltip: {enabled: false} },
      scales: { x: {display: false}, y: {display: false} } },
    plugins: [{
      id: "recadoVazio",
      afterDraw(chart) {
        const { ctx, chartArea } = chart;
        if (!chartArea) return;
        ctx.save();
        ctx.fillStyle = "#636E72";
        ctx.font = "500 13px system-ui, -apple-system, 'Segoe UI', sans-serif";
        ctx.textAlign = "center"; ctx.textBaseline = "middle";
        ctx.fillText(msg, (chartArea.left + chartArea.right) / 2,
                          (chartArea.top + chartArea.bottom) / 2);
        ctx.restore();
      }
    }]
  });
}

function makeChart(id, cfg) {
  destroyChart(id);
  const ctx = document.getElementById(id);
  if (!ctx) return;
  // rede de seguranca: se alguem deixou o canvas com tamanho cravado, devolve o
  // controle pro Chart.js em vez de renderizar num canvas estourado
  ctx.removeAttribute("width"); ctx.removeAttribute("height");
  ctx.style.width = ""; ctx.style.height = "";
  charts[id] = new Chart(ctx, cfg);
}

const _sortState = {}; // {tableId: {col: idx, dir: 'desc'|'asc'}}
function renderTable(tableId, columns, rows) {
  const tbl = document.getElementById(tableId);
  if (!tbl) return;
  if (!rows.length) { tbl.innerHTML = `<tr><td class="empty" colspan="${columns.length}">Sem dados pros filtros atuais.</td></tr>`; return; }
  const st = _sortState[tableId];
  let sortedRows = rows;
  if (st && columns[st.col] && columns[st.col].sort) {
    const k = columns[st.col].sort;
    const mul = st.dir === 'asc' ? 1 : -1;
    sortedRows = [...rows].sort((a,b)=>{
      const va = k(a), vb = k(b);
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === 'number' && typeof vb === 'number') return (va-vb)*mul;
      return String(va).localeCompare(String(vb), 'pt-BR', {numeric:true}) * mul;
    });
  }
  const thead = `<thead><tr>${columns.map((c,i)=>{
    const sortable = !!c.sort;
    let cls = c.cls || '';
    if (sortable) cls += (cls?' ':'') + 'sortable';
    if (st && st.col === i) cls += ' sort-' + st.dir;
    const attrs = sortable ? ` data-col="${i}"` : '';
    return `<th class="${cls}"${attrs}>${c.label}</th>`;
  }).join("")}</tr></thead>`;
  const tbody = `<tbody>${sortedRows.map(r=>{
    const cls = r._alert ? "row-alert" : "";
    return `<tr class="${cls}">${columns.map(c=>`<td class="${c.cls||''}">${c.fn(r)}</td>`).join("")}</tr>`;
  }).join("")}</tbody>`;
  tbl.innerHTML = thead + tbody;
  // bind sort handlers
  tbl.querySelectorAll('th.sortable').forEach(th => {
    th.addEventListener('click', () => {
      const idx = +th.dataset.col;
      const cur = _sortState[tableId];
      // 1o clique ordena CRESCENTE, o 2o inverte (pedido da Laura 27/08)
      const dir = (cur && cur.col === idx && cur.dir === 'asc') ? 'desc' : 'asc';
      _sortState[tableId] = {col: idx, dir};
      renderTable(tableId, columns, rows);
    });
  });
}

// ---------- HOME KPIs ----------
// Marcas com fatura vencida e ainda em aberto ha mais dias que a regua.
// Nao depende do filtro de periodo -- e' uma foto de hoje, igual Sem VP/Travadas.
// Os dias vem do vencimento MAIS ANTIGO em aberto (ver build_inadimplencia).
function marcasInadimplentes(minDias) {
  const min = minDias === undefined ? state.inadDias : minDias;
  return empresasFiltradas()
    .filter(e => (e.faturasVencidas || 0) > 0 && (e.diasAtraso || 0) > min)
    .sort((a, b) => (b.diasAtraso || 0) - (a.diasAtraso || 0));
}

function renderKpis() {
  const lista = empresasFiltradas();
  let gmv=0,cartao=0,pix=0,semVP=0,semFrete=0,cadProds=0,cadMarcas=0,p5=0,p25=0,cartaoMarcas=0,pixMarcas=0,reativ=0;
  const mesK = mesAtualChave();
  for (const e of lista) {
    const b = bucket(e);
    gmv += b.valTotal; cartao += b.valCartao; pix += b.valPix;
    if (b.qtCartao > 0) cartaoMarcas++;
    if (b.qtPix > 0) pixMarcas++;
    if (!e.temVPAtivo) semVP++;
    if (!e.temFreteAtivo) semFrete++;
    cadProds += e.qtProdutos||0;
    if ((e.qtProdutos||0) > 0) cadMarcas++;
    const coorteOk = !state.soCoorte || e.marcoConfiavel;
    if (coorteOk && mesMatch(e.mes5Vendas)) p5++;
    if (coorteOk && mesMatch(e.mes25Vendas)) p25++;
    if (reativNoPeriodo(e) > 0) reativ++;
  }
  $("kpi-gmv").textContent = fmtBRL(gmv);
  $("kpi-gmv-sub").textContent = `${lista.length} marcas · ${chaveLabel()}`;
  $("kpi-vp").textContent = fmtBRL(cartao + pix);
  $("kpi-cartao").textContent = fmtBRL(cartao);
  $("kpi-pix").textContent = fmtBRL(pix);
  $("kpi-cadastro").textContent = fmtInt(cadProds);
  $("kpi-cadastro-marcas").textContent = fmtInt(cadMarcas);
  $("kpi-p5").textContent = fmtInt(p5);
  $("kpi-p25").textContent = fmtInt(p25);
  $("kpi-reativ").textContent = fmtInt(reativ);
  $("kpi-reativ-sub").textContent = `marcas ativadas/reativadas em ${mesK||"—"}`;
  const tam = marcasComTamanho();
  $("kpi-tamanho").textContent = fmtInt(tam.filter(e=>e._def > 0).length);
  $("kpi-tamanho-sub").textContent = tam.length
    ? `de ${fmtInt(tam.length)} marcas com tamanho coletado`
    : "rode tamanho_marca.py";
  $("kpi-semvp").textContent = fmtInt(semVP);
  $("kpi-semfrete").textContent = fmtInt(semFrete);
  $("kpi-topstarter").textContent = fmtInt(lista.filter(e=>e.starter_interno).length);
  $("kpi-topparc").textContent = fmtInt(lista.filter(e=>!e.starter_interno && !PARC_EXCL.has((e.partner_raw||"").toLowerCase())).length);
  const hoje = new Date();
  const travadas = lista.filter(e => {
    if (e.modulos !== undefined && !(e.modulos||"").toLowerCase().includes("vendas")) return false;
    const ped = e.primeiroPedidoCadastrado ? new Date(e.primeiroPedidoCadastrado) : null;
    const venda = e.primeiraVenda ? new Date(e.primeiraVenda) : null;
    const dias = ped ? Math.floor((hoje-ped)/86400000) : null;
    return (ped && !venda && dias > 14) || (ped && (e.qtProdutos||0) === 0);
  });
  $("kpi-travadas").textContent = fmtInt(travadas.length);
  let cliquesTot=0, linksTot=0, marcasLink=0;
  for (const e of lista) {
    cliquesTot += e.cliquesTotal||0;
    linksTot   += e.linksCompartilhados||0;
    if ((e.linksCompartilhados||0) > 0) marcasLink++;
  }
  const upg = marcasUpgrade();
  const mesesUpg = ultimos3MesesFechados();
  $("kpi-upgrade").textContent = fmtInt(upg.length);
  $("kpi-upgrade-sub").textContent = mesesUpg.length
    ? `${fmtBRL(state.upgMin)}+ em ${mesesUpg[0]} … ${mesesUpg[mesesUpg.length-1]}`
    : "sem meses fechados";
  // Card da Home ignora o filtro da aba: mostra TODO mundo com fatura vencida,
  // quebrado entre bloqueadas (11d+) e em alerta (1-10d).
  const inad = marcasInadimplentes(0);
  const bloqN = inad.filter(e => inadStatus(e.diasAtraso) === "bloqueado").length;
  $("kpi-inad").textContent = fmtInt(inad.length);
  $("kpi-inad-sub").textContent =
    `${fmtInt(bloqN)} bloqueadas · ${fmtInt(inad.length - bloqN)} em alerta · `
    + `${fmtBRL(inad.reduce((s,e)=>s+(e.valorEmAberto||0),0))} em aberto`;
  $("kpi-cliques").textContent    = fmtInt(cliquesTot);
  $("kpi-links").textContent      = fmtInt(linksTot);
  $("kpi-marcas-link").textContent = fmtInt(marcasLink);
}

// ---------- TAB renderers ----------
function evolMensal(lista, campo) {
  // soma de campo (valTotal, valCartao, valPix) por mês acrosss lista
  const sum = {};
  for (const e of lista) for (const m of Object.keys(e.mensal||{})) {
    sum[m] = (sum[m]||0) + (e.mensal[m][campo]||0);
  }
  const meses = Object.keys(sum).sort();
  return { meses, vals: meses.map(m=>sum[m]) };
}

function renderTabGmv() {
  const lista = empresasFiltradas();
  const { meses, vals } = evolMensal(lista, "valTotal");
  makeChart("chart-gmv-evolucao", {
    type:"line",
    data:{labels:meses, datasets:[{label:"GMV", data:vals, borderColor:COLORS[0], backgroundColor:"rgba(108,92,231,.15)", fill:true, tension:.3}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{y:{ticks:{callback:v=>"R$"+(v/1000).toFixed(0)+"k"}}}}
  });
  // canal: Starter vs Parceiros no período
  let st=0, pa=0;
  for (const e of lista) {
    const v = bucket(e).valTotal;
    if (e.starter_interno) st+=v; else pa+=v;
  }
  makeChart("chart-gmv-canal", {
    type:"doughnut",
    data:{labels:["Starter Interno","Parceiros"], datasets:[{data:[st,pa], backgroundColor:[COLORS[0],COLORS[3]]}]},
    options:{responsive:true, maintainAspectRatio:false}
  });
  const rows = lista.map(e=>({...e,_g:bucket(e)})).filter(e=>e._g.valTotal>0).sort((a,b)=>b._g.valTotal-a._g.valTotal);
  renderTable("tbl-gmv", [
    {label:"Marca", fn:r=>r.name, sort:r=>r.name},
    {label:"CS", fn:r=>r.cs, sort:r=>r.cs},
    {label:"Canal", fn:r=>r.canal+(r.starter_interno?" (Int)":""),
      sort:r=>r.canal+(r.starter_interno?" (Int)":"")},
    {label:"GMV", cls:"num", fn:r=>fmtBRL(r._g.valTotal), sort:r=>r._g.valTotal},
    {label:"Pedidos", cls:"num", fn:r=>fmtInt(r._g.qtTotal), sort:r=>r._g.qtTotal},
    {label:"Mensalidade", cls:"num", fn:r=>fmtBRL(r.valor_mensal), sort:r=>r.valor_mensal||0},
  ], rows);
}

function renderTabVp() {
  const lista = empresasFiltradas();
  // evolução mensal: duas séries (Cartão e PIX) no mesmo gráfico
  const cMes = {}, pMes = {};
  for (const e of lista) for (const m of Object.keys(e.mensal||{})) {
    cMes[m] = (cMes[m]||0) + (e.mensal[m].valCartao||0);
    pMes[m] = (pMes[m]||0) + (e.mensal[m].valPix||0);
  }
  const meses = Object.keys({...cMes,...pMes}).sort();
  makeChart("chart-vp-evolucao", {
    type:"line",
    data:{labels:meses, datasets:[
      {label:"Cartão", data:meses.map(m=>cMes[m]||0),
       borderColor:"#6C5CE7", backgroundColor:"rgba(108,92,231,.1)", fill:true, tension:.4, pointRadius:3, pointBackgroundColor:"#6C5CE7"},
      {label:"PIX", data:meses.map(m=>pMes[m]||0),
       borderColor:"#00B894", backgroundColor:"rgba(0,184,148,.1)", fill:true, tension:.4, pointRadius:3, pointBackgroundColor:"#00B894"},
    ]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{position:"bottom"}},
             scales:{y:{ticks:{callback:v=>"R$"+(v/1000).toFixed(0)+"k"}}}}
  });
  // TOP 10 por VP total (cartão+PIX) no período
  const ranked = lista.map(e=>{
    const b = bucket(e);
    return {...e, _c:b.valCartao, _p:b.valPix, _qc:b.qtCartao, _qp:b.qtPix, _tot:b.valCartao+b.valPix, _qtot:b.qtCartao+b.qtPix};
  }).filter(e=>e._tot>0).sort((a,b)=>b._tot-a._tot);
  const top10 = ranked.slice(0,10);
  makeChart("chart-vp-top", {
    type:"bar",
    data:{labels:top10.map(e=>e.name), datasets:[
      {label:"Cartão", data:top10.map(e=>e._c), backgroundColor:"#6C5CE7"},
      {label:"PIX", data:top10.map(e=>e._p), backgroundColor:"#00B894"},
    ]},
    options:{indexAxis:"y", responsive:true, maintainAspectRatio:false,
             plugins:{legend:{position:"bottom"}},
             scales:{x:{stacked:true, ticks:{callback:v=>"R$"+(v/1000).toFixed(0)+"k"}}, y:{stacked:true}}}
  });
  renderTable("tbl-vp", [
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal},
    {label:"Status VP", fn:r=>vpBadge(r)},
    {label:"Cartão", cls:"num", fn:r=>fmtBRL(r._c)},
    {label:"Pedidos Cartão", cls:"num", fn:r=>fmtInt(r._qc)},
    {label:"PIX", cls:"num", fn:r=>fmtBRL(r._p)},
    {label:"Pedidos PIX", cls:"num", fn:r=>fmtInt(r._qp)},
    {label:"Total VP", cls:"num", fn:r=>fmtBRL(r._tot)},
  ], ranked);
}

function populaCadMesSelect() {
  const sel = $("filter-cad-mes");
  // coleta todos os meses presentes em produtosPorMes
  const mesSet = new Set();
  for (const e of D.empresas) for (const m of Object.keys(e.produtosPorMes||{})) mesSet.add(m);
  const meses = Array.from(mesSet).sort().reverse();
  const cur = state.cadMes;
  sel.innerHTML = `<option value="todos">Todos (total acumulado)</option>` +
    `<option value="primeiro">1º mês de cadastro de cada marca</option>` +
    meses.map(m=>`<option value="${m}">${m}</option>`).join("");
  sel.value = meses.includes(cur) || cur==="primeiro" || cur==="todos" ? cur : "todos";
}

function renderTabCadastro() {
  const lista0 = empresasFiltradas();
  // determina valor de "produtos" exibido baseado em cadMes
  const mode = state.cadMes;
  function valOf(e) {
    if (mode === "todos") return e.qtProdutos || 0;
    if (mode === "primeiro") return e.qtProdutos1oMes || 0;
    return (e.produtosPorMes||{})[mode] || 0;
  }
  const hint = mode === "todos" ? "Visão: total acumulado de produtos por marca"
    : mode === "primeiro" ? "Visão: produtos cadastrados no 1º mês (mês varia por marca)"
    : `Visão: produtos cadastrados em ${mode}`;
  $("cad-mes-hint").textContent = hint;
  const lista = lista0.map(e=>({...e, _v: valOf(e)})).filter(e=>e._v>0).sort((a,b)=>b._v-a._v);
  const top15 = lista.slice(0,15);
  makeChart("chart-cad-top", {
    type:"bar",
    data:{labels:top15.map(e=>e.name), datasets:[{label:"Produtos", data:top15.map(e=>e._v), backgroundColor:COLORS[2]}]},
    options:{indexAxis:"y", responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  const faixas = {"1-10":0,"11-50":0,"51-200":0,"201-500":0,"500+":0};
  for (const e of lista) {
    const q = e._v;
    if (q<=10) faixas["1-10"]++;
    else if (q<=50) faixas["11-50"]++;
    else if (q<=200) faixas["51-200"]++;
    else if (q<=500) faixas["201-500"]++;
    else faixas["500+"]++;
  }
  makeChart("chart-cad-faixa", {
    type:"doughnut",
    data:{labels:Object.keys(faixas), datasets:[{data:Object.values(faixas), backgroundColor:COLORS.slice(0,5)}]},
    options:{responsive:true, maintainAspectRatio:false}
  });
  const colVal = mode === "todos" ? "Produtos (total)"
    : mode === "primeiro" ? "Produtos no 1º mês"
    : `Produtos em ${mode}`;
  renderTable("tbl-cadastro", [
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal},
    {label:colVal, cls:"num", fn:r=>fmtInt(r._v)},
    {label:"1º mês", fn:r=>r.primeiroMesCadastro||"—"},
    {label:"1º mês qtd", cls:"num", fn:r=>fmtInt(r.qtProdutos1oMes||0)},
    {label:"Total acumulado", cls:"num", fn:r=>fmtInt(r.qtProdutos||0)},
  ], lista);
}

function diasEntre(d1, d2) {
  if (!d1 || !d2) return null;
  const a = new Date(d1), b = new Date(d2);
  if (isNaN(a) || isNaN(b)) return null;
  return Math.floor((b - a) / 86400000);
}

function renderTabPrimeiras(n) {
  const lista = empresasFiltradas().filter(e => !state.soCoorte || e.marcoConfiavel);
  const campo = n===5 ? "mes5Vendas" : "mes25Vendas";
  const elHint = $(n===5 ? "p5-coorte-hint" : "p25-coorte-hint");
  if (elHint) elHint.textContent = `Piso de dados: ${PISO_DADOS() || "—"}. `
    + (state.soCoorte ? `Mostrando só marcas que entraram a partir daí (${lista.length} de ${empresasFiltradas().length}).`
                      : `Mostrando todas — para quem entrou antes do piso, o marco NÃO é "primeiras ${n} vendas".`);
  const mesK = mesAtualChave();
  // evolução: marcas atingindo N por mes
  const porMes = {};
  for (const e of lista) {
    const m = e[campo];
    if (m) porMes[m] = (porMes[m]||0) + 1;
  }
  const meses = Object.keys(porMes).sort();
  const cId = n===5 ? "chart-p5-evolucao" : "chart-p25-evolucao";
  makeChart(cId, {
    type:"bar",
    data:{labels:meses, datasets:[{label:`marcas atingindo ${n} vendas`, data:meses.map(m=>porMes[m]), backgroundColor:COLORS[6]}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  if (n===5) {
    // por canal
    let st=0, pa=0;
    for (const e of lista) if (e.mes5Vendas) { if (e.starter_interno) st++; else pa++; }
    makeChart("chart-p5-canal", {
      type:"doughnut",
      data:{labels:["Starter","Parceiros"], datasets:[{data:[st,pa], backgroundColor:[COLORS[0],COLORS[3]]}]},
      options:{responsive:true, maintainAspectRatio:false}
    });
  } else {
    // tempo 1a venda paga até 25 vendas pagas
    const tempos = [];
    for (const e of lista) {
      const venda1 = e.primeiraVendaPaga || e.primeiraVenda;
      if (!e.data25Vendas || !venda1) continue;
      const dv = new Date(venda1);
      const dm = new Date(e.data25Vendas);
      const dias = Math.floor((dm-dv)/86400000);
      if (dias>=0 && dias<400) tempos.push(dias);
    }
    const faixas = {"0-30d":0,"31-60d":0,"61-90d":0,"91-180d":0,"180d+":0};
    for (const d of tempos) {
      if (d<=30) faixas["0-30d"]++;
      else if (d<=60) faixas["31-60d"]++;
      else if (d<=90) faixas["61-90d"]++;
      else if (d<=180) faixas["91-180d"]++;
      else faixas["180d+"]++;
    }
    makeChart("chart-p25-tempo", {
      type:"bar",
      data:{labels:Object.keys(faixas), datasets:[{label:"marcas", data:Object.values(faixas), backgroundColor:COLORS[5]}]},
      options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
    });
  }
  const campoData = n===5 ? "data5Vendas" : "data25Vendas";
  const rows = lista.filter(e=>mesMatch(e[campo])).map(e => {
    const venda1Paga = e.primeiraVendaPaga || e.primeiraVenda;
    const ate1aVenda = diasEntre((e.dataEntrada||"").slice(0,10), venda1Paga);
    // usa a data exata em que bateu N (antes era aproximado pelo dia 15 do mês)
    const ateNVendas = diasEntre((e.dataEntrada||"").slice(0,10), e[campoData] || null);
    const permanencia = diasEntre((e.dataEntrada||"").slice(0,10), new Date().toISOString().slice(0,10));
    const mesEntrada = (e.dataEntrada||"").slice(0,7);
    return {...e, _venda1Paga:venda1Paga, _ate1aVenda:ate1aVenda, _ateN:ateNVendas,
            _perm:permanencia, _noMesEntrada: !!e[campo] && e[campo] === mesEntrada};
  });
  const cols = [
    {label:"Marca", fn:r=>r.name, sort:r=>r.name},
    {label:"CS", fn:r=>r.cs, sort:r=>r.cs},
    {label:"Canal", fn:r=>r.canal, sort:r=>r.canal},
    {label:"Data entrada", fn:r=>(r.dataEntrada||"—").slice(0,10), sort:r=>r.dataEntrada||""},
    {label:"Permanência (d)", cls:"num", fn:r=>r._perm??"—", sort:r=>r._perm??-1},
    {label:"1ª venda paga", fn:r=>r._venda1Paga||"—", sort:r=>r._venda1Paga||""},
    {label:"Dias até 1ª venda paga", cls:"num", fn:r=>r._ate1aVenda??"—", sort:r=>r._ate1aVenda??-1},
    {label:`Data que bateu ${n}`, fn:r=>r[campoData]||"—", sort:r=>r[campoData]||""},
    {label:"No mês de entrada?", fn:r=>r._noMesEntrada
        ? `<span class="pill pill-ok">sim</span>`
        : `<span class="pill pill-warn">não</span>`, sort:r=>r._noMesEntrada?1:0},
    {label:`Dias até ${n} vendas pagas`, cls:"num", fn:r=>r._ateN??"—", sort:r=>r._ateN??-1},
  ];
  renderTable(n===5?"tbl-p5":"tbl-p25", cols, rows);
}

function renderTabTop(kind) { // "starter" ou "parceiros"
  const lista = empresasFiltradas();
  const filtroFn = kind==="starter"
    ? e=>e.starter_interno
    : e=>!e.starter_interno && !PARC_EXCL.has((e.partner_raw||"").toLowerCase());
  const ranked = lista.filter(filtroFn).map(e=>({...e,_g:bucket(e)})).sort((a,b)=>b._g.valTotal-a._g.valTotal);
  const top = ranked.slice(0,10);
  const totalCanal = ranked.reduce((s,e)=>s+e._g.valTotal, 0);
  const prefix = kind==="starter" ? "tops" : "topp";
  makeChart(`chart-${prefix}-bar`, {
    type:"bar",
    data:{labels:top.map(e=>e.name), datasets:[{label:"GMV", data:top.map(e=>e._g.valTotal), backgroundColor:COLORS[kind==='starter'?3:8]}]},
    options:{indexAxis:"y", responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{x:{ticks:{callback:v=>"R$"+(v/1000).toFixed(0)+"k"}}}}
  });
  const outros = totalCanal - top.reduce((s,e)=>s+e._g.valTotal, 0);
  makeChart(`chart-${prefix}-pie`, {
    type:"pie",
    data:{labels:[...top.map(e=>e.name),"Outros"], datasets:[{data:[...top.map(e=>e._g.valTotal), Math.max(0,outros)], backgroundColor:COLORS}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  const tblId = kind==="starter" ? "tbl-topstarter" : "tbl-topparc";
  renderTable(tblId, [
    {label:"#", fn:r=>r._rank},
    {label:"Marca", fn:r=>r.name},
    ...(kind==="parceiros"?[{label:"Parceiro", fn:r=>r.partner_raw||"—"}]:[]),
    {label:"GMV", cls:"num", fn:r=>fmtBRL(r._g.valTotal)},
    {label:"Pedidos", cls:"num", fn:r=>fmtInt(r._g.qtTotal)},
  ], top.map((e,i)=>({...e,_rank:i+1})));
}

function countBy(lista, fn) {
  const acc = {};
  for (const e of lista) { const k = fn(e) || "(vazio)"; acc[k] = (acc[k]||0)+1; }
  return acc;
}

function renderTabSemVp(canalSemVp) { // canalSemVp ignored — render both charts
  const lista = empresasFiltradas().filter(e=>!e.temVPAtivo);
  const porCs = countBy(lista, e=>e.cs);
  makeChart("chart-semvp-cs", {
    type:"bar",
    data:{labels:Object.keys(porCs), datasets:[{label:"marcas", data:Object.values(porCs), backgroundColor:COLORS[3]}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  const porCanal = countBy(lista, e=>e.starter_interno?"Starter":"Parceiros");
  makeChart("chart-semvp-canal", {
    type:"doughnut",
    data:{labels:Object.keys(porCanal), datasets:[{data:Object.values(porCanal), backgroundColor:[COLORS[0],COLORS[3]]}]},
    options:{responsive:true, maintainAspectRatio:false}
  });
  renderTable("tbl-semvp", [
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal},
    {label:"Status VP", fn:r=>vpBadge(r)},
    {label:"Mensalidade", cls:"num", fn:r=>fmtBRL(r.valor_mensal)},
    {label:"1ª venda", fn:r=>r.primeiraVenda||"—"},
  ], lista.sort((a,b)=>a.name.localeCompare(b.name)).map(e=>({...e,_alert:true})));
}

function renderTabSemFrete() {
  const lista = empresasFiltradas().filter(e=>!e.temFreteAtivo);
  const porCs = countBy(lista, e=>e.cs);
  makeChart("chart-semfrete-cs", {
    type:"bar",
    data:{labels:Object.keys(porCs), datasets:[{label:"marcas", data:Object.values(porCs), backgroundColor:COLORS[9]}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  const porCanal = countBy(lista, e=>e.starter_interno?"Starter":"Parceiros");
  makeChart("chart-semfrete-canal", {
    type:"doughnut",
    data:{labels:Object.keys(porCanal), datasets:[{data:Object.values(porCanal), backgroundColor:[COLORS[0],COLORS[3]]}]},
    options:{responsive:true, maintainAspectRatio:false}
  });
  renderTable("tbl-semfrete", [
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal},
    {label:"Mensalidade", cls:"num", fn:r=>fmtBRL(r.valor_mensal)},
  ], lista.sort((a,b)=>a.name.localeCompare(b.name)).map(e=>({...e,_alert:true})));
}

// As 4 origens da aba (regra fechada com a Laura em 13/08/2026 — ver
// _eventos_ambiente em build_data.py). Nenhum sinal sozinho cobria a lista das
// vendedoras: marca sem cobrança na Iugu não tem gap de pagamento, e cliente que
// volta com cadastro novo não tem histórico no domínio antigo.
// "Ambiente religado" junta duas deteccoes (voltou a pagar depois de pular ciclo,
// e religado sem pagamento na data). A Laura pediu um rotulo so' em 13/08: pra ela
// e' o mesmo evento de negocio, muda so' como a Vesti ficou sabendo -- e em 13/08
// pediu tambem pra tirar a coluna Detalhe, que era o unico lugar onde a diferenca
// aparecia. O campo `detalhe` segue em dashboard_data.js, so' nao e' exibido.
const ORIGENS_REATIV = [
  {id:"criacao",     nome:"Ambiente ativado",   cor:"#00B894"},
  {id:"religamento", nome:"Ambiente religado",  cor:"#E17055"},
  {id:"retorno",     nome:"1ª fatura tardia",   cor:"#6C5CE7"},
];
const somaFaixas = (o) => Object.values(o).reduce((s,v)=>s+v, 0);
const nomeOrigem = (id) => (ORIGENS_REATIV.find(o=>o.id===id)||{}).nome || id;

// eventos de reativação/ativação da empresa no período e no filtro de origem
function eventosReativ(e) {
  return (e.ambienteEventos || []).filter(ev =>
    ev.tipo !== "desligamento" &&
    mesMatch(ev.mes) &&
    (state.reativOrigem === "todas" || ev.origem === state.reativOrigem));
}

// clique numa barra filtra a tabela por canal + origem; clicar de novo limpa
function setReativDrill(canal, origem) {
  const d = state.reativDrill;
  state.reativDrill = (d && d.canal === canal && d.origem === origem) ? null : {canal, origem};
  renderActiveTab();
}
window.limparReativDrill = () => { state.reativDrill = null; renderActiveTab(); };

function renderTabReativ() {
  const lista = empresasFiltradas();
  const mesK = mesAtualChave();
  // evolução mensal: soma de reativações por mês acrosss lista
  // quanto tempo a marca ficou sem pagar, por canal (empilhado)
  const porCanal = {};
  for (const e of lista) {
    const c = canalDe(e);
    for (const ev of eventosReativ(e)) {
      const slot = porCanal[c] || (porCanal[c] = Object.fromEntries(ORIGENS_REATIV.map(o=>[o.nome,0])));
      slot[nomeOrigem(ev.origem)]++;
    }
  }
  const canaisComDado = Object.keys(porCanal)
    .sort((a,b)=>somaFaixas(porCanal[b])-somaFaixas(porCanal[a]));
  makeChart("chart-reativ-evolucao", {
    type:"bar",
    plugins:[valoresNaBarra],   // numeros visiveis sem passar o mouse
    data:{
      labels: canaisComDado,
      datasets: ORIGENS_REATIV.map(o=>({
        label: o.nome,
        data: canaisComDado.map(c=>porCanal[c][o.nome]),
        backgroundColor: o.cor,
      })),
    },
    options:{responsive:true, maintainAspectRatio:false,
      layout:{padding:{top:18}},   // espaco pro total desenhado acima da barra
      onClick:(evt, els, chart)=>{
        if (!els.length) return;
        const el = els[0];
        setReativDrill(chart.data.labels[el.index], chart.data.datasets[el.datasetIndex].label);
      },
      onHover:(evt, els)=>{
        if (evt.native && evt.native.target) evt.native.target.style.cursor = els.length ? "pointer" : "default";
      },
      plugins:{legend:{position:"bottom"},
        tooltip:{callbacks:{
          label:c=>`${c.dataset.label}: ${c.raw} marca(s)`,
          footer:items=>`total do canal: ${items.reduce((s,i)=>s+i.raw,0)} · clique para ver as marcas`}}},
      scales:{x:{stacked:true}, y:{stacked:true, ticks:{precision:0}, title:{display:true, text:"marcas"}}}}
  });
  // uma linha por EVENTO: marca, por que entrou (origem) e quando
  const drill = state.reativDrill;
  const rows = [];
  for (const e of lista) {
    for (const ev of eventosReativ(e)) {
      if (drill) {                              // clique numa barra do gráfico
        if (canalDe(e) !== drill.canal) continue;
        if (nomeOrigem(ev.origem) !== drill.origem) continue;
      }
      rows.push({...e, _origem: ev.origem, _data: ev.data,
                 _dias: ev.dias || 0, _novo: ev.origem === "criacao",
                 _alert: (ev.dias || 0) >= 91});
    }
  }
  rows.sort((a,b)=>(b._data||"").localeCompare(a._data||""));
  const el = $("reativ-resumo");
  if (el) {
    const marcas = new Set(rows.map(r=>r.domain_id)).size;
    const novos = rows.filter(r=>r._novo).length;
    el.textContent = rows.length
      ? `${rows.length} eventos de ${marcas} marcas em ${mesK||"—"} · `
        + `${novos} ativações e ${rows.length-novos} reativações`
      : "Nenhum evento com esses filtros.";
  }
  const elD = $("reativ-drill");
  if (elD) {
    elD.innerHTML = drill
      ? `<span class="drill-chip">${drill.canal} · ${drill.origem}
           <button type="button" onclick="limparReativDrill()" title="Ver todos de novo">×</button></span>
         <span class="drill-hint">mostrando só as marcas desse pedaço da barra</span>`
      : `<span class="drill-hint">Clique em um pedaço da barra para ver as marcas daquele canal e origem.</span>`;
  }
  renderTable("tbl-reativ", [
    {label:"Marca", fn:r=>r.name, sort:r=>r.name},
    {label:"CS", fn:r=>r.cs, sort:r=>r.cs},
    {label:"Canal", fn:r=>r.canal, sort:r=>r.canal},
    {label:"O que aconteceu", fn:r=>nomeOrigem(r._origem), sort:r=>nomeOrigem(r._origem)},
    {label:"Quando", fn:r=>r._data||"—", sort:r=>r._data||""},
    {label:"Mensalidade", cls:"num", fn:r=>{
      const v = r.valor_plano || r.valor_mensal; return v ? fmtBRL(v) : "—";
    }, sort:r=>r.valor_plano || r.valor_mensal || 0},
    {label:"Retornos acumulados", cls:"num", fn:r=>fmtInt(r.totalReativ||0), sort:r=>r.totalReativ||0},
    {label:"Maior ausência já tida", cls:"num", fn:r=>fmtInt(r.maiorAusencia||0), sort:r=>r.maiorAusencia||0},
  ], rows);
}

function renderTabTravadas() {
  const hoje = new Date();
  const lista = empresasFiltradas().map(e => {
    const ped = e.primeiroPedidoCadastrado ? new Date(e.primeiroPedidoCadastrado) : null;
    const venda = e.primeiraVenda ? new Date(e.primeiraVenda) : null;
    const dias = ped ? Math.floor((hoje-ped)/86400000) : null;
    return {...e, _ped:ped, _venda:venda, _dias:dias};
  }).filter(e => e.modulos === undefined || (e.modulos||"").toLowerCase().includes("vendas"))
    .filter(e => (e._ped && !e._venda && e._dias > 14) || (e._ped && (e.qtProdutos||0) === 0))
    .sort((a,b)=>(b._dias||0)-(a._dias||0));
  // faixas dias
  const faixas = {"15-30d":0,"31-60d":0,"61-90d":0,"91-180d":0,"180d+":0};
  for (const e of lista) {
    const d = e._dias || 0;
    if (d<=30) faixas["15-30d"]++;
    else if (d<=60) faixas["31-60d"]++;
    else if (d<=90) faixas["61-90d"]++;
    else if (d<=180) faixas["91-180d"]++;
    else faixas["180d+"]++;
  }
  makeChart("chart-trav-dias", {
    type:"bar",
    data:{labels:Object.keys(faixas), datasets:[{label:"marcas", data:Object.values(faixas), backgroundColor:COLORS[3]}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  const porCanal = countBy(lista, e=>e.starter_interno?"Starter":isUemtel(e)?"Uemtel":isAtta(e)?"Atta":"Parceiros");
  makeChart("chart-trav-canal", {
    type:"bar",
    data:{labels:Object.keys(porCanal), datasets:[{label:"marcas", data:Object.values(porCanal), backgroundColor:[COLORS[0],COLORS[4],COLORS[8],COLORS[3]]}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  renderTable("tbl-travadas", [
    {label:"Marca", fn:r=>r.name, sort:r=>r.name},
    {label:"CS", fn:r=>r.cs, sort:r=>r.cs},
    {label:"Canal", fn:r=>r.canal, sort:r=>r.canal},
    {label:"Data entrada", fn:r=>(r.dataEntrada||"—").slice(0,10), sort:r=>r.dataEntrada||""},
    {label:"1º pedido cadastrado", fn:r=>(r.primeiroPedidoCadastrado||"—").slice(0,10), sort:r=>r.primeiroPedidoCadastrado||""},
    {label:"1ª venda", fn:r=>(r.primeiraVenda||"—").slice(0,10), sort:r=>r.primeiraVenda||""},
    {label:"Produtos", cls:"num", fn:r=>fmtInt(r.qtProdutos||0), sort:r=>r.qtProdutos||0},
    {label:"Dias travada", cls:"num", fn:r=>r._dias??"—", sort:r=>r._dias??-1},
  ], lista.map(e=>({...e,_alert:true})));
}

function renderTabInadimplentes() {
  const lista = marcasInadimplentes();
  const total = lista.reduce((s,e)=>s+(e.valorEmAberto||0), 0);
  const bloq = lista.filter(e => inadStatus(e.diasAtraso) === "bloqueado");
  const alerta = lista.filter(e => inadStatus(e.diasAtraso) === "alerta");
  const hint = $("inad-hint");
  // faturas que nao casaram com nenhuma marca do painel -- melhor dizer do que sumir
  const fora = (D.inadSemDominio || {});
  const foraTxt = (fora.qtFaturas || 0)
    ? ` · <span class="hint">${fmtInt(fora.qtFaturas)} faturas sem marca no painel (${fmtBRL(fora.valor||0)})</span>`
    : "";
  if (hint) hint.innerHTML = lista.length
    ? `${fmtInt(lista.length)} marcas · ${fmtBRL(total)} em aberto `
      + `<span class="pill pill-inad-bloq">${fmtInt(bloq.length)} bloqueadas</span>`
      + `<span class="pill pill-inad-alerta">${fmtInt(alerta.length)} em alerta</span>` + foraTxt
    : "nenhuma marca acima dessa régua";

  // faixas cortadas na regua das tags: a primeira e' o amarelo (alerta), o resto e' bloqueio
  if (!lista.length) {
    const recado = "Nenhuma marca com fatura vencida nesses filtros";
    chartVazio("chart-inad-faixa", recado);
    chartVazio("chart-inad-cs", recado);
    renderTable("tbl-inadimplentes", [{label:"Marca", fn:r=>r.name}], []);
    return;
  }
  const faixas = {"1-10d (alerta)":0,"11-30d":0,"31-60d":0,"61-90d":0,"90d+":0};
  for (const e of lista) {
    const d = e.diasAtraso || 0;
    if (d<=INAD_LIMITE_ALERTA) faixas["1-10d (alerta)"]++;
    else if (d<=30) faixas["11-30d"]++;
    else if (d<=60) faixas["31-60d"]++;
    else if (d<=90) faixas["61-90d"]++;
    else faixas["90d+"]++;
  }
  makeChart("chart-inad-faixa", {
    type:"bar",
    plugins:[valoresNaBarra],   // numeros visiveis sem passar o mouse
    data:{labels:Object.keys(faixas), datasets:[{label:"marcas", data:Object.values(faixas),
      backgroundColor:["#F5C518","#E74C3C","#D63031","#C0392B","#8E1B14"]}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  const porCs = {};
  for (const e of lista) {
    const cs = e.cs || "sem CS";
    porCs[cs] = (porCs[cs]||0) + (e.valorEmAberto||0);
  }
  const csOrd = Object.keys(porCs).sort((a,b)=>porCs[b]-porCs[a]);
  makeChart("chart-inad-cs", {
    type:"bar",
    plugins:[valoresNaBarra],
    data:{labels:csOrd, datasets:[{label:"em aberto", data:csOrd.map(c=>porCs[c]), backgroundColor:COLORS[3]}]},
    options:{responsive:true, maintainAspectRatio:false,
      plugins:{legend:{display:false}, valoresNaBarra:{fmt:fmtBRLCurto}},
      scales:{y:{ticks:{callback:v=>"R$"+fmtInt(v)}}}}
  });
  renderTable("tbl-inadimplentes", [
    {label:"Marca", fn:r=>r.name, sort:r=>r.name},
    {label:"Situação", fn:r=>inadBadge(r.diasAtraso), sort:r=>r.diasAtraso||0},
    {label:"CS", fn:r=>r.cs, sort:r=>r.cs},
    {label:"Canal", fn:r=>r.canal, sort:r=>r.canal},
    {label:"Venc. mais antigo", fn:r=>r.vencimentoMaisAntigo||"—", sort:r=>r.vencimentoMaisAntigo||""},
    {label:"Dias em atraso", cls:"num", fn:r=>fmtInt(r.diasAtraso||0), sort:r=>r.diasAtraso||0},
    {label:"Faturas vencidas", cls:"num", fn:r=>fmtInt(r.faturasVencidas||0), sort:r=>r.faturasVencidas||0},
    {label:"Subconta Iugu", fn:r=>(r.subcontasIugu||[]).join(", ")||"—", sort:r=>(r.subcontasIugu||[])[0]||""},
    {label:"Valor em aberto", cls:"num", fn:r=>fmtBRL(r.valorEmAberto||0), sort:r=>r.valorEmAberto||0},
    {label:"Mensalidade", cls:"num", fn:r=>fmtBRL(r.valor_mensal), sort:r=>r.valor_mensal||0},
  ], lista.map(e=>({...e, _alert: inadStatus(e.diasAtraso) === "bloqueado"})));
}

function renderTabLinks() {
  const lista = empresasFiltradas();
  // evolução mensal — duas linhas: cliques e links
  const cliquesMes = {}, linksMes = {};
  for (const e of lista) {
    for (const [m,q] of Object.entries(e.cliquesPorMes||{})) cliquesMes[m] = (cliquesMes[m]||0) + q;
    for (const [m,q] of Object.entries(e.linksPorMes||{}))   linksMes[m]   = (linksMes[m]||0) + q;
  }
  const meses = Array.from(new Set([...Object.keys(cliquesMes), ...Object.keys(linksMes)])).sort();
  const labels = meses.map(m => m.substring(5)+"/"+m.substring(2,4));
  makeChart("chart-links-evolucao", {
    type:"line",
    data:{labels, datasets:[
      {label:"Cliques", data:meses.map(m=>cliquesMes[m]||0),
       borderColor:"#06B6D4", backgroundColor:"rgba(6,182,212,0.1)",
       fill:true, tension:.4, pointRadius:4, pointBackgroundColor:"#06B6D4", yAxisID:"y"},
      {label:"Links Enviados", data:meses.map(m=>linksMes[m]||0),
       borderColor:"#7C3AED", backgroundColor:"rgba(124,58,237,0.1)",
       fill:true, tension:.4, pointRadius:5, pointBackgroundColor:"#7C3AED",
       borderWidth:3, yAxisID:"y1"},
    ]},
    options:{
      responsive:true, maintainAspectRatio:false,
      plugins:{
        legend:{position:"bottom"},
        tooltip:{callbacks:{label:(c)=>`${c.dataset.label}: ${c.raw.toLocaleString("pt-BR")}`}}
      },
      scales:{
        y:  {type:"linear", position:"left",  title:{display:true, text:"Cliques", color:"#06B6D4"},
             ticks:{color:"#06B6D4"}},
        y1: {type:"linear", position:"right", title:{display:true, text:"Links Enviados", color:"#7C3AED"},
             ticks:{color:"#7C3AED", precision:0}, grid:{drawOnChartArea:false}, beginAtZero:true},
      }
    }
  });
  // breakdown por canal (Starter/Uemtel/Atta/Parceiros)
  const porCanal = {};
  for (const e of lista) {
    const c = canalDe(e) || "Outros";
    const slot = porCanal[c] || (porCanal[c] = {cliques:0, links:0});
    slot.cliques += e.cliquesTotal || 0;
    slot.links   += e.linksCompartilhados || 0;
  }
  const canais = Object.keys(porCanal).sort();
  const canalCor = {Starter:COLORS[0], Uemtel:COLORS[3], Atta:COLORS[5], Parceiros:COLORS[7], Outros:"#999"};
  makeChart("chart-links-cliques-canal", {
    type:"doughnut",
    data:{labels:canais, datasets:[{data:canais.map(c=>porCanal[c].cliques), backgroundColor:canais.map(c=>canalCor[c]||COLORS[10])}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{position:"bottom"},
      tooltip:{callbacks:{label:c=>`${c.label}: ${c.raw.toLocaleString("pt-BR")} cliques`}}}}
  });
  makeChart("chart-links-env-canal", {
    type:"doughnut",
    data:{labels:canais, datasets:[{data:canais.map(c=>porCanal[c].links), backgroundColor:canais.map(c=>canalCor[c]||COLORS[10])}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{position:"bottom"},
      tooltip:{callbacks:{label:c=>`${c.label}: ${c.raw.toLocaleString("pt-BR")} links`}}}}
  });
  // top 10 por cliques
  const ranked = lista.filter(e=>(e.cliquesTotal||0)>0).sort((a,b)=>b.cliquesTotal-a.cliquesTotal);
  const top10 = ranked.slice(0,10);
  makeChart("chart-links-top", {
    type:"bar",
    data:{labels:top10.map(e=>e.name), datasets:[{label:"cliques", data:top10.map(e=>e.cliquesTotal), backgroundColor:COLORS[10]}]},
    options:{indexAxis:"y", responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  renderTable("tbl-links", [
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal},
    {label:"Links Enviados", cls:"num", fn:r=>fmtInt(r.linksCompartilhados||0), sort:r=>r.linksCompartilhados||0},
    {label:"Cliques nos Links", cls:"num", fn:r=>fmtInt(r.cliquesTotal||0), sort:r=>r.cliquesTotal||0},
  ], ranked);
}

// ---------- Upgrade: 300k+ nos 3 ultimos meses FECHADOS ----------
// Independe do filtro de periodo de proposito: e' uma lista de acao comercial,
// precisa ser sempre a mesma janela pra todo mundo comparar a mesma coisa.
function ultimos3MesesFechados() {
  const hoje = new Date();
  const mesAtual = `${hoje.getFullYear()}-${String(hoje.getMonth()+1).padStart(2,"0")}`;
  const fechados = (D.mesesList || []).filter(m => m < mesAtual).sort();
  return fechados.slice(-3);
}
function gmvJanela(e, meses) {
  let v = 0;
  for (const m of meses) v += (e.mensal || {})[m]?.valTotal || 0;
  return v;
}
function pedidosJanela(e, meses) {
  let q = 0;
  for (const m of meses) q += (e.mensal || {})[m]?.qtTotal || 0;
  return q;
}
function marcasUpgrade() {
  const meses = ultimos3MesesFechados();
  const min = state.upgMin;
  return empresasFiltradas()
    // mensalidade de referencia: o plano contratado; se nao houver, a ultima fatura paga
    .map(e => ({...e, _g3: gmvJanela(e, meses), _q3: pedidosJanela(e, meses),
                _mens: e.valor_plano || e.valor_mensal || 0}))
    .filter(e => e._g3 >= min)
    .sort((a,b) => b._g3 - a._g3);
}

function renderTabUpgrade() {
  const meses = ultimos3MesesFechados();
  const lista = marcasUpgrade();
  $("upg-janela").textContent = meses.length ? meses.join(" + ") : "3 últimos meses fechados";
  $("upg-hint").textContent = `${lista.length} marcas · janela ${meses.join(", ") || "—"}`;

  const top15 = lista.slice(0, 15);
  makeChart("chart-upg-top", {
    type:"bar",
    data:{labels:top15.map(e=>e.name), datasets:[{label:"GMV 3 meses", data:top15.map(e=>e._g3), backgroundColor:COLORS[1]}]},
    options:{indexAxis:"y", responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
             scales:{x:{ticks:{callback:v=>"R$"+(v/1000).toFixed(0)+"k"}}}}
  });
  // faixas de mensalidade: quem paga pouco e fatura muito e' o alvo do upgrade
  const faixas = {"sem valor":0,"até R$ 400":0,"R$ 401-700":0,"R$ 701-1.200":0,"R$ 1.200+":0};
  for (const e of lista) {
    const v = e._mens || 0;
    if (!v) faixas["sem valor"]++;
    else if (v <= 400) faixas["até R$ 400"]++;
    else if (v <= 700) faixas["R$ 401-700"]++;
    else if (v <= 1200) faixas["R$ 701-1.200"]++;
    else faixas["R$ 1.200+"]++;
  }
  makeChart("chart-upg-mens", {
    type:"doughnut",
    data:{labels:Object.keys(faixas), datasets:[{data:Object.values(faixas),
          backgroundColor:["#B2BEC3",COLORS[3],COLORS[2],COLORS[0],COLORS[1]]}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{position:"bottom"}}}
  });

  renderTable("tbl-upgrade", [
    {label:"Marca", fn:r=>r.name, sort:r=>r.name},
    {label:"CS", fn:r=>r.cs, sort:r=>r.cs},
    {label:"Canal", fn:r=>r.canal, sort:r=>r.canal},
    {label:"Plano atual", fn:r=>r.plano||"—", sort:r=>r.plano||""},
    {label:"Mensalidade contratada", cls:"num", fn:r=>r._mens?fmtBRL(r._mens):"—", sort:r=>r._mens||0},
    {label:"Última fatura paga", cls:"num", fn:r=>r.valor_mensal?fmtBRL(r.valor_mensal):"—", sort:r=>r.valor_mensal||0},
    {label:"Filiais", cls:"num", fn:r=>fmtInt(r.qtdFiliais||0), sort:r=>r.qtdFiliais||0},
    {label:"GMV 3 meses", cls:"num", fn:r=>fmtBRL(r._g3), sort:r=>r._g3},
    {label:"Pedidos 3 meses", cls:"num", fn:r=>fmtInt(r._q3), sort:r=>r._q3},
    // quantas vezes o GMV do trimestre cabe na mensalidade: quanto maior, mais defasado o plano
    {label:"GMV ÷ mensalidade", cls:"num",
     fn:r=>r._mens?fmtInt(Math.round(r._g3/r._mens))+"x":"—",
     sort:r=>r._mens?r._g3/r._mens:-1},
    {label:"Status VP", fn:r=>vpBadge(r)},
  ], lista);
}

// ---------- Tamanho da marca (fila de upgrade de plano) ----------
// Vem de tamanho_marca.py: seguidores do Instagram + lojas físicas no Google Maps,
// a partir da planilha "Mensalidade até 400". A ideia é achar marca que ficou
// grande mas continua num plano antigo e barato.
const PORTES = ["Muito grande", "Grande", "Media", "Pequena", "Indefinido"];
const PORTE_COR = {"Muito grande":"#D63031", "Grande":"#E17055", "Media":"#FDCB6E",
                   "Pequena":"#74B9FF", "Indefinido":"#B2BEC3"};
const PORTE_PESO = {"Muito grande":4, "Grande":3, "Media":2, "Pequena":1, "Indefinido":0};

// Defasagem = marca grande pagando pouco. Quanto maior o porte e menor a
// mensalidade, mais alta a prioridade de ligar oferecendo upgrade.
function defasagem(e) {
  const peso = PORTE_PESO[e.porte] || 0;
  if (peso < 3) return 0;                       // só Grande e Muito grande entram na fila
  const mens = e.valor_plano || e.valor_mensal || 0;
  if (!mens) return peso;                       // sem mensalidade conhecida: mantém na fila
  return peso * Math.max(1, 400 / mens);
}

// Primeiro mês de 2025 em diante que EXISTE na base. Os dados começam em
// jul/2025 (piso da migração pro BigQuery, sem backfill), então "GMV desde 2025"
// na prática conta a partir daí — o rótulo da coluna diz de quando é a soma pra
// ninguém ler como se fosse o ano inteiro.
function inicioGmv2025() {
  const meses = (DADOS.mesesList || []).filter(m => m >= "2025-01");
  return meses.length ? meses[0] : "2025-01";
}
function gmvDesde(e, desde) {
  let v = 0;
  for (const [m, dados] of Object.entries(e.mensal || {})) {
    if (m >= desde) v += dados.valTotal || 0;
  }
  return v;
}
const MES_EXT = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"];
const rotuloMes = (m) => `${MES_EXT[Number(m.slice(5,7)) - 1]}/${m.slice(0,4)}`;

function marcasComTamanho() {
  const desde = inicioGmv2025();
  return empresasFiltradas()
    .filter(e => e.porte && e.porte !== "Indefinido")
    .map(e => ({...e, _mens: e.valor_plano || e.valor_mensal || 0, _def: defasagem(e),
                _gmv25: gmvDesde(e, desde)}));
}

function renderTabTamanho() {
  const todas = marcasComTamanho();
  const lista = state.portesSel.size
    ? todas.filter(e => state.portesSel.has(e.porte))
    : todas;
  const fila = todas.filter(e => e._def > 0).length;

  const el = $("tam-hint");
  if (el) {
    const coleta = todas.find(e => e.dataColetaTamanho)?.dataColetaTamanho || "—";
    el.textContent = `${todas.length} marcas com tamanho coletado · ${fila} na fila de upgrade `
      + `(Grande ou Muito grande) · GMV somado a partir de ${rotuloMes(inicioGmv2025())} `
      + `· coleta de ${coleta}`;
  }

  const porPorte = Object.fromEntries(PORTES.map(p=>[p,0]));
  for (const e of todas) porPorte[e.porte] = (porPorte[e.porte]||0) + 1;
  makeChart("chart-tam-porte", {
    type:"bar",
    data:{labels:PORTES, datasets:[{label:"marcas", data:PORTES.map(p=>porPorte[p]),
          backgroundColor:PORTES.map(p=>PORTE_COR[p])}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
             scales:{y:{ticks:{precision:0}}}}
  });

  // seguidores x mensalidade: o canto de cima à esquerda é a fila de upgrade
  // (muito seguidor, mensalidade baixa)
  makeChart("chart-tam-disp", {
    type:"scatter",
    data:{datasets: PORTES.filter(p=>porPorte[p]).map(p=>({
      label:p,
      data: todas.filter(e=>e.porte===p).map(e=>({x:e._mens||0, y:e.seguidores||0, m:e.name})),
      backgroundColor: PORTE_COR[p], pointRadius:6, pointHoverRadius:9,
    }))},
    options:{responsive:true, maintainAspectRatio:false,
      plugins:{legend:{position:"bottom"},
        tooltip:{callbacks:{label:c=>
          `${c.raw.m}: ${fmtInt(c.raw.y)} seguidores · ${c.raw.x?fmtBRL(c.raw.x):"sem mensalidade"}`}}},
      scales:{
        x:{title:{display:true,text:"mensalidade atual"}, ticks:{callback:v=>"R$"+v}},
        y:{title:{display:true,text:"seguidores no Instagram"},
           ticks:{callback:v=>v>=1000?(v/1000)+"k":v}}}}
  });

  renderTable("tbl-tamanho", [
    {label:"Marca", fn:r=>r.name, sort:r=>r.name},
    {label:"CS", fn:r=>r.cs, sort:r=>r.cs},
    {label:"Canal", fn:r=>r.canal, sort:r=>r.canal},
    {label:"Porte", fn:r=>r.porte, sort:r=>PORTE_PESO[r.porte]||0},
    {label:"Seguidores", cls:"num",
     fn:r=>r.seguidores
        ? fmtInt(r.seguidores) + (r.origemSeguidores==="manual" ? " (manual)" : "")
        : (r.seguidoresDesconhecidos?"perfil não achado":"—"),
     sort:r=>r.seguidores||0},
    {label:`GMV desde ${rotuloMes(inicioGmv2025())}`, cls:"num",
     fn:r=>r._gmv25?fmtBRL(r._gmv25):"—", sort:r=>r._gmv25||0},
    {label:"Loja física", fn:r=>r.temLojaFisica||"—", sort:r=>r.temLojaFisica||""},
    // teto da busca: "5+" avisa que pode haver mais lojas do que o Maps devolveu
    {label:"Unidades", cls:"num",
     fn:r=>fmtInt(r.qtdUnidades||0) + (r.unidadesNoTeto?"+":""), sort:r=>r.qtdUnidades||0},
    {label:"O que o Maps achou", fn:r=>r.lojasEncontradas||"—", sort:r=>r.lojasEncontradas||""},
    {label:"Plano atual", fn:r=>r.plano||r.planoPlanilha||"—", sort:r=>r.plano||r.planoPlanilha||""},
    {label:"Mensalidade", cls:"num", fn:r=>r._mens?fmtBRL(r._mens):"—", sort:r=>r._mens||0},
    {label:"Filiais na Vesti", cls:"num", fn:r=>fmtInt(r.qtdFiliais||0), sort:r=>r.qtdFiliais||0},
    {label:"Casou por", fn:r=>r.casouPorTamanho==="cnpj"?"CNPJ":"nome (conferir)",
     sort:r=>r.casouPorTamanho||""},
  ], lista.sort((a,b)=>b._def - a._def || (b.seguidores||0) - (a.seguidores||0)));
}

// ---------- Tab switching ----------
const TAB_LABELS = {
  gmv:"GMV", vp:"VestiPago (Cartão + PIX)", cadastro:"Cadastro de Produtos",
  primeiras5:"Primeiras 5 Vendas", primeiras25:"Primeiras 25 Vendas",
  reativ:"Ativações e reativações", topstarter:"TOP 10 Starter", topparceiros:"TOP 10 Parceiros",
  vpinativo:"Sem VP Ativo", freteinativo:"Sem Frete Ativo", travadas:"Marcas Travadas",
  upgrade:"Oportunidades de Upgrade", tamanho:"Tamanho da marca",
  inadimplentes:"Inadimplentes", links:"Links & Cliques"
};

function switchTab(tab) {
  state.tab = tab;
  document.querySelectorAll(".tab-panel").forEach(p=>p.classList.remove("active"));
  const panel = $("tab-"+tab);
  if (panel) panel.classList.add("active");
  $("backNav").classList.toggle("hidden", tab === "home");
  $("breadcrumb").textContent = TAB_LABELS[tab] || "";
  window.scrollTo({top:0, behavior:"smooth"});
  renderActiveTab();
}
function goHome() { switchTab("home"); }
window.switchTab = switchTab;
window.goHome = goHome;

function renderActiveTab() {
  // atualiza periodo-label em todas as tabs
  document.querySelectorAll(".periodo-label").forEach(el => el.textContent = chaveLabel());
  if (state.tab === "home") { renderKpis(); return; }
  if (state.tab === "gmv") renderTabGmv();
  else if (state.tab === "vp") renderTabVp();
  else if (state.tab === "cadastro") renderTabCadastro();
  else if (state.tab === "primeiras5") renderTabPrimeiras(5);
  else if (state.tab === "primeiras25") renderTabPrimeiras(25);
  else if (state.tab === "topstarter") renderTabTop("starter");
  else if (state.tab === "topparceiros") renderTabTop("parceiros");
  else if (state.tab === "vpinativo") renderTabSemVp();
  else if (state.tab === "freteinativo") renderTabSemFrete();
  else if (state.tab === "travadas") renderTabTravadas();
  else if (state.tab === "reativ") renderTabReativ();
  else if (state.tab === "upgrade") renderTabUpgrade();
  else if (state.tab === "tamanho") renderTabTamanho();
  else if (state.tab === "inadimplentes") renderTabInadimplentes();
  else if (state.tab === "links") renderTabLinks();
}

// ---------- Filtros ----------
function periodoLista() {
  return state.periodo === "mensal" ? D.mesesList : state.periodo === "anual" ? D.anosList : D.semanasList;
}
// popula/reseta a selecao ao trocar de tipo de periodo: default = ultimo periodo (1 item = comportamento antigo)
function populaPeriodoValor() {
  const lista = periodoLista();
  state.chaves = state.chaves.filter(k => lista.includes(k));
  if (!state.chaves.length) state.chaves = lista.length ? [lista[lista.length-1]] : [];
  renderPeriodoDropdown();
}
// dropdown de checkboxes p/ selecionar 1+ periodos
function renderPeriodoDropdown() {
  const box = $("filter-periodo-valor");
  if (!box) return;
  const lista = periodoLista().slice().reverse();
  const sel = new Set(state.chaves);
  box.innerHTML =
    `<button type="button" class="ms-toggle"><span class="ms-lbl">${chaveLabel()}</span><span class="ms-caret">▾</span></button>` +
    `<div class="ms-panel">` +
      `<div class="ms-actions"><button type="button" data-act="all">Todos</button>` +
      `<button type="button" data-act="clear">Limpar</button></div>` +
      `<div class="ms-options">` +
        lista.map(v=>`<label class="ms-opt"><input type="checkbox" value="${v}"${sel.has(v)?" checked":""}> ${v}</label>`).join("") +
      `</div>` +
    `</div>`;
  box.querySelector(".ms-toggle").addEventListener("click", ev => {
    ev.stopPropagation(); box.classList.toggle("open");
  });
  box.querySelectorAll(".ms-opt input").forEach(cb => cb.addEventListener("change", () => {
    if (cb.checked) { if (!state.chaves.includes(cb.value)) state.chaves.push(cb.value); }
    else state.chaves = state.chaves.filter(k => k !== cb.value);
    box.querySelector(".ms-lbl").textContent = chaveLabel();
    renderActiveTab();
  }));
  box.querySelector('[data-act="all"]').addEventListener("click", () => {
    state.chaves = periodoLista().slice(); renderPeriodoDropdown(); box.classList.add("open"); renderActiveTab();
  });
  box.querySelector('[data-act="clear"]').addEventListener("click", () => {
    state.chaves = []; renderPeriodoDropdown(); box.classList.add("open"); renderActiveTab();
  });
}
// CS disponiveis vem dos proprios dados (nao mais hardcoded Elisa/Jennyfer)
function populaCs() {
  const sel = $("filter-cs");
  const cont = {};
  for (const e of D.empresas) {
    if (!state.canais.has(canalDe(e))) continue;
    const cs = (e.cs || "").trim() || "(sem CS)";
    cont[cs] = (cont[cs] || 0) + 1;
  }
  const nomes = Object.keys(cont).sort((a,b)=>a.localeCompare(b,"pt-BR"));
  const cur = state.cs;
  sel.innerHTML = `<option value="todas">Todas</option>` +
    nomes.map(n=>`<option value="${n === "(sem CS)" ? "" : n}">${n} (${cont[n]})</option>`).join("");
  const valores = new Set(["todas", ...nomes.map(n=>n === "(sem CS)" ? "" : n)]);
  sel.value = valores.has(cur) ? cur : "todas";
  state.cs = sel.value;
}

// ---------- Combobox de empresa (com busca visivel) ----------
const _cbx = { opcoes: [], hi: -1 };
const _norm = (s) => (s||"").normalize("NFD").replace(/[̀-ͯ]/g,"").toLowerCase();
const _esc = (s) => String(s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]));

function populaEmpresas() {
  const lista = empresasFiltradas();
  _cbx.opcoes = lista.slice().sort((a,b)=>a.name.localeCompare(b.name,"pt-BR")).map(e=>e.name);
  const inp = $("filter-empresa-input");
  inp.placeholder = `Todas (${lista.length}) — digite para buscar`;
  // se a empresa selecionada saiu da lista pelos outros filtros, volta pra "todas"
  if (state.empresa !== "todas" && !_cbx.opcoes.includes(state.empresa)) selecionaEmpresa("todas");
  else if (state.empresa !== "todas") inp.value = state.empresa;
  renderCbxPanel("");
}

function renderCbxPanel(termo) {
  const box = $("filter-empresa"), panel = box.querySelector(".cbx-panel");
  const t = _norm(termo);
  const achados = t ? _cbx.opcoes.filter(n => _norm(n).includes(t)) : _cbx.opcoes;
  _cbx.hi = -1;
  const head = `<div class="cbx-opt${state.empresa==="todas"?" cur":""}" data-v="todas">Todas as empresas</div>`;
  if (!achados.length) { panel.innerHTML = head + `<div class="cbx-empty">Nenhuma empresa com “${_esc(termo)}”</div>`; return; }
  panel.innerHTML = head + achados.slice(0, 300).map(n => {
    let label = _esc(n);
    if (t) { // destaca o trecho digitado
      const i = _norm(n).indexOf(t);
      if (i >= 0) label = _esc(n.slice(0,i)) + "<mark>" + _esc(n.slice(i,i+termo.length)) + "</mark>" + _esc(n.slice(i+termo.length));
    }
    return `<div class="cbx-opt${n===state.empresa?" cur":""}" data-v="${_esc(n)}">${label}</div>`;
  }).join("") + (achados.length > 300 ? `<div class="cbx-empty">+${achados.length-300} … refine a busca</div>` : "");
  panel.querySelectorAll(".cbx-opt").forEach(o => o.addEventListener("mousedown", ev => {
    ev.preventDefault(); selecionaEmpresa(o.dataset.v); renderActiveTab();
  }));
}

function selecionaEmpresa(v) {
  state.empresa = v;
  const box = $("filter-empresa"), inp = $("filter-empresa-input");
  inp.value = v === "todas" ? "" : v;
  box.classList.toggle("sel", v !== "todas");
  box.classList.remove("open");
}

function bindEmpresaCbx() {
  const box = $("filter-empresa"), inp = $("filter-empresa-input");
  inp.addEventListener("focus", () => { inp.select(); renderCbxPanel(""); box.classList.add("open"); });
  inp.addEventListener("input", () => { renderCbxPanel(inp.value); box.classList.add("open"); });
  inp.addEventListener("keydown", ev => {
    const opts = [...box.querySelectorAll(".cbx-opt")];
    if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
      ev.preventDefault();
      if (!opts.length) return;
      _cbx.hi = ev.key === "ArrowDown"
        ? Math.min(_cbx.hi + 1, opts.length - 1)
        : Math.max(_cbx.hi - 1, 0);
      opts.forEach(o=>o.classList.remove("hi"));
      opts[_cbx.hi].classList.add("hi");
      opts[_cbx.hi].scrollIntoView({block:"nearest"});
    } else if (ev.key === "Enter") {
      ev.preventDefault();
      const alvo = _cbx.hi >= 0 ? opts[_cbx.hi] : opts.find(o => o.dataset.v !== "todas") || opts[0];
      if (alvo) { selecionaEmpresa(alvo.dataset.v); renderActiveTab(); }
    } else if (ev.key === "Escape") {
      selecionaEmpresa(state.empresa); inp.blur();
    }
  });
  // ao sair sem escolher, volta a mostrar a selecao atual (nao deixa texto solto)
  inp.addEventListener("blur", () => { setTimeout(()=>{ box.classList.remove("open"); selecionaEmpresa(state.empresa); }, 120); });
  $("filter-empresa-clear").addEventListener("click", () => {
    selecionaEmpresa("todas"); renderCbxPanel(""); renderActiveTab(); inp.focus();
  });
}

function bind() {
  document.querySelectorAll(".pt-btn").forEach(btn=>btn.addEventListener("click",()=>{
    document.querySelectorAll(".pt-btn").forEach(b=>b.classList.remove("active"));
    btn.classList.add("active"); state.periodo = btn.dataset.period;
    populaPeriodoValor(); renderActiveTab();
  }));
  // fecha o dropdown de periodo ao clicar fora
  document.addEventListener("click", e=>{
    const box = $("filter-periodo-valor");
    if (box && !box.contains(e.target)) box.classList.remove("open");
  });
  $("filter-cs").addEventListener("change", e=>{ state.cs=e.target.value; populaEmpresas(); renderActiveTab(); });
  $("filter-canal").addEventListener("change", e=>{
    const cb = e.target;
    if (cb.tagName !== "INPUT") return;
    if (cb.checked) state.canais.add(cb.value); else state.canais.delete(cb.value);
    state.reativDrill = null;
    populaCs(); populaEmpresas(); renderActiveTab();
  });
  bindEmpresaCbx();
  $("filter-cad-mes").addEventListener("change", e=>{ state.cadMes=e.target.value; renderActiveTab(); });
  $("filter-inad-dias").value = String(state.inadDias);
  $("filter-inad-dias").addEventListener("change", e=>{ state.inadDias=Number(e.target.value)||0; renderActiveTab(); });
  $("filter-upg-min").addEventListener("change", e=>{ state.upgMin=Number(e.target.value)||300000; renderActiveTab(); });
  // mudar filtro invalida a seleção feita no gráfico (o pedaço clicado pode nem existir mais)
  $("filter-tam-porte").addEventListener("change", e=>{
    state.portesSel = e.target.value ? new Set([e.target.value]) : new Set();
    renderActiveTab();
  });
  $("filter-reativ-origem").addEventListener("change", e=>{
    state.reativOrigem=e.target.value||"todas"; state.reativDrill=null; renderActiveTab();
  });
  document.querySelectorAll(".chk-coorte").forEach(cb=>cb.addEventListener("change", e=>{
    state.soCoorte = e.target.checked;
    document.querySelectorAll(".chk-coorte").forEach(o=>{ o.checked = state.soCoorte; });
    renderActiveTab();
  }));
}

(function init(){
  $("gerado-em").textContent = "Gerado em " + (D.geradoEm||"—").slice(0,16).replace("T"," ");
  normalizaCanal();
  marcaCoorte();
  buildAnual();
  populaPeriodoValor(); populaCs(); populaEmpresas(); populaCadMesSelect(); bind(); renderActiveTab();
})();
