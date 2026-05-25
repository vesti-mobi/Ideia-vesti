// PainelElisa — frontend com tabs + graficos
const fmtBRL = (n) => Number(n||0).toLocaleString("pt-BR",{style:"currency",currency:"BRL",maximumFractionDigits:0});
const fmtInt = (n) => Number(n||0).toLocaleString("pt-BR");
const $ = (id) => document.getElementById(id);

const state = { periodo:"mensal", chave:"", cs:"todas", canal:"todos", empresa:"todas", tab:"home", cadMes:"todos" };
const D = (typeof DADOS !== "undefined") ? DADOS : { empresas:[], mesesList:[], semanasList:[] };
const PARC_EXCL = new Set(["atta","attasoft","onix"]);
const COLORS = ["#6C5CE7","#00B894","#F39C12","#E17055","#0984E3","#FD79A8","#00CEC9","#A29BFE","#D63031","#74B9FF","#FFB94A","#55EFC4"];
const charts = {}; // canvas id -> Chart instance

// ---------- helpers ----------
function empresasFiltradas() {
  return D.empresas.filter(e => {
    if (state.cs !== "todas" && e.cs !== state.cs) return false;
    if (state.canal === "Starter"   && !e.starter_interno) return false;
    if (state.canal === "Parceiros" && e.starter_interno) return false;
    if (state.empresa !== "todas" && e.name !== state.empresa) return false;
    return true;
  });
}
function bucket(e) {
  const fonte = state.periodo === "mensal" ? e.mensal : e.semanal;
  return fonte[state.chave] || {valPix:0,valCartao:0,valTotal:0,qtPix:0,qtCartao:0,qtTotal:0};
}
const mesAtualChave = () => state.periodo === "mensal" ? state.chave : (state.chave||"").slice(0,7);

function destroyChart(id) { if (charts[id]) { charts[id].destroy(); delete charts[id]; } }
function makeChart(id, cfg) {
  destroyChart(id);
  const ctx = document.getElementById(id);
  if (!ctx) return;
  charts[id] = new Chart(ctx, cfg);
}

function renderTable(tableId, columns, rows) {
  const tbl = document.getElementById(tableId);
  if (!tbl) return;
  if (!rows.length) { tbl.innerHTML = `<tr><td class="empty" colspan="${columns.length}">Sem dados pros filtros atuais.</td></tr>`; return; }
  const thead = `<thead><tr>${columns.map(c=>`<th class="${c.cls||''}">${c.label}</th>`).join("")}</tr></thead>`;
  const tbody = `<tbody>${rows.map(r=>{
    const cls = r._alert ? "row-alert" : "";
    return `<tr class="${cls}">${columns.map(c=>`<td class="${c.cls||''}">${c.fn(r)}</td>`).join("")}</tr>`;
  }).join("")}</tbody>`;
  tbl.innerHTML = thead + tbody;
}

// ---------- HOME KPIs ----------
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
    if (e.mes5Vendas  && e.mes5Vendas  === mesK) p5++;
    if (e.mes25Vendas && e.mes25Vendas === mesK) p25++;
    if (((e.reativacoesPorMes||{})[mesK]||0) > 0) reativ++;
  }
  $("kpi-gmv").textContent = fmtBRL(gmv);
  $("kpi-gmv-sub").textContent = `${lista.length} marcas · ${state.chave||"—"}`;
  $("kpi-cartao").textContent = fmtBRL(cartao);
  $("kpi-cartao-sub").textContent = `${cartaoMarcas} marcas ativas`;
  $("kpi-pix").textContent = fmtBRL(pix);
  $("kpi-pix-sub").textContent = `${pixMarcas} marcas ativas`;
  $("kpi-cadastro").textContent = fmtInt(cadProds);
  $("kpi-cadastro-marcas").textContent = fmtInt(cadMarcas);
  $("kpi-p5").textContent = fmtInt(p5);
  $("kpi-p25").textContent = fmtInt(p25);
  $("kpi-reativ").textContent = fmtInt(reativ);
  $("kpi-reativ-sub").textContent = `marcas reativaram em ${mesK||"—"}`;
  $("kpi-semvp").textContent = fmtInt(semVP);
  $("kpi-semfrete").textContent = fmtInt(semFrete);
  $("kpi-topstarter").textContent = fmtInt(lista.filter(e=>e.starter_interno).length);
  $("kpi-topparc").textContent = fmtInt(lista.filter(e=>!e.starter_interno && !PARC_EXCL.has((e.partner_raw||"").toLowerCase())).length);
  const hoje = new Date();
  const travadas = lista.filter(e => {
    const ped = e.primeiroPedidoCadastrado ? new Date(e.primeiroPedidoCadastrado) : null;
    const venda = e.primeiraVenda ? new Date(e.primeiraVenda) : null;
    const dias = ped ? Math.floor((hoje-ped)/86400000) : null;
    return (ped && !venda && dias > 14) || (ped && (e.qtProdutos||0) === 0);
  });
  $("kpi-travadas").textContent = fmtInt(travadas.length);
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
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal+(r.starter_interno?" (Int)":"")},
    {label:"GMV", cls:"num", fn:r=>fmtBRL(r._g.valTotal)},
    {label:"Pedidos", cls:"num", fn:r=>fmtInt(r._g.qtTotal)},
    {label:"Mensalidade", cls:"num", fn:r=>fmtBRL(r.valor_mensal)},
  ], rows);
}

function renderTabVp(tipo) { // tipo: "Cartao" | "Pix"
  const lista = empresasFiltradas();
  const campoVal = "val"+tipo, campoQt = "qt"+tipo;
  const { meses, vals } = evolMensal(lista, campoVal);
  const colorIdx = tipo==="Cartao"?0:1;
  makeChart(`chart-${tipo.toLowerCase()=='cartao'?'cartao':'pix'}-evolucao`, {
    type:"line",
    data:{labels:meses, datasets:[{label:tipo, data:vals, borderColor:COLORS[colorIdx], backgroundColor:"rgba(108,92,231,.15)", fill:true, tension:.3}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{y:{ticks:{callback:v=>"R$"+(v/1000).toFixed(0)+"k"}}}}
  });
  const ranked = lista.map(e=>({...e,_v:bucket(e)[campoVal], _q:bucket(e)[campoQt]})).filter(e=>e._v>0).sort((a,b)=>b._v-a._v);
  const top10 = ranked.slice(0,10);
  makeChart(`chart-${tipo.toLowerCase()=='cartao'?'cartao':'pix'}-top`, {
    type:"bar",
    data:{labels:top10.map(e=>e.name), datasets:[{label:tipo, data:top10.map(e=>e._v), backgroundColor:COLORS[colorIdx]}]},
    options:{indexAxis:"y", responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{x:{ticks:{callback:v=>"R$"+(v/1000).toFixed(0)+"k"}}}}
  });
  const tblId = tipo==="Cartao" ? "tbl-vpcartao" : "tbl-vppix";
  renderTable(tblId, [
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal},
    {label:tipo, cls:"num", fn:r=>fmtBRL(r._v)},
    {label:"Pedidos", cls:"num", fn:r=>fmtInt(r._q)},
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

function renderTabPrimeiras(n) {
  const lista = empresasFiltradas();
  const campo = n===5 ? "mes5Vendas" : "mes25Vendas";
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
    // tempo 1a venda até 25
    const tempos = [];
    for (const e of lista) {
      if (!e.mes25Vendas || !e.primeiraVenda) continue;
      const dv = new Date(e.primeiraVenda);
      const dm = new Date(e.mes25Vendas+"-15");
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
  const rows = lista.filter(e=>e[campo] === mesK);
  renderTable(n===5?"tbl-p5":"tbl-p25", [
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal},
    {label:"1ª venda", fn:r=>r.primeiraVenda||"—"},
    {label:`Mês ${n} vendas`, fn:r=>r[campo]||"—"},
  ], rows);
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
    {label:"CS", fn:r=>r.cs},
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

function renderTabReativ() {
  const lista = empresasFiltradas();
  const mesK = mesAtualChave();
  // evolução mensal: soma de reativações por mês acrosss lista
  const porMes = {};
  for (const e of lista) for (const [m,q] of Object.entries(e.reativacoesPorMes||{})) {
    porMes[m] = (porMes[m]||0) + q;
  }
  const meses = Object.keys(porMes).sort();
  makeChart("chart-reativ-evolucao", {
    type:"bar",
    data:{labels:meses, datasets:[{label:"reativações", data:meses.map(m=>porMes[m]), backgroundColor:COLORS[7]}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  // TOP 10 marcas acumulado
  const ranked = lista.filter(e=>e.totalReativ>0).sort((a,b)=>b.totalReativ-a.totalReativ);
  const top10 = ranked.slice(0,10);
  makeChart("chart-reativ-top", {
    type:"bar",
    data:{labels:top10.map(e=>e.name), datasets:[{label:"reativações", data:top10.map(e=>e.totalReativ), backgroundColor:COLORS[7]}]},
    options:{indexAxis:"y", responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  const rows = lista.filter(e=>((e.reativacoesPorMes||{})[mesK]||0) > 0)
    .map(e=>({...e, _q: (e.reativacoesPorMes||{})[mesK]||0}))
    .sort((a,b)=>b._q - a._q);
  renderTable("tbl-reativ", [
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal},
    {label:`Reativações em ${mesK||"—"}`, cls:"num", fn:r=>fmtInt(r._q)},
    {label:"Total acumulado", cls:"num", fn:r=>fmtInt(r.totalReativ||0)},
  ], rows);
}

function renderTabTravadas() {
  const hoje = new Date();
  const lista = empresasFiltradas().map(e => {
    const ped = e.primeiroPedidoCadastrado ? new Date(e.primeiroPedidoCadastrado) : null;
    const venda = e.primeiraVenda ? new Date(e.primeiraVenda) : null;
    const dias = ped ? Math.floor((hoje-ped)/86400000) : null;
    return {...e, _ped:ped, _venda:venda, _dias:dias};
  }).filter(e => (e._ped && !e._venda && e._dias > 14) || (e._ped && (e.qtProdutos||0) === 0))
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
  const porCs = countBy(lista, e=>e.cs);
  makeChart("chart-trav-cs", {
    type:"bar",
    data:{labels:Object.keys(porCs), datasets:[{label:"marcas", data:Object.values(porCs), backgroundColor:COLORS[8]}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}
  });
  renderTable("tbl-travadas", [
    {label:"Marca", fn:r=>r.name},
    {label:"CS", fn:r=>r.cs},
    {label:"Canal", fn:r=>r.canal},
    {label:"1º pedido", fn:r=>(r.primeiroPedidoCadastrado||"—").slice(0,10)},
    {label:"1ª venda", fn:r=>(r.primeiraVenda||"—").slice(0,10)},
    {label:"Produtos", cls:"num", fn:r=>fmtInt(r.qtProdutos||0)},
    {label:"Dias", cls:"num", fn:r=>r._dias??"—"},
  ], lista.map(e=>({...e,_alert:true})));
}

// ---------- Tab switching ----------
const TAB_LABELS = {
  gmv:"GMV", vpcartao:"VP Cartão", vppix:"VP PIX", cadastro:"Cadastro de Produtos",
  primeiras5:"Primeiras 5 Vendas", primeiras25:"Primeiras 25 Vendas",
  reativ:"Reativações", topstarter:"TOP 10 Starter", topparceiros:"TOP 10 Parceiros",
  vpinativo:"Sem VP Ativo", freteinativo:"Sem Frete Ativo", travadas:"Marcas Travadas"
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
  document.querySelectorAll(".periodo-label").forEach(el => el.textContent = state.chave || "—");
  if (state.tab === "home") { renderKpis(); return; }
  if (state.tab === "gmv") renderTabGmv();
  else if (state.tab === "vpcartao") renderTabVp("Cartao");
  else if (state.tab === "vppix") renderTabVp("Pix");
  else if (state.tab === "cadastro") renderTabCadastro();
  else if (state.tab === "primeiras5") renderTabPrimeiras(5);
  else if (state.tab === "primeiras25") renderTabPrimeiras(25);
  else if (state.tab === "topstarter") renderTabTop("starter");
  else if (state.tab === "topparceiros") renderTabTop("parceiros");
  else if (state.tab === "vpinativo") renderTabSemVp();
  else if (state.tab === "freteinativo") renderTabSemFrete();
  else if (state.tab === "travadas") renderTabTravadas();
  else if (state.tab === "reativ") renderTabReativ();
}

// ---------- Filtros ----------
function populaPeriodoValor() {
  const sel = $("filter-periodo-valor");
  const lista = state.periodo === "mensal" ? D.mesesList : D.semanasList;
  sel.innerHTML = lista.slice().reverse().map(v=>`<option value="${v}">${v}</option>`).join("");
  if (!lista.includes(state.chave)) state.chave = lista[lista.length-1] || "";
  sel.value = state.chave;
}
function populaEmpresas() {
  const sel = $("filter-empresa");
  const lista = empresasFiltradas();
  sel.innerHTML = `<option value="todas">Todas (${lista.length})</option>` +
    lista.slice().sort((a,b)=>a.name.localeCompare(b.name)).map(e=>`<option value="${e.name}">${e.name}</option>`).join("");
}

function bind() {
  document.querySelectorAll(".pt-btn").forEach(btn=>btn.addEventListener("click",()=>{
    document.querySelectorAll(".pt-btn").forEach(b=>b.classList.remove("active"));
    btn.classList.add("active"); state.periodo = btn.dataset.period;
    populaPeriodoValor(); renderActiveTab();
  }));
  $("filter-periodo-valor").addEventListener("change", e=>{ state.chave=e.target.value; renderActiveTab(); });
  $("filter-cs").addEventListener("change", e=>{ state.cs=e.target.value; populaEmpresas(); renderActiveTab(); });
  $("filter-canal").addEventListener("change", e=>{ state.canal=e.target.value; populaEmpresas(); renderActiveTab(); });
  $("filter-empresa").addEventListener("change", e=>{ state.empresa=e.target.value; renderActiveTab(); });
  $("filter-cad-mes").addEventListener("change", e=>{ state.cadMes=e.target.value; renderActiveTab(); });
}

(function init(){
  $("gerado-em").textContent = "Gerado em " + (D.geradoEm||"—").slice(0,16).replace("T"," ");
  populaPeriodoValor(); populaEmpresas(); populaCadMesSelect(); bind(); renderActiveTab();
})();
