/* Relatório CS 2 — front. Renderiza 3 abas estilo planilha com edição de
   observação + cor por linha (compartilhado) e alertas recalculados ao vivo. */
(function () {
"use strict";

var API = (window.CS2_API || "").replace(/\/$/, "");   // backend compartilhado; vazio => só localStorage
var NS  = "cs2";
var D   = window.CS2_DATA || { aba1: [], aba2: [], aba3: [] };

/* ---------- datas / formato ---------- */
function today(){ var n=new Date(); return new Date(n.getFullYear(),n.getMonth(),n.getDate()); }
function pdate(s){ if(!s) return null; var p=String(s).slice(0,10).split("-"); if(p.length!==3) return null;
  var d=new Date(+p[0],+p[1]-1,+p[2]); return isNaN(d)?null:d; }
function daysBetween(a,b){ return Math.floor((a-b)/86400000); }
function fmtDate(s){ var d=pdate(s); if(!d) return ""; var D2=String(d.getDate()).padStart(2,"0"),
  M=String(d.getMonth()+1).padStart(2,"0"); return D2+"/"+M+"/"+d.getFullYear(); }
function money(v){ if(v===null||v===undefined||v==="") return "";
  return "R$ "+Number(v).toLocaleString("pt-BR",{minimumFractionDigits:0,maximumFractionDigits:0}); }
function pct(v){ if(v===null||v===undefined||v==="") return ""; return Number(v).toLocaleString("pt-BR",{maximumFractionDigits:1})+"%"; }
function esc(s){ return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;"); }

/* ---------- cores ---------- */
var COLORS=[
  {k:"",         c:""},                 // sem cor
  {k:"verde",    c:"#B6D7A8"},
  {k:"vermelho", c:"#F4C7C3"},
  {k:"amarelo",  c:"#FFE599"},
  {k:"azul",     c:"#CFE2F3"},
  {k:"roxo",     c:"#D9D2E9"},
  {k:"laranja",  c:"#FCE5CD"}
];
var GREENS=["#B6D7A8","#D9EAD3","#93C47D"];
var REDS  =["#F4C7C3","#E06666","#EA9999","#EA6666"];
function isGreen(h){ return GREENS.indexOf((h||"").toUpperCase())>=0; }
function isRed(h){   return REDS.indexOf((h||"").toUpperCase())>=0; }

/* ---------- store (compartilhado + cache local) ---------- */
var LS="cs2_overlays_v1", overlays={};
function loadOverlays(){
  return new Promise(function(res){
    function local(){ try{overlays=JSON.parse(localStorage.getItem(LS)||"{}");}catch(e){overlays={};} res(); }
    if(!API) return local();
    fetch(API+"/api/overlays?ns="+NS,{cache:"no-store"})
      .then(function(r){return r.json();})
      .then(function(j){ overlays=(j&&j.overlays)||{}; try{localStorage.setItem(LS,JSON.stringify(overlays));}catch(e){} res(); })
      .catch(local);
  });
}
var flashT;
function flash(msg){ var f=document.getElementById("flash"); f.textContent=msg||"salvo ✓"; f.classList.add("show");
  clearTimeout(flashT); flashT=setTimeout(function(){f.classList.remove("show");},1200); }
function ov(key){ return overlays[key]||{}; }
function saveOverlay(key,patch){
  overlays[key]=Object.assign({},overlays[key],patch,{ts:Date.now()});
  try{ localStorage.setItem(LS,JSON.stringify(overlays)); }catch(e){}
  if(API){
    fetch(API+"/api/overlays",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({ns:NS,key:key,obs:overlays[key].obs,color:overlays[key].color})})
      .then(function(){flash();}).catch(function(){flash("salvo local (offline)");});
  } else flash("salvo local");
}

/* ---------- célula editável: observação ---------- */
function obsCell(key,orig){
  var o=ov(key); var val=(o.obs!==undefined&&o.obs!==null)?o.obs:(orig||"");
  var td=document.createElement("td"); td.className="obs";
  var ta=document.createElement("textarea"); ta.className="obsedit"; ta.value=val; ta.rows=1;
  ta.placeholder="observação...";
  function autos(){ ta.style.height="auto"; ta.style.height=(ta.scrollHeight)+"px"; }
  ta.addEventListener("input",autos);
  ta.addEventListener("focus",autos);
  ta.addEventListener("blur",function(){
    if(ta.value!==val){ val=ta.value; saveOverlay(key,{obs:ta.value}); ta.classList.remove("dirty"); }
  });
  ta.addEventListener("input",function(){ ta.classList.toggle("dirty",ta.value!==val); });
  td.appendChild(ta); setTimeout(autos,0); return td;
}

/* ---------- célula editável: cor ---------- */
function colorCell(key,defColor,onChange){
  var o=ov(key);
  var cur=(o.color!==undefined&&o.color!==null)?o.color:(defColor||"");
  var td=document.createElement("td"); td.className="colorcell";
  var sw=document.createElement("span"); sw.className="swatch";
  sw.style.background=cur||"repeating-linear-gradient(45deg,#fff,#fff 4px,#eee 4px,#eee 8px)";
  var pop=document.createElement("div"); pop.className="pop";
  COLORS.forEach(function(col){
    var b=document.createElement("div"); b.className="opt"+(col.c?"":" none");
    if(col.c) b.style.background=col.c; b.title=col.k||"sem cor";
    b.addEventListener("click",function(e){
      e.stopPropagation(); cur=col.c;
      sw.style.background=cur||"repeating-linear-gradient(45deg,#fff,#fff 4px,#eee 4px,#eee 8px)";
      pop.classList.remove("open");
      saveOverlay(key,{color:col.c});
      if(onChange) onChange(col.c);
    });
    pop.appendChild(b);
  });
  sw.addEventListener("click",function(e){ e.stopPropagation();
    document.querySelectorAll(".pop.open").forEach(function(p){if(p!==pop)p.classList.remove("open");});
    pop.classList.toggle("open"); });
  td.appendChild(sw); td.appendChild(pop); return td;
}
document.addEventListener("click",function(){ document.querySelectorAll(".pop.open").forEach(function(p){p.classList.remove("open");}); });

/* ---------- header com sort ---------- */
function makeHead(cols,state,rerender){
  var tr=document.createElement("tr");
  cols.forEach(function(c){
    var th=document.createElement("th"); th.textContent=c.label;
    if(c.sort){ th.style.cursor="pointer";
      if(state.sort===c.key){ var ar=document.createElement("span"); ar.className="ar";
        ar.textContent=state.dir>0?" ▲":" ▼"; th.appendChild(ar); }
      th.addEventListener("click",function(){ if(state.sort===c.key)state.dir*=-1; else {state.sort=c.key;state.dir=c.def||-1;} rerender(); });
    } else th.style.cursor="default";
    if(c.num) th.style.textAlign="right";
    tr.appendChild(th);
  });
  return tr;
}
function sortRows(rows,state,accessors){
  if(!state.sort) return rows;
  var f=accessors[state.sort]; if(!f) return rows;
  return rows.slice().sort(function(a,b){
    var x=f(a),y=f(b);
    if(x===null||x===undefined||x==="") return 1; if(y===null||y===undefined||y==="") return -1;
    if(typeof x==="number"&&typeof y==="number") return (x-y)*state.dir;
    return String(x).localeCompare(String(y),"pt-BR")*state.dir;
  });
}

/* ================= ABA 1 — Passagem de bastão ================= */
var s1={sort:"dias",dir:-1};
function eff1(row){ // status efetivo considerando recolorização manual
  var o=ov("a1:"+row.marca);
  if(o.color!==undefined&&o.color!==null){
    if(o.color==="") return "sem_reuniao";
    if(isGreen(o.color)) return "ativa";
    if(isRed(o.color))   return "cancelada";
    return row.status;
  }
  return row.status;
}
function alert1(row){ // {level,text} ou null
  // alerta de reunião só p/ marcas EM BRANCO (sem reunião). Verde=reunião feita, vermelho=cancelada -> nunca alertam.
  if(eff1(row)!=="sem_reuniao") return null;
  var cad=pdate(row.entrada);   // data de cadastro (Entrada)
  if(!cad) return null;
  var d=daysBetween(today(),cad);
  if(d>=60) return {level:"alert",text:"⏰ 60 dias — chamar"};
  if(d>=45) return {level:"warn", text:"⏰ 45 dias — chamar"};
  return null;
}
function defColor1(row){ var st=row.status;
  return st==="ativa"?"#B6D7A8":st==="cancelada"?"#F4C7C3":""; }

function renderA1(){
  var q=(document.getElementById("q-a1").value||"").toLowerCase();
  var cs=document.getElementById("cs-a1").value;
  var st=document.getElementById("st-a1").value;
  var onlyAl=document.getElementById("al-a1").checked;
  var rows=D.aba1.filter(function(r){
    if(q && (r.marca||"").toLowerCase().indexOf(q)<0) return false;
    if(cs && r.cs!==cs) return false;
    if(st && eff1(r)!==st) return false;
    if(onlyAl && !alert1(r)) return false;
    return true;
  });
  var acc={ marca:function(r){return r.marca;}, entrada:function(r){return pdate(r.entrada)?pdate(r.entrada).getTime():null;},
    dias:function(r){var d=pdate(r.entrada);return d?daysBetween(today(),d):null;}, cs:function(r){return r.cs;},
    impl:function(r){return r.implementador;}, data25:function(r){var d=pdate(r.data25);return d?d.getTime():null;} };
  rows=sortRows(rows,s1,acc);

  var cols=[
    {label:"Cor"},
    {label:"Marca",key:"marca",sort:1,def:1},
    {label:"Entrada",key:"entrada",sort:1},
    {label:"Dias",key:"dias",sort:1,num:1},
    {label:"Implementador",key:"impl",sort:1},
    {label:"CS",key:"cs",sort:1},
    {label:"25+ / Últ. marco",key:"data25",sort:1},
    {label:"Status"},{label:"Alerta"},{label:"Observação"}
  ];
  var tbl=document.getElementById("tbl-a1"); tbl.innerHTML="";
  var thead=document.createElement("thead"); thead.appendChild(makeHead(cols,s1,renderA1)); tbl.appendChild(thead);
  var tb=document.createElement("tbody");
  rows.forEach(function(r){
    var tr=document.createElement("tr");
    var oc=ov("a1:"+r.marca); var rc=(oc.color!==undefined&&oc.color!==null)?oc.color:defColor1(r);
    if(rc){ tr.setAttribute("data-c","1"); tr.style.setProperty("--rowc",rc); }
    tr.appendChild(colorCell("a1:"+r.marca,defColor1(r),function(){ renderA1(); cardsA1(); pills(); }));
    var d=pdate(r.entrada), dias=d?daysBetween(today(),d):"";
    tr.appendChild(cell('<span class="marca">'+esc(r.marca)+"</span>"));
    tr.appendChild(cell(fmtDate(r.entrada),"nowrap mut"));
    tr.appendChild(cell(dias===""?'<span class="dash">—</span>':dias,"num"));
    tr.appendChild(cell(esc(r.implementador||"")||'<span class="dash">—</span>'));
    tr.appendChild(cell(esc(r.cs||"")||'<span class="dash">—</span>'));
    tr.appendChild(cell(r.data25?fmtDate(r.data25):'<span class="dash">—</span>',"nowrap mut"));
    var stx=eff1(r); tr.appendChild(cell('<span class="st '+stx+'">'+({ativa:"Ativa",cancelada:"Cancelada",sem_reuniao:"Sem reunião"}[stx])+"</span>"));
    var al=alert1(r); tr.appendChild(cell(al?'<span class="alertpill'+(al.level==="warn"?" warn":"")+'">'+al.text+"</span>":'<span class="dash">—</span>'));
    tr.appendChild(obsCell("a1:"+r.marca,r.obs_orig));
    tb.appendChild(tr);
  });
  tbl.appendChild(tb);
  if(!rows.length) tbl.innerHTML+='<tbody><tr><td colspan="10" class="empty">Nenhuma marca com esses filtros.</td></tr></tbody>';
  document.getElementById("cnt-a1").textContent=rows.length+" marca(s)";
}
function cell(html,cls){ var td=document.createElement("td"); if(cls)td.className=cls; td.innerHTML=html; return td; }

function cardsA1(){
  var a=D.aba1, ativa=0,canc=0,sem=0,al=0;
  a.forEach(function(r){ var s=eff1(r); if(s==="ativa")ativa++;else if(s==="cancelada")canc++;else sem++; if(alert1(r))al++; });
  document.getElementById("cards-a1").innerHTML=
    card(a.length,"Marcas")+card(ativa,"Ativas")+card(canc,"Canceladas")+card(sem,"Sem reunião")+card(al,"Com alerta ⏰",true);
}

/* ================= ABA 2 — Tino ================= */
var s2={sort:"dias",dir:-1};
function dias2(r){ return r.dias_sem_acesso; } // null = nunca
function alert2(r){
  if(r.dias_sem_acesso===null||r.dias_sem_acesso===undefined) return {level:"alert",text:"🚫 nunca acessou"};
  if(r.dias_sem_acesso>15) return {level:"warn",text:"⏰ "+r.dias_sem_acesso+"d sem acessar"};
  return null;
}
function renderA2(){
  var q=(document.getElementById("q-a2").value||"").toLowerCase();
  var st=document.getElementById("st-a2").value;
  var onlyAl=document.getElementById("al-a2").checked;
  var rows=D.aba2.filter(function(r){
    if(q && (r.nome||"").toLowerCase().indexOf(q)<0 && (r.company||"").toLowerCase().indexOf(q)<0) return false;
    if(st && r.status!==st) return false;
    if(onlyAl && !alert2(r)) return false;
    return true;
  });
  var acc={ nome:function(r){return r.nome;},
    dias:function(r){return r.dias_sem_acesso===null?99999:r.dias_sem_acesso;}, // nunca no topo
    last:function(r){var d=pdate(r.last_login);return d?d.getTime():-1;},
    login:function(r){return r.login_days;}, created:function(r){var d=pdate(r.created_at);return d?d.getTime():null;} };
  rows=sortRows(rows,s2,acc);
  var cols=[{label:"Cor"},{label:"Marca",key:"nome",sort:1,def:1},{label:"Status"},
    {label:"Criado em",key:"created",sort:1},{label:"Último acesso",key:"last",sort:1},
    {label:"Dias sem acessar",key:"dias",sort:1,num:1},{label:"Dias c/ login",key:"login",sort:1,num:1},
    {label:"Alerta"},{label:"Observação"}];
  var tbl=document.getElementById("tbl-a2"); tbl.innerHTML="";
  var thead=document.createElement("thead"); thead.appendChild(makeHead(cols,s2,renderA2)); tbl.appendChild(thead);
  var tb=document.createElement("tbody");
  rows.forEach(function(r){
    var key="a2:"+r.company; var tr=document.createElement("tr");
    var oc=ov(key); if(oc.color){ tr.setAttribute("data-c","1"); tr.style.setProperty("--rowc",oc.color); }
    tr.appendChild(colorCell(key,"",function(){}));
    tr.appendChild(cell('<span class="marca">'+esc(r.nome)+'</span>'));
    tr.appendChild(cell('<span class="st '+(r.status==="active"?"on":"off")+'">'+esc(r.status||"")+'</span>'));
    tr.appendChild(cell(fmtDate(r.created_at)||'<span class="dash">—</span>',"nowrap mut"));
    tr.appendChild(cell(r.last_login?fmtDate(r.last_login):'<span class="dash">nunca</span>',"nowrap"));
    tr.appendChild(cell(r.dias_sem_acesso===null?'<span class="dash">—</span>':r.dias_sem_acesso,"num"));
    tr.appendChild(cell(r.login_days==null?'<span class="dash">—</span>':r.login_days,"num mut"));
    var al=alert2(r); tr.appendChild(cell(al?'<span class="alertpill'+(al.level==="warn"?" warn":"")+'">'+al.text+"</span>":'<span class="dash">—</span>'));
    tr.appendChild(obsCell(key,""));
    tb.appendChild(tr);
  });
  tbl.appendChild(tb);
  if(!rows.length) tbl.innerHTML+='<tbody><tr><td colspan="9" class="empty">Nenhuma marca com esses filtros.</td></tr></tbody>';
  document.getElementById("cnt-a2").textContent=rows.length+" marca(s)";
}
function cardsA2(){
  var a=D.aba2, nunca=0,mais15=0;
  a.forEach(function(r){ if(r.dias_sem_acesso===null)nunca++; else if(r.dias_sem_acesso>15)mais15++; });
  document.getElementById("cards-a2").innerHTML=
    card(a.length,"Marcas c/ Tino")+card(mais15,">15d sem acessar",true)+card(nunca,"Nunca acessaram",true)+card(mais15+nunca,"Total alertas ⏰",true);
}

/* ================= ABA 3 — Ranking Upsell ================= */
var s3={sort:"cresc_rs",dir:-1};
function renderA3(){
  var q=(document.getElementById("q-a3").value||"").toLowerCase();
  var csBtn=document.querySelector("#cs-a3 button.active"); var cs=csBtn?csBtn.getAttribute("data-cs"):"";
  var rows=D.aba3.filter(function(r){
    if(cs && r.cs_tab!==cs) return false;
    if(q && (r.empresa||"").toLowerCase().indexOf(q)<0) return false;
    return true;
  });
  var acc={ empresa:function(r){return r.empresa;}, cs:function(r){return r.cs_tab;}, plano:function(r){return r.plano;},
    gmv_ant:function(r){return r.gmv_ant;}, gmv_atual:function(r){return r.gmv_atual;},
    cresc_rs:function(r){return r.cresc_rs;}, cresc_pct:function(r){return r.cresc_pct;} };
  rows=sortRows(rows,s3,acc);
  var cols=[{label:"Cor"},{label:"Empresa",key:"empresa",sort:1,def:1},{label:"CS",key:"cs",sort:1},
    {label:"Plano",key:"plano",sort:1},{label:"GMV anterior",key:"gmv_ant",sort:1,num:1},
    {label:"GMV atual",key:"gmv_atual",sort:1,num:1},{label:"Cresc. R$",key:"cresc_rs",sort:1,num:1},
    {label:"Cresc. %",key:"cresc_pct",sort:1,num:1},{label:"Mensalidade"},{label:"Tino"},{label:"VestiPago"},{label:"Observação"}];
  var tbl=document.getElementById("tbl-a3"); tbl.innerHTML="";
  var thead=document.createElement("thead"); thead.appendChild(makeHead(cols,s3,renderA3)); tbl.appendChild(thead);
  var tb=document.createElement("tbody");
  rows.forEach(function(r){
    var key="a3:"+r.cs_tab+":"+r.empresa; var tr=document.createElement("tr");
    var oc=ov(key); if(oc.color){ tr.setAttribute("data-c","1"); tr.style.setProperty("--rowc",oc.color); }
    tr.appendChild(colorCell(key,"",function(){}));
    tr.appendChild(cell('<span class="marca">'+esc(r.empresa)+'</span>'));
    tr.appendChild(cell(esc(r.cs_tab)));
    tr.appendChild(cell(esc(r.plano||"")||'<span class="dash">—</span>'));
    tr.appendChild(cell(money(r.gmv_ant),"num mut"));
    tr.appendChild(cell(money(r.gmv_atual),"num"));
    tr.appendChild(cell(money(r.cresc_rs),"num"));
    tr.appendChild(cell(r.cresc_pct==null?'<span class="dash">—</span>':pct(r.cresc_pct),"num"));
    tr.appendChild(cell(esc(r.mensalidade||"")||'<span class="dash">—</span>',"nowrap mut"));
    tr.appendChild(cell(boolPill(r.tino)));
    tr.appendChild(cell(boolPill(r.vestipago)));
    tr.appendChild(obsCell(key,r.obs_orig));
    tb.appendChild(tr);
  });
  tbl.appendChild(tb);
  if(!rows.length) tbl.innerHTML+='<tbody><tr><td colspan="12" class="empty">Nenhuma empresa com esses filtros.</td></tr></tbody>';
  document.getElementById("cnt-a3").textContent=rows.length+" empresa(s)";
}
function boolPill(v){ if(v===true) return '<span class="st on">Sim</span>';
  if(v===false) return '<span class="st off">Não</span>';
  if(v===null||v===undefined||v==="") return '<span class="dash">—</span>';
  return '<span class="mut">'+esc(v)+'</span>'; }
function cardsA3(){
  var a=D.aba3;
  var by={Busto:0,Luana:0,Thamiris:0}; a.forEach(function(r){ if(by[r.cs_tab]!=null)by[r.cs_tab]++; });
  document.getElementById("cards-a3").innerHTML=
    card(a.length,"Empresas")+card(by.Busto,"Busto")+card(by.Luana,"Luana")+card(by.Thamiris,"Thamiris");
}

/* ---------- cards / pills ---------- */
function card(v,l,alert){ return '<div class="card'+(alert?" alert":"")+'"><div class="v">'+v+'</div><div class="l">'+l+'</div></div>'; }
function pills(){
  var a1=D.aba1.reduce(function(n,r){return n+(alert1(r)?1:0);},0);
  var a2=D.aba2.reduce(function(n,r){return n+(alert2(r)?1:0);},0);
  setPill("pill-a1",a1); setPill("pill-a2",a2);
}
function setPill(id,n){ var e=document.getElementById(id); e.textContent=n; e.classList.toggle("zero",n===0); }

/* ---------- export ---------- */
function exportTab(which){
  var tbl=document.getElementById("tbl-"+which).cloneNode(true);
  // remove coluna Cor e transforma textarea/pop em texto
  tbl.querySelectorAll(".pop").forEach(function(p){p.remove();});
  tbl.querySelectorAll("textarea").forEach(function(t){ var s=document.createElement("span"); s.textContent=t.value; t.parentNode.replaceChild(s,t); });
  tbl.querySelectorAll("tr").forEach(function(tr){ if(tr.cells&&tr.cells.length) tr.deleteCell(0); });
  var html='<html><head><meta charset="utf-8"></head><body>'+tbl.outerHTML+'</body></html>';
  var blob=new Blob(["﻿"+html],{type:"application/vnd.ms-excel"});
  var url=URL.createObjectURL(blob), a=document.createElement("a");
  a.href=url; a.download="relatorio_cs_"+which+"_"+(D.hoje||"")+".xls"; a.click(); URL.revokeObjectURL(url);
}
window.exportTab=exportTab;

/* ---------- modal "marcas para chamar" ---------- */
function callItems(){
  var items=[];
  D.aba1.forEach(function(r){ var a=alert1(r); if(a) items.push({tab:"a1",marca:r.marca,cs:r.cs,
    motivo:a.text.replace(/^⏰ ?/,"").replace(/^🚫 ?/,""),level:a.level}); });
  D.aba2.forEach(function(r){ var a=alert2(r); if(a) items.push({tab:"a2",marca:r.nome,cs:"",
    motivo:a.text.replace(/^⏰ ?/,"").replace(/^🚫 ?/,""),level:a.level}); });
  return items;
}
function mitem(i){ return '<div class="mitem" data-go="'+i.tab+'"><span class="mmarca">'+esc(i.marca)+'</span>'+
  (i.cs?'<span class="mcs">'+esc(i.cs)+'</span>':"")+
  '<span class="alertpill'+(i.level==="warn"?" warn":"")+'">'+esc(i.motivo)+'</span></div>'; }
function buildCallModal(){
  var items=callItems();
  document.getElementById("callCount").textContent=items.length;
  var body=document.getElementById("callBody");
  if(!items.length){ body.innerHTML='<div class="empty">Nenhuma marca para chamar agora 🎉</div>'; return; }
  var g1=items.filter(function(i){return i.tab==="a1";}), g2=items.filter(function(i){return i.tab==="a2";});
  var html="";
  if(g1.length) html+='<div class="mgroup"><h4>📋 Passagem de bastão · '+g1.length+'</h4>'+g1.map(mitem).join("")+'</div>';
  if(g2.length) html+='<div class="mgroup"><h4>🔑 Acessos Tino · '+g2.length+'</h4>'+g2.map(mitem).join("")+'</div>';
  body.innerHTML=html;
  body.querySelectorAll("[data-go]").forEach(function(el){
    el.addEventListener("click",function(){ showTab(el.getAttribute("data-go")); closeCallModal(); });
  });
  var hideKey=localStorage.getItem("cs2_callmodal_hide");
  if(hideKey!==(D.hoje||"")) document.getElementById("callModal").classList.add("open");
}
function closeCallModal(){
  document.getElementById("callModal").classList.remove("open");
  if(document.getElementById("dontToday").checked) localStorage.setItem("cs2_callmodal_hide", D.hoje||"");
}
window.closeCallModal=closeCallModal;

/* ---------- tabs / boot ---------- */
function showTab(t){
  ["a1","a2","a3"].forEach(function(x){
    document.getElementById("panel-"+x).classList.toggle("active",x===t);
    document.querySelector('.tabs button[data-tab="'+x+'"]').classList.toggle("active",x===t);
  });
}
window.showTab=showTab;

function fillCS1(){
  var set={}; D.aba1.forEach(function(r){ if(r.cs) set[r.cs]=1; });
  var sel=document.getElementById("cs-a1");
  Object.keys(set).sort().forEach(function(cs){ var o=document.createElement("option"); o.value=cs; o.textContent="CS: "+cs; sel.appendChild(o); });
}
function wire(){
  ["q-a1","cs-a1","st-a1","al-a1"].forEach(function(id){ var e=document.getElementById(id);
    e.addEventListener(id.indexOf("q-")===0?"input":"change",renderA1); });
  ["q-a2","st-a2","al-a2"].forEach(function(id){ var e=document.getElementById(id);
    e.addEventListener(id.indexOf("q-")===0?"input":"change",renderA2); });
  document.getElementById("q-a3").addEventListener("input",renderA3);
  document.querySelectorAll("#cs-a3 button").forEach(function(b){ b.addEventListener("click",function(){
    document.querySelectorAll("#cs-a3 button").forEach(function(x){x.classList.remove("active");}); b.classList.add("active"); renderA3(); }); });
}
var booted=false;
window.__cs2_boot=function(){
  if(booted) return; booted=true;
  document.getElementById("gen").textContent="Atualizado "+(D.gerado_em||"");
  fillCS1(); wire();
  loadOverlays().then(function(){
    renderA1(); renderA2(); renderA3();
    cardsA1(); cardsA2(); cardsA3(); pills();
    buildCallModal();
  });
};
})();
