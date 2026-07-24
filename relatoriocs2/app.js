/* Relatório CS 2 — front. 3 abas estilo planilha.
   - TODA célula é editável (clique) em TODAS as abas; overrides salvos em overlays.
   - Observação é ÚNICA por marca: junta o texto original da Passagem de Bastão
     com o da aba Ranking Upsell e é compartilhada entre as duas abas.
   - Cor da linha na aba 1 (verde/vermelho/branco) manda no status e, por
     consequência, no quadro "Marcas para chamar", que é recalculado ao vivo. */
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
function pad2(n){ return String(n).padStart(2,"0"); }
function fmtDate(s){ var d=pdate(s); if(!d) return ""; return pad2(d.getDate())+"/"+pad2(d.getMonth()+1)+"/"+d.getFullYear(); }
function money(v){ if(v===null||v===undefined||v==="") return "";
  if(typeof v!=="number") return String(v);
  return "R$ "+Number(v).toLocaleString("pt-BR",{minimumFractionDigits:0,maximumFractionDigits:0}); }
function pct(v){ if(v===null||v===undefined||v==="") return "";
  if(typeof v!=="number") return String(v);
  return Number(v).toLocaleString("pt-BR",{maximumFractionDigits:1})+"%"; }
function esc(s){ return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;"); }
/* ícone do sprite inline (index.html). Sempre separado do texto: o texto puro é
   o que vai para o Excel e para o modal de marcas para chamar. */
function ic(nome,cls){ return nome?'<svg class="ic'+(cls?" "+cls:"")+'"><use href="#'+nome+'"/></svg>':""; }
function icEl(nome,cls){
  var s=document.createElementNS("http://www.w3.org/2000/svg","svg");
  s.setAttribute("class","ic"+(cls?" "+cls:""));
  var u=document.createElementNS("http://www.w3.org/2000/svg","use");
  u.setAttribute("href","#"+nome); s.appendChild(u); return s;
}
function deaccent(s){ return String(s==null?"":s).normalize("NFD").replace(/[\u0300-\u036f]/g,""); }

/* ---------- parsers (edição) ---------- */
function parseNumBR(s){
  if(s===null||s===undefined) return null;
  var t=String(s).trim(); if(t==="") return null;
  t=t.replace(/R\$/gi,"").replace(/%/g,"").replace(/[\s\u00a0]/g,"");
  if(t.indexOf(",")>=0) t=t.replace(/\./g,"").replace(",",".");
  var n=parseFloat(t);
  return isNaN(n) ? String(s).trim() : n;      // não numérico? guarda o texto
}
function parseDateBR(s){
  if(!s) return "";
  var t=String(s).trim(), m;
  m=t.match(/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})$/);
  if(m){ var y=+m[3]; if(y<100) y+=2000; return y+"-"+pad2(+m[2])+"-"+pad2(+m[1]); }
  m=t.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if(m) return m[1]+"-"+pad2(+m[2])+"-"+pad2(+m[3]);
  return t;
}
function parseBool(s){
  var t=deaccent(String(s==null?"":s)).trim().toLowerCase();
  if(t==="") return null;
  if(["sim","ok","true","1","x","ativo","s"].indexOf(t)>=0) return true;
  if(["nao","false","0","-","n"].indexOf(t)>=0) return false;
  return String(s).trim();
}
function toNum(v){
  if(v===null||v===undefined||v==="") return null;
  if(typeof v==="number") return isNaN(v)?null:v;
  var n=parseNumBR(v); return typeof n==="number"?n:null;
}

/* ---------- cores ---------- */
var COLORS=[
  {k:"sem cor",  c:""},
  {k:"branco",   c:"#FFFFFF"},
  {k:"verde",    c:"#B6D7A8"},
  {k:"vermelho", c:"#F4C7C3"},
  {k:"amarelo",  c:"#FFE599"},
  {k:"azul",     c:"#CFE2F3"},
  {k:"roxo",     c:"#D9D2E9"},
  {k:"laranja",  c:"#FCE5CD"}
];
var GREENS=["#B6D7A8","#D9EAD3","#93C47D","#6AA84F","#38761D"];
var REDS  =["#F4C7C3","#E06666","#EA9999","#EA6666","#F4CCCC","#CC0000"];
function isGreen(h){ return GREENS.indexOf((h||"").toUpperCase())>=0; }
function isRed(h){   return REDS.indexOf((h||"").toUpperCase())>=0; }
function isWhite(h){ var u=(h||"").toUpperCase(); return u==="#FFFFFF"||u==="#FFF"; }

/* ================= store: edições compartilhadas entre navegadores =================
   Fonte da verdade = relatoriocs2/overlays.json no repo, via API do stark-admin.
   localStorage é só cache (e salva-vidas quando a API está fora do ar).
   - escrita: as edições entram numa FILA e vão em lote 1,2s depois da última tecla
     (senão cada célula digitada viraria um commit);
   - leitura: poll a cada 25s traz o que as outras pessoas mudaram. */
var LS="cs2_overlays_v1", overlays={}, srvStamp="";
var fila={}, filaT=null, salvando=false, pollT=null;

function persistLocal(){ try{ localStorage.setItem(LS,JSON.stringify(overlays)); }catch(e){} }
function lerLocal(){ try{ overlays=JSON.parse(localStorage.getItem(LS)||"{}"); }catch(e){ overlays={}; } }
function senha(){ try{ return sessionStorage.getItem("cs2_pass")||""; }catch(e){ return ""; } }

var flashT;
function flash(msg){ var f=document.getElementById("flash"); f.textContent=msg||"salvo ✓"; f.classList.add("show");
  clearTimeout(flashT); flashT=setTimeout(function(){f.classList.remove("show");},1600); }
var SYNC={pend:["salvando…","p","ic-refresh"],ok:["salvo para todos","ok","ic-check"],
          off:["sem conexão — guardado aqui","er","ic-alert"],err:["não salvou para todos","er","ic-alert"],
          auth:["sessão expirada — clique para entrar","er","ic-alert"],
          only:["salvando só neste navegador","er","ic-alert"]};
var estadoSync="";
function setSync(st){ estadoSync=st; var e=document.getElementById("sync"); if(!e) return;
  var s=SYNC[st]; if(!s){ e.innerHTML=""; e.className="sync"; return; }
  e.innerHTML=ic(s[2])+"<span>"+s[0]+"</span>";
  e.className="sync "+s[1]+(st==="auth"?" clicavel":""); }
/* 401: nada foi perdido (a edição já está no navegador e a fila continua de pé).
   Pede a senha e reenvia o mesmo lote. */
var pedindoSenha=false;
function pedirSenha(){
  if(pedindoSenha) return; pedindoSenha=true;
  var p=window.prompt("Sua sessão expirou. Digite a senha do painel para salvar as edições — nada foi perdido:");
  pedindoSenha=false;
  if(!p) return;
  try{ sessionStorage.setItem("cs2_pass",p); sessionStorage.setItem("cs2_auth","1"); }catch(e){}
  setSync("pend"); agendarFlush(200);
}
window.pedirSenha=pedirSenha;

function ov(key){ return overlays[key]||{}; }
function enfileirar(key,valor){
  if(!API){ flash("salvo só neste navegador"); setSync("only"); return; }
  fila[key]=valor; setSync("pend"); agendarFlush();
}
function saveOverlay(key,patch){
  overlays[key]=Object.assign({},overlays[key],patch,{ts:(overlays[key]&&overlays[key].ts)||Date.now()});
  persistLocal(); enfileirar(key,overlays[key]);
}
function deleteOverlay(key){
  delete overlays[key]; persistLocal(); enfileirar(key,null);   // null = apagar no servidor
}

function agendarFlush(ms){ if(!API) return; clearTimeout(filaT); filaT=setTimeout(flush,ms===undefined?1200:ms); }
function devolver(lote){ Object.keys(lote).forEach(function(k){ if(fila[k]===undefined) fila[k]=lote[k]; }); }
function flush(){
  if(!API||salvando) return;
  var keys=Object.keys(fila); if(!keys.length) return;
  var lote={}; keys.forEach(function(k){ lote[k]=fila[k]; delete fila[k]; });   // tira da fila já
  var items=keys.map(function(k){ return {key:k,value:lote[k]}; });
  salvando=true;
  fetch(API+"/api/overlays",{method:"POST",
      headers:{"Content-Type":"application/json","Authorization":"Bearer "+senha()},
      body:JSON.stringify({ns:NS,items:items})})
    .then(function(r){ return r.json().catch(function(){return null;}).then(function(j){ return {st:r.status,j:j}; }); })
    .then(function(res){
      salvando=false;
      if(!res.j||!res.j.ok){
        devolver(lote);                                 // volta pra fila: nada se perde
        if(res.st===401){ setSync("auth"); flash("sessão expirada — entre de novo para salvar"); pedirSenha(); return; }
        setSync("err");
        flash((res.j&&res.j.erro)||"não consegui salvar para todos");
        agendarFlush(8000);
        return;
      }
      // aplica o que o servidor gravou (com o carimbo `up` dele, que é quem desempata)
      Object.keys(res.j.salvos||{}).forEach(function(k){
        if(fila[k]!==undefined) return;                 // já foi editado de novo aqui: não sobrescreve
        if(res.j.salvos[k]===null) delete overlays[k]; else overlays[k]=res.j.salvos[k];
      });
      srvStamp=res.j.atualizadoEm||srvStamp;
      persistLocal(); setSync("ok"); flash("salvo para todos ✓");
      if(Object.keys(fila).length) agendarFlush(300);
    })
    .catch(function(){
      salvando=false; devolver(lote); setSync("off");
      flash("sem conexão — guardei aqui e tento de novo"); agendarFlush(8000);
    });
}

/* junta o que veio do servidor sem atropelar o que ainda não foi salvo daqui */
function mergeRemote(remoto){
  var mudou=false;
  Object.keys(remoto).forEach(function(k){
    if(fila[k]!==undefined) return;                     // edição local pendente vence
    var loc=overlays[k];
    // chave sem `up` = editada aqui e ainda NÃO aceita pelo servidor (ex.: falhou
    // por sessão expirada). Ela vence o valor remoto e sobe depois — senão a
    // edição da pessoa sumiria na primeira recarga.
    if(loc && !loc.up) return;
    if(!loc || (remoto[k].up||0)>=(loc.up||0)){
      if(JSON.stringify(loc)!==JSON.stringify(remoto[k])){ overlays[k]=remoto[k]; mudou=true; }
    }
  });
  Object.keys(overlays).forEach(function(k){
    if(fila[k]!==undefined) return;
    // `up` só existe em chave que já passou pelo servidor: sumiu de lá = alguém apagou
    if(remoto[k]===undefined && overlays[k].up){ delete overlays[k]; mudou=true; }
  });
  if(mudou) persistLocal();
  return mudou;
}
/* edições antigas que só existiam neste navegador (sem `up`) sobem na primeira chance */
function subirPendentesLocais(){
  var n=0;
  Object.keys(overlays).forEach(function(k){ if(!overlays[k].up){ fila[k]=overlays[k]; n++; } });
  if(n){ setSync("pend"); flash("enviando "+n+" edição(ões) deste navegador…"); agendarFlush(200); }
}
function loadOverlays(){
  return new Promise(function(res){
    lerLocal();
    if(!API){ setSync("only"); return res(); }
    fetch(API+"/api/overlays?ns="+NS,{cache:"no-store"})
      .then(function(r){return r.json();})
      .then(function(j){
        if(!j||!j.ok) throw new Error("resposta inválida");
        srvStamp=j.atualizadoEm||"";
        mergeRemote(j.overlays||{});
        subirPendentesLocais();
        setSync(Object.keys(fila).length?"pend":"ok");
        res();
      })
      .catch(function(){ setSync("off"); res(); });   // segue com o cache local
  });
}
function editandoCelula(){
  var a=document.activeElement;
  return !!(a && a.closest && a.closest(".tbl-wrap") && /^(INPUT|TEXTAREA|SELECT)$/.test(a.tagName));
}
function pollOnce(){
  if(!API||document.hidden||salvando||Object.keys(fila).length||editandoCelula()) return;
  fetch(API+"/api/overlays?ns="+NS,{cache:"no-store"})
    .then(function(r){return r.json();})
    .then(function(j){
      if(!j||!j.ok) return;
      if(j.atualizadoEm && j.atualizadoEm===srvStamp) return;   // nada novo
      srvStamp=j.atualizadoEm||"";
      if(mergeRemote(j.overlays||{})){ reAll(); flash("atualizado por outra pessoa"); }
    })
    .catch(function(){});
}
function startPoll(){
  if(!API) return;
  clearInterval(pollT); pollT=setInterval(pollOnce,25000);
  document.addEventListener("visibilitychange",function(){ if(!document.hidden) pollOnce(); });
  window.addEventListener("beforeunload",function(){ if(Object.keys(fila).length) flush(); });
}
/* linhas adicionadas manualmente: overlays com chave "new:<tab>:<id>" */
function addedRows(tab){
  var pre="new:"+tab+":", out=[];
  Object.keys(overlays).forEach(function(k){ if(k.indexOf(pre)===0) out.push(Object.assign({__key:k},overlays[k])); });
  return out.sort(function(a,b){return (a.ts||0)-(b.ts||0);});
}
function newRowKey(tab){ return "new:"+tab+":"+Date.now().toString(36)+Math.floor(Math.random()*1e6).toString(36); }
/* linhas ocultas (originais "removidas" pelo usuário) */
function hiddenCount(pre){ var n=0; Object.keys(overlays).forEach(function(k){
  if(k.indexOf(pre)===0 && overlays[k].hidden) n++; }); return n; }
function restoreTab(tab){
  var pre=tab+":";
  Object.keys(overlays).forEach(function(k){ if(k.indexOf(pre)===0 && overlays[k].hidden) saveOverlay(k,{hidden:false}); });
  reAll();
}
window.restoreTab=restoreTab;

/* ---------- valor efetivo ---------- */
function pick(a,b){ return (a!==undefined&&a!==null) ? a : (b===undefined||b===null?"":b); }
/* CS efetiva (com override) */
function effCs(key,orig){ var o=ov(key); return (o.cs!==undefined&&o.cs!==null&&o.cs!=="")?o.cs:(orig||""); }

/* ---------- observação ÚNICA por marca (Passagem de Bastão + Ranking Upsell) ---------- */
function normName(s){ return (s==null?"":s).toString().toLowerCase().normalize("NFD").replace(/[^a-z0-9]/g,""); }
function obsKey(name){ return "obs:"+normName(name); }
var OBS_ORIG={};
function buildObsIndex(){
  OBS_ORIG={};
  function add(name,txt){
    var k=normName(name); if(!k) return;
    if(!OBS_ORIG[k]) OBS_ORIG[k]=[];
    txt=(txt==null?"":String(txt)).trim();
    if(txt && OBS_ORIG[k].indexOf(txt)<0) OBS_ORIG[k].push(txt);
  }
  D.aba1.forEach(function(r){ add(r.marca,r.obs_orig); });   // obs da Passagem de Bastão
  D.aba3.forEach(function(r){ add(r.empresa,r.obs_orig); }); // obs da Ranking Upsell
}
function obsOrig(name){ var a=OBS_ORIG[normName(name)]; return (a&&a.length)?a.join(" | "):""; }

/* ---------- célula editável: observação ----------
   Mostra o texto numa linha só (ellipsis + title). O textarea só nasce no clique
   — é ele que engordava toda linha da tabela. Salvamento e autosize preservados. */
function obsCell(key,orig){
  var td=document.createElement("td"); td.className="obs";
  function val(){ var o=ov(key); return (o.obs!==undefined&&o.obs!==null)?o.obs:(orig||""); }
  var span=document.createElement("span"); span.className="obsview";
  function paint(){
    var v=val();
    span.textContent=v||"observação";
    span.classList.toggle("ph",!v);
    td.title=v||"";
  }
  paint(); td.appendChild(span);
  td.addEventListener("click",function(){
    if(td.querySelector("textarea")) return;
    var antes=val();
    var ta=document.createElement("textarea"); ta.className="obsedit"; ta.value=antes; ta.rows=1;
    ta.placeholder="observação...";
    td.innerHTML=""; td.appendChild(ta);
    function autos(){ ta.style.height="auto"; ta.style.height=ta.scrollHeight+"px"; }
    ta.addEventListener("input",autos); autos(); ta.focus();
    var fechado=false;
    function fecha(salvar){
      if(fechado) return; fechado=true;
      var nv=ta.value;
      td.innerHTML=""; td.appendChild(span);
      if(salvar && nv!==antes) saveOverlay(key,{obs:nv});
      paint();
    }
    ta.addEventListener("blur",function(){ fecha(true); });
    ta.addEventListener("keydown",function(e){
      if(e.key==="Escape"){ e.preventDefault(); fecha(false); }
      else if(e.key==="Enter"&&(e.ctrlKey||e.metaKey)){ e.preventDefault(); fecha(true); }
    });
  });
  return td;
}

/* ---------- célula editável genérica (clique p/ editar) ---------- */
function editCell(key,field,orig,opt){
  opt=opt||{};
  var td=document.createElement("td"); td.className="edit"+(opt.cls?" "+opt.cls:"");
  var span=document.createElement("span"); span.className="cv";
  function cur(){ return pick(ov(key)[field],orig); }
  function paint(){
    var v=cur();
    var html=opt.fmt?opt.fmt(v):(v===""?"":esc(v));
    if(html===""||html===null||html===undefined)
      html=opt.ph?'<span class="ph">'+esc(opt.ph)+"</span>":'<span class="dash">—</span>';
    span.innerHTML=html;
  }
  paint(); td.appendChild(span);
  td.addEventListener("click",function(){
    if(td.querySelector("input")) return;
    var before=cur();
    var raw=(before===null||before===undefined)?"":String(before);
    if(opt.type==="date") raw=fmtDate(before)||raw;
    if(opt.type==="bool") raw=(before===true?"Sim":before===false?"Não":raw);
    var inp=document.createElement("input"); inp.type="text";
    inp.className="cellinp"+(opt.cls&&opt.cls.indexOf("num")>=0?" num":"");
    inp.value=raw; inp.placeholder=opt.ph||"";
    td.innerHTML=""; td.appendChild(inp); inp.focus(); inp.select();
    var closed=false;
    function close(save){
      if(closed) return; closed=true;
      var nv=inp.value;
      td.innerHTML=""; td.appendChild(span);
      if(save && nv!==raw){
        var parsed = opt.type==="num"  ? parseNumBR(nv)
                   : opt.type==="date" ? parseDateBR(nv)
                   : opt.type==="bool" ? parseBool(nv)  : nv;
        var p={}; p[field]=parsed; saveOverlay(key,p);
        paint();
        if(opt.onChange) opt.onChange(parsed);
      } else paint();
    }
    inp.addEventListener("blur",function(){close(true);});
    inp.addEventListener("keydown",function(e){
      if(e.key==="Enter"){ e.preventDefault(); close(true); }
      else if(e.key==="Escape"){ e.preventDefault(); close(false); }
    });
  });
  return td;
}

/* ---------- cor da linha: faixa de 3px na borda esquerda ----------
   A mesma faixa carrega DOIS sinais que não se anulam:
     - a cor dela = alerta automático (vermelho/âmbar), vindo de `alerta`;
     - clicar nela abre o seletor da cor MANUAL, que tinge o fundo da linha.
   A célula tem 6px de largura só pra dar área de clique; visualmente ela é a
   borda da linha, não uma coluna. */
function colorCell(key,defColor,onChange,alerta){
  var cur=(ov(key).color!==undefined&&ov(key).color!==null)?ov(key).color:(defColor||"");
  var td=document.createElement("td"); td.className="strip";
  td.title="cor da linha";
  if(alerta) td.style.setProperty("--stripc", alerta.level==="warn"?"var(--warn)":"var(--alert)");
  var pop=document.createElement("div"); pop.className="pop";
  COLORS.forEach(function(col){
    var b=document.createElement("div"); b.className="opt"+(col.c?"":" none");
    if(col.c) b.style.background=col.c; b.title=col.k;
    b.addEventListener("click",function(e){
      e.stopPropagation(); cur=col.c;
      pop.classList.remove("open");
      saveOverlay(key,{color:col.c});
      var tr=td.closest("tr");
      if(tr){ if(col.c){ tr.setAttribute("data-c","1"); tr.style.setProperty("--rowc",col.c); }
              else { tr.removeAttribute("data-c"); tr.style.removeProperty("--rowc"); } }
      if(onChange) onChange(col.c);
    });
    pop.appendChild(b);
  });
  td.addEventListener("click",function(e){ e.stopPropagation();
    document.querySelectorAll(".pop.open").forEach(function(p){if(p!==pop)p.classList.remove("open");});
    pop.classList.toggle("open"); });
  td.appendChild(pop); return td;
}
document.addEventListener("click",function(){ document.querySelectorAll(".pop.open").forEach(function(p){p.classList.remove("open");}); });

/* ---------- célula editável: CS (select) ---------- */
function csCell(key,orig,options,onChange){
  var td=document.createElement("td");
  var cur=effCs(key,orig);
  var sel=document.createElement("select"); sel.className="csedit";
  var opts=options.slice();
  if(cur && opts.indexOf(cur)<0) opts.unshift(cur);
  if(opts.indexOf("")<0) opts.unshift("");
  opts.forEach(function(o){ var op=document.createElement("option"); op.value=o; op.textContent=o||"—"; if(o===cur)op.selected=true; sel.appendChild(op); });
  sel.addEventListener("change",function(){ saveOverlay(key,{cs:sel.value}); if(onChange)onChange(sel.value); });
  td.appendChild(sel); return td;
}
/* ---------- célula de ações (remover / ocultar linha) ---------- */
function actCell(key,added,rerender){
  var td=document.createElement("td"); td.className="actcell";
  var b=document.createElement("button"); b.className="delrow"; b.appendChild(icEl("ic-trash"));
  b.title=added?"remover linha adicionada":"ocultar esta linha";
  b.addEventListener("click",function(e){
    e.stopPropagation();
    if(added){ if(confirm("Remover esta linha adicionada?")){ deleteOverlay(key); rerender(); } }
    else if(confirm("Ocultar esta linha? (dá para restaurar pelo botão ↺ da barra)")){
      saveOverlay(key,{hidden:true}); rerender(); }
  });
  td.appendChild(b); return td;
}
function cell(html,cls){ var td=document.createElement("td"); if(cls)td.className=cls; td.innerHTML=html; return td; }

/* ---------- header com sort ---------- */
function makeHead(cols,state,rerender){
  var tr=document.createElement("tr");
  cols.forEach(function(c){
    var th=document.createElement("th"); th.textContent=c.label;
    if(c.cls) th.className=c.cls;
    if(c.title) th.title=c.title;
    if(c.sort){ th.classList.add("sortable");
      if(state.sort===c.key){ var ar=document.createElement("span"); ar.className="ar";
        ar.textContent=state.dir>0?" ↑":" ↓"; th.appendChild(ar); }
      th.addEventListener("click",function(){ if(state.sort===c.key)state.dir*=-1; else {state.sort=c.key;state.dir=c.def||-1;} rerender(); });
    }
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
function undelBtn(tab){
  var b=document.getElementById("und-"+tab); if(!b) return;
  var n=hiddenCount(tab+":");
  b.style.display=n?"inline-flex":"none";
  b.innerHTML=ic("ic-refresh")+"restaurar "+n+" oculta(s)";
}

/* ================= ABA 1 — Passagem de bastão ================= */
var s1={sort:"dias",dir:-1};
var CS1_OPTS=["Luana","Thamiris","Gabriella Busto","Elisa","Alexia","Tatiane"];
/* branco / sem cor => sem reunião (entra no quadro de avisos)
   verde => ativa · vermelho => cancelada (saem do quadro de avisos) */
function statusFromColor(c){
  if(!c || isWhite(c)) return "sem_reuniao";
  if(isGreen(c)) return "ativa";
  if(isRed(c))   return "cancelada";
  return "sem_reuniao";
}
// alerta só p/ marcas SEM REUNIÃO (branco/sem cor); 45d (warn) / 60d / 90+d
function alertFor1(entradaStr,status){
  if(status!=="sem_reuniao") return null;
  var cad=pdate(entradaStr);
  if(!cad) return null;
  var d=daysBetween(today(),cad);
  // texto limpo (vai pro Excel e pro modal); o ícone é montado só na célula
  if(d>=90) return {level:"alert",icon:"ic-clock",text:"sem reunião ("+d+" dias)"};
  if(d>=60) return {level:"alert",icon:"ic-clock",text:"60 dias"};
  if(d>=45) return {level:"warn", icon:"ic-clock",text:"45 dias"};
  return null;
}
function defColor1(r){ var st=r.status;
  return st==="ativa"?"#B6D7A8":st==="cancelada"?"#F4C7C3":""; }

/* linha efetiva da aba 1 (original ou adicionada), já com todos os overrides */
function eff1row(r){
  var k=r.__key||("a1:"+r.marca), o=ov(k);
  var color=(o.color!==undefined&&o.color!==null)?o.color:defColor1(r);
  var entrada=pick(o.entrada,r.entrada), d=pdate(entrada);
  var st=statusFromColor(color);
  return { key:k, added:!!r.__key, src:r, hidden:!!o.hidden,
    marca:pick(o.marca,r.marca), entrada:entrada, dias:d?daysBetween(today(),d):null,
    implementador:pick(o.implementador,r.implementador), cs:effCs(k,r.cs),
    data25:pick(o.data25,r.data25), color:color, status:st, alert:alertFor1(entrada,st) };
}
function all1(){
  var out=[];
  addedRows("a1").forEach(function(r){ out.push(eff1row(r)); });
  D.aba1.forEach(function(r){ out.push(eff1row(r)); });
  return out.filter(function(r){ return !r.hidden; });
}

var STLABEL={ativa:"Ativa",cancelada:"Cancelada",sem_reuniao:"Sem reunião"};
function stCell(stx){ return cell('<span class="st '+stx+'">'+STLABEL[stx]+"</span>","derived"); }
function alPill(al){ return al?'<span class="alertpill'+(al.level==="warn"?" warn":"")+'">'+ic(al.icon)+esc(al.text)+"</span>":""; }
function alCell(al){ return cell(alPill(al),"derived"); }   /* sem alerta = célula vazia */

var re1=function(){ renderA1(); cardsA1(); syncAlerts(); };
function renderA1(){
  var q=(document.getElementById("q-a1").value||"").toLowerCase();
  var cs=document.getElementById("cs-a1").value;
  var st=document.getElementById("st-a1").value;
  var cf=cardFiltro.a1;                                   // filtro vindo do card clicado
  var rows=all1().filter(function(r){
    if(q && String(r.marca||"").toLowerCase().indexOf(q)<0) return false;
    if(cs && r.cs!==cs) return false;
    if(st && r.status!==st) return false;
    if(cf==="alerta" ? !r.alert : (cf && r.status!==cf)) return false;
    return true;
  });
  var added=rows.filter(function(r){return r.added;});
  var base =rows.filter(function(r){return !r.added;});
  var acc={ marca:function(r){return r.marca;},
    entrada:function(r){var d=pdate(r.entrada);return d?d.getTime():null;},
    dias:function(r){return r.dias;}, cs:function(r){return r.cs;},
    impl:function(r){return r.implementador;},
    data25:function(r){var d=pdate(r.data25);return d?d.getTime():null;} };
  base=sortRows(base,s1,acc);

  var cols=[
    {label:"",cls:"strip",title:"clique na faixa para pintar a linha"},
    {label:"Marca",key:"marca",sort:1,def:1},
    {label:"Entrada",key:"entrada",sort:1},
    {label:"Dias",key:"dias",sort:1,num:1},
    {label:"Implementador",key:"impl",sort:1},
    {label:"CS",key:"cs",sort:1},
    {label:"25+ / Últ. marco",key:"data25",sort:1},
    {label:"Status"},{label:"Alerta"},{label:"Observação"},{label:""}
  ];
  var tbl=document.getElementById("tbl-a1"); tbl.innerHTML="";
  var thead=document.createElement("thead"); thead.appendChild(makeHead(cols,s1,renderA1)); tbl.appendChild(thead);
  var tb=document.createElement("tbody");

  added.concat(base).forEach(function(r){
    var key=r.key, tr=document.createElement("tr");
    if(r.added) tr.className="addedrow";
    if(r.color){ tr.setAttribute("data-c","1"); tr.style.setProperty("--rowc",r.color); }
    tr.appendChild(colorCell(key,defColor1(r.src),re1,r.alert));
    tr.appendChild(editCell(key,"marca",r.src.marca,{cls:"marca",ph:r.added?"nova marca":"",
      fmt:function(v){return v===""?"":'<span class="marca trunc" title="'+esc(v)+'">'+esc(v)+"</span>";},onChange:re1}));
    tr.appendChild(editCell(key,"entrada",r.src.entrada,{cls:"nowrap mut",type:"date",ph:"dd/mm/aaaa",
      fmt:function(v){return fmtDate(v)||(v?esc(v):"");},onChange:re1}));
    tr.appendChild(cell(r.dias===null?"":r.dias,"num derived"));
    tr.appendChild(editCell(key,"implementador",r.src.implementador,{ph:"implementador"}));
    tr.appendChild(csCell(key,r.src.cs,CS1_OPTS,re1));
    tr.appendChild(editCell(key,"data25",r.src.data25,{cls:"nowrap mut",type:"date",ph:"dd/mm/aaaa",
      fmt:function(v){return fmtDate(v)||(v?esc(v):"");}}));
    tr.appendChild(stCell(r.status));
    tr.appendChild(alCell(r.alert));
    tr.appendChild(obsCell(obsKey(r.marca),obsOrig(r.marca)));   // obs única por marca (Bastão + Ranking)
    tr.appendChild(actCell(key,r.added,re1));
    tb.appendChild(tr);
  });
  tbl.appendChild(tb);
  if(!rows.length) tbl.innerHTML+='<tbody><tr><td colspan="11" class="empty">Nenhuma marca com esses filtros.</td></tr></tbody>';
  document.getElementById("cnt-a1").textContent=rows.length+" marca(s)";
  undelBtn("a1");
}

function cardsA1(){
  var a=all1(), ativa=0,canc=0,sem=0,al=0;
  a.forEach(function(r){ if(r.status==="ativa")ativa++;else if(r.status==="cancelada")canc++;else sem++; if(r.alert)al++; });
  var el=document.getElementById("cards-a1");
  el.innerHTML=
    card(a.length,"Marcas",{f:""})+card(ativa,"Ativas",{f:"ativa"})+card(canc,"Canceladas",{f:"cancelada"})+
    card(sem,"Sem reunião",{f:"sem_reuniao"})+card(al,"Com alerta",{f:"alerta",al:1})+
    card(callItems().length,"Para chamar",{acao:"modal",al:1});
  wireCards(el,"a1",re1);
}
function addRow(tab){
  saveOverlay(newRowKey(tab),{ts:Date.now()});
  if(tab==="a1") re1(); else if(tab==="a2") re2(); else if(tab==="a3") re3();
}
window.addRow=addRow;

/* ================= ABA 2 — Tino ================= */
var s2={sort:"dias",dir:-1};
function alert2(dias){
  // texto limpo: ele é reaproveitado no Excel e no modal de marcas para chamar
  if(dias===null||dias===undefined) return {level:"alert",icon:"ic-ban",text:"nunca acessou"};
  if(dias>15) return {level:"warn",icon:"ic-clock",text:dias+"d sem acessar"};
  return null;
}
function eff2row(r){
  var k=r.__key||("a2:"+r.company), o=ov(k);
  var last=pick(o.last_login,r.last_login), d=pdate(last);
  var dias=d?daysBetween(today(),d):null;
  return { key:k, added:!!r.__key, src:r, hidden:!!o.hidden,
    nome:pick(o.nome,r.nome), company:r.company||"",
    status:pick(o.status,r.status), created_at:pick(o.created_at,r.created_at),
    last_login:last, dias:dias, login_days:pick(o.login_days,r.login_days),
    color:pick(o.color,""), alert:alert2(dias) };
}
function all2(){
  var out=[];
  addedRows("a2").forEach(function(r){ out.push(eff2row(r)); });
  D.aba2.forEach(function(r){ out.push(eff2row(r)); });
  return out.filter(function(r){ return !r.hidden; });
}
/* mexer na aba 2 muda a coluna Tino automática da aba 3 — por isso re-renderiza as duas */
var re2=function(){ renderA2(); cardsA2(); syncAlerts(); renderA3(); cardsA3(); };
function renderA2(){
  var q=(document.getElementById("q-a2").value||"").toLowerCase();
  var st=document.getElementById("st-a2").value;
  var cf=cardFiltro.a2;
  var rows=all2().filter(function(r){
    if(q && String(r.nome||"").toLowerCase().indexOf(q)<0 && String(r.company||"").toLowerCase().indexOf(q)<0) return false;
    if(st && r.status!==st) return false;
    if(cf==="alerta" && !r.alert) return false;
    if(cf==="nunca"  && r.dias!==null) return false;
    if(cf==="mais15" && !(r.dias!==null && r.dias>15)) return false;
    return true;
  });
  var added=rows.filter(function(r){return r.added;});
  var base =sortRows(rows.filter(function(r){return !r.added;}),s2,{
    nome:function(r){return r.nome;},
    dias:function(r){return r.dias===null?99999:r.dias;},          // nunca acessou no topo
    last:function(r){var d=pdate(r.last_login);return d?d.getTime():-1;},
    login:function(r){return toNum(r.login_days);},
    created:function(r){var d=pdate(r.created_at);return d?d.getTime():null;} });
  var cols=[{label:"",cls:"strip",title:"clique na faixa para pintar a linha"},
    {label:"Marca",key:"nome",sort:1,def:1},{label:"Status"},
    {label:"Criado em",key:"created",sort:1},
    {label:"Último acesso",key:"dias",sort:1,title:"data do último acesso · dias desde então"},
    {label:"Dias c/ login",key:"login",sort:1,num:1},
    {label:"Alerta"},{label:"Observação"},{label:""}];
  var tbl=document.getElementById("tbl-a2"); tbl.innerHTML="";
  var thead=document.createElement("thead"); thead.appendChild(makeHead(cols,s2,renderA2)); tbl.appendChild(thead);
  var tb=document.createElement("tbody");
  added.concat(base).forEach(function(r){
    var key=r.key, tr=document.createElement("tr");
    if(r.added) tr.className="addedrow";
    if(r.color){ tr.setAttribute("data-c","1"); tr.style.setProperty("--rowc",r.color); }
    tr.appendChild(colorCell(key,"",re2,r.alert));
    tr.appendChild(editCell(key,"nome",r.src.nome,{cls:"marca",ph:r.added?"nova marca":"",
      fmt:function(v){return v===""?"":'<span class="marca trunc" title="'+esc(v)+'">'+esc(v)+"</span>";},onChange:re2}));
    tr.appendChild(editCell(key,"status",r.src.status,{ph:"active/inactive",
      fmt:function(v){return v===""?"":'<span class="st '+(v==="active"?"on":"off")+'">'+esc(v)+"</span>";},onChange:re2}));
    tr.appendChild(editCell(key,"created_at",r.src.created_at,{cls:"nowrap mut",type:"date",ph:"dd/mm/aaaa",
      fmt:function(v){return fmtDate(v)||(v?esc(v):"");}}));
    // "Último acesso" e "Dias sem acessar" numa coluna só: 02/07 · 22d
    tr.appendChild(editCell(key,"last_login",r.src.last_login,{cls:"nowrap",type:"date",ph:"nunca",
      fmt:function(v){
        var d=fmtDate(v);
        if(!d) return '<span class="dash">nunca</span>';
        return esc(d.slice(0,5))+' <span class="mut">· '+(r.dias===null?"":r.dias+"d")+"</span>";
      },onChange:re2}));
    tr.appendChild(editCell(key,"login_days",r.src.login_days,{cls:"num mut",type:"num",ph:"0"}));
    tr.appendChild(cell(alPill(r.alert),"derived"));
    tr.appendChild(obsCell(key,""));
    tr.appendChild(actCell(key,r.added,re2));
    tb.appendChild(tr);
  });
  tbl.appendChild(tb);
  if(!rows.length) tbl.innerHTML+='<tbody><tr><td colspan="9" class="empty">Nenhuma marca com esses filtros.</td></tr></tbody>';
  document.getElementById("cnt-a2").textContent=rows.length+" marca(s)";
  undelBtn("a2");
}
/* ---------- Tino ao vivo (rota /api/tino, com cache na CDN) ----------
   O dado da aba 2 nasce do build diário; aqui ele é substituído pelo que a API
   devolve agora. A API do Tino demora 4-9s, por isso o servidor cacheia 5 min e
   o painel só pede de 5 em 5 min — nada a ver com o poll de 25s das edições.
   Se falhar ou vier vazio, MANTÉM o dado do build (nunca zera a aba). */
var tinoT=null, tinoEm="";
function fmtHora(iso){ var d=new Date(iso); return isNaN(d)?"":
  String(d.getHours()).padStart(2,"0")+":"+String(d.getMinutes()).padStart(2,"0"); }
function statusTino(txt,cls,icone){ var e=document.getElementById("tino-st");
  if(e){ e.innerHTML=ic(icone||"ic-key")+"<span>"+esc(txt)+"</span>"; e.className="sync "+(cls||""); } }
function refreshTino(forcado){
  if(!API){ statusTino("dado do build diário","","ic-key"); return; }
  ultimoTino=Date.now();
  statusTino(forcado?"buscando no Tino…":"atualizando…","p","ic-refresh");
  fetch(API+"/api/tino"+(forcado?"?fresh=1":""),{cache:"no-store"})
    .then(function(r){ return r.json(); })
    .then(function(j){
      if(!j||!j.ok||!j.marcas||!j.marcas.length){
        statusTino("não consegui atualizar — dado do build","er","ic-alert"); return;
      }
      D.aba2=j.marcas; tinoEm=j.atualizadoEm||"";
      renderA2(); cardsA2(); syncAlerts();
      renderA3(); cardsA3();                 // muda a coluna Tino automática da aba 3
      if(j.obsoleto) statusTino("última leitura do servidor (origem fora do ar)","er","ic-alert");
      else statusTino("atualizado às "+fmtHora(tinoEm)+" · "+j.total+" marcas","ok","ic-key");
    })
    .catch(function(){ statusTino("sem conexão — dado do build","er","ic-alert"); });
}
window.refreshTino=refreshTino;
var ultimoTino=0;
function startTino(){
  if(!API) return;
  refreshTino(false);
  clearInterval(tinoT);
  tinoT=setInterval(function(){ if(!document.hidden) refreshTino(false); },5*60*1000);
  // voltou pra aba depois de um tempo? busca na hora, em vez de esperar o tique
  document.addEventListener("visibilitychange",function(){
    if(!document.hidden && Date.now()-ultimoTino>2*60*1000) refreshTino(false);
  });
}

function cardsA2(){
  var a=all2(), nunca=0,mais15=0;
  a.forEach(function(r){ if(r.dias===null)nunca++; else if(r.dias>15)mais15++; });
  var el=document.getElementById("cards-a2");
  el.innerHTML=
    card(a.length,"Marcas c/ Tino",{f:""})+card(mais15,"+15d sem acessar",{f:"mais15",al:1})+
    card(nunca,"Nunca acessaram",{f:"nunca",al:1})+card(mais15+nunca,"Total alertas",{f:"alerta",al:1})+
    card(callItems().length,"Para chamar",{acao:"modal",al:1});
  wireCards(el,"a2",re2);
}

/* ================= ABA 3 — Ranking Upsell ================= */
var s3={sort:"cresc_rs",dir:-1};
var CS3_OPTS=["Busto","Luana","Thamiris"];

/* Coluna Tino automática: se a marca aparece na aba "Acessos Tino", a coluna Tino
   da Ranking Upsell vira "Sim" sozinha (o slug da API casa com o nome da planilha:
   nova_versao_roupas -> "Nova Versão Roupas").
   NUNCA vira "Não" sozinha: nem toda marca do Tino casa por nome, então NÃO estar
   na lista não prova que a marca não tem Tino — nesse caso vale o que diz a planilha.
   Edição manual sempre vence; apagar a célula devolve o valor automático. */
var TINO_IDX=null;
function buildTinoIdx(){
  TINO_IDX={};
  all2().forEach(function(r){
    [r.company,r.nome].forEach(function(n){ var k=normName(n); if(k) TINO_IDX[k]=true; });
  });
  return TINO_IDX;
}
function temTino(nome){ var k=normName(nome); return !!(k && (TINO_IDX||buildTinoIdx())[k]); }

function eff3row(r){
  var k=r.__key||("a3:"+r.cs_tab+":"+r.empresa), o=ov(k);
  var gA=toNum(pick(o.gmv_ant,r.gmv_ant)), gB=toNum(pick(o.gmv_atual,r.gmv_atual));
  var cr = (o.cresc_rs!==undefined&&o.cresc_rs!==null&&o.cresc_rs!=="") ? toNum(o.cresc_rs)
         : (gA!==null&&gB!==null ? gB-gA : toNum(r.cresc_rs));
  var cp = (o.cresc_pct!==undefined&&o.cresc_pct!==null&&o.cresc_pct!=="") ? toNum(o.cresc_pct)
         : (gA ? (cr!==null?cr/gA*100:null) : toNum(r.cresc_pct));
  var empresa=pick(o.empresa,r.empresa);
  var tinoAuto=temTino(empresa);                        // marca está na aba Acessos Tino?
  var tinoBase=tinoAuto?true:(r.tino===undefined?null:r.tino);
  return { key:k, added:!!r.__key, src:r, hidden:!!o.hidden,
    empresa:empresa, cs:effCs(k,r.cs_tab||""),
    plano:pick(o.plano,r.plano), gmv_ant:gA, gmv_atual:gB, cresc_rs:cr, cresc_pct:cp,
    mensalidade:pick(o.mensalidade,r.mensalidade),
    tinoAuto:tinoAuto, tinoBase:tinoBase,
    // override null (célula apagada) cai de volta no automático — de propósito
    tino:(o.tino!==undefined&&o.tino!==null)?o.tino:tinoBase,
    vestipago:(o.vestipago!==undefined?o.vestipago:(r.vestipago===undefined?null:r.vestipago)),
    color:(o.color!==undefined&&o.color!==null)?o.color:(r.color||"") };
}
function all3(){
  buildTinoIdx();                                       // reflete edições feitas na aba 2
  var out=[];
  addedRows("a3").forEach(function(r){ out.push(eff3row(r)); });
  D.aba3.forEach(function(r){ out.push(eff3row(r)); });
  return out.filter(function(r){ return !r.hidden; });
}
var re3=function(){ renderA3(); cardsA3(); };
function renderA3(){
  var q=(document.getElementById("q-a3").value||"").toLowerCase();
  var csBtn=document.querySelector("#cs-a3 button.active"); var cs=csBtn?csBtn.getAttribute("data-cs"):"";
  var rows=all3().filter(function(r){
    if(cs && r.cs!==cs) return false;
    if(q && String(r.empresa||"").toLowerCase().indexOf(q)<0) return false;
    return true;
  });
  var added=rows.filter(function(r){return r.added;});
  var base =sortRows(rows.filter(function(r){return !r.added;}),s3,{
    empresa:function(r){return r.empresa;}, cs:function(r){return r.cs;}, plano:function(r){return r.plano;},
    gmv_ant:function(r){return r.gmv_ant;}, gmv_atual:function(r){return r.gmv_atual;},
    cresc_rs:function(r){return r.cresc_rs;}, cresc_pct:function(r){return r.cresc_pct;} });
  var cols=[{label:"",cls:"strip",title:"clique na faixa para pintar a linha"},
    {label:"Empresa",key:"empresa",sort:1,def:1},{label:"CS",key:"cs",sort:1},
    {label:"Plano",key:"plano",sort:1},{label:"GMV anterior",key:"gmv_ant",sort:1,num:1},
    {label:"GMV atual",key:"gmv_atual",sort:1,num:1},{label:"Cresc. R$",key:"cresc_rs",sort:1,num:1},
    {label:"Cresc. %",key:"cresc_pct",sort:1,num:1},{label:"Mensalidade"},
    {label:"Tino",title:'Preenchido automaticamente quando a marca aparece na aba Acessos Tino. Digite na célula para forçar outro valor; apagando, volta ao automático. Não estar na lista nunca vira "Não" sozinho.'},
    {label:"VestiPago"},{label:"Observação"},{label:""}];
  var tbl=document.getElementById("tbl-a3"); tbl.innerHTML="";
  var thead=document.createElement("thead"); thead.appendChild(makeHead(cols,s3,renderA3)); tbl.appendChild(thead);
  var tb=document.createElement("tbody");

  added.concat(base).forEach(function(r){
    var key=r.key, tr=document.createElement("tr");
    if(r.added) tr.className="addedrow";
    if(r.color){ tr.setAttribute("data-c","1"); tr.style.setProperty("--rowc",r.color); }
    tr.appendChild(colorCell(key,r.src.color||"",re3,null));
    tr.appendChild(editCell(key,"empresa",r.src.empresa,{cls:"marca",ph:r.added?"nova empresa":"",
      fmt:function(v){return v===""?"":'<span class="marca trunc" title="'+esc(v)+'">'+esc(v)+"</span>";},onChange:re3}));
    tr.appendChild(csCell(key,r.src.cs_tab,CS3_OPTS,re3));
    tr.appendChild(editCell(key,"plano",r.src.plano,{ph:"plano"}));
    tr.appendChild(editCell(key,"gmv_ant",r.src.gmv_ant,{cls:"num mut",type:"num",ph:"0",fmt:money,onChange:re3}));
    tr.appendChild(editCell(key,"gmv_atual",r.src.gmv_atual,{cls:"num",type:"num",ph:"0",fmt:money,onChange:re3}));
    tr.appendChild(editCell(key,"cresc_rs",r.cresc_rs,{cls:"num",type:"num",ph:"0",fmt:money,onChange:re3}));
    tr.appendChild(editCell(key,"cresc_pct",r.cresc_pct,{cls:"num",type:"num",ph:"%",fmt:pct,onChange:re3}));
    tr.appendChild(editCell(key,"mensalidade",r.src.mensalidade,{cls:"nowrap mut",ph:"R$"}));
    tr.appendChild(editCell(key,"tino",r.tinoBase,{type:"bool",onChange:re3,fmt:function(v){
      if(v===true && r.tinoAuto && ov(key).tino==null)
        return '<span class="st on auto" title="Automático: esta marca aparece na aba Acessos Tino. Para forçar outro valor, digite aqui; apagando a célula ela volta ao automático.">'+ic("ic-key")+"Sim</span>";
      return boolPill(v);
    }}));
    tr.appendChild(editCell(key,"vestipago",r.src.vestipago,{type:"bool",fmt:boolPill}));
    tr.appendChild(obsCell(obsKey(r.empresa),obsOrig(r.empresa)));  // mesma obs da Passagem de Bastão
    tr.appendChild(actCell(key,r.added,re3));
    tb.appendChild(tr);
  });
  tbl.appendChild(tb);
  if(!rows.length) tbl.innerHTML+='<tbody><tr><td colspan="13" class="empty">Nenhuma empresa com esses filtros.</td></tr></tbody>';
  document.getElementById("cnt-a3").textContent=rows.length+" empresa(s)";
  undelBtn("a3");
}
function boolPill(v){ if(v===true) return '<span class="st on">Sim</span>';
  if(v===false) return '<span class="st off">Não</span>';
  if(v===null||v===undefined||v==="") return "";
  return '<span class="mut">'+esc(v)+'</span>'; }
/* Os cards da aba 3 comandam o MESMO seletor de CS da barra (não um segundo
   estado paralelo, que poderia contradizer o segmentado). */
function cardsA3(){
  var a=all3();
  var by={Busto:0,Luana:0,Thamiris:0}; a.forEach(function(r){ if(by[r.cs]!=null)by[r.cs]++; });
  var el=document.getElementById("cards-a3");
  el.innerHTML=
    cardCs(a.length,"Empresas","")+cardCs(by.Busto,"Busto","Busto")+
    cardCs(by.Luana,"Luana","Luana")+cardCs(by.Thamiris,"Thamiris","Thamiris");
  el.querySelectorAll("[data-cs]").forEach(function(c){
    var cs=c.getAttribute("data-cs");
    var btn=document.querySelector('#cs-a3 button[data-cs="'+cs+'"]');
    if(btn && btn.classList.contains("active")) c.classList.add("on");
    c.addEventListener("click",function(){ if(btn) btn.click(); });
  });
}
function cardCs(v,l,cs){
  return '<div class="card" data-cs="'+esc(cs)+'"><div class="v">'+v+'</div>'+
         '<div class="l">'+esc(l)+ic("ic-x","x")+'</div></div>';
}

function reAll(){ re1(); re2(); re3(); }

/* ---------- cards = filtros ----------
   Cada card guarda o critério em data-f; clicar filtra a tabela e marca o card,
   clicar de novo limpa. O estado fica em cardFiltro[aba] e é aplicado dentro do
   filter() do render. O card "Para chamar" não filtra: abre o modal. */
var cardFiltro={a1:"",a2:"",a3:""};
function card(v,l,o){
  o=o||{};
  var attr=o.acao?' data-acao="'+o.acao+'"':(o.f!==undefined?' data-f="'+esc(o.f)+'"':"");
  return '<div class="card'+(o.al?" al":"")+'"'+attr+'><div class="v">'+v+'</div>'+
         '<div class="l">'+esc(l)+ic("ic-x","x")+'</div></div>';
}
function wireCards(el,tab,re){
  el.querySelectorAll("[data-f]").forEach(function(c){
    var f=c.getAttribute("data-f");
    if((cardFiltro[tab]||"")===f) c.classList.add("on");
    c.addEventListener("click",function(){ cardFiltro[tab]=(cardFiltro[tab]===f)?"":f; re(); });
  });
  el.querySelectorAll('[data-acao="modal"]').forEach(function(c){
    c.addEventListener("click",openCallModal);
  });
}
/* pills das abas saem da MESMA lista do quadro de avisos (sempre batem) */
function pills(){
  var it=callItems();
  setPill("pill-a1",it.filter(function(i){return i.tab==="a1";}).length);
  setPill("pill-a2",it.filter(function(i){return i.tab==="a2";}).length);
}
function setPill(id,n){ var e=document.getElementById(id); if(!e) return; e.textContent=n; e.classList.toggle("zero",n===0); }

/* ---------- export ---------- */
function exportTab(which){
  var tbl=document.getElementById("tbl-"+which).cloneNode(true);
  tbl.querySelectorAll(".pop").forEach(function(p){p.remove();});
  tbl.querySelectorAll("svg").forEach(function(s){s.remove();});   // ícone não vai pro Excel
  tbl.querySelectorAll(".ar").forEach(function(s){s.remove();});   // nem a setinha de ordenação
  tbl.querySelectorAll("textarea,input").forEach(function(t){ var s=document.createElement("span"); s.textContent=t.value; t.parentNode.replaceChild(s,t); });
  tbl.querySelectorAll("select").forEach(function(t){ var s=document.createElement("span"); s.textContent=t.value; t.parentNode.replaceChild(s,t); });
  tbl.querySelectorAll("tr").forEach(function(tr){
    if(!tr.cells||!tr.cells.length) return;
    tr.deleteCell(tr.cells.length-1);   // ações
    tr.deleteCell(0);                   // cor
  });
  var html='<html><head><meta charset="utf-8"></head><body>'+tbl.outerHTML+'</body></html>';
  var blob=new Blob(["﻿"+html],{type:"application/vnd.ms-excel"});
  var url=URL.createObjectURL(blob), a=document.createElement("a");
  a.href=url; a.download="relatorio_cs_"+which+"_"+(D.hoje||"")+".xls"; a.click(); URL.revokeObjectURL(url);
}
window.exportTab=exportTab;

/* ---------- quadro de aviso "marcas para chamar" (ligado às cores da aba 1) ---------- */
function callItems(){
  var items=[];   // o texto do alerta já vem limpo (ícone é separado)
  all1().forEach(function(r){ if(r.alert && r.marca) items.push({tab:"a1",marca:r.marca,cs:r.cs,al:r.alert}); });
  all2().forEach(function(r){ if(r.alert && r.nome) items.push({tab:"a2",marca:r.nome,cs:"",al:r.alert}); });
  return items;
}
function mitem(i){ return '<div class="mitem" data-go="'+i.tab+'"><span class="mmarca">'+esc(i.marca)+'</span>'+
  (i.cs?'<span class="mcs">'+esc(i.cs)+'</span>':"")+alPill(i.al)+'</div>'; }
/* recalcula o quadro AO VIVO — chamado sempre que uma cor/data muda */
function refreshCallPanel(){
  var items=callItems();
  setPill("callCount",items.length);
  setPill("callBadge",items.length);
  var body=document.getElementById("callBody");
  if(!items.length){ body.innerHTML='<div class="empty">Nenhuma marca para chamar agora.</div>'; return items; }
  var g1=items.filter(function(i){return i.tab==="a1";}), g2=items.filter(function(i){return i.tab==="a2";});
  var html="";
  if(g1.length) html+='<div class="mgroup"><h4>'+ic("ic-list")+'Passagem de bastão · '+g1.length+'</h4>'+g1.map(mitem).join("")+'</div>';
  if(g2.length) html+='<div class="mgroup"><h4>'+ic("ic-key")+'Acessos Tino · '+g2.length+'</h4>'+g2.map(mitem).join("")+'</div>';
  body.innerHTML=html;
  body.querySelectorAll("[data-go]").forEach(function(el){
    el.addEventListener("click",function(){ showTab(el.getAttribute("data-go")); closeCallModal(); });
  });
  return items;
}
function syncAlerts(){ pills(); refreshCallPanel(); }
function openCallModal(){ refreshCallPanel(); document.getElementById("callModal").classList.add("open"); }
window.openCallModal=openCallModal;
function closeCallModal(){
  document.getElementById("callModal").classList.remove("open");
  if(document.getElementById("dontToday").checked) localStorage.setItem("cs2_callmodal_hide", D.hoje||"");
}
window.closeCallModal=closeCallModal;
function bootCallModal(){
  var items=refreshCallPanel();
  var hideKey=localStorage.getItem("cs2_callmodal_hide");
  if(items.length && hideKey!==(D.hoje||"")) document.getElementById("callModal").classList.add("open");
}

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
  CS1_OPTS.forEach(function(c){ set[c]=1; });
  var sel=document.getElementById("cs-a1");
  Object.keys(set).sort().forEach(function(cs){ var o=document.createElement("option"); o.value=cs; o.textContent="CS: "+cs; sel.appendChild(o); });
}
function wire(){
  ["q-a1","cs-a1","st-a1"].forEach(function(id){ var e=document.getElementById(id);
    e.addEventListener(id.indexOf("q-")===0?"input":"change",renderA1); });
  ["q-a2","st-a2"].forEach(function(id){ var e=document.getElementById(id);
    e.addEventListener(id.indexOf("q-")===0?"input":"change",renderA2); });
  document.getElementById("q-a3").addEventListener("input",renderA3);
  document.querySelectorAll("#cs-a3 button").forEach(function(b){ b.addEventListener("click",function(){
    document.querySelectorAll("#cs-a3 button").forEach(function(x){x.classList.remove("active");}); b.classList.add("active");
    renderA3(); cardsA3(); }); });
  var sy=document.getElementById("sync");
  if(sy) sy.addEventListener("click",function(){ if(estadoSync==="auth") pedirSenha(); });
  fixSticky(); window.addEventListener("resize",fixSticky);
}
/* o cabeçalho é sticky; o <th> precisa colar logo abaixo dele, não por cima */
function fixSticky(){
  var t=document.querySelector(".top");
  if(t) document.documentElement.style.setProperty("--topH",t.offsetHeight+"px");
}
var booted=false;
window.__cs2_boot=function(){
  if(booted) return; booted=true;
  // (sem "Atualizado em" no topo: as edições são manuais e a aba Tino tem o
  //  próprio indicador de quando foi buscada)
  buildObsIndex(); fillCS1(); wire();
  loadOverlays().then(function(){
    renderA1(); renderA2(); renderA3();
    cardsA1(); cardsA2(); cardsA3(); pills();
    bootCallModal(); startPoll(); startTino();
  });
};
})();
