// Acelerador Qlik para Databricks · frontend sem build (ES module)

const $ = (s, el = document) => el.querySelector(s);
const esc = (v) => (v === null || v === undefined ? "" : String(v)).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmt = (n, d = 0) => (n === null || n === undefined || n === "" ? "" : Number(n).toLocaleString("pt-BR", { maximumFractionDigits: d, minimumFractionDigits: 0 }));
const dataHora = (v) => { if (!v) return ""; const d = typeof v === "number" ? new Date(v) : new Date(String(v).replace(" ", "T") + (String(v).includes("Z") || String(v).includes("+") ? "" : "Z")); return isNaN(d) ? esc(v) : d.toLocaleString("pt-BR"); };

let CTX = null;
let timer = null;

async function api(caminho, opts = {}) {
  const r = await fetch(caminho, { headers: opts.body && !(opts.body instanceof FormData) ? { "Content-Type": "application/json" } : {}, ...opts });
  const corpo = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(corpo.erro || corpo.detail || `Erro ${r.status}`);
  return corpo;
}
function avisar(msg, erro = false) {
  const a = $("#aviso"); a.textContent = msg; a.className = "aviso" + (erro ? " erro" : "");
  clearTimeout(a._t); a._t = setTimeout(() => a.classList.add("oculto"), 5000);
}

const CLASSE_STATUS = {
  ok: "s-ok", igual: "s-ok", SUCCESS: "s-ok", auto_aprovado: "s-ok", aprovado: "s-ok", publicado: "s-ok", paridade_ok: "s-ok",
  convertida: "s-ok", convertido: "s-ok", linhagem_ok: "s-ok", triado: "s-info", migrar: "s-info", aprovar: "s-ok", info: "s-info",
  revisao_humana: "s-aviso", convertida_com_ressalva: "s-aviso", aguardando_publicacao: "s-neutro", aviso: "s-aviso", eleger_oficial: "s-aviso", revisar: "s-aviso",
  RUNNING: "s-info", PENDING: "s-info", QUEUED: "s-info",
  bloqueado: "s-erro", erro: "s-erro", FAILED: "s-erro", rejeitado: "s-erro", rejeitar: "s-erro", divergente: "s-erro",
  paridade_divergente: "s-erro", falhou: "s-erro", nao_convertida: "s-erro", descartar: "s-neutro", descartado: "s-neutro",
  arquivar_historico: "s-neutro", arquivado: "s-neutro",
};
const ROTULO = {
  auto_aprovado: "aprovado pela nota", revisao_humana: "aguarda revisão", bloqueado: "bloqueado", aprovado: "aprovado por pessoa",
  rejeitado: "rejeitado", publicado: "publicado", paridade_ok: "paridade ok", paridade_divergente: "paridade divergente",
  arquivar_historico: "arquivar", eleger_oficial: "eleger oficial", linhagem_ok: "linhagem ok", aguardando_publicacao: "aguarda publicação",
};
const selo = (s) => s === null || s === undefined ? "" : `<span class="selo ${CLASSE_STATUS[s] || "s-neutro"}">${esc(ROTULO[s] || s)}</span>`;
const nota = (n) => { if (n === null || n === undefined) return ""; const c = n >= 85 ? "var(--verde)" : n >= 60 ? "var(--ambar)" : "var(--rosa)"; return `<span class="nota"><span class="barra"><span style="width:${n}%;background:${c}"></span></span><b>${fmt(n)}</b></span>`; };

function tabela(linhas, colunas, opts = {}) {
  if (!linhas || !linhas.length) return `<div class="vazio">${opts.vazio || "Nada por aqui ainda. Execute o passo correspondente."}</div>`;
  const cab = colunas.map((c) => `<th class="${c.num ? "num" : ""}">${esc(c.t)}</th>`).join("");
  const corpo = linhas.map((l) => `<tr ${opts.clique ? `class="clicavel" data-k="${esc(l[opts.chave])}"` : ""}>` +
    colunas.map((c) => `<td class="${c.num ? "num" : ""}">${c.f ? c.f(l[c.k], l) : esc(Array.isArray(l[c.k]) ? l[c.k].join(", ") : l[c.k])}</td>`).join("") + "</tr>").join("");
  return `<div style="overflow-x:auto"><table><thead><tr>${cab}</tr></thead><tbody>${corpo}</tbody></table></div>`;
}
const explica = (rot, html, cor = "") => `<div class="explica ${cor}"><div class="rot">${rot}</div>${html}</div>`;
const cartao = (tit, html, sub = "") => `<div class="cartao"><h3>${tit}${sub ? `<span class="sub">${sub}</span>` : ""}</h3>${html}</div>`;
const codigo = (txt) => `<div class="codigo">${(txt || "").split("\n").map((l, i) => `<span class="ln" data-n="${i + 1}">${esc(l) || " "}</span>`).join("")}</div>`;

// ------------------------------------------------------------------ execução em andamento (vale para todas as telas)
let EXEC = null;            // {rodando, na_fila, ativas}
let EXEC_ANTERIOR = null;   // run_id que estava ativo na última consulta
let aoMudarExecucao = null; // a tela Executar se inscreve aqui para redesenhar o painel
const NOME_PASSO = (p) => (DESC_PASSO[p] || [p])[0];
const duracao = (ms) => { const s = Math.max(0, Math.round(ms / 1000)); return s < 60 ? `${s}s` : `${Math.floor(s / 60)}min ${String(s % 60).padStart(2, "0")}s`; };

async function vigiarExecucao() {
  try { EXEC = await api("/api/execucao_ativa"); } catch (e) { return; }
  const r = EXEC.rodando, ind = $("#indicador");
  if (r) {
    const fila = EXEC.na_fila.filter((x) => x.run_id !== r.run_id).length;
    ind.innerHTML = `<span class="pulso"></span>${r.atual ? `Executando ${esc(NOME_PASSO(r.atual))}` : "Execução na fila"} · ${r.concluidas}/${r.total} · ${duracao(Date.now() - r.inicio)}${fila ? ` · +${fila} na fila` : ""}`;
    ind.classList.remove("oculto");
    EXEC_ANTERIOR = r.run_id;
  } else {
    ind.classList.add("oculto");
    if (EXEC_ANTERIOR) {
      const fim = EXEC_ANTERIOR; EXEC_ANTERIOR = null;
      api("/api/execucoes").then((runs) => {
        const x = runs.find((y) => y.run_id === fim);
        const ok = x && x.estado === "SUCCESS";
        avisar(ok ? `Execução ${fim} terminou com sucesso. Confira os resultados nas telas do fluxo.` : `Execução ${fim} terminou: ${x ? x.estado : "concluída"}. Veja os detalhes na tela Executar.`, !ok);
      }).catch(() => {});
      const pagina = (location.hash.slice(1) || "inicio").split("/")[0];
      if (!["entrada", "config"].includes(pagina) && !location.hash.includes("/")) rota();   // não apaga o que estiver sendo digitado
    }
  }
  if (aoMudarExecucao) aoMudarExecucao();
}

// ------------------------------------------------------------------ páginas
const PAGINAS = [
  { g: "COMEÇAR" },
  { id: "comousar", t: "Como usar", s: "Passo a passo do acelerador, do primeiro arquivo à paridade", f: pgComoUsar },
  { id: "inicio", t: "Visão geral", s: "Onde a migração está e o que fazer agora", f: pgInicio },
  { id: "prereqs", t: "Pré-requisitos", s: "O ambiente está pronto para o acelerador?", f: pgPrereqs },
  { id: "dicionario", t: "Qlik → Databricks", s: "Dicionário de equivalências para quem vem do Qlik", f: pgDicionario },
  { g: "MIGRAR" },
  { id: "entrada", n: 1, t: "Entrada de dados", s: "Modo, origens e onde estão os arquivos do Qlik (QVDs, scripts, medidas)", f: pgEntrada },
  { id: "executar", n: 2, t: "Executar", s: "Dispare o fluxo completo ou só alguns passos", f: pgExecutar },
  { id: "triagem", n: 3, t: "Triagem", s: "O que migrar, eleger, revisar, arquivar ou descartar", f: pgTriagem },
  { id: "linhagem", n: 4, t: "Linhagem", s: "Da métrica do app até a coluna de origem", f: pgLinhagem },
  { id: "conversao", n: 5, t: "Revisar conversão", s: "Código gerado pela IA, nota de confiança e aprovação", f: pgConversao, badge: "pendentes" },
  { id: "publicacao", n: 6, t: "Publicar", s: "Pipeline, tabelas geradas e bundle para dev, hml e prd", f: pgPublicacao },
  { id: "semantica", n: 7, t: "Métricas e segurança", s: "Metric views, glossário e Section Access", f: pgSemantica },
  { id: "paridade", n: 8, t: "Paridade", s: "Os números batem com o Qlik?", f: pgParidade },
  { g: "AJUSTES" },
  { id: "config", t: "Ajustes avançados", s: "Limites e regras que já vêm preenchidos; mude só se precisar", f: (el) => pgConfig(el, null, "avancado") },
  { g: "ACOMPANHAR" },
  { id: "mapa", t: "Mapa dos dados", s: "Onde está cada camada no Unity Catalog: entrada, bronze, silver, gold, metric views e controle", f: pgMapa },
  { id: "unidades", t: "Estado das unidades", s: "Cada tabela, objeto e medida, e onde está no fluxo", f: pgUnidades },
  { id: "diario", t: "Diário", s: "Tudo o que aconteceu, por quem e quando", f: pgDiario },
];
let PENDENTES = 0;

function montarMenu(atual) {
  $("#nav").innerHTML = PAGINAS.map((p) => p.g ? `<div class="grupo">${p.g}</div>` :
    `<a href="#${p.id}" class="${p.id === atual ? "ativo" : ""}">${p.n ? `<span class="num">${p.n}</span>` : `<span class="num">·</span>`}${p.t}${p.badge && PENDENTES ? `<span class="badge-menu">${PENDENTES}</span>` : ""}</a>`).join("");
}

async function rota() {
  clearInterval(timer);
  aoMudarExecucao = null;
  let [id, arg] = (location.hash.slice(1) || "inicio").split("/");
  if (id === "coleta") id = "entrada";
  const p = PAGINAS.find((x) => x.id === id) || PAGINAS.find((x) => x.id === "inicio");
  montarMenu(p.id);
  $("#titulo-pagina").innerHTML = `${p.n ? `Passo ${p.n} · ` : ""}${p.t}<small>${p.s}</small>`;
  // cada navegação desenha num contêiner novo: uma renderização atrasada escreve num nó que já saiu da página
  const alvo = document.createElement("div");
  alvo.innerHTML = `<div class="vazio">Carregando…</div>`;
  $("#conteudo").replaceChildren(alvo);
  try { await p.f(alvo, arg ? decodeURIComponent(arg) : null); }
  catch (e) { if (alvo.isConnected) alvo.innerHTML = explica("ERRO", esc(e.message), "laranja"); }
}

// ------------------------------------------------------------------ entradas (usadas em Como usar e Coleta)
const ENTRADAS = [
  { k: "inventario", n: "Inventário de QVDs", f: "inventario*.csv", ob: "Sim", usa: "Triagem (passo 03)",
    o: "Lista de todos os arquivos QVD do ambiente Qlik, só com metadados.",
    como: "Exportação da lista de arquivos de dados do Qlik (Qlik Cloud ou QMC) ou listagem das pastas de QVD. Colunas: <code>nome, espaco, linhas, tamanho_bytes, atualizado_em</code>." },
  { k: "scripts", n: "Load scripts", f: "*.qvs", ob: "Sim", usa: "Linhagem e conversão (04 e 05)",
    o: "Um arquivo por app ou QVW, com o script de carga completo (extração, transformação e app).",
    como: "Copiar do editor de carga de dados de cada app (ou do LoadScript do QVW) e salvar como <code>NomeDoApp.qvs</code>. O nome do arquivo identifica o script." },
  { k: "qvd", n: "QVDs ou só os cabeçalhos", f: "*.qvd", ob: "Sim", usa: "Linhagem, contagem e paridade (04, 05 e 08)",
    o: "O cabeçalho XML de cada QVD gerado pelos scripts: campos, número de linhas e de onde veio.",
    como: "Basta o cabeçalho (início do arquivo, sem dados) para linhagem e contagem. O QVD completo é necessário só para a paridade linha a linha (N1 a N3)." },
  { k: "apps", n: "Medidas mestras dos apps", f: "NomeDoApp_medidas.json", ob: "Recomendado", usa: "Metric views e paridade N4 (07 e 08)",
    o: "As medidas mestras de cada app (título, expressão, descrição) e a lista de tabelas exportadas para conferência.",
    como: "Formato: <code>{\"app\": \"NomeDoApp\", \"medidas_mestras\": [{\"qId\", \"title\", \"expression\", \"description\"}], \"hypercubes_exportados\": [{\"arquivo\", \"dimensoes\", \"medidas\"}]}</code>. O valor de <code>app</code> deve ser igual ao nome do .qvs do app." },
  { k: "apps", n: "Tabelas exportadas dos apps", f: "hypercube*.csv", ob: "Recomendado", usa: "Paridade N4 (08)",
    o: "Uma tabela simples exportada do app com dimensões e medidas: é o número que o negócio vê hoje.",
    como: "No app Qlik, montar uma tabela com as dimensões e medidas listadas em <code>hypercubes_exportados</code> e exportar como CSV (separador <code>;</code>, decimal <code>,</code>)." },
];
const SEM_ARQUIVO = [
  ["Section Access", "Vem dentro do load script do app. Nada a enviar."],
  ["Tabelas de origem (Oracle, SQL Server…)", "Não são enviadas como arquivo: chegam ao Lakehouse pelo Lakeflow Connect, e o schema é informado em Configurar."],
];

function tabelaEntradas(pastas) {
  return `<table><thead><tr><th>Entrada</th><th>Arquivo</th><th>Obrigatório</th><th>Para quê</th><th>O que é e como obter</th><th>Pasta no volume</th></tr></thead><tbody>
    ${ENTRADAS.map((e) => `<tr><td><b>${e.n}</b></td><td><code>${e.f}</code></td><td>${e.ob}</td><td>${e.usa}</td><td>${e.o}<br><span style="color:var(--cinza-2);font-size:12.5px">${e.como}</span></td><td><code style="font-size:11.5px">${esc((pastas || {})[e.k] || "")}</code></td></tr>`).join("")}
    ${SEM_ARQUIVO.map(([n, d]) => `<tr><td><b>${n}</b></td><td>–</td><td>–</td><td>–</td><td>${d}</td><td>–</td></tr>`).join("")}
  </tbody></table>`;
}

// ------------------------------------------------------------------ como usar
function pgComoUsar(el) {
  const passo = (n, t, faz, entra, sai, link) => `<div class="cartao" style="display:flex;gap:18px">
      <div style="flex-shrink:0;width:38px;height:38px;border-radius:50%;background:var(--navy);color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700">${n}</div>
      <div style="flex:1"><h3 style="margin-bottom:6px">${t}</h3><div style="line-height:1.6">${faz}</div>
        <div class="duas" style="margin-top:10px;gap:12px">
          <div style="background:var(--fundo);border-radius:8px;padding:10px 12px;font-size:13px"><b style="color:var(--teal)">Você fornece</b><br>${entra}</div>
          <div style="background:var(--fundo);border-radius:8px;padding:10px 12px;font-size:13px"><b style="color:var(--teal)">Você recebe</b><br>${sai}</div></div>
        ${link ? `<div style="margin-top:10px"><a class="botao sec" href="${link[0]}">${link[1]}</a></div>` : ""}</div></div>`;
  el.innerHTML = explica("EM UMA FRASE", `Você envia o que foi exportado do Qlik, o acelerador descobre o que precisa ser migrado, gera o código Databricks com IA, você aprova o que a IA não teve certeza, e no fim cada tabela e cada métrica é comparada com o número do Qlik. Quase tudo já vem configurado: no ambiente do cliente, o que precisa ser informado é <b>o modo</b>, <b>onde estão as tabelas de origem</b> e, se for o caso, <b>o volume onde já estão os arquivos</b>.`) +
    passo(0, "Conferir os pré-requisitos (uma vez)", "Abra <a href=\"#prereqs\">Pré-requisitos</a>. Se algum item estiver vermelho, a tela mostra o comando para o administrador do workspace resolver.", "Nada.", "Confirmação de que warehouse, catálogo, volume, modelo de IA e job estão acessíveis.", ["#prereqs", "Abrir Pré-requisitos"]) +
    passo(1, "Entrada de dados", `Numa tela só: escolha o <b>modo</b> (<i>Demonstração</i> ou <i>Ambiente do cliente</i>), diga onde estão as <b>tabelas de origem</b> no Lakehouse e <b>indique o volume</b> onde estão os QVDs, scripts e medidas, ou envie esses arquivos pelo próprio app. Dê um nome à <b>onda</b>. A tela confere o pacote e mostra o que falta. No modo demonstração, nada precisa ser preenchido.`, "Modo, schema das origens, volume dos arquivos (ou o .zip), nome da onda. Arquivos: inventário (.csv), load scripts (.qvs), QVDs ou cabeçalhos (.qvd), medidas dos apps (.json) e tabelas exportadas (.csv).", "Configuração salva e checagem do pacote: o que está ok e o que falta.", ["#entrada", "Abrir Entrada de dados"]) +
    passo(2, "Executar o fluxo", "Clique em <b>Executar selecionados</b> com todos os passos marcados. Na amostra, leva cerca de 12 minutos. A tela atualiza sozinha.", "Nada além do que já foi enviado.", "Inventário triado, linhagem, código gerado, pipeline do que foi aprovado automaticamente, metric views e paridade.", ["#executar", "Abrir Executar"]) +
    passo(3, "Conferir a triagem", "Veja quantas tabelas lógicas entram no escopo e por quê. Se um limite não fizer sentido para o cliente (por exemplo, quantos dias sem atualizar é \"parado\"), ajuste em Ajustes avançados → Triagem e rode só o passo 03.", "Opcional: ajustes nos limites.", "Lista do que migrar, eleger, revisar, arquivar ou descartar.", ["#triagem", "Abrir Triagem"]) +
    passo(4, "Conferir a linhagem", "Cada medida do app é rastreada até a coluna da tabela de origem. É assim que se define o escopo mínimo: só entra o que sustenta as métricas em uso.", "Nada.", "Grafo, origens de cada métrica e alertas de semântica Qlik.", ["#linhagem", "Abrir Linhagem"]) +
    passo(5, "Revisar o código gerado", "Objetos com nota abaixo de 85, ou que falharam na validação, esperam você. Compare o script Qlik com o SQL gerado lado a lado. <b>Aprovar</b> libera para publicação. <b>Rejeitar</b> exige uma orientação, que volta para a IA quando o passo 05 rodar de novo.", "Sua decisão e, ao rejeitar, a orientação.", "Objetos liberados; histórico de quem decidiu.", ["#conversao", "Abrir Revisão"]) +
    passo(6, "Publicar", "Rode os passos 06 a 08 (botão <b>Só 05 a 08</b> na tela Executar). A tela Publicar traz o <b>link do pipeline gerado</b>, de cada tabela no Catalog Explorer e o bundle para levar o mesmo código a dev, hml e prd.", "Nada.", "Pipeline no workspace (com link), tabelas silver e gold, bundle (.zip) e glossário para as Pages do Unity Catalog.", ["#publicacao", "Abrir Publicar"]) +
    passo(7, "Validar métricas, segurança e paridade", "Confira se as medidas viraram metric views, se cada perfil do Section Access vê o mesmo que via no Qlik, e se N1 a N4 estão verdes. Com tudo verde, o QVD correspondente pode ser desligado no Qlik.", "Nada.", "Semáforo por objeto e histórico da paridade.", ["#paridade", "Abrir Paridade"]) +
    cartao("Quais arquivos enviar e onde ficam", tabelaEntradas(CTX.entrada)) +
    cartao("Palavras que aparecem no app", `<table class="dic"><tbody>
      <tr><td>Modo</td><td><b>Demonstração</b>: gera um ERP e QVDs fictícios e usa uma amostra pronta, para conhecer o fluxo sem dado do cliente. <b>Ambiente do cliente</b>: usa os arquivos enviados e as tabelas de origem do Lakehouse.</td></tr>
      <tr><td>Onda</td><td>O lote da migração que está sendo feito agora, normalmente um app ou um domínio de negócio (ex.: <code>onda_1_custo_assistencial</code>). O nome vai como tag em tudo que o acelerador cria, para acompanhar andamento e custo por lote. Para começar o próximo lote, troque o nome e envie os scripts daquele lote. O inventário vale para o ambiente inteiro.</td></tr>
      <tr><td>Tabela lógica</td><td>Um conjunto de QVDs que são a mesma tabela fatiada por período (ex.: <code>BI_CONTAS_202401.qvd</code> a <code>BI_CONTAS_202412.qvd</code>). No Lakehouse, vira uma única tabela com coluna de período.</td></tr>
      <tr><td>Unidade de migração</td><td>Qualquer coisa cujo andamento é acompanhado: tabela lógica, QVD, script, objeto gerado, medida, perfil de acesso. Cada uma tem um estado em <a href="#unidades">Estado das unidades</a>.</td></tr>
      <tr><td>Nota de confiança</td><td>De 0 a 100, calculada para cada objeto gerado pela IA: começa em 100 e perde pontos por rodada de correção, alerta de semântica Qlik, premissa assumida pela IA e violação de qualidade. A partir de 85, segue sem revisão humana.</td></tr>
      <tr><td>Paridade</td><td>Comparação com o Qlik em quatro níveis: N1 contagem, N2 somas, N3 linha a linha, N4 métricas do app.</td></tr>
    </tbody></table>`);
}

// ------------------------------------------------------------------ início
async function pgInicio(el) {
  const v = await api("/api/visao");
  PENDENTES = v.pendentes || 0; montarMenu("inicio");
  const conta = (tipo, estados) => v.estados.filter((e) => e.tipo === tipo && (!estados || estados.includes(e.estado))).reduce((a, b) => a + b.n, 0);
  const tl = conta("tabela_logica"), tlm = conta("tabela_logica", ["triado"]);
  const obj = conta("objeto"), pub = conta("objeto", ["publicado", "paridade_ok", "paridade_divergente"]), parok = conta("objeto", ["paridade_ok"]);
  const testes = v.paridade.reduce((a, b) => a + b.total, 0), testesok = v.paridade.reduce((a, b) => a + b.ok, 0);

  let prox;
  if (!v.estados.length) prox = { t: "Rodar o fluxo pela primeira vez", d: "Confira os pré-requisitos, envie os arquivos e execute o fluxo completo.", h: "#prereqs", b: "Ver pré-requisitos" };
  else if (PENDENTES) prox = { t: `Revisar ${PENDENTES} objeto(s) gerado(s) pela IA`, d: "Eles não atingiram a nota mínima ou falharam na validação. Aprove ou rejeite com orientação.", h: "#conversao", b: "Abrir revisão" };
  else if (testes && testesok < testes) prox = { t: `Investigar ${testes - testesok} teste(s) de paridade divergente(s)`, d: "Veja onde os números diferem do Qlik antes de liberar o objeto.", h: "#paridade", b: "Abrir paridade" };
  else if (pub) prox = { t: "Baixar o bundle e promover para dev", d: "Tudo validado. O bundle leva o mesmo código para dev, hml e prd.", h: "#publicacao", b: "Abrir publicação" };
  else prox = { t: "Publicar os objetos aprovados", d: "Execute os passos 06 a 08 para gerar o pipeline, as metric views e a paridade.", h: "#executar", b: "Executar" };

  el.innerHTML = `
    <div class="proximo"><div><div class="rot">PRÓXIMO PASSO</div><div class="t">${prox.t}</div><div class="d">${prox.d}</div></div><a class="botao" href="${prox.h}">${prox.b}</a></div>
    <div class="kpis">
      <div class="kpi"><div class="v">${fmt(tl)}</div><div class="r">tabelas lógicas no inventário</div></div>
      <div class="kpi"><div class="v">${fmt(tlm)}</div><div class="r">no escopo de migração</div></div>
      <div class="kpi"><div class="v">${fmt(obj)}</div><div class="r">objetos gerados pela IA</div></div>
      <div class="kpi ${PENDENTES ? "alerta" : ""}"><div class="v">${fmt(PENDENTES)}</div><div class="r">aguardando revisão humana</div></div>
      <div class="kpi"><div class="v">${testes ? fmt(testesok) + "/" + fmt(testes) : "–"}</div><div class="r">testes de paridade ok</div></div>
      <div class="kpi"><div class="v">${v.tokens ? fmt((v.tokens.entrada + v.tokens.saida) / 1000, 1) + " mil" : "–"}</div><div class="r">tokens de IA${v.tokens ? ` · ${fmt(v.tokens.chamadas)} chamadas, ${fmt(v.tokens.do_cache)} do cache` : ""}</div></div>
    </div>
    ${cartao("Funil da migração", `<div class="funil">
      ${[["inventário", tl], ["no escopo", tlm], ["convertidos", obj], ["publicados", pub], ["paridade ok", parok]].map(([r, n]) => `<div class="etapa"><div class="v">${fmt(n)}</div><div class="r">${r}</div></div>`).join("")}
    </div>`)}
    <div class="duas">
      ${cartao("Última execução", v.ultima_execucao.length ? `<div class="linha-tempo">${v.ultima_execucao.map((p) => `<span class="chip ${p.status}">${esc(p.passo)} · ${fmt(p.segundos)}s</span>`).join("")}</div>
        <p style="font-size:12px;color:var(--cinza-2)">por ${esc(v.ultima_execucao[0].usuario)} · ${dataHora(v.ultima_execucao.at(-1).fim)}</p>` : `<div class="vazio">Nenhuma execução ainda.</div>`)}
      ${cartao("Revisão do código gerado", tabela(v.revisao, [{ t: "Situação", k: "status_revisao", f: selo }, { t: "Objetos", k: "n", num: 1 }, { t: "Nota média", k: "nota_media", num: 1 }]))}
    </div>
    ${cartao("Triagem do inventário", tabela(v.triagem, [{ t: "Decisão", k: "decisao", f: selo }, { t: "Tabelas lógicas", k: "unidades", num: 1, f: (x) => fmt(x) }, { t: "GB", k: "gb", num: 1, f: (x) => fmt(x, 2) }]))}
    ${explica("COMO O ACELERADOR PENSA", `<b>Planeja de trás para frente:</b> parte das métricas que o negócio usa no Qlik e rastreia cada uma até a coluna de origem, para migrar só o que sustenta essas métricas.
      <b>Constrói de frente para trás:</b> bronze, silver, gold e metric view, porque não existe métrica sem origem.
      <b>Fecha com paridade:</b> cada objeto é comparado com o Qlik em quatro níveis antes de ser liberado.`)}`;
}

// ------------------------------------------------------------------ pré-requisitos
async function pgPrereqs(el) {
  const c = await api("/api/prereqs");
  const ok = c.filter((x) => x.ok).length;
  el.innerHTML = `
    ${explica("O QUE É CONFERIDO", "O app usa a própria identidade (service principal) para ler e gravar as tabelas de controle, enviar arquivos ao volume e disparar o job. Se algum item falhar, a coluna da direita traz o comando ou a ação para o administrador do workspace.")}
    ${cartao(`${ok} de ${c.length} itens prontos`, c.map((x) => `<div class="check"><span class="ic ${x.ok ? "ok" : "no"}">${x.ok ? "✓" : "!"}</span>
      <div style="flex:1"><b>${esc(x.nome)}</b><div class="d">${esc(x.detalhe)}</div></div>
      ${x.ok ? "" : `<div style="flex:1;font-size:12.5px"><code>${esc(x.como_resolver)}</code></div>`}</div>`).join(""))}
    <div class="acoes"><button class="sec" id="rever">Conferir de novo</button><a class="botao" href="#coleta">Seguir para a coleta</a></div>`;
  $("#rever", el).onclick = () => rota();
}

// ------------------------------------------------------------------ dicionário
function pgDicionario(el) {
  const L = [
    ["QVD", "Tabela Delta", "Formato aberto, versionado (time travel), com estatísticas e governança no Unity Catalog."],
    ["Script de extract (SQL SELECT + STORE)", "Lakeflow Connect", "Conector gerenciado que traz a tabela de origem para o bronze. A lógica incremental sai do script."],
    ["Script de transformação (LOAD RESIDENT, JOIN, STORE)", "Materialized view em pipeline declarativo", "Você declara o resultado; a plataforma resolve a ordem, o incremental e a qualidade."],
    ["RESIDENT", "CTE (WITH ...)", "Etapa intermediária que não precisa virar tabela física."],
    ["ApplyMap", "LEFT JOIN em tabela de referência + COALESCE", "O default do ApplyMap vira o valor do COALESCE."],
    ["Exists()", "Semi-join (IN / EXISTS)", "Depende da ordem de carga no Qlik; o acelerador trata isso explicitamente."],
    ["Concatenate", "UNION ALL com colunas explícitas", "Campo ausente de um lado entra como nulo tipado."],
    ["Medida mestra / Set Analysis", "Metric view (Unity Catalog)", "Métrica governada, usada por AI/BI, Genie, Power BI e SQL com a mesma definição."],
    ["Section Access", "Row filter + tabela de acesso", "A regra vale para qualquer ferramenta, não só para o app."],
    ["Espaço / stream", "Catálogo e schema", "Organização e permissão por domínio."],
    ["Tarefa de recarga agendada", "Job (Lakeflow Jobs)", "Agendamento, dependências, alertas e histórico."],
    ["Monitoring apps / QMC", "System tables + tabelas de controle", "Uso, custo, execução e auditoria consultáveis em SQL."],
    ["Recarga parcial / incremental com Peek", "Materialized view incremental ou streaming table", "A plataforma decide o incremental sem controle manual de última carga."],
  ];
  el.innerHTML = explica("POR QUE ISSO IMPORTA", "No Qlik, ingestão, transformação, modelo e regra de negócio vivem dentro do mesmo script. No Databricks, cada responsabilidade tem um lugar próprio, o que facilita governar, testar e reaproveitar. O acelerador faz essa separação automaticamente.") +
    cartao("Equivalências", `<table class="dic"><thead><tr><th>No Qlik</th><th>No Databricks</th><th>O que muda</th></tr></thead><tbody>${L.map((l) => `<tr><td>${l[0]}</td><td>${l[1]}</td><td>${l[2]}</td></tr>`).join("")}</tbody></table>`);
}

// ------------------------------------------------------------------ configuração
const DESCR = {
  "ambiente.catalogo": "Catálogo do Unity Catalog onde o acelerador trabalha. Alterado pelo parâmetro do job.",
  "ambiente.prefixo_schema": "Prefixo dos schemas criados (ctrl, bronze, silver, gold, sandbox). Alterado pelo parâmetro do job.",
  "ambiente.schema_bronze": "catálogo.schema onde o Lakeflow Connect grava as tabelas de origem. Vazio = usa o bronze do acelerador.",
  "ambiente.modo": "demo gera dados fictícios; cliente usa os arquivos enviados na coleta.",
  "ambiente.onda": "Rótulo da onda de migração. Vai para as tags e permite medir custo e andamento por onda.",
  "ambiente.dono": "Responsável técnico; recebe alertas de falha.",
  "ambiente.tags": "Tags aplicadas a schemas, volume e tabelas (JSON).",
  "ia.endpoint": "Modelo usado na conversão. Deve estar disponível em Serving.",
  "ia.max_tokens": "Limite de resposta por chamada.",
  "ia.tentativas": "Quantas vezes o modelo pode corrigir o próprio código depois de uma validação falhar.",
  "ia.usar_cache": "Reaproveita a resposta quando script e contexto não mudaram. Economiza tempo e tokens.",
  "ia.experimento_mlflow": "Onde os traces das chamadas ao modelo ficam. Vazio = pasta do dono.",
  "triagem.dias_ativo": "QVD atualizado até este número de dias é considerado ativo.",
  "triagem.dias_recente": "Até este número de dias, recente.",
  "triagem.dias_parado": "Acima disto, o QVD é arquivado como histórico em vez de migrado.",
  "triagem.espacos_descartar": "Espaços do Qlik descartados (pessoal, teste).",
  "triagem.termo_teste": "Nome de QVD com este termo é tratado como teste.",
  "triagem.prefixos_gold": "Prefixos de nome que indicam modelo (fato, dimensão).",
  "triagem.prefixos_bronze": "Prefixos de nome que indicam extração bruta.",
  "conversao.prefixos_qvd_gold": "QVD de transformação com estes prefixos vira gold; os demais, silver.",
  "conversao.prefixo_tabela_bronze": "Prefixo das tabelas de origem no bronze.",
  "conversao.prefixos_tabela_origem_remover": "Prefixos removidos do nome da tabela de origem (ex.: TB_).",
  "conversao.prefixos_chave_primaria": "Prefixos de campo candidatos a chave primária.",
  "conversao.colunas_cursor": "Colunas de data usadas como cursor de carga incremental.",
  "revisao.nota_minima_auto": "Conversão validada com nota igual ou maior segue sem revisão humana. Para a primeira onda, um valor alto (ex.: 101) força revisão de tudo.",
  "paridade.tolerancia_relativa": "Diferença relativa aceita nas somas (0,0001 = 0,01%).",
  "paridade.tolerancia_absoluta": "Diferença absoluta aceita nas somas.",
  "paridade.colunas_periodo": "Colunas usadas como período nos testes N1 e N2.",
  "paridade.dimensoes_n2": "Dimensões usadas para quebrar as somas no N2.",
  "paridade.prefixos_numericos": "Prefixos das colunas somadas no N2.",
  "seguranca.grupo_admin": "Grupo que enxerga tudo no row filter.",
  "seguranca.prefixo_grupo_idp": "Prefixo usado para derivar o grupo do IdP a partir do usuário Qlik.",
  "seguranca.tabela_alvo": "Tabela gold que recebe o row filter.",
  "seguranca.coluna_filtro": "Coluna filtrada pela regra de acesso.",
  "seguranca.coluna_valor_teste": "Coluna somada no teste por perfil.",
  "publicacao.nome_bundle": "Nome do bundle e do pipeline gerados.",
  "publicacao.agendamento_cron": "Agendamento do job D-1 gerado (Quartz).",
  "publicacao.fuso": "Fuso do agendamento.",
  "publicacao.ambientes": "catálogo.schema de bronze, silver e gold por ambiente (JSON).",
  "entrada.pasta_inventario": "Subpasta do volume com o inventário de QVDs.",
  "entrada.pasta_scripts": "Subpasta com os load scripts (.qvs).",
  "entrada.pasta_apps": "Subpasta com medidas mestras (.json) e tabelas exportadas dos apps (.csv).",
  "entrada.pasta_qvd": "Subpasta com os QVDs ou só os cabeçalhos.",
};
const GRUPOS = { ambiente: "Ambiente", ia: "Inteligência artificial", triagem: "Triagem do inventário", conversao: "Conversão", revisao: "Revisão humana", paridade: "Paridade", seguranca: "Segurança", publicacao: "Publicação" };

async function pgEntrada(el) {
  el.innerHTML = `<div id="ent-a"></div><div id="ent-b"></div>`;
  const a = $("#ent-a", el), b = $("#ent-b", el);
  await pgConfig(a, null, "essencial");
  await pgColeta(b);
}

async function pgConfig(el, _arg, parte = "avancado") {
  const c = await api("/api/config");
  if (!c.efetiva) { el.innerHTML = explica("CONFIGURAÇÃO AINDA NÃO GERADA", "Execute o passo 01 (tela Executar). Ele cria o ambiente e grava a configuração padrão, que passa a ser editável aqui.", "laranja"); return; }
  const cfg = JSON.parse(JSON.stringify(c.efetiva));
  cfg.entrada = cfg.entrada || { volume: null };
  const amb = cfg.ambiente, ent = cfg.entrada;
  const volPadrao = CTX.volume;
  const pastas = () => {
    const base = ($("#ent-volume", el)?.value || "").trim().replace(/\/$/, "") || volPadrao;
    return { inventario: `${base}/${ent.pasta_inventario || "inventario"}`, scripts: `${base}/${ent.pasta_scripts || "scripts"}`,
             apps: `${base}/${ent.pasta_apps || "apps"}`, qvd: `${base}/${ent.pasta_qvd || "qvd"}` };
  };
  const opcao = (nome, valor, atual, titulo, desc) => `<label class="passo ${valor === atual ? "sel" : ""}" style="cursor:pointer"><input type="radio" name="${nome}" value="${valor}" ${valor === atual ? "checked" : ""}>
      <div class="t">${titulo}</div><div class="d">${desc}</div></label>`;
  const origemNoLake = !!amb.schema_bronze || amb.modo === "demo";

  const campoAvancado = (g, k, v) => {
    const id = `${g}.${k}`;
    let inp;
    if (typeof v === "boolean") inp = `<select data-id="${id}" data-t="bool"><option value="true" ${v ? "selected" : ""}>sim</option><option value="false" ${!v ? "selected" : ""}>não</option></select>`;
    else if (typeof v === "number") inp = `<input type="number" step="any" data-id="${id}" data-t="num" value="${v}">`;
    else if (Array.isArray(v)) inp = `<input type="text" data-id="${id}" data-t="lista" value="${esc(v.join(", "))}">`;
    else if (v && typeof v === "object") inp = `<textarea data-id="${id}" data-t="json">${esc(JSON.stringify(v, null, 2))}</textarea>`;
    else inp = `<input type="text" data-id="${id}" data-t="txt" value="${esc(v ?? "")}">`;
    return `<label class="campo"><div class="n">${k}</div><div class="d">${esc(DESCR[id] || "")}</div>${inp}</label>`;
  };
  const AVANCADOS = { ia: "Inteligência artificial", triagem: "Triagem do inventário", conversao: "Conversão", revisao: "Revisão humana",
                      paridade: "Paridade", seguranca: "Segurança", publicacao: "Publicação", entrada: "Nomes das pastas de entrada" };
  const avancado = (g) => Object.entries(cfg[g] || {}).filter(([k]) => !(g === "entrada" && k === "volume")).map(([k, v]) => campoAvancado(g, k, v)).join("");

  const htmlEssencial = explica("O QUE PREENCHER AQUI", `Escolha o <b>modo</b>. No modo cliente, diga também <b>onde estão as tabelas de origem</b>. A <b>pasta de entrada</b> e a <b>onda</b> já vêm preenchidas: mude só se precisar. O resto (limites de triagem, nota mínima, tolerâncias da paridade) fica em <a href="#config">Ajustes avançados</a>.`) +
    `<div class="cartao"><h3>1a. Modo e origens</h3>
      <div class="n" style="font-weight:700;color:var(--navy);margin-bottom:6px">1. Modo</div>
      <div class="duas" style="gap:12px;margin-bottom:18px">
        ${opcao("modo", "demo", amb.modo, "Demonstração", "Gera um ERP e QVDs fictícios e usa uma amostra pronta. Serve para conhecer o fluxo sem nenhum dado do cliente. O resto já vem preenchido.")}
        ${opcao("modo", "cliente", amb.modo, "Ambiente do cliente", "Usa os arquivos exportados do Qlik do cliente e as tabelas de origem que já estão no Lakehouse. A simulação (passo 02) é pulada.")}
      </div>
      <div id="bloco-cliente">
        <div class="n" style="font-weight:700;color:var(--navy);margin-bottom:6px">2. Onde estão as tabelas de origem (Oracle, SQL Server…)?</div>
        <div class="duas" style="gap:12px;margin-bottom:10px">
          ${opcao("origem", "lake", origemNoLake ? "lake" : "nao", "Já estão no Lakehouse", "Foram ingeridas pelo Lakeflow Connect (ou outra ingestão). Informe o schema abaixo. O fluxo roda completo, até a paridade.")}
          ${opcao("origem", "nao", origemNoLake ? "lake" : "nao", "Ainda não foram ingeridas", "O acelerador entrega triagem, linhagem e a especificação de ingestão de cada tabela (passos 03 a 05). A conversão completa e a paridade dependem das origens no Lakehouse.")}
        </div>
        <label class="campo" id="campo-bronze"><div class="d">catálogo.schema onde estão as tabelas de origem (ex.: <code>prd_bronze.erp</code>)</div>
          <input type="text" id="schema-bronze" value="${esc(amb.schema_bronze || "")}" placeholder="catalogo.schema"></label>
      </div>
      <h3 style="margin:22px 0 4px">1b. Arquivos do Qlik (QVDs, scripts, medidas)</h3>
      <div class="n" style="font-weight:700;color:var(--navy);margin:8px 0 6px"><span id="num-arq">3</span>. Pasta de entrada</div>
      <label class="campo"><div class="d" id="nota-arquivos"></div>
        <div class="acoes" style="flex-wrap:nowrap"><input type="text" id="ent-volume" value="${esc(ent.volume || volPadrao)}" placeholder="${esc(volPadrao)}">
        <button class="sec" type="button" id="restaurar-volume" style="white-space:nowrap">Usar o padrão</button></div></label>
      <div class="explica" style="margin:0 0 6px"><div class="rot">DENTRO DESSA PASTA, CADA TIPO DE ARQUIVO FICA EM</div><div id="pastas"></div></div>
      <div class="n" style="font-weight:700;color:var(--navy);margin:16px 0 6px"><span id="num-onda">4</span>. Onda</div>
      <label class="campo"><div class="d">O lote que está sendo migrado agora, normalmente um app ou um domínio de negócio. O nome vai como tag em tudo o que for criado, para acompanhar andamento e custo por lote. Ex.: <code>onda_1_custo_assistencial</code>.</div>
        <input type="text" id="onda" value="${esc(amb.onda || "")}"></label>
      <label class="campo"><div class="n">Responsável técnico (opcional)</div><div class="d">Recebe os alertas de falha. Vazio = quem executar o job.</div>
        <input type="text" id="dono" value="${esc(amb.dono || "")}" placeholder="vazio = quem executa"></label>
      <p style="font-size:12.5px;color:var(--cinza-2);margin-bottom:0">Catálogo <code>${esc(CTX.catalogo)}</code> e schemas <code>${esc(CTX.prefixo)}_*</code> são definidos na instalação do acelerador (job e app).</p>
      <div class="acoes" style="margin-top:14px"><button id="salvar">Salvar entrada de dados</button><span style="color:var(--cinza-2);font-size:12.5px">Grava modo, origens, pasta de entrada, onda e responsável. Vale a partir da próxima execução.</span></div>
    </div>`;
  const htmlAvancado = explica("AJUSTES AVANÇADOS", `Tudo aqui já vem com um padrão que funciona. Mude apenas se o ambiente do cliente pedir (ex.: outro limite de dias para considerar um QVD parado). O que você salva fica em <code>config/acelerador.json</code> no volume, vale a partir da próxima execução e entra no Diário.`) +
    `<div class="cartao"><h3>Ajustes avançados <span class="sub">já vêm preenchidos; abra só se precisar</span></h3>
      ${Object.entries(AVANCADOS).filter(([g]) => cfg[g]).map(([g, t]) => `<details style="margin:8px 0"><summary>${t}</summary><div style="padding:12px 0 0">${avancado(g)}</div></details>`).join("")}
      <details style="margin:8px 0"><summary>Tags extras</summary><div style="padding:12px 0 0">${campoAvancado("ambiente", "tags", amb.tags || {})}</div></details>
    </div>
    <div class="acoes"><button id="salvar">Salvar ajustes</button><span style="color:var(--cinza-2);font-size:12.5px">Depois de salvar, execute de novo os passos afetados.</span></div>`;
  el.innerHTML = parte === "essencial" ? htmlEssencial : htmlAvancado;

  const atualizar = () => {
    const modo = el.querySelector("input[name=modo]:checked").value;
    const origem = el.querySelector("input[name=origem]:checked").value;
    $("#bloco-cliente", el).style.display = modo === "cliente" ? "" : "none";
    $("#num-arq", el).textContent = modo === "cliente" ? "3" : "2";
    $("#num-onda", el).textContent = modo === "cliente" ? "4" : "3";
    $("#campo-bronze", el).style.display = origem === "lake" ? "" : "none";
    el.querySelectorAll("label.passo").forEach((l) => l.classList.toggle("sel", l.querySelector("input").checked));
    const valor = ($("#ent-volume", el).value || "").trim().replace(/\/$/, "");
    const padrao = !valor || valor === volPadrao;
    $("#restaurar-volume", el).style.display = padrao ? "none" : "";
    $("#nota-arquivos", el).innerHTML = modo === "demo"
      ? `O padrão é o volume do próprio acelerador. O passo 01 copia a amostra fictícia para esta pasta. Mude só se quiser testar com outro volume (é preciso ter escrita nele).`
      : (padrao
        ? `O padrão é o volume do próprio acelerador: envie os arquivos pelo bloco 1c logo abaixo. Se o time já copiou os arquivos para outro volume do Unity Catalog, cole o caminho aqui (ex.: <code>/Volumes/prd_landing/qlik/export</code>).`
        : `Os arquivos serão lidos deste volume. O app precisa de leitura nele (e de escrita, se você também for enviar arquivos pelo bloco 1c).`);
    const p = pastas();
    $("#pastas", el).innerHTML = `<table style="font-size:12.5px"><tbody>${[["Inventário (.csv)", p.inventario], ["Load scripts (.qvs)", p.scripts], ["Medidas e tabelas dos apps (.json, .csv)", p.apps], ["QVDs ou cabeçalhos (.qvd)", p.qvd]].map(([a, b]) => `<tr><td style="background:none">${a}</td><td style="background:none"><code>${esc(b)}</code></td></tr>`).join("")}</tbody></table>
      <div style="font-size:12px;margin-top:4px">Os nomes das subpastas ficam em Ajustes avançados → Nomes das pastas de entrada.</div>`;
  };
  if (parte === "essencial") {
    el.querySelectorAll("input[type=radio]").forEach((r) => r.onchange = atualizar);
    $("#ent-volume", el).oninput = atualizar;
    $("#restaurar-volume", el).onclick = () => { $("#ent-volume", el).value = volPadrao; atualizar(); };
    atualizar();
  }

  $("#salvar", el).onclick = async () => {
    const nova = JSON.parse(JSON.stringify(cfg));
    try {
      if (parte === "essencial") {
      const modo = el.querySelector("input[name=modo]:checked").value;
      const origem = el.querySelector("input[name=origem]:checked").value;
      nova.ambiente.modo = modo;
      nova.ambiente.schema_bronze = modo === "cliente" && origem === "lake" ? ($("#schema-bronze", el).value.trim() || null) : null;
      if (modo === "cliente" && origem === "lake" && !nova.ambiente.schema_bronze) throw new Error("Informe o catálogo.schema das tabelas de origem, ou marque que ainda não foram ingeridas.");
      if (nova.ambiente.schema_bronze && nova.ambiente.schema_bronze.split(".").length !== 2) throw new Error("O schema das origens deve estar no formato catalogo.schema.");
      const vol = ($("#ent-volume", el).value || "").trim().replace(/\/$/, "");
      nova.entrada.volume = !vol || vol === volPadrao ? null : vol;     // null = padrão do acelerador
      if (nova.entrada.volume && !nova.entrada.volume.startsWith("/Volumes/")) throw new Error("O caminho do volume começa com /Volumes/.");
      nova.ambiente.onda = $("#onda", el).value.trim() || "onda_1";
      nova.ambiente.dono = $("#dono", el).value.trim() || null;
      }
      el.querySelectorAll("[data-id]").forEach((i) => {
        const [g, k] = i.dataset.id.split("."), t = i.dataset.t, v = i.value;
        nova[g][k] = t === "bool" ? v === "true" : t === "num" ? Number(v) : t === "lista" ? v.split(",").map((x) => x.trim()).filter(Boolean) : t === "json" ? JSON.parse(v) : (v === "" ? null : v);
      });
      const r = await api("/api/config", { method: "PUT", body: JSON.stringify(nova) });
      const n = Object.values(r.mudancas || {}).reduce((a, b) => a + Object.keys(b).length, 0);
      avisar(n ? `Configuração salva (${n} alteração(ões)).` : "Nada mudou.");
      CTX = await api("/api/contexto");
      if (parte === "essencial") rota();
    } catch (e) { avisar(e.message, true); }
  };
}

// ------------------------------------------------------------------ coleta
async function pgColeta(el) {
  const c = await api("/api/coleta");
  const lista = (k) => tabela(c.arquivos[k], [{ t: "Arquivo", k: "nome" }, { t: "KB", k: "bytes", num: 1, f: (x) => fmt(x / 1024, 1) }, { t: "Modificado", k: "modificado", f: dataHora }], { vazio: "Nenhum arquivo." });
  const volumeProprio = !String(c.pastas.scripts || "").startsWith(CTX.volume);
  const demo = CTX.modo === "demo";
  if (demo) {
    el.innerHTML = `<div class="cartao"><h3>1c. Arquivos da demonstração</h3>
      <p style="margin-top:0;line-height:1.55">No modo demonstração não é preciso enviar nada. O <b>passo 01</b> copia para as pastas abaixo uma amostra fictícia (scripts, medidas e inventário de uma cooperativa de saúde inventada), e o <b>passo 02</b> gera os QVDs e as tabelas exportadas a partir de um ERP sintético. É o mesmo tipo de arquivo que o cliente enviaria no modo Ambiente do cliente.</p>
      <details><summary>Quais arquivos o cliente enviaria, para quê e como obter</summary><div style="margin-top:12px">${tabelaEntradas(c.pastas)}</div></details></div>
    ${cartao("O que já está nas pastas", c.validacoes.map((v) => `<div class="check"><span class="ic ${v.ok ? "ok" : "no"}">${v.ok ? "✓" : "!"}</span><div><b>${esc(v.item)}</b><div class="d">${esc(v.dica)}</div></div></div>`).join("") + (c.arquivos.scripts.length ? "" : `<p style="font-size:12.5px;color:var(--cinza-2)">Ainda vazio: execute o passo 01 (tela Executar) para copiar a amostra.</p>`))}
    <div class="duas">${cartao("Scripts", lista("scripts"))}${cartao("Apps: medidas e tabelas exportadas", lista("apps"))}${cartao("QVDs", lista("qvd"))}${cartao("Inventário", lista("inventario"))}</div>`;
    return;
  }
  el.innerHTML = `<div class="cartao"><h3>1c. ${volumeProprio ? "Conferir os arquivos no volume indicado" : "Enviar e conferir os arquivos"}</h3>
      <p style="margin-top:0;line-height:1.55">${volumeProprio
        ? `Os arquivos são lidos de <code>${esc(c.pastas.scripts.replace(/\/[^/]+$/, ""))}</code>. Confira abaixo se o acelerador encontrou tudo. Se preferir, ainda dá para enviar arquivos pelo app para as mesmas pastas.`
        : `Arraste um <b>.zip</b> com tudo, ou os arquivos soltos: o app separa por tipo e grava cada um na pasta certa.`}
      Nenhum dado de negócio vai para o modelo de IA: ele recebe só scripts e metadados.</p>
    <details style="margin-bottom:14px"><summary>Quais arquivos, para quê e como obter</summary><div style="margin-top:12px">${tabelaEntradas(c.pastas)}</div></details>
    <div class="zona" id="zona"><div class="g">Arraste os arquivos aqui ou clique para escolher</div><div>.zip, .qvs, .qvd, .json, .csv</div><input type="file" id="arq" multiple style="display:none"></div>
    <div id="resultado-upload" style="margin:12px 0"></div></div>
    ${cartao("Checagem do pacote", c.validacoes.map((v) => `<div class="check"><span class="ic ${v.ok ? "ok" : "no"}">${v.ok ? "✓" : "!"}</span><div><b>${esc(v.item)}</b><div class="d">${esc(v.dica)}</div></div></div>`).join(""))}
    <div class="duas">${cartao("Scripts", lista("scripts"))}${cartao("Apps: medidas e hypercubes", lista("apps"))}${cartao("QVDs e cabeçalhos", lista("qvd"))}${cartao("Inventário", lista("inventario"))}</div>`;
  const zona = $("#zona", el), arq = $("#arq", el);
  zona.onclick = () => arq.click();
  zona.ondragover = (e) => { e.preventDefault(); zona.classList.add("arrastando"); };
  zona.ondragleave = () => zona.classList.remove("arrastando");
  const enviar = async (files) => {
    zona.classList.remove("arrastando");
    const res = $("#resultado-upload", el); res.innerHTML = "Enviando…";
    let gravados = [], ignorados = [];
    for (const f of files) {
      const fd = new FormData(); fd.append("arquivo", f);
      try { const r = await api("/api/coleta/upload", { method: "POST", body: fd }); gravados = gravados.concat(r.gravados); ignorados = ignorados.concat(r.ignorados); }
      catch (e) { avisar(`${f.name}: ${e.message}`, true); }
    }
    avisar(`${gravados.length} arquivo(s) gravados no volume.`);
    if (ignorados.length) avisar(`Ignorados (tipo não reconhecido): ${ignorados.join(", ")}`, true);
    rota();
  };
  zona.ondrop = (e) => { e.preventDefault(); enviar(e.dataTransfer.files); };
  arq.onchange = () => enviar(arq.files);
}

// ------------------------------------------------------------------ executar
const DESC_PASSO = {
  "01_preparar_ambiente": ["Preparar ambiente", "Schemas, volume, tabelas de controle e tags."],
  "02_simular_ambiente_qlik": ["Simular Qlik", "Só no modo demo: ERP fictício e QVDs reais para teste."],
  "03_inventario_triagem": ["Inventário e triagem", "Agrupa QVDs em tabelas lógicas e decide o destino de cada uma."],
  "04_linhagem": ["Linhagem", "Lê scripts e cabeçalhos: grafo, complexidade, linhagem por campo e por métrica."],
  "05_conversao_ia": ["Conversão com IA", "Gera, valida e dá nota a cada objeto. Nota baixa vai para revisão."],
  "06_publicar": ["Publicar", "Bundle e pipeline só com o que foi aprovado."],
  "07_semantica_seguranca": ["Métricas e segurança", "Metric views, glossário e Section Access como row filter."],
  "08_paridade": ["Paridade", "N1 volume, N2 somas, N3 linha a linha, N4 métricas contra o app."],
};
async function pgExecutar(el) {
  const sel = new Set(CTX.passos);
  let iniciando = false;
  const desenhar = () => {
    $("#passos", el).innerHTML = CTX.passos.map((p, i) => `<label class="passo ${sel.has(p) ? "sel" : ""}"><input type="checkbox" data-p="${p}" ${sel.has(p) ? "checked" : ""}>
      <div class="t">${String(i + 1).padStart(2, "0")} · ${DESC_PASSO[p][0]}</div><div class="d">${DESC_PASSO[p][1]}</div></label>`).join("");
    $("#passos", el).querySelectorAll("input").forEach((i) => i.onchange = () => { i.checked ? sel.add(i.dataset.p) : sel.delete(i.dataset.p); desenhar(); });
  };
  el.innerHTML = `<div id="andamento"></div>` +
    explica("DICA", `Na primeira vez, rode tudo. Depois de aprovar ou rejeitar objetos na revisão, rode só <b>05 a 08</b> (rejeitados são reconvertidos com a sua orientação). Mudou a triagem? Rode <b>03</b>. Só uma execução roda por vez: enquanto ela estiver em andamento, o botão fica travado e o progresso aparece aqui e no topo de todas as telas.`) +
    cartao("Passos", `<div class="passos" id="passos"></div>
      <div class="acoes" style="margin-top:14px">
        <div style="width:220px"><select id="modo" title="Vazio = o modo salvo em Entrada de dados"><option value="">modo salvo em Entrada de dados</option><option value="demo">Demonstração</option><option value="cliente">Ambiente do cliente</option></select></div>
        <div style="width:200px"><input type="text" id="onda" placeholder="onda salva em Entrada de dados" title="Lote da migração em andamento; vai como tag em tudo que for criado"></div>
        <button id="tudo" class="sec">Selecionar tudo</button><button id="pos" class="sec">Só 05 a 08</button><button id="ir">Executar selecionados</button>
        <span id="motivo-trava" style="font-size:12.5px;color:var(--cinza-2)"></span>
      </div>`) +
    cartao("Execuções recentes", `<div id="runs" class="vazio">Carregando…</div>`, "atualiza a cada 10 segundos");
  desenhar();

  const painel = () => {
    const alvo = $("#andamento", el); if (!alvo) return;
    const r = EXEC && EXEC.rodando;
    const ir = $("#ir", el);
    if (!r) {
      alvo.innerHTML = "";
      if (!iniciando) { ir.disabled = false; ir.textContent = "Executar selecionados"; $("#motivo-trava", el).textContent = ""; }
      return;
    }
    iniciando = false;
    ir.disabled = true; ir.textContent = "Execução em andamento…";
    $("#motivo-trava", el).textContent = "Aguarde terminar ou cancele a execução atual para iniciar outra.";
    const pct = r.total ? Math.round(100 * (r.concluidas + (r.atual ? 0.5 : 0)) / r.total) : 5;
    const fila = EXEC.na_fila.filter((x) => x.run_id !== r.run_id);
    const classe = (t) => ["SUCCESS"].includes(t.estado) ? "ok" : ["FAILED", "UPSTREAM_FAILED", "CANCELED", "TIMEDOUT"].includes(t.estado) ? "erro"
      : ["EXCLUDED", "DISABLED"].includes(t.estado) ? "fora" : t.passo === r.atual ? "agora" : "";
    const icone = (c) => ({ ok: "✓", erro: "✕", agora: '<span class="pulso"></span>', fora: "–" }[c] || "○");
    alvo.innerHTML = `<div class="andamento">
      <div class="topo-and">
        <div class="tit"><span class="pulso"></span>${r.atual ? `Executando o passo ${esc(r.atual.slice(0, 2))} · ${esc(NOME_PASSO(r.atual))}` : "Execução na fila, aguardando compute"}</div>
        <div class="acoes"><span style="font-size:13px">há ${duracao(Date.now() - r.inicio)}${r.inicio_atual ? ` · neste passo há ${duracao(Date.now() - r.inicio_atual)}` : ""}</span>
          <a class="botao sec" href="${r.url}" target="_blank">Ver no Lakeflow Jobs</a><button class="rosa" id="cancelar">Cancelar</button></div></div>
      <div class="barra-prog"><span style="width:${pct}%"></span></div>
      <div class="trilha">${r.tarefas.map((t) => { const c = classe(t); return `<span class="et ${c}" title="${esc(t.estado || "")}">${icone(c)} ${esc(t.passo.slice(0, 2))} ${esc(NOME_PASSO(t.passo))}${t.segundos && c === "ok" ? ` · ${t.segundos}s` : ""}</span>`; }).join("")}</div>
      <div style="font-size:12.5px;color:var(--cinza-2);margin-top:10px">${r.concluidas} de ${r.total} passos concluídos · execução ${r.run_id}. Pode navegar pelo app: o progresso continua no topo da tela.</div>
      ${fila.length ? `<div class="explica laranja" style="margin:12px 0 0"><div class="rot">TAMBÉM NA FILA</div>${fila.length} execução(ões) esperando esta terminar: ${fila.map((f) => f.run_id).join(", ")}. <button class="sec" id="cancelar-fila" style="margin-left:8px">Cancelar as da fila</button></div>` : ""}
    </div>`;
    $("#cancelar", el).onclick = async () => {
      if (!confirm(`Cancelar a execução ${r.run_id}? Os passos já concluídos continuam valendo.`)) return;
      try { await api(`/api/execucoes/${r.run_id}/cancelar`, { method: "POST" }); avisar("Cancelamento pedido."); vigiarExecucao(); } catch (e) { avisar(e.message, true); }
    };
    const cf = $("#cancelar-fila", el);
    if (cf) cf.onclick = async () => {
      for (const f of fila) { try { await api(`/api/execucoes/${f.run_id}/cancelar`, { method: "POST" }); } catch (e) { avisar(e.message, true); } }
      avisar(`${fila.length} execução(ões) da fila cancelada(s).`); vigiarExecucao();
    };
  };
  aoMudarExecucao = painel;
  painel();

  $("#tudo", el).onclick = () => { CTX.passos.forEach((p) => sel.add(p)); desenhar(); };
  $("#pos", el).onclick = () => { sel.clear(); CTX.passos.slice(4).forEach((p) => sel.add(p)); desenhar(); };
  $("#ir", el).onclick = async () => {
    if (!sel.size) return avisar("Selecione ao menos um passo.", true);
    const ir = $("#ir", el);
    iniciando = true; ir.disabled = true; ir.textContent = "Iniciando…";      // trava na hora: evita cliques repetidos
    try {
      const r = await api("/api/executar", { method: "POST", body: JSON.stringify({ passos: CTX.passos.filter((p) => sel.has(p)), modo: $("#modo", el).value || null, onda: $("#onda", el).value || null }) });
      avisar(`Execução ${r.run_id} iniciada. O progresso aparece logo acima.`);
      window.scrollTo({ top: 0, behavior: "smooth" });
      await vigiarExecucao(); carregarRuns();
    } catch (e) { iniciando = false; avisar(e.message, true); await vigiarExecucao(); }
  };
  const carregarRuns = async () => {
    const runs = await api("/api/execucoes");
    const alvo = $("#runs", el); if (!alvo) return;
    alvo.className = "";
    alvo.innerHTML = runs.length ? tabela(runs, [
      { t: "Execução", k: "run_id", f: (x, l) => `<a href="${l.url}" target="_blank">${x}</a>` },
      { t: "Início", k: "inicio", f: dataHora }, { t: "Situação", k: "estado", f: selo },
      { t: "Parâmetros", k: "parametros", f: (p) => esc(Object.entries(p || {}).filter(([k]) => k !== "execucao_id").map(([k, v]) => `${k}=${v}`).join(" · ")) },
      { t: "Passos", k: "tarefas", f: (t) => `<div class="linha-tempo">${t.map((x) => `<span class="chip ${x.estado}" title="${x.estado}">${x.passo.slice(0, 2)}${x.segundos ? ` · ${x.segundos}s` : ""}</span>`).join("")}</div>` },
    ]) : `<div class="vazio">Nenhuma execução ainda.</div>`;
  };
  carregarRuns(); timer = setInterval(carregarRuns, 10000);
}

// ------------------------------------------------------------------ triagem
async function pgTriagem(el) {
  const [t, c] = await Promise.all([api("/api/triagem"), api("/api/config")]);
  const i = t.inventario[0] || {};
  const tri = (c.efetiva || {}).triagem || {};
  const lst = (x) => (x || []).map((v) => `<code>${esc(v)}</code>`).join(", ");
  const REGRAS = [
    ["descartar", "Arquivo com zero linhas, ou em um dos espaços " + lst(tri.espacos_descartar) + ", ou com " + `<code>${esc(tri.termo_teste)}</code>` + " no nome.", "Não migra. Confirmar com o dono do espaço antes de apagar no Qlik."],
    ["revisar", "Nome anômalo: começa com ponto ou repete o prefixo (sinal de erro de caminho no STORE).", "Alguém olha o caso antes de decidir."],
    ["arquivar_historico", `Sem atualização há mais de <b>${esc(tri.dias_parado)}</b> dias.`, "Não entra no pipeline. Se ainda for consultado, carrega uma vez como histórico."],
    ["eleger_oficial", "O mesmo nome aparece em mais de um espaço (ex.: oficial e pessoal, com contagens diferentes).", "Escolher qual versão é a oficial; só ela entra na paridade."],
    ["migrar", "Todo o resto.", "Entra no escopo: vira tabela ou materialized view no Lakehouse."],
  ];
  el.innerHTML = `<div class="kpis">
      <div class="kpi"><div class="v">${fmt(i.arquivos)}</div><div class="r">arquivos QVD</div></div>
      <div class="kpi"><div class="v">${fmt(i.gb, 1)}</div><div class="r">GB no total</div></div>
      <div class="kpi"><div class="v">${fmt(i.milhoes_linhas)}</div><div class="r">milhões de linhas</div></div>
      <div class="kpi"><div class="v">${fmt(i.espacos)}</div><div class="r">espaços</div></div></div>
    ${explica("COMO LER", `O volume costuma ser pequeno; o esforço está na lógica. Antes de classificar, o acelerador <b>junta os arquivos fatiados por período</b> (ex.: <code>BI_CONTAS_202401</code> a <code>BI_CONTAS_202412</code>) em uma única <b>tabela lógica</b>: é isso que significa consolidar. Depois, cada arquivo recebe uma das cinco classificações abaixo, testadas nesta ordem (vale a primeira que se aplicar). Os limites vêm de <a href="#config">Configurar → Ajustes avançados → Triagem</a>.`)}
    ${cartao("As cinco classificações", `<table><thead><tr><th>Classificação</th><th>Quando</th><th>O que acontece</th></tr></thead><tbody>${REGRAS.map(([d, q, a]) => `<tr><td>${selo(d)}</td><td>${q}</td><td>${a}</td></tr>`).join("")}</tbody></table>
      <p style="font-size:12.5px;color:var(--cinza-2);margin:10px 0 0">Também é calculada a <b>atividade</b> (ativo até ${esc(tri.dias_ativo)} dias, recente até ${esc(tri.dias_recente)}, parado até ${esc(tri.dias_parado)}, histórico acima) e a <b>camada provável</b> pelo nome: prefixos ${lst(tri.prefixos_gold)} indicam gold, ${lst(tri.prefixos_bronze)} indicam bronze, o resto silver. A camada é confirmada na linhagem. Uma tabela lógica fica com a classificação mais favorável entre seus arquivos: basta um arquivo ativo para a família inteira migrar.</p>`)}
    ${cartao("Resultado por classificação", tabela(t.por_decisao, [{ t: "Classificação", k: "decisao", f: selo }, { t: "Arquivos", k: "arquivos", num: 1, f: (x) => fmt(x) }, { t: "Tabelas lógicas", k: "tabelas_logicas", num: 1, f: (x) => fmt(x) }, { t: "GB", k: "gb", num: 1, f: (x) => fmt(x, 2) }]))}
    ${cartao("Por espaço", tabela(t.por_espaco, [{ t: "Espaço", k: "espaco" }, { t: "Arquivos", k: "arquivos", num: 1, f: (x) => fmt(x) }, { t: "GB", k: "gb", num: 1, f: (x) => fmt(x, 2) }, { t: "Ativos", k: "ativos", num: 1 }, { t: "Vazios", k: "vazios", num: 1 }, { t: "Duplicados", k: "duplicados", num: 1 }]))}
    ${cartao("Maiores tabelas lógicas a migrar", tabela(t.top_migrar, [{ t: "Espaço", k: "espaco" }, { t: "Tabela lógica", k: "familia" }, { t: "Arquivos", k: "arquivos", num: 1 }, { t: "GB", k: "gb", num: 1, f: (x) => fmt(x, 3) }, { t: "Camada provável", k: "camada_hipotese" }, { t: "Dias sem atualizar", k: "dias_sem_atualizar", num: 1 }]))}`;
}

// ------------------------------------------------------------------ linhagem
async function pgLinhagem(el) {
  const l = await api("/api/linhagem");
  el.innerHTML = explica("COMO LER O GRAFO", "Cilindro = tabela do banco de origem · retângulo = QVD (cor indica a camada de destino: bronze, silver ou gold) · hexágono = app Qlik. Cada QVD intermediário deixa de existir: vira uma tabela ou materialized view no Lakehouse.") +
    cartao("Grafo de linhagem", l.mermaid ? `<pre class="mermaid">${esc(l.mermaid)}</pre>` : `<div class="vazio">Execute o passo 04.</div>`) +
    cartao("De cada métrica até a origem", tabela(l.medidas, [{ t: "App", k: "app" }, { t: "Medida no Qlik", k: "medida_qlik" }, { t: "Metric view", k: "metric_view" }, { t: "Medida", k: "medida_databricks" }, { t: "Conversão", k: "status_conversao", f: selo }, { t: "Tabelas gold", k: "tabelas_gold" }, { t: "Colunas de origem", k: "colunas_origem" }]), "o escopo mínimo da migração") +
    `<div class="duas">${cartao("Complexidade dos scripts", tabela(l.scripts, [{ t: "Script", k: "script" }, { t: "Papel", k: "papel" }, { t: "Linhas", k: "linhas", num: 1 }, { t: "Score", k: "score", num: 1, f: (x) => fmt(x, 1) }, { t: "Classe", k: "classe" }, { t: "Alertas", k: "alertas", num: 1 }]))}
     ${cartao("Destino de cada QVD", tabela(l.qvds, [{ t: "QVD", k: "qvd" }, { t: "Camada", k: "camada_destino" }, { t: "Objeto no Lakehouse", k: "objeto_destino" }]))}</div>` +
    cartao("Alertas de semântica", tabela(l.alertas, [{ t: "Script", k: "script" }, { t: "Tipo", k: "tipo" }, { t: "Detalhe", k: "detalhe" }]), "pontos onde o Qlik se comporta diferente do SQL");
  if (l.mermaid) {
    const m = (await import("https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs")).default;
    m.initialize({ startOnLoad: false, theme: "neutral", securityLevel: "strict" });
    await m.run({ querySelector: ".mermaid" });
  }
}

// ------------------------------------------------------------------ conversão
async function pgConversao(el, nome) {
  if (nome) return pgConversaoDetalhe(el, nome);
  const c = await api("/api/conversao");
  PENDENTES = c.objetos.filter((o) => ["revisao_humana", "bloqueado"].includes(o.status_revisao) && o.estado_atual !== "aprovado").length; montarMenu("conversao");
  el.innerHTML = explica("COMO A REVISÃO FUNCIONA", `Cada objeto gerado passa por lint, compilação, contrato de colunas e contagem contra o QVD, e recebe uma <b>nota de 0 a 100</b>. Nota igual ou acima do mínimo configurado segue sozinha. Abaixo disso, ou se a validação falhou, o objeto espera uma decisão aqui.
      <b>Aprovar</b> libera para o passo 06. <b>Rejeitar</b> exige uma orientação, que volta para o modelo na próxima execução do passo 05.`, "roxo") +
    cartao("Objetos gerados", tabela(c.objetos, [
      { t: "Objeto", k: "nome", f: (x) => `<b>${esc(x)}</b>` }, { t: "Script Qlik", k: "script" }, { t: "Camada", k: "camada" },
      { t: "Validação", k: "status", f: selo }, { t: "Nota", k: "nota", f: nota }, { t: "Situação", k: "status_revisao", f: selo },
      { t: "Estado atual", k: "estado_atual", f: selo }, { t: "Decidido por", k: "decidido_por", f: (x, l) => x ? `${esc(x)}<br><span style="color:var(--cinza-2);font-size:11.5px">${dataHora(l.decidido_em)}</span>` : "" },
    ], { clique: true, chave: "nome", vazio: "Execute o passo 05." }), "clique em um objeto para revisar") +
    cartao("Extração: especificação de ingestão para o Lakeflow Connect", tabela(c.ingestao, [
      { t: "QVD", k: "qvd" }, { t: "Tabela de origem", k: "tabela_origem" }, { t: "Destino bronze", k: "tabela_bronze" }, { t: "Chave", k: "chave_primaria" },
      { t: "Cursor", k: "coluna_cursor" }, { t: "Modo inicial", k: "modo_inicial" }, { t: "SQL de origem", k: "sql_de_origem" }]));
  el.querySelectorAll("tr.clicavel").forEach((tr) => tr.onclick = () => location.hash = `#conversao/${encodeURIComponent(tr.dataset.k)}`);
}

async function pgConversaoDetalhe(el, nome) {
  const o = await api(`/api/conversao/${encodeURIComponent(nome)}`);
  const ultima = o.historico[0];
  el.innerHTML = `<div class="acoes" style="margin-bottom:14px"><a class="botao sec" href="#conversao">← Voltar à lista</a>
      <span>validação ${selo(o.status)} · estado atual ${selo((o.estado[0] || {}).estado || o.status_revisao)} ${ultima ? `· última decisão: ${selo(ultima.decisao)} por ${esc(ultima.usuario)} em ${dataHora(ultima.ts)}` : ""}</span></div>
    <div class="tres">
      ${cartao("Nota de confiança", `<div style="font-size:34px;font-weight:700;color:var(--navy)">${fmt(o.nota)}</div><ul class="lista-motivos">${(o.motivos || []).map((m) => `<li>${esc(m)}</li>`).join("")}</ul>`)}
      ${cartao("Validação automática", `<div>${selo(o.status)}</div><p>Contagem no QVD: <b>${fmt(o.contagem_qvd)}</b> · no Databricks: <b>${fmt(o.contagem_obtida)}</b></p><p>Tentativas: <b>${o.tentativas}</b></p>${(o.mensagens || []).length ? `<ul class="lista-motivos">${o.mensagens.map((m) => `<li>${esc(m)}</li>`).join("")}</ul>` : ""}`)}
      ${cartao("Chamadas ao modelo", tabela(o.llm, [{ t: "Tent.", k: "tentativa", num: 1 }, { t: "s", k: "segundos", num: 1 }, { t: "Tokens", k: "tokens_entrada", num: 1, f: (x, l) => fmt(x + l.tokens_saida) }, { t: "Cache", k: "do_cache", f: (x) => x ? "sim" : "não" }]))}
    </div>
    ${cartao(`${esc(o.nome)} <span class="sub">${esc(o.descricao || "")}</span>`, `<div class="lado">
      <div><div class="codigo-tit"><span>Script Qlik · ${esc(o.script)}.qvs</span></div>${codigo(o.script_qlik)}</div>
      <div><div class="codigo-tit"><span>SQL gerado · camada ${esc(o.camada)}</span><span style="font-weight:400;font-size:12px">cluster by: ${esc((o.cluster_by || []).join(", ") || "–")}</span></div>${codigo(o.sql_gerado)}</div></div>`)}
    <div class="cartao"><h3>Sua decisão</h3>
      <label class="campo"><div class="n">Orientação ou comentário</div><div class="d">Obrigatório ao rejeitar: diga o que está errado e como deveria ser. Ex.: "o filtro de competência precisa incluir o mês corrente" ou "Exists aqui depende só da carga de beneficiários ativos".</div>
      <textarea id="coment"></textarea></label>
      <div class="acoes"><button class="verde" id="aprovar" ${o.status !== "ok" ? "disabled title='Validação falhou: só é possível rejeitar com orientação'" : ""}>Aprovar</button><button class="rosa" id="rejeitar">Rejeitar com orientação</button></div></div>
    <div class="duas">
      ${cartao("Documentação das colunas", tabela(o.colunas, [{ t: "Coluna", k: "nome" }, { t: "Descrição", k: "descricao" }, { t: "Regra herdada do Qlik", k: "regra" }]), "vai para o Unity Catalog como comentário")}
      ${cartao("Expectativas de qualidade", tabela(o.expectativas, [{ t: "Nome", k: "nome" }, { t: "Condição", k: "expressao", f: (x) => `<code>${esc(x)}</code>` }, { t: "Motivo", k: "motivo" }, { t: "Violações", k: "violacoes_sandbox", num: 1 }]), "registram violações sem descartar linhas")}
    </div>
    <div class="duas">
      ${cartao("Premissas do modelo", tabela(o.premissas, [{ t: "Tipo", k: "tipo" }, { t: "Texto", k: "texto" }]), "o que a IA assumiu; confira")}
      ${cartao("Alertas do analisador", tabela(o.alertas, [{ t: "Tipo", k: "tipo" }, { t: "Detalhe", k: "detalhe" }]))}
    </div>
    ${cartao("Histórico de decisões", tabela(o.historico, [{ t: "Quando", k: "ts", f: dataHora }, { t: "Decisão", k: "decisao", f: selo }, { t: "Quem", k: "usuario" }, { t: "Nota", k: "nota_no_momento", num: 1 }, { t: "Comentário", k: "comentario" }], { vazio: "Nenhuma decisão ainda." }))}`;
  const decidir = async (d) => {
    try {
      await api(`/api/conversao/${encodeURIComponent(nome)}/decisao`, { method: "POST", body: JSON.stringify({ decisao: d, comentario: $("#coment", el).value || null }) });
      avisar(d === "aprovar" ? "Aprovado. Rode o passo 06 para publicar." : "Rejeitado. Rode o passo 05 para reconverter com a sua orientação.");
      location.hash = "#conversao";
    } catch (e) { avisar(e.message, true); }
  };
  $("#aprovar", el).onclick = () => decidir("aprovar");
  $("#rejeitar", el).onclick = () => decidir("rejeitar");
}

// ------------------------------------------------------------------ publicação
async function pgPublicacao(el) {
  const [c, u, p] = await Promise.all([api("/api/conversao"), api("/api/unidades?tipo=objeto"), api("/api/publicacao")]);
  const pub = u.filter((x) => ["publicado", "paridade_ok", "paridade_divergente"].includes(x.estado));
  const pl = p.pipeline;
  const tabelas = p.tabelas.filter((t) => !t.table_name.startsWith("__"));
  el.innerHTML = explica("O QUE O PASSO 06 FAZ", "Junta só os objetos liberados (pela nota ou por uma pessoa) e cria um <b>pipeline declarativo</b> no workspace com eles. Gera também um <b>Databricks Asset Bundle</b> com o mesmo código, para levar a dev, hml e prd. Objetos que dependem de algo pendente esperam a próxima publicação.") +
    `<div class="proximo"><div><div class="rot">PIPELINE GERADO</div>
        <div class="t">${pl ? `${fmt(pl.objetos)} objeto(s) · ${selo(pl.estado === "COMPLETED" ? "ok" : pl.estado)}` : "Ainda não gerado"}</div>
        <div class="d">${pl ? `Última publicação em ${dataHora(pl.ts)}. Abra para ver o grafo de dependências, a qualidade de cada tabela e o histórico de execuções.` : "Aprove os objetos na revisão e execute os passos 06 a 08."}</div></div>
      <div class="acoes">${pl ? `<a class="botao" href="${pl.url}" target="_blank">Abrir pipeline</a>${pl.url_execucao ? `<a class="botao sec" style="background:transparent;color:#fff;border-color:#fff" href="${pl.url_execucao}" target="_blank">Última execução</a>` : ""}` : `<a class="botao" href="#executar">Executar</a>`}</div></div>` +
    `<div class="kpis"><div class="kpi"><div class="v">${pub.length}</div><div class="r">objetos publicados</div></div>
      <div class="kpi"><div class="v">${Math.max(0, c.objetos.length - pub.length)}</div><div class="r">aguardando liberação</div></div>
      <div class="kpi"><div class="v">${tabelas.length}</div><div class="r">tabelas e views no silver e gold</div></div></div>` +
    cartao("Onde encontrar o que foi gerado", `<table><tbody>
      <tr><td><b>Pipeline</b></td><td>${pl ? `<a href="${pl.url}" target="_blank">${esc(pl.pipeline_id)}</a>` : "–"}</td><td>Grafo, qualidade e execuções das tabelas silver e gold.</td></tr>
      <tr><td><b>Tabelas gold</b></td><td><a href="${p.schema_gold_url}" target="_blank">${esc(CTX.catalogo)}.${esc(CTX.prefixo)}_gold</a></td><td>No Catalog Explorer: colunas com descrição, linhagem e permissões.</td></tr>
      <tr><td><b>Job do acelerador</b></td><td><a href="${p.job_url}" target="_blank">${esc(CTX.job_id)}</a></td><td>Histórico de execuções dos passos 01 a 08.</td></tr>
      <tr><td><b>Arquivos gerados</b></td><td><a href="${p.volume_url}" target="_blank">volume arquivos/saida</a></td><td>Bundle e glossário, também disponíveis para download abaixo.</td></tr>
    </tbody></table>`) +
    cartao("Tabelas e views geradas", tabela(tabelas, [
      { t: "Nome", k: "table_name", f: (x, l) => `<a href="${l.url}" target="_blank"><b>${esc(x)}</b></a>` },
      { t: "Camada", k: "table_schema", f: (x) => esc(String(x).replace(CTX.prefixo + "_", "")) },
      { t: "Tipo", k: "tipo" }, { t: "Descrição", k: "comment" }], { vazio: "Nada publicado ainda." }), "clique para abrir no Catalog Explorer") +
    explica("ONDE FICA CADA CAMADA", `Entrada, bronze, silver, gold, metric views e controle, com links para o Catalog Explorer: veja o <a href="#mapa">Mapa dos dados</a>.`) +
    cartao("Baixar", `<div class="acoes"><a class="botao" href="/api/download/bundle">Bundle (.zip)</a><a class="botao sec" href="/api/download/glossario">Glossário para as Pages do UC (.zip)</a></div>`) +
    cartao("Levar para dev, hml e prd", `<ol style="line-height:1.9;margin:0">
      <li>Descompacte o bundle num repositório Git do time.</li>
      <li>Confira em <code>databricks.yml</code> os catálogos e schemas de cada alvo (ou ajuste antes em <a href="#config">Ajustes avançados → Publicação</a>).</li>
      <li><code>databricks bundle validate -t dev</code> e depois <code>databricks bundle deploy -t dev</code></li>
      <li>Em hml e prd, configure o service principal em <code>run_as</code> e faça o deploy por CI/CD.</li></ol>`);
}

// ------------------------------------------------------------------ semântica e segurança
async function pgSemantica(el) {
  const s = await api("/api/semantica");
  el.innerHTML = explica("PARA QUEM VEM DO QLIK", "As <b>medidas mestras</b> viram <b>metric views</b>: a mesma definição de Custo, Glosa % ou Sinistralidade passa a valer em dashboard, Genie, Power BI e SQL. O <b>Section Access</b> vira tabela de acesso + <b>row filter</b> no Unity Catalog, testado perfil a perfil contra o que o usuário via no Qlik.") +
    cartao("Medidas convertidas", tabela(s.medidas, [{ t: "App", k: "app" }, { t: "Medida no Qlik", k: "titulo" }, { t: "Metric view", k: "metric_view" }, { t: "Medida", k: "medida" }, { t: "Conversão", k: "status", f: selo }, { t: "Teste", k: "teste", f: selo }, { t: "Observação", k: "observacao" }])) +
    `<div class="duas">${cartao("Teste por perfil de acesso", tabela(s.perfis, [{ t: "Usuário Qlik", k: "usuario_qlik" }, { t: "Grupo no IdP", k: "grupo_idp" }, { t: "Linhas Qlik", k: "linhas_qlik", num: 1, f: (x) => fmt(x) }, { t: "Linhas UC", k: "linhas_uc", num: 1, f: (x) => fmt(x) }, { t: "Resultado", k: "status", f: selo }], { vazio: "Sem Section Access nos scripts, ou passo 07 não executado." }))}
     ${cartao("Acesso efetivo por grupo", tabela(s.acesso, [{ t: "Grupo", k: "grupo_idp" }, { t: "Usuário Qlik", k: "usuario_qlik" }, { t: "Valores liberados", k: "valores" }]))}</div>` +
    cartao("Glossário de negócio", tabela(s.glossario, [{ t: "App", k: "app" }, { t: "Termo", k: "termo" }, { t: "Tipo", k: "tipo" }, { t: "Definição", k: "definicao" }, { t: "Fórmula Qlik", k: "formula_qlik", f: (x) => x ? `<code>${esc(x)}</code>` : "" }, { t: "Onde consultar", k: "objeto" }]), "importe nas Pages do Unity Catalog");
}

// ------------------------------------------------------------------ paridade
async function pgParidade(el) {
  const p = await api("/api/paridade");
  const objetos = [...new Set(p.resultado.map((r) => r.objeto))];
  const niveis = ["N1", "N2", "N3", "N4"];
  const matriz = objetos.map((o) => {
    const linha = { objeto: o };
    niveis.forEach((n) => { const rs = p.resultado.filter((r) => r.objeto === o && r.nivel === n); linha[n] = !rs.length ? null : rs.every((r) => r.status === "ok") ? "ok"
      : rs.every((r) => r.status === "aguardando_publicacao") ? "aguardando_publicacao" : "divergente"; });
    return linha;
  });
  el.innerHTML = explica("OS QUATRO NÍVEIS", "<b>N1</b> contagem de linhas por período · <b>N2</b> somas das colunas numéricas por período e dimensão · <b>N3</b> linha a linha, por hash, nos dois sentidos · <b>N4</b> as métricas da metric view contra a tabela exportada do app Qlik. Um objeto só deve substituir o QVD quando os quatro estão verdes. A paridade roda a cada execução enquanto os dois ambientes convivem, e o histórico fica guardado.") +
    cartao("Semáforo por objeto", tabela(matriz, [{ t: "Objeto", k: "objeto", f: (x) => `<b>${esc(x)}</b>` }, ...niveis.map((n) => ({ t: n, k: n, f: (x) => x ? selo(x) : "–" }))], { vazio: "Execute o passo 08." })) +
    cartao("Detalhe dos testes", tabela(p.resultado, [{ t: "Objeto", k: "objeto" }, { t: "Nível", k: "nivel" }, { t: "Teste", k: "teste" }, { t: "Grupos", k: "grupos", num: 1 }, { t: "Divergências", k: "divergencias", num: 1 }, { t: "Esperado (Qlik)", k: "esperado", num: 1, f: (x) => fmt(x, 2) }, { t: "Obtido (Databricks)", k: "obtido", num: 1, f: (x) => fmt(x, 2) }, { t: "Resultado", k: "status", f: selo }, { t: "Detalhe", k: "detalhe" }])) +
    cartao("Histórico", tabela(p.historico, [{ t: "Execução", k: "ts_execucao", f: dataHora }, { t: "Testes", k: "testes", num: 1 }, { t: "Ok", k: "ok", num: 1 }]));
}

// ------------------------------------------------------------------ mapa dos dados
const GRUPOS_CTRL = [
  ["Inventário e triagem (passo 03)", (n) => n.startsWith("inv_")],
  ["Linhagem (passo 04)", (n) => n.startsWith("lin_") || n === "qvd_campo"],
  ["Conversão e documentação (passos 05 e 06)", (n) => /^(conv_|doc_coluna|qual_)/.test(n)],
  ["Métricas e segurança (passo 07)", (n) => /^(sem_|gov_|doc_glossario)/.test(n)],
  ["Paridade (passo 08)", (n) => /^(par_|baseline_)/.test(n)],
  ["Governança e auditoria (todos os passos e o app)", (n) => ["evento", "execucao_passo", "unidade_status", "aprovacao"].includes(n)],
];

async function pgMapa(el) {
  const m = await api("/api/camadas");
  const demo = m.modo === "demo";
  const listaTabs = (tabs, max = 8) => {
    if (!tabs.length) return `<div class="vazio" style="padding:6px 0">Vazio por enquanto.</div>`;
    const item = (t) => `<li><a href="${t.url}" target="_blank">${esc(t.nome)}</a> <span style="color:var(--cinza-2);font-size:11.5px">${esc(t.tipo)}</span></li>`;
    const vis = tabs.slice(0, max).map(item).join(""), resto = tabs.slice(max).map(item).join("");
    return `<ul style="margin:6px 0 0;padding-left:16px;line-height:1.7;font-size:12.5px">${vis}</ul>` +
      (resto ? `<details><summary style="font-size:12.5px">mais ${tabs.length - max}</summary><ul style="margin:4px 0 0;padding-left:16px;line-height:1.7;font-size:12.5px">${resto}</ul></details>` : "");
  };
  const camada = (cor, titulo, qlik, o_que, fqn, url, n, corpo) => `<div class="cartao" style="border-top:5px solid ${cor};margin:0;display:flex;flex-direction:column">
      <div style="font-size:11px;letter-spacing:1.5px;font-weight:700;color:${cor}">${titulo}</div>
      <div style="margin:6px 0 2px">${url ? `<a href="${url}" target="_blank"><code style="font-size:12px">${esc(fqn)}</code></a>` : `<code style="font-size:12px">${esc(fqn)}</code>`}</div>
      <div style="font-size:22px;font-weight:700;color:var(--navy)">${n}</div>
      <div style="font-size:12.5px;line-height:1.5;margin:4px 0">${o_que}</div>
      <div style="font-size:12px;color:var(--cinza-2)"><b>No Qlik:</b> ${qlik}</div>
      <div style="flex:1">${corpo}</div>
      ${url ? `<a class="botao sec" style="margin-top:10px;text-align:center;font-size:12.5px" href="${url}" target="_blank">Abrir no Catalog Explorer</a>` : ""}</div>`;
  const seta = `<div class="seta-camada">→</div>`;
  const e = m.entrada, totalArq = Object.values(e.arquivos).reduce((a, b) => a + b, 0);
  const ctrl = m.controle.tabelas;
  const usados = new Set();
  const grupos = GRUPOS_CTRL.map(([t, f]) => { const ts = ctrl.filter((x) => f(x.nome)); ts.forEach((x) => usados.add(x.nome)); return [t, ts]; });
  const outros = ctrl.filter((x) => !usados.has(x.nome));
  if (outros.length) grupos.push(["Outras", outros]);

  el.innerHTML = explica("COMO LER", `O fluxo de cima é o caminho do dado: os <b>arquivos exportados do Qlik</b> entram num volume, as <b>tabelas de origem</b> ficam no bronze, e o pipeline gerado pelo acelerador constrói o <b>silver</b> e o <b>gold</b> no lugar dos QVDs. As <b>metric views</b> ficam no schema gold, no lugar das medidas mestras. Embaixo fica o <b>controle</b>: é ali que caem o primeiro processamento dos arquivos do Qlik (inventário, linhagem, conversão) e a auditoria. Cada nome abre no Catalog Explorer.`) +
    `<div class="fluxo-camadas">
      ${camada("#0B7285", "ENTRADA · ARQUIVOS (RAW)", "arquivos .qvs, .qvd e exports dos apps", `Os arquivos como vieram do Qlik, sem transformação.${demo ? " Na demonstração, é a amostra fictícia copiada pelo passo 01 e os QVDs gerados pelo passo 02." : ""}`, e.volume, e.url, `${fmt(totalArq)} arquivos`,
        `<ul style="margin:6px 0 0;padding-left:16px;line-height:1.7;font-size:12.5px">${[["inventario", "inventário"], ["scripts", "load scripts"], ["apps", "medidas e tabelas dos apps"], ["qvd", "QVDs ou cabeçalhos"]].map(([k, t]) => `<li>${t}: <b>${fmt(e.arquivos[k])}</b> <code style="font-size:11px">${esc(e.pastas[k].split("/").slice(-1)[0])}/</code></li>`).join("")}</ul>`)}
      ${seta}
      ${camada("#B8732A", "BRONZE · ORIGENS", "o SQL SELECT dos scripts de extract", demo ? "Tabelas do ERP fictício geradas pelo passo 02, no papel do que o Lakeflow Connect traria do banco." : "Tabelas de origem como estão no banco, ingeridas pelo Lakeflow Connect. Sem regra de negócio.", m.bronze.fqn, m.bronze.url, `${m.bronze.tabelas.length} tabelas`, listaTabs(m.bronze.tabelas))}
      ${seta}
      ${camada("#7A8691", "SILVER · INTERMEDIÁRIO", "QVDs de transformação e MAPPING LOAD", "Entidades conformadas e tabelas de referência, geradas pelo pipeline como materialized views.", m.silver.fqn, m.silver.url, `${m.silver.tabelas.length} objetos`, listaTabs(m.silver.tabelas))}
      ${seta}
      ${camada("#B8860B", "GOLD · MODELO", "QVDs Fato* e Dim* lidos pelos apps", "Fatos e dimensões consumidos pelo negócio, com descrição de cada coluna e regras de qualidade.", m.gold.fqn, m.gold.url, `${m.gold.tabelas.length} objetos`, listaTabs(m.gold.tabelas))}
      ${seta}
      ${camada("#5F3DC4", "METRIC VIEWS · SEMÂNTICA", "medidas mestras e Set Analysis", "Métricas governadas para AI/BI, Genie, Power BI e SQL. Ficam no mesmo schema do gold.", m.metric_views.fqn, m.metric_views.url, `${m.metric_views.tabelas.length} metric views`, listaTabs(m.metric_views.tabelas))}
    </div>` +
    (m.pipeline_url ? `<div class="acoes" style="margin:-6px 0 18px"><a class="botao" href="${m.pipeline_url}" target="_blank">Abrir o pipeline que gera silver e gold</a><span style="font-size:12.5px;color:var(--cinza-2)">Grafo de dependências, qualidade e execuções.</span></div>` : "") +
    `<div class="cartao" style="border-top:5px solid var(--navy)">
      <div class="acoes" style="justify-content:space-between"><div><div style="font-size:11px;letter-spacing:1.5px;font-weight:700;color:var(--navy)">CONTROLE · O QUE O ACELERADOR DESCOBRE E DECIDE</div>
        <a href="${m.controle.url}" target="_blank"><code style="font-size:12px">${esc(m.controle.fqn)}</code></a> · ${ctrl.length} tabelas · também guarda o volume de arquivos</div>
        <a class="botao sec" href="${m.controle.url}" target="_blank">Abrir no Catalog Explorer</a></div>
      <div class="tres" style="margin-top:12px;gap:14px">${grupos.map(([t, ts]) => `<div style="background:var(--fundo);border-radius:8px;padding:10px 14px"><b style="font-size:13px;color:var(--navy)">${t}</b>${listaTabs(ts, 6)}</div>`).join("")}</div>
      <p style="font-size:12px;color:var(--cinza-2);margin:12px 0 0"><code>baseline_*</code> são os QVDs originais convertidos em Delta, usados como referência na paridade.</p>
    </div>` +
    cartao(`Sandbox <span class="sub">${esc(m.sandbox.fqn)}</span>`, `<p style="margin:0 0 6px;font-size:13px">Views temporárias onde cada SQL gerado pela IA é compilado e testado antes de entrar no pipeline. Não é para consumo.</p>${listaTabs(m.sandbox.tabelas, 10)}
      <a class="botao sec" style="margin-top:10px" href="${m.sandbox.url}" target="_blank">Abrir no Catalog Explorer</a>`);
}

// ------------------------------------------------------------------ acompanhamento
async function pgUnidades(el) {
  const u = await api("/api/unidades");
  const tipos = [...new Set(u.map((x) => x.tipo))];
  el.innerHTML = explica("MÁQUINA DE ESTADOS", "Cada unidade de migração (tabela lógica do inventário, QVD, script, objeto gerado, medida, perfil de acesso) tem um estado atual. O caminho completo é: descoberto → triado → linhagem ok → convertido → aprovado → publicado → paridade ok → desligado no Qlik.") +
    `<div class="acoes" style="margin-bottom:14px"><select id="filtro" style="width:220px"><option value="">todos os tipos</option>${tipos.map((t) => `<option>${t}</option>`).join("")}</select></div><div id="lista"></div>`;
  const desenhar = () => {
    const f = $("#filtro", el).value, linhas = u.filter((x) => !f || x.tipo === f);
    $("#lista", el).innerHTML = cartao(`${linhas.length} unidades`, tabela(linhas, [{ t: "Tipo", k: "tipo" }, { t: "Unidade", k: "unidade" }, { t: "Estado", k: "estado", f: selo }, { t: "Atualizado", k: "ts_atualizado", f: dataHora }, { t: "Por", k: "atualizado_por" },
      { t: "Detalhe", k: "detalhe", f: (x) => x ? `<details><summary>ver</summary><code style="font-size:11.5px;white-space:pre-wrap">${esc(x)}</code></details>` : "" }]));
  };
  $("#filtro", el).onchange = desenhar; desenhar();
}

async function pgDiario(el) {
  const ev = await api("/api/eventos?limite=300");
  el.innerHTML = explica("AUDITORIA", "Cada passo do job, cada envio de arquivo, cada alteração de configuração e cada aprovação ou rejeição entra aqui, com o usuário. A fonte é a tabela <code>evento</code> do schema de controle, consultável também em SQL e em dashboards.") +
    cartao("Eventos recentes", tabela(ev, [{ t: "Quando", k: "ts", f: dataHora }, { t: "Passo", k: "passo" }, { t: "Nível", k: "nivel", f: selo }, { t: "Objeto", k: "objeto" }, { t: "Mensagem", k: "mensagem" }, { t: "Usuário", k: "usuario" }]));
}

// ------------------------------------------------------------------ início
(async () => {
  try {
    CTX = await api("/api/contexto");
    $("#contexto").innerHTML = `${esc(CTX.usuario)}<br>${esc(CTX.catalogo)} · ${esc(CTX.prefixo)}_* · job <a href="${CTX.job_url}" target="_blank">${CTX.job_id}</a>`;
    $("#rodape-menu").innerHTML = `Controle: <span class="mono">${esc(CTX.schema_ctrl)}</span><br>Modelo: ${esc(CTX.endpoint)}`;
    api("/api/visao").then((v) => { PENDENTES = v.pendentes || 0; montarMenu((location.hash.slice(1) || "inicio").split("/")[0]); }).catch(() => {});
  } catch (e) { avisar(e.message, true); }
  window.addEventListener("hashchange", rota);
  rota();
  vigiarExecucao(); setInterval(vigiarExecucao, 5000);
})();
