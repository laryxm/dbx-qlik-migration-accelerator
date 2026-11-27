"""App do Acelerador de Migração Qlik para Databricks.

O app é a porta de entrada para quem está migrando: confere pré-requisitos, recebe os arquivos
exportados do Qlik, ajusta parâmetros, dispara o job do acelerador e mostra cada resultado
(triagem, linhagem, conversão, paridade, segurança) a partir das tabelas de controle.
Revisões humanas (aprovar ou rejeitar código gerado) são gravadas com o e-mail de quem decidiu.

O motor continua sendo o job de notebooks: o app só orquestra e lê. Sem o app, tudo roda pelo job.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")
JOB_ID = int(os.environ.get("JOB_ID", "0") or 0)
ENDPOINT = os.environ.get("SERVING_ENDPOINT", "")

PASTAS_PADRAO = {"inventario": "inventario", "scripts": "scripts", "apps": "apps", "qvd": "qvd"}
PASSOS = ["01_preparar_ambiente", "02_simular_ambiente_qlik", "03_inventario_triagem", "04_linhagem",
          "05_conversao_ia", "06_publicar", "07_semantica_seguranca", "08_paridade"]

w = WorkspaceClient()


def _parametros_do_job() -> dict:
    """Catálogo e prefixo vêm dos parâmetros do job do acelerador: uma única fonte de configuração da instalação."""
    try:
        return {p.name: p.default for p in (w.jobs.get(JOB_ID).settings.parameters or [])}
    except Exception:
        return {}


_PJ = _parametros_do_job()
CATALOGO = os.environ.get("CATALOGO") or _PJ.get("catalogo") or "main"
PREFIXO = os.environ.get("PREFIXO_SCHEMA") or _PJ.get("prefixo_schema") or "qlik_acel"
C = f"{CATALOGO}.{PREFIXO}_ctrl"
VOL = f"/Volumes/{CATALOGO}/{PREFIXO}_ctrl/arquivos"
app = FastAPI(title="Acelerador Qlik para Databricks")


@app.middleware("http")
async def sem_cache_na_interface(request: Request, call_next):
    """index.html, app.js e style.css são revalidados a cada carga: depois de um deploy, a versão nova aparece sem Cmd+Shift+R."""
    resposta = await call_next(request)
    if not request.url.path.startswith("/api/"):
        resposta.headers["Cache-Control"] = "no-cache"
    return resposta


# --------------------------------------------------------------------------- utilitários
def usuario(request: Request) -> str:
    return request.headers.get("X-Forwarded-Email") or request.headers.get("X-Forwarded-Preferred-Username") or "desconhecido"


def sql(q: str, params: dict | None = None) -> list[dict]:
    """Executa SQL no warehouse e devolve lista de dicts."""
    p = [StatementParameterListItem(name=k, value=None if v is None else str(v)) for k, v in (params or {}).items()]
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=q, parameters=p or None,
                                                wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise HTTPException(500, f"SQL falhou: {r.status.error.message if r.status.error else r.status.state}")
    if not r.manifest or not r.manifest.schema or not r.manifest.schema.columns:
        return []
    cols = [(c.name, c.type_name.value if c.type_name else "STRING") for c in r.manifest.schema.columns]
    linhas = list(r.result.data_array or []) if r.result else []
    chunk = r.result.next_chunk_index if r.result else None
    while chunk:
        parte = w.statement_execution.get_statement_result_chunk_n(r.statement_id, chunk)
        linhas += parte.data_array or []
        chunk = parte.next_chunk_index
    out = []
    for row in linhas:
        d = {}
        for (nome, tipo), v in zip(cols, row):
            if v is not None and tipo in ("INT", "LONG", "SHORT", "BYTE"):
                v = int(v)
            elif v is not None and tipo in ("DOUBLE", "FLOAT", "DECIMAL"):
                v = float(v)
            elif v is not None and tipo == "BOOLEAN":
                v = v == "true"
            elif v is not None and tipo in ("ARRAY", "MAP", "STRUCT"):
                try:
                    v = json.loads(v)
                except Exception:
                    pass
            d[nome] = v
        out.append(d)
    return out


def sql_seguro(q: str, params: dict | None = None) -> list[dict]:
    """Igual a sql(), mas devolve [] quando a tabela ainda não existe (passo não executado)."""
    try:
        return sql(q, params)
    except HTTPException as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e.detail) or "does not exist" in str(e.detail):
            return []
        raise


def paralelo(consultas: dict) -> dict:
    """Roda várias consultas ao mesmo tempo: as telas abrem em ~1 consulta de latência."""
    from concurrent.futures import ThreadPoolExecutor
    itens = {k: (v if isinstance(v, tuple) else (v, None)) for k, v in consultas.items()}
    with ThreadPoolExecutor(max_workers=max(1, len(itens))) as ex:
        futuros = {k: ex.submit(sql_seguro, q, p) for k, (q, p) in itens.items()}
        return {k: f.result() for k, f in futuros.items()}


def ler_arquivo(caminho: str) -> bytes | None:
    try:
        return w.files.download(caminho).contents.read()
    except Exception:
        return None


def gravar_arquivo(caminho: str, dados: bytes):
    w.files.upload(caminho, io.BytesIO(dados), overwrite=True)


def config_efetiva() -> dict:
    bruto = ler_arquivo(f"{VOL}/config/efetiva.json")
    return json.loads(bruto) if bruto else {}


def dirs() -> dict:
    """Pastas de entrada: vêm do grupo 'entrada' da configuração (volume do acelerador por padrão)."""
    ent = config_efetiva().get("entrada", {})
    base = (ent.get("volume") or VOL).rstrip("/")
    return {k: f"{base}/{ent.get('pasta_' + k) or v}" for k, v in PASTAS_PADRAO.items()}


def registrar_evento(request: Request, passo: str, mensagem: str, objeto=None, tipo_objeto=None, detalhe=None, nivel="info"):
    sql(f"""INSERT INTO {C}.evento VALUES (current_timestamp(), :exec, :onda, :passo, :nivel, :tipo, :obj, :msg, :det, :usr)""",
        {"exec": "app", "onda": None, "passo": passo, "nivel": nivel, "tipo": tipo_objeto, "obj": objeto, "msg": mensagem,
         "det": json.dumps(detalhe, ensure_ascii=False, default=str) if detalhe is not None else None, "usr": usuario(request)})


# --------------------------------------------------------------------------- contexto e visão geral
@app.get("/api/contexto")
def contexto(request: Request):
    host = w.config.host.rstrip("/")
    return {"catalogo": CATALOGO, "prefixo": PREFIXO, "schema_ctrl": C, "volume": VOL, "job_id": JOB_ID,
            "job_url": f"{host}/jobs/{JOB_ID}", "host": host, "usuario": usuario(request), "endpoint": ENDPOINT,
            "passos": PASSOS, "entrada": dirs(), "modo": (config_efetiva().get("ambiente") or {}).get("modo", "demo")}


@app.get("/api/visao")
def visao():
    consultas = {
        "estados": f"SELECT tipo, estado, count(*) AS n FROM {C}.unidade_status GROUP BY ALL ORDER BY tipo, n DESC",
        "revisao": f"SELECT status_revisao, count(*) AS n, round(avg(nota), 0) AS nota_media FROM {C}.conv_objeto GROUP BY ALL",
        "paridade": f"SELECT nivel, sum(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok, count(*) AS total FROM {C}.par_resultado WHERE status <> 'aguardando_publicacao' GROUP BY ALL ORDER BY nivel",
        "ultima_execucao": f"""SELECT passo, status, round(segundos) AS segundos, fim, usuario FROM {C}.execucao_passo
                               WHERE execucao_id = (SELECT execucao_id FROM {C}.execucao_passo ORDER BY fim DESC LIMIT 1) ORDER BY inicio""",
        "tokens": f"""SELECT coalesce(sum(tokens_entrada), 0) AS entrada, coalesce(sum(tokens_saida), 0) AS saida,
                             count(*) AS chamadas, coalesce(sum(CAST(do_cache AS INT)), 0) AS do_cache FROM {C}.conv_auditoria_llm""",
        "triagem": f"SELECT decisao, count(*) AS unidades, round(sum(gb), 2) AS gb FROM {C}.inv_unidade_migracao GROUP BY ALL ORDER BY unidades DESC",
        "pendentes": f"""SELECT count(*) AS n FROM {C}.conv_objeto c LEFT JOIN {C}.unidade_status u ON u.tipo = 'objeto' AND u.unidade = c.nome
                         WHERE c.status_revisao IN ('revisao_humana', 'bloqueado') AND coalesce(u.estado, '') NOT IN ('aprovado', 'publicado', 'paridade_ok', 'paridade_divergente')""",
    }
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=len(consultas)) as ex:
        r = dict(zip(consultas, ex.map(sql_seguro, consultas.values())))
    r["tokens"] = r["tokens"][0] if r["tokens"] else None
    r["pendentes"] = r["pendentes"][0]["n"] if r["pendentes"] else 0
    return r


# --------------------------------------------------------------------------- pré-requisitos
@app.get("/api/prereqs")
def prereqs():
    checks = []

    def check(nome, func, como):
        try:
            detalhe = func()
            checks.append({"nome": nome, "ok": True, "detalhe": detalhe, "como_resolver": None})
        except Exception as e:
            checks.append({"nome": nome, "ok": False, "detalhe": str(e)[:300], "como_resolver": como})

    check("SQL warehouse acessível", lambda: (sql("SELECT 1 AS ok"), f"warehouse {WAREHOUSE_ID}")[1],
          "Adicionar um SQL warehouse como recurso do app (permissão Can use).")
    check("Catálogo acessível", lambda: f"{len(sql(f'SHOW SCHEMAS IN {CATALOGO}'))} schemas visíveis em {CATALOGO}",
          f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `<service principal do app>`")
    check("Schema de controle criado", lambda: (sql(f"DESCRIBE SCHEMA {C}"), C)[1],
          "Executar o passo 01 (tela Executar) ou criar o schema e dar USE SCHEMA, SELECT e MODIFY ao app.")
    check("Volume de arquivos", lambda: f"{sum(1 for _ in w.files.list_directory_contents(VOL))} pastas em {VOL}",
          f"Executar o passo 01 e dar READ VOLUME e WRITE VOLUME em {C}.arquivos ao app.")
    ent_vol = (config_efetiva().get("entrada", {}) or {}).get("volume")
    if ent_vol:
        check("Volume de entrada do cliente", lambda: f"{sum(1 for _ in w.files.list_directory_contents(ent_vol))} itens em {ent_vol}",
              f"Dar READ VOLUME (e WRITE VOLUME, se for enviar pelo app) em {ent_vol} ao service principal do app, e USE CATALOG / USE SCHEMA nos pais.")
    check("Modelo de IA disponível", lambda: f"{ENDPOINT}: {w.serving_endpoints.get(ENDPOINT).state.ready.value}",
          "Adicionar o endpoint de foundation model como recurso do app (Can query) e conferir se está READY.")
    check("Job do acelerador", lambda: f"{w.jobs.get(JOB_ID).settings.name} ({JOB_ID})",
          "Adicionar o job como recurso do app com permissão Can manage run.")
    check("Configuração efetiva", lambda: "lida de config/efetiva.json" if ler_arquivo(f"{VOL}/config/efetiva.json") else (_ for _ in ()).throw(Exception("arquivo ainda não gerado")),
          "Executar o passo 01: ele grava a configuração efetiva no volume.")
    return checks


# --------------------------------------------------------------------------- configuração
@app.get("/api/config")
def get_config():
    efetiva = ler_arquivo(f"{VOL}/config/efetiva.json")
    sobrescrita = ler_arquivo(f"{VOL}/config/acelerador.json")
    return {"efetiva": json.loads(efetiva) if efetiva else None, "sobrescrita": json.loads(sobrescrita) if sobrescrita else None}


@app.put("/api/config")
async def put_config(request: Request):
    """Grava só o que mudou em relação à configuração efetiva (o resto continua no padrão do pacote)."""
    nova = await request.json()
    efetiva = config_efetiva()
    bruto = ler_arquivo(f"{VOL}/config/acelerador.json")
    sobrescrita = json.loads(bruto) if bruto else {}
    mudancas = {}
    for grupo, valores in nova.items():
        if not isinstance(valores, dict):
            continue
        for chave, valor in valores.items():
            if efetiva.get(grupo, {}).get(chave) != valor:
                mudancas.setdefault(grupo, {})[chave] = valor
                sobrescrita.setdefault(grupo, {})[chave] = valor
                efetiva.setdefault(grupo, {})[chave] = valor
    if not mudancas:
        return {"ok": True, "mudancas": {}}
    gravar_arquivo(f"{VOL}/config/acelerador.json", json.dumps(sobrescrita, ensure_ascii=False, indent=2).encode("utf-8"))
    # efetiva passa a refletir a mudança até a próxima execução do passo 01
    gravar_arquivo(f"{VOL}/config/efetiva.json", json.dumps(efetiva, ensure_ascii=False, indent=2).encode("utf-8"))
    registrar_evento(request, "app_configuracao", f"configuração alterada: {', '.join(f'{g}.{k}' for g, v in mudancas.items() for k in v)}",
                     detalhe={"mudancas": mudancas})
    return {"ok": True, "mudancas": mudancas}


# --------------------------------------------------------------------------- coleta de arquivos
def _listar(d):
    try:
        return [{"nome": f.name, "bytes": f.file_size, "modificado": f.last_modified}
                for f in w.files.list_directory_contents(d) if not f.is_directory]
    except Exception:
        return []


@app.get("/api/coleta")
def coleta():
    DIRS = dirs()
    arquivos = {k: _listar(v) for k, v in DIRS.items()}
    scripts = {a["nome"][:-4] for a in arquivos["scripts"] if a["nome"].lower().endswith(".qvs")}
    qvds = [a for a in arquivos["qvd"] if a["nome"].lower().endswith(".qvd")]
    medidas = [a for a in arquivos["apps"] if a["nome"].endswith("_medidas.json")]
    validacoes = [
        {"item": "Inventário de QVDs (.csv)", "ok": any(a["nome"].lower().endswith(".csv") for a in arquivos["inventario"]),
         "dica": "Colunas: nome, espaco, linhas, tamanho_bytes, atualizado_em. Lista de arquivos de dados exportada do Qlik (Qlik Cloud ou QMC)."},
        {"item": "Load scripts (.qvs)", "ok": bool(scripts), "dica": f"{len(scripts)} script(s). Exporte com qlik-cli ou copie o LoadScript de cada app/QVW."},
        {"item": "Cabeçalhos ou QVDs (.qvd)", "ok": bool(qvds), "dica": f"{len(qvds)} arquivo(s). Só o cabeçalho XML já basta para linhagem e contagem; o QVD completo é usado como baseline da paridade."},
        {"item": "Medidas mestras dos apps (_medidas.json)", "ok": bool(medidas), "dica": f"{len(medidas)} app(s). Necessário para metric views e paridade N4."},
    ]
    for m in medidas:
        conteudo = ler_arquivo(f"{DIRS['apps']}/{m['nome']}")
        try:
            j = json.loads(conteudo)
            app_nome = j.get("app")
            hcs = [h["arquivo"] for h in j.get("hypercubes_exportados", [])]
            faltam = [h for h in hcs if h not in {a["nome"] for a in arquivos["apps"]}]
            validacoes.append({"item": f"{m['nome']}", "ok": app_nome in scripts and not faltam,
                               "dica": ("script do app encontrado" if app_nome in scripts else f"falta o script {app_nome}.qvs")
                                       + (f" · hypercubes ausentes: {', '.join(faltam)}" if faltam else f" · {len(hcs)} hypercube(s) para N4")})
        except Exception as e:
            validacoes.append({"item": m["nome"], "ok": False, "dica": f"JSON inválido: {e}"})
    return {"arquivos": arquivos, "validacoes": validacoes, "pastas": DIRS}


def _destino(nome: str) -> str | None:
    n = nome.lower()
    if n.endswith(".qvs"):
        return "scripts"
    if n.endswith(".qvd"):
        return "qvd"
    if n.endswith("_medidas.json") or n.startswith("hypercube"):
        return "apps"
    if n.endswith(".csv") and "invent" in n:
        return "inventario"
    if n.endswith(".csv"):
        return "apps"
    return None


@app.post("/api/coleta/upload")
async def upload(request: Request, arquivo: UploadFile):
    DIRS = dirs()
    dados = await arquivo.read()
    gravados, ignorados = [], []
    if arquivo.filename.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(dados)) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                nome = os.path.basename(info.filename)
                destino = _destino(nome)
                if not destino or nome.startswith("."):
                    ignorados.append(info.filename)
                    continue
                gravar_arquivo(f"{DIRS[destino]}/{nome}", z.read(info))
                gravados.append(f"{destino}/{nome}")
    else:
        destino = _destino(arquivo.filename)
        if not destino:
            raise HTTPException(400, f"Tipo de arquivo não reconhecido: {arquivo.filename}")
        gravar_arquivo(f"{DIRS[destino]}/{arquivo.filename}", dados)
        gravados.append(f"{destino}/{arquivo.filename}")
    registrar_evento(request, "app_coleta", f"{len(gravados)} arquivo(s) enviados", detalhe={"gravados": gravados, "ignorados": ignorados})
    return {"gravados": gravados, "ignorados": ignorados}


# --------------------------------------------------------------------------- execução
class Execucao(BaseModel):
    passos: list[str] | None = None
    onda: str | None = None
    modo: str | None = None


def _estado_tarefa(t):
    st = t.state
    if not st:
        return None
    if st.result_state:
        return st.result_state.value
    return st.life_cycle_state.value if st.life_cycle_state else None


def _resumo_run(r):
    tarefas = sorted([{"passo": t.task_key, "estado": _estado_tarefa(t), "segundos": round((t.execution_duration or 0) / 1000),
                       "inicio": t.start_time} for t in (r.tasks or [])], key=lambda x: x["passo"])
    selecionadas = [t for t in tarefas if t["estado"] not in ("EXCLUDED", "DISABLED")]
    concluidas = [t for t in selecionadas if t["estado"] in ("SUCCESS", "FAILED", "SKIPPED", "UPSTREAM_FAILED", "CANCELED", "TIMEDOUT")]
    atual = next((t for t in selecionadas if t["estado"] in ("RUNNING", "PENDING", "QUEUED", "BLOCKED", "TERMINATING")), None)
    ciclo = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else None
    return {"run_id": r.run_id, "inicio": r.start_time, "ciclo": ciclo,
            "estado": r.state.result_state.value if r.state and r.state.result_state else ciclo,
            "tarefas": tarefas, "total": len(selecionadas), "concluidas": len(concluidas),
            "atual": atual["passo"] if atual else None, "inicio_atual": atual["inicio"] if atual else None,
            "url": f"{w.config.host.rstrip('/')}/jobs/{JOB_ID}/runs/{r.run_id}"}


def _ativas():
    return [_resumo_run(w.jobs.get_run(r.run_id)) for r in w.jobs.list_runs(job_id=JOB_ID, active_only=True, limit=10)]


@app.get("/api/execucao_ativa")
def execucao_ativa():
    """Execuções rodando ou na fila, com passo atual e progresso. O app consulta a cada poucos segundos."""
    ativas = _ativas()
    rodando = [a for a in ativas if a["ciclo"] not in ("QUEUED", "PENDING") or a["atual"]]
    return {"ativas": ativas, "rodando": rodando[0] if rodando else (ativas[0] if ativas else None),
            "na_fila": [a for a in ativas if a["ciclo"] == "QUEUED"]}


@app.post("/api/execucoes/{run_id}/cancelar")
def cancelar(request: Request, run_id: int):
    w.jobs.cancel_run(run_id)
    registrar_evento(request, "app_execucao", f"execução {run_id} cancelada pelo app", detalhe={"run_id": run_id})
    return {"ok": True}


@app.post("/api/executar")
def executar(request: Request, e: Execucao):
    ativas = _ativas()
    if ativas:
        a = ativas[0]
        raise HTTPException(409, f"Já existe uma execução em andamento ({a['run_id']}). Acompanhe por aqui ou cancele antes de iniciar outra.")
    passos = [p for p in (e.passos or PASSOS) if p in PASSOS]
    params = {k: v for k, v in {"catalogo": CATALOGO, "prefixo_schema": PREFIXO, "onda": e.onda, "modo": e.modo}.items() if v}
    run = w.jobs.run_now(job_id=JOB_ID, job_parameters=params, only=passos if len(passos) < len(PASSOS) else None)
    registrar_evento(request, "app_execucao", f"execução disparada: {', '.join(passos)}", detalhe={"run_id": run.run_id, "parametros": params})
    return {"run_id": run.run_id, "url": f"{w.config.host.rstrip('/')}/jobs/{JOB_ID}/runs/{run.run_id}"}


@app.get("/api/execucoes")
def execucoes():
    host = w.config.host.rstrip("/")
    out = []
    for r in w.jobs.list_runs(job_id=JOB_ID, limit=8, expand_tasks=True):
        tarefas = sorted([{"passo": t.task_key, "estado": (t.state.result_state.value if t.state and t.state.result_state else
                                                            (t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else None)),
                           "segundos": round((t.execution_duration or 0) / 1000)} for t in (r.tasks or [])], key=lambda x: x["passo"])
        out.append({"run_id": r.run_id, "inicio": r.start_time, "fim": r.end_time,
                    "estado": r.state.result_state.value if r.state and r.state.result_state else (r.state.life_cycle_state.value if r.state else None),
                    "parametros": {p.name: (p.value if p.value is not None else p.default) for p in (r.job_parameters or [])},
                    "tarefas": tarefas, "url": f"{host}/jobs/{JOB_ID}/runs/{r.run_id}"})
    return out


# --------------------------------------------------------------------------- triagem e linhagem
@app.get("/api/triagem")
def triagem():
    return paralelo({
        "inventario": (f"SELECT count(*) AS arquivos, round(sum(qt_gb), 1) AS gb, round(sum(qt_linhas) / 1e6, 0) AS milhoes_linhas, count(DISTINCT espaco) AS espacos FROM {C}.inv_qvd"),
        "por_decisao": (f"SELECT decisao, count(*) AS arquivos, round(sum(qt_gb), 2) AS gb, count(DISTINCT familia) AS tabelas_logicas FROM {C}.inv_triagem GROUP BY ALL ORDER BY arquivos DESC"),
        "por_espaco": (f"""SELECT espaco, count(*) AS arquivos, round(sum(qt_gb), 2) AS gb,
                                    sum(CASE WHEN status_atividade = 'ativo' THEN 1 ELSE 0 END) AS ativos,
                                    sum(CASE WHEN fl_vazio THEN 1 ELSE 0 END) AS vazios,
                                    sum(CASE WHEN fl_duplicado THEN 1 ELSE 0 END) AS duplicados
                                    FROM {C}.inv_triagem GROUP BY ALL ORDER BY gb DESC"""),
        "top_migrar": (f"""SELECT espaco, familia, arquivos, gb, camada_hipotese, dias_sem_atualizar
                                    FROM {C}.inv_unidade_migracao WHERE decisao = 'migrar' ORDER BY gb DESC LIMIT 30"""),
    })


@app.get("/api/linhagem")
def linhagem():
    r = paralelo({
        "arestas": f"SELECT origem, tipo_origem, destino, tipo_destino FROM {C}.lin_aresta",
        "qvds": f"SELECT qvd, camada_destino, objeto_destino, script_produtor, profundidade FROM {C}.lin_qvd ORDER BY profundidade, qvd",
        "medidas": f"""SELECT app, medida_qlik, metric_view, medida_databricks, status_conversao,
                              concat_ws(', ', collect_set(tabela_gold)) AS tabelas_gold,
                              concat_ws(', ', collect_set(concat(tabela_origem, '.', coluna_origem))) AS colunas_origem
                       FROM {C}.lin_medida_completa GROUP BY ALL ORDER BY app, medida_qlik""",
        "scripts": f"SELECT script, papel, linhas, score, classe, size(alertas) AS alertas FROM {C}.lin_script ORDER BY score DESC",
        "alertas": f"SELECT script, tipo, detalhe FROM {C}.lin_alerta ORDER BY script, tipo",
    })
    arestas, qvds, medidas, scripts, alertas = r["arestas"], r["qvds"], r["medidas"], r["scripts"], r["alertas"]

    def no(x):
        return "n_" + "".join(ch if ch.isalnum() else "_" for ch in x)
    cores = {"bronze": "#E8C9A6", "silver": "#D5DCE0", "gold": "#F2DC8C"}
    linhas = ["flowchart LR"]
    for r in arestas:
        o = f'{no(r["origem"])}[("{r["origem"]}")]' if r["tipo_origem"] == "BANCO" else f'{no(r["origem"])}["{r["origem"]}"]'
        d = f'{no(r["destino"])}{{{{"{r["destino"]}"}}}}' if r["tipo_destino"] == "APP" else f'{no(r["destino"])}["{r["destino"]}"]'
        linhas.append(f"  {o} --> {d}")
    for q in qvds:
        linhas.append(f"  style {no(q['qvd'])} fill:{cores.get(q['camada_destino'], '#FFFFFF')},color:#1B3139")
    return {"mermaid": "\n".join(linhas) if arestas else None, "qvds": qvds, "medidas": medidas, "scripts": scripts, "alertas": alertas}


# --------------------------------------------------------------------------- conversão e revisão
@app.get("/api/conversao")
def conversao():
    objetos = sql_seguro(f"""
        SELECT c.nome, c.script, c.camada, c.origem_qvd, c.status, c.nota, c.status_revisao, c.motivos, c.tentativas,
               c.contagem_qvd, c.contagem_obtida, u.estado AS estado_atual, a.decisao AS ultima_decisao, a.usuario AS decidido_por, a.ts AS decidido_em
        FROM {C}.conv_objeto c
        LEFT JOIN {C}.unidade_status u ON u.tipo = 'objeto' AND u.unidade = c.nome
        LEFT JOIN (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY objeto ORDER BY ts DESC) rn FROM {C}.aprovacao) WHERE rn = 1) a
               ON a.objeto = c.nome
        ORDER BY CASE c.status_revisao WHEN 'bloqueado' THEN 0 WHEN 'revisao_humana' THEN 1 ELSE 2 END, c.nota, c.nome""")
    ingestao = sql_seguro(f"SELECT * FROM {C}.conv_ingestao ORDER BY qvd")
    return {"objetos": objetos, "ingestao": ingestao}


@app.get("/api/conversao/{nome}")
def conversao_detalhe(nome: str):
    o = sql(f"SELECT * FROM {C}.conv_objeto WHERE nome = :n", {"n": nome})
    if not o:
        raise HTTPException(404, "objeto não encontrado")
    o = o[0]
    obj = json.loads(o.pop("objeto_json") or "{}")
    qvs = ler_arquivo(f"{dirs()['scripts']}/{o['script']}.qvs")
    extra = paralelo({
        "premissas": (f"SELECT tipo, texto FROM {C}.conv_premissa WHERE script = :s", {"s": o["script"]}),
        "alertas": (f"SELECT tipo, detalhe FROM {C}.lin_alerta WHERE script = :s", {"s": o["script"]}),
        "historico": (f"SELECT ts, decisao, comentario, usuario, nota_no_momento FROM {C}.aprovacao WHERE objeto = :n ORDER BY ts DESC", {"n": nome}),
        "estado": (f"SELECT estado, atualizado_por, ts_atualizado FROM {C}.unidade_status WHERE tipo = 'objeto' AND unidade = :n", {"n": nome}),
        "llm": (f"""SELECT tentativa, segundos, tokens_entrada, tokens_saida, do_cache, ts FROM {C}.conv_auditoria_llm
                    WHERE script = :s ORDER BY ts DESC LIMIT 6""", {"s": o["script"]}),
    })
    return {**o, **extra,
            "colunas": obj.get("colunas", []), "expectativas": obj.get("expectativas", []), "cluster_by": obj.get("cluster_by", []),
            "script_qlik": qvs.decode("utf-8", errors="replace") if qvs else None}


class Decisao(BaseModel):
    decisao: str
    comentario: str | None = None


@app.post("/api/conversao/{nome}/decisao")
def decidir(request: Request, nome: str, d: Decisao):
    if d.decisao not in ("aprovar", "rejeitar"):
        raise HTTPException(400, "decisao deve ser aprovar ou rejeitar")
    if d.decisao == "rejeitar" and not (d.comentario or "").strip():
        raise HTTPException(400, "Ao rejeitar, escreva a orientação: ela volta para o modelo na próxima conversão.")
    o = sql(f"SELECT script, nota, sql_hash, status FROM {C}.conv_objeto WHERE nome = :n", {"n": nome})
    if not o:
        raise HTTPException(404, "objeto não encontrado")
    o = o[0]
    if d.decisao == "aprovar" and o["status"] != "ok":
        raise HTTPException(400, "Objeto não passou na validação automática; só é possível rejeitar com orientação.")
    u = usuario(request)
    sql(f"INSERT INTO {C}.aprovacao VALUES (current_timestamp(), :n, :s, :d, :c, :u, :nota, :h)",
        {"n": nome, "s": o["script"], "d": d.decisao, "c": d.comentario, "u": u, "nota": o["nota"], "h": o["sql_hash"]})
    estado = "aprovado" if d.decisao == "aprovar" else "rejeitado"
    sql(f"""MERGE INTO {C}.unidade_status t
            USING (SELECT 'objeto' AS tipo, :n AS unidade, CAST(NULL AS STRING) AS onda, :e AS estado, :det AS detalhe,
                          current_timestamp() AS ts_atualizado, :u AS atualizado_por, 'app' AS execucao_id) s
            ON t.tipo = s.tipo AND t.unidade = s.unidade
            WHEN MATCHED THEN UPDATE SET estado = s.estado, detalhe = s.detalhe, ts_atualizado = s.ts_atualizado,
                                         atualizado_por = s.atualizado_por, execucao_id = s.execucao_id
            WHEN NOT MATCHED THEN INSERT *""",
        {"n": nome, "e": estado, "det": json.dumps({"comentario": d.comentario}, ensure_ascii=False), "u": u})
    registrar_evento(request, "app_revisao", f"{nome}: {d.decisao}", objeto=nome, tipo_objeto="objeto",
                     detalhe={"comentario": d.comentario, "nota": o["nota"]})
    return {"ok": True, "estado": estado}


# --------------------------------------------------------------------------- paridade, segurança, diário
@app.get("/api/paridade")
def paridade():
    return paralelo({
        "resultado": (f"SELECT objeto, nivel, teste, grupos, divergencias, esperado, obtido, status, detalhe, ts_execucao FROM {C}.par_resultado ORDER BY objeto, nivel"),
        "historico": (f"""SELECT ts_execucao, count(*) AS testes, sum(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok
                                   FROM {C}.par_historico GROUP BY ALL ORDER BY ts_execucao DESC LIMIT 20"""),
    })


@app.get("/api/semantica")
def semantica():
    return paralelo({
        "medidas": (f"SELECT app, qId, titulo, metric_view, medida, status, observacao, teste FROM {C}.sem_medida ORDER BY app, qId"),
        "perfis": (f"SELECT * FROM {C}.gov_teste_perfil ORDER BY usuario_qlik"),
        "acesso": (f"SELECT grupo_idp, usuario_qlik, concat_ws(', ', sort_array(collect_set(valor_liberado))) AS valores FROM {C}.gov_acesso_regional_efetivo GROUP BY ALL ORDER BY grupo_idp"),
        "glossario": (f"SELECT app, termo, tipo, definicao, formula_qlik, objeto, status FROM {C}.doc_glossario ORDER BY app, tipo, termo"),
    })


@app.get("/api/eventos")
def eventos(limite: int = 200, passo: str | None = None):
    filtro = "WHERE passo = :p" if passo else ""
    return sql_seguro(f"""SELECT ts, execucao_id, passo, nivel, tipo_objeto, objeto, mensagem, usuario FROM {C}.evento {filtro}
                          ORDER BY ts DESC LIMIT {int(limite)}""", {"p": passo} if passo else None)


@app.get("/api/unidades")
def unidades(tipo: str | None = None):
    filtro = "WHERE tipo = :t" if tipo else ""
    return sql_seguro(f"SELECT tipo, unidade, estado, detalhe, ts_atualizado, atualizado_por FROM {C}.unidade_status {filtro} ORDER BY tipo, unidade",
                      {"t": tipo} if tipo else None)


_WSID = None


def workspace_id():
    global _WSID
    if _WSID is None:
        try:
            _WSID = w.get_workspace_id()
        except Exception:
            _WSID = ""
    return _WSID


def link(caminho: str) -> str:
    host, o = w.config.host.rstrip("/"), workspace_id()
    sep = "&" if "?" in caminho else "?"
    return f"{host}{caminho}{sep}o={o}" if o else f"{host}{caminho}"


@app.get("/api/publicacao")
def publicacao():
    r = paralelo({
        "pipeline": f"SELECT pipeline_id, update_id, estado, objetos, ts FROM {C}.conv_pipeline",
        "tabelas": f"""SELECT table_schema, table_name, table_type, comment FROM {CATALOGO}.information_schema.tables
                       WHERE table_schema IN ('{PREFIXO}_silver', '{PREFIXO}_gold')
                         AND left(table_name, 2) <> '__' AND left(table_name, 10) <> 'event_log_'
                       ORDER BY table_schema DESC, table_name""",
        "bundle": f"""SELECT ts, detalhe FROM {C}.evento WHERE passo = '06_publicar' AND mensagem LIKE 'bundle gerado%' ORDER BY ts DESC LIMIT 1""",
    })
    p = r["pipeline"][0] if r["pipeline"] else None
    if p and p.get("pipeline_id") and p["pipeline_id"] != "None":
        p["url"] = link(f"/pipelines/{p['pipeline_id']}")
        if p.get("update_id") and p["update_id"] != "None":
            p["url_execucao"] = link(f"/pipelines/{p['pipeline_id']}/updates/{p['update_id']}")
    else:
        p = None
    for t in r["tabelas"]:
        t["tipo"] = {"METRIC_VIEW": "metric view", "MATERIALIZED_VIEW": "materialized view", "MANAGED": "tabela",
                     "VIEW": "view", "STREAMING_TABLE": "streaming table"}.get(t["table_type"], str(t["table_type"]).lower())
        t["url"] = link(f"/explore/data/{CATALOGO}/{t['table_schema']}/{t['table_name']}")
    return {"pipeline": p, "tabelas": r["tabelas"], "bundle": r["bundle"][0] if r["bundle"] else None,
            "job_url": link(f"/jobs/{JOB_ID}"), "schema_gold_url": link(f"/explore/data/{CATALOGO}/{PREFIXO}_gold"),
            "volume_url": link(f"/explore/data/volumes/{CATALOGO}/{PREFIXO}_ctrl/arquivos")}


@app.get("/api/camadas")
def camadas():
    """Mapa de onde cada camada da migração vive no Unity Catalog, com links para o Catalog Explorer."""
    amb = config_efetiva().get("ambiente", {}) or {}
    bronze = amb.get("schema_bronze") or f"{CATALOGO}.{PREFIXO}_bronze"
    cat_b, sch_b = bronze.split(".")
    schemas = {"bronze": (cat_b, sch_b), "silver": (CATALOGO, f"{PREFIXO}_silver"), "gold": (CATALOGO, f"{PREFIXO}_gold"),
               "controle": (CATALOGO, f"{PREFIXO}_ctrl"), "sandbox": (CATALOGO, f"{PREFIXO}_sandbox")}
    filtro = "left(table_name, 2) <> '__' AND left(table_name, 10) <> 'event_log_'"
    consultas = {k: (f"""SELECT table_name, table_type, comment FROM {c}.information_schema.tables
                         WHERE table_schema = :s AND {filtro} ORDER BY table_name""", {"s": sc}) for k, (c, sc) in schemas.items()}
    r = paralelo(consultas)
    tipo = {"METRIC_VIEW": "metric view", "MATERIALIZED_VIEW": "materialized view", "MANAGED": "tabela", "VIEW": "view",
            "STREAMING_TABLE": "streaming table", "EXTERNAL": "tabela externa"}
    out = {}
    for k, (c, sc) in schemas.items():
        tabs = [{"nome": t["table_name"], "tipo": tipo.get(t["table_type"], str(t["table_type"]).lower()), "descricao": t["comment"],
                 "url": link(f"/explore/data/{c}/{sc}/{t['table_name']}")} for t in r[k]]
        out[k] = {"catalogo": c, "schema": sc, "fqn": f"{c}.{sc}", "url": link(f"/explore/data/{c}/{sc}"), "tabelas": tabs}
    # metric views ficam no gold; separadas para o mapa
    out["metric_views"] = {**out["gold"], "tabelas": [t for t in out["gold"]["tabelas"] if t["tipo"] == "metric view"]}
    out["gold"] = {**out["gold"], "tabelas": [t for t in out["gold"]["tabelas"] if t["tipo"] != "metric view"]}
    # entrada (raw): arquivos no volume
    d = dirs()
    ent = {k: len(_listar(v)) for k, v in d.items()}
    vol = (config_efetiva().get("entrada", {}) or {}).get("volume") or VOL
    partes = vol.strip("/").split("/")   # Volumes/cat/sch/vol/...
    out["entrada"] = {"volume": vol, "pastas": d, "arquivos": ent,
                      "url": link(f"/explore/data/volumes/{partes[1]}/{partes[2]}/{partes[3]}") if len(partes) >= 4 else None}
    out["modo"] = amb.get("modo", "demo")
    out["pipeline"] = (sql_seguro(f"SELECT pipeline_id FROM {C}.conv_pipeline") or [{}])[0].get("pipeline_id")
    if out["pipeline"] and out["pipeline"] != "None":
        out["pipeline_url"] = link(f"/pipelines/{out['pipeline']}")
    return out


@app.get("/api/download/bundle")
def download_bundle():
    nomes = [f for f in _listar(f"{VOL}/saida") if f["nome"].endswith("_bundle.zip")]
    if not nomes:
        raise HTTPException(404, "Bundle ainda não gerado. Execute o passo 06.")
    dados = ler_arquivo(f"{VOL}/saida/{nomes[0]['nome']}")
    return StreamingResponse(io.BytesIO(dados), media_type="application/zip",
                             headers={"Content-Disposition": f"attachment; filename={nomes[0]['nome']}"})


@app.get("/api/download/glossario")
def download_glossario():
    docs = [f for f in _listar(f"{VOL}/saida/docs") if f["nome"].endswith(".md")]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for d in docs:
            z.writestr(d["nome"], ler_arquivo(f"{VOL}/saida/docs/{d['nome']}") or b"")
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/zip", headers={"Content-Disposition": "attachment; filename=glossario.zip"})


@app.exception_handler(HTTPException)
async def erro_http(_, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"erro": exc.detail})


app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="static")
