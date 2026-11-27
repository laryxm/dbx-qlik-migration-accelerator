# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR QLIK · INSTALAÇÃO</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Instalar pelo workspace, sem linha de comando</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">Preencha os campos no topo da página e clique em <b>Run all</b>. Este notebook cria o job do acelerador, publica o app no Databricks Apps, prepara os schemas e dá as permissões ao app. Leva de 5 a 10 minutos. No fim, aparece o link do app.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Antes de começar
# MAGIC 1. Este notebook precisa estar dentro do repositório importado como **Git folder** (Workspace → Create → Git folder → URL do repositório). Ele usa as pastas `src/` e `app/` ao lado dele.
# MAGIC 2. Você precisa de: permissão para criar apps e jobs, **CREATE SCHEMA** no catálogo escolhido, um **SQL warehouse** e um **endpoint de foundation model** (Serving).
# MAGIC 3. Conecte o notebook a um compute **serverless**.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Campos da instalação (preencha no topo da página)
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

armazens = {f"{x.name} ({x.id})": x.id for x in w.warehouses.list()}
modelos = sorted(e.name for e in w.serving_endpoints.list()
                 if any(k in e.name for k in ("claude", "llama", "gpt", "gemini", "qwen", "mixtral", "dbrx")))
dbutils.widgets.text("catalogo", "", "1. Catálogo (precisa de CREATE SCHEMA)")
dbutils.widgets.dropdown("warehouse", next(iter(armazens), "nenhum"), list(armazens) or ["nenhum"], "2. SQL warehouse do app")
dbutils.widgets.dropdown("modelo", "databricks-claude-sonnet-5-5" if "databricks-claude-sonnet-5-5" in modelos else (modelos[0] if modelos else "nenhum"),
                         modelos or ["nenhum"], "3. Modelo de IA")
dbutils.widgets.text("prefixo_schema", "qlik_acel", "4. Prefixo dos schemas")
dbutils.widgets.text("app_nome", "qlik-acelerador", "5. Nome do app")

CATALOGO = dbutils.widgets.get("catalogo").strip()
WAREHOUSE_ID = armazens.get(dbutils.widgets.get("warehouse"))
MODELO = dbutils.widgets.get("modelo")
PREFIXO = dbutils.widgets.get("prefixo_schema").strip()
APP_NOME = dbutils.widgets.get("app_nome").strip()
assert CATALOGO, "Preencha o campo 1 (catálogo) no topo da página e rode de novo."
assert WAREHOUSE_ID, "Nenhum SQL warehouse disponível: crie um em SQL Warehouses e rode de novo."
assert MODELO != "nenhum", "Nenhum endpoint de foundation model encontrado em Serving."

caminho = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO = caminho.rsplit("/", 2)[0]          # .../<repositorio>/instalar/Instalar_pelo_workspace
print(f"Repositório: {REPO}\nCatálogo {CATALOGO} · prefixo {PREFIXO} · warehouse {WAREHOUSE_ID} · modelo {MODELO} · app {APP_NOME}")

# COMMAND ----------

# DBTITLE 1,1. Criar ou atualizar o job do acelerador
from databricks.sdk.service import jobs

NOME_JOB = f"{APP_NOME}-fluxo"          # um job por instalação: dá para ter mais de uma no mesmo workspace
PASSOS = ["01_preparar_ambiente", "02_simular_ambiente_qlik", "03_inventario_triagem", "04_linhagem",
          "05_conversao_ia", "06_publicar", "07_semantica_seguranca", "08_paridade"]
tarefas, anterior = [], None
for p in PASSOS:
    tarefas.append(jobs.Task(task_key=p, notebook_task=jobs.NotebookTask(notebook_path=f"{REPO}/src/{p}"),
                             depends_on=[jobs.TaskDependency(task_key=anterior)] if anterior else None))
    anterior = p
spec = dict(
    name=NOME_JOB, tasks=tarefas, max_concurrent_runs=1, tags={"gerado_por": "dbx-qlik-migration-accelerator"},
    description=f"Acelerador de migração Qlik para Databricks. Use pelo app {APP_NOME}.",
    parameters=[jobs.JobParameterDefinition(name=k, default=v) for k, v in {
        "catalogo": CATALOGO, "prefixo_schema": PREFIXO, "onda": "onda_1", "modo": "demo",
        "modelo": MODELO, "app_nome": APP_NOME, "execucao_id": "{{job.run_id}}"}.items()])
existente = list(w.jobs.list(name=NOME_JOB))
if existente:
    JOB_ID = existente[0].job_id
    w.jobs.reset(job_id=JOB_ID, new_settings=jobs.JobSettings(**spec))
else:
    JOB_ID = w.jobs.create(**spec).job_id
print(f"Job {NOME_JOB}: {JOB_ID}")

# COMMAND ----------

# DBTITLE 1,2. Criar o app e publicar o código
import time

recursos = [
    {"name": "sql-warehouse", "sql_warehouse": {"id": WAREHOUSE_ID, "permission": "CAN_USE"}},
    {"name": "job", "job": {"id": str(JOB_ID), "permission": "CAN_MANAGE_RUN"}},
    {"name": "serving-endpoint", "serving_endpoint": {"name": MODELO, "permission": "CAN_QUERY"}},
]
desc = "Acelerador de migração Qlik para Databricks: entrada de dados, execução, revisão do código gerado por IA, publicação e paridade."
try:
    w.api_client.do("GET", f"/api/2.0/apps/{APP_NOME}")
    w.api_client.do("PATCH", f"/api/2.0/apps/{APP_NOME}", body={"name": APP_NOME, "description": desc, "resources": recursos})
    print("App já existia: recursos atualizados.")
except Exception:
    w.api_client.do("POST", "/api/2.0/apps", body={"name": APP_NOME, "description": desc, "resources": recursos})
    print("App criado. Aguardando o compute do app ficar pronto...")

for _ in range(90):
    estado = (w.api_client.do("GET", f"/api/2.0/apps/{APP_NOME}").get("compute_status") or {}).get("state")
    if estado == "ACTIVE":
        break
    if estado in ("STOPPED",):
        w.api_client.do("POST", f"/api/2.0/apps/{APP_NOME}/start")
    time.sleep(10)
print(f"Compute do app: {estado}")

dep = w.api_client.do("POST", f"/api/2.0/apps/{APP_NOME}/deployments", body={"source_code_path": f"/Workspace{REPO}/app"})
for _ in range(90):
    st = w.api_client.do("GET", f"/api/2.0/apps/{APP_NOME}/deployments/{dep['deployment_id']}").get("status", {})
    if st.get("state") in ("SUCCEEDED", "FAILED"):
        break
    time.sleep(10)
print(f"Publicação do app: {st.get('state')} {st.get('message', '')}")
assert st.get("state") == "SUCCEEDED", "A publicação do app falhou. Veja os logs em Compute → Apps → o app → Logs."

# COMMAND ----------

# DBTITLE 1,3. Preparar o ambiente (passo 01: schemas, volume e permissões do app)
run = w.jobs.run_now(job_id=JOB_ID, only=["01_preparar_ambiente"])
print(f"Executando o passo 01: {w.config.host}/jobs/{JOB_ID}/runs/{run.run_id}")
while True:
    r = w.jobs.get_run(run.run_id)
    if r.state.life_cycle_state.value in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
        break
    time.sleep(10)
print(f"Passo 01: {r.state.result_state.value if r.state.result_state else r.state.life_cycle_state.value}")

# COMMAND ----------

# DBTITLE 1,Pronto
url = w.api_client.do("GET", f"/api/2.0/apps/{APP_NOME}").get("url")
displayHTML(f"""<div style="font-family:'DM Sans',Arial,sans-serif;border-left:6px solid #2B8A3E;background:#EBFBEE;border-radius:0 8px 8px 0;padding:18px 22px;">
<div style="color:#2B8A3E;font-weight:700;letter-spacing:1px;font-size:13px;">INSTALAÇÃO CONCLUÍDA</div>
<div style="font-size:18px;margin:6px 0;"><a href="{url}" target="_blank">Abrir o acelerador</a></div>
<div style="color:#495057;font-size:14px;">Comece pela aba <b>Como usar</b>. No modo Demonstração, vá em <b>Executar</b> e rode tudo.<br>
Se o passo 01 avisou que alguma permissão não pôde ser concedida, peça ao dono do catálogo para rodar os comandos listados na saída do job.</div></div>""")
