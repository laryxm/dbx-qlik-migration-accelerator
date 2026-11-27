# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR QLIK · PASSO 6</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Publicar o que foi aprovado</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">Só entra no pipeline o objeto validado e aprovado, seja pela nota de confiança ou por uma pessoa no app. Objetos que dependem de algo ainda pendente esperam. O resultado é um Databricks Asset Bundle com alvos dev, hml e prd, e o pipeline do ambiente de demonstração é criado e executado com os mesmos arquivos.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:6px solid #0B7285;background:#F5F7F8;border-radius:0 8px 8px 0;padding:18px 22px;font-family:'DM Sans',Arial,sans-serif;color:#495057;font-size:15px;">
# MAGIC <div style="color:#0B7285;font-weight:700;letter-spacing:1px;font-size:13px;">PARA QUEM VEM DO QLIK</div>
# MAGIC <b>Materialized view</b> ocupa o lugar do QVD gerado por script: a tabela é recalculada pelo pipeline, com dependências resolvidas automaticamente, sem ordem de recarga manual.<br>
# MAGIC <b>Pipeline declarativo</b> é o equivalente à cadeia de tarefas de recarga, só que você declara o resultado e a plataforma cuida da ordem, do incremental e da qualidade.<br>
# MAGIC <b>Asset Bundle</b> é o pacote versionável (Git) que leva o mesmo código para dev, hml e prd mudando só os parâmetros.
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,O que pode ser publicado
_etapa = CTRL.etapa("06_publicar"); _etapa.__enter__()
import json, os, re, shutil, time
import qlik_conversor as qc
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
APROVACOES = CTRL.aprovacoes()
objetos = {r.nome: {**json.loads(r.objeto_json), "script": r.script, "status": r.status, "status_revisao": r.status_revisao,
                    "sql_hash": r.sql_hash, "nota": r.nota}
           for r in spark.table(f"{C}.conv_objeto").collect()}
parametros_pipeline = {r.parametro: r.valor for r in spark.table(f"{C}.conv_parametro").collect()}

def liberado(o):
    h = APROVACOES.get(o["nome"])
    if o["status"] != "ok":
        return False, "não passou na validação automática"
    if h and h[0] == "rejeitar":
        return False, f"rejeitado por {h[2]}: {h[1]}"
    if h and h[0] == "aprovar" and h[3] == o["sql_hash"]:
        return True, f"aprovado por {h[2]}"
    if o["status_revisao"] == "auto_aprovado":
        return True, f"nota {o['nota']:.0f} ≥ {REV['nota_minima_auto']}"
    return False, "aguardando revisão humana"

decisao = {n: liberado(o) for n, o in objetos.items()}
# dependências: quem lê um objeto não liberado também espera
mudou = True
while mudou:
    mudou = False
    for n, o in objetos.items():
        if not decisao[n][0]:
            continue
        deps = {d for _, d in re.findall(r"\$\{(silver|gold)\}\.(\w+)", o["sql"]) if d != n}
        pend = [d for d in deps if d in decisao and not decisao[d][0]]
        if pend:
            decisao[n] = (False, f"depende de objeto pendente: {', '.join(pend)}")
            mudou = True

publicar = [objetos[n] for n, (ok, _) in decisao.items() if ok]
for n, (ok, motivo) in sorted(decisao.items()):
    print(f"{'PUBLICA ' if ok else 'ESPERA  '} {n:32s} {motivo}")
    CTRL.evento("06_publicar", motivo, objeto=n, tipo_objeto="objeto", nivel="info" if ok else "aviso")

# COMMAND ----------

# DBTITLE 1,Gerar o bundle
def txt(v):
    return (v or "").replace("'", "’").replace("\n", " ").strip()

DIR_BUNDLE = f"{WS_DIR}/bundle_gerado"
if os.path.exists(DIR_BUNDLE):
    shutil.rmtree(DIR_BUNDLE)
os.makedirs(f"{DIR_BUNDLE}/src", exist_ok=True)
os.makedirs(f"{DIR_BUNDLE}/resources", exist_ok=True)

arquivos = []
for o in publicar:
    tipos = {f.name: f.dataType.simpleString() for f in spark.table(f"{CATALOG}.{SCH_SANDBOX}.{o['nome']}").schema.fields}
    doc = {c.get("nome"): c for c in o.get("colunas", [])}
    defs = [f"  {c} {t} COMMENT '{txt(doc.get(c, {}).get('descricao'))}'" for c, t in tipos.items()]
    defs += [f"  CONSTRAINT {e['nome']} EXPECT ({e['expressao']})" for e in o.get("expectativas", [])]
    cluster = f"CLUSTER BY ({', '.join(o['cluster_by'])})\n" if o.get("cluster_by") else ""
    origem = o.get("origem_qvd") or "mapping_load"
    corpo = (f"-- Gerado pelo acelerador Qlik a partir de {o['script']}.qvs · QVD de origem: {origem}\n"
             f"-- Liberação: {decisao[o['nome']][1]} · onda {ONDA}\n"
             "CREATE OR REFRESH MATERIALIZED VIEW ${" + o['camada'] + "}." + o['nome'] + " (\n" + ",\n".join(defs) + "\n)\n"
             f"{cluster}COMMENT '{txt(o.get('descricao'))}'\n"
             f"TBLPROPERTIES ('origem_qlik' = '{origem}', 'gerado_por' = 'acelerador_qlik', 'script_qlik' = '{o['script']}', 'onda' = '{ONDA}')\n"
             f"AS\n{o['sql']}\n")
    caminho = f"{DIR_BUNDLE}/src/{o['camada']}_{o['nome']}.sql"
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(corpo)
    arquivos.append(caminho)

NOME = PUB["nome_bundle"]
ambientes = {"demo": {"bronze": BRONZE_FQN, "silver": f"{CATALOG}.{SCH_SILVER}", "gold": f"{CATALOG}.{SCH_GOLD}"}, **PUB["ambientes"]}
params_extra = "".join(f"\n  {k}:\n    default: \"{v}\"" for k, v in parametros_pipeline.items())
alvos = []
for nome, a in ambientes.items():
    cat, sch = a["gold"].split(".")
    bloco = [f"  {nome}:"]
    if nome in ("demo", "dev"):
        bloco += ["    mode: development"] + (["    default: true"] if nome == "demo" else [])
    else:
        bloco += ["    mode: production", "    workspace:",
                  "      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}",
                  "    # run_as:", "    #   service_principal_name: <application-id do service principal>"]
    bloco += ["    variables:"] + [f"      {k}: {v}" for k, v in a.items()] + [f"      catalogo: {cat}", f"      schema: {sch}"]
    alvos.append("\n".join(bloco))
tags_yml = "".join(f"\n        {k}: \"{v}\"" for k, v in TAGS.items())
databricks_yml = f"""bundle:
  name: {NOME}

include:
  - resources/*.yml

variables:
  bronze:
    description: catálogo.schema das origens no bronze
  silver:
    description: catálogo.schema do silver
  gold:
    description: catálogo.schema do gold
  catalogo:
    description: catálogo padrão do pipeline
  schema:
    description: schema padrão do pipeline{params_extra}

targets:
""" + "\n".join(alvos) + "\n"
config_params = "".join(f"\n        {k}: ${{var.{k}}}" for k in parametros_pipeline)
pipeline_yml = f"""resources:
  pipelines:
    {NOME}:
      name: "[${{bundle.target}}] {NOME}"
      catalog: ${{var.catalogo}}
      schema: ${{var.schema}}
      serverless: true
      channel: CURRENT
      tags:{tags_yml}
      configuration:
        bronze: ${{var.bronze}}
        silver: ${{var.silver}}
        gold: ${{var.gold}}{config_params}
      libraries:
""" + "".join(f"        - file:\n            path: ../src/{os.path.basename(a)}\n" for a in arquivos) + f"""
  jobs:
    {NOME}_d1:
      name: "[${{bundle.target}}] {NOME} D-1"
      tags:{tags_yml}
      schedule:
        quartz_cron_expression: "{PUB['agendamento_cron']}"
        timezone_id: {PUB['fuso']}
        pause_status: PAUSED
      email_notifications:
        on_failure: ["{AMB['dono']}"]
      tasks:
        - task_key: atualizar_gold
          pipeline_task:
            pipeline_id: ${{resources.pipelines.{NOME}.id}}
          max_retries: 1
"""
with open(f"{DIR_BUNDLE}/databricks.yml", "w", encoding="utf-8") as f:
    f.write(databricks_yml)
with open(f"{DIR_BUNDLE}/resources/{NOME}.yml", "w", encoding="utf-8") as f:
    f.write(pipeline_yml)
with open(f"{DIR_BUNDLE}/README.md", "w", encoding="utf-8") as f:
    f.write(f"# {NOME}\n\nGerado pelo acelerador de migração Qlik (onda {ONDA}).\n\n"
            "```bash\ndatabricks bundle validate -t dev\ndatabricks bundle deploy -t dev\ndatabricks bundle run "
            f"{NOME}_d1 -t dev\n```\n\nPromoção: `-t hml` e `-t prd` (configure o service principal em `run_as`).\n")

# cópia zipada no volume, para download pelo app
os.makedirs(DIR_SAIDA, exist_ok=True)
zip_local = shutil.make_archive(f"/tmp/{NOME}_bundle", "zip", DIR_BUNDLE)
shutil.copyfile(zip_local, f"{DIR_SAIDA}/{NOME}_bundle.zip")
CTRL.evento("06_publicar", f"bundle gerado com {len(arquivos)} objetos", detalhe={"zip": f"{DIR_SAIDA}/{NOME}_bundle.zip", "objetos": [o["nome"] for o in publicar]})
print(f"{len(arquivos)} objetos no bundle · download: {DIR_SAIDA}/{NOME}_bundle.zip")

# COMMAND ----------

# DBTITLE 1,Criar ou atualizar o pipeline de demonstração e executar
if not arquivos:
    CTRL.evento("06_publicar", "nada liberado para publicar; pipeline não executado", nivel="aviso")
    estado, pipeline_id, update_id = "SEM_OBJETOS", None, None
else:
    config_demo = {**ambientes["demo"], **parametros_pipeline}
    corpo = {"name": PIPELINE_NAME, "catalog": CATALOG, "schema": SCH_GOLD, "serverless": True, "development": True,
             "channel": "CURRENT", "configuration": config_demo, "tags": TAGS,
             "libraries": [{"file": {"path": a.replace("/Workspace", "", 1)}} for a in arquivos]}
    existente = [p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'")]
    if existente:
        pipeline_id = existente[0].pipeline_id
        w.api_client.do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body={**corpo, "id": pipeline_id})
    else:
        pipeline_id = w.api_client.do("POST", "/api/2.0/pipelines", body=corpo)["pipeline_id"]
    update_id = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True).update_id
    print(f"Pipeline {pipeline_id} · update {update_id}")
    while True:
        estado = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state.value
        if estado in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(15)
    print(f"Estado final: {estado} · {w.config.host}/pipelines/{pipeline_id}")
    if estado == "COMPLETED":
        for o in publicar:
            CTRL.estado("objeto", o["nome"], "publicado", {"pipeline_id": pipeline_id, "liberacao": decisao[o["nome"]][1]})
    else:
        erros = [e.message for e in w.pipelines.list_pipeline_events(pipeline_id=pipeline_id, max_results=50) if e.level and e.level.value == "ERROR"]
        CTRL.evento("06_publicar", f"pipeline terminou em {estado}", nivel="erro", detalhe=erros[:5])
        print("\n".join(erros[:5]))

spark.sql(f"""CREATE OR REPLACE TABLE {C}.conv_pipeline AS
              SELECT '{pipeline_id}' AS pipeline_id, '{update_id}' AS update_id, '{estado}' AS estado,
                     {len(arquivos)} AS objetos, current_timestamp() AS ts""")
spark.sql(tags_sql("TABLE", f"{C}.conv_pipeline"))
CTRL.evento("06_publicar", f"pipeline {estado}", detalhe={"pipeline_id": pipeline_id, "update_id": update_id})
_etapa.__exit__(None, None, None)
if estado not in ("COMPLETED", "SEM_OBJETOS"):
    raise RuntimeError(f"Pipeline terminou em {estado}")

# COMMAND ----------

# DBTITLE 1,Documentação publicada no Unity Catalog
display(spark.sql(f"""SELECT table_name, column_name, comment FROM {CATALOG}.information_schema.columns
                      WHERE table_schema = '{SCH_GOLD}' ORDER BY table_name, ordinal_position"""))
