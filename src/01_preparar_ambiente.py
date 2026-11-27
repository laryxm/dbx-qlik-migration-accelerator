# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR QLIK · PASSO 1</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Preparar o ambiente</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">Cria os schemas por camada, o volume onde ficam os arquivos exportados do Qlik e as tabelas de controle que registram tudo o que o acelerador faz. No modo demonstração, também copia a amostra fictícia para o volume.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:6px solid #0B7285;background:#F5F7F8;border-radius:0 8px 8px 0;padding:18px 22px;font-family:'DM Sans',Arial,sans-serif;color:#495057;font-size:15px;">
# MAGIC <div style="color:#0B7285;font-weight:700;letter-spacing:1px;font-size:13px;">PARA QUEM VEM DO QLIK</div>
# MAGIC <b>Catálogo e schema</b> fazem o papel das pastas e espaços do Qlik: organizam e dão permissão.<br>
# MAGIC <b>Volume</b> é a pasta de arquivos governada pelo Unity Catalog. É onde entram scripts, cabeçalhos de QVD e exports de medidas.<br>
# MAGIC <b>Tabelas de controle</b> (<code>evento</code>, <code>unidade_status</code>, <code>execucao_passo</code>, <code>aprovacao</code>) são o diário da migração. O app lê essas tabelas para mostrar o andamento.
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Schemas, volume, tabelas de controle e tags
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}")   # o controle precisa existir antes de registrar o passo
with CTRL.etapa("01_preparar_ambiente"):
    schemas = [SCH_CTRL, SCH_SILVER, SCH_GOLD, SCH_SANDBOX] + ([] if AMB.get("schema_bronze") else [SCH_BRONZE])
    for sch in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{sch}")
        spark.sql(tags_sql("SCHEMA", f"{CATALOG}.{sch}"))

    spark.sql(f"CREATE VOLUME IF NOT EXISTS {C}.arquivos")
    spark.sql(tags_sql("VOLUME", f"{C}.arquivos"))
    for d in [DIR_INVENT, DIR_SCRIPTS, DIR_APPS, DIR_QVD, DIR_SAIDA, f"{VOLUME}/config"]:
        dbutils.fs.mkdirs(d)

    if MODO == "cliente" and not AMB.get("schema_bronze"):
        CTRL.evento("01_preparar_ambiente", "modo cliente sem schema das origens: o fluxo entrega triagem, linhagem e especificação de ingestão; "
                    "a conversão completa e a paridade dependem das origens no Lakehouse (Configurar → Essencial).", nivel="aviso")
    CTRL.garantir_tabelas()
    for t in acel_ctrl.DDL:
        spark.sql(tags_sql("TABLE", f"{C}.{t}"))

    # configuração efetiva: o app lê este arquivo para montar a tela de Configuração
    with open(f"{VOLUME}/config/efetiva.json", "w", encoding="utf-8") as f:
        json.dump(CFG, f, ensure_ascii=False, indent=2)
    CTRL.evento("01_preparar_ambiente", "configuração em uso", detalhe=CFG)
    print(f"Schemas: {', '.join(schemas)} · volume {VOLUME}")

# COMMAND ----------

# DBTITLE 1,Permissões do app do acelerador
# O app lê e grava as tabelas de controle, envia arquivos ao volume e lê silver, gold, bronze e sandbox.
# Quem executa este passo precisa poder conceder privilégios no catálogo (dono ou MANAGE).
with CTRL.etapa("01_permissoes_app"):
    if not APP_NOME:
        print("Parâmetro app_nome vazio: nenhuma permissão concedida.")
    else:
        from databricks.sdk import WorkspaceClient
        try:
            sp = WorkspaceClient().apps.get(APP_NOME).service_principal_client_id
        except Exception as e:
            sp = None
            CTRL.evento("01_permissoes_app", f"app {APP_NOME} não encontrado: {e}", nivel="aviso")
        if sp:
            comandos = [f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{sp}`",
                        f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA {C} TO `{sp}`",
                        f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {C}.arquivos TO `{sp}`"]
            comandos += [f"GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOG}.{s_} TO `{sp}`" for s_ in (SCH_SILVER, SCH_GOLD, SCH_SANDBOX)]
            comandos += [f"GRANT USE CATALOG ON CATALOG {CAT_BRONZE} TO `{sp}`"] if CAT_BRONZE != CATALOG else []
            comandos += [f"GRANT USE SCHEMA, SELECT ON SCHEMA {BRONZE_FQN} TO `{sp}`"]
            falhas = []
            for q in comandos:
                try:
                    spark.sql(q)
                except Exception as e:
                    falhas.append((q, str(e).splitlines()[0][:200]))
            if falhas:
                CTRL.evento("01_permissoes_app", "algumas permissões não puderam ser concedidas; peça ao dono do catálogo para rodar os comandos",
                            nivel="aviso", detalhe=[q for q, _ in falhas])
                print("Não foi possível conceder (peça ao dono do catálogo):")
                for q, e in falhas:
                    print(f"  {q};   -- {e}")
            else:
                CTRL.evento("01_permissoes_app", f"permissões concedidas ao app {APP_NOME} ({sp})")
                print(f"Permissões concedidas ao service principal do app {APP_NOME}.")

# COMMAND ----------

# DBTITLE 1,Modo demonstração: copiar a amostra fictícia para o volume
# No cliente, os arquivos chegam pelo app (tela Coleta) a partir do kit de extração.
import os, shutil

with CTRL.etapa("01_copiar_amostra"):
    if MODO != "demo":
        CTRL.evento("01_copiar_amostra", "modo cliente: amostra não copiada, usar os arquivos enviados pelo app")
        print("Modo cliente: nada a copiar.")
    else:
        origem = f"{WS_DIR}/qlik_amostra"
        copias = {f"{origem}/inventario_qvds.csv": f"{DIR_INVENT}/inventario_qvds.csv"}
        for f in os.listdir(f"{origem}/scripts"):
            copias[f"{origem}/scripts/{f}"] = f"{DIR_SCRIPTS}/{f}"
        for f in os.listdir(f"{origem}/apps"):
            copias[f"{origem}/apps/{f}"] = f"{DIR_APPS}/{f}"
        for src, dst in copias.items():
            shutil.copyfile(src, dst)
            print(f"{os.path.basename(src):45s} -> {dst}")
        CTRL.evento("01_copiar_amostra", f"{len(copias)} arquivos da amostra copiados")

# COMMAND ----------

# DBTITLE 1,Conferência dos arquivos de entrada
def listar(d, ext):
    return sorted(f for f in os.listdir(d) if f.lower().endswith(ext)) if os.path.exists(d) else []

entrada = {"inventário (.csv)": listar(DIR_INVENT, ".csv"), "scripts (.qvs)": listar(DIR_SCRIPTS, ".qvs"),
           "medidas de app (_medidas.json)": listar(DIR_APPS, "_medidas.json"), "QVDs ou cabeçalhos (.qvd)": listar(DIR_QVD, ".qvd")}
for k, v in entrada.items():
    print(f"{k:32s} {len(v):4d}  {', '.join(v[:5])}{' ...' if len(v) > 5 else ''}")
if MODO == "demo":
    print("\nNo modo demonstração, os QVDs e hypercubes são gerados no passo 2.")
