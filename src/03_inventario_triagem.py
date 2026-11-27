# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR QLIK · PASSO 3</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Inventário e triagem de QVDs</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">Carrega o inventário de QVDs exportado do Qlik (nome, espaço, linhas, tamanho, data de atualização), agrupa arquivos fatiados por período em tabelas lógicas e classifica cada item em migrar, consolidar, arquivar ou descartar. Usa somente metadados: nenhum dado de negócio é lido.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Parâmetros de triagem (vêm da configuração; editáveis pelo app)
def sql_lista(xs):
    return ", ".join("'" + x.replace("'", "''") + "'" for x in xs)

ESPACOS_DESCARTAR = sql_lista(TRI["espacos_descartar"])
TERMO_TESTE = TRI["termo_teste"].upper()
RX_GOLD = "^(" + "|".join(TRI["prefixos_gold"]) + ")"
RX_BRONZE = "^(" + "|".join(TRI["prefixos_bronze"]) + ")"
D_ATIVO, D_RECENTE, D_PARADO = TRI["dias_ativo"], TRI["dias_recente"], TRI["dias_parado"]
print(f"ativo ≤ {D_ATIVO}d · recente ≤ {D_RECENTE}d · parado ≤ {D_PARADO}d · descartar espaços {ESPACOS_DESCARTAR}")
_etapa = CTRL.etapa("03_inventario_triagem"); _etapa.__enter__()

# COMMAND ----------

# DBTITLE 1,Carregar inventário
inv = (spark.read.option("header", True).option("inferSchema", False)
       .csv(f"{DIR_INVENT}/*.csv"))
inv.createOrReplaceTempView("inv_raw")

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.inv_qvd
COMMENT 'Inventário de QVDs exportado do Qlik. Uma linha por arquivo.'
AS
SELECT
  nome,
  espaco,
  CAST(CAST(linhas AS DOUBLE) AS BIGINT)                  AS qt_linhas,
  CAST(tamanho_bytes AS BIGINT)                           AS qt_bytes,
  CAST(tamanho_bytes AS BIGINT) / 1e9                     AS qt_gb,
  to_timestamp(atualizado_em)                             AS ts_atualizado,
  upper(regexp_replace(nome, '(?i)\\\\.qvd$', ''))        AS nome_base
FROM inv_raw
""")
spark.sql(tags_sql("TABLE", f"{C}.inv_qvd"))
display(spark.sql(f"SELECT count(*) AS arquivos, round(sum(qt_gb),1) AS gb, round(sum(qt_linhas)/1e6,0) AS milhoes_linhas, count(DISTINCT espaco) AS espacos FROM {C}.inv_qvd"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Regras de triagem
# MAGIC | Regra | Critério | Decisão sugerida |
# MAGIC |---|---|---|
# MAGIC | Vazio | zero linhas | Descartar |
# MAGIC | Teste ou pessoal | espaços listados em `triagem.espacos_descartar` ou nome com `triagem.termo_teste` | Descartar após confirmar com o dono |
# MAGIC | Nome anômalo | nome começa com ponto ou repete o prefixo (erro de caminho no STORE) | Revisar |
# MAGIC | Família particionada | sufixo AAAA, AAAAMM ou AAAAMMDD | Consolidar em uma tabela Delta com coluna de período |
# MAGIC | Duplicado | mesmo nome em mais de um espaço | Eleger a versão oficial antes da paridade |
# MAGIC | Atividade | dias desde a última atualização, contados a partir da data mais recente do inventário | Limites em `triagem.dias_ativo`, `dias_recente` e `dias_parado` |
# MAGIC
# MAGIC Todos os limites ficam na configuração e podem ser ajustados pela tela Triagem do app, sem editar código.

# COMMAND ----------

# DBTITLE 1,Classificar arquivos
spark.sql(f"""
CREATE OR REPLACE TABLE {C}.inv_triagem
COMMENT 'Triagem por arquivo: família lógica, atividade, flags e decisão sugerida.'
AS
WITH ref AS (SELECT max(ts_atualizado) AS ts_ref FROM {C}.inv_qvd),
base AS (
  SELECT q.*,
    regexp_replace(nome_base, '[_-]?(19|20)[0-9]{{2}}(0[1-9]|1[0-2])?([_-]?[0-3][0-9])?[A-Z]?$', '{{P}}') AS familia,
    datediff(r.ts_ref, q.ts_atualizado) AS dias_sem_atualizar
  FROM {C}.inv_qvd q CROSS JOIN ref r
),
dup AS (
  SELECT nome_base, count(DISTINCT espaco) AS n_espacos FROM base GROUP BY nome_base
)
SELECT
  b.*,
  b.familia LIKE '%{{P}}%'                                         AS fl_particionado,
  coalesce(qt_linhas, 0) = 0                                       AS fl_vazio,
  d.n_espacos > 1                                                  AS fl_duplicado,
  (espaco IN ({ESPACOS_DESCARTAR}) OR nome_base LIKE '%{TERMO_TESTE}%') AS fl_teste_pessoal,
  (nome LIKE '.%' OR nome_base RLIKE '^([A-Z_]+)_\\\\1')           AS fl_nome_anomalo,
  CASE WHEN dias_sem_atualizar <= {D_ATIVO}  THEN 'ativo'
       WHEN dias_sem_atualizar <= {D_RECENTE} THEN 'recente'
       WHEN dias_sem_atualizar <= {D_PARADO} THEN 'parado'
       ELSE 'historico' END                                       AS status_atividade,
  CASE
    WHEN coalesce(qt_linhas, 0) = 0 THEN 'descartar'
    WHEN espaco IN ({ESPACOS_DESCARTAR}) OR nome_base LIKE '%{TERMO_TESTE}%' THEN 'descartar'
    WHEN nome LIKE '.%' OR nome_base RLIKE '^([A-Z_]+)_\\\\1' THEN 'revisar'
    WHEN dias_sem_atualizar > {D_PARADO} THEN 'arquivar_historico'
    WHEN d.n_espacos > 1 THEN 'eleger_oficial'
    ELSE 'migrar' END                                              AS decisao,
  CASE
    WHEN nome_base RLIKE '{RX_GOLD}' THEN 'gold'
    WHEN nome_base RLIKE '{RX_BRONZE}' THEN 'bronze'
    ELSE 'silver' END                                              AS camada_hipotese
FROM base b JOIN dup d USING (nome_base)
""")
spark.sql(tags_sql("TABLE", f"{C}.inv_triagem"))

# COMMAND ----------

# DBTITLE 1,Resumo por decisão
display(spark.sql(f"""
SELECT decisao, count(*) AS arquivos, round(sum(qt_gb), 2) AS gb, count(DISTINCT familia) AS tabelas_logicas
FROM {C}.inv_triagem GROUP BY decisao ORDER BY arquivos DESC
"""))

# COMMAND ----------

# DBTITLE 1,Resumo por espaço e atividade
display(spark.sql(f"""
SELECT espaco,
       count(*) AS arquivos,
       round(sum(qt_gb), 2) AS gb,
       sum(CASE WHEN status_atividade = 'ativo' THEN 1 ELSE 0 END) AS ativos_30d,
       sum(CASE WHEN fl_vazio THEN 1 ELSE 0 END) AS vazios,
       sum(CASE WHEN fl_particionado THEN 1 ELSE 0 END) AS particionados,
       sum(CASE WHEN fl_duplicado THEN 1 ELSE 0 END) AS duplicados
FROM {C}.inv_triagem
GROUP BY espaco ORDER BY gb DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Backlog de migração por tabela lógica
# MAGIC Cada família vira uma unidade de migração. O tamanho estimado orienta a ordem das ondas e a quantidade de pipelines.

# COMMAND ----------

# DBTITLE 1,Unidades de migração
spark.sql(f"""
CREATE OR REPLACE TABLE {C}.inv_unidade_migracao
COMMENT 'Backlog: uma linha por tabela lógica (família) e espaço, com decisão consolidada.'
AS
SELECT
  espaco,
  familia,
  count(*)                                   AS arquivos,
  round(sum(qt_gb), 3)                       AS gb,
  max(qt_linhas)                             AS maior_arquivo_linhas,
  max(ts_atualizado)                         AS ultima_atualizacao,
  min(dias_sem_atualizar)                    AS dias_sem_atualizar,
  max(camada_hipotese)                       AS camada_hipotese,
  CASE WHEN array_contains(collect_set(decisao), 'migrar') THEN 'migrar'
       WHEN array_contains(collect_set(decisao), 'eleger_oficial') THEN 'eleger_oficial'
       WHEN array_contains(collect_set(decisao), 'revisar') THEN 'revisar'
       WHEN array_contains(collect_set(decisao), 'arquivar_historico') THEN 'arquivar_historico'
       ELSE 'descartar' END                  AS decisao
FROM {C}.inv_triagem
GROUP BY espaco, familia
""")
spark.sql(tags_sql("TABLE", f"{C}.inv_unidade_migracao"))

display(spark.sql(f"""
SELECT decisao, camada_hipotese, count(*) AS unidades, round(sum(gb), 2) AS gb
FROM {C}.inv_unidade_migracao
GROUP BY ALL ORDER BY decisao, camada_hipotese
"""))

# COMMAND ----------

# DBTITLE 1,Top 25 unidades a migrar por volume
display(spark.sql(f"""
SELECT espaco, familia, arquivos, gb, maior_arquivo_linhas, ultima_atualizacao, camada_hipotese
FROM {C}.inv_unidade_migracao
WHERE decisao = 'migrar'
ORDER BY gb DESC LIMIT 25
"""))

# COMMAND ----------

# DBTITLE 1,Registrar o estado de cada tabela lógica
ESTADO_POR_DECISAO = {"migrar": "triado", "eleger_oficial": "triado", "revisar": "triado",
                      "arquivar_historico": "arquivado", "descartar": "descartado"}
unidades = spark.table(f"{C}.inv_unidade_migracao").select("espaco", "familia", "decisao", "gb", "camada_hipotese").collect()
for u in unidades:
    CTRL.estado("tabela_logica", f"{u.espaco}/{u.familia}", ESTADO_POR_DECISAO[u.decisao],
                {"decisao": u.decisao, "gb": u.gb, "camada_hipotese": u.camada_hipotese})
resumo = {}
for u in unidades:
    resumo[u.decisao] = resumo.get(u.decisao, 0) + 1
CTRL.evento("03_inventario_triagem", f"{len(unidades)} tabelas lógicas triadas", detalhe=resumo)
_etapa.__exit__(None, None, None)
print(resumo)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:6px solid #0B7285;background:#F5F7F8;border-radius:0 8px 8px 0;padding:18px 22px;font-family:'DM Sans',Arial,sans-serif;color:#495057;font-size:15px;">
# MAGIC <div style="color:#0B7285;font-weight:700;letter-spacing:1px;font-size:13px;">LIMITE DESTA ETAPA</div>
# MAGIC A camada sugerida é uma hipótese baseada em nome e espaço. Ela é confirmada no passo 4, quando os scripts e os cabeçalhos dos QVDs entram na análise de linhagem.
# MAGIC </div>
