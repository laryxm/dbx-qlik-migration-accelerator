# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR QLIK · PASSO 2 · SIMULAÇÃO</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Simular o ambiente Qlik de origem</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">Cria um ERP de saúde sintético da Cooperativa de Saúde Vale do Ipê (empresa fictícia) e executa a recarga dos scripts Qlik de exemplo, gerando arquivos QVD reais. Esses QVDs são o baseline da paridade. No cliente, este notebook não existe: os QVDs e as tabelas de origem são os reais.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %pip install pyqvd==2.4.0 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Só roda no modo demonstração
if MODO != "demo":
    CTRL.evento("02_simular_ambiente_qlik", "modo cliente: simulação pulada")
    CTRL.gravar()
    dbutils.notebook.exit("modo cliente: o passo de simulação não se aplica")
import time as _t
_t0 = _t.time()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Tabelas de origem no bronze
# MAGIC Representam o que o Lakeflow Connect entrega a partir do Oracle do ERP: uma tabela por tabela de origem, sem regra de negócio. Os dados são sintéticos e determinísticos (seed fixa).

# COMMAND ----------

# DBTITLE 1,Gerar ERP sintético
B = BRONZE_FQN

spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bronze_erp_beneficiario AS
SELECT
  id                                                         AS cd_beneficiario,
  concat('Beneficiário ', id)                                AS nm_beneficiario,
  date_add(DATE'1950-01-01', CAST(rand(11) * 25000 + id % 7 AS INT)) AS dt_nascimento,
  CASE WHEN rand(12) < 0.52 THEN 'F' ELSE 'M' END            AS ie_sexo,
  CAST(1 + floor(rand(13) * 12) AS INT)                      AS cd_plano,
  CAST(1 + floor(rand(14) * 900) AS INT)                     AS cd_contrato,
  element_at(array('CAP','CAP','CAP','NOR','SUL','AGR'), CAST(1 + floor(rand(15) * 6) AS INT)) AS cd_regional,
  date_add(DATE'2015-01-01', CAST(rand(16) * 3900 AS INT))   AS dt_adesao,
  CASE WHEN rand(17) < 0.15
       THEN date_add(DATE'2023-06-01', CAST(rand(18) * 1200 AS INT)) END AS dt_cancelamento
FROM range(1, 20001) AS t(id)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bronze_erp_prestador AS
SELECT
  id                                                         AS cd_prestador,
  concat('Prestador ', id)                                   AS nm_prestador,
  element_at(array('H','C','C','L','M','M','M','X'), CAST(1 + floor(rand(21) * 8) AS INT)) AS ie_tipo_prestador,
  CAST(2700000 + floor(rand(22) * 100) AS INT)               AS cd_municipio,
  CASE WHEN rand(23) < 0.6 THEN 'S' ELSE 'N' END             AS ie_cooperado
FROM range(1, 601) AS t(id)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bronze_erp_procedimento AS
SELECT
  id                                                         AS cd_procedimento,
  concat('Procedimento ', id)                                AS ds_procedimento,
  element_at(array(10,10,20,20,20,30,40,50,60), CAST(1 + floor(rand(31) * 9) AS INT)) AS cd_grupo
FROM range(10101, 10501) AS t(id)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bronze_erp_conta_medica_item AS
WITH base AS (
  SELECT id,
         timestampadd(SECOND, CAST(rand(41) * 86400 * 1035 AS INT), TIMESTAMP'2024-01-01 00:00:00') AS dt_atendimento,
         CAST(round(30 + pow(rand(42), 3) * 8000, 2) AS DECIMAL(12,2)) AS vl_apresentado,
         rand(43) AS r_glosa
  FROM range(1, 300001) AS t(id)
)
SELECT
  id                                                         AS nr_item_conta,
  CAST(1000000 + floor(id / 3) AS BIGINT)                    AS nr_conta,
  CAST(1 + floor(rand(44) * 20000) AS INT)                   AS cd_beneficiario,
  CAST(1 + floor(rand(45) * 600) AS INT)                     AS cd_prestador,
  CAST(10101 + floor(rand(46) * 400) AS INT)                 AS cd_procedimento,
  dt_atendimento,
  CASE WHEN rand(47) < 0.01 THEN NULL
       ELSE element_at(array('C','C','C','S','S','S','S','I'), CAST(1 + floor(rand(48) * 8) AS INT)) END AS ie_tipo_guia,
  element_at(array('A','F','F','F','F','F','F','F','F','F','F','F','F','F','F','F','F','F','F','F','F','F','F','C'), CAST(1 + floor(rand(49) * 24) AS INT)) AS ie_status,
  vl_apresentado,
  CAST(CASE WHEN r_glosa < 0.12 THEN round(vl_apresentado * r_glosa * 4, 2) ELSE 0 END AS DECIMAL(12,2)) AS vl_glosa,
  CAST(vl_apresentado - CASE WHEN r_glosa < 0.12 THEN round(vl_apresentado * r_glosa * 4, 2) ELSE 0 END AS DECIMAL(12,2)) AS vl_liberado,
  timestampadd(DAY, CAST(5 + rand(50) * 40 AS INT), dt_atendimento) AS dt_atualizacao
FROM base
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bronze_erp_reembolso AS
WITH base AS (
  SELECT id,
         CAST(round(80 + rand(52) * 2500, 2) AS DECIMAL(12,2)) AS vl_solicitado,
         rand(53) AS r
  FROM range(1, 6001) AS t(id)
)
SELECT
  id                                                         AS nr_reembolso,
  CAST(1 + floor(rand(54) * 20000) AS INT)                   AS cd_beneficiario,
  CAST(10101 + floor(rand(55) * 400) AS INT)                 AS cd_procedimento,
  timestampadd(SECOND, CAST(rand(56) * 86400 * 670 AS INT), TIMESTAMP'2025-01-01 00:00:00') AS dt_solicitacao,
  vl_solicitado,
  CAST(round(vl_solicitado * (0.4 + r * 0.6), 2) AS DECIMAL(12,2)) AS vl_reembolsado,
  element_at(array('P','P','P','P','N','A'), CAST(1 + floor(rand(57) * 6) AS INT)) AS ie_status
FROM base
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bronze_erp_mensalidade AS
WITH meses AS (
  SELECT explode(sequence(DATE'2025-01-01', DATE'2026-10-01', INTERVAL 1 MONTH)) AS dt_competencia
)
SELECT
  b.cd_beneficiario,
  b.cd_contrato,
  m.dt_competencia,
  CAST(round(180 + (b.cd_plano * 37.5) + (b.cd_beneficiario % 17) * 4.1, 2) AS DECIMAL(12,2)) AS vl_mensalidade
FROM {B}.bronze_erp_beneficiario b
CROSS JOIN meses m
WHERE b.dt_adesao <= m.dt_competencia
  AND (b.dt_cancelamento IS NULL OR m.dt_competencia <= add_months(b.dt_cancelamento, 2))
""")

for t in ["beneficiario", "prestador", "procedimento", "conta_medica_item", "reembolso", "mensalidade"]:
    spark.sql(tags_sql("TABLE", f"{B}.bronze_erp_{t}"))
    n = spark.table(f"{B}.bronze_erp_{t}").count()
    print(f"bronze_erp_{t:20s} {n:>9,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Recarga Qlik de referência
# MAGIC Implementação em pandas que reproduz a semântica dos scripts `EXT_ERP_Saude`, `TRF_CustoAssistencial` e `TRF_Sinistralidade` como o motor do Qlik executaria: `Concatenate`, `Left Join` por campos homônimos, `ApplyMap` com default, `Exists` dependente da ordem de carga e `Join` sem prefixo como outer join. O resultado é gravado em QVD com `pyqvd`.

# COMMAND ----------

# DBTITLE 1,Executar recarga e gravar QVDs
import pandas as pd, numpy as np
from pyqvd import QvdTable

def src(t):
    return spark.table(f"{B}.bronze_erp_{t}").toPandas()

def upper_cols(df):
    df.columns = [c.upper() for c in df.columns]
    return df

ext = {
    "EXT_Beneficiario":    upper_cols(src("beneficiario")),
    "EXT_Prestador":       upper_cols(src("prestador")),
    "EXT_Procedimento":    upper_cols(src("procedimento")),
    "EXT_ContaMedicaItem": upper_cols(src("conta_medica_item")),
    "EXT_Reembolso":       upper_cols(src("reembolso")),
    "EXT_Mensalidade":     upper_cols(src("mensalidade")),
}
for k, df in ext.items():
    for c in df.columns:
        if c.startswith(("VL_",)):
            df[c] = df[c].astype(float)
        if c.startswith(("DT_",)):
            df[c] = pd.to_datetime(df[c])

ANO_INICIO = 2025
map_guia = {"C": "Consulta", "S": "SP/SADT", "I": "Internação"}
map_grupo = {10: "Consultas", 20: "Exames e Diagnose", 30: "Terapias", 40: "Internação", 50: "OPME"}
map_prest = {"H": "Hospital", "C": "Clínica", "L": "Laboratório", "M": "Médico Cooperado"}

# TRF_CustoAssistencial
cm = ext["EXT_ContaMedicaItem"]
cm = cm[(cm.IE_STATUS != "C") & (cm.DT_ATENDIMENTO.dt.year >= ANO_INICIO)]
custo_cm = pd.DataFrame({
    "ID_ITEM": cm.NR_ITEM_CONTA.astype(str),
    "NR_CONTA": cm.NR_CONTA,
    "CD_BENEFICIARIO": cm.CD_BENEFICIARIO,
    "CD_PRESTADOR": cm.CD_PRESTADOR,
    "CD_PROCEDIMENTO": cm.CD_PROCEDIMENTO,
    "DT_ATENDIMENTO": cm.DT_ATENDIMENTO.dt.normalize(),
    "Competência": cm.DT_ATENDIMENTO.dt.to_period("M").dt.to_timestamp(),
    "Tipo Guia": cm.IE_TIPO_GUIA.map(map_guia).fillna("Outros"),
    "ORIGEM": "Conta Médica",
    "VL_APRESENTADO": cm.VL_APRESENTADO,
    "VL_GLOSA": cm.VL_GLOSA,
    "VL_LIBERADO": cm.VL_LIBERADO,
})
rb = ext["EXT_Reembolso"]
rb = rb[(rb.IE_STATUS == "P") & (rb.DT_SOLICITACAO.dt.year >= ANO_INICIO)]
custo_rb = pd.DataFrame({
    "ID_ITEM": "R" + rb.NR_REEMBOLSO.astype(str),
    "CD_BENEFICIARIO": rb.CD_BENEFICIARIO,
    "CD_PROCEDIMENTO": rb.CD_PROCEDIMENTO,
    "DT_ATENDIMENTO": rb.DT_SOLICITACAO.dt.normalize(),
    "Competência": rb.DT_SOLICITACAO.dt.to_period("M").dt.to_timestamp(),
    "Tipo Guia": "Reembolso",
    "ORIGEM": "Reembolso",
    "VL_APRESENTADO": rb.VL_SOLICITADO,
    "VL_GLOSA": rb.VL_SOLICITADO - rb.VL_REEMBOLSADO,
    "VL_LIBERADO": rb.VL_REEMBOLSADO,
})
custo = pd.concat([custo_cm, custo_rb], ignore_index=True)   # Concatenate: campos ausentes ficam nulos

proc = ext["EXT_Procedimento"]
dim_proc = pd.DataFrame({
    "CD_PROCEDIMENTO": proc.CD_PROCEDIMENTO,
    "DS_PROCEDIMENTO": proc.DS_PROCEDIMENTO,
    "Grupo Procedimento": proc.CD_GRUPO.map(map_grupo).fillna("Outros"),
})
custo = custo.merge(dim_proc[["CD_PROCEDIMENTO", "Grupo Procedimento"]], on="CD_PROCEDIMENTO", how="left")
custo = custo.merge(ext["EXT_Beneficiario"][["CD_BENEFICIARIO", "CD_PLANO", "CD_CONTRATO", "CD_REGIONAL"]], on="CD_BENEFICIARIO", how="left")
pr = ext["EXT_Prestador"]
dim_pr = pd.DataFrame({
    "CD_PRESTADOR": pr.CD_PRESTADOR,
    "Tipo Prestador": pr.IE_TIPO_PRESTADOR.map(map_prest).fillna("Não informado"),
    "FL_COOPERADO": np.where(pr.IE_COOPERADO == "S", 1, 0),
})
custo["CD_PRESTADOR"] = custo["CD_PRESTADOR"].astype("Int64")
dim_pr["CD_PRESTADOR"] = dim_pr["CD_PRESTADOR"].astype("Int64")
custo = custo.merge(dim_pr, on="CD_PRESTADOR", how="left")   # chave nula não associa no Qlik

# TRF_Sinistralidade
benef = ext["EXT_Beneficiario"]
ativos = benef[benef.DT_CANCELAMENTO.isna() | (benef.DT_CANCELAMENTO >= pd.Timestamp(ANO_INICIO, 1, 1))]
exists_benef = set(custo.CD_BENEFICIARIO.dropna()) | set(ativos.CD_BENEFICIARIO)   # Exists: tudo já carregado no campo
mens = ext["EXT_Mensalidade"]
mens = mens[mens.CD_BENEFICIARIO.isin(exists_benef)]
mens = pd.DataFrame({"CD_CONTRATO": mens.CD_CONTRATO,
                     "Competência": mens.DT_COMPETENCIA.dt.to_period("M").dt.to_timestamp(),
                     "VL_MENSALIDADE": mens.VL_MENSALIDADE})
agg_c = custo.groupby(["CD_CONTRATO", "Competência"], dropna=False).agg(
    VL_CUSTO=("VL_LIBERADO", "sum"), QT_BENEF_ATENDIDOS=("CD_BENEFICIARIO", "nunique")).reset_index()
agg_r = mens.groupby(["CD_CONTRATO", "Competência"]).agg(VL_RECEITA=("VL_MENSALIDADE", "sum")).reset_index()
sin = agg_c.merge(agg_r, on=["CD_CONTRATO", "Competência"], how="outer")   # Join sem Left/Inner = outer
sin["PC_SINISTRALIDADE"] = np.where(sin.VL_RECEITA > 0, sin.VL_CUSTO.fillna(0) / sin.VL_RECEITA, np.nan)

modelo = {"FatoCustoAssistencial": custo, "DimProcedimento": dim_proc, "FatoSinistralidade": sin}
print({k: len(v) for k, v in {**ext, **modelo}.items()})

# COMMAND ----------

# DBTITLE 1,Gravar QVDs com cabeçalho de linhagem
import re, os, uuid

LINEAGE = {
    "EXT_Beneficiario":    ("EXT_ERP_Saude", "SQL SELECT ... FROM ERP.TB_BENEFICIARIO"),
    "EXT_Prestador":       ("EXT_ERP_Saude", "SQL SELECT ... FROM ERP.TB_PRESTADOR"),
    "EXT_Procedimento":    ("EXT_ERP_Saude", "SQL SELECT ... FROM ERP.TB_PROCEDIMENTO"),
    "EXT_ContaMedicaItem": ("EXT_ERP_Saude", "SQL SELECT ... FROM ERP.TB_CONTA_MEDICA_ITEM"),
    "EXT_Reembolso":       ("EXT_ERP_Saude", "SQL SELECT ... FROM ERP.TB_REEMBOLSO"),
    "EXT_Mensalidade":     ("EXT_ERP_Saude", "SQL SELECT ... FROM ERP.TB_MENSALIDADE"),
    "FatoCustoAssistencial": ("TRF_CustoAssistencial", "lib://QVD_Extract/EXT_ContaMedicaItem.qvd;lib://QVD_Extract/EXT_Reembolso.qvd;lib://QVD_Extract/EXT_Procedimento.qvd;lib://QVD_Extract/EXT_Beneficiario.qvd;lib://QVD_Extract/EXT_Prestador.qvd"),
    "DimProcedimento":     ("TRF_CustoAssistencial", "lib://QVD_Extract/EXT_Procedimento.qvd"),
    "FatoSinistralidade":  ("TRF_Sinistralidade", "lib://QVD_Model/FatoCustoAssistencial.qvd;lib://QVD_Extract/EXT_Beneficiario.qvd;lib://QVD_Extract/EXT_Mensalidade.qvd"),
}

def gravar_qvd(nome, df):
    local = f"/tmp/{nome}.qvd"
    QvdTable.from_pandas(df).to_qvd(local)
    raw = open(local, "rb").read()
    fim = raw.index(b"</QvdTableHeader>") + len(b"</QvdTableHeader>")
    header, corpo = raw[:fim].decode("utf-8"), raw[fim:]
    doc, stmt = LINEAGE[nome]
    lineage = "".join(
        f"<LineageInfo><Discriminator>{s}</Discriminator><Statement></Statement></LineageInfo>"
        for s in stmt.split(";"))
    header = header.replace("<TableName>UNKNOWN</TableName>", f"<TableName>{nome}</TableName>")
    header = re.sub(r"<CreatorDoc>.*?</CreatorDoc>", f"<CreatorDoc>lib://Scripts/{doc}.qvs</CreatorDoc>", header)
    header = header.replace("<Fields>", f"<Lineage>{lineage}</Lineage><Fields>", 1)
    with open(f"{DIR_QVD}/{nome}.qvd", "wb") as f:
        f.write(header.encode("utf-8") + corpo)
    return os.path.getsize(f"{DIR_QVD}/{nome}.qvd")

for nome, df in {**ext, **modelo}.items():
    tam = gravar_qvd(nome, df)
    chk = QvdTable.from_qvd(f"{DIR_QVD}/{nome}.qvd").shape[0]
    print(f"{nome:24s} {len(df):>9,} linhas  {tam/1e6:6.1f} MB  releitura {chk:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Hypercubes exportados do app
# MAGIC Equivalem à tabela simples que o key user exporta do app Qlik (ou ao `qHyperCubeDef` via Engine API). Servem para a paridade de métricas (N4).

# COMMAND ----------

# DBTITLE 1,Gerar hypercubes
hc1 = custo.groupby(["Competência", "Tipo Guia"]).agg(
    **{"Custo Assistencial": ("VL_LIBERADO", "sum"),
       "Valor Apresentado": ("VL_APRESENTADO", "sum"),
       "VL_GLOSA": ("VL_GLOSA", "sum"),
       "Beneficiários Atendidos": ("CD_BENEFICIARIO", "nunique")}).reset_index()
hc1["Glosa %"] = hc1["VL_GLOSA"] / hc1["Valor Apresentado"]
hc1 = hc1.drop(columns=["VL_GLOSA"])
hc1["Competência"] = hc1["Competência"].dt.strftime("%d/%m/%Y")
hc1.to_csv(f"{DIR_APPS}/hypercube_custo_competencia_guia.csv", index=False, sep=";", decimal=",")

hc2 = sin.groupby("Competência").agg(VL_CUSTO=("VL_CUSTO", "sum"), VL_RECEITA=("VL_RECEITA", "sum")).reset_index()
hc2["Sinistralidade"] = np.where(hc2.VL_RECEITA > 0, hc2.VL_CUSTO / hc2.VL_RECEITA.where(hc2.VL_RECEITA > 0), np.nan)   # Qlik: divisão por zero = nulo; Sum sem valores = 0
hc2 = hc2.rename(columns={"VL_RECEITA": "Receita Mensalidades"})[["Competência", "Sinistralidade", "Receita Mensalidades"]]
hc2["Competência"] = hc2["Competência"].dt.strftime("%d/%m/%Y")
hc2.to_csv(f"{DIR_APPS}/hypercube_sinistralidade_competencia.csv", index=False, sep=";", decimal=",")

display(dbutils.fs.ls(DIR_QVD))
display(dbutils.fs.ls(DIR_APPS))

# COMMAND ----------

# DBTITLE 1,Registrar no controle
CTRL.evento("02_simular_ambiente_qlik", f"ERP sintético, {len(os.listdir(DIR_QVD))} QVDs e hypercubes gerados em {_t.time() - _t0:.0f}s")
CTRL.gravar()
