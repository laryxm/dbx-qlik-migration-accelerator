# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR QLIK · PASSO 8</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Paridade com o Qlik em quatro níveis</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">Lê os QVDs originais como baseline e compara com o que o pipeline publicou: N1 volume, N2 agregados por grão de negócio, N3 hash de linha nos dois sentidos e N4 métricas da metric view contra o hypercube exportado do app. O resultado fica numa tabela e é o critério de liberação de cada objeto.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %pip install pyqvd==2.4.0 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Baseline: QVD para Delta
_etapa = CTRL.etapa("08_paridade"); _etapa.__enter__()
import pandas as pd, os, json
from pyqvd import QvdTable
from pyspark.sql import functions as F, Row
import qlik_parser as qp

TOL_REL, TOL_ABS = PAR["tolerancia_relativa"], PAR["tolerancia_absoluta"]
alvos = [r.asDict() for r in spark.sql(f"""
    SELECT q.qvd, q.objeto_destino FROM {C}.lin_qvd q
    JOIN {C}.conv_objeto c ON c.origem_qvd = q.qvd AND c.status = 'ok'
    JOIN {C}.unidade_status u ON u.tipo = 'objeto' AND u.unidade = c.nome AND u.estado IN ('publicado', 'paridade_ok', 'paridade_divergente')
    WHERE q.camada_destino IN ('silver', 'gold')""").collect()]
print(f"{len(alvos)} objeto(s) publicados para testar")
if not alvos and not spark.catalog.tableExists(f"{C}.sem_medida"):
    CTRL.evento("08_paridade", "nada publicado para comparar com o Qlik ainda", nivel="aviso")
    _etapa.__exit__(None, None, None)
    dbutils.notebook.exit("nada publicado")

for a in alvos:
    pdf = QvdTable.from_qvd(f"{DIR_QVD}/{a['qvd']}").to_pandas()
    pdf.columns = [qp.nome_campo(c) for c in pdf.columns]
    for c in pdf.columns:
        if pdf[c].dtype == object or str(pdf[c].dtype).startswith("str"):
            pdf[c] = pdf[c].astype("string")
    destino = spark.table(a["objeto_destino"])
    sdf = spark.createDataFrame(pdf)
    sdf = sdf.select([F.col(f.name).cast(f.dataType).alias(f.name) if f.name in sdf.columns else F.lit(None).cast(f.dataType).alias(f.name)
                      for f in destino.schema.fields])
    a["baseline"] = f"{C}.baseline_{a['objeto_destino'].split('.')[-1]}"
    sdf.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(a["baseline"])
    spark.sql(tags_sql("TABLE", a["baseline"]))
    print(f"{a['qvd']:28s} -> {a['baseline']}  {len(pdf):,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Critérios de aceite
# MAGIC | Nível | Teste | Tolerância |
# MAGIC |---|---|---|
# MAGIC | N1 | Contagem por período | exata |
# MAGIC | N2 | Soma de cada coluna numérica por período e dimensão principal | `paridade.tolerancia_relativa` ou `tolerancia_absoluta` |
# MAGIC | N3 | Hash de linha com colunas normalizadas, EXCEPT ALL nos dois sentidos | zero linhas sem explicação |
# MAGIC | N4 | Medidas da metric view contra o hypercube do app nas mesmas dimensões | igual ao N2, distinct exato |

# COMMAND ----------

# DBTITLE 1,N1, N2 e N3
resultados = []

def periodo_de(cols):
    for c in PAR["colunas_periodo"]:
        if c in cols:
            return c
    return None

for a in alvos:
    obj, base = a["objeto_destino"], a["baseline"]
    schema = spark.table(obj).schema
    cols = [f.name for f in schema.fields]
    nums = [f.name for f in schema.fields if f.dataType.typeName() in ("decimal", "double", "float", "long", "integer") and f.name.startswith(tuple(PAR["prefixos_numericos"]))]
    per = periodo_de(cols)
    dim = next((c for c in PAR["dimensoes_n2"] if c in cols), None)
    nome = obj.split(".")[-1]

    # N1
    g = f"{per}" if per else "'total'"
    n1 = spark.sql(f"""
        SELECT coalesce(b.k, d.k) AS k, b.n AS esperado, d.n AS obtido FROM
        (SELECT CAST({g} AS STRING) k, count(*) n FROM {base} GROUP BY 1) b
        FULL OUTER JOIN (SELECT CAST({g} AS STRING) k, count(*) n FROM {obj} GROUP BY 1) d ON b.k <=> d.k""").collect()
    div = [r for r in n1 if r.esperado != r.obtido]
    resultados.append(Row(objeto=nome, nivel="N1", teste=f"contagem por {per or 'total'}", grupos=len(n1), divergencias=len(div),
                          esperado=float(sum(r.esperado or 0 for r in n1)), obtido=float(sum(r.obtido or 0 for r in n1)),
                          status="ok" if not div else "divergente", detalhe=str([r.asDict() for r in div[:3]])))
    # N2
    chaves = ", ".join([c for c in (per, dim) if c]) or "'total'"
    for m in nums:
        r = spark.sql(f"""
            WITH b AS (SELECT {chaves}, sum({m}) v FROM {base} GROUP BY ALL),
                 d AS (SELECT {chaves}, sum({m}) v FROM {obj} GROUP BY ALL)
            SELECT count(*) grupos,
                   sum(CASE WHEN abs(coalesce(b.v,0) - coalesce(d.v,0)) > greatest({TOL_ABS}, abs(coalesce(b.v,0)) * {TOL_REL}) THEN 1 ELSE 0 END) divergencias,
                   sum(b.v) esperado, sum(d.v) obtido
            FROM b FULL OUTER JOIN d ON {' AND '.join(f'b.{c} <=> d.{c}' for c in [per, dim] if c) or '1=1'}""").first()
        resultados.append(Row(objeto=nome, nivel="N2", teste=f"soma de {m} por {chaves}", grupos=r.grupos, divergencias=r.divergencias,
                              esperado=float(r.esperado or 0), obtido=float(r.obtido or 0),
                              status="ok" if r.divergencias == 0 else "divergente", detalhe=None))
    # N3
    def hash_expr(t):
        partes = []
        for f in schema.fields:
            tn = f.dataType.typeName()
            if tn in ("decimal", "double", "float"):
                partes.append(f"coalesce(CAST(round({f.name}, 2) AS STRING), '~')")
            else:
                partes.append(f"coalesce(CAST({f.name} AS STRING), '~')")
        return f"SELECT xxhash64(concat_ws('|', {', '.join(partes)})) h FROM {t}"
    so_qlik = spark.sql(f"SELECT count(*) n FROM ({hash_expr(base)} EXCEPT ALL {hash_expr(obj)})").first().n
    so_dbx = spark.sql(f"SELECT count(*) n FROM ({hash_expr(obj)} EXCEPT ALL {hash_expr(base)})").first().n
    resultados.append(Row(objeto=nome, nivel="N3", teste="hash de linha, EXCEPT ALL nos dois sentidos", grupos=None,
                          divergencias=int(so_qlik + so_dbx), esperado=float(so_qlik), obtido=float(so_dbx),
                          status="ok" if so_qlik + so_dbx == 0 else "divergente",
                          detalhe=f"{so_qlik} linhas só no Qlik · {so_dbx} linhas só no Databricks"))

# COMMAND ----------

# DBTITLE 1,N4: metric view contra hypercube do app
import json
APPS = [json.load(open(f"{DIR_APPS}/{f}", encoding="utf-8")) for f in sorted(os.listdir(DIR_APPS)) if f.endswith("_medidas.json")]
sem = [r for r in spark.table(f"{C}.sem_medida").collect() if r.teste == "ok"] if spark.catalog.tableExists(f"{C}.sem_medida") else []
hcs = [(app, hc) for app in APPS for hc in app.get("hypercubes_exportados", [])]
for app, hc in hcs:
    medidas = {r.titulo: r for r in sem if r.app == app["app"]}
    pdf = pd.read_csv(f"{DIR_APPS}/{hc['arquivo']}", sep=";", decimal=",")
    dims = [qp.nome_campo(d) for d in hc["dimensoes"]]
    pdf.columns = [qp.nome_campo(c) if c in hc["dimensoes"] else c for c in pdf.columns]
    dims_data = [d for d in dims if d in PAR["colunas_periodo"]]
    for d in dims_data:
        pdf[d] = pd.to_datetime(pdf[d], dayfirst=True).dt.date
    expr_qlik = {mm["title"]: mm["expression"] for mm in app["medidas_mestras"]}
    for titulo in hc["medidas"]:
        m = medidas.get(titulo)
        if not m:
            resultados.append(Row(objeto=hc["arquivo"], nivel="N4", teste=f"{titulo}", grupos=None, divergencias=None,
                                  esperado=None, obtido=None, status="aguardando_publicacao", detalhe="a metric view desta medida ainda não existe: aprove e publique os objetos do gold e rode 05 a 08"))
            continue
        dbx = spark.sql(f"""SELECT {', '.join(dims)}, MEASURE(`{m.medida}`) AS v
                            FROM {CATALOG}.{SCH_GOLD}.{m.metric_view} GROUP BY ALL""").toPandas()
        for d in dims_data:
            dbx[d] = pd.to_datetime(dbx[d]).dt.date
        j = pdf[dims + [titulo]].merge(dbx, on=dims, how="outer")
        j["dif"] = (j[titulo].astype(float) - j["v"].astype(float)).abs()
        if "/" in expr_qlik.get(titulo, ""):          # razão: tolerância praticamente exata
            tol = 0.000001
        else:
            tol = (j[titulo].astype(float).abs() * TOL_REL).clip(lower=TOL_ABS)
        esp, obt = j[titulo].astype(float), j["v"].astype(float)
        um_nulo = esp.isna() ^ obt.isna()
        mask = (j["dif"] > tol) | um_nulo
        div = int(mask.sum())
        amostra = j[mask][dims + [titulo, "v"]].head(5).astype(str).to_dict("records")
        resultados.append(Row(objeto=f"{m.metric_view}", nivel="N4", teste=f"{titulo} por {', '.join(dims)}", grupos=len(j),
                              divergencias=div, esperado=float(esp.sum()), obtido=float(obt.sum()),
                              status="ok" if div == 0 else "divergente",
                              detalhe=None if div == 0 else f"Qlik x Databricks: {amostra}"))

# COMMAND ----------

# DBTITLE 1,Resultado da paridade
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
SCHEMA_PAR = StructType([StructField("objeto", StringType()), StructField("nivel", StringType()), StructField("teste", StringType()),
                         StructField("grupos", LongType()), StructField("divergencias", LongType()), StructField("esperado", DoubleType()),
                         StructField("obtido", DoubleType()), StructField("status", StringType()), StructField("detalhe", StringType())])
linhas_par = [(r.objeto, r.nivel, r.teste, None if r.grupos is None else int(r.grupos), None if r.divergencias is None else int(r.divergencias),
               None if r.esperado is None else float(r.esperado), None if r.obtido is None else float(r.obtido), r.status, r.detalhe) for r in resultados]
# histórico: a paridade roda a cada execução enquanto Qlik e Databricks convivem
(spark.createDataFrame(linhas_par, SCHEMA_PAR).withColumn("ts_execucao", F.current_timestamp())
 .write.mode("append").option("mergeSchema", True).saveAsTable(f"{C}.par_historico"))
spark.sql(f"""CREATE OR REPLACE VIEW {C}.par_resultado AS
              SELECT * FROM {C}.par_historico WHERE ts_execucao = (SELECT max(ts_execucao) FROM {C}.par_historico)""")
for t, k in [("par_historico", "TABLE"), ("par_resultado", "VIEW")]:
    spark.sql(tags_sql(k, f"{C}.{t}"))

res = spark.table(f"{C}.par_resultado").toPandas()
cor = {"ok": "#2B8A3E", "divergente": "#C2255C"}
linhas = "".join(
    f"<tr style='background:{'#F5F7F8' if i % 2 else '#FFFFFF'}'>"
    f"<td style='padding:10px 14px;font-weight:600'>{r.objeto}</td><td style='padding:10px 14px'>{r.nivel}</td>"
    f"<td style='padding:10px 14px'>{r.teste}</td><td style='padding:10px 14px;text-align:right'>{'' if pd.isna(r.divergencias) else int(r.divergencias)}</td>"
    f"<td style='padding:10px 14px;color:{cor.get(r.status, '#E8590C')};font-weight:700'>{r.status}</td></tr>"
    for i, r in res.iterrows())
ok = (res.status == "ok").sum()
_testados = int((res.status != "aguardando_publicacao").sum())
displayHTML(f"""<div style="font-family:'DM Sans',Arial,sans-serif;">
<h2 style="color:#1B3139;border-bottom:3px solid #FF3621;display:inline-block;padding-bottom:4px;">Paridade · {ok} de {_testados} testes ok</h2>
<table style="border-collapse:collapse;width:100%;font-size:14px;color:#495057;">
<tr style="background:#1B3139;color:#FFFFFF"><th style="padding:10px 14px;text-align:left">Objeto</th><th style="padding:10px 14px;text-align:left">Nível</th>
<th style="padding:10px 14px;text-align:left">Teste</th><th style="padding:10px 14px;text-align:right">Divergências</th><th style="padding:10px 14px;text-align:left">Status</th></tr>
{linhas}</table></div>""")

# COMMAND ----------

# DBTITLE 1,Registrar estado de cada objeto no controle
nome_por_mv = {}
for r in res.itertuples():
    nome_por_mv.setdefault(r.objeto, []).append(r.status)
for obj, sts in nome_por_mv.items():
    tipo = "objeto" if not obj.startswith("mv_") else "metric_view"
    estado = "paridade_ok" if all(x == "ok" for x in sts) else "paridade_divergente"
    if tipo == "objeto" and obj.endswith(".csv"):
        continue
    CTRL.estado(tipo, obj, estado, {"testes": len(sts), "ok": sum(x == "ok" for x in sts)})
aguardando = int((res.status == "aguardando_publicacao").sum())
testados = len(res) - aguardando
if testados == 0:
    CTRL.evento("08_paridade", f"nada publicado para comparar ainda ({aguardando} teste(s) aguardando publicação)", nivel="aviso")
else:
    CTRL.evento("08_paridade", f"{ok} de {testados} testes ok" + (f" · {aguardando} aguardando publicação" if aguardando else ""),
                nivel="info" if ok == testados else "aviso")
_etapa.__exit__(None, None, None)

# COMMAND ----------

# DBTITLE 1,Histórico de execuções da paridade
display(spark.sql(f"""SELECT ts_execucao, count(*) AS testes, sum(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok,
                      concat_ws(' · ', collect_list(CASE WHEN status <> 'ok' THEN concat(objeto, ' ', teste) END)) AS divergentes
                      FROM {C}.par_historico GROUP BY ts_execucao ORDER BY ts_execucao"""))

# COMMAND ----------

# DBTITLE 1,Investigação: amostra de linhas divergentes (quando houver)
for a in alvos:
    nome = a["objeto_destino"].split(".")[-1]
    r = res[(res.objeto == nome) & (res.nivel == "N3")]
    if len(r) and r.iloc[0].status != "ok":
        print(f"== {nome}")
        cols = [f.name for f in spark.table(a["objeto_destino"]).schema.fields]
        display(spark.sql(f"SELECT 'so_qlik' AS lado, * FROM (SELECT {', '.join(cols)} FROM {a['baseline']} EXCEPT ALL SELECT {', '.join(cols)} FROM {a['objeto_destino']}) LIMIT 10"))
