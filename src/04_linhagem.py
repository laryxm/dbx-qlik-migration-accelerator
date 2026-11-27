# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR QLIK · PASSO 4</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Linhagem, complexidade e destino de cada QVD</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">Lê os load scripts e os cabeçalhos XML dos QVDs, monta o grafo fonte, QVD e app, calcula o score de complexidade por script e decide a camada de destino de cada QVD. Os resultados ficam em tabelas do Unity Catalog e alimentam a conversão do passo 5.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Ler scripts e cabeçalhos
_etapa = CTRL.etapa("04_linhagem"); _etapa.__enter__()
import os, json
import qlik_parser as qp

scripts = {}
for f in sorted(os.listdir(DIR_SCRIPTS)):
    if f.lower().endswith(".qvs"):
        scripts[f[:-4]] = open(f"{DIR_SCRIPTS}/{f}", encoding="utf-8").read()
analises = {nome: qp.parse_script(nome, txt) for nome, txt in scripts.items()}

headers = {}
for f in sorted(os.listdir(DIR_QVD)):
    if f.lower().endswith(".qvd"):
        headers[f] = qp.parse_qvd_header(f"{DIR_QVD}/{f}")

for nome, a in analises.items():
    print(f"{nome:30s} papel={a['papel']:9s} score={a['score']:5} classe={a['classe']:6s} produz={len(a['qvds_produzidos'])} lê={len(a['qvds_lidos'])}")
print(f"\n{len(headers)} cabeçalhos de QVD lidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Destino de cada QVD
# MAGIC | Situação no script | Destino | Por quê |
# MAGIC |---|---|---|
# MAGIC | QVD gravado direto de `SQL SELECT` (script de extract) | Bronze, ingestão pelo Lakeflow Connect | A lógica incremental passa para o conector. O QVD deixa de existir. |
# MAGIC | QVD gravado a partir de outros QVDs com nome `Fato*` ou `Dim*` | Gold, materialized view no pipeline | É o modelo consumido pelos apps. |
# MAGIC | Demais QVDs de transformação | Silver, materialized view no pipeline | Entidade intermediária conformada. |
# MAGIC | App sem STORE | Camada semântica e consumo | Medidas viram metric view; Section Access vira row filter. |

# COMMAND ----------

# DBTITLE 1,Montar tabelas de linhagem
from pyspark.sql import Row

produtor = {}
for a in analises.values():
    for p in a["qvds_produzidos"]:
        produtor[p["qvd"]] = (a["script"], a["papel"], p)

consumidores = {}
for a in analises.values():
    for q in a["qvds_lidos"]:
        if produtor.get(q, (None,))[0] != a["script"]:
            consumidores.setdefault(q, []).append(a["script"])

def destino(qvd, papel, p):
    if papel == "extract":
        banco = [f[1] for f in p["fontes"] if f[0] == "BANCO"][0]
        tabela = banco.split(".")[-1]
        for pref in CONV["prefixos_tabela_origem_remover"]:
            if tabela.upper().startswith(pref.upper()):
                tabela = tabela[len(pref):]
        return "bronze", f"{BRONZE_FQN}.{CONV['prefixo_tabela_bronze']}{qp.snake(tabela)}"
    base = qvd[:-4]
    camada = "gold" if base.lower().startswith(tuple(x.lower() for x in CONV["prefixos_qvd_gold"])) else "silver"
    return camada, f"{CATALOG}.{SCH_GOLD if camada == 'gold' else SCH_SILVER}.{qp.snake(base)}"

prof_cache = {}
def profundidade(qvd):
    if qvd in prof_cache:
        return prof_cache[qvd]
    if qvd not in produtor:
        return 0
    _, papel, p = produtor[qvd]
    fontes_qvd = [f[1] for f in p["fontes"] if f[0] == "QVD"]
    prof_cache[qvd] = 1 + max([profundidade(q) for q in fontes_qvd] or [0])
    return prof_cache[qvd]

linhas_qvd, arestas = [], []
for qvd, (script, papel, p) in produtor.items():
    camada, objeto = destino(qvd, papel, p)
    h = headers.get(qvd, {})
    linhas_qvd.append(Row(
        qvd=qvd, script_produtor=script, papel_script=papel, camada_destino=camada, objeto_destino=objeto,
        profundidade=profundidade(qvd), fontes=[f"{f[0]}:{f[1]}" for f in p["fontes"]],
        campos_script=p["campos"], consumido_por=sorted(consumidores.get(qvd, [])),
        registros_header=h.get("registros"), lineage_header=h.get("lineage", [])))
    for f in p["fontes"]:
        arestas.append(Row(origem=f[1], tipo_origem=f[0], destino=qvd, tipo_destino="QVD", script=script))
for a in analises.values():
    if a["papel"] == "app":
        for q in a["qvds_lidos"]:
            arestas.append(Row(origem=q, tipo_origem="QVD", destino=a["script"], tipo_destino="APP", script=a["script"]))

spark.createDataFrame(linhas_qvd).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.lin_qvd")
spark.createDataFrame(arestas).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.lin_aresta")

spark.createDataFrame([Row(
    script=a["script"], papel=a["papel"], linhas=a["linhas"], score=float(a["score"]), classe=a["classe"],
    construcoes=json.dumps(a["construcoes"], ensure_ascii=False), qvds_produzidos=[p["qvd"] for p in a["qvds_produzidos"]],
    qvds_lidos=a["qvds_lidos"], alertas=a["alertas"]) for a in analises.values()]
).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.lin_script")

spark.createDataFrame([Row(
    script=s["script"], ordem=s["ordem"], tipo=s["tipo"], tabela=s["tabela"], prefixo=s["prefixo"],
    origem_tipo=s["origem_tipo"], origem=s["origem"], construcoes=s["construcoes"], texto=s["texto"])
    for a in analises.values() for s in a["statements"]]
).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.lin_statement")

spark.createDataFrame([Row(
    qvd=q, tabela=h["tabela"], creator_doc=h["creator_doc"], registros=h["registros"], lineage=h["lineage"],
    campo=c["campo"], campo_databricks=qp.nome_campo(c["campo"]), simbolos=c["simbolos"],
    tipo_numero=c["tipo_numero"], tags=c["tags"]) for q, h in headers.items() for c in h["campos"]]
).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.qvd_campo")

for t in ["lin_qvd", "lin_aresta", "lin_script", "lin_statement", "qvd_campo"]:
    spark.sql(tags_sql("TABLE", f"{C}.{t}"))

display(spark.sql(f"SELECT qvd, script_produtor, camada_destino, objeto_destino, profundidade, consumido_por, registros_header FROM {C}.lin_qvd ORDER BY profundidade, qvd"))

# COMMAND ----------

# DBTITLE 1,Score de complexidade por script
display(spark.sql(f"SELECT script, papel, linhas, score, classe, construcoes, size(alertas) AS alertas FROM {C}.lin_script ORDER BY score DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Alertas de semântica
# MAGIC Inclui os alertas do parser e as chaves sintéticas que o motor associativo criaria no app: tabelas carregadas no mesmo app que compartilham mais de um campo pelo nome. Os campos vêm do cabeçalho do QVD, então valem mesmo quando o script usa `LOAD *`.

# COMMAND ----------

# DBTITLE 1,Alertas e chaves sintéticas
alertas = [Row(script=a["script"], tipo="semantica", detalhe=m) for a in analises.values() for m in a["alertas"]]
for a in analises.values():
    if a["papel"] != "app":
        continue
    # campos efetivamente carregados: lista do LOAD quando explícita, cabeçalho do QVD quando LOAD *
    campos_por_qvd = {}
    for st in a["statements"]:
        if st["tipo"] == "LOAD" and st["origem_tipo"] == "QVD" and st["origem"] in headers:
            explicitos = {c for c in st["campos"] if c != "*"}
            campos_por_qvd[st["origem"]] = explicitos or {c["campo"] for c in headers[st["origem"]]["campos"]}
    qvds = sorted(campos_por_qvd)
    for i in range(len(qvds)):
        for j in range(i + 1, len(qvds)):
            comuns = sorted(campos_por_qvd[qvds[i]] & campos_por_qvd[qvds[j]])
            if len(comuns) > 1:
                alertas.append(Row(script=a["script"], tipo="chave_sintetica",
                    detalhe=f"{qvds[i]} e {qvds[j]} compartilham {', '.join(comuns)}. O Qlik cria $Syn. No Gold, declarar o relacionamento explícito pelo grão."))
            elif len(comuns) == 1:
                alertas.append(Row(script=a["script"], tipo="associacao",
                    detalhe=f"{qvds[i]} e {qvds[j]} associam por {comuns[0]}. Vira FK declarada no star schema."))
spark.createDataFrame(alertas).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.lin_alerta")
spark.sql(tags_sql("TABLE", f"{C}.lin_alerta"))
display(spark.table(f"{C}.lin_alerta"))

# COMMAND ----------

# DBTITLE 1,Section Access encontrado
sa = [Row(script=a["script"], access=r.get("ACCESS"), userid=r.get("USERID"),
          campo=[k for k in r if k not in ("ACCESS", "USERID", "PASSWORD", "NTNAME")][0],
          valor=[v for k, v in r.items() if k not in ("ACCESS", "USERID", "PASSWORD", "NTNAME")][0])
      for a in analises.values() for r in a["section_access"]]
if sa:
    spark.createDataFrame(sa).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.gov_section_access_qlik")
    spark.sql(tags_sql("TABLE", f"{C}.gov_section_access_qlik"))
    display(spark.table(f"{C}.gov_section_access_qlik"))

# COMMAND ----------

# DBTITLE 1,Conferência: linhagem do script contra o cabeçalho do QVD
display(spark.sql(f"""
SELECT qvd,
       transform(fontes, f -> split(f, ':')[1])                        AS fontes_pelo_script,
       transform(lineage_header, l -> element_at(split(l, '/'), -1))   AS fontes_pelo_cabecalho,
       CASE WHEN papel_script = 'extract' THEN 'n/a (fonte é banco)'
            WHEN array_sort(transform(fontes, f -> split(f, ':')[1])) = array_sort(transform(lineage_header, l -> element_at(split(l, '/'), -1)))
            THEN 'confere' ELSE 'divergente' END                       AS status
FROM {C}.lin_qvd ORDER BY profundidade, qvd
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 De trás para frente: linhagem de cada métrica
# MAGIC O planejamento começa pelo que o negócio usa. Cada medida mestra do app é decomposta nos campos que ela lê, e cada campo é rastreado através dos scripts até a coluna da tabela Oracle de origem. Com isso, o escopo da migração passa a ser o conjunto mínimo de fontes, QVDs e scripts que sustentam as métricas em uso. A construção segue depois no sentido inverso: bronze, silver, gold, metric view.

# COMMAND ----------

# DBTITLE 1,Linhagem por campo, do QVD de modelo até a coluna de origem
def resolver_campo(qvd, campo, visitados=()):
    if qvd not in produtor:
        return [{"tabela_origem": None, "coluna_origem": None, "caminho": [f"{qvd}.{campo}"], "transformacoes": [], "scripts": []}]
    script, papel, p = produtor[qvd]
    lc = p.get("linhagem_campos", {}).get(campo)
    origens = lc["origens"] if lc else [("QVD", q, campo) for q in p.get("estrela", [])]
    exprs = lc["exprs"] if lc else []
    passo = f"{qvd}.{campo}"
    saida = []
    for tipo, orig, col in origens:
        if tipo == "BANCO":
            saida.append({"tabela_origem": orig, "coluna_origem": col, "caminho": [passo, f"{orig}.{col}"],
                          "transformacoes": list(exprs), "scripts": [script]})
        elif tipo == "QVD" and orig not in visitados:
            for r in resolver_campo(orig, col, visitados + (qvd,)):
                saida.append({**r, "caminho": [passo] + r["caminho"],
                              "transformacoes": list(exprs) + [e for e in r["transformacoes"] if e not in exprs],
                              "scripts": [script] + [x for x in r["scripts"] if x != script]})
    return saida or [{"tabela_origem": None, "coluna_origem": None, "caminho": [passo], "transformacoes": list(exprs), "scripts": [script]}]

lin_campo = []
for qvd, (script, papel, p) in produtor.items():
    if papel == "extract":
        continue
    for campo in p["campos"]:
        for r in resolver_campo(qvd, campo):
            lin_campo.append(Row(qvd=qvd, campo=campo, coluna_databricks=qp.nome_campo(campo),
                                 tabela_origem=r["tabela_origem"], coluna_origem=r["coluna_origem"],
                                 caminho=" <- ".join(r["caminho"]), transformacoes=r["transformacoes"], scripts=r["scripts"]))
spark.createDataFrame([tuple(r) for r in lin_campo], "qvd STRING, campo STRING, coluna_databricks STRING, tabela_origem STRING, coluna_origem STRING, caminho STRING, transformacoes ARRAY<STRING>, scripts ARRAY<STRING>") \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.lin_campo")
spark.sql(tags_sql("TABLE", f"{C}.lin_campo"))
display(spark.sql(f"SELECT qvd, campo, tabela_origem, coluna_origem, transformacoes FROM {C}.lin_campo ORDER BY qvd, campo"))

# COMMAND ----------

# DBTITLE 1,Linhagem de cada medida mestra até o Oracle
app_json = [f for f in os.listdir(DIR_APPS) if f.endswith("_medidas.json")]
lin_medida = []
for f in app_json:
    app = json.load(open(f"{DIR_APPS}/{f}", encoding="utf-8"))
    a = analises.get(app["app"])
    campo_qvd = {}
    for st in a["statements"]:
        if st["tipo"] == "LOAD" and st["origem_tipo"] == "QVD" and st["origem"] in headers:
            explicitos = [c for c in st["campos"] if c != "*"]
            for c in (explicitos or [h["campo"] for h in headers[st["origem"]]["campos"]]):
                campo_qvd.setdefault(c, []).append(st["origem"])
    for m in app["medidas_mestras"]:
        for campo in qp.refs_campos(m["expression"]):
            for qvd in campo_qvd.get(campo, [None]):
                for r in (resolver_campo(qvd, campo) if qvd else [{"tabela_origem": None, "coluna_origem": None, "caminho": [campo], "transformacoes": [], "scripts": []}]):
                    lin_medida.append(Row(app=app["app"], qId=m["qId"], medida_qlik=m["title"], expressao_qlik=m["expression"],
                                          campo_qlik=campo, qvd_app=qvd, tabela_origem=r["tabela_origem"], coluna_origem=r["coluna_origem"],
                                          caminho=" <- ".join(r["caminho"]), transformacoes=r["transformacoes"], scripts=r["scripts"]))
spark.createDataFrame([tuple(r) for r in lin_medida], "app STRING, qId STRING, medida_qlik STRING, expressao_qlik STRING, campo_qlik STRING, qvd_app STRING, tabela_origem STRING, coluna_origem STRING, caminho STRING, transformacoes ARRAY<STRING>, scripts ARRAY<STRING>") \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.lin_medida")
spark.sql(tags_sql("TABLE", f"{C}.lin_medida"))
display(spark.sql(f"""
SELECT medida_qlik, campo_qlik, qvd_app,
       concat_ws(', ', collect_set(concat(tabela_origem, '.', coluna_origem))) AS colunas_oracle
FROM {C}.lin_medida GROUP BY ALL ORDER BY medida_qlik, campo_qlik
"""))

# COMMAND ----------

# DBTITLE 1,Escopo dirigido pelas métricas
display(spark.sql(f"""
WITH usados AS (
  SELECT DISTINCT tabela_origem, explode(scripts) AS script FROM {C}.lin_medida WHERE tabela_origem IS NOT NULL
)
SELECT
  (SELECT count(DISTINCT qId) FROM {C}.lin_medida)                                     AS medidas_analisadas,
  (SELECT count(DISTINCT tabela_origem) FROM usados)                                    AS tabelas_oracle_necessarias,
  (SELECT count(DISTINCT f) FROM {C}.lin_qvd LATERAL VIEW explode(fontes) t AS f WHERE f LIKE 'BANCO:%') AS tabelas_oracle_nos_scripts,
  (SELECT count(DISTINCT script) FROM usados)                                            AS scripts_necessarios,
  (SELECT count(*) FROM {C}.lin_script WHERE papel <> 'app')                            AS scripts_geradores
"""))

# COMMAND ----------

# DBTITLE 1,SQL de origem com regra de negócio (ponto de atenção)
sql_regras = [Row(script=a["script"], tabela_qlik=r["tabela"], tabela_origem=r["origem"],
                  construcoes=r["construcoes"], funcoes=r["funcoes"], trecho=r["texto"])
              for a in analises.values() for r in a.get("sql_origem_regras", [])]
schema_sql = "script STRING, tabela_qlik STRING, tabela_origem STRING, construcoes ARRAY<STRING>, funcoes ARRAY<STRING>, trecho STRING"
spark.createDataFrame([tuple(r) for r in sql_regras], schema_sql).write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.lin_sql_origem")
spark.sql(tags_sql("TABLE", f"{C}.lin_sql_origem"))
display(spark.table(f"{C}.lin_sql_origem"))

# COMMAND ----------

# DBTITLE 1,Grafo de linhagem
def no(x):
    return "n_" + "".join(ch if ch.isalnum() else "_" for ch in x)

cores = {"bronze": "#CD7F32", "silver": "#9AA5AE", "gold": "#D4A017"}
linhas = ["flowchart LR"]
for r in spark.table(f"{C}.lin_aresta").collect():
    o = f'{no(r.origem)}["{r.origem}"]' if r.tipo_origem != "BANCO" else f'{no(r.origem)}[("{r.origem}")]'
    d = f'{no(r.destino)}["{r.destino}"]' if r.tipo_destino != "APP" else f'{no(r.destino)}{{{{"{r.destino}"}}}}'
    linhas.append(f"  {o} --> {d}")
for r in spark.table(f"{C}.lin_qvd").collect():
    linhas.append(f"  style {no(r.qvd)} fill:{cores[r.camada_destino]},color:#1B3139")
mermaid = "\n".join(linhas)

displayHTML(f"""
<div style="font-family:'DM Sans',Arial,sans-serif;color:#495057;font-size:14px;margin-bottom:8px;">
Cilindro = tabela de banco · bronze, prata e ouro = camada de destino do QVD · hexágono = app
</div>
<pre class="mermaid">{mermaid}</pre>
<script type="module">
import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
mermaid.initialize({{ startOnLoad: true, theme: 'neutral' }});
</script>
""")

# COMMAND ----------

# DBTITLE 1,Registrar estados e resumo no controle
n_alertas = spark.table(f"{C}.lin_alerta").count()
for r in spark.table(f"{C}.lin_qvd").collect():
    CTRL.estado("qvd", r.qvd, "linhagem_ok", {"camada": r.camada_destino, "destino": r.objeto_destino, "script": r.script_produtor})
for r in spark.table(f"{C}.lin_script").collect():
    CTRL.estado("script", r.script, "linhagem_ok", {"papel": r.papel, "score": r.score, "classe": r.classe, "alertas": len(r.alertas or [])})
CTRL.evento("04_linhagem", f"{len(analises)} scripts, {len(produtor)} QVDs, {len(lin_medida)} ligações medida-origem, {n_alertas} alertas")
_etapa.__exit__(None, None, None)
