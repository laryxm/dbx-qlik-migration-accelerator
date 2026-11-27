# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR QLIK · PASSO 7</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Medidas mestras para metric views e Section Access para Unity Catalog</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">As medidas mestras e expressões de Set Analysis exportadas do app viram metric views sobre o Gold, prontas para AI/BI, Genie e Power BI. A tabela de Section Access vira tabela de acesso e função de row filter, com teste por perfil comparando o que cada usuário via no Qlik.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Contexto
_etapa = CTRL.etapa("07_semantica_seguranca"); _etapa.__enter__()
import json, time, re, os, html
import qlik_conversor as qc
from pyspark.sql import Row

APPS = [json.load(open(f"{DIR_APPS}/{f}", encoding="utf-8")) for f in sorted(os.listdir(DIR_APPS)) if f.endswith("_medidas.json")]
gold = {}
for r in spark.sql(f"""SELECT table_name, column_name, full_data_type FROM {CATALOG}.information_schema.columns
                       WHERE table_schema = '{SCH_GOLD}' ORDER BY table_name, ordinal_position""").collect():
    gold.setdefault(r.table_name, []).append((r.column_name, r.full_data_type))
mapa_qvd = {r.qvd: r.objeto_destino for r in spark.table(f"{C}.lin_qvd").collect()}
print(f"{len(APPS)} app(s) · {sum(len(a['medidas_mestras']) for a in APPS)} medidas mestras · tabelas gold: {sorted(gold)}")
if not gold:
    CTRL.evento("07_semantica_seguranca", "nenhuma tabela gold publicada ainda: aprove objetos na revisão e rode o passo 06", nivel="aviso")
    _etapa.__exit__(None, None, None)
    dbutils.notebook.exit("sem gold publicado")

# COMMAND ----------

# DBTITLE 1,Prompt de conversão de medidas
SYSTEM_MV = """Você converte medidas mestras e Set Analysis do Qlik em Unity Catalog Metric Views (YAML versão 1.1).
Responda SOMENTE com JSON válido:
{
  "metric_views": [{"nome": "mv_<processo>", "yaml": "<conteúdo YAML completo>"}],
  "mapa_medidas": [{"qId": "m01", "titulo": "título Qlik", "metric_view": "mv_...", "medida": "nome_medida",
                    "status": "convertida" | "convertida_com_ressalva" | "nao_convertida", "observacao": "..."}]
}
Regras:
M1. Uma metric view por processo de negócio, não por app. Medidas sobre a tabela de sinistralidade vão numa metric view própria.
M2. YAML no formato:
version: 1.1
comment: "..."
source: <catalogo.schema.tabela_fato>
joins:
  - name: <alias>
    source: <catalogo.schema.dim>
    on: source.<chave> = <alias>.<chave>
dimensions:
  - name: <snake_case>
    expr: <coluna ou expressão SQL>
    display_name: "<nome amigável em português>"
    synonyms: ["...", "..."]
measures:
  - name: <snake_case>
    expr: <agregação SQL>
    display_name: "<título Qlik>"
    synonyms: ["..."]
M3. Set Analysis {<Campo={'v'}>} vira agregação com FILTER (WHERE campo = 'v'). Use os nomes de coluna do Gold.
M4. Razões (Glosa %, per capita, sinistralidade) são medidas compostas: MEASURE(a) / MEASURE(b), nunca média de razões.
M5. Expressões dependentes da seleção do usuário (ex.: $(=Max(...))) que não têm equivalente direto: marque
    "convertida_com_ressalva" e proponha a alternativa mais próxima, explicando na observação.
M6. Inclua como dimensões as colunas de filtro relevantes (competência, tipo de guia, grupo de procedimento, tipo de prestador, regional, plano).
M7. Nomes de medidas em snake_case ASCII. display_name igual ao título Qlik.
M8. Dimensão que é uma coluna direta do Gold tem name IGUAL ao nome da coluna (ex.: competencia, tipo_guia).
"""

def user_mv(app):
    return ("MEDIDAS MESTRAS EXPORTADAS DO APP " + app["app"] + "\n" + json.dumps(app["medidas_mestras"], ensure_ascii=False, indent=1)
            + "\n\nMAPEAMENTO DOS QVDs DO APP PARA O GOLD\n" + "\n".join(f"- {q} -> {o}" for q, o in mapa_qvd.items() if f".{SCH_GOLD}." in o)
            + "\n\nCOLUNAS DO GOLD\n" + "\n".join(f"- {CATALOG}.{SCH_GOLD}.{t}(" + ", ".join(f"{c} {ty}" for c, ty in cols) + ")" for t, cols in gold.items())
            + "\n\nNomes Qlik com espaço ou acento estão normalizados no Gold: [Tipo Guia] = tipo_guia, [Competência] = competencia.")

def chamar(msgs):
    return qc.chamar_modelo_detalhado(LLM_ENDPOINT, msgs, max_tokens=12000, rotulo="metric_view")["texto"]

# COMMAND ----------

# DBTITLE 1,Gerar, criar e testar as metric views
def criar_e_testar(saida):
    falhas = []
    for mv in saida["metric_views"]:
        nome = f"{CATALOG}.{SCH_GOLD}.{mv['nome']}"
        try:
            spark.sql(f"CREATE OR REPLACE VIEW {nome} WITH METRICS LANGUAGE YAML AS $$\n{mv['yaml']}\n$$")
        except Exception as e:
            falhas.append((mv["nome"], "nao_cria", str(e).split("\n")[0][:500]))
    for m in saida["mapa_medidas"]:
        if m["status"] == "nao_convertida":
            continue
        try:
            spark.sql(f"SELECT MEASURE(`{m['medida']}`) FROM {CATALOG}.{SCH_GOLD}.{m['metric_view']}").collect()
        except Exception as e:
            falhas.append((m["medida"], "nao_consulta", str(e).split("\n")[0][:300]))
    return falhas

def falhou_criar(falhas):
    return {f[0] for f in falhas if f[1] == "nao_cria"}

SYSTEM_MV_FINAL = SYSTEM_MV + "\nREGRAS APRENDIDAS NA PARIDADE\n" + "\n".join(qc.REGRAS_APRENDIDAS_MV)
print(f"{len(qc.REGRAS_APRENDIDAS_MV)} regra(s) aprendida(s) aplicada(s) ao prompt")
todas_mv, linhas_sem = [], []
for app in APPS:
    msgs = [{"role": "system", "content": SYSTEM_MV_FINAL}, {"role": "user", "content": user_mv(app)}]
    for tentativa in range(1, IA["tentativas"] + 1):
        resposta = chamar(msgs)
        saida = qc.extrair_json(resposta)
        falhas = criar_e_testar(saida)
        print(f"{app['app']} · tentativa {tentativa}: {len(saida['metric_views'])} metric views · {len(falhas)} falhas")
        for f in falhas:
            print("   ", f)
        if not falhas:
            break
        msgs += [{"role": "assistant", "content": resposta},
                 {"role": "user", "content": "Erros ao criar ou consultar:\n" + "\n".join(f"- {a}: {b} · {c}" for a, b, c in falhas)
                  + "\nCorrija e devolva o JSON completo."}]
    for mv in saida["metric_views"]:
        if mv["nome"] in falhou_criar(falhas):
            continue
        spark.sql(tags_sql("VIEW", f"{CATALOG}.{SCH_GOLD}.{mv['nome']}"))
        todas_mv.append(mv)
    falhou = {f[0] for f in falhas}
    for m in saida["mapa_medidas"]:
        teste = "falhou" if m["medida"] in falhou else ("nao_aplicavel" if m["status"] == "nao_convertida" else "ok")
        linhas_sem.append((app["app"], m["qId"], m["titulo"], m["metric_view"], m["medida"], m["status"], m.get("observacao"), teste))
        CTRL.estado("medida", f"{app['app']}/{m['qId']}", "convertido" if teste == "ok" else "bloqueado",
                    {"titulo": m["titulo"], "metric_view": m["metric_view"], "status": m["status"], "teste": teste})
    CTRL.evento("07_semantica_seguranca", f"{app['app']}: {len(saida['metric_views'])} metric views, {len(falhas)} falhas", objeto=app["app"], tipo_objeto="app")
saida = {"metric_views": todas_mv}
spark.createDataFrame(linhas_sem, "app STRING, qId STRING, titulo STRING, metric_view STRING, medida STRING, status STRING, observacao STRING, teste STRING") \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.sem_medida")
spark.sql(tags_sql("TABLE", f"{C}.sem_medida"))
display(spark.table(f"{C}.sem_medida"))

# COMMAND ----------

# DBTITLE 1,YAML gerado
import html
displayHTML("".join(
    f"<div style=\"font-family:'DM Sans',Arial,sans-serif;color:#1B3139;font-size:15px;font-weight:700;margin:12px 0 6px;\">{mv['nome']}</div>"
    f"<pre style='background:#F5F7F8;padding:16px;border-radius:8px;font-size:12px;line-height:1.45;white-space:pre-wrap;'>{html.escape(mv['yaml'])}</pre>"
    for mv in saida["metric_views"]))

# COMMAND ----------

# DBTITLE 1,Consulta de exemplo na metric view
# Pega a primeira metric view, uma dimensão e até três medidas testadas, só para mostrar a sintaxe
ok = spark.sql(f"SELECT metric_view, medida FROM {C}.sem_medida WHERE teste = 'ok'").collect()
if ok:
    mv0 = ok[0].metric_view
    meds = [r.medida for r in ok if r.metric_view == mv0][:3]
    mv_yaml = next(m["yaml"] for m in todas_mv if m["nome"] == mv0)
    dim0 = re.search(r"dimensions:\s*\n\s*-\s*name:\s*(\w+)", mv_yaml).group(1)
    display(spark.sql(f"SELECT `{dim0}`, " + ", ".join(f"MEASURE(`{m}`) AS `{m}`" for m in meds)
                      + f" FROM {CATALOG}.{SCH_GOLD}.{mv0} GROUP BY ALL ORDER BY 2 DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Glossário de negócio e linhagem de ponta a ponta
# MAGIC As regras deixam de existir só dentro do app. Cada termo de negócio ganha definição, fórmula, regra herdada do Qlik e origem até a coluna Oracle. O documento gerado aqui é importado em lote nas **Pages** do Unity Catalog pelo Genie Code (Discover, Pages, Bulk import pages), onde o Genie One passa a usar a definição oficial ao responder.

# COMMAND ----------

# DBTITLE 1,Linhagem de ponta a ponta por medida
spark.sql(f"""
CREATE OR REPLACE VIEW {C}.lin_medida_completa AS
SELECT l.app, l.qId, l.medida_qlik, l.expressao_qlik, s.metric_view, s.medida AS medida_databricks, s.status AS status_conversao,
       l.campo_qlik, q.objeto_destino AS tabela_gold, lower(regexp_replace(translate(l.campo_qlik,
         'áàâãéêíóôõúçÁÀÂÃÉÊÍÓÔÕÚÇ', 'aaaaeeiooouc' || 'AAAAEEIOOOUC'), '[^A-Za-z0-9]+', '_')) AS coluna_gold,
       l.tabela_origem, l.coluna_origem, l.transformacoes, l.caminho
FROM {C}.lin_medida l
LEFT JOIN {C}.sem_medida s ON s.qId = l.qId AND s.app = l.app
LEFT JOIN {C}.lin_qvd q ON q.qvd = l.qvd_app
""")
spark.sql(tags_sql("VIEW", f"{C}.lin_medida_completa"))
display(spark.sql(f"""
SELECT medida_qlik, metric_view, medida_databricks, collect_set(tabela_gold) AS tabelas_gold,
       collect_set(concat(tabela_origem, '.', coluna_origem)) AS colunas_oracle
FROM {C}.lin_medida_completa GROUP BY ALL ORDER BY medida_qlik
"""))

# COMMAND ----------

# DBTITLE 1,Gerar o glossário para as Pages (um documento por app)
lin = spark.table(f"{C}.lin_medida_completa").collect()
doc = spark.table(f"{C}.doc_coluna").collect()
dir_docs = f"{DIR_SAIDA}/docs"
os.makedirs(dir_docs, exist_ok=True)
os.makedirs(f"{WS_DIR}/docs_gerados", exist_ok=True)

todos_termos = []
for app in APPS:
    termos = []
    for m in app["medidas_mestras"]:
        linhas_m = [r for r in lin if r.qId == m["qId"] and r.app == app["app"]]
        if not linhas_m:
            continue
        r0 = linhas_m[0]
        origens = sorted({f"{r.tabela_origem}.{r.coluna_origem}" for r in linhas_m if r.tabela_origem})
        # renomeação simples é linhagem, não regra de negócio
        regras = sorted({t for r in linhas_m for t in (r.transformacoes or []) if not re.fullmatch(r"[\w\.\[\] ]+", t)})
        termos.append({"app": app["app"], "termo": m["title"], "tipo": "Medida", "definicao": m.get("description", ""),
                       "formula_qlik": m["expression"], "objeto": f"{r0.metric_view}.{r0.medida_databricks}",
                       "status": r0.status_conversao, "origens": origens, "regras": regras})
    for d in doc:
        if d.regra and d.camada == "gold":
            termos.append({"app": app["app"], "termo": d.coluna, "tipo": f"Atributo de {d.objeto}", "definicao": d.descricao,
                           "formula_qlik": None, "objeto": f"{SCH_GOLD}.{d.objeto}.{d.coluna}", "status": "documentado",
                           "origens": [], "regras": [d.regra]})
    titulo = app.get("dominio") or app["app"]
    linhas_md = [f"# Glossário · {titulo}", "",
                 f"Gerado pelo acelerador de migração Qlik a partir do app {app['app']} e dos scripts geradores de QVD. "
                 "Revisar com o key user antes de publicar.", ""]
    for t in termos:
        linhas_md += [f"## {t['termo']}", "", f"**Tipo:** {t['tipo']}", "", t["definicao"] or "", ""]
        linhas_md += [f"**Onde consultar:** `{t['objeto']}`", ""]
        if t["formula_qlik"]:
            linhas_md += [f"**Fórmula no Qlik:** `{t['formula_qlik']}`", ""]
        if t["regras"]:
            linhas_md += ["**Regras herdadas:**"] + [f"- `{x}`" if t["tipo"] == "Medida" else f"- {x}" for x in t["regras"]] + [""]
        if t["origens"]:
            linhas_md += ["**Origem:** " + ", ".join(f"`{o}`" for o in t["origens"]), ""]
        if t["status"] == "convertida_com_ressalva":
            linhas_md += ["**Atenção:** conversão com ressalva, validar com o key user.", ""]
    markdown = "\n".join(linhas_md)
    nome_md = f"glossario_{app['app']}.md"
    for destino in (f"{dir_docs}/{nome_md}", f"{WS_DIR}/docs_gerados/{nome_md}"):
        with open(destino, "w", encoding="utf-8") as f:
            f.write(markdown)
    todos_termos += termos
    print(f"{app['app']}: {len(termos)} termos · {dir_docs}/{nome_md}")
    displayHTML(f"<pre style='background:#F5F7F8;padding:16px;border-radius:8px;font-size:12px;white-space:pre-wrap;'>{html.escape(markdown[:3000])}</pre>")

spark.createDataFrame([(t["app"], t["termo"], t["tipo"], t["definicao"], t["formula_qlik"], t["objeto"], t["status"], t["origens"], t["regras"]) for t in todos_termos],
                      "app STRING, termo STRING, tipo STRING, definicao STRING, formula_qlik STRING, objeto STRING, status STRING, origens ARRAY<STRING>, regras ARRAY<STRING>") \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.doc_glossario")
spark.sql(tags_sql("TABLE", f"{C}.doc_glossario"))
CTRL.evento("07_semantica_seguranca", f"glossário com {len(todos_termos)} termos em {dir_docs}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Section Access para Unity Catalog
# MAGIC No Qlik, `*` na tabela de acesso significa todos os valores **listados na própria coluna** da tabela de acesso, não todos os valores existentes. Se uma regional não aparece em nenhuma linha, ninguém a vê, nem quem tem `*`. O teste abaixo reproduz essa regra e mostra o impacto, para o time decidir se é intencional antes de migrar.

# COMMAND ----------

# DBTITLE 1,Existe Section Access nos scripts?
if not spark.catalog.tableExists(f"{C}.gov_section_access_qlik"):
    CTRL.evento("07_semantica_seguranca", "nenhum Section Access encontrado nos scripts; segurança por linha não gerada", nivel="aviso")
    _etapa.__exit__(None, None, None)
    dbutils.notebook.exit("sem Section Access")

# COMMAND ----------

# DBTITLE 1,Tabela de acesso e função de row filter
spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gov_acesso_regional
COMMENT 'Tabela de acesso consolidada a partir do Section Access do app Qlik. Usuário Qlik mapeado para grupo do IdP.'
AS
SELECT script AS app_origem, access AS perfil_qlik, userid AS usuario_qlik,
       concat('{SEG["prefixo_grupo_idp"]}', lower(regexp_replace(element_at(split(userid, '\\\\\\\\'), -1), '[^A-Za-z0-9]', '_'))) AS grupo_idp,
       campo, valor
FROM {C}.gov_section_access_qlik
""")
spark.sql(tags_sql("TABLE", f"{C}.gov_acesso_regional"))

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gov_acesso_regional_efetivo
COMMENT 'Acesso efetivo por grupo e regional. O * do Qlik é expandido para os valores listados na tabela de acesso.'
AS
WITH listados AS (SELECT DISTINCT valor FROM {C}.gov_acesso_regional WHERE valor <> '*')
SELECT DISTINCT a.grupo_idp, a.usuario_qlik, CASE WHEN a.valor = '*' THEN l.valor ELSE a.valor END AS valor_liberado
FROM {C}.gov_acesso_regional a
LEFT JOIN listados l ON a.valor = '*'
""")
spark.sql(tags_sql("TABLE", f"{C}.gov_acesso_regional_efetivo"))

spark.sql(f"""
CREATE OR REPLACE FUNCTION {C}.rf_regional(p_regional STRING)
RETURNS BOOLEAN
COMMENT 'Row filter para o Gold: usuário vê as regionais liberadas para os grupos do IdP dos quais é membro.'
RETURN is_account_group_member('{SEG["grupo_admin"]}')
    OR EXISTS (SELECT 1 FROM {C}.gov_acesso_regional_efetivo e
               WHERE e.valor_liberado = p_regional AND is_account_group_member(e.grupo_idp))
""")
display(spark.table(f"{C}.gov_acesso_regional_efetivo"))

# COMMAND ----------

# DBTITLE 1,Teste por perfil
fato = f"{CATALOG}.{SCH_GOLD}.{SEG['tabela_alvo']}"
COL, VAL = SEG["coluna_filtro"], SEG["coluna_valor_teste"]
if not spark.catalog.tableExists(fato):
    CTRL.evento("07_semantica_seguranca", f"{fato} ainda não foi publicada: o teste por perfil roda depois que ela for aprovada e publicada "
                "(a tabela de acesso e a função de row filter já foram criadas)", nivel="aviso")
    _etapa.__exit__(None, None, None)
    dbutils.notebook.exit("tabela alvo do row filter ainda não publicada")
spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gov_teste_perfil AS
WITH perfis AS (SELECT DISTINCT usuario_qlik, grupo_idp FROM {C}.gov_acesso_regional),
listados AS (SELECT DISTINCT valor FROM {C}.gov_acesso_regional WHERE valor <> '*'),
esperado_qlik AS (
  SELECT p.usuario_qlik, count(*) AS linhas_qlik, round(sum(f.{VAL}), 2) AS custo_qlik
  FROM perfis p
  JOIN {C}.gov_acesso_regional a ON a.usuario_qlik = p.usuario_qlik
  JOIN {fato} f ON f.{COL} = a.valor OR (a.valor = '*' AND f.{COL} IN (SELECT valor FROM listados))
  GROUP BY p.usuario_qlik
),
regra_uc AS (
  SELECT e.usuario_qlik, count(*) AS linhas_uc, round(sum(f.{VAL}), 2) AS custo_uc
  FROM {C}.gov_acesso_regional_efetivo e JOIN {fato} f ON f.{COL} = e.valor_liberado
  GROUP BY e.usuario_qlik
)
SELECT p.usuario_qlik, p.grupo_idp, e.linhas_qlik, r.linhas_uc, e.custo_qlik, r.custo_uc,
       CASE WHEN e.linhas_qlik = r.linhas_uc AND e.custo_qlik = r.custo_uc THEN 'igual' ELSE 'divergente' END AS status
FROM perfis p LEFT JOIN esperado_qlik e USING (usuario_qlik) LEFT JOIN regra_uc r USING (usuario_qlik)
""")
spark.sql(tags_sql("TABLE", f"{C}.gov_teste_perfil"))
display(spark.table(f"{C}.gov_teste_perfil"))

display(spark.sql(f"""
SELECT {COL}, count(*) AS linhas, round(sum({VAL}), 2) AS valor,
       {COL} IN (SELECT valor FROM {C}.gov_acesso_regional) AS listada_no_section_access
FROM {fato} GROUP BY ALL ORDER BY {COL}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Aplicação no Gold
# MAGIC Com os grupos criados no IdP e sincronizados por SCIM, a função entra na definição da tabela publicada pelo pipeline:
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW <catalogo>.<prefixo>_gold.<seguranca.tabela_alvo>
# MAGIC WITH ROW FILTER <catalogo>.<prefixo>_ctrl.rf_regional ON (<seguranca.coluna_filtro>)
# MAGIC AS SELECT ...
# MAGIC ```
# MAGIC A regra passa a valer para dashboards, Genie, notebooks, Power BI e APIs. Metric views herdam o filtro da tabela de origem.

# COMMAND ----------

# DBTITLE 1,Registrar no controle
perfis = spark.table(f"{C}.gov_teste_perfil").collect()
div = [r.usuario_qlik for r in perfis if r.status != "igual"]
for r in perfis:
    CTRL.estado("perfil_acesso", r.usuario_qlik, "paridade_ok" if r.status == "igual" else "paridade_divergente",
                {"grupo_idp": r.grupo_idp, "linhas_qlik": r.linhas_qlik, "linhas_uc": r.linhas_uc})
CTRL.evento("07_semantica_seguranca", f"Section Access: {len(perfis)} perfis testados, {len(div)} divergentes",
            nivel="aviso" if div else "info", detalhe={"divergentes": div})
_etapa.__exit__(None, None, None)
