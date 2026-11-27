# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR QLIK · PASSO 5</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Conversão assistida por IA, com nota de confiança</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">Scripts de extract viram especificação de ingestão. Scripts de transformação viram materialized views geradas por um modelo hospedado no Databricks a partir do script e dos metadados. Cada objeto passa por lint, compilação em sandbox, contrato de colunas e contagem contra o cabeçalho do QVD, e recebe uma nota de 0 a 100. Nota alta segue sozinha; nota baixa vai para a fila de revisão no app.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:6px solid #5F3DC4;background:#F5F7F8;border-radius:0 8px 8px 0;padding:18px 22px;font-family:'DM Sans',Arial,sans-serif;color:#495057;font-size:15px;">
# MAGIC <div style="color:#5F3DC4;font-weight:700;letter-spacing:1px;font-size:13px;">GUARDRAILS DA CONVERSÃO</div>
# MAGIC <b>1. Entrada mínima.</b> O modelo recebe só o texto do script, nomes de tabelas, colunas e tipos. Nenhuma linha de dado.<br>
# MAGIC <b>2. Modelo dentro da plataforma.</b> Endpoint de foundation model do Databricks, governável pelo Unity AI Gateway.<br>
# MAGIC <b>3. Lint anti-viés.</b> Bloqueia DDL no SELECT, UDF, chave não determinística, particionamento manual, catálogo fixo e referência a tabela inexistente.<br>
# MAGIC <b>4. Compilação em sandbox.</b> Cada objeto vira uma view no schema sandbox antes de ir para o pipeline.<br>
# MAGIC <b>5. Contrato de colunas e contagem.</b> Colunas de saída iguais aos campos do QVD e contagem igual ao NoOfRecords do cabeçalho.<br>
# MAGIC <b>6. Correção automática.</b> Falhou, o erro volta para o modelo (quantidade de rodadas em <code>ia.tentativas</code>).<br>
# MAGIC <b>7. Nota de confiança.</b> Rodadas de correção, alertas de semântica, premissas e violações de qualidade descontam pontos. Abaixo de <code>revisao.nota_minima_auto</code>, um humano aprova no app.<br>
# MAGIC <b>8. Rastreabilidade.</b> Cada chamada ao modelo fica no MLflow (trace) e na tabela <code>conv_auditoria_llm</code>, com tokens. Respostas iguais são reaproveitadas do cache.
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Contexto da conversão
_etapa = CTRL.etapa("05_conversao_ia"); _etapa.__enter__()
import json, re, time, os, hashlib
import qlik_parser as qp
import qlik_conversor as qc
from pyspark.sql import Row, functions as F

lin_qvd = [r.asDict() for r in spark.table(f"{C}.lin_qvd").collect()]
lin_script = {r.script: r.asDict() for r in spark.table(f"{C}.lin_script").collect()}
campos_qvd = {}
for r in spark.sql(f"SELECT qvd, campo, campo_databricks, registros FROM {C}.qvd_campo").collect():
    campos_qvd.setdefault(r.qvd, {"campos": [], "registros": r.registros})["campos"].append((r.campo, r.campo_databricks))

cols_bronze = {}
for r in spark.sql(f"""SELECT table_name, column_name, full_data_type FROM {CAT_BRONZE}.information_schema.columns
                       WHERE table_schema = '{SCH_BRONZE}' ORDER BY table_name, ordinal_position""").collect():
    cols_bronze.setdefault(r.table_name, []).append((r.column_name, r.full_data_type))

def ref_objeto(q):
    return "${" + q["camada_destino"] + "}." + q["objeto_destino"].split(".")[-1]

mapa_qvd = {q["qvd"]: q for q in lin_qvd}

# MLflow: um experimento por instalação do acelerador, para os traces das chamadas ao modelo
try:
    import mlflow
    EXPERIMENTO = IA.get("experimento_mlflow") or f"/Users/{AMB['dono']}/qlik_acelerador_{PREFIXO}"
    mlflow.set_experiment(EXPERIMENTO)
    print(f"Traces em {EXPERIMENTO}")
except Exception as e:
    EXPERIMENTO = None
    print(f"MLflow indisponível, seguindo sem trace: {e}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {C}.conv_cache (chave STRING, endpoint STRING, resposta STRING,
              tokens_entrada BIGINT, tokens_saida BIGINT, ts TIMESTAMP) COMMENT 'Cache de respostas do modelo por hash do prompt'""")
print(f"{len(lin_qvd)} QVDs mapeados · {len(cols_bronze)} tabelas no bronze {BRONZE_FQN} · modelo {LLM_ENDPOINT} · cache {'ligado' if IA['usar_cache'] else 'desligado'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Scripts de extract: especificação de ingestão
# MAGIC O QVD de extract some e vira uma tabela de ingestão no Lakeflow Connect. A chave primária é inferida do cabeçalho do QVD (campo com número de símbolos igual ao número de registros e prefixo em `conversao.prefixos_chave_primaria`) e a coluna de cursor vem de `conversao.colunas_cursor`. O modo padrão é consulta com carga D-1; CDC entra como evolução. SQL de origem com função ou join é sinalizado para decisão antes da ingestão.

# COMMAND ----------

# DBTITLE 1,Especificação de ingestão
sql_origem = {r.tabela_origem: r for r in spark.table(f"{C}.lin_sql_origem").collect()}
rx_pk = "^(" + "|".join(re.escape(x) for x in CONV["prefixos_chave_primaria"]) + ")"
ingestao = []
for q in lin_qvd:
    if q["papel_script"] != "extract":
        continue
    h = campos_qvd.get(q["qvd"], {"campos": [], "registros": 0})
    simbolos = {r.campo: r.simbolos for r in spark.sql(f"SELECT campo, simbolos FROM {C}.qvd_campo WHERE qvd = '{q['qvd']}'").collect()}
    pk = [c for c, s_ in simbolos.items() if s_ == h["registros"] and re.match(rx_pk, c)]
    cursor = [c for c in simbolos if c in CONV["colunas_cursor"]]
    script = lin_script[q["script_produtor"]]
    deletes = "INNER_JOIN" in json.loads(script["construcoes"]) and bool(cursor)
    origem = [f.split(":", 1)[1] for f in q["fontes"] if f.startswith("BANCO:")][0]
    regra = sql_origem.get(origem)
    ingestao.append((
        q["qvd"], origem, q["objeto_destino"], pk[:1], cursor[:1],
        "consulta incremental por cursor, D-1" if cursor else "consulta completa, D-1",
        "CDC quando liberado no banco" if cursor else "manter consulta completa (tabela pequena)",
        "detectar por CDC ou por snapshot de chaves" if deletes else "não tratada no script",
        ("atenção: " + ", ".join(regra.construcoes) + (" · funções " + ", ".join(regra.funcoes) if regra.funcoes else "")) if regra else "cópia direta",
        int(h["registros"] or 0)))
spark.createDataFrame(ingestao, "qvd STRING, tabela_origem STRING, tabela_bronze STRING, chave_primaria ARRAY<STRING>, coluna_cursor ARRAY<STRING>, modo_inicial STRING, evolucao STRING, delecoes STRING, sql_de_origem STRING, registros_qvd BIGINT") \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.conv_ingestao")
spark.sql(tags_sql("TABLE", f"{C}.conv_ingestao"))
display(spark.table(f"{C}.conv_ingestao"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Scripts de transformação: geração, validação, correção e nota

# COMMAND ----------

# DBTITLE 1,Funções de chamada ao modelo (com cache) e validação
SCHEMAS_SANDBOX = {"bronze": BRONZE_FQN, "silver": f"{CATALOG}.{SCH_SANDBOX}", "gold": f"{CATALOG}.{SCH_SANDBOX}"}
CATALOGOS_PROIBIDOS = {CATALOG, CAT_BRONZE}

def chamar_llm(mensagens):
    """Devolve (texto, tokens_entrada, tokens_saida, segundos, veio_do_cache)."""
    chave = qc.chave_cache(LLM_ENDPOINT, mensagens)
    if IA["usar_cache"]:
        hit = spark.sql(f"SELECT resposta, tokens_entrada, tokens_saida FROM {C}.conv_cache WHERE chave = '{chave}' LIMIT 1").collect()
        if hit:
            return hit[0].resposta, int(hit[0].tokens_entrada or 0), int(hit[0].tokens_saida or 0), 0.0, True
    r = qc.chamar_modelo_detalhado(LLM_ENDPOINT, mensagens, max_tokens=IA["max_tokens"], rotulo="conversao_script")
    spark.createDataFrame([(chave, LLM_ENDPOINT, r["texto"], r["tokens_entrada"], r["tokens_saida"])],
                          "chave STRING, endpoint STRING, resposta STRING, tokens_entrada BIGINT, tokens_saida BIGINT") \
         .selectExpr("*", "current_timestamp() AS ts").write.mode("append").saveAsTable(f"{C}.conv_cache")
    return r["texto"], r["tokens_entrada"], r["tokens_saida"], r["segundos"], False

def validar(obj, esperado, parametros, objetos_validos):
    """Retorna (status, mensagens, contagem_obtida)."""
    msgs = qc.lint(obj, objetos_validos, set(cols_bronze), CATALOGOS_PROIBIDOS)
    if msgs:
        return "lint_falhou", msgs, None
    sql = qc.resolver(obj["sql"], SCHEMAS_SANDBOX, parametros)
    try:
        spark.sql(f"CREATE OR REPLACE VIEW {CATALOG}.{SCH_SANDBOX}.{obj['nome']} AS {sql}")
    except Exception as e:
        return "nao_compila", [str(e).split("\n")[0][:600]], None
    cols = [f.name for f in spark.table(f"{CATALOG}.{SCH_SANDBOX}.{obj['nome']}").schema.fields]
    n = None
    if esperado:
        faltando = [c for _, c in esperado["campos"] if c not in cols]
        sobrando = [c for c in cols if c not in {c2 for _, c2 in esperado["campos"]}]
        if faltando or sobrando:
            return "colunas_divergentes", [f"faltando: {faltando}", f"sobrando: {sobrando}"], None
        n = spark.table(f"{CATALOG}.{SCH_SANDBOX}.{obj['nome']}").count()
        if n != esperado["registros"]:
            return "contagem_divergente", [f"esperado {esperado['registros']} linhas (cabeçalho do QVD), obtido {n}"], n
    documentadas = {c.get("nome") for c in obj.get("colunas", [])}
    sem_doc = [c for c in cols if c not in documentadas]
    if sem_doc:
        return "documentacao_incompleta", [f"colunas sem descrição: {sem_doc}"], n
    for e in obj.get("expectativas", []):
        try:
            v = spark.sql(f"SELECT count(*) AS v FROM {CATALOG}.{SCH_SANDBOX}.{obj['nome']} WHERE NOT ({e['expressao']})").first().v
            e["violacoes_sandbox"] = int(v)
        except Exception as ex:
            return "expectativa_invalida", [f"{e.get('nome')}: {str(ex).splitlines()[0][:300]}"], n
    return "ok", [], n

# Decisões humanas anteriores: rejeição com comentário volta para o modelo como orientação
APROVACOES = CTRL.aprovacoes()
try:
    SCRIPT_DO_OBJETO = {r.nome: r.script for r in spark.table(f"{C}.conv_objeto").select("nome", "script").collect()}
except Exception:
    SCRIPT_DO_OBJETO = {}
print(f"{len(APROVACOES)} decisão(ões) humana(s) registrada(s)")

# COMMAND ----------

# DBTITLE 1,Converter scripts em ordem de dependência
scripts_transform = sorted(
    {q["script_produtor"] for q in lin_qvd if q["papel_script"] == "transform"},
    key=lambda s: max(q["profundidade"] for q in lin_qvd if q["script_produtor"] == s))

todos_objetos, registros, parametros_pipeline, premissas, auditoria = {}, [], {}, [], []
alertas_por_script = {r.script: r.n for r in spark.sql(
    f"SELECT script, count(*) AS n FROM {C}.lin_alerta WHERE tipo IN ('semantica', 'chave_sintetica') GROUP BY script").collect()}

for script in scripts_transform:
    texto = open(f"{DIR_SCRIPTS}/{script}.qvs", encoding="utf-8").read()
    destinos = [{"qvd": q["qvd"], "nome": q["objeto_destino"].split(".")[-1], "camada": q["camada_destino"]}
                for q in lin_qvd if q["script_produtor"] == script]
    esperados = {d["nome"]: campos_qvd[d["qvd"]] for d in destinos}
    lidos = lin_script[script]["qvds_lidos"]
    existentes = {q: ref_objeto(mapa_qvd[q]) for q in lidos if q in mapa_qvd and mapa_qvd[q]["script_produtor"] != script}
    tabelas_bronze = {mapa_qvd[q]["objeto_destino"].split(".")[-1] for q in lidos
                      if q in mapa_qvd and mapa_qvd[q]["camada_destino"] == "bronze"}
    orientacoes = {o: d[1] for o, d in APROVACOES.items() if d[0] == "rejeitar" and d[1]
                   and (o in {x["nome"] for x in destinos} or SCRIPT_DO_OBJETO.get(o) == script)}
    prompt = qc.montar_prompt(script, texto, destinos,
                              {n: e["campos"] for n, e in esperados.items()},
                              {t: cols_bronze[t] for t in sorted(tabelas_bronze) if t in cols_bronze}, existentes,
                              orientacoes_revisor=orientacoes)
    mensagens = [{"role": "system", "content": qc.SYSTEM_PROMPT}, {"role": "user", "content": prompt}]
    if orientacoes:
        CTRL.evento("05_conversao_ia", f"{script}: reconvertendo com orientação do revisor", objeto=script, tipo_objeto="script", detalhe=orientacoes)

    tok_in = tok_out = 0
    for tentativa in range(1, IA["tentativas"] + 1):
        resposta, ti, to, dur, do_cache = chamar_llm(mensagens)
        tok_in, tok_out = tok_in + ti, tok_out + to
        auditoria.append((script, tentativa, LLM_ENDPOINT, float(dur), ti, to, do_cache, mensagens[-1]["content"], resposta))
        try:
            saida = qc.extrair_json(resposta)
            falhas = []
        except Exception as e:
            saida = {"objetos": [], "premissas": [], "parametros": {}}
            falhas = [("(json)", "json_invalido", [str(e)[:300]])]
        objetos = qc.ordem_dependencia(saida.get("objetos", [])) if saida.get("objetos") else []
        params = {k: str(v) for k, v in saida.get("parametros", {}).items()}
        validos = set(todos_objetos) | {o["nome"] for o in objetos}
        resultados = []
        for o in objetos:
            esperado = esperados.get(o["nome"]) if o.get("origem_qvd") else None
            status, msgs, n = validar(o, esperado, params, validos)
            resultados.append((o, status, msgs, n))
            if status != "ok":
                falhas.append((o["nome"], status, msgs))
        for n in [d["nome"] for d in destinos if d["nome"] not in {o["nome"] for o in objetos}]:
            falhas.append((n, "objeto_ausente", ["o destino não foi gerado"]))
        print(f"{script} · tentativa {tentativa} · {dur}s{' (cache)' if do_cache else ''} · {len(objetos)} objetos · falhas {len(falhas)}")
        for f in falhas:
            print("   ", f)
        if not falhas or tentativa == IA["tentativas"]:
            break
        alertas = [r.detalhe for r in spark.sql(f"SELECT detalhe FROM {C}.lin_alerta WHERE script = '{script}'").collect()]
        feedback = ("A validação automática encontrou problemas. Corrija SOMENTE o necessário e devolva o JSON completo.\n"
                    + "\n".join(f"- {n}: {s} · {' | '.join(m)}" for n, s, m in falhas)
                    + "\n\nAlertas de semântica do analisador para este script:\n" + "\n".join(f"- {a}" for a in alertas)
                    + "\n\nNão force a contagem: corrija a semântica Qlik (Exists, Join sem prefixo, Concatenate, filtros).")
        mensagens += [{"role": "assistant", "content": resposta}, {"role": "user", "content": feedback}]

    parametros_pipeline.update(params)
    prem_script = saida.get("premissas", [])
    premissas += [(script, "premissa", str(p)) for p in prem_script]
    premissas += [(script, "alerta_semantico", str(p)) for p in saida.get("alertas_semanticos", [])]
    CTRL.evento("05_conversao_ia", f"{script}: {len(objetos)} objetos em {tentativa} tentativa(s), {tok_in + tok_out} tokens",
                objeto=script, tipo_objeto="script", detalhe={"falhas": [f[:2] for f in falhas]})
    for o, status, msgs, n in resultados:
        violacoes = sum(int(e.get("violacoes_sandbox") or 0) for e in o.get("expectativas", []))
        nota, motivos = qc.nota_confianca(status, tentativa, alertas_por_script.get(script, 0), len(prem_script),
                                          violacoes, lin_script[script]["classe"], eh_referencia=not o.get("origem_qvd"))
        sql_hash = hashlib.sha256(o["sql"].encode("utf-8")).hexdigest()[:16]
        humana = APROVACOES.get(o["nome"])
        if status != "ok":
            revisao = "bloqueado"
        elif humana and humana[0] == "aprovar" and humana[3] == sql_hash:
            revisao = "aprovado"                       # aprovado antes, e o SQL não mudou
        elif humana and humana[0] == "rejeitar":
            revisao = "revisao_humana"                 # foi rejeitado: a nova versão volta para quem rejeitou
        elif nota >= REV["nota_minima_auto"]:
            revisao = "auto_aprovado"
        else:
            revisao = "revisao_humana"
        todos_objetos[o["nome"]] = {**o, "script": script, "status": status, "parametros": params}
        registros.append((script, o["nome"], o["camada"], o.get("origem_qvd"), o.get("descricao"), o.get("cluster_by") or [],
                          o["sql"], status, [str(m) for m in msgs],
                          int((esperados.get(o["nome"]) or {}).get("registros") or 0) if o.get("origem_qvd") else None,
                          int(n) if n is not None else None, int(tentativa), LLM_ENDPOINT, float(nota), motivos, revisao,
                          sql_hash, json.dumps({**o, "parametros": params}, ensure_ascii=False)))
        CTRL.estado("objeto", o["nome"], revisao, {"script": script, "nota": nota, "status": status, "motivos": motivos})
        if o.get("origem_qvd"):
            CTRL.estado("qvd", o["origem_qvd"], "convertido", {"objeto": o["nome"], "nota": nota})

SCHEMA_CONV = ("script STRING, nome STRING, camada STRING, origem_qvd STRING, descricao STRING, cluster_by ARRAY<STRING>, "
               "sql_gerado STRING, status STRING, mensagens ARRAY<STRING>, contagem_qvd BIGINT, contagem_obtida BIGINT, "
               "tentativas INT, modelo STRING, nota DOUBLE, motivos ARRAY<STRING>, status_revisao STRING, sql_hash STRING, objeto_json STRING")
spark.createDataFrame(registros, SCHEMA_CONV).withColumn("execucao_id", F.lit(EXECUCAO_ID)) \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.conv_objeto")

spark.createDataFrame(auditoria, "script STRING, tentativa INT, modelo STRING, segundos DOUBLE, tokens_entrada BIGINT, tokens_saida BIGINT, "
                                 "do_cache BOOLEAN, prompt_usuario STRING, resposta STRING") \
     .selectExpr("*", f"'{EXECUCAO_ID}' AS execucao_id", "current_timestamp() AS ts") \
     .write.mode("append").option("mergeSchema", True).saveAsTable(f"{C}.conv_auditoria_llm")

doc_cols = [(o["script"], o["nome"], o["camada"], c.get("nome"), c.get("descricao"), c.get("regra"))
            for o in todos_objetos.values() for c in o.get("colunas", [])]
spark.createDataFrame(doc_cols, "script STRING, objeto STRING, camada STRING, coluna STRING, descricao STRING, regra STRING") \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.doc_coluna")
expect = [(o["nome"], e.get("nome"), e.get("expressao"), e.get("motivo"), e.get("violacoes_sandbox"))
          for o in todos_objetos.values() for e in o.get("expectativas", [])]
spark.createDataFrame(expect, "objeto STRING, expectativa STRING, expressao STRING, motivo STRING, violacoes_sandbox BIGINT") \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.qual_expectativa")
spark.createDataFrame(premissas, "script STRING, tipo STRING, texto STRING") \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.conv_premissa")
spark.createDataFrame([(k, v) for k, v in parametros_pipeline.items()], "parametro STRING, valor STRING") \
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{C}.conv_parametro")
for t in ["conv_objeto", "conv_auditoria_llm", "conv_cache", "doc_coluna", "qual_expectativa", "conv_premissa", "conv_parametro"]:
    spark.sql(tags_sql("TABLE", f"{C}.{t}"))

display(spark.sql(f"""SELECT script, nome, camada, status, nota, status_revisao, contagem_qvd, contagem_obtida, tentativas, motivos
                      FROM {C}.conv_objeto ORDER BY script, camada DESC, nome"""))

# COMMAND ----------

# DBTITLE 1,Resumo para a fila de revisão
resumo = {r.status_revisao: r.n for r in spark.sql(f"SELECT status_revisao, count(*) AS n FROM {C}.conv_objeto GROUP BY ALL").collect()}
tokens = spark.sql(f"SELECT sum(tokens_entrada) AS e, sum(tokens_saida) AS s, sum(CAST(do_cache AS INT)) AS c, count(*) AS n FROM {C}.conv_auditoria_llm WHERE execucao_id = '{EXECUCAO_ID}'").first()
CTRL.evento("05_conversao_ia", f"conversão concluída: {resumo}", detalhe={"revisao": resumo, "tokens_entrada": tokens.e, "tokens_saida": tokens.s, "chamadas": tokens.n, "do_cache": tokens.c})
_etapa.__exit__(None, None, None)
print(f"Revisão: {resumo}")
print(f"Chamadas ao modelo: {tokens.n} ({tokens.c} do cache) · tokens entrada {tokens.e} · saída {tokens.s}")
print("Objetos em 'revisao_humana' aparecem na tela Conversão do app. 'bloqueado' precisa de correção (rejeitar com orientação reconverte).")

# COMMAND ----------

# DBTITLE 1,Premissas e alertas registrados pelo modelo
display(spark.table(f"{C}.conv_premissa"))
