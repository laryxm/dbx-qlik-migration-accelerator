"""Conversão assistida por IA de scripts Qlik para Lakeflow Spark Declarative Pipelines.

Regras anti-viés aprendidas em conversões reais de scripts Qlik,
adaptadas para gerar materialized views de pipeline declarativo. O modelo recebe só
texto de script e metadados (nomes de tabela, colunas e tipos). Nenhuma linha de dado.
"""
import json
import re

SYSTEM_PROMPT = """Você é um engenheiro de dados sênior especialista em Qlik e em Databricks.
Sua tarefa é converter um load script Qlik (.qvs) em objetos de um pipeline declarativo
(Lakeflow Spark Declarative Pipelines) em Databricks SQL. O objetivo é refatoração
arquitetural, não tradução literal: o resultado deve parecer código nativo Databricks.

Responda SOMENTE com um objeto JSON válido, sem texto antes ou depois, no formato:
{
  "objetos": [
    {
      "nome": "nome_do_objeto",
      "camada": "silver" | "gold",
      "origem_qvd": "Arquivo.qvd ou null para tabelas de referência",
      "descricao": "uma frase em português",
      "cluster_by": ["coluna1", "coluna2"],
      "sql": "SELECT ... (uma única consulta Spark SQL, pode usar CTEs)",
      "colunas": [{"nome": "coluna", "descricao": "significado de negócio em uma frase", "regra": "regra Qlik de origem, em linguagem de negócio, ou null"}],
      "expectativas": [{"nome": "snake_case", "expressao": "condição SQL que toda linha válida cumpre", "motivo": "por quê"}]
    }
  ],
  "parametros": {"nome_parametro": "valor"},
  "premissas": ["premissa assumida, citando o trecho do script"],
  "alertas_semanticos": ["diferença de semântica Qlik x SQL tratada e como"]
}

REGRAS DE ESTRUTURA
E1. Cada QVD gravado com STORE vira exatamente UM objeto, com o nome e a camada informados em DESTINOS.
E2. Cada MAPPING LOAD vira um objeto de referência na camada silver chamado ref_<nome_em_snake_case>,
    construído com SELECT * FROM VALUES (...) AS t(col1, col2). Ele entra na lista de objetos com origem_qvd null.
E3. O campo "sql" contém apenas uma consulta SELECT (ou WITH ... SELECT). Nada de CREATE, INSERT, MERGE, DROP.
E4. Referencie tabelas SEMPRE com placeholders: ${bronze}.<tabela> para origens, ${silver}.<objeto> e ${gold}.<objeto>
    para objetos gerados (inclusive os de outros scripts listados em DESTINOS). Nunca escreva nome de catálogo.
E5. As colunas de saída de cada objeto com origem_qvd devem ser EXATAMENTE as listadas em COLUNAS ESPERADAS,
    com esses nomes, sem colunas a mais ou a menos. A ordem pode seguir a lista.
E6. Variáveis SET/LET com valor constante usado em filtro viram entradas em "parametros" (nome em snake_case)
    e são referenciadas como ${nome_parametro} no SQL. Variáveis só de caminho (lib://) são descartadas.

REGRAS DE DOCUMENTAÇÃO E QUALIDADE
D1. "colunas" descreve TODAS as colunas de saída de cada objeto, em português de negócio, sem jargão técnico.
    Em "regra", explique a transformação do Qlik quando houver (ex.: "Internação quando o tipo de guia é I; Outros quando não mapeado").
D2. "expectativas": de 2 a 4 regras de qualidade por objeto com origem_qvd (chaves não nulas, valores não negativos,
    domínios conhecidos). Elas só registram violações e nunca descartam linhas, para preservar a paridade com o Qlik.
    Use apenas colunas de saída do próprio objeto.

REGRAS ANTI-VIÉS
R1. RESIDENT é CTE. Nunca materialize tabela intermediária que não esteja em DESTINOS ou não seja ref_.
R2. Consolide a cadeia LOAD, RESIDENT, LEFT JOIN, CONCATENATE em uma única consulta com CTEs por objeto.
R3. Concatenate é UNION ALL com colunas explícitas; campo ausente em um dos lados entra como CAST(NULL AS <tipo>).
R4. Left Join (T) é LEFT JOIN dentro da CTE. Join sem prefixo é FULL OUTER JOIN, com COALESCE das chaves.
R5. Nunca crie UDF. Funções Qlik têm equivalente nativo: Date(Floor(d)) = CAST(d AS DATE);
    MonthStart(d) = CAST(date_trunc('MONTH', d) AS DATE); Year(d) = year(d); MakeDate(a,m,d) = make_date(a,m,d);
    IsNull(x) = x IS NULL; If(c, a, b) = CASE WHEN c THEN a ELSE b END; If(c, a) sem else = CASE WHEN c THEN a END;
    RangeSum(a, b) = coalesce(a,0) + coalesce(b,0); Alt(a,b) = coalesce(a,b); 'x' & y = concat('x', y).
R6. ApplyMap('M', campo, 'default') = LEFT JOIN na ref_ correspondente + COALESCE(valor, 'default').
    Chave nula no ApplyMap retorna o default.
R7. Exists(F) NÃO é IS NOT NULL. Significa: o valor está entre os valores do campo F já carregados em QUALQUER
    tabela antes desta linha do script. Converta como semi-join (WHERE x IN (SELECT ...)) contra a UNIÃO das
    origens que carregaram o campo antes, respeitando os filtros que elas tinham.
R8. NOT Exists e Peek usados como controle de carga incremental não viram SQL: registre em premissas.
R9. Nunca use monotonically_increasing_id nem row_number sem ORDER BY. Chave substituta = xxhash64(concat_ws('|', ...)).
R10. Nunca use PARTITIONED BY, cache, collect ou ORDER BY final. Clustering vai em cluster_by (até 3 colunas de filtro).
R11. Identificadores que misturam número e texto (ex.: 'R' & id) saem como STRING nos dois lados do UNION.
R12. Nulos: Qlik ignora nulo em Sum e não associa chave nula em join. Escreva filtros e joins com isso em mente.
R13. Datas e decimais: as origens no bronze já estão tipadas (DATE, TIMESTAMP, DECIMAL). Não reconverta texto.
R14. Group By do Qlik vira GROUP BY com as mesmas chaves; Count(DISTINCT x) = count(DISTINCT x).
R15. Section Access não entra no SQL. Registre em premissas que vira row filter no Unity Catalog.
"""


def montar_prompt(script_nome, script_texto, destinos, colunas_esperadas, origens_bronze, objetos_existentes,
                  orientacoes_revisor=None):
    linhas = [f"SCRIPT: {script_nome}.qvs", "-" * 70, script_texto, "-" * 70, "", "DESTINOS (QVD gravado -> objeto a gerar)"]
    for d in destinos:
        linhas.append(f"- {d['qvd']} -> ${{{d['camada']}}}.{d['nome']} (camada {d['camada']})")
    linhas += ["", "COLUNAS ESPERADAS por objeto (nome Qlik -> coluna Databricks)"]
    for nome, cols in colunas_esperadas.items():
        linhas.append(f"- {nome}: " + ", ".join(f"{q} -> {c}" for q, c in cols))
    linhas += ["", "MAPEAMENTO DE QVDs LIDOS (o QVD deixa de existir; use o objeto equivalente)"]
    for qvd, ref in objetos_existentes.items():
        linhas.append(f"- {qvd} -> {ref}")
    linhas += ["", "SCHEMA DAS ORIGENS NO BRONZE"]
    for tabela, cols in origens_bronze.items():
        linhas.append(f"- ${{bronze}}.{tabela}(" + ", ".join(f"{c} {t}" for c, t in cols) + ")")
    if orientacoes_revisor:
        linhas += ["", "ORIENTAÇÕES DO REVISOR HUMANO (a versão anterior foi rejeitada; siga estas orientações)"]
        linhas += [f"- {o}: {c}" for o, c in orientacoes_revisor.items()]
    linhas += ["", "Gere o JSON pedido."]
    return "\n".join(linhas)


def extrair_json(texto):
    texto = texto.strip()
    texto = re.sub(r"^```(json)?", "", texto).strip()
    texto = re.sub(r"```$", "", texto).strip()
    ini, fim = texto.find("{"), texto.rfind("}")
    return json.loads(texto[ini: fim + 1])


PROIBIDOS = [
    (r"\bCREATE\b|\bINSERT\b|\bMERGE\b|\bDROP\b", "DDL/DML dentro do SELECT (E3)"),
    (r"(?i)\budf\b|pandas_udf", "UDF (R5)"),
    (r"(?i)monotonically_increasing_id", "chave não determinística (R9)"),
    (r"(?i)partitioned\s+by", "particionamento manual (R10)"),
    (r"(?i)\.cache\(|\.collect\(|toPandas", "operação de driver (R10)"),
]


def lint(obj, objetos_validos, tabelas_bronze, catalogos=()):
    msgs = []
    sql = obj.get("sql", "")
    for cat in set(catalogos) | {"hive_metastore"}:
        if re.search(rf"(?i)\b{re.escape(cat)}\.", sql):
            msgs.append(f"nome de catálogo fixo: {cat} (E4)")
    for rx, msg in PROIBIDOS:
        if re.search(rx, sql, flags=re.I if rx.startswith("\\b") else 0):
            msgs.append(msg)
    if re.search(r"(?is)\border\s+by\b[^()]*$", sql.strip()):
        msgs.append("ORDER BY final (R10)")
    for camada, nome in re.findall(r"\$\{(bronze|silver|gold)\}\.(\w+)", sql):
        if camada == "bronze" and nome not in tabelas_bronze:
            msgs.append(f"tabela bronze inexistente: {nome}")
        if camada in ("silver", "gold") and nome not in objetos_validos:
            msgs.append(f"objeto gerado inexistente: ${{{camada}}}.{nome}")
    return msgs


def resolver(sql, schemas, parametros):
    for k, v in schemas.items():
        sql = sql.replace("${" + k + "}", v)
    for k, v in (parametros or {}).items():
        sql = sql.replace("${" + k + "}", str(v))
    return sql


def ordem_dependencia(objetos):
    nomes = {o["nome"] for o in objetos}
    deps = {o["nome"]: {n for _, n in re.findall(r"\$\{(silver|gold)\}\.(\w+)", o["sql"]) if n in nomes and n != o["nome"]} for o in objetos}
    ordem, feitos = [], set()
    while len(ordem) < len(objetos):
        prontos = [n for n in deps if n not in feitos and deps[n] <= feitos]
        if not prontos:
            raise ValueError(f"Dependência circular entre objetos: {deps}")
        for n in sorted(prontos):
            ordem.append(n)
            feitos.add(n)
    por_nome = {o["nome"]: o for o in objetos}
    return [por_nome[n] for n in ordem]


def chamar_modelo(endpoint, mensagens, max_tokens=16000, temperature=None, timeout=900):
    """Compatível com a v1: devolve só o texto."""
    return chamar_modelo_detalhado(endpoint, mensagens, max_tokens, temperature, timeout)["texto"]


def chave_cache(endpoint, mensagens):
    import hashlib
    return hashlib.sha256(json.dumps([endpoint, mensagens], ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def chamar_modelo_detalhado(endpoint, mensagens, max_tokens=16000, temperature=None, timeout=900, rotulo="conversao"):
    """Chama o endpoint e devolve {texto, tokens_entrada, tokens_saida, segundos}.
    Cada chamada vira um trace no MLflow (quando disponível), com prompt, resposta e tokens."""
    import time
    t0 = time.time()
    try:
        import mlflow
        span_ctx = mlflow.start_span(name=f"qlik_{rotulo}", span_type="LLM")
    except Exception:
        span_ctx = None
    if span_ctx is None:
        r = _invocar(endpoint, mensagens, max_tokens, temperature, timeout)
    else:
        with span_ctx as span:
            span.set_inputs({"endpoint": endpoint, "mensagens": mensagens})
            r = _invocar(endpoint, mensagens, max_tokens, temperature, timeout)
            span.set_outputs({"texto": r["texto"]})
            span.set_attributes({"tokens_entrada": r["tokens_entrada"], "tokens_saida": r["tokens_saida"], "rotulo": rotulo})
    r["segundos"] = round(time.time() - t0, 1)
    return r


def _invocar(endpoint, mensagens, max_tokens, temperature, timeout):
    import requests
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    h = w.config.authenticate()
    if callable(h):
        h = h()
    r = requests.post(f"{w.config.host.rstrip('/')}/serving-endpoints/{endpoint}/invocations",
                      headers={**h, "Content-Type": "application/json"},
                      json={"messages": mensagens, "max_tokens": max_tokens,
                            **({"temperature": temperature} if temperature is not None else {})},
                      timeout=timeout)
    r.raise_for_status()
    corpo = r.json()
    c = corpo["choices"][0]["message"]["content"]
    if isinstance(c, list):
        c = "".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in c)
    uso = corpo.get("usage") or {}
    return {"texto": c, "tokens_entrada": int(uso.get("prompt_tokens") or 0), "tokens_saida": int(uso.get("completion_tokens") or 0)}


def nota_confianca(status, tentativas, n_alertas_script, n_premissas, violacoes_expectativa, classe_script,
                   eh_referencia=False):
    """Nota de 0 a 100 que decide se a conversão segue sozinha ou vai para revisão humana.
    Devolve (nota, motivos). Os motivos aparecem no app para o revisor saber onde olhar.

    Pesos (ajustáveis aqui): rodada de correção -15 · alerta de semântica -8 (até -24) ·
    premissa -2 (até -10) · violação de expectativa -20 · script de complexidade alta -10.
    Tabelas de referência (MAPPING LOAD) não herdam alertas e premissas do script: são listas de valores."""
    if status != "ok":
        return 0.0, [f"validação automática falhou: {status}"]
    nota, motivos = 100.0, []
    if tentativas > 1:
        nota -= 15 * (tentativas - 1)
        motivos.append(f"precisou de {tentativas - 1} rodada(s) de correção (-{15 * (tentativas - 1)})")
    if eh_referencia:
        motivos.append("tabela de referência (MAPPING LOAD): lista de valores, validada por compilação")
        return max(nota, 0.0), motivos
    if n_alertas_script:
        d = min(24, 8 * n_alertas_script)
        nota -= d
        motivos.append(f"{n_alertas_script} alerta(s) de semântica Qlik no script (-{d})")
    if n_premissas:
        d = min(10, 2 * n_premissas)
        nota -= d
        motivos.append(f"modelo registrou {n_premissas} premissa(s) (-{d})")
    if violacoes_expectativa:
        nota -= 20
        motivos.append(f"{violacoes_expectativa} linha(s) violam expectativas de qualidade no sandbox (-20)")
    if classe_script == "alta":
        nota -= 10
        motivos.append("script de complexidade alta (-10)")
    if len(motivos) == 0:
        motivos.append("validado na primeira tentativa, sem alertas nem premissas")
    return max(nota, 0.0), motivos


# Regras aprendidas na paridade. Cada divergência explicada vira regra para as próximas ondas.
REGRAS_APRENDIDAS_MV = [
    "A1. Sum() no Qlik devolve 0 quando não há valores no grupo; SUM() em SQL devolve NULL. "
    "Medidas de soma usam COALESCE(SUM(x), 0). Divisões continuam devolvendo NULL quando o denominador é zero. "
    "(origem: paridade N4, Receita Mensalidades, competência sem mensalidade)",
]
