"""Parser heurístico de load scripts Qlik (.qvs) e de cabeçalhos de QVD.

Extrai statements, tabelas em memória, QVDs lidos e gravados, fontes de banco,
construções de risco semântico e um score de complexidade por script.
Não executa nada e não lê dados: trabalha só com texto de script e cabeçalho XML.
"""
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict

PESOS = {
    "LOAD": 1, "SQL_SELECT": 1, "STORE": 1, "LEFT_JOIN": 3, "INNER_JOIN": 3, "RIGHT_JOIN": 3,
    "OUTER_JOIN": 4, "CONCATENATE": 2, "MAPPING_LOAD": 1, "APPLYMAP": 1, "EXISTS": 3,
    "NOT_EXISTS": 3, "GROUP_BY": 2, "PEEK": 2, "ITERNO": 3, "AUTOGENERATE": 2, "WHILE": 3,
    "FOR": 3, "SECTION_ACCESS": 4, "RANGESUM": 1, "INTERVALMATCH": 5, "AUTONUMBER": 2,
    "HASH": 1, "PREVIOUS": 3, "INCLUDE": 2, "INCREMENTAL": 4, "INLINE": 1, "IF": 0.5,
    "SQL_ORIGEM_FUNCAO": 4, "SQL_ORIGEM_JOIN": 3, "SQL_ORIGEM_AGREGACAO": 3,
}

ALERTAS = {
    "OUTER_JOIN": "Join sem prefixo é FULL OUTER JOIN no Qlik. Converter como FULL OUTER JOIN e tratar nulos dos dois lados.",
    "EXISTS": "Exists(campo) olha todos os valores já carregados no campo em qualquer tabela, na ordem do script. Converter como semi-join contra a união das tabelas carregadas antes.",
    "NOT_EXISTS": "NOT Exists em carga incremental. Substituir pelo controle do conector (CDC ou cursor) ou por AUTO CDC/MERGE.",
    "RANGESUM": "RangeSum trata nulo como zero. Em SQL usar COALESCE antes da operação.",
    "APPLYMAP": "ApplyMap vira LEFT JOIN em tabela de referência com COALESCE para o default.",
    "PEEK": "Peek usado como controle de estado. No Databricks o estado fica no checkpoint ou no SEQUENCE BY.",
    "SECTION_ACCESS": "Section Access vira row filter e column mask no Unity Catalog, com grupos do IdP.",
    "INCREMENTAL": "Padrão incremental Qlik (Peek + NOT Exists + Inner Join). Vira ingestão com CDC ou cursor, não código de transformação.",
    "ITERNO": "Calendário gerado com IterNo/AutoGenerate. Substituir por uma dim_data corporativa única.",
    "SQL_ORIGEM_FUNCAO": "SQL de origem chama função do banco. A regra roda no Oracle, não no Qlik: decidir entre trazer o resultado pela consulta do conector ou reproduzir a função no silver.",
    "SQL_ORIGEM_JOIN": "SQL de origem com join. O QVD de extract já é uma transformação: ingerir as tabelas base e levar o join para o silver, ou manter a consulta no conector.",
    "SQL_ORIGEM_AGREGACAO": "SQL de origem agrega dados. O bronze deixa de ser cópia fiel da fonte: avaliar ingerir o detalhe.",
    "CONCATENATE": "Concatenate alinha por nome de campo e preenche ausentes com nulo. Em SQL, UNION ALL BY NAME ou colunas explícitas.",
}


def _limpar(texto):
    linhas = []
    for ln in texto.splitlines():
        ln = re.sub(r"(?<![:/])//.*$", "", ln)
        linhas.append(ln)
    texto = "\n".join(linhas)
    texto = re.sub(r"/\*.*?\*/", "", texto, flags=re.S)
    return texto


def _statements(texto):
    out, buf, dentro_colchete, dentro_aspas = [], [], 0, None
    for ch in texto:
        if dentro_aspas:
            buf.append(ch)
            if ch == dentro_aspas:
                dentro_aspas = None
            continue
        if ch in ("'", '"'):
            dentro_aspas = ch
        elif ch == "[":
            dentro_colchete += 1
        elif ch == "]":
            dentro_colchete = max(0, dentro_colchete - 1)
        if ch == ";" and dentro_colchete == 0:
            s = "".join(buf).strip()
            if s:
                out.append(s)
            buf = []
        else:
            buf.append(ch)
    s = "".join(buf).strip()
    if s:
        out.append(s)
    return out


def _resolver(s, variaveis):
    for _ in range(3):
        s = re.sub(r"\$\((\w+)\)", lambda m: variaveis.get(m.group(1), m.group(0)), s)
    return s


def _nome_arquivo(path):
    p = path.strip().strip("[]'\"")
    return p.replace("\\", "/").split("/")[-1]


def _construcoes(s):
    u = s.upper()
    c = []
    checks = [
        ("APPLYMAP", r"\bAPPLYMAP\s*\("), ("NOT_EXISTS", r"\bNOT\s+EXISTS\s*\("),
        ("EXISTS", r"(?<!NOT )\bEXISTS\s*\("), ("GROUP_BY", r"\bGROUP\s+BY\b"),
        ("PEEK", r"\bPEEK\s*\("), ("ITERNO", r"\bITERNO\s*\("), ("AUTOGENERATE", r"\bAUTOGENERATE\b"),
        ("WHILE", r"\bWHILE\b"), ("RANGESUM", r"\bRANGESUM\s*\("), ("INTERVALMATCH", r"\bINTERVALMATCH\b"),
        ("AUTONUMBER", r"\bAUTONUMBER\w*\s*\("), ("HASH", r"\bHASH(128|160|256)\s*\("),
        ("PREVIOUS", r"\bPREVIOUS\s*\("), ("INCLUDE", r"\$\((MUST_)?INCLUDE"), ("INLINE", r"\bINLINE\b"),
        ("IF", r"\bIF\s*\("),
    ]
    for nome, rx in checks:
        n = len(re.findall(rx, u))
        c += [nome] * n
    if re.search(r"(?<!NOT )\bEXISTS\s*\(", u) and "NOT_EXISTS" in c:
        c = [x for x in c if x != "EXISTS"] + ["EXISTS"] * (len(re.findall(r"\bEXISTS\s*\(", u)) - c.count("NOT_EXISTS"))
    return c


@dataclass
class Statement:
    script: str
    ordem: int
    tipo: str
    tabela: str = None
    alvo: str = None
    prefixo: str = None
    origem_tipo: str = None
    origem: str = None
    campos: list = field(default_factory=list)
    construcoes: list = field(default_factory=list)
    texto: str = ""


def _itens_load(corpo):
    """Lista de (nome_do_campo, expressão) do LOAD ou do SELECT."""
    m = re.search(r"\bLOAD\b(.*?)\b(FROM|RESIDENT|INLINE|AUTOGENERATE)\b", corpo, flags=re.I | re.S)
    if not m:
        m = re.search(r"\bSELECT\b(.*?)\bFROM\b", corpo, flags=re.I | re.S)
    if not m:
        return []
    lista, prof, buf = [], 0, ""
    for ch in m.group(1):
        if ch in "([":
            prof += 1
        elif ch in ")]":
            prof -= 1
        if ch == "," and prof == 0:
            lista.append(buf)
            buf = ""
        else:
            buf += ch
    lista.append(buf)
    itens = []
    for item in lista:
        item = " ".join(item.split())
        if not item:
            continue
        a = re.search(r"^(.*)\bas\s+(\[[^\]]+\]|\w+)\s*$", item, flags=re.I)
        if a:
            itens.append((a.group(2).strip("[]"), a.group(1).strip()))
        else:
            itens.append((re.sub(r"^\w+\.", "", item).strip("[]"), item))
    return itens


def _campos_load(corpo):
    return [n for n, _ in _itens_load(corpo)]


_PALAVRAS = {"as", "and", "or", "not", "null", "like", "in", "distinct", "is", "then", "else", "end", "case", "when", "total"}


def refs_campos(expr):
    """Campos referenciados numa expressão Qlik ou SQL (ignora funções, literais e palavras reservadas)."""
    e = re.sub(r"'[^']*'", " ", expr)
    e = re.sub(r"\b[A-Za-z_]\w*\.(?=[A-Za-z_])", "", e)
    refs = re.findall(r"\[([^\]]+)\]", e)
    e2 = re.sub(r"\[[^\]]+\]", " ", e)
    for m in re.finditer(r"(?<![\w$])([A-Za-z_][A-Za-z0-9_]*)\b(\s*\()?", e2):
        w = m.group(1)
        if m.group(2) or w.lower() in _PALAVRAS or w.isdigit():
            continue
        refs.append(w)
    return list(dict.fromkeys(refs))


def _sql_origem(corpo):
    """Construções de regra de negócio dentro de um SQL SELECT enviado ao banco."""
    sel = re.search(r"\bSELECT\b(.*?)\bFROM\b", corpo, flags=re.I | re.S)
    c, funcoes = [], []
    if sel:
        funcoes = [f for f in re.findall(r"\b([A-Za-z_][\w\.]*)\s*\(", sel.group(1))
                   if f.upper() not in ("CAST", "COALESCE", "NVL", "TRUNC", "TO_DATE", "TO_CHAR")]
        if funcoes:
            c.append("SQL_ORIGEM_FUNCAO")
    if re.search(r"\bJOIN\b|\bFROM\s+[\w\.]+\s*(\w+\s*)?,", corpo, flags=re.I):
        c.append("SQL_ORIGEM_JOIN")
    if re.search(r"\bGROUP\s+BY\b", corpo, flags=re.I):
        c.append("SQL_ORIGEM_AGREGACAO")
    return c, funcoes


def parse_script(nome, texto):
    texto = _limpar(texto)
    variaveis, stmts, tabelas = {}, [], {}
    lin = {}            # tabela -> campo -> {"origens": set((tipo, origem, campo)), "exprs": [..]}
    estrela = {}        # tabela -> set de QVDs carregados com LOAD *
    sql_regras = []     # SQL de origem com função, join ou agregação
    em_section_access = False
    section_access = []
    ultimo_label = None
    for i, s in enumerate(_statements(texto)):
        s_res = _resolver(s, variaveis)
        u = s_res.upper().strip()
        cons = _construcoes(s)
        mvar = re.match(r"^(LET|SET)\s+(\w+)\s*=\s*(.*)$", s_res, flags=re.I | re.S)
        if mvar:
            val = mvar.group(3).strip()
            if val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
            variaveis[mvar.group(2)] = val
            stmts.append(Statement(nome, i, "VARIAVEL", alvo=mvar.group(2), texto=s[:400], construcoes=cons))
            continue
        if re.match(r"^SECTION\s+ACCESS", u):
            em_section_access = True
            stmts.append(Statement(nome, i, "SECTION_ACCESS", construcoes=["SECTION_ACCESS"], texto=s[:400]))
            continue
        if re.match(r"^SECTION\s+APPLICATION", u):
            em_section_access = False
            continue
        if re.match(r"^LIB\s+CONNECT\s+TO", u):
            stmts.append(Statement(nome, i, "CONEXAO", alvo=re.sub(r"(?i)^LIB\s+CONNECT\s+TO\s+", "", s_res).strip("'\" "), texto=s[:400]))
            continue
        mstore = re.match(r"^STORE\s+(\w+)\s+INTO\s+(\[[^\]]+\]|\S+)", s_res, flags=re.I)
        if mstore:
            t, arq = mstore.group(1), _nome_arquivo(mstore.group(2))
            stmts.append(Statement(nome, i, "STORE", tabela=t, alvo=arq, construcoes=cons, texto=s[:400]))
            continue
        if re.match(r"^DROP\s+TABLES?\b", u):
            stmts.append(Statement(nome, i, "DROP", alvo=re.sub(r"(?i)^DROP\s+TABLES?\s+", "", s_res).strip(), texto=s[:400]))
            continue
        label = None
        mlabel = re.match(r"^(\w+)\s*:\s*(.*)$", s_res, flags=re.S)
        corpo = s_res
        if mlabel and not re.match(r"^(LIB|SQL)$", mlabel.group(1), flags=re.I):
            label, corpo = mlabel.group(1), mlabel.group(2)
        prefixo, alvo = None, None
        mp = re.match(r"^\s*(LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+)?(JOIN|KEEP)\s*(\((\w+)\))?\s*(.*)$", corpo, flags=re.I | re.S)
        mc = re.match(r"^\s*(NO)?CONCATENATE\s*(\((\w+)\))?\s*(.*)$", corpo, flags=re.I | re.S)
        if mp:
            tipo_join = (mp.group(1) or "OUTER ").strip().upper()
            prefixo = f"{tipo_join}_JOIN"
            alvo = mp.group(4) or ultimo_label
            corpo = mp.group(5)
            cons.append(prefixo)
        elif mc and not mc.group(1):
            prefixo, alvo, corpo = "CONCATENATE", mc.group(3) or ultimo_label, mc.group(4)
            cons.append("CONCATENATE")
        mapping = bool(re.match(r"^\s*MAPPING\s+", corpo, flags=re.I))
        if mapping:
            corpo = re.sub(r"(?i)^\s*MAPPING\s+", "", corpo)
            cons.append("MAPPING_LOAD")
        origem_tipo, origem = None, None
        if re.match(r"^\s*SQL\s+SELECT", corpo, flags=re.I):
            mt = re.search(r"\bFROM\s+([\w\.\"]+)", corpo, flags=re.I)
            origem_tipo, origem = "BANCO", (mt.group(1).replace('"', "") if mt else None)
            tipo = "SQL_SELECT"
            c_sql, funcoes = _sql_origem(corpo)
            cons += c_sql
            if c_sql:
                sql_regras.append({"tabela": alvo or label or ultimo_label, "origem": origem,
                                   "construcoes": c_sql, "funcoes": funcoes, "texto": s[:500]})
        elif re.match(r"^\s*LOAD\b", corpo, flags=re.I):
            tipo = "LOAD"
            mf = re.search(r"\bFROM\s+(\[[^\]]+\]|\S+)\s*(\(\s*(\w+)[^)]*\))?", corpo, flags=re.I)
            mr = re.search(r"\bRESIDENT\s+(\w+)", corpo, flags=re.I)
            if re.search(r"\bINLINE\b", corpo, flags=re.I):
                origem_tipo, origem = "INLINE", None
            elif mr:
                origem_tipo, origem = "RESIDENT", mr.group(1)
            elif mf:
                arq = _nome_arquivo(mf.group(1))
                fmt = (mf.group(3) or "").lower()
                origem_tipo = "QVD" if fmt == "qvd" or arq.lower().endswith(".qvd") else "ARQUIVO"
                origem = arq
            elif re.search(r"\bAUTOGENERATE\b", corpo, flags=re.I):
                origem_tipo = "AUTOGENERATE"
        else:
            continue
        tabela = alvo or label or ultimo_label
        if not prefixo and label:
            ultimo_label = label
        if em_section_access:
            cons.append("SECTION_ACCESS")
            tipo = "SECTION_ACCESS_TABLE"
            m_inl = re.search(r"INLINE\s*\[(.*)\]", corpo, flags=re.S | re.I)
            if m_inl:
                linhas = [l.strip() for l in m_inl.group(1).strip().splitlines() if l.strip()]
                cab = [c.strip() for c in linhas[0].split(",")]
                for l in linhas[1:]:
                    section_access.append(dict(zip(cab, [v.strip() for v in l.split(",")])))
        st = Statement(nome, i, "MAPPING_LOAD" if mapping else tipo, tabela=tabela, alvo=alvo, prefixo=prefixo,
                       origem_tipo=origem_tipo, origem=origem, campos=_campos_load(corpo),
                       construcoes=cons, texto=s[:600])
        stmts.append(st)
        if tabela and not mapping and not em_section_access:
            info = tabelas.setdefault(tabela, {"fontes": set(), "campos": []})
            if origem_tipo in ("QVD", "BANCO", "ARQUIVO"):
                info["fontes"].add((origem_tipo, origem))
            elif origem_tipo == "RESIDENT" and origem in tabelas:
                info["fontes"] |= tabelas[origem]["fontes"]
            if "EXISTS" in cons:
                # Exists depende de tudo que já foi carregado antes no script
                for outra, oinfo in list(tabelas.items()):
                    if outra != tabela:
                        info["fontes"] |= oinfo["fontes"]
            # linhagem por campo
            mapa = lin.setdefault(tabela, {})
            for nome_c, expr in _itens_load(corpo):
                if nome_c == "*":
                    if origem_tipo == "QVD":
                        estrela.setdefault(tabela, set()).add(origem)
                    elif origem_tipo == "RESIDENT":
                        for k, v in lin.get(origem, {}).items():
                            d = mapa.setdefault(k, {"origens": set(), "exprs": []})
                            d["origens"] |= v["origens"]
                            d["exprs"] += [e for e in v["exprs"] if e not in d["exprs"]]
                        estrela.setdefault(tabela, set()).update(estrela.get(origem, set()))
                    continue
                d = mapa.setdefault(nome_c, {"origens": set(), "exprs": []})
                if expr != nome_c and expr.strip("[]") != nome_c and expr not in d["exprs"]:
                    d["exprs"].append(expr)
                for r in refs_campos(expr):
                    if origem_tipo in ("QVD", "BANCO", "ARQUIVO"):
                        d["origens"].add((origem_tipo, origem, r))
                    elif origem_tipo == "RESIDENT":
                        ro = lin.get(origem, {}).get(r)
                        if ro:
                            d["origens"] |= ro["origens"]
                            d["exprs"] += [e for e in ro["exprs"] if e not in d["exprs"]]
                        else:
                            for q in estrela.get(origem, set()):
                                d["origens"].add(("QVD", q, r))
            for c in st.campos:
                if c == "*" and origem_tipo == "RESIDENT" and origem in tabelas:
                    for rc in tabelas[origem]["campos"]:
                        if rc not in info["campos"]:
                            info["campos"].append(rc)
                elif c != "*" and c not in info["campos"]:
                    info["campos"].append(c)

    todas = [c for st in stmts for c in st.construcoes]
    incremental = any(st.tipo == "LOAD" and "NOT_EXISTS" in st.construcoes for st in stmts) and "PEEK" in todas
    if incremental:
        todas.append("INCREMENTAL")
    produzidos = []
    for st in stmts:
        if st.tipo == "STORE":
            fontes = sorted(tabelas.get(st.tabela, {}).get("fontes", set()))
            fontes = [f for f in fontes if not (f[0] == "QVD" and f[1] == st.alvo)]
            mapa = lin.get(st.tabela, {})
            produzidos.append({"qvd": st.alvo, "tabela": st.tabela, "fontes": fontes,
                               "campos": tabelas.get(st.tabela, {}).get("campos", []),
                               "linhagem_campos": {k: {"origens": sorted(v["origens"]), "exprs": v["exprs"]} for k, v in mapa.items()},
                               "estrela": sorted(estrela.get(st.tabela, set()))})
    lidos = sorted({st.origem for st in stmts if st.origem_tipo == "QVD"})
    score = round(sum(PESOS.get(c, 0) for c in todas), 1)
    if produzidos and all(all(f[0] == "BANCO" for f in p["fontes"]) for p in produzidos):
        papel = "extract"
    elif produzidos:
        papel = "transform"
    else:
        papel = "app"
    alertas = sorted({ALERTAS[c] for c in todas if c in ALERTAS})
    return {
        "script": nome,
        "papel": papel,
        "linhas": len(texto.splitlines()),
        "statements": [asdict(s) for s in stmts],
        "variaveis": variaveis,
        "tabelas": {k: {"fontes": sorted(v["fontes"]), "campos": v["campos"]} for k, v in tabelas.items()},
        "qvds_produzidos": produzidos,
        "qvds_lidos": [q for q in lidos if q not in {p["qvd"] for p in produzidos}] if papel != "extract" else lidos,
        "construcoes": {c: todas.count(c) for c in sorted(set(todas))},
        "score": score,
        "classe": "baixa" if score < 15 else ("media" if score < 35 else "alta"),
        "alertas": alertas,
        "section_access": section_access,
        "sql_origem_regras": sql_regras,
        "linhagem_tabelas": {t: {k: {"origens": sorted(v["origens"]), "exprs": v["exprs"]} for k, v in m.items()} for t, m in lin.items()},
        "estrela_tabelas": {t: sorted(v) for t, v in estrela.items()},
    }


def parse_qvd_header(caminho, max_bytes=5_000_000):
    with open(caminho, "rb") as f:
        raw = f.read(max_bytes)
    fim = raw.find(b"</QvdTableHeader>")
    if fim < 0:
        raise ValueError(f"Cabeçalho QVD não encontrado em {caminho}")
    root = ET.fromstring(raw[: fim + len(b"</QvdTableHeader>")].decode("utf-8", errors="replace"))
    campos = []
    for fh in root.iter("QvdFieldHeader"):
        campos.append({
            "campo": fh.findtext("FieldName"),
            "simbolos": int(fh.findtext("NoOfSymbols") or 0),
            "tipo_numero": fh.findtext("NumberFormat/Type"),
            "tags": [t.text for t in fh.iter("String")],
        })
    lineage = [li.findtext("Discriminator") for li in root.iter("LineageInfo")]
    return {
        "tabela": root.findtext("TableName"),
        "creator_doc": root.findtext("CreatorDoc"),
        "criado_utc": root.findtext("CreateUtcTime"),
        "registros": int(root.findtext("NoOfRecords") or 0),
        "campos": campos,
        "lineage": [l for l in lineage if l],
    }


def snake(nome):
    import unicodedata
    s = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode()
    s = re.sub(r"[\[\]%$#]", "", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
    return re.sub(r"_+", "_", s)


def nome_campo(nome):
    """Normalização de nome de campo: minúsculo ASCII, sem quebrar siglas (VL_LIBERADO -> vl_liberado)."""
    import unicodedata
    s = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode()
    s = re.sub(r"[\[\]%$#]", "", s)
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
    return re.sub(r"_+", "_", s)
