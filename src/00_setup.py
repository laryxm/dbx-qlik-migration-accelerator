# Databricks notebook source
# DBTITLE 1,Configuração central do acelerador
# Os demais notebooks fazem %run ./00_setup. Sem SQL aqui, para o %run ficar instantâneo.
# Ordem de precedência da configuração:
#   1. config/acelerador.yml (padrão, vem no pacote)
#   2. /Volumes/<catalogo>/<prefixo>_ctrl/arquivos/config/acelerador.json (gravado pelo app)
#   3. parâmetros do job ou widgets: catalogo, prefixo_schema, onda, modo, modelo, app_nome, execucao_id

import json, os, uuid, copy
import yaml

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
WS_DIR = "/Workspace" + _nb_path.rsplit("/", 1)[0]

with open(f"{WS_DIR}/config/acelerador.yml", encoding="utf-8") as _f:
    _CFG_PADRAO = yaml.safe_load(_f)

def _widget(nome, padrao):
    try:
        dbutils.widgets.text(nome, str(padrao) if padrao is not None else "")
        v = dbutils.widgets.get(nome)
        return v if v != "" else padrao
    except Exception:
        return padrao

def _mesclar(base, extra):
    out = copy.deepcopy(base)
    for k, v in (extra or {}).items():
        out[k] = _mesclar(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out

CATALOG = _widget("catalogo", _CFG_PADRAO["ambiente"]["catalogo"])
PREFIXO = _widget("prefixo_schema", _CFG_PADRAO["ambiente"]["prefixo_schema"])
SCH_CTRL = f"{PREFIXO}_ctrl"
VOLUME = f"/Volumes/{CATALOG}/{SCH_CTRL}/arquivos"
ARQ_CONFIG = f"{VOLUME}/config/acelerador.json"

CFG = _CFG_PADRAO
if os.path.exists(ARQ_CONFIG):
    with open(ARQ_CONFIG, encoding="utf-8") as _f:
        CFG = _mesclar(_CFG_PADRAO, json.load(_f))
CFG["ambiente"]["catalogo"], CFG["ambiente"]["prefixo_schema"] = CATALOG, PREFIXO
CFG["ambiente"]["onda"] = _widget("onda", CFG["ambiente"]["onda"])
CFG["ambiente"]["modo"] = _widget("modo", CFG["ambiente"]["modo"])
CFG["ia"]["endpoint"] = _widget("modelo", CFG["ia"]["endpoint"])          # endpoint de IA do workspace
APP_NOME = _widget("app_nome", None)                                        # app do acelerador (recebe permissões no passo 01)

AMB, IA, TRI, CONV, REV, PAR, SEG, PUB = (CFG[k] for k in
    ("ambiente", "ia", "triagem", "conversao", "revisao", "paridade", "seguranca", "publicacao"))

MODO, ONDA = AMB["modo"], AMB["onda"]
if MODO not in ("demo", "cliente"):
    raise ValueError(f"modo deve ser 'demo' ou 'cliente', veio '{MODO}'")
if not AMB.get("dono"):
    AMB["dono"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
SCH_SILVER, SCH_GOLD, SCH_SANDBOX = f"{PREFIXO}_silver", f"{PREFIXO}_gold", f"{PREFIXO}_sandbox"
if AMB.get("schema_bronze"):
    BRONZE_FQN = AMB["schema_bronze"]                      # catalogo.schema existente (Lakeflow Connect)
    SCH_BRONZE = BRONZE_FQN.split(".")[-1]
    CAT_BRONZE = BRONZE_FQN.split(".")[0]
else:
    SCH_BRONZE, CAT_BRONZE = f"{PREFIXO}_bronze", CATALOG
    BRONZE_FQN = f"{CAT_BRONZE}.{SCH_BRONZE}"
C = f"{CATALOG}.{SCH_CTRL}"

ENT = CFG.get("entrada", {})
VOLUME_ENTRADA = (ENT.get("volume") or VOLUME).rstrip("/")   # pode ser um volume do cliente
DIR_INVENT  = f"{VOLUME_ENTRADA}/{ENT.get('pasta_inventario', 'inventario')}"
DIR_SCRIPTS = f"{VOLUME_ENTRADA}/{ENT.get('pasta_scripts', 'scripts')}"
DIR_APPS    = f"{VOLUME_ENTRADA}/{ENT.get('pasta_apps', 'apps')}"
DIR_QVD     = f"{VOLUME_ENTRADA}/{ENT.get('pasta_qvd', 'qvd')}"
DIR_SAIDA   = f"{VOLUME}/saida"                             # bundle e documentos gerados

LLM_ENDPOINT = IA["endpoint"]
PIPELINE_NAME = f"{PUB['nome_bundle']}_{PREFIXO}_demo"
TAGS = {"Owner": AMB["dono"], "onda": ONDA, "gerado_por": "acelerador_qlik", **AMB.get("tags", {})}
EXECUCAO_ID = _widget("execucao_id", None) or f"interativo-{uuid.uuid4().hex[:8]}"
DIR_PIPELINE_SQL = f"{WS_DIR}/pipeline_gerado"

def tags_sql(kind, name):
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in TAGS.items())
    return f"ALTER {kind} {name} SET TAGS ({pares})"

import acel_ctrl
CTRL = acel_ctrl.Controle(spark, C, EXECUCAO_ID, ONDA)

print(f"Catálogo {CATALOG} · prefixo {PREFIXO} · modo {MODO} · onda {ONDA} · bronze {BRONZE_FQN} · modelo {LLM_ENDPOINT} · execução {EXECUCAO_ID}")
