#!/usr/bin/env bash
# =====================================================================
# Instalação guiada do Acelerador de Migração Qlik para Databricks
#
# O que este script faz, nesta ordem:
#   1. confere a Databricks CLI e o login no seu workspace
#   2. pergunta catálogo, SQL warehouse e modelo de IA (mostra as opções)
#   3. instala o job e o app com "databricks bundle deploy"
#   4. publica e inicia o app
#   5. roda o passo 01 (cria os schemas e dá as permissões ao app)
#   6. mostra o link do app
#
# Uso:  ./instalar/instalar.sh            (na raiz do repositório)
# Requisitos: Databricks CLI recente e python3.
# Nenhuma credencial é gravada por este script: o login fica no perfil da CLI.
# =====================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

azul=$'\033[1;34m'; verde=$'\033[1;32m'; amarelo=$'\033[1;33m'; vermelho=$'\033[1;31m'; fim=$'\033[0m'
titulo() { echo; echo "${azul}== $1 ==${fim}"; }
ok()     { echo "${verde}✓ $1${fim}"; }
aviso()  { echo "${amarelo}! $1${fim}"; }
erro()   { echo "${vermelho}✕ $1${fim}"; exit 1; }
perguntar() { local msg="$1" padrao="${2:-}" r; if [[ -n "$padrao" ]]; then read -r -p "$msg [$padrao]: " r; echo "${r:-$padrao}"; else read -r -p "$msg: " r; echo "$r"; fi; }

# --------------------------------------------------------------------- 1. CLI e login
titulo "1/6 · Databricks CLI"
command -v databricks >/dev/null 2>&1 || erro "Databricks CLI não encontrada. Instale: https://docs.databricks.com/dev-tools/cli/install.html"
command -v python3 >/dev/null 2>&1 || erro "python3 não encontrado (usado só para ler as respostas da CLI)."
databricks bundle --help >/dev/null 2>&1 || erro "Sua CLI não tem o comando 'bundle'. Atualize a Databricks CLI."
ok "$(databricks --version)"

echo
echo "Perfis configurados na CLI:"
databricks auth profiles 2>/dev/null || true
PERFIL=$(perguntar "Perfil da CLI do workspace onde instalar" "DEFAULT")
if ! databricks current-user me -p "$PERFIL" -o json >/dev/null 2>&1; then
  aviso "O perfil '$PERFIL' não está logado. Abrindo o login..."
  HOST=$(perguntar "URL do workspace (ex.: https://adb-123.azuredatabricks.net)")
  databricks auth login --host "$HOST" --profile "$PERFIL"
fi
USUARIO=$(databricks current-user me -p "$PERFIL" -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['userName'])")
ok "Logado como $USUARIO"

# --------------------------------------------------------------------- 2. escolhas
titulo "2/6 · Onde instalar"
echo "Catálogos visíveis para você:"
databricks catalogs list -p "$PERFIL" -o json 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin); d=d if isinstance(d,list) else d.get('catalogs',[])
print('  ' + ', '.join(sorted(c['name'] for c in d if c.get('catalog_type') not in ('SYSTEM_CATALOG',) and c['name'] not in ('system','samples'))))" || true
CATALOGO=$(perguntar "Catálogo onde o acelerador vai criar os schemas (precisa de CREATE SCHEMA)")
[[ -n "$CATALOGO" ]] || erro "Catálogo é obrigatório."
PREFIXO=$(perguntar "Prefixo dos schemas" "qlik_acel")

echo
echo "SQL warehouses:"
WH_JSON=$(databricks warehouses list -p "$PERFIL" -o json)
echo "$WH_JSON" | python3 -c "
import sys,json
d=json.load(sys.stdin); d=d if isinstance(d,list) else d.get('warehouses',[])
for i,w in enumerate(d,1): print(f\"  {i}. {w['name']}  (id {w['id']}, {w.get('cluster_size','')}, {w.get('state','')})\")"
ESCOLHA=$(perguntar "Número do warehouse que o app vai usar" "1")
WAREHOUSE_ID=$(echo "$WH_JSON" | python3 -c "
import sys,json
d=json.load(sys.stdin); d=d if isinstance(d,list) else d.get('warehouses',[])
print(d[int(sys.argv[1])-1]['id'])" "$ESCOLHA") || erro "Escolha de warehouse inválida."
ok "Warehouse $WAREHOUSE_ID"

MODELO=$(perguntar "Endpoint do modelo de IA" "databricks-claude-sonnet-5-5")
if ! databricks serving-endpoints get "$MODELO" -p "$PERFIL" >/dev/null 2>&1; then
  aviso "Endpoint '$MODELO' não encontrado. Endpoints de chat disponíveis:"
  databricks serving-endpoints list -p "$PERFIL" -o json | python3 -c "
import sys,json
d=json.load(sys.stdin); d=d if isinstance(d,list) else d.get('endpoints',[])
for e in d:
    n=e['name']
    if any(k in n for k in ('claude','llama','gpt','gemini','qwen','mixtral','dbrx')): print('  ', n)"
  MODELO=$(perguntar "Endpoint do modelo de IA")
fi
APP_NOME=$(perguntar "Nome do app" "qlik-acelerador")

VARS=(--var="catalogo=$CATALOGO" --var="warehouse_id=$WAREHOUSE_ID" --var="prefixo_schema=$PREFIXO" --var="modelo=$MODELO" --var="app_nome=$APP_NOME" -p "$PERFIL")

echo
echo "Resumo:"
echo "  workspace : perfil $PERFIL ($USUARIO)"
echo "  catálogo  : $CATALOGO   schemas: ${PREFIXO}_ctrl, _bronze, _silver, _gold, _sandbox"
echo "  warehouse : $WAREHOUSE_ID"
echo "  modelo    : $MODELO"
echo "  app       : $APP_NOME"
[[ "$(perguntar "Instalar com essas opções? (s/n)" "s")" =~ ^[sS] ]] || erro "Instalação cancelada."

# --------------------------------------------------------------------- 3. deploy
titulo "3/6 · Instalando job e app (bundle deploy)"
databricks bundle deploy "${VARS[@]}"
ok "Job e app criados"

# --------------------------------------------------------------------- 4. app
titulo "4/6 · Publicando e iniciando o app (alguns minutos)"
databricks bundle run acelerador "${VARS[@]}"
ok "App no ar"

# --------------------------------------------------------------------- 5. passo 01
titulo "5/6 · Preparando o ambiente (passo 01: schemas, volume e permissões do app)"
databricks bundle run acelerador_fluxo --only 01_preparar_ambiente "${VARS[@]}"
ok "Ambiente preparado"

# --------------------------------------------------------------------- 6. link
titulo "6/6 · Pronto"
URL=$(databricks apps get "$APP_NOME" -p "$PERFIL" -o json | python3 -c "import sys,json;print(json.load(sys.stdin).get('url',''))")
echo
echo "${verde}Abra o acelerador:${fim} $URL"
echo
echo "Comece pela aba 'Como usar'. No modo Demonstração, vá em Executar e rode tudo."
echo "Se algum item de Pré-requisitos aparecer em vermelho, a própria tela mostra o comando para resolver."
