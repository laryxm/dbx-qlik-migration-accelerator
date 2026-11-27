# dbx-qlik-migration-accelerator

**Acelerador de migração Qlik para Databricks.** Leva os scripts, QVDs e apps do Qlik para o Lakehouse com conversão assistida por IA, revisão humana onde ela importa e prova de que os números batem. Tudo conduzido por um app no Databricks Apps, pensado para quem está começando no Databricks.

![Como o acelerador funciona](docs/diagrama.svg)

---

## Sumário

1. [O que ele faz](#1-o-que-ele-faz)
2. [Antes de instalar](#2-antes-de-instalar)
3. [Instalar](#3-instalar) (três caminhos: pelo workspace, guiada ou manual)
4. [Primeiro uso: a demonstração em 5 minutos](#4-primeiro-uso-a-demonstração-em-5-minutos)
5. [Migrar o seu ambiente Qlik](#5-migrar-o-seu-ambiente-qlik)
6. [Onde ficam os dados](#6-onde-ficam-os-dados)
7. [Segurança, privacidade e governança](#7-segurança-privacidade-e-governança)
8. [Configuração](#8-configuração)
9. [Problemas comuns](#9-problemas-comuns)
10. [Limitações conhecidas](#10-limitações-conhecidas)
11. [Desinstalar](#11-desinstalar)
12. [Estrutura do repositório](#12-estrutura-do-repositório)

---

## 1. O que ele faz

No Qlik, ingestão, transformação, modelo e regra de negócio costumam viver dentro do mesmo script. O acelerador separa cada responsabilidade no lugar certo do Lakehouse e mostra, passo a passo, o que está acontecendo.

| Passo | O que faz | Você recebe |
|---|---|---|
| **1 · Preparar** | Cria schemas, volume, tabelas de controle e dá as permissões ao app | Ambiente pronto |
| **2 · Simular Qlik** | Só na demonstração: gera um ERP e QVDs fictícios | Dados para testar sem nada do cliente |
| **3 · Triagem** | Lê o inventário de QVDs e decide o que migrar, eleger, revisar, arquivar ou descartar | O escopo real da migração (normalmente bem menor que o total de QVDs) |
| **4 · Linhagem** | Lê scripts e cabeçalhos de QVD e rastreia cada medida do app até a coluna de origem | Grafo, complexidade e o conjunto mínimo de fontes |
| **5 · Conversão com IA** | Gera SQL nativo Databricks, valida em sandbox e dá uma **nota de confiança** de 0 a 100 | Código pronto; o que ficar abaixo de 85 vai para a sua revisão |
| **6 · Publicar** | Monta o pipeline só com o que foi aprovado e gera um bundle para dev, hml e prd | Pipeline no workspace e um pacote versionável |
| **7 · Métricas e segurança** | Medidas mestras viram **metric views**; Section Access vira **row filter** | Métricas governadas para AI/BI, Genie e Power BI |
| **8 · Paridade** | Compara com o Qlik em 4 níveis: contagem, somas, linha a linha e métricas do app | A prova de que os números batem |

**Princípios**

- **A IA só vê metadados.** O modelo recebe scripts, nomes de tabelas e colunas. Nenhuma linha de dado de negócio.
- **Nada publica sem validar.** Lint anti-viés, compilação em sandbox, contrato de colunas e contagem contra o QVD.
- **Você decide onde a IA não teve certeza.** Aprovar libera; rejeitar exige uma orientação, que volta para a IA na próxima conversão.
- **Paridade é entregável.** Cada objeto é comparado com o Qlik antes de o QVD ser desligado.

---

## 2. Antes de instalar

Confira com o administrador do workspace:

| Item | Por quê | Onde ver |
|---|---|---|
| Workspace com **Unity Catalog** | Tudo é criado em um catálogo | Catalog |
| **CREATE SCHEMA** em um catálogo | O acelerador cria `<prefixo>_ctrl`, `_bronze`, `_silver`, `_gold`, `_sandbox` | Catalog → catálogo → Permissions |
| Permissão para conceder privilégios nesse catálogo (dono ou MANAGE) | O passo 1 dá ao app só o acesso que ele precisa. Sem isso, o passo mostra os comandos para o dono rodar | Catalog → catálogo → Permissions |
| **Databricks Apps** habilitado | É a interface do acelerador | Menu lateral → Compute → Apps |
| **Compute serverless** para jobs e notebooks | O fluxo roda em serverless | Settings → Compute |
| Um **SQL warehouse** | O app lê as tabelas por ele (2X-Small basta) | SQL Warehouses |
| Um **endpoint de foundation model** de chat | Usado na conversão com IA (ex.: `databricks-claude-sonnet-5-5`) | Serving |
| Acesso à internet a partir do serverless (PyPI) | Os passos 2 e 8 instalam a biblioteca `pyqvd` | – |

---

## 3. Instalar

Escolha **um** dos três caminhos. Os três instalam a mesma coisa: o app `qlik-acelerador` e o job `qlik-acelerador-fluxo` (o nome do job segue o nome do app, então dá para ter mais de uma instalação no mesmo workspace, cada uma com seu catálogo ou prefixo).

### Caminho A · Pelo workspace, sem linha de comando (recomendado para quem está começando)

1. No Databricks, vá em **Workspace → Create → Git folder** e cole a URL deste repositório.
2. Dentro da pasta criada, abra o notebook **`instalar/Instalar_pelo_workspace`**.
3. Conecte em **Serverless** e clique em **Run all** uma vez. No topo da página aparecem cinco campos:
   - **Catálogo**: onde o acelerador vai criar os schemas.
   - **SQL warehouse** e **Modelo de IA**: escolha nas listas.
   - **Prefixo** e **nome do app**: já vêm preenchidos.
4. Preencha o catálogo e clique em **Run all** de novo. Leva de 5 a 10 minutos.
5. No fim, aparece o link **Abrir o acelerador**.

### Caminho B · Instalação guiada pelo terminal

Requer a [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) e `python3`.

```bash
git clone https://github.com/laryxm/dbx-qlik-migration-accelerator.git
cd dbx-qlik-migration-accelerator
./instalar/instalar.sh
```

O script confere o login, **lista os catálogos, os warehouses e os modelos disponíveis** para você escolher, instala, publica o app, prepara o ambiente e mostra o link.

### Caminho C · Manual, com Databricks Asset Bundles

```bash
databricks auth login --host https://<seu-workspace> --profile meu-workspace

databricks bundle deploy -p meu-workspace \
  --var="catalogo=<catalogo>" \
  --var="warehouse_id=<id-do-warehouse>"

databricks bundle run acelerador -p meu-workspace \
  --var="catalogo=<catalogo>" --var="warehouse_id=<id-do-warehouse>"

databricks bundle run acelerador_fluxo --only 01_preparar_ambiente -p meu-workspace \
  --var="catalogo=<catalogo>" --var="warehouse_id=<id-do-warehouse>"
```

Variáveis opcionais: `prefixo_schema` (padrão `qlik_acel`), `modelo` (padrão `databricks-claude-sonnet-5-5`) e `app_nome` (padrão `qlik-acelerador`). O link do app aparece no fim do segundo comando e em **Compute → Apps**.

> **O que é instalado:** um job serverless com 8 tarefas e um app com três recursos (o SQL warehouse, o job e o endpoint do modelo). Nada é criado fora do catálogo que você escolheu.

---

## 4. Primeiro uso: a demonstração em 5 minutos

O modo **Demonstração** usa uma empresa fictícia (Cooperativa de Saúde Vale do Ipê) e dados sintéticos. Serve para conhecer o fluxo antes de usar qualquer dado real.

1. Abra o app e leia a aba **Como usar**.
2. Em **Pré-requisitos**, confira se está tudo verde.
3. Em **Executar**, deixe todos os passos marcados e clique em **Executar selecionados**. O progresso aparece na tela e no topo de todas as abas. São cerca de 12 minutos.
4. Navegue pelas abas na ordem: **Triagem**, **Linhagem**, **Revisar conversão** (aprove os objetos que pedem revisão), depois **Executar → Só 05 a 08**.
5. Veja o resultado em **Publicar**, **Paridade** e **Mapa dos dados** (onde está cada camada, com links para o Catalog Explorer).

---

## 5. Migrar o seu ambiente Qlik

### 5.1 O que exportar do Qlik

Não é preciso exportar todos os QVDs. Cada arquivo tem um papel:

| Entrada | Arquivo | De quais | Para quê |
|---|---|---|---|
| **Inventário** | `inventario*.csv` com `nome, espaco, linhas, tamanho_bytes, atualizado_em` | **Todos** os QVDs (só metadados) | Triagem do ambiente inteiro |
| **Load scripts** | `NomeDoApp.qvs` (um por app ou QVW) | Os scripts da onda | Linhagem e conversão |
| **Cabeçalhos de QVD** | `*.qvd` (basta o cabeçalho XML) | Os QVDs que os scripts da onda gravam e leem | Colunas, contagem e linhagem |
| **QVDs completos** | `*.qvd` | Só os que os apps da onda leem (modelo) | Paridade linha a linha |
| **Medidas dos apps** | `NomeDoApp_medidas.json` | Os apps da onda | Metric views e paridade de métricas |
| **Tabelas exportadas** | `hypercube*.csv` (separador `;`, decimal `,`) | Uma por visão importante do app | Paridade de métricas (N4) |

Os QVDs de extração (o "bronze" do Qlik) não precisam vir completos: o dado de origem chega ao Lakehouse pelo **Lakeflow Connect**, direto do banco. Formato do arquivo de medidas: veja `src/qlik_amostra/apps/APP_PainelCustoAssistencial_medidas.json`.

### 5.2 Configurar no app

Na aba **1 · Entrada de dados**:

1. **Modo:** *Ambiente do cliente*.
2. **Tabelas de origem:** se já estão no Lakehouse, informe o `catálogo.schema` onde o Lakeflow Connect grava. Se ainda não foram ingeridas, o acelerador entrega a triagem, a linhagem e a especificação de ingestão de cada tabela.
3. **Pasta de entrada:** use o padrão e envie os arquivos pelo próprio app (arraste um .zip), ou indique um volume do Unity Catalog onde o time já copiou os arquivos.
4. **Onda:** o lote que está sendo migrado agora, normalmente um app ou um domínio (ex.: `onda_1_custo_assistencial`). Vai como tag em tudo, para acompanhar andamento e custo por lote.

Depois, siga as abas na ordem numerada. Cada aba explica o conceito para quem vem do Qlik.

> **Data de corte:** para a paridade ser justa, a recarga do Qlik e a ingestão das origens precisam olhar para o mesmo momento (por exemplo, os dois em D-1), ou compare só períodos fechados.

---

## 6. Onde ficam os dados

A aba **Mapa dos dados** mostra isso com links. Resumo, com o prefixo padrão `qlik_acel`:

| Camada | Onde | No Qlik equivale a |
|---|---|---|
| Entrada (arquivos) | Volume `/Volumes/<catalogo>/qlik_acel_ctrl/arquivos`, ou o volume que você indicar | arquivos .qvs, .qvd e exports |
| Bronze | `<catalogo>.qlik_acel_bronze`, ou o schema do Lakeflow Connect | o SQL SELECT dos scripts de extração |
| Silver | `<catalogo>.qlik_acel_silver` | QVDs de transformação e MAPPING LOAD |
| Gold | `<catalogo>.qlik_acel_gold` | QVDs Fato* e Dim* |
| Metric views | `<catalogo>.qlik_acel_gold` (`mv_*`) | medidas mestras e Set Analysis |
| Controle | `<catalogo>.qlik_acel_ctrl` | o que o acelerador descobre e decide (inventário, linhagem, conversão, paridade, auditoria) |
| Sandbox | `<catalogo>.qlik_acel_sandbox` | views temporárias de teste do código gerado |

---

## 7. Segurança, privacidade e governança

- **Nenhuma credencial neste repositório.** O app autentica com o próprio service principal, criado pelo Databricks Apps. A instalação usa o seu login da CLI ou do workspace, que nunca é gravado em arquivo.
- **Permissões mínimas para o app.** O passo 1 concede ao service principal do app: `USE CATALOG` no catálogo; `USE SCHEMA`, `SELECT` e `MODIFY` no schema de controle; `READ VOLUME` e `WRITE VOLUME` no volume de arquivos; `USE SCHEMA` e `SELECT` em silver, gold, bronze e sandbox.
- **A IA só recebe metadados.** Scripts, nomes de tabelas, colunas e tipos. Nenhuma linha de dado vai para o modelo, que roda dentro do Databricks (Model Serving).
- **Tudo auditável.** As tabelas `evento` (linha do tempo com usuário), `unidade_status` (estado de cada tabela, objeto e medida), `aprovacao` (quem aprovou o quê, com o hash do SQL) e `conv_auditoria_llm` (cada chamada ao modelo, com tokens) ficam no schema de controle. As chamadas ao modelo também geram traces no MLflow.
- **Custo por onda.** Tudo o que o acelerador cria leva as tags `onda` e `gerado_por`, consultáveis em `system.billing.usage`.
- **Section Access vira regra do Unity Catalog.** A regra passa a valer em dashboard, Genie, notebook, Power BI e API.

---

## 8. Configuração

Quase nada precisa ser mudado. A ordem de precedência é:

1. `src/config/acelerador.yml`: padrões do pacote, comentados linha a linha.
2. O que você salva no app (**Entrada de dados** e **Ajustes avançados**), gravado em `config/acelerador.json` no volume.
3. Parâmetros do job: `catalogo`, `prefixo_schema`, `onda`, `modo` e `modelo`.

Em **Ajustes avançados** ficam, já preenchidos: os limites da triagem (dias para ativo, recente e parado; espaços descartados), a nota mínima para aprovação automática, as tolerâncias da paridade, as regras de segurança e os catálogos de dev, hml e prd do bundle gerado.

---

## 9. Problemas comuns

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| Um item vermelho em **Pré-requisitos** | Falta permissão ou recurso | A própria tela mostra o comando ou a ação |
| O passo 1 avisa que não conseguiu conceder permissões | Quem rodou não é dono do catálogo | Peça ao dono para rodar os comandos `GRANT` listados na saída do job |
| O passo 5 falha com 403 ou "endpoint not found" | O modelo não existe no workspace ou está desabilitado | Reinstale com outro `modelo` ou troque em **Ajustes avançados → IA** |
| Os passos 2 ou 8 falham no `pip install` | O serverless não tem acesso ao PyPI | Libere o PyPI ou use um repositório interno de pacotes |
| O app abre a versão antiga depois de reinstalar | Cache do navegador | Recarregue com Cmd+Shift+R (ou Ctrl+F5) uma vez |
| Uma tela mostra "Nada por aqui ainda" | O passo correspondente ainda não rodou | Vá em **Executar** |

---

## 10. Limitações conhecidas

- **Paridade só com o cabeçalho do QVD:** os passos 4 e 5 aceitam arquivos `.qvd` que contêm só o cabeçalho, mas o passo 8 ainda precisa do QVD completo dos objetos que compara.
- **Escopo da onda:** o acelerador processa todos os scripts da pasta de entrada. Para migrar por lotes, envie os scripts de uma onda por vez, ou use uma pasta por onda.
- **Exportação do lado do Qlik:** ainda não há um kit automático. Os arquivos são exportados manualmente ou com `qlik-cli`.
- **Conversão sequencial:** os scripts são convertidos um por vez, em ordem de dependência.
- Expressões de Set Analysis que dependem da seleção do usuário são convertidas **com ressalva** e marcadas para revisão.

---

## 11. Desinstalar

```bash
databricks bundle destroy -p meu-workspace --var="catalogo=<catalogo>" --var="warehouse_id=<id>"
```

Os schemas e os dados não são apagados automaticamente. Para remover também os dados:

```sql
DROP SCHEMA <catalogo>.qlik_acel_ctrl CASCADE;
DROP SCHEMA <catalogo>.qlik_acel_bronze CASCADE;
DROP SCHEMA <catalogo>.qlik_acel_silver CASCADE;
DROP SCHEMA <catalogo>.qlik_acel_gold CASCADE;
DROP SCHEMA <catalogo>.qlik_acel_sandbox CASCADE;
```

Se instalou pelo Caminho A, apague o job `<app>-fluxo` em **Jobs & Pipelines**, o app em **Compute → Apps** e o pipeline gerado, cujo nome termina em `_demo`.

---

## 12. Estrutura do repositório

```
databricks.yml                 bundle: variáveis da instalação
resources/                     job (8 passos) e app
src/
  00_LEIA_ME.py                visão geral, para quem abre os notebooks
  00_setup.py                  configuração central (lida por todos os passos)
  01_ … 08_*.py                os 8 passos
  acel_ctrl.py                 governança: eventos, estados, aprovações
  qlik_parser.py               parser de load script e de cabeçalho QVD
  qlik_conversor.py            prompt anti-viés, lint, nota de confiança
  config/acelerador.yml        padrões comentados
  qlik_amostra/                amostra fictícia usada no modo Demonstração
app/                           o app (FastAPI + HTML, sem etapa de build)
instalar/
  instalar.sh                  instalação guiada pelo terminal
  Instalar_pelo_workspace.py   instalação por notebook, sem linha de comando
docs/diagrama.svg              o diagrama acima
```

---

> Projeto independente da comunidade, sem suporte oficial da Databricks. Revise o código gerado e os resultados de paridade antes de usar em produção.
