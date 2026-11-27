# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background:#1B3139;padding:28px 32px;border-radius:10px;font-family:'DM Sans',Arial,sans-serif;">
# MAGIC <div style="color:#FF3621;font-size:13px;letter-spacing:2px;font-weight:700;">ACELERADOR DE MIGRAÇÃO QLIK PARA DATABRICKS</div>
# MAGIC <div style="color:#FFFFFF;font-size:30px;font-weight:700;margin-top:6px;">Do QVD ao Lakehouse, com IA e paridade comprovada</div>
# MAGIC <p style="color:#C7D0D4;font-size:16px;line-height:1.5;margin:10px 0 0 0;">Fábrica reproduzível para migração em massa de ambientes Qlik que concentram ingestão, transformação, modelo e camada semântica. Usa conversão assistida por IA com regras anti-viés e o método de estrangulamento com paridade: inventário, linhagem, conversão assistida, metric views, segurança e paridade, tudo registrado em tabelas do Unity Catalog.</p>
# MAGIC </div>

# COMMAND ----------
# MAGIC %md-sandbox
# MAGIC <div style="font-family:'DM Sans',Arial,sans-serif;color:#495057;font-size:15px;">
# MAGIC <h2 style="color:#1B3139;border-bottom:3px solid #FF3621;display:inline-block;padding-bottom:4px;">Fluxo</h2>
# MAGIC <table style="border-collapse:collapse;width:100%;margin-top:8px;">
# MAGIC <tr style="background:#1B3139;color:#FFFFFF;"><th style="padding:10px 14px;text-align:left;">Passo</th><th style="padding:10px 14px;text-align:left;">O que faz</th><th style="padding:10px 14px;text-align:left;">Entrada</th><th style="padding:10px 14px;text-align:left;">Saída no UC</th></tr>
# MAGIC <tr><td style="padding:10px 14px;font-weight:600;">01 Preparar ambiente</td><td style="padding:10px 14px;">Schemas por camada, volume, tabelas de controle e tags</td><td style="padding:10px 14px;">Configuração</td><td style="padding:10px 14px;">volume <code>arquivos</code>, evento, unidade_status, execucao_passo, aprovacao</td></tr>
# MAGIC <tr style="background:#F5F7F8;"><td style="padding:10px 14px;font-weight:600;">02 Simular Qlik</td><td style="padding:10px 14px;">Só no modo demo: ERP sintético e recarga dos scripts gravando QVDs reais</td><td style="padding:10px 14px;">Scripts de exemplo</td><td style="padding:10px 14px;">bronze_erp_*, .qvd, hypercubes</td></tr>
# MAGIC <tr><td style="padding:10px 14px;font-weight:600;">03 Inventário e triagem</td><td style="padding:10px 14px;">Agrupa QVDs fatiados em tabelas lógicas e decide migrar, consolidar, arquivar ou descartar, com limites da configuração</td><td style="padding:10px 14px;">Inventário (metadados)</td><td style="padding:10px 14px;">inv_qvd, inv_triagem, inv_unidade_migracao</td></tr>
# MAGIC <tr style="background:#F5F7F8;"><td style="padding:10px 14px;font-weight:600;">04 Linhagem</td><td style="padding:10px 14px;">Parser de script e cabeçalho QVD: grafo, score, destino por QVD, linhagem por campo e por métrica, SQL de origem, alertas e Section Access</td><td style="padding:10px 14px;">.qvs, .qvd, medidas</td><td style="padding:10px 14px;">lin_*, qvd_campo</td></tr>
# MAGIC <tr><td style="padding:10px 14px;font-weight:600;">05 Conversão com IA</td><td style="padding:10px 14px;">Gera, valida e dá nota de confiança a cada objeto. Nota baixa vai para revisão no app; rejeição volta como orientação. Traces no MLflow e cache por hash</td><td style="padding:10px 14px;">Script + metadados</td><td style="padding:10px 14px;">conv_objeto, conv_auditoria_llm, conv_cache, doc_coluna, qual_expectativa</td></tr>
# MAGIC <tr style="background:#F5F7F8;"><td style="padding:10px 14px;font-weight:600;">06 Publicar</td><td style="padding:10px 14px;">Bundle e pipeline só com o que foi aprovado (pela nota ou por pessoa); dependentes de pendentes esperam</td><td style="padding:10px 14px;">Aprovações</td><td style="padding:10px 14px;">bundle_gerado, saida/*_bundle.zip, silver e gold</td></tr>
# MAGIC <tr><td style="padding:10px 14px;font-weight:600;">07 Métricas e segurança</td><td style="padding:10px 14px;">Medidas mestras viram metric views; glossário para as Pages do UC; Section Access vira row filter com teste por perfil</td><td style="padding:10px 14px;">Export de medidas</td><td style="padding:10px 14px;">mv_*, sem_medida, doc_glossario, gov_*</td></tr>
# MAGIC <tr style="background:#F5F7F8;"><td style="padding:10px 14px;font-weight:600;">08 Paridade</td><td style="padding:10px 14px;">N1 volume, N2 somas, N3 hash de linha, N4 métricas contra o hypercube, com histórico</td><td style="padding:10px 14px;">QVDs e hypercubes</td><td style="padding:10px 14px;">par_historico, par_resultado</td></tr>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-family:'DM Sans',Arial,sans-serif;color:#495057;font-size:15px;">
# MAGIC <h2 style="color:#1B3139;border-bottom:3px solid #FF3621;display:inline-block;padding-bottom:4px;">O que é real e o que é simulado</h2>
# MAGIC <table style="border-collapse:collapse;width:100%;margin-top:8px;">
# MAGIC <tr style="background:#1B3139;color:#FFFFFF;"><th style="padding:10px 14px;text-align:left;">Item</th><th style="padding:10px 14px;text-align:left;">Nesta versão</th><th style="padding:10px 14px;text-align:left;">No cliente</th></tr>
# MAGIC <tr><td style="padding:10px 14px;font-weight:600;">Inventário de QVDs</td><td style="padding:10px 14px;">Inventário sintético com os mesmos padrões de um ambiente real (famílias mensais, vazios, duplicados, testes)</td><td style="padding:10px 14px;">Mesmo arquivo, atualizado pelo kit de extração</td></tr>
# MAGIC <tr style="background:#F5F7F8;"><td style="padding:10px 14px;font-weight:600;">Scripts, QVDs e dados</td><td style="padding:10px 14px;">Cooperativa de Saúde Vale do Ipê, empresa fictícia, dados sintéticos</td><td style="padding:10px 14px;">Scripts via qlik-cli e LoadScript dos QVW; QVDs reais como baseline</td></tr>
# MAGIC <tr><td style="padding:10px 14px;font-weight:600;">Origens no bronze</td><td style="padding:10px 14px;">Tabelas geradas no passo 2</td><td style="padding:10px 14px;">Lakeflow Connect a partir do banco do ERP (Oracle, SQL Server…)</td></tr>
# MAGIC <tr style="background:#F5F7F8;"><td style="padding:10px 14px;font-weight:600;">Grupos de acesso</td><td style="padding:10px 14px;">Nomes de grupo derivados do usuário Qlik</td><td style="padding:10px 14px;">Grupos do IdP sincronizados por SCIM</td></tr>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:6px solid #0B7285;background:#F5F7F8;border-radius:0 8px 8px 0;padding:18px 22px;font-family:'DM Sans',Arial,sans-serif;color:#495057;font-size:15px;">
# MAGIC <div style="color:#0B7285;font-weight:700;letter-spacing:1px;font-size:13px;">PRINCÍPIOS</div>
# MAGIC <b>Refatorar, não traduzir.</b> O código gerado é nativo Databricks: CTEs, funções SQL nativas, materialized views, liquid clustering.<br>
# MAGIC <b>A IA só vê metadados.</b> Scripts, nomes de tabelas, colunas e tipos. Nenhuma linha de dado vai para o modelo.<br>
# MAGIC <b>Nada publica sem validação.</b> Lint anti-viés, compilação em sandbox, contrato de colunas e contagem contra o cabeçalho do QVD.<br>
# MAGIC <b>Paridade é entregável.</b> Cada objeto tem teste N1 a N4 registrado em tabela, que roda enquanto os dois ambientes convivem.<br>
# MAGIC <b>Governança no dado.</b> Section Access vira regra do Unity Catalog, válida para dashboard, Genie, notebook, Power BI e API.
# MAGIC </div>

# COMMAND ----------
# MAGIC %md
# MAGIC ### Lógica da migração
# MAGIC O planejamento é de trás para frente: parte das métricas que o negócio usa, rastreia cada uma até a coluna de origem e define o escopo mínimo de fontes e scripts (passos 03 e 04). A construção é de frente para trás, porque não existe métrica sem origem: bronze, silver, gold e metric view (passos 05 a 07). A paridade fecha o ciclo comparando cada camada com o Qlik (passo 08).
# MAGIC
# MAGIC ### Como usar
# MAGIC **Pelo app `qlik-acelerador`** (recomendado): pré-requisitos, configuração, envio dos arquivos, execução, revisão do código gerado, publicação, paridade e diário, sem abrir notebook.
# MAGIC **Pelo job `<app>-fluxo`** (ex.: `qlik-acelerador-fluxo`): parâmetros `catalogo`, `prefixo_schema`, `onda`, `modo`, `modelo`. Para rodar só alguns passos, use *Run now with different settings*.
# MAGIC **Pelos notebooks**: rodar 01 a 08 em ordem, em serverless. Widgets com os mesmos parâmetros do job.
# MAGIC
# MAGIC ### Configuração
# MAGIC Padrões em `config/acelerador.yml`. O app grava alterações em `/Volumes/<catalogo>/<prefixo>_ctrl/arquivos/config/acelerador.json`, que tem prioridade. Parâmetros do job têm prioridade sobre os dois para catálogo, prefixo, onda e modo.
# MAGIC
# MAGIC ### Governança e observabilidade
# MAGIC | Tabela | Para quê |
# MAGIC |---|---|
# MAGIC | `evento` | Linha do tempo de tudo: passos, envios, configuração, aprovações, com usuário |
# MAGIC | `execucao_passo` | Duração e resultado de cada passo por execução |
# MAGIC | `unidade_status` | Estado atual de cada tabela lógica, QVD, script, objeto, medida e perfil |
# MAGIC | `aprovacao` | Decisões humanas sobre o código gerado, com hash do SQL aprovado |
# MAGIC | `conv_auditoria_llm` | Cada chamada ao modelo: prompt, resposta, tokens, cache |
# MAGIC
# MAGIC Todos os objetos recebem as tags da configuração, inclusive `onda`, o que permite medir custo por onda em `system.billing.usage`.
# MAGIC
# MAGIC ### Para levar a outro cliente
# MAGIC 1. Instalar pelo notebook `instalar/Instalar_pelo_workspace` ou pelo script `instalar/instalar.sh` (veja o README do repositório).
# MAGIC 2. No app: Entrada de dados → modo Ambiente do cliente e o schema onde o Lakeflow Connect grava as origens.
# MAGIC 3. No app: Entrada de dados → enviar os arquivos exportados do Qlik (ou indicar o volume onde já estão).
# MAGIC 4. Executar tudo, revisar, publicar, acompanhar a paridade.
