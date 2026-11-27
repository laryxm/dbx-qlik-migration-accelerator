"""Controle e observabilidade do acelerador.

Tudo o que o acelerador faz deixa rastro em quatro tabelas do schema de controle:

  evento          linha do tempo: quem, quando, qual passo, qual objeto, o que aconteceu
  execucao_passo  duração e resultado de cada passo de cada execução
  unidade_status  estado atual de cada unidade de migração (máquina de estados)
  aprovacao       decisões humanas sobre o código gerado (gravadas pelo app)

Estados possíveis de uma unidade, na ordem do fluxo:
  descoberto > triado > linhagem_ok > convertido > (revisao_humana | bloqueado | auto_aprovado | aprovado | rejeitado)
  > publicado > (paridade_ok | paridade_divergente) > desligado_no_qlik
"""
import json
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone

ESTADOS = ["descoberto", "triado", "descartado", "arquivado", "linhagem_ok", "convertido", "revisao_humana",
           "bloqueado", "auto_aprovado", "aprovado", "rejeitado", "publicado", "paridade_ok",
           "paridade_divergente", "desligado_no_qlik"]

DDL = {
    "evento": """ts TIMESTAMP, execucao_id STRING, onda STRING, passo STRING, nivel STRING,
                 tipo_objeto STRING, objeto STRING, mensagem STRING, detalhe STRING, usuario STRING""",
    "execucao_passo": """execucao_id STRING, onda STRING, passo STRING, inicio TIMESTAMP, fim TIMESTAMP,
                         segundos DOUBLE, status STRING, erro STRING, usuario STRING""",
    "unidade_status": """tipo STRING, unidade STRING, onda STRING, estado STRING, detalhe STRING,
                         ts_atualizado TIMESTAMP, atualizado_por STRING, execucao_id STRING""",
    "aprovacao": """ts TIMESTAMP, objeto STRING, script STRING, decisao STRING, comentario STRING,
                    usuario STRING, nota_no_momento DOUBLE, sql_hash STRING""",
}


class Controle:
    def __init__(self, spark, schema_ctrl, execucao_id, onda):
        self.spark, self.C, self.execucao_id, self.onda = spark, schema_ctrl, execucao_id, onda
        self._eventos, self._estados, self._pronto, self._usuario = [], [], False, None

    # ------------------------------------------------------------------ infra
    def garantir_tabelas(self):
        if self._pronto:
            return
        for nome, cols in DDL.items():
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.C}.{nome} ({cols}) "
                           f"COMMENT 'Controle do acelerador Qlik: {nome}'")
        self._pronto = True

    @property
    def usuario(self):
        if self._usuario is None:
            self._usuario = self.spark.sql("SELECT current_user() AS u").first().u
        return self._usuario

    @staticmethod
    def _agora():
        return datetime.now(timezone.utc).replace(tzinfo=None)

    # ------------------------------------------------------------------ eventos
    def evento(self, passo, mensagem, nivel="info", objeto=None, tipo_objeto=None, detalhe=None):
        det = detalhe if detalhe is None or isinstance(detalhe, str) else json.dumps(detalhe, ensure_ascii=False, default=str)
        self._eventos.append((self._agora(), self.execucao_id, self.onda, passo, nivel, tipo_objeto, objeto,
                              str(mensagem)[:4000], det, self.usuario))

    def estado(self, tipo, unidade, estado, detalhe=None):
        assert estado in ESTADOS, f"estado desconhecido: {estado}"
        det = detalhe if detalhe is None or isinstance(detalhe, str) else json.dumps(detalhe, ensure_ascii=False, default=str)
        self._estados.append((tipo, unidade, self.onda, estado, det, self._agora(), self.usuario, self.execucao_id))

    def gravar(self):
        """Grava eventos e estados pendentes. Chamado no fim de cada etapa."""
        self.garantir_tabelas()
        if self._eventos:
            self.spark.createDataFrame(self._eventos, DDL["evento"]).write.mode("append").saveAsTable(f"{self.C}.evento")
            self._eventos = []
        if self._estados:
            # último estado por unidade vence
            ultimo = {}
            for e in self._estados:
                ultimo[(e[0], e[1])] = e
            df = self.spark.createDataFrame(list(ultimo.values()), DDL["unidade_status"])
            df.createOrReplaceTempView("_novos_estados")
            self.spark.sql(f"""
                MERGE INTO {self.C}.unidade_status t USING _novos_estados s
                ON t.tipo = s.tipo AND t.unidade = s.unidade
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *""")
            self._estados = []

    # ------------------------------------------------------------------ etapas
    @contextmanager
    def etapa(self, passo):
        """Envolve um passo: mede duração, registra sucesso ou erro e grava tudo no fim."""
        self.garantir_tabelas()
        inicio, t0 = self._agora(), time.time()
        self.evento(passo, "início do passo")
        try:
            yield self
        except Exception as e:
            fim = self._agora()
            self.evento(passo, f"falhou: {e}", nivel="erro", detalhe=traceback.format_exc()[-3000:])
            self._registrar_passo(passo, inicio, fim, time.time() - t0, "erro", str(e)[:2000])
            self.gravar()
            raise
        fim = self._agora()
        self.evento(passo, f"concluído em {time.time() - t0:.0f}s")
        self._registrar_passo(passo, inicio, fim, time.time() - t0, "ok", None)
        self.gravar()

    def _registrar_passo(self, passo, inicio, fim, segundos, status, erro):
        self.spark.createDataFrame(
            [(self.execucao_id, self.onda, passo, inicio, fim, float(segundos), status, erro, self.usuario)],
            DDL["execucao_passo"]).write.mode("append").saveAsTable(f"{self.C}.execucao_passo")

    # ------------------------------------------------------------------ aprovações
    def aprovacoes(self):
        """Última decisão humana por objeto: {objeto: (decisao, comentario, usuario, sql_hash)}."""
        self.garantir_tabelas()
        linhas = self.spark.sql(f"""
            SELECT objeto, decisao, comentario, usuario, sql_hash FROM (
              SELECT *, row_number() OVER (PARTITION BY objeto ORDER BY ts DESC) AS rn FROM {self.C}.aprovacao)
            WHERE rn = 1""").collect()
        return {r.objeto: (r.decisao, r.comentario, r.usuario, r.sql_hash) for r in linhas}
