#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
atualizar_logs.py - ETL incremental de logs de estoque (Postgres -> Postgres).

Copia registros de movimentacao de estoque do banco de ORIGEM (DB_ORIGEM_URL)
para a tabela de historico `public.stock_logs_historico` no banco de DESTINO
(DB_DESTINO_URL), construindo um historico append-only versionado (a origem e
um snapshot que perde o passado).

Seguranca de memoria (CRITICO - a origem gera ~1,5M linhas/dia):
  - Leitura via CURSOR SERVER-SIDE (named cursor do psycopg2): o Postgres mantem
    o resultado e nos entrega em blocos; o dataset inteiro NUNCA fica em RAM.
  - Escrita em lotes de 5.000 linhas com COMMIT a cada lote, liberando memoria
    e locks continuamente.

Datas dinamicas (modo de operacao):
  - Destino VAZIO     -> backfill desde DATA_INICIAL_BACKFILL ate o momento atual.
  - Destino COM dados -> incremental continuo: copia apenas created_at > MAX(created_at)
                         ja presente no destino.

Variaveis de ambiente:
  DB_ORIGEM_URL   (obrigatoria)  - string de conexao do Postgres de origem.
  DB_DESTINO_URL  (obrigatoria)  - string de conexao do Postgres de destino.
  TABELA_ORIGEM   (opcional)     - default 'public.stock_logs'.

A tabela de destino deve existir previamente. DDL de referencia:
  CREATE TABLE IF NOT EXISTS public.stock_logs_historico (
      domain_id   bigint,
      company_id  bigint,
      product_id  bigint,
      stock_id    bigint,
      created_at  timestamptz,
      action      text,
      origin      text,
      user_id     bigint,
      order_id    bigint
  );
  CREATE INDEX IF NOT EXISTS idx_slh_created_at
      ON public.stock_logs_historico (created_at);
"""

import os
import sys
from datetime import datetime

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------
DB_ORIGEM_URL = os.environ.get("DB_ORIGEM_URL")
DB_DESTINO_URL = os.environ.get("DB_DESTINO_URL")

# Tabela de origem (logs ao vivo). Sobrescrevivel por env sem mexer no codigo.
TABELA_ORIGEM = os.environ.get("TABELA_ORIGEM", "public.stock_logs")
TABELA_DESTINO = "public.stock_logs_historico"

# Quando o destino esta vazio, o backfill comeca aqui.
DATA_INICIAL_BACKFILL = datetime(2026, 5, 30, 0, 0, 0)

# Colunas copiadas. A ORDEM importa: e usada no SELECT e no INSERT.
COLUNAS = [
    "domain_id", "company_id", "product_id", "stock_id",
    "created_at", "action", "origin", "user_id", "order_id",
]

# Tamanho do lote: quantas linhas o cursor server-side puxa por round-trip
# e quantas sao inseridas por commit. 5.000 equilibra throughput x memoria.
LOTE = 5000


def log(msg):
    """Log com timestamp UTC e flush imediato (aparece em tempo real no Actions)."""
    print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def obter_watermark(destino_conn):
    """Maior created_at ja presente no destino, ou None se a tabela estiver vazia."""
    with destino_conn.cursor() as cur:
        cur.execute(f"SELECT MAX(created_at) FROM {TABELA_DESTINO}")
        (maximo,) = cur.fetchone()
    return maximo


def main():
    if not DB_ORIGEM_URL or not DB_DESTINO_URL:
        log("ERRO: defina DB_ORIGEM_URL e DB_DESTINO_URL no ambiente.")
        sys.exit(1)

    colunas_sql = ", ".join(COLUNAS)
    placeholders = ", ".join(["%s"] * len(COLUNAS))
    insert_sql = f"INSERT INTO {TABELA_DESTINO} ({colunas_sql}) VALUES ({placeholders})"

    origem_conn = None
    destino_conn = None
    inicio = datetime.utcnow()

    try:
        # --- Conexao de destino: decide a janela de datas -------------------
        log("Conectando ao DESTINO...")
        destino_conn = psycopg2.connect(DB_DESTINO_URL)
        destino_conn.autocommit = False

        try:
            watermark = obter_watermark(destino_conn)
        except psycopg2.Error as e:
            destino_conn.rollback()
            log(f"ERRO ao ler {TABELA_DESTINO} (a tabela existe?). Detalhe: {e}")
            log("Crie a tabela de destino (veja a DDL no cabecalho deste script).")
            sys.exit(1)

        if watermark is None:
            data_inicio = DATA_INICIAL_BACKFILL
            log(f"Destino VAZIO -> BACKFILL desde {data_inicio} ate agora.")
        else:
            data_inicio = watermark
            log(f"Destino com dados -> INCREMENTAL: created_at > {data_inicio}.")

        # --- Conexao de origem: cursor server-side --------------------------
        log(f"Conectando a ORIGEM e abrindo cursor server-side em {TABELA_ORIGEM}...")
        origem_conn = psycopg2.connect(DB_ORIGEM_URL)
        # autocommit=False: o named cursor precisa de uma transacao para existir.
        origem_conn.autocommit = False

        select_sql = (
            f"SELECT {colunas_sql} FROM {TABELA_ORIGEM} "
            f"WHERE created_at > %s ORDER BY created_at"
        )

        # name=... -> cursor server-side (o Postgres nao manda tudo de uma vez).
        leitura = origem_conn.cursor(name="cur_stock_logs_export")
        leitura.itersize = LOTE
        leitura.execute(select_sql, (data_inicio,))

        gravacao = destino_conn.cursor()

        total = 0
        lote_num = 0
        while True:
            linhas = leitura.fetchmany(LOTE)
            if not linhas:
                break

            lote_num += 1
            # execute_batch: insere as 5.000 linhas com poucos round-trips.
            # Drop-in do executemany, porem MUITO mais rapido para carga em massa
            # e com o mesmo perfil de memoria (processamos so o lote atual).
            psycopg2.extras.execute_batch(gravacao, insert_sql, linhas, page_size=LOTE)
            destino_conn.commit()  # commit por lote -> libera memoria e locks

            total += len(linhas)
            log(f"Lote {lote_num:>5}: +{len(linhas):>5} linhas | acumulado: {total}")

        leitura.close()
        gravacao.close()
        # Encerra a transacao de leitura (read-only) sem efeitos colaterais.
        origem_conn.rollback()

        dur = (datetime.utcnow() - inicio).total_seconds()
        log(f"CONCLUIDO: {total} linhas inseridas em {TABELA_DESTINO} em {dur:.1f}s.")

    except Exception as e:
        if destino_conn is not None:
            try:
                destino_conn.rollback()
            except psycopg2.Error:
                pass
        log(f"ERRO FATAL: {e}")
        raise  # propaga para o job do GitHub Actions falhar
    finally:
        if origem_conn is not None:
            origem_conn.close()
        if destino_conn is not None:
            destino_conn.close()
        log("Conexoes encerradas.")


if __name__ == "__main__":
    main()
