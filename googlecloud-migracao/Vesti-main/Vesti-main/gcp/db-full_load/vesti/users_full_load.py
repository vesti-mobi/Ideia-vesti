import os
import json
import logging
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

LOG_FILE = "/home/diego/crons/sync_users.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

BASTION_IP = "b.meuvesti.com"
BASTION_USER = "diego"
BASTION_PASS = "Tobi17092013#"
DB_HOST = "vesti-db-rds-prd.cc6sxzxxktuf.us-east-1.rds.amazonaws.com"
DB_PORT = 5432
DB_USER = "root"
DB_PASS = "3eroep3KLwDaDLm"
DB_NAME = "erp_prod"

PROJECT_ID = "vesti-data-499015"
BQ_DATASET = "vestilake_BI"
BQ_TABLE = "odbc_users"
CSV_FILE = "/tmp/spooling_temp_users.csv"

# 1. QUERY CORRIGIDA: Apenas as 12 colunas estruturadas essenciais (sem os JSONs pesados)
QUERY = """
SELECT
    id, role_id, domain_id, name, lastname, email,
    status, phone, cellphone, document, created_at, updated_at
FROM users
"""

# Schema híbrido fixo das 12 colunas (tipos conforme o Postgres).
# Idêntico ao usado no users-daily.py, para o APPEND diário casar com a tabela.
SCHEMA_FORNECIDO = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("role_id", "STRING"),
    bigquery.SchemaField("domain_id", "INT64"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("lastname", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("status", "INT64"),
    bigquery.SchemaField("phone", "STRING"),
    bigquery.SchemaField("cellphone", "STRING"),
    bigquery.SchemaField("document", "STRING"),
    bigquery.SchemaField("created_at", "DATETIME"),
    bigquery.SchemaField("updated_at", "DATETIME"),
]

# Colunas inteiras: limpamos no pandas para evitar "10.0" (NULL faz virar float),
# que falha ao carregar em campo INT64 no BigQuery.
INT_COLUMNS = [f.name for f in SCHEMA_FORNECIDO if f.field_type == "INT64"]

def run_sync():
    logging.info("--- Iniciando sincronização de users (Otimizada) ---")

    try:
        logging.info("Conectando ao Bastion e ao PostgreSQL...")
        with SSHTunnelForwarder(
            (BASTION_IP, 22),
            ssh_username=BASTION_USER,
            ssh_password=BASTION_PASS,
            remote_bind_address=(DB_HOST, DB_PORT),
        ) as tunnel:
            engine = create_engine(
                f"postgresql://{DB_USER}:{DB_PASS}@127.0.0.1:{tunnel.local_bind_port}/{DB_NAME}"
            )

            total_linhas = 0
            first_chunk = True
            lote = 1

            logging.info("Iniciando extração de dados em lotes...")
            # chunksize seguro de 5000 linhas para preservar a memória RAM
            for chunk in pd.read_sql_query(QUERY, engine, chunksize=5000):
                linhas = len(chunk)
                logging.info(f"Processando lote {lote} com {linhas} linhas.")

                total_linhas += linhas

                # Limpa colunas inteiras: NULL faz o pandas ler como float (10.0).
                for col in INT_COLUMNS:
                    if col in chunk.columns:
                        chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("Int64")

                mode = "w" if first_chunk else "a"
                header = first_chunk
                chunk.to_csv(CSV_FILE, index=False, mode=mode, header=header)

                first_chunk = False
                lote += 1

        if total_linhas == 0:
            logging.info("Nenhum dado encontrado. Sincronização finalizada.")
            return

        logging.info(f"Extração concluída. Total de {total_linhas} registros baixados.")

        logging.info("Conectando ao BigQuery...")
        client = bigquery.Client(project=PROJECT_ID)

        logging.info("Iniciando upload (Truncate) com schema híbrido fixo...")

        # 2. CONFIGURAÇÃO JOB: schema fixo (autodetect=False) para o daily casar depois
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            schema=SCHEMA_FORNECIDO,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            allow_quoted_newlines=True,
        )

        with open(CSV_FILE, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
                job_config=job_config,
            )
        job.result()
        logging.info("Upload concluído com sucesso.")

        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)
            logging.info("Arquivo temporário removido.")

        logging.info("--- Sincronização finalizada com sucesso ---")

    except Exception as e:
        logging.error(f"Erro durante a execução: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_sync()
