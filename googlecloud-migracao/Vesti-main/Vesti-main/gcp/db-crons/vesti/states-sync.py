import os
import logging
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

LOG_FILE = "/home/diego/crons/sync_states.log"
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
BQ_TABLE = "odbc_states"
CSV_FILE = "/tmp/spooling_temp_states.csv"

# ATENÇÃO: a tabela `states` (UFs) NÃO tem created_at/updated_at, então não há
# como fazer carga incremental por data. Como é uma tabela de referência
# minúscula e praticamente estática (~27 linhas), fazemos recarga COMPLETA
# diária via WRITE_TRUNCATE — barato e sempre consistente.
QUERY = """
SELECT
    id, name, initials
FROM states
ORDER BY id
"""

SCHEMA_FORNECIDO = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("initials", "STRING"),
]

INT_COLUMNS = [f.name for f in SCHEMA_FORNECIDO if f.field_type == "INT64"]


def run_sync():
    logging.info("--- Iniciando sincronização de states (recarga completa diária) ---")

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
            for chunk in pd.read_sql_query(QUERY, engine, chunksize=5000):
                linhas = len(chunk)
                logging.info(f"Processando lote {lote} com {linhas} linhas.")
                total_linhas += linhas

                for col in INT_COLUMNS:
                    if col in chunk.columns:
                        chunk[col] = (
                            pd.to_numeric(chunk[col], errors="coerce").astype("Int64")
                        )

                mode = "w" if first_chunk else "a"
                header = first_chunk
                chunk.to_csv(CSV_FILE, index=False, mode=mode, header=header)

                first_chunk = False
                lote += 1

        if total_linhas == 0:
            logging.info("Nenhum dado encontrado. Sincronização finalizada.")
            return

        logging.info(f"Extração concluída. Total de {total_linhas} registros baixados.")

        logging.info("Conectando ao BigQuery e fazendo upload (Truncate)...")
        client = bigquery.Client(project=PROJECT_ID)

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
