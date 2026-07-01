import os
import json
import time
import logging
import gc
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

LOG_FILE = "/home/diego/populate/full_load_integrations.log"
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
BQ_TABLE = "odbc_integrations"
CSV_FILE = "/tmp/full_load_temp_integrations.csv"

BATCH_SIZE = 100000
SLEEP_SECONDS = 5


def run_full_load():
    logging.info("--- Iniciando Carga Completa (Massiva) de Integrations via OFFSET ---")

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
            client = bigquery.Client(project=PROJECT_ID)

            schema_fornecido = [
                bigquery.SchemaField("id", "INT64"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("options_set", "JSON"),
                bigquery.SchemaField("active", "BOOL"),
                bigquery.SchemaField("created_at", "DATETIME"),
                bigquery.SchemaField("updated_at", "DATETIME"),
            ]

            # Colunas inteiras: NULL faz o pandas ler como float (10.0),
            # que falha ao carregar em campo INT64 no BigQuery. Limpamos
            # SOMENTE estas.
            int_columns = [
                f.name for f in schema_fornecido if f.field_type == "INT64"
            ]

            json_columns = [
                f.name for f in schema_fornecido if f.field_type == "JSON"
            ]

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=False,
                schema=schema_fornecido,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                allow_quoted_newlines=True,
            )

            lote = 1
            offset = 0

            while True:
                logging.info(
                    f"Extraindo lote {lote} do PostgreSQL (OFFSET {offset})..."
                )
                query = f"SELECT * FROM integrations ORDER BY id ASC LIMIT {BATCH_SIZE} OFFSET {offset}"
                df = pd.read_sql_query(query, engine)

                linhas = len(df)
                if linhas == 0:
                    logging.info("Nenhum dado retornado. Carga completa finalizada.")
                    break

                logging.info(
                    f"Lote {lote} extraído com {linhas} linhas. Aplicando transformações..."
                )

                # Limpa apenas as colunas inteiras declaradas no schema.
                for col in int_columns:
                    if col in df.columns:
                        df[col] = (
                            pd.to_numeric(df[col], errors="coerce").astype("Int64")
                        )

                for j_col in json_columns:
                    if j_col in df.columns:
                        df[j_col] = df[j_col].apply(
                            lambda x: (
                                json.dumps(x)
                                if isinstance(x, (dict, list))
                                else (str(x).replace("'", '"') if pd.notnull(x) else x)
                            )
                        )

                logging.info("Salvando lote em disco...")
                df.to_csv(CSV_FILE, index=False)

                del df
                gc.collect()

                logging.info(f"Enviando lote {lote} para o BigQuery...")
                with open(CSV_FILE, "rb") as source_file:
                    job = client.load_table_from_file(
                        source_file,
                        f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
                        job_config=job_config,
                    )
                job.result()

                logging.info(
                    f"Lote {lote} concluído com sucesso. Pausando {SLEEP_SECONDS}s..."
                )
                lote += 1
                offset += BATCH_SIZE
                time.sleep(SLEEP_SECONDS)

        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)

        logging.info("--- Execução finalizada com sucesso ---")

    except Exception as e:
        logging.error(f"Erro durante a execução: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_full_load()
