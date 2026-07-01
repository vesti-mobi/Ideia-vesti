import os
import time
import logging
import gc
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

LOG_FILE = "/home/diego/populate/full_load_quotes.log"
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
BQ_TABLE = "odbc_quotes"
CSV_FILE = "/tmp/full_load_temp_quotes.csv"

BATCH_SIZE = 100000
SLEEP_SECONDS = 5


def run_full_load():
    logging.info("--- Iniciando Carga Completa (Massiva) de Quotes via OFFSET ---")

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
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("domain_id", "INT64"),
                bigquery.SchemaField("company_id", "STRING"),
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("code", "INT64"),
                bigquery.SchemaField("total_price", "NUMERIC"),
                bigquery.SchemaField("obs", "STRING"),
                bigquery.SchemaField("status", "INT64"),
                bigquery.SchemaField("created_at", "DATETIME"),
                bigquery.SchemaField("updated_at", "DATETIME"),
                bigquery.SchemaField("payment_method_id", "STRING"),
                bigquery.SchemaField("display_price", "NUMERIC"),
                bigquery.SchemaField("final_price", "NUMERIC"),
                bigquery.SchemaField("seller_id", "STRING"),
                bigquery.SchemaField("status_attended", "INT64"),
                bigquery.SchemaField("price_attended", "NUMERIC"),
                bigquery.SchemaField("address_id", "STRING"),
                bigquery.SchemaField("company_method_payment_id", "STRING"),
                bigquery.SchemaField("offprice_value", "NUMERIC"),
                bigquery.SchemaField("offprice_percentage", "FLOAT64"),
                bigquery.SchemaField("freight_id", "STRING"),
                bigquery.SchemaField("freight_value", "NUMERIC"),
                bigquery.SchemaField("app", "STRING"),
                bigquery.SchemaField("installments_allowed", "STRING"),
                bigquery.SchemaField("status_payment", "STRING"),
                bigquery.SchemaField("quote_integration_id", "STRING"),
                bigquery.SchemaField("billing_address_id", "STRING"),
                bigquery.SchemaField("items", "STRING"),
                bigquery.SchemaField("payment_type", "STRING"),
                bigquery.SchemaField("freight_description", "STRING"),
                bigquery.SchemaField("freight_deadline", "STRING"),
                bigquery.SchemaField("other_value", "NUMERIC"),
                bigquery.SchemaField("quote_integration_code", "STRING"),
                bigquery.SchemaField("freight_vestiprime_name", "STRING"),
                bigquery.SchemaField("quote_integration_date", "STRING"),
                bigquery.SchemaField("card_id", "STRING"),
                bigquery.SchemaField("reseller_id", "STRING"),
                bigquery.SchemaField("reseller_seller_id", "STRING"),
            ]

            # Colunas inteiras: NULL faz o pandas ler como float (10.0),
            # que falha ao carregar em campo INT64 no BigQuery. Limpamos
            # SOMENTE estas - as colunas NUMERIC/FLOAT64 (precos) sao preservadas.
            int_columns = [
                f.name for f in schema_fornecido if f.field_type == "INT64"
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
                query = f"SELECT * FROM quotes ORDER BY id ASC LIMIT {BATCH_SIZE} OFFSET {offset}"
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
