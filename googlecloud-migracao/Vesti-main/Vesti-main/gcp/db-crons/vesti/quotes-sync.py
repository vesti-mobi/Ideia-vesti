import os
import logging
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

LOG_FILE = "/home/diego/crons/sync_quotes.log"
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
CSV_FILE = "/tmp/spooling_temp_quotes.csv"

# Puxa o DIA ANTERIOR COMPLETO via updated_at. A fronteira usa CURRENT_DATE do
# próprio Postgres, então segue o relógio do banco (independe do fuso do cron).
#   [ontem 00:00 , hoje 00:00 )
QUERY = """
SELECT
    id, domain_id, company_id, customer_id, code, total_price, obs, status,
    created_at, updated_at, payment_method_id, display_price, final_price,
    seller_id, status_attended, price_attended, address_id,
    company_method_payment_id, offprice_value, offprice_percentage, freight_id,
    freight_value, app, installments_allowed, status_payment,
    quote_integration_id, billing_address_id, items, payment_type,
    freight_description, freight_deadline, other_value, quote_integration_code,
    freight_vestiprime_name, quote_integration_date, card_id, reseller_id,
    reseller_seller_id
FROM quotes
WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
  AND updated_at <  CURRENT_DATE
ORDER BY id
"""

SCHEMA_FORNECIDO = [
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

# id é STRING -> os ids levam aspas no DELETE.
ID_IS_STRING = True

INT_COLUMNS = [f.name for f in SCHEMA_FORNECIDO if f.field_type == "INT64"]
JSON_COLUMNS = [f.name for f in SCHEMA_FORNECIDO if f.field_type == "JSON"]


def run_sync():
    logging.info("--- Iniciando sincronização de quotes (diária) ---")

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

            extracted_ids = []
            first_chunk = True
            lote = 1

            logging.info("Iniciando extração de dados em lotes...")
            for chunk in pd.read_sql_query(QUERY, engine, chunksize=100000):
                linhas = len(chunk)
                logging.info(f"Processando lote {lote} com {linhas} linhas.")

                extracted_ids.extend(chunk["id"].astype(str).tolist())

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

        total_ids = len(extracted_ids)
        if total_ids == 0:
            logging.info("Nenhum dado novo encontrado. Sincronização finalizada.")
            return

        logging.info(f"Extração concluída. Total de {total_ids} registros baixados.")

        logging.info("Conectando ao BigQuery e removendo registros antigos...")
        client = bigquery.Client(project=PROJECT_ID)

        if ID_IS_STRING:
            ids_string = ",".join([f"'{str(i)}'" for i in extracted_ids])
        else:
            ids_string = ",".join([str(i) for i in extracted_ids])

        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE id IN ({ids_string})
        """
        client.query(delete_query).result()
        logging.info("Registros antigos removidos do BigQuery.")

        logging.info("Iniciando upload (Append) do CSV para o BigQuery...")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            schema=SCHEMA_FORNECIDO,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
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
