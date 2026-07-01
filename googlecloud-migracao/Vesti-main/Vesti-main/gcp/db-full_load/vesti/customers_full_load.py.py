import os
import gc
import logging
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

LOG_FILE = "/home/diego/crons/sync_customers.log"
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
BQ_TABLE = "odbc_customers"
CSV_FILE = "/tmp/spooling_temp_customers.csv"

# Schema híbrido fixo: cada coluna tipada conforme o Postgres.
# Colunas "character varying"/uuid viram STRING (a prova de dados sujos,
# ex.: integration_customer_id que tinha valores como "tdkdkdk").
SCHEMA_FORNECIDO = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("domain_id", "INT64"),
    bigquery.SchemaField("seller_id", "STRING"),
    bigquery.SchemaField("tax_document", "STRING"),
    bigquery.SchemaField("name_social_name", "STRING"),
    bigquery.SchemaField("lastname_company_name", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("phone", "STRING"),
    bigquery.SchemaField("status", "INT64"),
    bigquery.SchemaField("created_at", "DATETIME"),
    bigquery.SchemaField("updated_at", "DATETIME"),
    bigquery.SchemaField("customer_profile_id", "STRING"),
    bigquery.SchemaField("google_api_token", "STRING"),
    bigquery.SchemaField("cep", "STRING"),
    bigquery.SchemaField("address", "STRING"),
    bigquery.SchemaField("number", "STRING"),
    bigquery.SchemaField("complement", "STRING"),
    bigquery.SchemaField("neighborhood", "STRING"),
    bigquery.SchemaField("reference", "STRING"),
    bigquery.SchemaField("city_id", "STRING"),
    bigquery.SchemaField("state_id", "STRING"),
    bigquery.SchemaField("address_equal", "BOOL"),
    bigquery.SchemaField("cep2", "STRING"),
    bigquery.SchemaField("address2", "STRING"),
    bigquery.SchemaField("number2", "STRING"),
    bigquery.SchemaField("complement2", "STRING"),
    bigquery.SchemaField("neighborhood2", "STRING"),
    bigquery.SchemaField("reference2", "STRING"),
    bigquery.SchemaField("city2_id", "STRING"),
    bigquery.SchemaField("state2_id", "STRING"),
    bigquery.SchemaField("unread", "BOOL"),
    bigquery.SchemaField("lastvisited_at", "DATETIME"),
    bigquery.SchemaField("following", "INT64"),
    bigquery.SchemaField("price_franchise", "BOOL"),
    bigquery.SchemaField("integration_customer_id", "STRING"),
    bigquery.SchemaField("date_last_purchase", "DATETIME"),
    bigquery.SchemaField("tax_document_integration", "STRING"),
    bigquery.SchemaField("date_last_contact", "DATETIME"),
    bigquery.SchemaField("updated_follow", "DATETIME"),
    bigquery.SchemaField("is_checked_terms", "BOOL"),
    bigquery.SchemaField("status_virtual_store", "INT64"),
    bigquery.SchemaField("status_virtual_store_obs", "STRING"),
    bigquery.SchemaField("reseller_id", "STRING"),
    bigquery.SchemaField("referrer", "INT64"),
    bigquery.SchemaField("orders_quantity", "INT64"),
    bigquery.SchemaField("orders_amount", "NUMERIC"),
    bigquery.SchemaField("reseller_seller_id", "STRING"),
    bigquery.SchemaField("contacted", "BOOL"),
    bigquery.SchemaField("observations", "STRING"),
    bigquery.SchemaField("tax_document_number", "STRING"),
    bigquery.SchemaField("phone_number", "STRING"),
    bigquery.SchemaField("user_domain_id", "INT64"),
    bigquery.SchemaField("user_name", "STRING"),
    bigquery.SchemaField("user_lastname", "STRING"),
    bigquery.SchemaField("user_email", "STRING"),
    bigquery.SchemaField("user_phone", "STRING"),
    bigquery.SchemaField("user_tax_document", "STRING"),
    bigquery.SchemaField("user_updated_at", "DATETIME"),
    bigquery.SchemaField("general_updated_at", "DATETIME"),
    bigquery.SchemaField("utm_source", "STRING"),
    bigquery.SchemaField("approval_date", "DATETIME"),
    bigquery.SchemaField("origin_company_id", "STRING"),
    bigquery.SchemaField("instagram", "STRING"),
    bigquery.SchemaField("total_orders_count", "INT64"),
    bigquery.SchemaField("total_orders_amount", "NUMERIC"),
    bigquery.SchemaField("paid_orders_count", "INT64"),
    bigquery.SchemaField("paid_orders_amount", "NUMERIC"),
]

# Colunas inteiras: limpamos no pandas para evitar "10.0" (NULL faz virar float),
# que falha ao carregar em campo INT64 no BigQuery.
INT_COLUMNS = [f.name for f in SCHEMA_FORNECIDO if f.field_type == "INT64"]


def run_sync():
    logging.info("--- Iniciando sincronização ultra-leve de customers ---")

    try:
        client = bigquery.Client(project=PROJECT_ID)

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

            # Configurações de paginação estrita no banco
            tamanho_lote = 10000
            offset = 0
            is_first_load = True
            lote = 1
            total_linhas = 0

            logging.info("Iniciando extração paginada por LIMIT/OFFSET...")

            while True:
                # Busca lotes diretamente no banco ordenados por ID (essencial para paginação)
                query = f"SELECT * FROM customers ORDER BY id LIMIT {tamanho_lote} OFFSET {offset}"
                chunk = pd.read_sql_query(query, engine)

                linhas = len(chunk)
                if linhas == 0 or chunk.empty:
                    break  # Acabaram os dados

                logging.info(f"Processando lote {lote} (Registros {offset} até {offset + linhas}).")
                total_linhas += linhas

                # Limpa colunas inteiras: NULL faz o pandas ler como float (10.0).
                # Int64 anulável grava "10" e "" (NULL), compatível com INT64.
                for col in INT_COLUMNS:
                    if col in chunk.columns:
                        chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("Int64")

                # Salva estritamente o lote atual no arquivo temporário
                chunk.to_csv(CSV_FILE, index=False, header=True)

                # Define se limpa a tabela no BigQuery (apenas no 1º lote) ou se adiciona (nos seguintes)
                disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if is_first_load else bigquery.WriteDisposition.WRITE_APPEND

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    autodetect=False,
                    schema=SCHEMA_FORNECIDO,
                    write_disposition=disposition,
                    allow_quoted_newlines=True,
                )

                # Envia o lote atual para o BigQuery
                with open(CSV_FILE, "rb") as source_file:
                    job = client.load_table_from_file(
                        source_file,
                        f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
                        job_config=job_config,
                    )
                job.result()

                logging.info(f"Lote {lote} enviado com sucesso ao BigQuery.")

                # LIBERAÇÃO AGRESSIVA DE MEMÓRIA RAM
                del chunk
                gc.collect()

                is_first_load = False
                lote += 1
                offset += tamanho_lote  # Avança para o próximo bloco no banco

        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)

        logging.info(f"--- Sincronização finalizada com sucesso. Total: {total_linhas} registros ---")

    except Exception as e:
        logging.error(f"Erro durante a execução: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_sync()
