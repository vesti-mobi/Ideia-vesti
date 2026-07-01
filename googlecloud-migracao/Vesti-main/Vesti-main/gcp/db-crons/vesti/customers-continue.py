import os
import gc
import logging
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

LOG_FILE = "/home/diego/crons/sync_customers_continue.log"
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
CSV_FILE = "/tmp/spooling_temp_customers_cont.csv"

# MESMO schema híbrido do customers-sync.py. Precisa ser idêntico ao que já
# está na tabela, senão o APPEND falha com "Field has changed type".
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
    logging.info("--- Iniciando continuação da carga de customers por cursor ---")

    try:
        client = bigquery.Client(project=PROJECT_ID)

        # Descobre qual o maior ID que já está no BigQuery para continuar a partir dele
        logging.info("Buscando o último ID processado no BigQuery...")
        bq_query = f"SELECT max(id) as max_id FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"
        query_job = client.query(bq_query)
        results = query_job.result()

        ultimo_id = None
        for row in results:
            ultimo_id = row.max_id

        if not ultimo_id:
            logging.error("Não encontramos dados anteriores no BigQuery. Execute o script principal primeiro.")
            return

        logging.info(f"Continuando a partir do ID: {ultimo_id}")

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

            # Lotes de 15.000 (Rápido e não pesa na RAM usando busca direta por ID)
            tamanho_lote = 15000
            lote = 1
            total_linhas_novas = 0

            logging.info("Iniciando extração via cursor (WHERE id > ultimo_id)...")

            while True:
                # Query ultraveloz usando índice do ID
                query = f"SELECT * FROM customers WHERE id > '{ultimo_id}' ORDER BY id LIMIT {tamanho_lote}"
                chunk = pd.read_sql_query(query, engine)

                linhas = len(chunk)
                if linhas == 0 or chunk.empty:
                    logging.info("Nenhum registro novo encontrado. Carga de continuação concluída!")
                    break

                logging.info(f"Processando lote {lote} com {linhas} linhas...")
                total_linhas_novas += linhas

                # Limpa colunas inteiras: NULL faz o pandas ler como float (10.0).
                # Int64 anulável grava "10" e "" (NULL), compatível com INT64.
                for col in INT_COLUMNS:
                    if col in chunk.columns:
                        chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("Int64")

                chunk.to_csv(CSV_FILE, index=False, header=True)

                # WRITE_APPEND adiciona/soma os dados sem apagar o que já foi feito.
                # Schema híbrido fixo (autodetect=False) para casar com a tabela existente.
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

                logging.info(f"Lote {lote} somado com sucesso ao BigQuery.")

                # Atualiza o ponteiro para o próximo loop
                ultimo_id = chunk['id'].iloc[-1]

                del chunk
                gc.collect()

                lote += 1

        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)

        logging.info(f"--- Continuação finalizada! Adicionadas mais {total_linhas_novas} linhas ---")

    except Exception as e:
        logging.error(f"Erro durante a execução: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_sync()
