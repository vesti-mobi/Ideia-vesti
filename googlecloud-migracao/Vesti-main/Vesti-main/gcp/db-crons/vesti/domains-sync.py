import os
import logging
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

# Configuração do arquivo de Log
LOG_FILE = "/home/diego/crons/domains.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Configurações do Banco e SSH
BASTION_IP = "b.meuvesti.com"
BASTION_USER = "diego"
BASTION_PASS = "Tobi17092013#"
DB_HOST = "vesti-db-rds-prd.cc6sxzxxktuf.us-east-1.rds.amazonaws.com"
DB_PORT = 5432
DB_USER = "root"
DB_PASS = "3eroep3KLwDaDLm"
DB_NAME = "erp_prod"

# Configurações do BigQuery
PROJECT_ID = "vesti-data-499015"
BQ_DATASET = "vestilake_BI"
BQ_TABLE = "odbc_domains"
CSV_FILE = "/tmp/spooling_temp.csv"

QUERY = "SELECT * FROM domains WHERE updated_at >= NOW() - INTERVAL '1 day'"


def run_sync():
    logging.info("--- Iniciando sincronização de domains ---")

    try:
        # 1. Conexão SSH e Banco de Dados
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

            # 2. Extração para CSV local e coleta de IDs
            extracted_ids = []
            first_chunk = True
            lote = 1

            logging.info("Iniciando extração de dados em lotes...")
            # Limite ajustado para 100.000 linhas
            for chunk in pd.read_sql_query(QUERY, engine, chunksize=100000):
                linhas = len(chunk)
                logging.info(f"Processando lote {lote} com {linhas} linhas.")

                # Coleta os IDs do lote atual
                extracted_ids.extend(chunk["id"].astype(str).tolist())

                # Salva no CSV (sobrescreve no primeiro lote, faz append nos seguintes)
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

        # 3. Exclusão dos IDs no BigQuery
        logging.info("Conectando ao BigQuery e removendo registros antigos...")
        client = bigquery.Client(project=PROJECT_ID)
        ids_string = ",".join(extracted_ids)

        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE ID IN ({ids_string})
        """
        client.query(delete_query).result()
        logging.info("Registros antigos removidos do BigQuery.")

        # 4. Carga direta (Append) do CSV para a tabela principal do BigQuery
        logging.info(
            "Iniciando upload (Append) do arquivo CSV para o BigQuery com schema forçado..."
        )

        schema_fornecido = [
            bigquery.SchemaField("ID", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("modulos", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_At", "TIMESTAMP"),
            bigquery.SchemaField("host", "STRING"),
            bigquery.SchemaField("login", "STRING"),
            bigquery.SchemaField("password", "STRING"),
            bigquery.SchemaField("options", "STRING"),
            bigquery.SchemaField("integration_id", "STRING"),
            bigquery.SchemaField("type_register", "STRING"),
            bigquery.SchemaField("partner_id", "STRING"),
            bigquery.SchemaField("angel_id", "STRING"),
            bigquery.SchemaField("has_webhook_products", "STRING"),
            bigquery.SchemaField("has_webhook_stocks", "STRING"),
            bigquery.SchemaField("has_webhook_prices", "STRING"),
            bigquery.SchemaField("integration_owner", "STRING"),
            bigquery.SchemaField("integration_type", "STRING"),
            bigquery.SchemaField("has_webhook_customers", "STRING"),
        ]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            schema=schema_fornecido,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,  # Permite quebras de linha em campos de texto
        )

        with open(CSV_FILE, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
                job_config=job_config,
            )
        job.result()  # Aguarda conclusão
        logging.info("Upload concluído com sucesso.")

        # 5. Limpeza do arquivo temporário
        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)
            logging.info("Arquivo temporário removido.")

        logging.info("--- Sincronização finalizada com sucesso ---")

    except Exception as e:
        logging.error(f"Erro durante a execução: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_sync()
