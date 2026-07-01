import os
import json
import logging
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

LOG_FILE = "/home/diego/crons/sync_companies.log"
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
BQ_TABLE = "odbc_companies"
CSV_FILE = "/tmp/spooling_temp_companies.csv"

QUERY = "SELECT * FROM companies WHERE updated_at >= NOW() - INTERVAL '1 day'"


def run_sync():
    logging.info("--- Iniciando sincronização de companies ---")

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

                for col in chunk.select_dtypes(include=["float64"]).columns:
                    chunk[col] = (
                        pd.to_numeric(chunk[col], errors="coerce")
                        .round()
                        .astype("Int64")
                    )

                json_columns = [
                    "parameters",
                    "integration_options",
                    "parameters_franquia_digital",
                    "markups_franchises",
                    "notification_providers",
                ]
                for j_col in json_columns:
                    if j_col in chunk.columns:
                        chunk[j_col] = chunk[j_col].apply(
                            lambda x: (
                                json.dumps(x)
                                if isinstance(x, (dict, list))
                                else (str(x).replace("'", '"') if pd.notnull(x) else x)
                            )
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

        ids_string = ",".join([f"'{str(id_val)}'" for id_val in extracted_ids])

        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE ID IN ({ids_string})
        """
        client.query(delete_query).result()
        logging.info("Registros antigos removidos do BigQuery.")

        logging.info(
            "Iniciando upload (Append) do arquivo CSV para o BigQuery com schema forçado..."
        )

        schema_fornecido = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("domain_id", "INT64"),
            bigquery.SchemaField("parent_id", "STRING"),
            bigquery.SchemaField("number", "STRING"),
            bigquery.SchemaField("tax_document", "STRING"),
            bigquery.SchemaField("social_name", "STRING"),
            bigquery.SchemaField("company_name", "STRING"),
            bigquery.SchemaField("inscription", "STRING"),
            bigquery.SchemaField("status", "INT64"),
            bigquery.SchemaField("created_at", "DATETIME"),
            bigquery.SchemaField("updated_at", "DATETIME"),
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("scheme_url", "STRING"),
            bigquery.SchemaField("market", "STRING"),
            bigquery.SchemaField("itunes", "STRING"),
            bigquery.SchemaField("acessar_loja", "INT64"),
            bigquery.SchemaField("icon", "STRING"),
            bigquery.SchemaField("text_banner", "STRING"),
            bigquery.SchemaField("cep", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("address_number", "STRING"),
            bigquery.SchemaField("complement", "STRING"),
            bigquery.SchemaField("neighborhood", "STRING"),
            bigquery.SchemaField("reference", "STRING"),
            bigquery.SchemaField("city_id", "STRING"),
            bigquery.SchemaField("state_id", "STRING"),
            bigquery.SchemaField("address_equal", "BOOL"),
            bigquery.SchemaField("cep2", "STRING"),
            bigquery.SchemaField("address2", "STRING"),
            bigquery.SchemaField("address_number2", "STRING"),
            bigquery.SchemaField("complement2", "STRING"),
            bigquery.SchemaField("neighborhood2", "STRING"),
            bigquery.SchemaField("reference2", "STRING"),
            bigquery.SchemaField("city2_id", "STRING"),
            bigquery.SchemaField("state2_id", "STRING"),
            bigquery.SchemaField("color_default", "STRING"),
            bigquery.SchemaField("logo_app", "STRING"),
            bigquery.SchemaField("appid", "STRING"),
            bigquery.SchemaField("phone1", "STRING"),
            bigquery.SchemaField("phone2", "STRING"),
            bigquery.SchemaField("site", "STRING"),
            bigquery.SchemaField("lojista", "BOOL"),
            bigquery.SchemaField("layout_webmobile", "INT64"),
            bigquery.SchemaField("markup", "INT64"),
            bigquery.SchemaField("stock_headquarter", "BOOL"),
            bigquery.SchemaField("list_headquarter", "BOOL"),
            bigquery.SchemaField("parameters", "JSON"),
            bigquery.SchemaField("markup_supplier", "NUMERIC"),
            bigquery.SchemaField("firebase_dynamic_link", "STRING"),
            bigquery.SchemaField("firebase_key", "STRING"),
            bigquery.SchemaField("firebase_ibi", "STRING"),
            bigquery.SchemaField("instagram", "STRING"),
            bigquery.SchemaField("bio", "STRING"),
            bigquery.SchemaField("firebase_sender_id", "STRING"),
            bigquery.SchemaField("integration_id", "INT64"),
            bigquery.SchemaField("integration_options", "JSON"),
            bigquery.SchemaField("parameters_franquia_digital", "JSON"),
            bigquery.SchemaField("markups_franchises", "JSON"),
            bigquery.SchemaField("has_integrations", "BOOL"),
            bigquery.SchemaField("custom_url", "STRING"),
            bigquery.SchemaField("access_ziro", "STRING"),
            bigquery.SchemaField("payment_link_enabled", "BOOL"),
            bigquery.SchemaField("payment_link_enabled_access", "BOOL"),
            bigquery.SchemaField("notification_providers", "JSON"),
            bigquery.SchemaField("has_payment", "BOOL"),
            bigquery.SchemaField("has_freight", "BOOL"),
            bigquery.SchemaField("has_trigger", "BOOL"),
            bigquery.SchemaField("survey_quantity", "INT64"),
            bigquery.SchemaField("survey_media", "NUMERIC"),
            bigquery.SchemaField("tax_document_number", "STRING"),
            bigquery.SchemaField("google_site_verification", "STRING"),
            bigquery.SchemaField("has_autostore", "BOOL"),
            bigquery.SchemaField("company_mirror_id", "STRING"),
            bigquery.SchemaField("dropshipping", "INT64"),
            bigquery.SchemaField("nearest_schedule_date", "DATETIME"),
            bigquery.SchemaField("company_stock_id", "STRING"),
        ]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            schema=schema_fornecido,
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
