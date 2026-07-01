import os
import json
import logging
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from google.cloud import bigquery

LOG_FILE = "/home/diego/crons/sync_vesti_tables.log"
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

CHUNK_SIZE = 100000

# ---------------------------------------------------------------------------
# Configuração das tabelas.
#
# Estratégia de sincronização por tabela:
#   - "incremental": tabela tem coluna `updated_at`. Pegamos a marca d'água
#     (MAX(updated_at) no BigQuery) e puxamos do Postgres só as linhas com
#     updated_at >= marca. A carga é feita numa staging e aplicada na tabela
#     final via MERGE (upsert por `id`): atualiza o que mudou, insere o novo.
#   - "full": tabela de referência SEM `updated_at` (states, cities). Não há
#     como detectar deltas, então recarrega tudo (WRITE_TRUNCATE). São tabelas
#     pequenas e quase estáticas.
#
# A query do SELECT é montada a partir dos nomes das colunas do schema (não
# usamos SELECT *), garantindo que colunas removidas de propósito não voltem.
# ---------------------------------------------------------------------------
TABLES = [
    {
        "source": "quotes",
        "bq_table": "odbc_quotes",
        "schema": [
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
        ],
    },
    {
        "source": "product_details",
        "bq_table": "odbc_product_details",
        "schema": [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("domain_id", "INT64"),
            bigquery.SchemaField("company_id", "STRING"),
            bigquery.SchemaField("active", "BOOL"),
            bigquery.SchemaField("created_at", "DATETIME"),
            bigquery.SchemaField("updated_at", "DATETIME"),
        ],
    },
    {
        "source": "partners",
        "bq_table": "odbc_partners",
        "schema": [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("active", "BOOL"),
            bigquery.SchemaField("created_at", "DATETIME"),
            bigquery.SchemaField("updated_at", "DATETIME"),
            bigquery.SchemaField("provider_subaccount_id", "STRING"),
            bigquery.SchemaField("provider_token", "STRING"),
            bigquery.SchemaField("slug", "STRING"),
            bigquery.SchemaField("email", "STRING"),
        ],
    },
    {
        "source": "integrations",
        "bq_table": "odbc_integrations",
        "schema": [
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("options_set", "JSON"),
            bigquery.SchemaField("active", "BOOL"),
            bigquery.SchemaField("created_at", "DATETIME"),
            bigquery.SchemaField("updated_at", "DATETIME"),
        ],
    },
    {
        "source": "company_tags",
        "bq_table": "odbc_company_tags",
        "schema": [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("domain_id", "INT64"),
            bigquery.SchemaField("tag_id", "STRING"),
            bigquery.SchemaField("company_id", "STRING"),
            bigquery.SchemaField("created_at", "DATETIME"),
            bigquery.SchemaField("updated_at", "DATETIME"),
        ],
    },
    {
        "source": "tags",
        "bq_table": "odbc_tags",
        "schema": [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("created_at", "DATETIME"),
            bigquery.SchemaField("updated_at", "DATETIME"),
            bigquery.SchemaField("slug", "STRING"),
        ],
    },
    {
        # Sem updated_at -> reload completo (tabela de referência pequena).
        "source": "states",
        "bq_table": "odbc_states",
        "schema": [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("initials", "STRING"),
        ],
    },
    {
        # Sem updated_at -> reload completo (tabela de referência pequena).
        "source": "cities",
        "bq_table": "odbc_cities",
        "schema": [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("state_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
        ],
    },
]


def _clean_chunk(chunk, int_columns, json_columns):
    """Limpa INT64 (NULL vira float no pandas) e serializa colunas JSON."""
    for col in int_columns:
        if col in chunk.columns:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("Int64")
    for j_col in json_columns:
        if j_col in chunk.columns:
            chunk[j_col] = chunk[j_col].apply(
                lambda x: (
                    json.dumps(x)
                    if isinstance(x, (dict, list))
                    else (str(x).replace("'", '"') if pd.notnull(x) else x)
                )
            )
    return chunk


def _extract_to_csv(engine, query, csv_file, int_columns, json_columns):
    """Extrai a query em chunks para um CSV. Retorna total de linhas."""
    first_chunk = True
    lote = 1
    total_linhas = 0
    for chunk in pd.read_sql_query(query, engine, chunksize=CHUNK_SIZE):
        linhas = len(chunk)
        total_linhas += linhas
        logging.info(f"  Lote {lote}: {linhas} linhas.")
        chunk = _clean_chunk(chunk, int_columns, json_columns)
        mode = "w" if first_chunk else "a"
        chunk.to_csv(csv_file, index=False, mode=mode, header=first_chunk)
        first_chunk = False
        lote += 1
    return total_linhas


def _load_csv(client, csv_file, table_ref, schema, write_disposition):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=schema,
        write_disposition=write_disposition,
        allow_quoted_newlines=True,
    )
    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()


def get_watermark(client, bq_table):
    """MAX(updated_at) atual no BigQuery. None se a tabela estiver vazia/ausente."""
    table_ref = f"`{PROJECT_ID}.{BQ_DATASET}.{bq_table}`"
    try:
        rows = list(client.query(f"SELECT MAX(updated_at) AS wm FROM {table_ref}").result())
        return rows[0]["wm"] if rows else None
    except Exception as e:
        logging.warning(f"  Não foi possível ler marca d'água de {bq_table}: {e}")
        return None


def sync_full(config, engine, client):
    """Reload completo (WRITE_TRUNCATE) para tabelas sem updated_at."""
    source = config["source"]
    bq_table = config["bq_table"]
    schema = config["schema"]
    csv_file = f"/tmp/spooling_temp_{source}.csv"

    int_columns = [f.name for f in schema if f.field_type == "INT64"]
    json_columns = [f.name for f in schema if f.field_type == "JSON"]
    columns = ", ".join(f.name for f in schema)
    query = f"SELECT {columns} FROM {source}"

    logging.info(f"=== [FULL] '{source}' -> '{bq_table}' ===")
    total = _extract_to_csv(engine, query, csv_file, int_columns, json_columns)

    if total == 0:
        logging.info(f"  Nenhum dado. Pulando upload.")
        if os.path.exists(csv_file):
            os.remove(csv_file)
        return

    _load_csv(
        client,
        csv_file,
        f"{PROJECT_ID}.{BQ_DATASET}.{bq_table}",
        schema,
        bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    logging.info(f"  Reload completo concluído: {total} linhas.")
    if os.path.exists(csv_file):
        os.remove(csv_file)


def sync_incremental(config, engine, client):
    """Carga incremental via staging + MERGE (upsert por id)."""
    source = config["source"]
    bq_table = config["bq_table"]
    schema = config["schema"]
    csv_file = f"/tmp/spooling_temp_{source}.csv"
    staging_table = f"{bq_table}__staging"

    int_columns = [f.name for f in schema if f.field_type == "INT64"]
    json_columns = [f.name for f in schema if f.field_type == "JSON"]
    col_names = [f.name for f in schema]
    columns = ", ".join(col_names)

    logging.info(f"=== [INCREMENTAL] '{source}' -> '{bq_table}' ===")

    watermark = get_watermark(client, bq_table)
    if watermark is None:
        logging.info("  Tabela vazia/sem marca d'água -> puxando TODAS as linhas.")
        query = f"SELECT {columns} FROM {source}"
    else:
        # >= para não perder linhas com updated_at igual ao máximo (o MERGE é
        # idempotente, então reprocessar a borda não duplica nada).
        wm_str = pd.Timestamp(watermark).strftime("%Y-%m-%d %H:%M:%S.%f")
        logging.info(f"  Marca d'água: {wm_str}. Puxando updated_at >= marca.")
        query = (
            f"SELECT {columns} FROM {source} "
            f"WHERE updated_at >= '{wm_str}'"
        )

    total = _extract_to_csv(engine, query, csv_file, int_columns, json_columns)

    if total == 0:
        logging.info("  Nenhuma linha nova/alterada. Nada a fazer.")
        if os.path.exists(csv_file):
            os.remove(csv_file)
        return

    # 1) Carrega o delta numa staging (substitui a staging anterior).
    logging.info(f"  Carregando {total} linhas na staging '{staging_table}'...")
    _load_csv(
        client,
        csv_file,
        f"{PROJECT_ID}.{BQ_DATASET}.{staging_table}",
        schema,
        bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # 2) MERGE staging -> tabela final (upsert por id).
    non_id = [c for c in col_names if c != "id"]
    set_clause = ", ".join(f"T.{c} = S.{c}" for c in non_id)
    insert_cols = ", ".join(col_names)
    insert_vals = ", ".join(f"S.{c}" for c in col_names)
    merge_sql = f"""
        MERGE `{PROJECT_ID}.{BQ_DATASET}.{bq_table}` T
        USING `{PROJECT_ID}.{BQ_DATASET}.{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    logging.info("  Executando MERGE...")
    client.query(merge_sql).result()

    # 3) Limpa a staging e o CSV.
    client.query(
        f"DROP TABLE IF EXISTS `{PROJECT_ID}.{BQ_DATASET}.{staging_table}`"
    ).result()
    if os.path.exists(csv_file):
        os.remove(csv_file)
    logging.info(f"  MERGE concluído: {total} linhas upsertadas.")


def run_sync():
    logging.info("--- Iniciando sincronização diária (incremental) das tabelas Vesti ---")

    falhas = []
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

            for config in TABLES:
                has_updated_at = any(f.name == "updated_at" for f in config["schema"])
                # Uma tabela com erro não derruba as demais.
                try:
                    if has_updated_at:
                        sync_incremental(config, engine, client)
                    else:
                        sync_full(config, engine, client)
                except Exception as e:
                    falhas.append(config["source"])
                    logging.error(
                        f"[{config['source']}] Erro na sincronização: {e}",
                        exc_info=True,
                    )

        if falhas:
            logging.error(f"--- Finalizado COM FALHAS em: {', '.join(falhas)} ---")
            raise RuntimeError(f"Falha ao sincronizar: {', '.join(falhas)}")

        logging.info("--- Sincronização diária finalizada com sucesso ---")

    except Exception as e:
        logging.error(f"Erro fatal durante a execução: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_sync()
