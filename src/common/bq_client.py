import pandas as pd
from google.cloud import bigquery
from common.config import PROJECT_ID, BQ_DATASET

client = bigquery.Client()


# =========================
# LOAD JSONL FROM GCS
# =========================
def load_jsonl_from_gcs(uri, table, write_disposition="WRITE_APPEND"):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table}"

    job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=bigquery.LoadJobConfig(
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition=write_disposition,
        ),
    )
    job.result()

    print(f"✅ Loaded JSONL into {table_id}")


# =========================
# LOAD DATAFRAME (NEW)
# =========================
def load_table_from_dataframe(df: pd.DataFrame, table: str, schema=None, write_disposition="WRITE_TRUNCATE"):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
        ignore_unknown_values=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"✅ Loaded {len(df)} rows into {table_id}")


# =========================
# INSERT ROWS (STREAMING)
# =========================
def insert_rows(table, rows):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table}"
    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(errors)


# =========================
# QUERY HELPERS
# =========================
def fetch_query(query):
    return list(client.query(query))


def fetch_query_df(query):
    return client.query(query).to_dataframe()


# =========================
# TABLE OPS
# =========================
def truncate_table(table):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table}"

    query = f"TRUNCATE TABLE `{table_id}`"
    job = client.query(query)
    job.result()

    print(f"✅ Table {table_id} truncated successfully.")
