import os
import logging
import pandas as pd
from google.cloud import bigquery
from common.config import PROJECT_ID, BQ_DATASET

logger = logging.getLogger(__name__)
client = bigquery.Client()


# =========================
# LOAD JSONL FROM GCS (conservative default)
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

    logger.info(f"Loaded JSONL into {table_id} (job: {getattr(job, 'job_id', None)})")


# =========================
# LOAD DATAFRAME (conservative default -> append)
# =========================
def load_table_from_dataframe(df: pd.DataFrame, table: str, schema=None, write_disposition="WRITE_APPEND"):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
        ignore_unknown_values=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    logger.info(f"Loaded {len(df)} rows into {table_id} (job: {getattr(job, 'job_id', None)})")


# =========================
# INSERT ROWS (STREAMING)
# =========================
def insert_rows(table, rows):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table}"
    errors = client.insert_rows_json(table_id, rows)

    if errors:
        logger.error("Streaming insert errors: %s", errors)
        raise RuntimeError(errors)


# =========================
# QUERY HELPERS
# =========================
def fetch_query(query):
    return list(client.query(query))


def fetch_query_df(query):
    return client.query(query).to_dataframe()


# =========================
# STAGING + MERGE helper
# =========================
def load_jsonl_to_staging_and_merge(uri, table, merge_keys, schema=None):
    """Load JSONL to a staging table and MERGE into the target table.

    - uri: GCS uri
    - table: final table name (string)
    - merge_keys: string or list of column names to use as merge key
    - schema: optional BigQuery schema

    This helper loads the file into `{table}__staging` with WRITE_TRUNCATE and then performs
    a MERGE into the final table based on merge_keys.
    """
    if not merge_keys:
        raise ValueError("merge_keys must be provided for staging+merge")

    if isinstance(merge_keys, str):
        merge_keys = [merge_keys]

    staging_table = f"{table}__staging"
    staging_id = f"{PROJECT_ID}.{BQ_DATASET}.{staging_table}"
    final_id = f"{PROJECT_ID}.{BQ_DATASET}.{table}"

    # Load staging
    load_job = client.load_table_from_uri(
        uri,
        staging_id,
        job_config=bigquery.LoadJobConfig(
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            schema=schema,
        ),
    )
    load_job.result()
    logger.info("Loaded staging table %s (job=%s)", staging_id, getattr(load_job, "job_id", None))

    # Discover columns for MERGE
    table_ref = client.get_table(staging_id)
    columns = [f.name for f in table_ref.schema]

    # Build ON clause
    on_clause = " AND ".join([f"T.{k} = S.{k}" for k in merge_keys])

    # Build SET clause for updates (exclude merge keys)
    update_cols = [c for c in columns if c not in merge_keys]
    if update_cols:
        set_clause = ", ".join([f"T.{c} = S.{c}" for c in update_cols])
    else:
        set_clause = None

    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"S.{c}" for c in columns])

    merge_sql = f"""
    MERGE `{final_id}` T
    USING `{staging_id}` S
    ON {on_clause}
    """

    if set_clause:
        merge_sql += f"WHEN MATCHED THEN UPDATE SET {set_clause}\n"

    merge_sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"

    job = client.query(merge_sql)
    job.result()
    logger.info("Merge completed into %s (job=%s)", final_id, getattr(job, "job_id", None))

    # Optionally clean up staging
    client.delete_table(staging_id, not_found_ok=True)
    logger.info("Staging table %s removed", staging_id)


# =========================
# TABLE OPS (guarded truncate)
# =========================
def truncate_table(table):
    allow = os.getenv("ALLOW_TRUNCATE", "false").lower() == "true"
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table}"

    if not allow:
        logger.warning("Skipping TRUNCATE on %s because ALLOW_TRUNCATE is not set to true", table_id)
        return

    query = f"TRUNCATE TABLE `{table_id}`"
    job = client.query(query)
    job.result()

    logger.info("Table %s truncated successfully.", table_id)
