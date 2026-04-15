import pandas as pd
import logging
from typing import List

from google import genai
from google.cloud import bigquery

from common.config import GEMINI_API_KEY, PROJECT_ID, BQ_DATASET, RESEARCHERS, EMBEDDING_DIMENSION
from common.bq_client import fetch_query_df, load_table_from_dataframe
from common.queries import RESEARCHER_EMBEDDING_QUERY
from datetime import datetime, timezone



# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# =========================
# CLIENTS
# =========================
def get_clients():
    return genai.Client(api_key=GEMINI_API_KEY), bigquery.Client()


# =========================
# FETCH FROM BQ
# =========================
def fetch_source_data():
    logger.info("Fetching source data from BigQuery...")

    df = fetch_query_df(RESEARCHER_EMBEDDING_QUERY)

    logger.info(f"Fetched {len(df)} rows")
    return df


# =========================
# PREP + SPLIT
# =========================
def prepare_and_split(df: pd.DataFrame):
    df = df.copy()

    df["interests_text"] = df["researcher_interests"]

    df_to_embed = df[df["interests_text"] != ""].copy()
    df_empty = df[df["interests_text"] == ""].copy()

    df_to_embed["interests_text"] = (
        "Research interests include " + df_to_embed["interests_text"]
    )

    return df_to_embed, df_empty


# =========================
# EMBEDDING
# =========================
def batch_embed(client, texts: List[str], batch_size: int = 100):
    embeddings = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]

        resp = client.models.embed_content(
            model="gemini-embedding-001",
            contents=batch,
            config={"output_dimensionality": EMBEDDING_DIMENSION}
        )

        embeddings.extend([list(map(float, e.values)) for e in resp.embeddings])

    return embeddings


# =========================
# VALIDATION
# =========================
def validate(df: pd.DataFrame):
    sizes = df["embedding"].apply(len)

    assert sizes.min() == EMBEDDING_DIMENSION and sizes.max() == EMBEDDING_DIMENSION, \
        f"Embedding size mismatch. Expected {EMBEDDING_DIMENSION}"

    logger.info(f"Embedding validation passed (dim={EMBEDDING_DIMENSION})")


# =========================
# UPLOAD
# =========================
def upload(df: pd.DataFrame, bq_client):

    schema=[
        bigquery.SchemaField("researcher_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("external_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("external_system", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),

        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("affiliation", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("profile_picture_url", "STRING"),

        bigquery.SchemaField("researcher_interests", "STRING", mode="REPEATED"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    logger.info(f"Uploading {len(df)} rows to BigQuery...")
    job = load_table_from_dataframe(df=df, table= RESEARCHERS, schema = schema )
    

    logger.info("Upload completed successfully")


# =========================
# PIPELINE
# =========================
def run():
    gemini_client, bq_client = get_clients()

    # Step 0: Fetch
    df = fetch_source_data()

    # Step 1: Prepare
    df_to_embed, df_empty = prepare_and_split(df)

    logger.info(f"Embedding {len(df_to_embed)} rows (skipped {len(df_empty)} empty)")

    # Step 2: Embeddings
    df_to_embed["embedding"] = batch_embed(
        gemini_client,
        df_to_embed["interests_text"].tolist()
    )

    if not df_empty.empty:
        df_empty["embedding"] = batch_embed(
            gemini_client,
            ["No research interests available"] * len(df_empty)
        )

    # Step 3: Merge
    df_final = pd.concat([df_to_embed, df_empty], ignore_index=True)

    current_ts = datetime.now(timezone.utc)

    df_final["created_at"] = current_ts
    df_final["updated_at"] = current_ts

    # Step 4: Validate
    validate(df_final)

    # Step 5: Upload
    upload(df_final, bq_client)


