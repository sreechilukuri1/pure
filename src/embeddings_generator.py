import logging
import time

from src.utils.batching import chunked
from src.common.genai_client import embed
from src.common.bq_client import load_jsonl_from_gcs  , insert_rows , fetch_query
from src.common.telemetry import start_span
from src.utils.retry import default_retry
from src.common.config import (
    PROJECT_ID,
    DATASET,
    RAW_TABLE,
    FINAL_TABLE,
    EMBED_BATCH_SIZE,
)

logger = logging.getLogger(__name__)


# fetch only new rows that haven't been embedded yet
def fetch_new_rows(limit=10000):
    query = f"""
    SELECT r.*
    FROM `{PROJECT_ID}.{DATASET}.{RAW_TABLE}` r
    LEFT JOIN `{PROJECT_ID}.{DATASET}.{FINAL_TABLE}` e
    ON r.pure_id = e.pure_id AND r.term = e.term
    WHERE e.pure_id IS NULL
    LIMIT {limit}
    """
    return fetch_query(query)


# embedding call
@default_retry()
def embed_batch(texts):
    return embed(texts)


# process batch: embed + transform
def process_batch(batch):
    texts = [
        f"{r['term']} {r['researcher_name']}"
        for r in batch
    ]

    with start_span("embed_batch"):
        embeddings = embed_batch(texts)

    results = []
    now = time.time()

    for r, emb in zip(batch, embeddings):
        results.append({
            "pure_id": r["pure_id"],
            "researcher_name": r["researcher_name"],
            "term": r["term"],
            "rank": r["rank"],
            "weighted_rank": r["weighted_rank"],
            "embedding": emb,
            "embedded_at": now,
        })

    return results

# main pipeline
def run():
    total = 0

    with start_span("embedding_pipeline"):

        while True:
            with start_span("fetch_new_rows"):
                rows = fetch_new_rows()

            if not rows:
                logger.info("No more rows to embed")
                break

            logger.info(f"Fetched {len(rows)} new rows")

            for batch in chunked(rows, EMBED_BATCH_SIZE):

                with start_span("process_batch"):
                    results = process_batch(batch)

                with start_span("bq_insert"):
                    insert_rows(FINAL_TABLE, results)

                total += len(results)
                logger.info(f"Embedded batch of {len(results)} rows")

    logger.info(f"Embedding pipeline complete. Total rows: {total}")