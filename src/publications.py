import requests
import pandas as pd
import time
import logging
import json
import uuid
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery, storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from common.config import (
    API_KEY, API_BASE_URL, PROJECT_ID, BQ_DATASET, GCS_BUCKET,
    PUBLICATIONS, PUBLICATIONS_AUTHOR_MAP
)

# =========================
# LOGGING (WITH ROTATION)
# =========================

BASE_DIR = os.path.dirname(__file__)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

INFO_LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "error.log")

formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Info logs
info_handler = RotatingFileHandler(
    INFO_LOG_FILE, maxBytes=10_000_000, backupCount=5
)
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(formatter)

# Error logs
error_handler = RotatingFileHandler(
    ERROR_LOG_FILE, maxBytes=5_000_000, backupCount=3
)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(info_handler)
logger.addHandler(error_handler)

logger.info("Logging initialized ✅")

# =========================
# CONFIG
# =========================
class Config:
    BASE_URL = API_BASE_URL
    API_KEY = API_KEY

    PAGE_SIZE = 1000
    SLEEP_SECONDS = 0.0
    REQUEST_TIMEOUT = 60
    MAX_RETRIES = 5

    BQ_PROJECT = PROJECT_ID
    BQ_DATASET = BQ_DATASET

    PUBLICATIONS_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{PUBLICATIONS}"
    AUTHOR_MAP_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{PUBLICATIONS_AUTHOR_MAP}"

    GCS_BUCKET = GCS_BUCKET
    GCS_PREFIX = "pure_pipeline"


# =========================
# UTILS
# =========================
def current_timestamp():
    return datetime.now(timezone.utc)


def safe_get(d, path, default=None):
    for key in path:
        if not isinstance(d, dict):
            return default
        d = d.get(key)
        if d is None:
            return default
    return d


def get_localized_text(obj):
    if not isinstance(obj, dict):
        return None
    return next((v.strip() for v in obj.values() if isinstance(v, str) and v.strip()), None)


# =========================
# API CLIENT
# =========================
class APIClient:
    def __init__(self, config: Config):
        self.config = config
        self.session = self._create_session()

    def _create_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.MAX_RETRIES,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def call(self, endpoint, params=None):
        url = f"{self.config.BASE_URL}/{endpoint}"
        headers = {"api-key": self.config.API_KEY, "Accept": "application/json"}

        for attempt in range(3):
            try:
                logger.info(f"[API] Calling {endpoint} | params={params} | attempt={attempt+1}")

                res = self.session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.config.REQUEST_TIMEOUT
                )

                logger.info(f"[API] Status Code: {res.status_code}")
                res.raise_for_status()

                data = res.json()
                logger.info(f"[API] Response items: {len(data.get('items', []))}")

                return data

            except requests.exceptions.RequestException as e:
                logger.warning(f"[API] Error attempt {attempt+1}: {str(e)}")

                if attempt == 2:
                    logger.error("[API] Max retries reached. Failing.")
                    raise

                sleep_time = 2 ** attempt
                logger.info(f"[API] Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
        return None


# =========================
# TRANSFORMERS
# =========================
def extract_publication(item):
    uuid_val = item.get("uuid")
    now = current_timestamp()

    pub_date = None
    for status in item.get("publicationStatuses", []):
        if status.get("current") and safe_get(status, ["publicationStatus", "term", "en_US"]) == "Published":
            pd_info = status.get("publicationDate", {})
            if pd_info.get("year"):
                pub_date = f"{pd_info.get('year'):04d}-{pd_info.get('month',1):02d}-{pd_info.get('day',1):02d}"
            break

    main_title = safe_get(item, ['title', 'value']) or ""
    sub_title = safe_get(item, ['subTitle', 'value'])
    title = f"{main_title} : {sub_title}" if sub_title else main_title
    title = title.strip()

    return {
        "id": uuid_val,
        "title": title,
        "abstract": get_localized_text(item.get("abstract")),
        "publication_date": pub_date,
        "citations": 0,
        "external_id": None,
        "external_system": None,
        "created_at": now,
        "updated_at": now
    }


def extract_authors(item):
    uuid_val = item.get("uuid")
    now = current_timestamp()
    authors = []

    for c in item.get("contributors") or []:
        if isinstance(c, dict) and c.get("typeDiscriminator") == "InternalContributorAssociation":
            person_uuid = safe_get(c, ["person", "uuid"])
            if person_uuid:
                authors.append({
                    "author_id": person_uuid,
                    "publication_id": uuid_val,
                    "created_at": now,
                    "updated_at": now
                })

    return authors


# =========================
# GCS WRITER
# =========================
class GCSWriter:
    def __init__(self, config: Config):
        self.client = storage.Client()
        self.bucket = self.client.bucket(config.GCS_BUCKET)
        self.prefix = config.GCS_PREFIX

    def upload_jsonl(self, records, filename):
        blob = self.bucket.blob(f"{self.prefix}/{filename}")

        jsonl_data = "\n".join(json.dumps(r, default=str) for r in records)
        size_kb = len(jsonl_data.encode("utf-8")) / 1024

        logger.info(f"[GCS] Uploading {filename} | records={len(records)} | size={size_kb:.2f} KB")

        blob.upload_from_string(jsonl_data, content_type="application/json")

        uri = f"gs://{self.bucket.name}/{blob.name}"
        logger.info(f"[GCS] Uploaded → {uri}")

        return uri


# =========================
# BIGQUERY LOADER
# =========================
class BigQueryLoader:
    def __init__(self, config: Config):
        self.client = bigquery.Client(project=config.BQ_PROJECT)

    def load_from_gcs(self, gcs_uri, table_name, schema):
        logger.info(f"[BQ] Loading {gcs_uri}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            schema=schema
        )

        job = self.client.load_table_from_uri(
            gcs_uri,
            table_name,
            job_config=job_config
        )

        logger.info(f"[BQ] Job started: {job.job_id}")
        job.result()
        logger.info(f"[BQ] Job completed: {job.job_id}")


# =========================
# SCHEMA
# =========================
def get_schemas():
    publications_schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("citations", "INTEGER"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("abstract", "STRING"),
        bigquery.SchemaField("publication_date", "DATE"),
        bigquery.SchemaField("external_id", "STRING"),
        bigquery.SchemaField("external_system", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    author_map_schema = [
        bigquery.SchemaField("author_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("publication_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    return publications_schema, author_map_schema


# =========================
# DATA FETCHER (PARALLEL)
# =========================
class DataFetcher:
    def __init__(self, client, config, loader, gcs):
        self.client = client
        self.config = config
        self.loader = loader
        self.gcs = gcs
        self.max_workers = 3

    def process_batch(self, offset, batch_num):
        try:
            logger.info(f"[BATCH {batch_num}] Fetching offset={offset}")

            params = {"offset": offset, "size": self.config.PAGE_SIZE}
            data = self.client.call("research-outputs", params=params)
            items = data.get("items", [])

            if not items:
                return 0

            publications = []
            author_map = []

            for item in items:
                publications.append(extract_publication(item))
                author_map.extend(extract_authors(item))

            batch_id = str(uuid.uuid4())

            pub_uri = self.gcs.upload_jsonl(publications, f"publications/{batch_id}.jsonl")
            auth_uri = self.gcs.upload_jsonl(author_map, f"author_map/{batch_id}.jsonl")

            pub_schema, auth_schema = get_schemas()

            self.loader.load_from_gcs(pub_uri, self.config.PUBLICATIONS_TABLE, pub_schema)
            self.loader.load_from_gcs(auth_uri, self.config.AUTHOR_MAP_TABLE, auth_schema)

            logger.info(f"[BATCH {batch_num}] Completed ✅")

            return len(publications)

        except Exception as e:
            logger.error(f"[BATCH {batch_num}] Failed ❌ {str(e)}")
            return 0

    def fetch_and_load(self):
        logger.info("========== PARALLEL PIPELINE STARTED ==========")

        offset = 0
        batch_num = 1
        total_records = 0
        futures = []
        stop = False

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            while not stop:
                while len(futures) < self.max_workers:
                    futures.append(
                        executor.submit(self.process_batch, offset, batch_num)
                    )
                    offset += self.config.PAGE_SIZE
                    batch_num += 1

                for future in as_completed(futures):
                    result = future.result()
                    futures.remove(future)

                    if result == 0:
                        stop = True
                    else:
                        total_records += result

                    break

        logger.info(f"========== PIPELINE COMPLETED | Total records: {total_records} ==========")


# =========================
# MAIN
# =========================
def run():
    logger.info("Starting Research Outputs Pipeline...")

    config = Config()

    client = APIClient(config)
    loader = BigQueryLoader(config)
    gcs = GCSWriter(config)

    fetcher = DataFetcher(client, config, loader, gcs)
    fetcher.fetch_and_load()

    logger.info("Pipeline finished successfully 🎉")


