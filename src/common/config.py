import os
from dotenv import load_dotenv
from genai_client import get_secret_payload

# Load .env
load_dotenv()


# =========================
# BIGQUERY + GCS
# =========================
PROJECT_ID = os.getenv("PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
GCS_BUCKET = os.getenv("GCS_BUCKET")


# =========================
# TABLES
# =========================
PERSONS = os.getenv("PERSONS")
FINGERPRINT = os.getenv("FINGERPRINT")
CONCEPTS = os.getenv("CONCEPTS")
RESEARCHERS_RAW = os.getenv("RESEARCHERS_RAW")
RESEARCHERS = os.getenv("RESEARCHERS")
PUBLICATIONS=os.getenv("PUBLICATIONS")
PUBLICATIONS_AUTHOR_MAP=os.getenv("PUBLICATIONS_AUTHOR_MAP")

# =========================
# API CONFIG (From Secret Manager)
# =========================
# Use the Secret Name/ID as it appears in the GCP Console
API_KEY = get_secret_payload("suny-gai-pure-api-key-dev") 
API_BASE_URL = os.getenv("API_BASE_URL") # Usually kept as env var as it's not a secret

# =========================
# GEMINI
# =========================
GEMINI_API_KEY = get_secret_payload("suny-gai-gemini-api-key-dev")

# =========================
# PIPELINE CONFIG
# =========================
JOB_TYPE = os.getenv("JOB_TYPE")

MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", 10))
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", 100))
EMBEDDING_DIMENSION = int(os.getenv("EMBEDDING_DIMENSION", 768))

CONCEPT_BATCH_SIZE = int(os.getenv("CONCEPT_BATCH_SIZE", 500))
FINGERPRINT_BATCH_SIZE = int(os.getenv("FINGERPRINT_BATCH_SIZE", 500))
PUBLICATION_BATCH_SIZE = int(os.getenv("PUBLICATION_BATCH_SIZE", 1000))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 1000))


