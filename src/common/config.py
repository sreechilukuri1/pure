
import os
import logging
from dotenv import load_dotenv

# Load .env (harmless if not present)
load_dotenv()

logger = logging.getLogger(__name__)


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
PUBLICATIONS = os.getenv("PUBLICATIONS")
PUBLICATIONS_AUTHOR_MAP = os.getenv("PUBLICATIONS_AUTHOR_MAP")


# =========================
# SECRET / API CONFIG
# =========================
# Do NOT fetch secrets at import time. Prefer environment-injected secrets (Cloud Run Secret Manager)
# or call the lazy getters below from application startup.

_API_KEY = os.getenv("API_KEY")
_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
API_BASE_URL = os.getenv("API_BASE_URL")  # usually not secret


def get_api_key(refresh: bool = False):
	"""Return API key. If not present in env, attempt to fetch from Secret Manager lazily.

	This function caches value on first successful fetch. Call this from startup (entrypoint) so
	failures happen early and not during module import.
	"""
	global _API_KEY
	if _API_KEY and not refresh:
		return _API_KEY

	# attempt to fetch from Secret Manager only if env var not set
	try:
		if not _API_KEY:
			# import locally to avoid circular imports at module load time
			from common.genai_client import get_secret_payload
			_API_KEY = get_secret_payload("suny-gai-pure-api-key-dev")
			logger.info("Loaded API_KEY from Secret Manager")
	except Exception as e:
		logger.debug(f"Could not load API_KEY from Secret Manager: {e}")

	return _API_KEY


def get_gemini_api_key(refresh: bool = False):
	"""Return Gemini/GEMINI API key; lazy-load from Secret Manager if missing in env."""
	global _GEMINI_API_KEY
	if _GEMINI_API_KEY and not refresh:
		return _GEMINI_API_KEY

	try:
		if not _GEMINI_API_KEY:
			from common.genai_client import get_secret_payload
			_GEMINI_API_KEY = get_secret_payload("suny-gai-gemini-api-key-dev")
			logger.info("Loaded GEMINI_API_KEY from Secret Manager")
	except Exception as e:
		logger.debug(f"Could not load GEMINI_API_KEY from Secret Manager: {e}")

	return _GEMINI_API_KEY


# Convenience initializer to attempt to load secrets at startup (call from entrypoint before job imports)
def init_secrets():
	get_api_key()
	get_gemini_api_key()


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


