import google.genai as genai
import os
from google.cloud import secretmanager
from dotenv import load_dotenv

load_dotenv()


def get_gemini_client(api_key: str | None = None):
    """Return a gemini/genai client. Prefer an explicit api_key passed or via env var.

    This avoids creating a client at import time and allows callers to provide keys from
    secret manager or env-based injection.
    """
    key = api_key or os.getenv("GEMINI_API_KEY")
    if key:
        return genai.Client(api_key=key)

    # Fallback to vertex/ADC mode if available
    return genai.Client(vertexai=True)


def embed_with_client(client, texts):
    resp = client.models.embed_content(
        model="gemini-embedding-001",
        contents=texts,
    )
    return [e.values for e in resp.embeddings]


def get_secret_payload(secret_id, version_id="latest"):
    """Retrieve a secret payload from Secret Manager.

    Uses PROJECT_ID from env if present. This function is safe to call lazily.
    """
    project_id = os.getenv("PROJECT_ID")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")