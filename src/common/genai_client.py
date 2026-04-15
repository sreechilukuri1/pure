import google.genai as genai
import os
from google.cloud import secretmanager
from dotenv import load_dotenv

load_dotenv()
client = genai.Client(vertexai=True)
PROJECT_ID = os.getenv("PROJECT_ID")

def embed(texts):
    resp = client.models.embed_content(
        model="gemini-embedding-001",
        contents=texts,
    )
    return [e.values for e in resp.embeddings]


def get_secret_payload(secret_id, version_id="latest"):
    """
    Retrieves the payload for the given secret version.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")