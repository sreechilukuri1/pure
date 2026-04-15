import json
import uuid
from google.cloud import storage
from common.config import GCS_BUCKET

client = storage.Client()


def upload_jsonl(rows, prefix="raw"):
    blob_name = f"{prefix}/{uuid.uuid4()}.jsonl"
    blob = client.bucket(GCS_BUCKET).blob(blob_name)

    blob.upload_from_string(
        "\n".join(json.dumps(r) for r in rows)
    )

    return f"gs://{GCS_BUCKET}/{blob_name}"
