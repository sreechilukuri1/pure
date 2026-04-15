import asyncio

from utils.logging import setup_logging

# Jobs
from persons import run as ingest_persons
from fingerprint_concepts import run as ingest_fingerprint_concepts
from publications import run as ingest_publications
from generate_embeddings import run as ingest_researchers


logger = setup_logging()


JOB_REGISTRY = {
    "INGEST_PERSONS": ingest_persons,
    "INGEST_FINGERPRINT_CONCEPTS": ingest_fingerprint_concepts,
    "INGEST_RESEARCHERS": ingest_researchers,
    "INGEST_PUBLICATIONS": ingest_publications,
}


def run_job(job_name, job_func):
    logger.info(f"🚀 Starting job: {job_name}")

    try:
        if asyncio.iscoroutinefunction(job_func):
            asyncio.run(job_func())
        else:
            job_func()

        logger.info(f"✅ Completed job: {job_name}")

    except Exception as e:
        logger.error(f"❌ Job failed: {job_name} | Error: {e}")
        raise  # stop pipeline if any job fails


def main():
    for job_name, job_func in JOB_REGISTRY.items():
        run_job(job_name, job_func)


if __name__ == "__main__":
    main()
