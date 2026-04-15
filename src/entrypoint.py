import asyncio
import signal
import sys
import os

from utils.logging import setup_logging


logger = setup_logging()

# Global flag set by signal handlers to request graceful shutdown
STOP_REQUESTED = False


def _handle_signal(signum, frame):
    global STOP_REQUESTED
    logger.warning(f"Received signal {signum}. Requesting graceful shutdown...")
    STOP_REQUESTED = True


def run_job(job_name, job_func):
    logger.info(f"Starting job: {job_name}")

    if STOP_REQUESTED:
        logger.warning(f"Skipping job {job_name} because shutdown was requested")
        return

    try:
        if asyncio.iscoroutinefunction(job_func):
            asyncio.run(job_func())
        else:
            job_func()

        logger.info(f"✅ Completed job: {job_name}")

    except Exception as e:
        logger.exception(f"Job failed: {job_name} | Error: {e}")
        raise  # preserve non-zero exit code for Cloud Run Job


def main():
    # Install simple signal handlers so Cloud Run can gracefully terminate the job.
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    # Initialize any secrets/config that may be required by job modules.
    # This will attempt to populate missing secrets from Secret Manager only at startup
    # (and will not run during module import).
    try:
        import common.config as cfg
        cfg.init_secrets()
    except Exception:
        logger.debug("Unable to init secrets at startup; proceeding — modules may fetch lazily")

    # Lazy import job modules to avoid doing heavy work (or secret access) at import time.
    # This helps the container start reliably in Cloud Run where we prefer predictable startup.
    from persons import run as ingest_persons
    from fingerprint_concepts import run as ingest_fingerprint_concepts
    from publications import run as ingest_publications
    from generate_embeddings import run as ingest_researchers

    JOB_REGISTRY = {
        "INGEST_PERSONS": ingest_persons,
        "INGEST_FINGERPRINT_CONCEPTS": ingest_fingerprint_concepts,
        "INGEST_RESEARCHERS": ingest_researchers,
        "INGEST_PUBLICATIONS": ingest_publications,
    }

    # Allow selecting a single job to run via JOB_TYPE env var or CLI arg.
    job_type = os.getenv("JOB_TYPE")
    if len(sys.argv) > 1:
        # first CLI arg can be the job name
        job_type = sys.argv[1]

    try:
        if job_type:
            # Normalize and match
            desired = job_type.strip().upper()
            matched = [k for k in JOB_REGISTRY.keys() if k == desired]
            if not matched:
                logger.error("Requested JOB_TYPE '%s' not found. Available: %s", job_type, list(JOB_REGISTRY.keys()))
                sys.exit(2)

            for job_name in matched:
                if STOP_REQUESTED:
                    logger.warning("Shutdown requested; aborting before starting %s", job_name)
                    break
                run_job(job_name, JOB_REGISTRY[job_name])

        else:
            for job_name, job_func in JOB_REGISTRY.items():
                if STOP_REQUESTED:
                    logger.warning("Shutdown requested; stopping job loop before starting next job")
                    break

                run_job(job_name, job_func)

    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)

    logger.info("All jobs completed")
    sys.exit(0)


if __name__ == "__main__":
    main()
