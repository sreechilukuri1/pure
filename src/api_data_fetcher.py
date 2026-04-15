import asyncio
import aiohttp
import logging
from datetime import datetime

from common.config import (
    API_KEY, 
    MAX_CONCURRENT_REQUESTS, 
    RAW_TABLE, 
    API_BASE_URL, 
    CONCEPT_BATCH_SIZE, 
    PAGE_SIZE
)
from common.gcs_client import upload_jsonl
from common.bq_client import load_jsonl_from_gcs
from common.telemetry import start_span
from utils.retry import default_retry


logger = logging.getLogger(__name__)

HEADERS = {
    "api-key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# retry wrapper for API calls
@default_retry()
async def fetch_json(session, method, url, **kwargs):
    async with session.request(method, url, **kwargs) as resp:
        resp.raise_for_status()
        return await resp.json()

# producer: fetch persons with pagination and put in queue
async def produce_persons(session, person_queue):
    offset = 0

    while True:
        data = await fetch_json(
            session,
            "GET",
            f"{API_BASE_URL}/persons",
            params={"size": PAGE_SIZE, "offset": offset},
        )

        items = data.get("items", [])
        if not items:
            break

        for p in items:
            await person_queue.put(p)

        offset += PAGE_SIZE
        logger.info(f"Queued persons up to offset {offset}")

    # signal done
    for _ in range(MAX_CONCURRENT_REQUESTS):
        await person_queue.put(None)

# fingerprint worker: fetch fingerprints for each person and put concepts in queue
async def fingerprint_worker(session, person_queue, concept_queue):
    while True:
        person = await person_queue.get()

        if person is None:
            await concept_queue.put(None)
            break

        try:
            data = await fetch_json(
                session,
                "POST",
                f"{API_BASE_URL}/fingerprints/search",
                json={
                    "size": PAGE_SIZE,
                    "offset": 0,
                    "relatedContentUuid": person["uuid"],
                    "relatedContentSystemName": "Person",
                },
            )

            name_obj = person.get("name", {})
            name = f"{name_obj.get('firstName','')} {name_obj.get('lastName','')}"

            for fp in data.get("items", []):
                for c in fp.get("concepts", []):
                    cid = c.get("concept", {}).get("uuid")
                    if cid:
                        await concept_queue.put({
                            "concept_id": cid.lower(),
                            "person_id": person["uuid"],
                            "person_name": name,
                            "rank": c.get("rank"),
                            "weighted_rank": c.get("weightedRank"),
                        })

        except Exception as e:
            logger.error(f"Fingerprint error: {e}")

# concept worker: fetch concept details for each concept and write to BQ
async def concept_worker(session, concept_queue):
    buffer = []
    active_workers = MAX_CONCURRENT_REQUESTS

    while True:
        item = await concept_queue.get()

        if item is None:
            active_workers -= 1
            if active_workers == 0:
                break
            continue

        buffer.append(item)

        if len(buffer) >= CONCEPT_BATCH_SIZE:
            await process_batch(session, buffer)
            buffer = []

    # flush remaining
    if buffer:
        await process_batch(session, buffer)

# process batch: fetch concept details, transform, and load to BQ
async def process_batch(session, batch):
    try:
        data = await fetch_json(
            session,
            "POST",
            f"{API_BASE_URL}/concepts/search",
            json={"uuids": [x["concept_id"] for x in batch]},
        )

        concept_map = {}
        for c in data.get("items", []):
            cid = c.get("uuid") or c.get("concept", {}).get("uuid")
            name_obj = c.get("name") or c.get("concept", {}).get("name")

            term = None
            if isinstance(name_obj, dict):
                term = next((v for v in name_obj.values() if v), None)

            if cid and term:
                concept_map[cid.lower()] = term

        rows = []
        for item in batch:
            term = concept_map.get(item["concept_id"])
            if term:
                rows.append({
                    "pure_id": item["person_id"],
                    "researcher_name": item["person_name"],
                    "concept_name": term,
                    "rank": item["rank"],
                    "weighted_rank": item["weighted_rank"],
                    "ingested_at": datetime.utcnow().isoformat(),
                })

        if rows:
            uri = upload_jsonl(rows, prefix="raw")
            load_jsonl_from_gcs(uri, RAW_TABLE)

    except Exception as e:
        logger.error(f"Concept batch error: {e}")

# main pipeline
async def run():
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        person_queue = asyncio.Queue(maxsize=5000)
        concept_queue = asyncio.Queue(maxsize=10000)

        with start_span("pipeline"):

            producer = asyncio.create_task(produce_persons(session, person_queue))

            fingerprint_workers = [
                asyncio.create_task(
                    fingerprint_worker(session, person_queue, concept_queue)
                )
                for _ in range(MAX_CONCURRENT_REQUESTS)
            ]

            concept_task = asyncio.create_task(
                concept_worker(session, concept_queue)
            )

            await producer
            await asyncio.gather(*fingerprint_workers)
            await concept_task

    logger.info("Streaming ingest pipeline complete")
