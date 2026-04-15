import asyncio
import aiohttp
import logging
import pandas as pd
import sys
from typing import List, Dict, Any
from datetime import datetime

from common.config import (
    API_KEY, 
    MAX_CONCURRENT_REQUESTS, 
    API_BASE_URL, 
    FINGERPRINT_BATCH_SIZE, 
    CONCEPT_BATCH_SIZE
)
from common.gcs_client import upload_jsonl
from common.bq_client import load_jsonl_from_gcs, truncate_table
from utils.retry import default_retry

logger = logging.getLogger(__name__)

HEADERS = {
    "api-key": str(API_KEY),
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# =========================
# HELPER: FLATTEN LOGIC (Step 2)
# =========================
def process_items_to_rows(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extracts all required columns including rank and weightedRank."""
    rows = []
    now = datetime.utcnow().isoformat()  # ✅ FIX

    for item in items:
        p_uuid = item.get("relatedContent", {}).get("uuid")
        f_uuid = item.get("uuid")
        concepts = item.get("concepts", []) or []

        for c in concepts:
            concept_obj = c.get("concept", {}) or {}
            rows.append({
                "fingerprint_uuid": f_uuid,
                "person_uuid": p_uuid,
                "conceptId": concept_obj.get("conceptId"),
                "concept_uuid": concept_obj.get("uuid"),
                "rank": c.get("rank"),
                "weightedRank": c.get("weightedRank"),
                "frequency": c.get("frequency"),
                "created_at": now,
                "updated_at": now
            })
    return rows


# =========================
# STEP 3: FETCH CONCEPTS (Parallel)
# =========================
async def fetch_concepts_parallel(session: aiohttp.ClientSession, concept_uuids: List[str]):
    unique_uuids = sorted(list(set(u for u in concept_uuids if u)))
    logger.info(f"🚀 Step 3: Fetching {len(unique_uuids)} unique concepts in parallel...")
    
    tasks = []
    for i in range(0, len(unique_uuids), CONCEPT_BATCH_SIZE):
        chunk = unique_uuids[i:i + CONCEPT_BATCH_SIZE]
        payload = {"uuids": chunk, "size": len(chunk), "offset": 0}
        tasks.append(fetch_json_post(session, "concepts/search", payload))
    
    results = await asyncio.gather(*tasks)
    
    concept_rows = []
    now = datetime.utcnow().isoformat()  # ✅ FIX

    for data in results:
        for item in data.get("items", []):
            name_obj = item.get("name", {}) or {}
            concept_rows.append({
                "concept_uuid": item.get("uuid"),
                "concept_name": name_obj.get("en_GB") or name_obj.get("en_US") or next(iter(name_obj.values()), None),
                "conceptId_from_concepts": item.get("conceptId"),
                "created_at": now,
                "updated_at": now
            })
    
    if concept_rows:
        uri = upload_jsonl(concept_rows, prefix="raw/concepts")
        load_jsonl_from_gcs(uri, "concepts")
        logger.info(f"✅ Step 3 Complete: {len(concept_rows)} concepts loaded to BQ.")


@default_retry()
async def fetch_json_post(session: aiohttp.ClientSession, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{API_BASE_URL}/{endpoint}"
    async with session.post(url, json=payload, timeout=120) as resp:
        resp.raise_for_status()
        return await resp.json()


# =========================
# MAIN PIPELINE
# =========================
async def run():
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        
        # 1. Get Total Count
        first_payload = {"size": FINGERPRINT_BATCH_SIZE, "offset": 0, "relatedContentSystemName": "person"}
        first_data = await fetch_json_post(session, "fingerprints/search", first_payload)
        total_count = first_data.get("count", 0)
        logger.info(f"🚀 Total fingerprints to process: {total_count}")
        
        # 2. Prepare Tasks
        tasks = []
        for offset in range(0, total_count, FINGERPRINT_BATCH_SIZE):
            payload = {"size": FINGERPRINT_BATCH_SIZE, "offset": offset, "relatedContentSystemName": "person"}
            tasks.append(fetch_json_post(session, "fingerprints/search", payload))

        all_concept_uuids = set()
        current_batch_rows = []
        processed_count = 0
        batch_limit = 50000

        truncate_table("fingerprint")
        truncate_table("concepts")

        # 3. Process and Load in Waves
        for coro in asyncio.as_completed(tasks):
            page = await coro
            items = page.get("items", [])
            
            # Flatten this specific page
            page_rows = process_items_to_rows(items)
            current_batch_rows.extend(page_rows)
            
            # Track unique concepts for Step 3 later
            for r in page_rows:
                if r['concept_uuid']:
                    all_concept_uuids.add(r['concept_uuid'])

            processed_count += FINGERPRINT_BATCH_SIZE
            
            # Trigger a Load Wave
            if len(current_batch_rows) >= 1000000:
                logger.info(f"📤 Wave Load: Uploading {len(current_batch_rows)} rows to BQ... (Progress: {processed_count}/{total_count})")
                uri = upload_jsonl(current_batch_rows, prefix=f"raw/fingerprints/batch_{processed_count}")
                load_jsonl_from_gcs(uri, "fingerprint")
                
                # Clear memory for next wave
                current_batch_rows = []

        # 4. Final Cleanup Load
        if current_batch_rows:
            logger.info(f"📤 Final Wave: Uploading remaining {len(current_batch_rows)} rows...")
            uri = upload_jsonl(current_batch_rows, prefix="raw/fingerprints/final_batch")
            load_jsonl_from_gcs(uri, "fingerprint")

        # 5. Step 3: Concepts
        if all_concept_uuids:
            await fetch_concepts_parallel(session, list(all_concept_uuids))

    logger.info("🏁 Pipeline complete.")
