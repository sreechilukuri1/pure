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
from common.bq_client import load_jsonl_from_gcs, truncate_table
from common.telemetry import start_span
from utils.retry import default_retry

logger = logging.getLogger(__name__)

HEADERS = {
    "api-key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

CONCEPT_WORKERS = 5
BQ_BATCH_SIZE = 5000


@default_retry()
async def fetch_json(session, method, url, **kwargs):
    async with session.request(method, url, **kwargs) as resp:
        resp.raise_for_status()
        return await resp.json()

def safe_get(d, key, default=None):
    try:
        return d.get(key, default) if isinstance(d, dict) else default
    except Exception:
        return default


def safe_get_nested(d, keys, default=None):
    try:
        for key in keys:
            if isinstance(d, dict):
                d = d.get(key)
            else:
                return default
        return d if d is not None else default
    except Exception:
        return default


def safe_first(lst):
    try:
        return lst[0] if isinstance(lst, list) and len(lst) > 0 else None
    except Exception:
        return None


def extract_localized_value(obj):
    try:
        if isinstance(obj, dict) and obj:
            return next(iter(obj.values()))
    except Exception:
        pass
    return None


# =========================
# FIELD EXTRACTORS
# =========================
def extract_location(private_address):
    try:
        if not isinstance(private_address, dict):
            return None

        country_term = safe_get_nested(private_address, ["country", "term"])
        country = extract_localized_value(country_term)

        parts = [
            safe_get(private_address, "building"),
            safe_get(private_address, "road"),
            safe_get(private_address, "room"),
            safe_get(private_address, "postalCode"),
            safe_get(private_address, "city"),
            country
        ]

        return ", ".join([str(p) for p in parts if p])

    except Exception:
        return None


def extract_profile_photo(person):
    try:
        photos = safe_get(person, "profilePhotos", [])
        photo = safe_first(photos)
        return safe_get(photo, "url")
    except Exception:
        return None


def extract_titles(person):
    try:
        titles = safe_get(person, "titles", [])
        values = []

        for t in titles:
            val = extract_localized_value(safe_get(t, "value", {}))
            if val:
                values.append(val)

        return ", ".join(values) if values else None
    except Exception:
        return None


def extract_suny_global_id(person):
    try:
        identifiers = safe_get(person, "identifiers", [])

        for identifier in identifiers:
            term = safe_get_nested(identifier, ["type", "term", "en_US"])
            if term == "SUNY Global ID":
                return safe_get(identifier, "id")

        return None
    except Exception:
        return None


def extract_academic_field(person, field_name):
    try:
        aq = safe_get(person, "academicQualifications", [])
        first = safe_first(aq)

        field = safe_get(first, field_name, {})
        return extract_localized_value(field)

    except Exception:
        return None


def extract_affiliation(associations):
    try:
        first = safe_first(associations)
        return safe_get(first, "affiliationId")
    except Exception:
        return None


def extract_email(associations):
    try:
        first = safe_first(associations)
        emails = safe_get(first, "emails", [])
        email_obj = safe_first(emails)
        return safe_get(email_obj, "value")
    except Exception:
        return None


# =========================
# TRANSFORMATION
# =========================
def transform_person(person):
    try:
        first_name = safe_get_nested(person, ["name", "firstName"], "")
        last_name = safe_get_nested(person, ["name", "lastName"], "")

        return {
            "uuid": safe_get(person, "uuid"),

            "name": f"{first_name} {last_name}".strip() if (first_name or last_name) else None,

            "location": extract_location(safe_get(person, "privateAddress")),

            "profile_picture_url": extract_profile_photo(person),

            "SUNY_global_id": extract_suny_global_id(person),

            "titles": extract_titles(person),

            "orcid_id": safe_get(person, "orcid"),

            "orcidAuthenticated": safe_get(person, "orcidAuthenticated"),

            # Kept as-is, but likely not a valid field unless confirmed
            "researcher_inter": safe_get(person, "researcher_inter"),

            "fieldOfStudyUnstructured": extract_academic_field(person, "fieldOfStudyUnstructured"),

            "projectTitle": extract_academic_field(person, "projectTitle"),

            "qualificationUnstructured": extract_academic_field(person, "qualificationUnstructured"),

            # Affiliations
            "honorary_affiliationId": extract_affiliation(
                safe_get(person, "honoraryStaffOrganizationAssociations", [])
            ),
            "staff_affiliationId": extract_affiliation(
                safe_get(person, "staffOrganizationAssociations", [])
            ),
            "student_affiliationId": extract_affiliation(
                safe_get(person, "studentOrganizationAssociations", [])
            ),
            "visiting_affiliationId": extract_affiliation(
                safe_get(person, "visitingScholarOrganizationAssociations", [])
            ),

            # Emails
            "honorary_email": extract_email(
                safe_get(person, "honoraryStaffOrganizationAssociations", [])
            ),
            "staff_email": extract_email(
                safe_get(person, "staffOrganizationAssociations", [])
            ),
            "student_email": extract_email(
                safe_get(person, "studentOrganizationAssociations", [])
            ),
            "visiting_email": extract_email(
                safe_get(person, "visitingScholarOrganizationAssociations", [])
            ),
        }

    except Exception as e:
        print(f"⚠️ Error transforming person: {e}")
        return None

# -----------------------
# PERSON PRODUCER
# -----------------------
async def produce_persons(session,row_queue):
    truncate_table(RAW_TABLE)
    offset = 0
    rows = []
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

        for person in items:
            pid = person["uuid"]

            name_obj = person.get("name", {})
            name = f"{name_obj.get('firstName','')} {name_obj.get('lastName','')}"
            rows.append({
                "researcher_id": pid,
                "name": name,
                "location": extract_location(safe_get(person, "privateAddress")),
                "affiliation": extract_affiliation(safe_get(person, "staffOrganizationAssociations", [])),
                "email": extract_email(safe_get(person, "staffOrganizationAssociations", [])),
                "researcher_interests": extract_academic_field(person, "researcherInter"),
                "orcid_id": extract_academic_field(person, "orcid"),
                "orcidAuthenticated": extract_academic_field(person, "orcidAuthenticated"),
                "profile_picture_url": extract_profile_photo(person),
                "researcher_title": extract_titles(person),
                "qualifications": extract_academic_field(person, "qualificationUnstructured"),
                "suny_global_id": extract_suny_global_id(person),
                "project_title": extract_academic_field(person, "projectTitle"),
                "created_at": datetime.now(),
                "updated_at": datetime.now()

                })
            

        offset += PAGE_SIZE
        logger.info(f"Queued persons up to offset {offset}")
    if rows:
        uri = upload_jsonl(rows, prefix="raw")
        load_jsonl_from_gcs(uri, RAW_TABLE)

    return rows 


# -----------------------
# MAIN
# -----------------------
async def run():
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        # concept_queue = asyncio.Queue(maxsize=50000)
        row_queue = asyncio.Queue(maxsize=50000)

        with start_span("pipeline"):

            rows = await produce_persons(session,row_queue)
            logger.info("Total rows fetched and loaded:"+str(len(rows)))

    logger.info("Streaming ingest pipeline complete")


if __name__ == "__main__":
    asyncio.run(run())