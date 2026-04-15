# =========================
# RESEARCHER EMBEDDING QUERY
# =========================

RESEARCHER_EMBEDDING_QUERY = """
SELECT 
    A.researcher_id,
    A.suny_global_id AS external_id,
    'PURE' AS external_system,
    A.name,
    A.location,
    CAST(NULL AS STRING) AS affiliation,
    A.email,
    A.profile_picture_url,
    STRING_AGG(DISTINCT concept_name , ' ,'ORDER BY concept_name ) AS researcher_interests
FROM suny-prj-grants-ai-dev.grantai.fingerprint B
JOIN `suny-prj-grants-ai-dev.grantai.persons`A    ON B.person_uuid = A.researcher_id
JOIN `suny-prj-grants-ai-dev.grantai.concepts` c     ON C.concept_uuid = B.concept_uuid
WHERE A.suny_global_id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8
"""
