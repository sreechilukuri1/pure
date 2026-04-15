CREATE TABLE `suny-prj-grants-ai-dev.grantai.pure_researcher_concepts_raw` (
  pure_id STRING NOT NULL,
  researcher_name STRING,
  concept_name STRING NOT NULL,

  rank FLOAT64,
  weighted_rank FLOAT64,

  ingested_at TIMESTAMP
)
PARTITION BY DATE(ingested_at)
CLUSTER BY pure_id, concept_name;

CREATE TABLE `suny-prj-grants-ai-dev.grantai.pure_researcher_concept_embeddings` (
  pure_id STRING NOT NULL,
  researcher_name STRING,
  concept_name STRING NOT NULL,

  rank FLOAT64,
  weighted_rank FLOAT64,

  embedding ARRAY<FLOAT64>,

  embedded_at TIMESTAMP
)
CLUSTER BY concept_name, pure_id;

CREATE OR REPLACE VIEW `suny-prj-grants-ai-dev.grantai.pure_researcher_concepts_deduped` AS
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY pure_id, concept_name
           ORDER BY ingested_at DESC
         ) as rn
  FROM `suny-prj-grants-ai-dev.grantai.pure_researcher_concepts_raw`
)
WHERE rn = 1;

CREATE VECTOR INDEX idx_embeddings
ON `suny-prj-grants-ai-dev.grantai.pure_researcher_concept_embeddings`(embedding)
OPTIONS(index_type = 'IVF',distance_type = 'COSINE');


SELECT
  concept_name,
  researcher_name
FROM `suny-prj-grants-ai-dev.grantai.pure_researcher_concept_embeddings`
ORDER BY ML.DISTANCE(embedding, @query_embedding)
LIMIT 10;