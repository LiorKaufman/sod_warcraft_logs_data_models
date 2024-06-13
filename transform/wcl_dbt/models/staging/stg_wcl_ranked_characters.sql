{{ config(
    materialized = 'table'
) }}

WITH source AS (

    SELECT
        file_name,
        report_code,
        unnest(
            report_json.rankedCharacters
        ) AS ranked_character,
        report_start_time,
        uploaded_at,
        created_date
    FROM
        {{ ref("stg_wcl_report") }}
),
renamed AS (
    SELECT
        file_name,
        report_code,
        ranked_character as ranked_character_json,
        hash(ranked_character) as hashed_ranked_character_json,
        ranked_character.id as character_id,
        ranked_character.canonicalID as character_cannonical_id,
        ranked_character.name as character_name,
        ranked_character.classID as class_id,
        ranked_character.level as character_level,
        created_date,
        uploaded_at
    FROM
        source
)
SELECT
    *
FROM
    renamed
