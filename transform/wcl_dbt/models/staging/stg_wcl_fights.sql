{{ config(
    materialized = 'table'
) }}

WITH source AS (

    SELECT
        file_name,
        report_code,
        unnest(
            report_json.fights
        ) AS fight,
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
        fight as fight_json,
        hash(fight) as hashed_fight_json,
        fight.id AS fight_id,
        fight.name AS fight_name,
        fight.encounterID AS encounter_id,
        fight.kill AS is_kill,
        {{ convert_millis_to_timestamp(
            'fight.startTime',
            'report_start_time'
        ) }} AS fight_start_time,
        {{ convert_millis_to_timestamp(
            'fight.endTime',
            'report_start_time'
        ) }} AS fight_end_time,
        fight.size,
        fight.originalEncounterID AS original_encounter_id,
        created_date,
        uploaded_at
    FROM
        source
)
SELECT
    *
FROM
    renamed
