{{ config(
    materialized = 'table'
) }}

WITH source AS (

    SELECT
        file_name,
        report_code,
        unnest(
            report_json.masterData.actors
        ) AS actor,
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
        actor as actor_json,
        hash(actor) as hashed_actor_json,
        actor.id as actor_id,
        actor.gameID as game_id,
        actor.server as server_name,
        actor.subType as class_name,
        actor.name as character_name,
        created_date,
        uploaded_at
    FROM
        source
)
SELECT
    *
FROM
    renamed
