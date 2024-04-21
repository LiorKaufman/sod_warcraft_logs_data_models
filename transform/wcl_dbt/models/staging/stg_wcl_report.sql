{{ config(
    materialized = 'table'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref("stg_wcl_raw_file") }}
    WHERE
        json_request_errors IS NULL
),
renamed AS (
    SELECT
        file_name,
        json_payload.reportData.report AS report_json,
        HASH(
            json_payload.reportData.report
        ) hashed_report_json,
        json_payload.reportData.report.code AS report_code,
        -- Conversion from milliseconds to timestamp, handling BigInt
        timezone(
            'America/Los Angeles',
            {{ convert_millis_to_timestamp(
                'json_payload.reportData.report.startTime'
            ) }}
        ) AS report_start_time,
        timezone(
            'America/Los Angeles',
            {{ convert_millis_to_timestamp(
                'json_payload.reportData.report.endTime'
            ) }}
        ) AS report_end_time,
        created_date,
        uploaded_at
    FROM
        source
)
SELECT
    *
FROM
    renamed
