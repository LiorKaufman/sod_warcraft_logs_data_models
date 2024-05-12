CREATE schema if not exists raw;
CREATE OR REPLACE TABLE raw.wcl_report AS (
    SELECT
        *,
        get_current_timestamp() AS uploaded_at
    FROM
        read_json_auto(
            './seeds/data/*.json',
            filename = TRUE
        )
);
