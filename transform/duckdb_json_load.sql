CREATE
OR REPLACE TABLE raw.wcl_report AS (
    SELECT
        *,
        get_current_timestamp() AS uploaded_at
    FROM
        read_json_auto(
            '/Users/liorkaufman/Desktop/personal_project/sod_warcraft_logs_data_models/transform/data/*.json',
            filename = TRUE
        )
)
