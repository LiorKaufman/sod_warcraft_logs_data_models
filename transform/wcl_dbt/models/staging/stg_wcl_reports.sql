SELECT
    id,
    json_col -> 'data' -> 'reportData' -> 'report' -> 'code' AS report_id,
    file_created_at,
    uploaded_at
FROM
    {{ source(
        'wcl',
        'wcl_reports'
    ) }}
WHERE
    id = 1
