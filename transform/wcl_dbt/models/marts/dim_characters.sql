WITH chars AS (
    SELECT
        character_id,
        character_name,
        class_id,
        MAX(uploaded_at) AS uploaded_at,
        MAX(created_date) AS created_date
    FROM
        {{ ref('stg_wcl_ranked_characters') }}
    GROUP BY
        ALL
),
class_names AS (
    SELECT
        server_name,
        class_name,
        character_name
    FROM
        {{ ref('stg_wcl_actors') }}
    GROUP BY
        ALL
),
FINAL AS (
    SELECT
        chars.character_id,
        chars.character_name,
        chars.class_id,
        class_names.class_name,
        class_names.server_name,
        chars.uploaded_at,
        chars.created_date
    FROM
        chars
        INNER JOIN class_names
        ON chars.character_name = class_names.character_name
    where class_names.class_name != 'Unknown'
)
SELECT
    *
FROM
    FINAL
