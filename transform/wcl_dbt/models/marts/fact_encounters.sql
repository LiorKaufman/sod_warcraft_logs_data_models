WITH fights AS (
    SELECT
        *
    FROM
        {{ ref('stg_wcl_fights') }}
),
chars AS (
    SELECT
        distinct
        report_code,
        character_id,
        character_name
    FROM
        {{ ref("stg_wcl_ranked_characters") }}
),
classes AS (
    SELECT
        distinct 
        report_code,
        class_name
    FROM
        {{ ref('stg_wcl_actors') }}
    GROUP BY
        ALL
),
FINAL AS (
    SELECT
        fights.fight_id,
        fights.fight_name,
        fights.report_code,
        fights.is_kill,
        MAX(
            CASE
                WHEN fights.is_kill = TRUE THEN DATEDIFF(
                    'seconds',
                    fights.fight_start_time,
                    fight_end_time
                )
            END
        ) AS slowest_kill,
        list(
            DISTINCT chars.character_id
            ORDER BY
                chars.character_name
        ) AS list_characters_ids,
        list(
            DISTINCT chars.character_name
            ORDER BY
                chars.character_name
        ) AS list_characters_names,
        COUNT(
            DISTINCT chars.character_id
        ) AS cnt_characters,
        list(
            classes.class_name
            ORDER BY
                classes.class_name
        ) AS list_class_names,
        COUNT(
            DISTINCT classes.class_name
        ) AS cnt_classes
    FROM
        fights
        LEFT JOIN chars
        ON fights.report_code = chars.report_code
        LEFT JOIN classes
        ON fights.report_code = classes.report_code
    GROUP BY
        fights.report_code, fight_name
    ORDER BY
        wipes DESC
)
SELECT
    *
FROM
    FINAL
