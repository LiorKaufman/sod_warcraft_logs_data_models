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
    where class_name != 'Unknown'
    GROUP BY
        ALL
),
FINAL AS (
    SELECT
        fights.fight_id,
        fights.fight_name,
        fights.report_code,
        fights.is_kill,
 DATEDIFF(
                    'seconds',
                    fights.fight_start_time,
                    fight_end_time
                )
            
         AS fight_duration,
         list( distinct chars.character_name) as list_chars,
         list(classes.class_name) as list_classes
    FROM
        fights
        LEFT JOIN chars
        ON fights.report_code = chars.report_code
        LEFT JOIN classes
        ON fights.report_code = classes.report_code
    GROUP BY
all
    ORDER BY
        fight_duration DESC
)
SELECT
    *
FROM
    FINAL
