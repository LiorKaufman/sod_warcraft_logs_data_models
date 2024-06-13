WITH dim_chars AS (
    SELECT
        *
    FROM
        {{ ref('dim_characters') }}
),
fct_encounters AS (
    SELECT
        *
    FROM
        {{ ref("fact_encounters") }}
),
histogram_chars AS (
    SELECT
        fight_name,
        is_kill,
        unnest(list_chars) AS character_name,
    FROM
        {{ ref("fact_encounters") }}
),
agg_chars_kills AS (
    SELECT
        fight_name,
        histogram(character_name) AS hist
    FROM
        histogram_chars
    WHERE
        is_kill = TRUE
    GROUP BY
        ALL
),
agg_chars_wipes AS (
    SELECT
        fight_name,
        histogram(character_name) AS hist
    FROM
        histogram_chars
    WHERE
        is_kill = FALSE
    GROUP BY
        ALL
),
FINAL AS (
    SELECT
        fct.fight_name,
        SUM(
            CASE
                WHEN is_kill = TRUE THEN 1
                ELSE 0
            END
        ) AS kills,
        SUM(
            CASE
                WHEN is_kill = FALSE THEN 1
                ELSE 0
            END
        ) AS wipes,
        MAX(
            CASE
                WHEN is_kill = TRUE THEN fight_duration
                ELSE 0
            END
        ) AS longest_kill_time,
        MIN(
            CASE
                WHEN is_kill = TRUE THEN fight_duration
            END
        ) AS fastest_kill_time,
        AVG(
            CASE
                WHEN is_kill = TRUE THEN fight_duration
            END
        ) AS avg_kill_time,
        MEDIAN(
            CASE
                WHEN is_kill = TRUE THEN fight_duration
            END
        ) AS median_kill_time,
        ack.hist AS kills_per_char,
        acw.hist AS wipes_per_char
    FROM
        fact_encounters AS fct
        LEFT JOIN agg_chars_kills AS ack USING (fight_name)
        LEFT JOIN agg_chars_wipes AS acw USING (fight_name)
    GROUP BY
        ALL
)
SELECT
    *
FROM
    FINAL
