/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/
{{ config(
    materialized = 'table'
) }}

WITH source_data AS (

    SELECT
        1 AS id
    UNION ALL
    SELECT
        NULL AS id
)
SELECT
    *
FROM
    source_data
    /*
        Uncomment the line below to remove records with null `id` values
    */
    -- where id is not null
