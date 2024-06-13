{{ config(
    materialized = 'table'
) }}


with fights as (

    select distinct hash(fight_name) as sk_fight_id, fight_name as encounter_name
    from {{ref("stg_wcl_fights")}}
)

select * from fights