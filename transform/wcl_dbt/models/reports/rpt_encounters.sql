with
    dim_chars as (select * from "dev"."main"."dim_characters"),
    fct_encounters as (select * from "dev"."main"."fact_encounters"),
    histogram_chars as (
        select fight_name, is_kill, unnest(list_chars) as character_name,
        from "dev".main.fact_encounters

    ),
    agg_chars_kills as (
        select fight_name, histogram(character_name) as hist
        from histogram_chars
        where is_kill = true
        group by all
    ),
    agg_chars_wipes as (
        select fight_name, histogram(character_name) as hist
        from histogram_chars
        where is_kill = false
        group by all
    ),
    final as (
        select
            fct.fight_name,
            sum(case when is_kill = true then 1 else 0 end) as kills,
            sum(case when is_kill = false then 1 else 0 end) as wipes,
            max(
                case when is_kill = true then fight_duration else 0 end
            ) as longest_kill_time,
            min(case when is_kill = true then fight_duration end) as fastest_kill_time,
            avg(case when is_kill = true then fight_duration end) as avg_kill_time,
            median(
                case when is_kill = true then fight_duration end
            ) as median_kill_time,
            ack.hist as kills_per_char,
            acw.hist as wipes_per_char

        from fact_encounters as fct
        left join agg_chars_kills as ack using (fight_name)
        left join agg_chars_wipes as acw using (fight_name)
        group by all

    )

select  -- Step 3: Sort by the swapped tuples
    -- list_transform(kills_per_char, (m) -> m) as sorted_list,
    *
from final
