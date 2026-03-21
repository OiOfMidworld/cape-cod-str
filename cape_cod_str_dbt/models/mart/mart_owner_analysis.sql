{{ config(materialized='table') }}

with best_match as (
    select distinct on (certificate_id)
        certificate_id,
        loc_id,
        town,
        match_type,
        match_score,
        own_state,
        own_city,
        total_val,
        lot_size,
        use_desc,
        year_built
    from {{ ref('mart_parcel_str_overlap') }}
    order by certificate_id, match_score desc nulls last
),

classified as (
    select
        *,
        case
            when own_state = 'MA' then 'In-State'
            when own_state is null or own_state = '' then 'Unknown'
            else 'Out-of-State'
        end as owner_residency
    from best_match
),

town_summary as (
    select
        town,
        count(*)                                                                            as str_count,
        count(case when owner_residency = 'In-State' then 1 end)                           as instate_count,
        count(case when owner_residency = 'Out-of-State' then 1 end)                       as outofstate_count,
        count(case when owner_residency = 'Unknown' then 1 end)                            as unknown_count,
        round(avg(total_val))                                                               as avg_assessed_val,
        round(avg(lot_size)::numeric, 2)                                                   as avg_lot_size,
        round(
            count(case when owner_residency = 'Out-of-State' then 1 end)::numeric
            / nullif(count(*), 0) * 100, 1
        )                                                                                   as outofstate_pct
    from classified
    group by town
)

select * from town_summary
order by outofstate_pct desc nulls last