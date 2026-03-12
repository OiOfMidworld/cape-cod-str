{{ config(materialized='table') }}

with str_counts as (
    select
        town,
        count(distinct certificate_id) as str_count,
        snapshot_date
    from {{ source('staging', 'stg_str_registry') }}
    group by town, snapshot_date
),

census as (
    select
        town,
        survey_year,
        total_housing_units,
        occupied_units,
        vacant_units
    from {{ source('staging', 'stg_census_acs') }}
),

latest_census as (
    select *
    from census
    where survey_year = (select max(survey_year) from census)
),

final as (
    select
        s.town,
        s.snapshot_date,
        s.str_count,
        c.total_housing_units,
        c.occupied_units,
        c.vacant_units,
        round(
            s.str_count::numeric / nullif(c.total_housing_units, 0) * 100,
            2
        ) as str_pct_of_total
    from str_counts s
    left join latest_census c on s.town = c.town
)

select * from final
order by str_pct_of_total desc nulls last