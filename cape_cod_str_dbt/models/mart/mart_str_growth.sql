{{ config(materialized='table') }}

with str_counts as (
    select
        town,
        date_part('year', snapshot_date::date) as snapshot_year,
        count(distinct certificate_id) as str_count
    from {{ source('staging', 'stg_str_registry') }}
    where town is not null
    and trim(town) = town
    group by town, date_part('year', snapshot_date::date)
),

latest_census_year as (
    select max(survey_year) as max_year
    from {{ source('staging', 'stg_census_acs') }}
),

census as (
    select
        town,
        survey_year,
        total_housing_units
    from {{ source('staging', 'stg_census_acs') }}
    where survey_year = (select max_year from latest_census_year)
),

final as (
    select
        s.town,
        s.snapshot_year,
        s.str_count,
        c.total_housing_units,
        c.survey_year as census_year_used,
        round(
            s.str_count::numeric / nullif(c.total_housing_units, 0) * 100,
            2
        ) as str_pct_of_total
    from str_counts s
    left join census c on s.town = c.town
)

select * from final
order by town, snapshot_year