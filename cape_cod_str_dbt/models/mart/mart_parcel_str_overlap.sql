{{ config(materialized='table') }}

with matches as (
    select
        certificate_id,
        loc_id,
        match_type,
        match_score
    from {{ source('staging', 'stg_str_parcel_match') }}
),

parcels as (
    select
        loc_id,
        town,
        use_desc,
        total_val,
        lot_size,
        year_built,
        own_city,
        own_state
    from {{ source('staging', 'stg_massgis_parcels') }}
),

str_ranked as (
    select
        certificate_id,
        street_name,
        town,
        zip_code,
        snapshot_date,
        row_number() over (
            partition by certificate_id
            order by snapshot_date desc
        ) as rn
    from {{ source('staging', 'stg_str_registry') }}
),

str as (
    select certificate_id, street_name, town, zip_code, snapshot_date
    from str_ranked
    where rn = 1
),

final as (
    select
        m.certificate_id,
        m.loc_id,
        m.match_type,
        m.match_score,
        s.street_name,
        s.snapshot_date,
        p.town,
        p.use_desc,
        p.total_val,
        p.lot_size,
        p.year_built,
        p.own_city,
        p.own_state
    from matches m
    inner join parcels p on m.loc_id = p.loc_id
    inner join str s on m.certificate_id = s.certificate_id
)

select * from final