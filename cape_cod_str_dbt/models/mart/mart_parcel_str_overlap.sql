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
        prop_id,
        site_addr,
        full_str,
        city,
        zip,
        town,
        use_code,
        use_desc,
        total_val,
        bldg_val,
        land_val,
        lot_size,
        lot_units,
        year_built,
        bld_area,
        res_area,
        units,
        style,
        stories,
        num_rooms,
        owner1,
        own_addr,
        own_city,
        own_state,
        own_zip,
        zoning
    from {{ source('staging', 'stg_massgis_parcels') }}
),

str as (
    select
        certificate_id,
        street_name,
        town,
        zip_code,
        snapshot_date
    from {{ source('staging', 'stg_str_registry') }}
),

final as (
    select
        m.certificate_id,
        m.loc_id,
        m.match_type,
        m.match_score,
        s.street_name,
        s.snapshot_date,
        p.prop_id,
        p.site_addr,
        p.full_str,
        p.city,
        p.zip,
        p.town,
        p.use_code,
        p.use_desc,
        p.total_val,
        p.bldg_val,
        p.land_val,
        p.lot_size,
        p.lot_units,
        p.year_built,
        p.bld_area,
        p.res_area,
        p.units,
        p.style,
        p.stories,
        p.num_rooms,
        p.owner1,
        p.own_addr,
        p.own_city,
        p.own_state,
        p.own_zip,
        p.zoning
    from matches m
    inner join parcels p on m.loc_id = p.loc_id
    inner join str s on m.certificate_id = s.certificate_id
)

select * from final