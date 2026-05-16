with source as (
    select * from {{ source('raw', 'weather_stations') }}
),

renamed as (
    select
        station_id,
        station_name,
        city,
        state,
        latitude::float             as latitude,
        longitude::float            as longitude,
        elevation_ft::float         as elevation_ft,
        active::boolean             as active,
        installed_date::date        as installed_date,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed