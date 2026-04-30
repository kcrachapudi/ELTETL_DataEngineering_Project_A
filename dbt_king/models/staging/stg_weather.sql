with source as (
    select * from {{ source('raw', 'weather_json') }}
),

renamed as (
    select
        time::timestamp           as observation_ts,
        latitude::float           as latitude,
        longitude::float          as longitude,
        temperature_2m::float     as temperature_f,
        relativehumidity_2m::float as humidity_pct,
        precipitation::float      as precipitation_in,
        windspeed_10m::float      as windspeed_mph,
        weathercode::int          as weather_code,
        _source_file              as source_file,
        _ingested_at::timestamp   as ingested_at
    from source
)

select * from renamed