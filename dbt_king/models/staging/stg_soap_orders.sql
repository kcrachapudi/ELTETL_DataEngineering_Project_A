with source as (
    select * from {{ source('raw', 'soap_orders') }}
),

renamed as (
    select
        order_orderid               as order_id,
        order_partnerid             as partner_id,
        order_status                as status,
        order_statusdate::date      as status_date,
        order_trackingnumber        as tracking_number,
        order_carrier               as carrier,
        order_estimateddelivery::date as estimated_delivery,
        order_totalvalue::float     as total_value,
        order_currency              as currency,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed