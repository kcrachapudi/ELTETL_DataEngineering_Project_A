with source as (
    select * from {{ source('raw', 'partner_orders') }}
),

renamed as (
    select
        order_id,
        order_date::date            as order_date,
        partner_id,
        product_id,
        product_description,
        quantity::float             as quantity,
        unit_of_measure,
        unit_price::float           as unit_price,
        line_total::float           as line_total,
        currency,
        ship_to_name,
        ship_to_city,
        ship_to_state,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed