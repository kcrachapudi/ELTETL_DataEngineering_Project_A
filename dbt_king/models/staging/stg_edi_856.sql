with source as (
    select * from {{ source('raw', 'edi_856') }}
),

renamed as (
    select
        shipment_id,
        ship_date::date             as ship_date,
        po_number,
        carrier_code,
        tracking_number,
        hl_number,
        hl_level,
        item_id,
        quantity_shipped::float     as quantity_shipped,
        unit_of_measure,
        sender_id,
        receiver_id,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed