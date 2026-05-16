with source as (
    select * from {{ source('raw', 'edi_850') }}
),

renamed as (
    select
        po_number,
        po_date::date               as po_date,
        buyer_name,
        seller_name,
        ship_to_name,
        line_number,
        product_id_1                as product_id,
        description,
        quantity::float             as quantity,
        unit_of_measure,
        unit_price::float           as unit_price,
        line_total::float           as line_total,
        currency,
        sender_id,
        receiver_id,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed