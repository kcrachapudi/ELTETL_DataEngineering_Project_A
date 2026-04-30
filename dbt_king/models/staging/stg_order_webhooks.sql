with source as (
    select * from {{ source('raw', 'order_webhooks') }}
),

renamed as (
    select
        event_type,
        event_id,
        occurred_at::timestamp      as occurred_at,
        partner_id,
        data_order_id               as order_id,
        data_order_date::date       as order_date,
        data_status                 as status,
        data_currency               as currency,
        data_order_total::float     as order_total,
        data_notes                  as notes,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed