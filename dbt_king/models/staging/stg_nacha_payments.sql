with source as (
    select * from {{ source('raw', 'nacha_payments') }}
),

renamed as (
    select
        account_number,
        routing_number,
        individual_id,
        individual_name,
        amount::float               as amount,
        transaction_code,
        trace_number,
        addenda_indicator,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed