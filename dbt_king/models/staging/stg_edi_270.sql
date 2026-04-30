with source as (
    select * from {{ source('raw', 'edi_270') }}
),

renamed as (
    select
        subscriber_id,
        subscriber_last,
        subscriber_first,
        subscriber_dob::date        as date_of_birth,
        subscriber_gender           as gender,
        service_type_code,
        service_type,
        provider_npi,
        provider_name,
        payer_name,
        payer_id,
        trace_number,
        inquiry_date::date          as inquiry_date,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed