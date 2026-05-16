with source as (
    select * from {{ source('raw', 'edi_837') }}
),

renamed as (
    select
        claim_id,
        patient_last,
        patient_first,
        patient_dob::date           as date_of_birth,
        patient_gender              as gender,
        subscriber_id,
        subscriber_last,
        subscriber_first,
        diagnosis_codes,
        procedure_code,
        modifier,
        charge_amount::float        as charge_amount,
        units::float                as units,
        place_of_service,
        service_date::date          as service_date,
        billing_provider_npi,
        billing_provider_name,
        rendering_provider_npi,
        rendering_provider_name,
        payer_name,
        payer_id,
        claim_filing_indicator,
        charge_total::float         as charge_total,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed