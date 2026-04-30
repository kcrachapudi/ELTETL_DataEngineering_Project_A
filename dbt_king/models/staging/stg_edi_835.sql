with source as (
    select * from {{ source('raw', 'edi_835') }}
),

renamed as (
    select
        claim_id,
        patient_last,
        patient_first,
        patient_dob::date           as date_of_birth,
        claim_status_code,
        claim_status,
        claim_charge::float         as claim_charge,
        claim_payment::float        as claim_payment,
        patient_responsibility::float as patient_responsibility,
        procedure_code,
        modifier,
        line_charge::float          as line_charge,
        line_payment::float         as line_payment,
        allowed_amount::float       as allowed_amount,
        adjustment_group,
        adjustment_reason,
        adjustment_amount::float    as adjustment_amount,
        service_date::date          as service_date,
        check_number,
        check_date::date            as check_date,
        check_amount::float         as check_amount,
        payer_name,
        payer_id,
        payee_name,
        payee_npi,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed