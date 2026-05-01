with remittance as (
    select * from {{ ref('stg_edi_835') }}
),

final as (
    select
        claim_id,
        patient_last,
        patient_first,
        payer_name,
        payer_id,
        payee_name,
        payee_npi,
        check_number,
        check_date,
        check_amount,
        claim_status,
        claim_charge,
        claim_payment,
        patient_responsibility,
        claim_charge - claim_payment as write_off_amount,
        adjustment_group,
        adjustment_reason,
        adjustment_amount,
        procedure_code,
        line_charge,
        line_payment,
        allowed_amount,
        service_date,
        ingested_at
    from remittance
)

select * from final