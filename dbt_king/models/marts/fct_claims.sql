with claims as (
    select * from {{ ref('stg_edi_837') }}
),

remittance as (
    select * from {{ ref('stg_edi_835') }}
),

final as (
    select
        c.claim_id,
        c.patient_last,
        c.patient_first,
        c.date_of_birth,
        c.gender,
        c.subscriber_id,
        c.diagnosis_codes,
        c.procedure_code,
        c.modifier,
        c.service_date,
        c.place_of_service,
        c.charge_amount,
        c.units,
        c.billing_provider_npi,
        c.billing_provider_name,
        c.rendering_provider_npi,
        c.payer_name,
        c.payer_id,
        c.claim_filing_indicator,
        r.claim_status,
        r.claim_payment,
        r.patient_responsibility,
        r.allowed_amount,
        r.adjustment_group,
        r.adjustment_reason,
        r.adjustment_amount,
        r.check_number,
        r.check_date,
        c.charge_amount - coalesce(r.claim_payment, 0) as outstanding_amount,
        case
            when r.claim_status = 'processed_primary' then 'paid'
            when r.claim_status = 'denied'            then 'denied'
            when r.claim_payment is null              then 'pending'
            else r.claim_status
        end                                            as claim_outcome,
        c.ingested_at
    from claims c
    left join remittance r
        on c.claim_id = r.claim_id
        and c.procedure_code = r.procedure_code
)

select * from final