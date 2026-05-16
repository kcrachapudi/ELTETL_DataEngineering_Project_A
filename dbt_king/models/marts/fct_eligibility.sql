with eligibility as (
    select * from {{ ref('stg_edi_271') }}
),

inquiry as (
    select * from {{ ref('stg_edi_270') }}
),

final as (
    select
        e.subscriber_id,
        e.subscriber_last,
        e.subscriber_first,
        e.date_of_birth,
        e.gender,
        e.plan_id,
        e.plan_name,
        e.group_number,
        e.payer_name,
        e.payer_id,
        e.eligibility_code,
        e.eligibility_description,
        e.service_type,
        e.coverage_level,
        e.in_network,
        e.benefit_amount,
        e.benefit_percent,
        e.benefit_period,
        e.coverage_active,
        e.messages,
        i.inquiry_date,
        i.provider_npi         as inquiring_provider_npi,
        i.provider_name        as inquiring_provider_name,
        e.ingested_at
    from eligibility e
    left join inquiry i
        on e.subscriber_id = i.subscriber_id
)

select * from final