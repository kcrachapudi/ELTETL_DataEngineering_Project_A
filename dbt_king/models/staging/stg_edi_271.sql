with source as (
    select * from {{ source('raw', 'edi_271') }}
),

renamed as (
    select
        subscriber_id,
        subscriber_last,
        subscriber_first,
        subscriber_dob::date        as date_of_birth,
        subscriber_gender           as gender,
        plan_id,
        plan_name,
        group_number,
        eligibility_code,
        eligibility_description,
        service_type_code,
        service_type,
        coverage_level,
        in_network,
        benefit_amount::float       as benefit_amount,
        benefit_percent::float      as benefit_percent,
        benefit_period,
        coverage_active::boolean    as coverage_active,
        messages,
        payer_name,
        payer_id,
        provider_npi,
        provider_name,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed