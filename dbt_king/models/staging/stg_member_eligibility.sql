with source as (
    select * from {{ source('raw', 'member_eligibility') }}
),

renamed as (
    select
        member_id,
        subscriber_id,
        last_name,
        first_name,
        dob::date                   as date_of_birth,
        gender,
        plan_id,
        group_number,
        employer_name,
        effective_date::date        as effective_date,
        termination_date::date      as termination_date,
        coverage_type,
        pcp_npi,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed