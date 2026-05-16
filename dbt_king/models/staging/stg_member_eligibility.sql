with source as (
    select * from {{ source('raw', 'member_eligibility') }}
),

renamed as (
    select
        member_id,
        subscriber_id,
        last_name,
        first_name,
        case when dob = 'NaN' then null
            else dob::date
        end                         as date_of_birth,
        gender,
        plan_id,
        group_number,
        employer_name,
        effective_date::date        as effective_date,
        case when termination_date = 'NaN' then null
            else termination_date::date
        end                         as termination_date,        
        coverage_type,
        pcp_npi,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed