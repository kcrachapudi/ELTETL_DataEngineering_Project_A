with source as (
    select * from {{ source('raw', 'edi_834') }}
),

renamed as (
    select
        member_id,
        subscriber_id,
        last_name,
        first_name,
        dob::date              as date_of_birth,
        gender,
        relationship_code,
        relationship,
        maintenance_type_code,
        maintenance_type,
        plan_id,
        coverage_type,
        case when effective_date::text = 'NaN' then null
            else effective_date::date
        end                         as effective_date,
        case when termination_date::text = 'NaN' then null
            else termination_date::date
        end                         as termination_date,        
        employer_name,
        payer_name,
        payer_id,
        _source_file           as source_file,
        _ingested_at::timestamp as ingested_at
    from source
)

select * from renamed