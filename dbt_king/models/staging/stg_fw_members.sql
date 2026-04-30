with source as (
    select * from {{ source('raw', 'fw_members') }}
),

renamed as (
    select
        member_id,
        sub_id                      as subscriber_id,
        last_name,
        first_name,
        dob::date                   as date_of_birth,
        gender,
        plan_id,
        group_id,
        cov_type                    as coverage_type,
        eff_date::date              as effective_date,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed