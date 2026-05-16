with edi_members as (
    select
        member_id,
        subscriber_id,
        last_name,
        first_name,
        date_of_birth,
        gender,
        plan_id,
        coverage_type,
        case when effective_date::text = 'NaN' then null::date
             else effective_date::date
        end                     as effective_date,
        case when termination_date::text = 'NaN' then null::date
             else termination_date::date
        end                     as termination_date,
        employer_name,
        payer_name,
        payer_id,
        maintenance_type,
        'edi_834'               as source_system
    from {{ ref('stg_edi_834') }}
),

csv_members as (
    select
        member_id,
        subscriber_id,
        last_name,
        first_name,
        date_of_birth,
        gender,
        plan_id,
        coverage_type,
        case when effective_date::text = 'NaN' then null::date
             else effective_date::date
        end                     as effective_date,
        case when termination_date::text = 'NaN' then null::date
             else termination_date::date
        end                     as termination_date,
        null::text              as employer_name,
        null::text              as payer_name,
        null::text              as payer_id,
        null::text              as maintenance_type,
        'csv_eligibility'       as source_system
    from {{ ref('stg_member_eligibility') }}
),

fw_members as (
    select
        member_id,
        subscriber_id,
        last_name,
        first_name,
        date_of_birth,
        gender,
        plan_id,
        coverage_type,
        case when effective_date::text = 'NaN' then null::date
             else effective_date::date
        end                     as effective_date,
        null::date              as termination_date,
        null::text              as employer_name,
        null::text              as payer_name,
        null::text              as payer_id,
        null::text              as maintenance_type,
        'mainframe'             as source_system
    from {{ ref('stg_fw_members') }}
),

combined as (
    select * from edi_members
    union all
    select * from csv_members
    union all
    select * from fw_members
)

select * from combined