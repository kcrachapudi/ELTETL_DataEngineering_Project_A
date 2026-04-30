with source as (
    select * from {{ source('raw', 'okta_users') }}
),

renamed as (
    select
        id                          as okta_id,
        status,
        created::timestamp          as created_at,
        activated::timestamp        as activated_at,
        lastlogin::timestamp        as last_login_at,
        lastupdated::timestamp      as last_updated_at,
        profile_firstname           as first_name,
        profile_lastname            as last_name,
        profile_email               as email,
        profile_login               as login,
        profile_department          as department,
        profile_title               as title,
        profile_manager             as manager,
        profile_city                as city,
        profile_state               as state,
        _source_file                as source_file,
        _ingested_at::timestamp     as ingested_at
    from source
)

select * from renamed