{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

with source_data as (
    select * from {{ source('bronze', 'customers') }}
),

cleaned as (
    select
        customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(trim(email)) as email,
        phone,
        address,
        city,
        state,
        zip_code,
        country,
        registration_date::date as registration_date,
        customer_segment,
        is_active,
        current_timestamp as dbt_updated_at
    from source_data
    where customer_id is not null
        and email is not null
        and email like '%@%'
),

with_full_name as (
    select
        *,
        concat(first_name, ' ', last_name) as full_name,
        datediff('day', registration_date, current_date) as days_since_registration
    from cleaned
)

select * from with_full_name