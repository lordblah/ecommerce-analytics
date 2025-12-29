{{
    config(
        materialized='table',
        unique_key='product_id'
    )
}}

with source_data as (
    select * from {{ source('bronze', 'products') }}
),

cleaned as (
    select
        product_id,
        trim(product_name) as product_name,
        trim(category) as category,
        trim(subcategory) as subcategory,
        trim(brand) as brand,
        cost_price,
        list_price,
        current_price,
        stock_quantity,
        reorder_level,
        is_active,
        created_at::date as created_at,
        current_timestamp as dbt_updated_at
    from source_data
    where product_id is not null
        and product_name is not null
        and list_price > 0
        and cost_price > 0
),

with_metrics as (
    select
        *,
        round((current_price - cost_price) / current_price * 100, 2) as margin_pct,
        case
            when stock_quantity = 0 then 'Out of Stock'
            when stock_quantity <= reorder_level then 'Low Stock'
            when stock_quantity > reorder_level * 3 then 'Overstocked'
            else 'Normal'
        end as stock_status,
        datediff('day', created_at, current_date) as days_in_catalog
    from cleaned
)

select * from with_metrics