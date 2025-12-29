{{
    config(
        materialized='table',
        unique_key='order_id'
    )
}}

with source_orders as (
    select * from {{ source('bronze', 'orders') }}
),

source_items as (
    select * from {{ source('bronze', 'order_items') }}
),

order_totals as (
    select
        order_id,
        sum(line_total) as subtotal,
        sum(discount_amount) as item_discounts,
        count(*) as item_count,
        sum(quantity) as total_quantity
    from source_items
    group by order_id
),

enriched_orders as (
    select
        o.order_id,
        o.customer_id,
        o.order_date::timestamp as order_date,
        o.order_status,
        o.shipping_address,
        o.shipping_city,
        o.shipping_state,
        o.shipping_zip,
        o.payment_method,
        o.shipping_cost,
        coalesce(ot.subtotal, 0) as subtotal,
        coalesce(o.discount_amount, 0) + coalesce(ot.item_discounts, 0) as total_discount,
        coalesce(ot.subtotal, 0) * 0.08 as tax_amount,
        coalesce(ot.subtotal, 0) 
            - (coalesce(o.discount_amount, 0) + coalesce(ot.item_discounts, 0))
            + (coalesce(ot.subtotal, 0) * 0.08)
            + coalesce(o.shipping_cost, 0) as order_total,
        coalesce(ot.item_count, 0) as item_count,
        coalesce(ot.total_quantity, 0) as total_quantity,
        o.created_at::timestamp as created_at,
        o.updated_at::timestamp as updated_at,
        current_timestamp as dbt_updated_at
    from source_orders o
    left join order_totals ot on o.order_id = ot.order_id
)

select * from enriched_orders
where order_status in ('completed', 'shipped', 'processing', 'cancelled', 'returned')