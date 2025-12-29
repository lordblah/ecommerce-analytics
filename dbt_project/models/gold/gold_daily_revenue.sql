{{
    config(
        materialized='table'
    )
}}

with orders as (
    select * from {{ ref('silver_orders') }}
    where order_status in ('completed', 'shipped')
),

daily_stats as (
    select
        date_trunc('day', order_date)::date as order_date,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(order_total) as revenue,
        avg(order_total) as avg_order_value,
        sum(total_quantity) as items_sold
    from orders
    group by date_trunc('day', order_date)::date
),

with_moving_averages as (
    select
        order_date,
        total_orders,
        unique_customers,
        revenue,
        avg_order_value,
        items_sold,
        avg(revenue) over (
            order by order_date 
            rows between 6 preceding and current row
        ) as revenue_7day_ma,
        avg(revenue) over (
            order by order_date 
            rows between 29 preceding and current row
        ) as revenue_30day_ma
    from daily_stats
)

select 
    order_date,
    total_orders,
    unique_customers,
    round(revenue, 2) as revenue,
    round(avg_order_value, 2) as avg_order_value,
    items_sold,
    round(revenue_7day_ma, 2) as revenue_7day_ma,
    round(revenue_30day_ma, 2) as revenue_30day_ma,
    current_timestamp as dbt_updated_at
from with_moving_averages
order by order_date desc