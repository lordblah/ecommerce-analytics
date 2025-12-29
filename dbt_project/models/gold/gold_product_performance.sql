{{
    config(
        materialized='table'
    )
}}

with products as (
    select * from {{ source('bronze', 'products') }}
),

order_items as (
    select * from {{ source('bronze', 'order_items') }}
),

orders as (
    select * from {{ ref('silver_orders') }}
    where order_status in ('completed', 'shipped')
),

product_sales as (
    select
        oi.product_id,
        count(distinct o.order_id) as total_orders,
        sum(oi.quantity) as units_sold,
        sum(oi.line_total) as gross_revenue,
        sum(oi.discount_amount) as total_discounts,
        sum(oi.line_total - oi.discount_amount) as net_revenue,
        avg(oi.unit_price) as avg_selling_price,
        min(o.order_date) as first_sale_date,
        max(o.order_date) as last_sale_date,
        count(distinct o.customer_id) as unique_customers
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    group by oi.product_id
),

product_metrics as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.list_price,
        p.current_price,
        p.cost_price,
        p.stock_quantity,
        p.is_active,
        coalesce(ps.total_orders, 0) as total_orders,
        coalesce(ps.units_sold, 0) as units_sold,
        coalesce(ps.net_revenue, 0) as net_revenue,
        coalesce(ps.avg_selling_price, 0) as avg_selling_price,
        ps.first_sale_date,
        ps.last_sale_date,
        case
            when ps.net_revenue > 0 then
                (ps.net_revenue - (ps.units_sold * p.cost_price)) / ps.net_revenue * 100
            else 0
        end as profit_margin_pct,
        case
            when ps.net_revenue > 0 then
                ps.net_revenue - (ps.units_sold * p.cost_price)
            else 0
        end as total_profit,
        case
            when p.stock_quantity = 0 then 'Out of Stock'
            when p.stock_quantity <= p.reorder_level then 'Low Stock'
            else 'Normal'
        end as stock_status,
        case
            when ps.net_revenue >= 10000 then 'Top Performer'
            when ps.net_revenue >= 5000 then 'Strong Performer'
            when ps.net_revenue >= 1000 then 'Average Performer'
            when ps.net_revenue > 0 then 'Underperformer'
            else 'No Sales'
        end as performance_tier,
        current_timestamp as dbt_updated_at
    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from product_metrics
order by net_revenue desc