{{
    config(
        materialized='table'
    )
}}

with customers as (
    select * from {{ ref('silver_customers') }}
),

orders as (
    select * from {{ ref('silver_orders') }}
    where order_status in ('completed', 'shipped')
),

customer_order_stats as (
    select
        o.customer_id,
        count(distinct o.order_id) as total_orders,
        sum(o.order_total) as total_revenue,
        avg(o.order_total) as avg_order_value,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        sum(o.total_quantity) as total_items_purchased
    from orders o
    group by o.customer_id
),

rfm_scores as (
    select
        customer_id,
        datediff('day', last_order_date, current_date) as recency_days,
        total_orders as frequency,
        total_revenue as monetary,
        case
            when datediff('day', last_order_date, current_date) <= 30 then 5
            when datediff('day', last_order_date, current_date) <= 90 then 4
            when datediff('day', last_order_date, current_date) <= 180 then 3
            when datediff('day', last_order_date, current_date) <= 365 then 2
            else 1
        end as recency_score,
        case
            when total_orders >= 10 then 5
            when total_orders >= 7 then 4
            when total_orders >= 4 then 3
            when total_orders >= 2 then 2
            else 1
        end as frequency_score,
        case
            when total_revenue >= 1000 then 5
            when total_revenue >= 500 then 4
            when total_revenue >= 250 then 3
            when total_revenue >= 100 then 2
            else 1
        end as monetary_score
    from customer_order_stats
),

customer_segments as (
    select
        *,
        recency_score + frequency_score + monetary_score as rfm_score,
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champions'
            when recency_score >= 3 and frequency_score >= 3 and monetary_score >= 3 then 'Loyal Customers'
            when recency_score >= 4 and frequency_score <= 2 then 'Promising'
            when recency_score >= 3 and monetary_score >= 3 then 'Potential Loyalists'
            when recency_score <= 2 and frequency_score >= 3 then 'At Risk'
            when recency_score <= 2 and frequency_score <= 2 then 'Lost'
            else 'Need Attention'
        end as customer_segment
    from rfm_scores
),

final as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.registration_date,
        c.is_active,
        coalesce(cos.total_orders, 0) as total_orders,
        coalesce(cos.total_revenue, 0) as lifetime_value,
        coalesce(cos.avg_order_value, 0) as avg_order_value,
        cos.first_order_date,
        cos.last_order_date,
        coalesce(cs.recency_days, 999) as days_since_last_order,
        coalesce(cs.recency_score, 0) as recency_score,
        coalesce(cs.frequency_score, 0) as frequency_score,
        coalesce(cs.monetary_score, 0) as monetary_score,
        coalesce(cs.rfm_score, 0) as rfm_score,
        coalesce(cs.customer_segment, 'Never Purchased') as rfm_segment,
        case
            when cos.total_orders is null then 'Never Purchased'
            when cs.recency_days <= 90 then 'Active'
            when cs.recency_days <= 180 then 'Cooling Down'
            when cs.recency_days <= 365 then 'At Risk'
            else 'Churned'
        end as lifecycle_stage,
        current_timestamp as dbt_updated_at
    from customers c
    left join customer_order_stats cos on c.customer_id = cos.customer_id
    left join customer_segments cs on c.customer_id = cs.customer_id
)

select * from final