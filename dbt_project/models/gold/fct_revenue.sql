{{
  config(
    materialized='incremental',
    unique_key=['order_date', 'region', 'store_id'],
    incremental_strategy='merge',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=['region']
  )
}}

with orders as (
    select
        order_id,
        store_id,
        customer_id_masked,
        order_amount,
        order_date::date as order_date,
        status
    from {{ ref('silver_orders') }}
    where status != 'CANCELLED'

    {% if is_incremental() %}
      and order_date >= (select max(order_date) - interval '3 days' from {{ this }})
    {% endif %}
),

stores as (
    select
        store_id,
        store_name,
        region,
        state
    from {{ ref('dim_store') }}
),

joined as (
    select
        o.order_date,
        s.region,
        s.state,
        o.store_id,
        s.store_name,
        o.order_id,
        o.customer_id_masked,
        o.order_amount
    from orders o
    left join stores s on o.store_id = s.store_id
),

aggregated as (
    select
        order_date,
        region,
        state,
        store_id,
        store_name,
        count(order_id)                         as total_orders,
        sum(order_amount)                       as total_revenue,
        avg(order_amount)                       as avg_order_value,
        count(distinct customer_id_masked)      as unique_customers,
        min(order_amount)                       as min_order_value,
        max(order_amount)                       as max_order_value
    from joined
    group by 1, 2, 3, 4, 5
)

select
    *,
    current_timestamp() as _dbt_updated_at
from aggregated
