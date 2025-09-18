{{ config(
    materialized = "incremental",
    unique_key = "order_id",
    partition_by = {
      "field": "order_created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    incremental_strategy = "insert_overwrite"
) }}

with orders as (

    select
        order_id,
        user_name as user_id,
        order_status,

        -- Ensure TIMESTAMP type for partitioning (works if source is TIMESTAMP, DATETIME, DATE, or STRING)
        TIMESTAMP(order_date) as order_created_at,
        TIMESTAMP(order_approved_date) as order_approved_at,
        TIMESTAMP(pickup_date) as picked_up_at,
        TIMESTAMP(delivered_date) as delivered_at,
        TIMESTAMP(estimated_time_delivery) as estimated_time_delivery

    from {{ source('sales_database', 'order') }}

)

select *
from orders

{% if is_incremental() %}
where order_created_at > COALESCE(
  -- Cast MAX from existing table to TIMESTAMP to avoid TIMESTAMP vs DATETIME mismatches
  (select CAST(MAX(order_created_at) as TIMESTAMP) from {{ this }}),
  -- Fallback for first run / empty table to limit source scan
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
)
{% endif %}
