with

orders_summary as (

    select
        mapping.account_manager,
        mapping.state,
        date_trunc(order_created_at, day) as report_date,
        count(distinct order_id) as total_orders,
        avg(total_distinct_items) as average_total_distinct_items,
        sum(sum_feedback_score) / sum(total_feedbacks) as average_feedback_score,
        avg(total_order_amount) as average_total_order_amount

    from {{ ref('int_sales_database__order') }} as orders

    left join {{ ref('stg_google_sheets__account_manager_region_mapping') }} as mapping
        on orders.user_state = mapping.state

    group by
        report_date,
        account_manager,
        state

)

select * from orders_summary
