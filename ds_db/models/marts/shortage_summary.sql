{{ config(materialized='table') }}

with weekly_summary as (
    select
        date_trunc('week', cast(update_date as date)) as week_start,
        shortage_status_category,
        count(*) as shortage_count,
        count(distinct manufacturer) as unique_manufacturers,
        avg(shortage_duration_days) as avg_shortage_duration_days,
        count(case when estimated_resupply_date is not null then 1 end) as with_estimated_resupply,
        count(case when actual_resupply_date is not null then 1 end) as with_actual_resupply
    from {{ ref('fact_drug_shortages') }}
    where update_date is not null
    group by 
        date_trunc('week', cast(update_date as date)),
        shortage_status_category
),

monthly_summary as (
    select
        date_trunc('month', cast(update_date as date)) as month_start,
        shortage_status_category,
        count(*) as shortage_count,
        count(distinct manufacturer) as unique_manufacturers,
        avg(shortage_duration_days) as avg_shortage_duration_days,
        count(case when estimated_resupply_date is not null then 1 end) as with_estimated_resupply,
        count(case when actual_resupply_date is not null then 1 end) as with_actual_resupply
    from {{ ref('fact_drug_shortages') }}
    where update_date is not null
    group by 
        date_trunc('month', cast(update_date as date)),
        shortage_status_category
)

select 
    'weekly' as summary_type,
    week_start as period_start,
    shortage_status_category,
    shortage_count,
    unique_manufacturers,
    avg_shortage_duration_days,
    with_estimated_resupply,
    with_actual_resupply
from weekly_summary

union all

select 
    'monthly' as summary_type,
    month_start as period_start,
    shortage_status_category,
    shortage_count,
    unique_manufacturers,
    avg_shortage_duration_days,
    with_estimated_resupply,
    with_actual_resupply
from monthly_summary