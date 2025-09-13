{{ config(materialized='table') }}

with base_data as (
    select
        generic_name,
        company_name,
        availability_status,
        update_date,
        presentation,
        therapeutic_category
    from {{ ref('stg_drug_shortages') }}
    where generic_name is not null
      and update_date is not null
      and availability_status != 'discontinued'
),

episodes as (
    select
        generic_name,
        company_name,
        presentation,
        therapeutic_category,
        availability_status,
        update_date as episode_start_date,
        
        coalesce(
            lead(update_date) over (
                partition by generic_name, company_name, presentation 
                order by update_date
            ),
            current_date
        ) as episode_end_date,
        
        coalesce(
            lead(update_date) over (
                partition by generic_name, company_name, presentation 
                order by update_date
            ),
            current_date
        ) - update_date as episode_duration_days
        
    from base_data
)

select
    generic_name,
    company_name, 
    presentation,
    therapeutic_category,
    availability_status,
    episode_start_date,
    episode_end_date,
    episode_duration_days,
    
    -- For Plotly Gantt charts
    generic_name || ' (' || company_name || ')' as drug_display_name,
    
    case 
        when availability_status = 'not available' then '#ff4444'
        when availability_status = 'limited availability' then '#ff8800'  
        when availability_status = 'available' then '#44ff44'
        when availability_status = 'discontinued' then '#888888'
        else '#cccccc'
    end as status_color
    
from episodes
where episode_duration_days > 0
order by generic_name, episode_start_date