{{ config(materialized='table') }}

with shortage_facts as (
    select
        id,
        generic_name,
        company_name,
        presentation,
        update_type,
        case 
            when update_date is not null 
            then cast(update_date as date)
            else null 
        end as update_date,
        availability,
        related_info,
        resolved_note,
        reason_for_shortage,
        therapeutic_category,
        status,
        case 
            when change_date is not null 
            then cast(change_date as date)
            else null 
        end as change_date,
        case 
            when date_discontinued is not null 
            then cast(date_discontinued as date)
            else null 
        end as date_discontinued,
        availability_status,
        ndc,
        created_at,
        data_source,
        case 
            when status like '%Discontinued%' or date_discontinued is not null then 'Discontinued'
            when status like '%Resolved%' or resolved_note is not null then 'Resolved'
            when status like '%Shortage%' then 'Active Shortage'
            else 'Other'
        end as shortage_status_category,
        case 
            when company_name is not null then 1 else 0
        end as has_manufacturer_info
    from {{ ref('stg_drug_shortages') }}
)

select * from shortage_facts