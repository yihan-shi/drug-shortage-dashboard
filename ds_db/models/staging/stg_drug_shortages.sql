{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('drug_shortages', 'drug_shortages_combined') }}
),

cleaned as (
    select
        id,
        generic_name,
        company_name,
        presentation,
        update_type,
        case 
            when update_date = '' then null
            else update_date
        end as update_date,
        availability,
        related_info,
        resolved_note,
        reason_for_shortage,
        therapeutic_category,
        status,
        case 
            when change_date = '' then null
            else change_date
        end as change_date,
        case 
            when date_discontinued = '' then null
            else date_discontinued
        end as date_discontinued,
        availability_status,
        ndc,
        created_at,
        data_source
    from source_data
)

select * from cleaned