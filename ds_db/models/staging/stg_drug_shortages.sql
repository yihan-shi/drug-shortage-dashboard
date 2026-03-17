{{ config(materialized='view') }}

-- This model performs light data cleaning and transformation on the combined drug shortages data

with source_data as (
    select * from {{ ref('drug_shortages_combined') }}
),

cleaned as (
    select
        id,
        generic_name,
        company_name,
        presentation,
        update_type,
        case 
            when update_date::text = '' then null
            else update_date
        end as update_date,
        availability,
        related_info,
        resolved_note,
        reason_for_shortage,
        therapeutic_category,
        status,
        case 
            when change_date::text = '' then null
            else change_date
        end as change_date,
        case 
            when date_discontinued::text = '' then null
            else date_discontinued
        end as date_discontinued,
        shortage_status,
        ndc,
        created_at
    from source_data
),

exploded as (
    select
        id,
        generic_name,
        company_name,
        presentation,
        update_type,
        update_date,
        availability,
        related_info,
        resolved_note,
        reason_for_shortage,
        therapeutic_category,
        status,
        change_date,
        date_discontinued,
        shortage_status,
        trim(regexp_replace(unnest(coalesce(string_to_array(ndc, ','), array[null::text])),'-[^-]*$', '')) as ndc_raw,
        created_at
    from cleaned
)

select
    id,
    generic_name,
    company_name,
    presentation,
    update_type,
    update_date,
    availability,
    related_info,
    resolved_note,
    reason_for_shortage,
    therapeutic_category,
    status,
    change_date,
    date_discontinued,
    shortage_status,
    nullif(ndc_raw, '') as ndc,
    created_at
from exploded