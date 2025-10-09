{{ config(materialized='view') }}

-- Combined view of historical and staging data with deduplication
WITH all_records AS (
    SELECT 
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
        status_change_date,
        change_date,
        date_discontinued,
        availability_status,
        shortage_status,
        ndc,
        created_at,
        'staging' as data_source
    FROM drug_shortages_staging

    UNION ALL

    SELECT 
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
        status_change_date,
        change_date,
        date_discontinued,
        availability_status,
        shortage_status,
        ndc,
        CAST('2025-09-11 10:30:00' AS DATE) as created_at,
        'historical' as data_source
    FROM drug_shortages_historical_classified
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY generic_name, company_name, presentation, update_type, 
            update_date, availability, related_info, resolved_note, 
            reason_for_shortage, therapeutic_category, status, 
            status_change_date, change_date, date_discontinued, 
            availability_status, shortage_status, ndc
        ) as row_num
    FROM
        all_records
)

SELECT 
    id, generic_name, company_name, presentation, update_type, 
    update_date, availability, related_info, resolved_note, 
    reason_for_shortage, therapeutic_category, status, 
    status_change_date, change_date, date_discontinued, 
    availability_status, shortage_status, ndc, created_at
FROM deduplicated
WHERE row_num = 1