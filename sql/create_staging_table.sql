-- Create the staging table for drug shortage data
CREATE TABLE IF NOT EXISTS drug_shortages_staging (
    id INTEGER PRIMARY KEY,
    generic_name TEXT,
    company_name TEXT,
    presentation TEXT,
    update_type TEXT,
    update_date DATE,
    availability TEXT,
    related_info TEXT,
    resolved_note TEXT,
    reason_for_shortage TEXT,
    therapeutic_category TEXT,
    status TEXT,
    status_change_date DATE,
    change_date DATE,
    date_discontinued DATE,
    availability_status TEXT,
    ndc TEXT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_status ON drug_shortages_staging(status);
CREATE INDEX IF NOT EXISTS idx_update_date ON drug_shortages_staging(update_date);
CREATE INDEX IF NOT EXISTS idx_company_name ON drug_shortages_staging(company_name);
CREATE INDEX IF NOT EXISTS idx_generic_name ON drug_shortages_staging(generic_name);

-- Create a union view combining historical and staging data with deduplication
CREATE OR REPLACE VIEW drug_shortages_combined AS
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
        ndc,
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
        ndc,
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
            availability_status, ndc
        ) as row_num
    FROM
        all_records
)

SELECT 
    id, generic_name, company_name, presentation, update_type, 
    update_date, availability, related_info, resolved_note, 
    reason_for_shortage, therapeutic_category, status, 
    status_change_date, change_date, date_discontinued, 
    availability_status, ndc
FROM deduplicated
WHERE row_num = 1;