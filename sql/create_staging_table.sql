-- Create the staging table for drug shortage data
CREATE TABLE IF NOT EXISTS drug_shortages_staging (
    id VARCHAR(255) PRIMARY KEY,
    generic_name TEXT,
    company_name TEXT,
    presentation TEXT,
    update_type VARCHAR(255),
    update_date VARCHAR(255),
    availability TEXT,
    related_info TEXT,
    resolved_note TEXT,
    reason_for_shortage TEXT,
    therapeutic_category TEXT,
    status TEXT,
    change_date VARCHAR(255),
    date_discontinued VARCHAR(255),
    availability_status TEXT,
    ndc TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_status ON drug_shortages_staging(status);
CREATE INDEX IF NOT EXISTS idx_update_date ON drug_shortages_staging(update_date);
CREATE INDEX IF NOT EXISTS idx_created_at ON drug_shortages_staging(created_at);
CREATE INDEX IF NOT EXISTS idx_company_name ON drug_shortages_staging(company_name);
CREATE INDEX IF NOT EXISTS idx_generic_name ON drug_shortages_staging(generic_name);

-- Create a view for recent data (last 30 days)
CREATE OR REPLACE VIEW recent_drug_shortages AS
SELECT *
FROM drug_shortages_staging
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days';

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
        change_date,
        date_discontinued,
        availability_status,
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
        change_date,
        date_discontinued,
        availability_status,
        ndc,
        created_at,
        'historical' as data_source
    FROM drug_shortages_historical_classified
),
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COALESCE(id, ''),
                COALESCE(generic_name, ''),
                COALESCE(company_name, ''),
                COALESCE(presentation, ''),
                COALESCE(update_type, ''),
                COALESCE(update_date, ''),
                COALESCE(availability, ''),
                COALESCE(related_info, ''),
                COALESCE(resolved_note, ''),
                COALESCE(reason_for_shortage, ''),
                COALESCE(therapeutic_category, ''),
                COALESCE(status, ''),
                COALESCE(change_date, ''),
                COALESCE(date_discontinued, ''),
                COALESCE(availability_status, ''),
                COALESCE(ndc, '')
            ORDER BY 
                CASE WHEN data_source = 'staging' THEN 1 ELSE 2 END,
                created_at DESC
        ) as row_num
    FROM all_records
)
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
    change_date,
    date_discontinued,
    availability_status,
    ndc,
    created_at,
    data_source
FROM deduplicated
WHERE row_num = 1;