{{ config(materialized='table') }}

with manufacturer_data as (
    select 
        company_name,
        count(*) as shortage_count,
        min(created_at) as first_seen,
        max(created_at) as last_seen
    from {{ ref('stg_drug_shortages') }}
    where company_name is not null
    group by company_name
)

select
    md5(company_name) as manufacturer_key,
    company_name as manufacturer_name,
    shortage_count,
    first_seen,
    last_seen
from manufacturer_data