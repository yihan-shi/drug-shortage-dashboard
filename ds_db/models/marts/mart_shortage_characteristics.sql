{{ config(materialized='table') }}

-- Mart model for drug shortage characteristics pie charts.
-- One row per drug_identifier with its characteristics and date range.

with shortage_ndc as (
    select * from {{ ref('int_shortage_ndc') }}
),

drug_summary as (
    select
        drug_identifier,
        route_category,
        "single_source",
        min(update_date) as first_update_date,
        max(update_date) as last_update_date
    from shortage_ndc
    where drug_identifier is not null
      and update_date is not null
      and (shortage_status is null or shortage_status != 'discontinued')
    group by drug_identifier, route_category, "single_source"
)

select
    drug_identifier,
    route_category,
    "single_source",
    first_update_date,
    last_update_date
from drug_summary
