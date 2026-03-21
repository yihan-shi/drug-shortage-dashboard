{{ config(materialized='table') }}

-- Kaplan-Meier survival data: one row per drug_identifier shortage episode.
-- Duration = days from first 'new' status to resolution ('ended').
-- Censored if not yet resolved.
-- Excludes discontinued drugs.

with shortage_data as (
    select
        drug_identifier,
        route_category,
        "single_source",
        shortage_status,
        update_date
    from {{ ref('int_shortage_ndc') }}
    where drug_identifier is not null
      and update_date is not null
      and shortage_status is not null
      and shortage_status != 'discontinued'
),

episodes as (
    select
        drug_identifier,
        route_category,
        "single_source",
        min(case when shortage_status in ('new', 'continued') then update_date end) as shortage_start_date,
        min(case when shortage_status = 'ended' then update_date end) as resolution_date
    from shortage_data
    group by drug_identifier, route_category, "single_source"
)

select
    drug_identifier,
    route_category,
    "single_source",
    shortage_start_date,
    resolution_date,
    case when resolution_date is not null then true else false end as resolved,
    case
        when resolution_date is not null then resolution_date - shortage_start_date
        else current_date - shortage_start_date
    end as duration_days
from episodes
where shortage_start_date is not null
