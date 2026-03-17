{{ config(materialized='view') }}

with shortages as (
    select * from {{ ref('stg_drug_shortages') }}
),

ndc_dir as (
    select * from {{ source('drug_shortages', 'ndc_fda') }}
),

joined as (
    select
        s.*,
        n."key",
        n."ApplNo",
        n."DrugName",
        n."SponsorName_x",
        n."single_source",
        n."ActiveIngredient",
        n."PROPRIETARYNAME",
        n."APPLICATIONNUMBER",
        n."ROUTENAME"      as route_name_raw,
        n."SUBSTANCENAME"  as substance_name,
        n."LABELERNAME"
    from shortages s
    left join ndc_dir n
        on s.ndc = n."PRODUCTNDC"
),

classified as (
    select
        *,
        case
            when lower(route_name_raw) like any(array[
                '%intravenous%', '%intramuscular%', '%subcutaneous%', '%parenteral%',
                '%epidural%', '%intrathecal%', '%intradermal%', '%intraperitoneal%',
                '%intrapleural%', '%intravascular%', '%intracardiac%', '%intracavitary%',
                '%intracoronary%', '%intraventricular%', '%intracerebral%', '%intramedullary%',
                '%intralesional%', '%subarachnoid%', '%intraspinal%', '%perineural%',
                '%infiltration%', '%submucosal%', '%intrathoracic%', '%intrauterine%',
                '%intragastric%', '%intraepidermal%', '%intrasinal%', '%hemodialysis%',
                '%extracorporeal%', '%retrobulbar%', '%subgingival%', '%endocervical%',
                '%intraluminal%', '%intramedullary%', '%intracavernous%', '%intratympanic%'
            ]) then 'injectable'
            when lower(route_name_raw) like any(array[
                '%inhalation%', '%endotracheal%', '%intrabronchial%',
                '%laryngeal%', '%transtracheal%'
            ]) then 'inhalation'
            when lower(route_name_raw) like any(array[
                '%ophthalmic%', '%intraocular%', '%intravitreal%',
                '%intracameral%', '%intracanalicular%', '%suprachoroidal%', '%conjunctival%'
            ]) then 'ophthalmic'
            when lower(route_name_raw) like any(array[
                '%oral%', '%sublingual%', '%buccal%', '%enteral%',
                '%oropharyngeal%', '%nasogastric%', '%transmucosal%'
            ]) then 'oral'
            when lower(route_name_raw) like any(array[
                '%topical%', '%cutaneous%', '%transdermal%', '%percutaneous%'
            ]) then 'topical'
            when lower(route_name_raw) like '%nasal%' then 'nasal'
            when lower(route_name_raw) like any(array['%otic%', '%intratympanic%']) then 'otic'
            when lower(route_name_raw) like '%rectal%' then 'rectal'
            when lower(route_name_raw) like '%vaginal%' then 'vaginal'
            when lower(route_name_raw) like any(array[
                '%dental%', '%periodontal%', '%subgingival%'
            ]) then 'dental'
            when lower(route_name_raw) like any(array[
                '%ureteral%', '%urethral%', '%intravesical%', '%irrigation%'
            ]) then 'urological'
            when route_name_raw is null then null
            else 'other'
        end as route_category
    from joined
)

select
    *,
    lower(substance_name) || '_' || coalesce(route_category, 'unknown') as drug_identifier
from classified
