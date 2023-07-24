{{ config(
    materialized = 'table'
) }}

with scores as(
    select distinct
    ip,
    normalized_company_domain,
    source,
    source_confidence,
    case source
        when 'DIGITAL ELEMENT' then 0.3
        when 'IP FLOW' then 0.5
        when 'FIVE BY FIVE' then 0.9
        when 'LASTBOUNCE' then 0.7
    else 0.5 end as source_score,
    1.0-(0.01*(current_date - LAST_QUERY_DATE)) as recency_score,
    ifnull(source_confidence, 1)*recency_score*source_score as score
from {{ ref('stg_ipflow_merge_domain_observations') }}
where normalized_company_domain != 'Shared'
and normalized_company_domain is not null
and len(normalized_company_domain)>1
),
map_scores as (
    select
    ip,
    normalized_company_domain,
    array_agg(distinct source) as sources,
    sum(score) as score
  from scores
  group by 1,2
),
score_totals as (
    select
    ip,
    sum(score) as score_total
    from map_scores
  group by 1
)
select distinct
    a.ip,
    normalized_company_domain,
    greatest(0.1, (CASE WHEN score_total=0 THEN 0 ELSE score/score_total END) ) as score
from map_scores a
join score_totals b
on a.ip = b.ip
