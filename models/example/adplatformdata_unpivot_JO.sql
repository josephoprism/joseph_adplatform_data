{{ config(
    materialized='table'
) }}

select 
    date,
    'criteo' as traffic_source,
    criteo_cost as cost,
    criteo_clicks as clicks,
    criteo_impressions as impressions
from 
    {{ source('prism_acquire', 'adplatform_data') }}
union all
select 
    date,
    'google' as traffic_source,
    google_cost as cost,
    google_clicks as clicks,
    google_impressions as impressions
from 
    {{ source('prism_acquire', 'adplatform_data') }}
union all
select 
    date,
    'meta' as traffic_source,
    meta_cost as cost,
    meta_clicks as clicks,
    meta_impressions as impressions
from 
    {{ source('prism_acquire', 'adplatform_data') }}
union all
select 
    date,
    'rtbhouse' as traffic_source,
    rtbhouse_cost as cost,
    rtbhouse_clicks as clicks,
    rtbhouse_impressions as impressions
from 
    {{ source('prism_acquire', 'adplatform_data') }}
union all
select 
    date,
    'tiktok' as traffic_source,
    tiktok_cost as cost,
    tiktok_clicks as clicks,
    tiktok_impressions as impressions
from 
    {{ source('prism_acquire', 'adplatform_data') }}
