{{ config(
    materialized='table'
) }}

WITH adplatform_data AS (
    SELECT 
        date,
        'criteo' AS traffic_source,
        criteo_cost AS cost,
        criteo_clicks AS clicks,
        criteo_impressions AS impressions
    FROM 
        {{ source('prism_acquire', 'adplatform_data') }}
    UNION ALL
    SELECT 
        date,
        'google' AS traffic_source,
        google_cost AS cost,
        google_clicks AS clicks,
        google_impressions AS impressions
    FROM 
        {{ source('prism_acquire', 'adplatform_data') }}
    UNION ALL
    SELECT 
        date,
        'meta' AS traffic_source,
        meta_cost AS cost,
        meta_clicks AS clicks,
        meta_impressions AS impressions
    FROM 
        {{ source('prism_acquire', 'adplatform_data') }}
    UNION ALL
    SELECT 
        date,
        'rtbhouse' AS traffic_source,
        rtbhouse_cost AS cost,
        rtbhouse_clicks AS clicks,
        rtbhouse_impressions AS impressions
    FROM 
        {{ source('prism_acquire', 'adplatform_data') }}
    UNION ALL
    SELECT 
        date,
        'tiktok' AS traffic_source,
        tiktok_cost AS cost,
        tiktok_clicks AS clicks,
        tiktok_impressions AS impressions
    FROM 
        {{ source('prism_acquire', 'adplatform_data') }}
)

SELECT 
    s.date,
    s.user_cookie_id,
    s.user_crm_id,
    s.session_id,
    s.traffic_source,
    s.traffic_medium,
    s.device_category,
    s.city,
    a.cost,
    a.clicks,
    a.impressions
FROM 
    {{ source('prism_acquire', 'sessions') }} AS s
LEFT JOIN
    adplatform_data AS a
ON 
    s.date = a.date AND s.traffic_source = a.traffic_source
