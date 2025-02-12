WITH adplatform_data AS (
    SELECT 
        date,
        traffic_source,
        session_id,
        user_crm_id,
        SUM(cost) AS total_cost,
        SUM(clicks) AS total_clicks,
        SUM(impressions) AS total_impressions,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(DISTINCT user_crm_id) AS total_new_customers
    FROM 
        {{ ref('adplatformdata_unpivot_JO') }}
    WHERE
        traffic_medium = 'cpc'
    GROUP BY 
        date, traffic_source, session_id, user_crm_id
),
funnelevents AS (
    SELECT
        session_id,
        COUNT(DISTINCT transaction_id) AS total_conversions
    FROM
        {{ ref('funnelevents') }}
    GROUP BY session_id
)
SELECT 
    a.date,
    a.traffic_source,
    SUM(a.total_cost) AS total_cost,
    SUM(a.total_clicks) AS total_clicks,
    SUM(a.total_impressions) AS total_impressions,
    SUM(a.total_sessions) AS total_sessions,
    SUM(f.total_conversions) AS total_transactions,
    SUM(a.total_new_customers) AS total_new_customers,
    (SUM(a.total_clicks) / SUM(a.total_impressions)) * 100 AS click_through_rate,
    (SUM(f.total_conversions) / SUM(a.total_new_customers)) * 100 AS conversion_rate,
    (SUM(a.total_cost) / SUM(a.total_clicks)) AS cost_per_click,
    (SUM(a.total_cost) / SUM(a.total_new_customers)) AS cost_per_acquisition,
    (SUM(a.total_cost) / SUM(a.total_sessions)) AS cost_per_session
FROM 
    adplatform_data AS a
LEFT JOIN 
    funnelevents AS f
    ON a.session_id = f.session_id
WHERE 
    a.traffic_source IN ('google', 'meta', 'rtbhouse')
GROUP BY 
    a.date, a.traffic_source
ORDER BY 
    a.date
