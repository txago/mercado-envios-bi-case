{{
    config(
        materialized='table'
    )
}}

-- Primeiro descobrimos as datas do dataset
WITH date_range AS (
    SELECT 
        MIN(APPOINTMENT_DATE) as min_date,
        MAX(APPOINTMENT_DATE) as max_date
    FROM {{ ref('stg_inbound_operations') }}
),

historical_data AS (
    SELECT 
        op.SHP_SITE_ID,
        op.APPOINTMENT_DATE,
        op.APPOINTMENT_HOUR,
        COUNT(*) as historical_demand
    FROM {{ ref('stg_inbound_operations') }} op
    CROSS JOIN date_range dr
    WHERE op.APPOINTMENT_DATE BETWEEN dr.min_date AND dr.max_date
    AND op.LAST_INB_STATUS != 'cancelled'
    GROUP BY op.SHP_SITE_ID, op.APPOINTMENT_DATE, op.APPOINTMENT_HOUR
),

forecast_base AS (
    SELECT 
        SHP_SITE_ID,
        APPOINTMENT_HOUR,
        AVG(historical_demand) as avg_demand,
        EXTRACT(DAYOFWEEK FROM APPOINTMENT_DATE) as day_of_week
    FROM historical_data
    GROUP BY SHP_SITE_ID, APPOINTMENT_HOUR, day_of_week
),

forecast_with_day_names AS (
    SELECT 
        *,
        CASE day_of_week
            WHEN 1 THEN '1-Sunday'
            WHEN 2 THEN '2-Monday' 
            WHEN 3 THEN '3-Tuesday'
            WHEN 4 THEN '4-Wednesday'
            WHEN 5 THEN '5-Thursday'
            WHEN 6 THEN '6-Friday'
            WHEN 7 THEN '7-Saturday'
        END as day_name
    FROM forecast_base
)

SELECT 
    SHP_SITE_ID,
    APPOINTMENT_HOUR,
    day_of_week,
    day_name,
    avg_demand as forecasted_demand,
    CASE 
        WHEN avg_demand > 8 THEN 'CRITICAL'
        WHEN avg_demand > 5 THEN 'HIGH'
        ELSE 'NORMAL'
    END as demand_level
FROM forecast_with_day_names
ORDER BY SHP_SITE_ID, day_of_week, APPOINTMENT_HOUR