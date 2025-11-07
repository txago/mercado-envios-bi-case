{{
    config(
        materialized='table'
    )
}}

WITH inbound_ops AS (
    SELECT * FROM {{ ref('stg_inbound_operations') }}
    WHERE MIN_ARRIVAL_DATETIME_TZ IS NOT NULL
    AND LAST_INB_STATUS IN ('delivered', 'in_progress', 'delayed')
),

operational_metrics AS (
    SELECT 
        -- Dimensões
        SHP_SITE_ID,
        ADD_STATE_NAME_SHP,
        APPOINTMENT_HOUR,
        APPOINTMENT_DAY_OF_WEEK,
        APPOINTMENT_DATE,
        
        -- KPIs principais
        COUNT(*) as TOTAL_APPOINTMENTS,
        
        -- Delivery Time em minutos
        AVG(DELIVERY_DELAY_MINUTES) as AVG_DELIVERY_TIME_MINUTES,
        
        -- Delivery Time em horas
        AVG(DELIVERY_DELAY_MINUTES) / 60.0 as AVG_DELIVERY_TIME_HOURS,
        
        -- On Time
        AVG(IS_ON_TIME) as ON_TIME_FULFILLMENT_RATE,
        
        -- Identificar slots críticos
        CASE 
            WHEN COUNT(*) > 6 THEN 'HIGH_DEMAND'
            WHEN COUNT(*) > 4 THEN 'MEDIUM_DEMAND' 
            ELSE 'LOW_DEMAND'
        END as DEMAND_SEGMENT

    FROM inbound_ops
    GROUP BY 
        SHP_SITE_ID, ADD_STATE_NAME_SHP, APPOINTMENT_HOUR, 
        APPOINTMENT_DAY_OF_WEEK, APPOINTMENT_DATE
)

SELECT 
    SHP_SITE_ID,
    ADD_STATE_NAME_SHP,
    APPOINTMENT_HOUR,
    APPOINTMENT_DAY_OF_WEEK,
    APPOINTMENT_DATE,
    TOTAL_APPOINTMENTS,
    AVG_DELIVERY_TIME_MINUTES,
    AVG_DELIVERY_TIME_HOURS,
    ON_TIME_FULFILLMENT_RATE,
    DEMAND_SEGMENT,
    
    -- Formatação simplificada
    CASE 
        WHEN AVG_DELIVERY_TIME_MINUTES IS NULL OR AVG_DELIVERY_TIME_MINUTES = 0 THEN '0:00'
        ELSE 
            CONCAT(
                CAST(FLOOR(AVG_DELIVERY_TIME_MINUTES / 60) AS STRING), 
                ':', 
                LPAD(CAST(ROUND(AVG_DELIVERY_TIME_MINUTES - (FLOOR(AVG_DELIVERY_TIME_MINUTES / 60) * 60)) AS STRING), 2, '0')
            )
    END as AVG_DELIVERY_TIME_FORMATTED,
    
    -- Formatar taxa como porcentagem
    ROUND(ON_TIME_FULFILLMENT_RATE * 100, 2) as ON_TIME_FULFILLMENT_RATE_PCT
    
FROM operational_metrics