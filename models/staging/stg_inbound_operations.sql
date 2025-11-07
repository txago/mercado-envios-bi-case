{{
    config(
        materialized='view'
    )
}}

WITH operations AS (
    SELECT 
        CUS_CUST_ID,
        WAREHOUSE_ID,
        INVENTORY_ID,
        MIN_APPOINTMENT_DATETIME_TZ,
        CASE 
            WHEN LAST_INB_STATUS IN ('cancelled', 'scheduled') THEN NULL
            ELSE MIN_ARRIVAL_DATETIME_TZ
        END as MIN_ARRIVAL_DATETIME_TZ,
        LAST_INB_STATUS
    FROM {{ ref('bt_fbm_inbound_operations_agg') }}
),

facilities AS (
    SELECT 
        SHP_FACILITY_ID,
        SHP_FACILITY_TYPE,
        SHP_SITE_ID
    FROM {{ ref('lk_shp_facilities') }}
    WHERE SHP_FACILITY_TYPE IN ('warehouse', 'receiving_center')
),

sellers AS (
    SELECT 
        CUS_CUST_ID,
        ADD_STATE_NAME_SHP
    FROM {{ ref('lk_sf_commercial_sellers_data') }}
)

SELECT 
    op.CUS_CUST_ID,
    op.WAREHOUSE_ID,
    op.INVENTORY_ID,
    op.MIN_APPOINTMENT_DATETIME_TZ,
    op.MIN_ARRIVAL_DATETIME_TZ,
    op.LAST_INB_STATUS,
    fac.SHP_SITE_ID,
    fac.SHP_FACILITY_TYPE,
    sel.ADD_STATE_NAME_SHP,
    
    -- Calcular atraso em minutos
    CASE 
        WHEN op.LAST_INB_STATUS IN ('delivered', 'delayed', 'in_progress') 
        AND op.MIN_ARRIVAL_DATETIME_TZ IS NOT NULL
        THEN TIMESTAMP_DIFF(
            CAST(op.MIN_ARRIVAL_DATETIME_TZ AS TIMESTAMP), 
            CAST(op.MIN_APPOINTMENT_DATETIME_TZ AS TIMESTAMP), 
            MINUTE
        )
        ELSE NULL 
    END as DELIVERY_DELAY_MINUTES,
    
    -- Flag de on-time (até 15 minutos de atraso)
    CASE 
        WHEN op.LAST_INB_STATUS IN ('delivered', 'in_progress') 
        AND op.MIN_ARRIVAL_DATETIME_TZ IS NOT NULL
        AND TIMESTAMP_DIFF(
            CAST(op.MIN_ARRIVAL_DATETIME_TZ AS TIMESTAMP), 
            CAST(op.MIN_APPOINTMENT_DATETIME_TZ AS TIMESTAMP), 
            MINUTE
        ) <= 15
        THEN 1 
        WHEN op.LAST_INB_STATUS IN ('delivered', 'in_progress') 
        AND op.MIN_ARRIVAL_DATETIME_TZ IS NOT NULL
        THEN 0
        ELSE NULL 
    END as IS_ON_TIME,
    
    -- Componentes para análise
    EXTRACT(HOUR FROM op.MIN_APPOINTMENT_DATETIME_TZ) as APPOINTMENT_HOUR,
    CASE EXTRACT(DAYOFWEEK FROM op.MIN_APPOINTMENT_DATETIME_TZ)
        WHEN 1 THEN '1-Sunday'
        WHEN 2 THEN '2-Monday' 
        WHEN 3 THEN '3-Tuesday'
        WHEN 4 THEN '4-Wednesday'
        WHEN 5 THEN '5-Thursday'
        WHEN 6 THEN '6-Friday'
        WHEN 7 THEN '7-Saturday'
    END as APPOINTMENT_DAY_OF_WEEK,
    EXTRACT(DAYOFWEEK FROM op.MIN_APPOINTMENT_DATETIME_TZ) as APPOINTMENT_DAY_OF_WEEK_NUM,
    
    EXTRACT(DATE FROM op.MIN_APPOINTMENT_DATETIME_TZ) as APPOINTMENT_DATE

FROM operations op
LEFT JOIN facilities fac 
    ON op.WAREHOUSE_ID = fac.SHP_FACILITY_ID
LEFT JOIN sellers sel 
    ON op.CUS_CUST_ID = sel.CUS_CUST_ID