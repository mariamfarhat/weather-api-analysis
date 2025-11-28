{{ config(materialized = 'table') }}

with weather as (
    select * from {{ ref('stg_daily_weather_city') }}
),

weather_information_by_date as (
    select
        weather_hour,
        weather_main,
        main_temp,
        min_temp,
        max_temp,
        main_pressure,
        main_humidity,
        wind_speed_kmh,
        rain_for_last_three_hours
    from weather
    where
        weather_date = '2025-11-24'
    and
        city_name = 'london'
)

select * from weather_information_by_date
