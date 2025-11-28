{{ config(materialized = 'view') }}

with source as (
    select * from {{ source('weather_data_api', 'raw_data') }}
),

renamed as (

    select
        cast(dt_txt as date) as weather_date,
        format(cast(dt_txt as datetime), 'HH:mm') as weather_hour,
        city as city_name,
        [weather_main] as weather_main,
        round([main.temp],0) as main_temp,
        round([main.temp_min],0) as min_temp,
        round([main.temp_max],0) as max_temp,
        [main.pressure] as main_pressure, 
        [main.humidity] as main_humidity,
        [wind.speed]*3.6 as wind_speed_kmh,
        [sys.pod] as day_night_indicator,
        COALESCE([rain.3h], 0) as rain_for_last_three_hours
    from source
)

select * from renamed