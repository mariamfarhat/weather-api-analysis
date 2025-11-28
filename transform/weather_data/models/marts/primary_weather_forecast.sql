{{ config(materialized = 'table') }}

with weather_day as (
    select * from {{ ref('int_daily_day_weather_description') }}
),

weather_night as (
    select * from {{ ref('int_daily_night_weather_description') }}
),

weather_upcoming_summary as (
    select
        d.weather_date,
        d.city_name,
        d.day_avg_temp,
        d.day_min_temp,
        d.day_max_temp,
        d.day_avg_pressure,
        d.day_avg_humidity,
        d.day_avg_wind_speed_kmh,
        d.weather_main_day_summary,
        d.day_total_rain_mm,

        n.night_avg_temp,
        n.night_min_temp,
        n.night_max_temp,
        n.night_avg_pressure,
        n.night_avg_humidity,
        n.night_avg_wind_speed_kmh,
        n.night_total_rain_mm,
        n.weather_main_night_summary

    from weather_day d
    left join weather_night n
    on d.weather_date = n.weather_date
    and d.city_name = n.city_name
),

final as(
    select
        weather_date,
        city_name,
        day_avg_temp,
        night_avg_temp,
        round((day_avg_wind_speed_kmh + night_avg_wind_speed_kmh)/2,0) as overall_avg_wind_speed_kmh,
        (day_total_rain_mm + night_total_rain_mm) as total_rain_mm
    from weather_upcoming_summary
)
select * from final
