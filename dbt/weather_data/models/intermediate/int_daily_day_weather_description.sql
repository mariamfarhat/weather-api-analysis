{{ config(materialized = 'view') }}

with weather as (
    select * from {{ ref('stg_daily_weather_city') }}
),

weather_day_desc as (
    select
        weather_date,
        weather_main,
		city_name
    from weather
	where 
        day_night_indicator ='d'
),

weather_day_desc_count as (
	select
		weather_date,
		weather_main,
		city_name,
		count(weather_main) as weather_desc_main_count
	from weather_day_desc
	group by
		weather_date, weather_main, city_name
),

weather_day_final_desc as (
	select
		weather_date,
		city_name,
		string_agg(weather_main, ' / ') as weather_main_day_summary
	from weather_day_desc_count
	group by weather_date, city_name
),

avg_weather_at_day as (
    select
        weather_date,
        city_name,
        round(avg(main_temp),0) as day_avg_temp,
        min(min_temp) as day_min_temp,
        max(max_temp) as day_max_temp,
        avg(main_pressure) as day_avg_pressure,
        avg(main_humidity) as day_avg_humidity,
        avg(wind_speed_kmh) as day_avg_wind_speed_kmh,
        sum(COALESCE(rain_for_last_three_hours, 0)) as day_total_rain_mm
    from weather
    where 
		day_night_indicator ='d'
    group by weather_date, city_name
),

final as (
    select
        a.weather_date,
        a.day_avg_temp,
        a.day_min_temp,
        a.day_max_temp,
        a.day_avg_pressure,
        a.day_avg_humidity,
        a.day_avg_wind_speed_kmh,
        a.day_total_rain_mm,
        b.city_name,
        b.weather_main_day_summary
    from avg_weather_at_day a
    left join weather_day_final_desc b
    on a.weather_date = b.weather_date
    and a.city_name = b.city_name
)

select * from final