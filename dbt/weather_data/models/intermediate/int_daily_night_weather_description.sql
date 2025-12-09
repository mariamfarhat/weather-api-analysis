{{ config(materialized = 'view') }}

with weather as (
    select * from {{ ref('stg_daily_weather_city') }}
),

weather_night_desc as (
    select
        weather_date,
        weather_main,
		city_name
	from weather
   	where 
        day_night_indicator ='n'
),

weather_night_desc_count as (
	select
		weather_date,
		weather_main,
		city_name,
		count(weather_main) as weather_desc_main_count
	from weather_night_desc
	group by
		weather_date, weather_main, city_name
),

weather_night_final_desc as (
	select
		weather_date,
		city_name,
		string_agg(weather_main, ' / ') as weather_main_night_summary
	from weather_night_desc_count
	group by weather_date, city_name
),

avg_weather_at_night as (
    select
        weather_date,
        round(avg(main_temp),0) as night_avg_temp,
        min(min_temp) as night_min_temp,
        max(max_temp) as night_max_temp,
        avg(main_pressure) as night_avg_pressure,
        avg(main_humidity) as night_avg_humidity,
        avg(wind_speed_kmh) as night_avg_wind_speed_kmh,
        sum(COALESCE(rain_for_last_three_hours, 0)) as night_total_rain_mm,
        city_name
    from weather
    where 
		day_night_indicator ='n'

    group by weather_date, city_name
),

final as (
    select
        a.weather_date,
        a.night_avg_temp,
        a.night_min_temp,
        a.night_max_temp,
        a.night_avg_pressure,
        a.night_avg_humidity,
        a.night_avg_wind_speed_kmh,
        a.night_total_rain_mm,
        b.city_name,
        b.weather_main_night_summary
    from avg_weather_at_night a
    left join weather_night_final_desc b
    on a.weather_date = b.weather_date
    and a.city_name = b.city_name
)

select * from final
