drop table if exists energy_weather_data;
with weather_agg as(
	select 
		date_time,
		avg(avg_temp) as avg_temp,
		avg(wind_speed) as avg_wind_speed,
		avg(humidity) as avg_humidity
	from
		weather
	group by
		date_time
)
select
	e.date_time,
	generation_biomass,
	generation_hydro_pumped_storage_consumption,
	generation_hydro_run_of_river_and_poundage,
	generation_hydro_water_reservoir,
	generation_other_renewable,
	generation_solar,
	generation_wind_onshore,
	total_renewable_generation,
	forecast_solar_day_ahead,
	forecast_wind_onshore_day_ahead,
	total_load_forecast,
	total_load_actual,
	price_day_ahead,
	price_actual,
	price_difference,
	price_lag_1d,
	price_lag_7d,
	price_lag_30d,
	avg_temp,
	avg_wind_speed,
	avg_humidity
into
	energy_weather_data
from
	energy as e
join
	weather_agg as wa
on
	e.date_time = wa.date_time;