DROP TABLE IF EXISTS weather;
CREATE TABLE weather (
    date_time TIMESTAMP WITHOUT TIME ZONE,
    city_name VARCHAR(100),
    avg_temp FLOAT,
    wind_speed FLOAT,
    humidity FLOAT
);

DROP TABLE IF EXISTS energy;
CREATE TABLE energy (
	date_time TIMESTAMP WITHOUT TIME ZONE,
	generation_biomass FLOAT,
	generation_hydro_pumped_storage_consumption FLOAT,
	generation_hydro_run_of_river_and_poundage FLOAT,
    generation_hydro_water_reservoir FLOAT,
	generation_other_renewable FLOAT,
    generation_solar FLOAT,
    generation_wind_onshore FLOAT,
	total_renewable_generation FLOAT,
    forecast_solar_day_ahead FLOAT,
    forecast_wind_onshore_day_ahead FLOAT,
    total_load_forecast FLOAT,
	total_load_actual FLOAT,
	price_day_ahead FLOAT,
    price_actual FLOAT,
    price_difference FLOAT,
    price_lag_1d FLOAT, 
    price_lag_7d FLOAT,
    price_lag_30d FLOAT
);

CREATE INDEX idx_weather_date ON weather(date_time);
CREATE INDEX idx_energy_date ON energy(date_time);