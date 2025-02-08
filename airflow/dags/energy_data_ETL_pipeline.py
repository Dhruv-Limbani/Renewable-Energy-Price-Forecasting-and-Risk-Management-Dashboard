from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from datetime import datetime
import logging

BASE_PATH = '/opt/airflow/data/Renewable_Energy_Market_Analysis/'
RAW_PATH = BASE_PATH + 'data/'
TMP_PATH = BASE_PATH + 'tmp/'

weather_file = RAW_PATH + 'weather_features.csv'
energy_file = RAW_PATH + 'energy_dataset.csv'
weather_transformed_file = TMP_PATH + 'weather_transformed.csv'
energy_transformed_file = TMP_PATH + 'energy_transformed.csv'

def extract_and_save_csv(file_path, save_path):
    try:
        df = pd.read_csv(file_path, skipinitialspace = True)
        df.to_csv(save_path, index=False)
        logging.info(f"Extracted {df.shape[0]} rows from {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error extracting {file_path}: {e}")
        raise

def extract_weather_data():
    return extract_and_save_csv(weather_file, weather_transformed_file)

def extract_energy_data():
    return extract_and_save_csv(energy_file, energy_transformed_file)

def remove_timezone(dt_str):
    dt = pd.to_datetime(dt_str)
    return dt.tz_localize(None)

def transform_weather_data(**kwargs):
    try:
        weather_df = pd.read_csv(weather_transformed_file)
        weather_df.fillna(method='ffill', inplace=True)
        weather_df['dt_iso'] = weather_df['dt_iso'].apply(remove_timezone)
        weather_df.drop_duplicates(['dt_iso', 'city_name'], inplace=True)
        weather_df['avg_temp'] = (weather_df['temp_min'] + weather_df['temp_max']) / 2
        weather_df = weather_df[['dt_iso', 'city_name', 'avg_temp', 'wind_speed', 'humidity']]
        weather_df.rename(columns={'dt_iso': 'date_time'}, inplace=True)
        weather_df.to_csv(weather_transformed_file, index=False)    
    except Exception as e:
        logging.error(f"Error transforming weather data: {e}")
        raise


def transform_energy_data(**kwargs):
    try:
        energy_df = pd.read_csv(energy_transformed_file)
        energy_df.fillna(method='ffill', inplace=True)
        energy_df['time'] = energy_df['time'].apply(remove_timezone)
        energy_df.drop_duplicates('time', inplace=True)
        energy_df['price lag 1d'] = energy_df['price actual'].shift(1)
        energy_df['price lag 7d'] = energy_df['price actual'].shift(7)
        energy_df['price lag 30d'] = energy_df['price actual'].shift(30)
        energy_df[['price lag 1d', 'price lag 7d', 'price lag 30d']] = energy_df[['price lag 1d', 'price lag 7d', 'price lag 30d']].fillna(method='bfill')
        energy_df['price difference'] = energy_df['price actual'] - energy_df['price day ahead']
        renewable_sources = ['generation solar', 'generation wind onshore', 'generation biomass', 
                            'generation hydro pumped storage consumption', 'generation hydro run-of-river and poundage',
                            'generation hydro water reservoir', 'generation other renewable']
        energy_df['total renewable generation'] = energy_df[renewable_sources].sum(axis=1)

        energy_df = energy_df[['time', 'generation biomass', 'generation hydro pumped storage consumption', 
                                'generation hydro run-of-river and poundage','generation hydro water reservoir',
                                'generation other renewable', 'generation solar', 'generation wind onshore', 'total renewable generation',
                                'forecast solar day ahead','forecast wind onshore day ahead','total load forecast',
                                'total load actual','price day ahead','price actual','price difference', 'price lag 1d', 'price lag 7d', 'price lag 30d']]
        energy_df.rename(columns={'time': 'date_time', 'price lag 1d': 'price_lag_1d', 'price lag 7d': 'price_lag_7d',
                                    'price lag 30d': 'price_lag_30d'}, inplace=True)
        energy_df.to_csv(energy_transformed_file, index=False)
    except Exception as e:
        logging.error(f"Error transforming energy data: {e}")
        raise

def load_to_postgresql(file_path, table_name):
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = pg_hook.get_sqlalchemy_engine()
        df = pd.read_csv(file_path)
        if 'date_time' in df.columns:
            df['date_time'] = pd.to_datetime(df['date_time'])
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Successfully loaded {table_name} into PostgreSQL.")
    except Exception as e:
        logging.error(f"Error loading {table_name} into PostgreSQL: {e}")
        raise

# def create_weather_aggregates_view():
#     try:
#         pg_hook = PostgresHook(postgres_conn_id='postgres_default')
#         conn = pg_hook.get_conn()
#         cursor = conn.cursor()
#         view_sql = """
#         CREATE OR REPLACE VIEW weather_aggregates AS
#         SELECT 
#             date_time,
#             AVG(avg_temp) AS avg_temp,
#             MAX(avg_temp) AS max_temp,
#             STDDEV(avg_temp) AS std_temp,
#             AVG(wind_speed) AS avg_wind_speed,
#             MAX(wind_speed) AS max_wind_speed,
#             STDDEV(wind_speed) AS std_wind_speed,
#             AVG(humidity) AS avg_humidity,
#             MAX(humidity) AS max_humidity,
#             STDDEV(humidity) AS std_humidity
#         FROM 
#             weather
#         GROUP BY 
#             date_time;
#         """
        
#         cursor.execute(view_sql)
#         conn.commit()
#         logging.info("Successfully created or updated the weather_aggregates view in PostgreSQL.")
#     except Exception as e:
#         logging.error(f"Error creating or updating the weather_aggregates view: {e}")
#         raise


def load_to_postgresql_weather():
    load_to_postgresql(weather_transformed_file, 'weather')

def load_to_postgresql_energy():
    load_to_postgresql(energy_transformed_file, 'energy')

dag = DAG(
    'ETL_pipeline',
    description='ETL Pipeline for Energy and Weather Data',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

with dag:

    with TaskGroup("extract_tasks") as extract_tasks:
        extract_weather = PythonOperator(
            task_id='extract_weather',
            python_callable=extract_weather_data
        )
        extract_energy = PythonOperator(
            task_id='extract_energy',
            python_callable=extract_energy_data
        )

    with TaskGroup("transform_tasks") as transform_tasks:
        transform_weather = PythonOperator(
            task_id='transform_weather',
            python_callable=transform_weather_data
        )
        transform_energy = PythonOperator(
            task_id='transform_energy',
            python_callable=transform_energy_data
        )

    with TaskGroup("load_tasks") as load_tasks:
        load_weather = PythonOperator(
            task_id='load_weather',
            python_callable=load_to_postgresql_weather
        )
        load_energy = PythonOperator(
            task_id='load_energy',
            python_callable=load_to_postgresql_energy
        )

    # with TaskGroup("create_views") as create_views:
    #     create_weather_agg_view = PythonOperator(
    #         task_id = 'create_weather_agg_view',
    #         python_callable=create_weather_aggregates_view
    #     )

    extract_tasks >> transform_tasks >> load_tasks
# host.docker.internal