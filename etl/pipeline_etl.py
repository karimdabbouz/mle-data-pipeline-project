import pandas as pd
import os
from google.cloud import storage
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector



@task(name="Load Data Task",
    retries=3,
    retry_delay_seconds=60,
    log_prints=True)
def load_data_from_gcs(bucket, key_path):
    dfs = []
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    for month in range(1, 4):
        gcs_path = f'green_taxi/green_tripdata_2021-{month:02d}.parquet'
        temp_path = f'./data/{month:02d}.parquet'
        blob = bucket.blob(gcs_path)
        content = blob.download_to_filename(temp_path)
        df = pd.read_parquet(temp_path)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


@task(name="Transform Revenue Task",
    retries=3,
    retry_delay_seconds=60,
    log_prints=True)
def transform_revenue_per_day(df):
    columns_to_sum_up = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime']).dt.date
    df_grouped = df.groupby('lpep_dropoff_datetime')[columns_to_sum_up].sum().reset_index()
    return df_grouped


@task(name="Upload Revenue Data",
    retries=3,
    retry_delay_seconds=60,
    log_prints=True)
def upload_revenue_data(df):
    database_block = SqlAlchemyConnector.load("ny-taxi-connector")
    with database_block.get_connection(begin=False) as engine:
        # engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')
        df.to_sql('revenue_per_day', engine, if_exists='replace', index=False)


@flow(name="ETL Pipeline Flow")
def etl_pipeline_flow(bucket, key_path):
    df_source = load_data_from_gcs(bucket, key_path)
    df_revenue_per_day = transform_revenue_per_day(df_source)
    upload_revenue_data(df_revenue_per_day, )


if __name__ == '__main__':
    bucket = 'mle-batch-and-stream-processing-bucket'
    key_path = './service_account_key.json'
    etl_pipeline_flow(bucket, key_path)