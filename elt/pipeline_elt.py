import pandas as pd
import os
from google.cloud import storage
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_dbt.cli.commands import DbtCoreOperation



@task(name="Load Source Data",
    retries=3,
    retry_delay_seconds=60,
    log_prints=True)
def load_from_gcs_to_postgres(bucket, key_path):
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
    result_df = pd.concat(dfs, ignore_index=True)
    database_block = SqlAlchemyConnector.load("ny-taxi-connector")
    with database_block.get_connection(begin=False) as engine:
        result_df.to_sql('source_data', engine, if_exists='replace', index=False)



@task(name="DBT Task", 
    retries=3,
    retry_delay_seconds=60,
    log_prints=True)
def dbt_task():
    result = DbtCoreOperation(
        commands=["pwd", "dbt debug", "dbt run"],
        overwrite_profiles=False,
        # profiles_path="flows/dbt/dbt-intro-prod/profiles.yml",
        profiles_path="./dbt_elt_pipeline/profiles.yml",
        project_dir="./dbt_elt_pipeline"
    ).run()



@flow(name="ELT Pipeline Flow")
def elt_pipeline_flow(bucket, key_path):
    load_from_gcs_to_postgres(bucket, key_path)
    dbt_task()


if __name__ == '__main__':
    bucket = 'mle-batch-and-stream-processing-bucket'
    key_path = './service_account_key.json'