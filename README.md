# WHAT ARE THE STEPS YOU TOOK TO COMPLETE THE PROJECT?

As a preparation I created a script `raw_data_ingestion.py` that loads the raw data into three parquet files in GCS.

## ETL

I started with creating functions to extract, transform and load data in a notebook `data_pipeline.ipynb`. I then refactored them into a prefect pipeline in `/etl/pipeline_etl.py`.

## ELT

I started with initiatlizing a dbt project in the `/elt` folder. First, I tried to use dbt models to load the parquet files directly into a source table in my postgres database, but this didn't work. I then created the `pipeline_elt.py` script which would later become the prefect script and used it to load the parquet files from GCS into a new postgres table `source_data`.

I then continued working with dbt models without orchestrating them with prefect to see if they worked. I don't have an intermedia stage, only a staging and mart. I only load all columns with prices/fares/fees into the staging table using `stg_green_taxi.sql`. I then proceed with `fct_daily_revenue.sql` where I group the table by day and calculate the sums for each day and save it as a table called `fct_daily_revenue`.

Lastly, I orchestrated all steps using prefect with a single script called `pipeline_elt.py`.