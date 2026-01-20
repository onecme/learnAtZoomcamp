#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import time
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click


prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

CSV_URL = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/"
    "download/yellow/yellow_tripdata_2021-01.csv.gz"
)

# CLI
@click.command()
@click.option('--user', default='root')
@click.option('--password', default='root')
@click.option('--host', default='localhost')
@click.option('--port', default=5432, type=int)
@click.option('--db', default='ny_taxi')
@click.option('--table', default='yellow_taxi_data')

def inges_data(user, password, host, port, db, table):
    print("INGEST SCRIPT STARTED")
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}'
    )

    df_inter = pd.read_csv(
        CSV_URL,
        iterator=True,
        chunksize=100_000,
        dtype=dtype,
        parse_dates=parse_dates
    )

    # first chunk
    df = next(df_inter)

    df.head(0).to_sql(
        name=table,
        con=engine,
        if_exists='replace'
    )

    df.to_sql(
        name=table,
        con=engine,
        if_exists='append'
    )
    print(f"Inserted {len(df)} rows")

    for df in tqdm(df_inter):
        df.to_sql(
            name=table,
            con=engine,
            if_exists='append'
        )
        print(f"Inserted {len(df)} rows")
    print("Ingestion completed")

if __name__ == "__main__":
    inges_data()