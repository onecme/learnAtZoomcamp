"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
import requests
import io

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    start_dt = datetime.strptime(start_date[:10], "%Y-%m-%d")
    end_dt = datetime.strptime(end_date[:10], "%Y-%m-%d")

    months = []
    current = start_dt.replace(day=1)
    while current <= end_dt:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    dfs = []
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    for taxi_type in taxi_types:
        for year, month in months:
            url = f"{base_url}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            print(f"Fetching: {url}")

            response = requests.get(url)
            if response.status_code != 200:
                print(f"Skipping {url} — status {response.status_code}")
                continue

            # Baca parquet dan langsung convert timestamp ke naive datetime
            df = pd.read_parquet(
                io.BytesIO(response.content),
                dtype_backend="numpy_nullable",
            )

            # Paksa semua kolom datetime jadi timezone-naive
            for col in df.columns:
                if hasattr(df[col], "dt") and hasattr(df[col].dt, "tz") and df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)

            df["taxi_type"] = taxi_type
            df = df.rename(columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
            })

            dfs.append(df)

    final_dataframe = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    return final_dataframe