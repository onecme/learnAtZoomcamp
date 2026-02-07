-- SELECT THE COLUMNS INTERESTED FOR YOU
SELECT
  passenger_count, 
  trip_distance,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  tolls_amount,
  tip_amount
FROM `zoomcamp-3.ny_taxi.green_tripdata_partitioned`

WHERE fare_amount != 0;
  

-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `zoomcamp-3.ny_taxi.green_tripdata_ml`(
  passenger_count INT64, 
  trip_distance FLOAT64,
  PULocationID STRING,
  DOLocationID STRING,
  payment_type STRING,
  fare_amount FLOAT64,
  tolls_amount FLOAT64,
  tip_amount FLOAT64
)AS (
  SELECT
    CAST(passenger_count AS INT64) AS passenger_count,
    trip_distance,
    CAST(PULocationID AS STRING) AS PULocationID,
    CAST(DOLocationID AS STRING) AS DOLocationID,
    CAST(payment_type AS STRING) AS Payment_type,
    fare_amount,
    tolls_amount,
    tip_amount
  FROM `zoomcamp-3.ny_taxi.green_tripdata_partitioned`
  WHERE fare_amount != 0
);
  

-- CREATE MODEL WITH DEFAULT SETTING
CREATE  OR REPLACE MODEL `zoomcamp-3.ny_taxi.tip_model`
OPTIONS(
  model_type = 'linear_reg',
  input_label_cols = ['tip_amount'],
  data_split_method = 'AUTO_SPLIT'
) AS
SELECT
*
FROM `zoomcamp-3.ny_taxi.green_tripdata_ml`
WHERE tip_amount IS NOT NULL;


-- CHECK FEATURES
SELECT *
FROM ML.FEATURE_INFO(
  MODEL `zoomcamp-3.ny_taxi.tip_model`
);


-- EVALUATE THE MODEL
SELECT *
FROM ML.EVALUATE(
  MODEL `zoomcamp-3.ny_taxi.tip_model`,
  (
    SELECT *
    FROM `zoomcamp-3.ny_taxi.green_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  )
);


-- PREDICT THE MODEL
SELECT *
FROM ML.PREDICT(
  MODEL `zoomcamp-3.ny_taxi.tip_model`,
  (
    SELECT *
    FROM `zoomcamp-3.ny_taxi.green_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  )
);


-- PREDICT AND EXPLAIN
SELECT *
FROM ML.EXPLAIN_PREDICT(
  MODEL `zoomcamp-3.ny_taxi.tip_model`,
  (
    SELECT *
    FROM `zoomcamp-3.ny_taxi.green_tripdata_ml`
    WHERE tip_amount IS NOT NULL
  ),
  STRUCT(3 AS top_k_features)
);


-- HYPER PARAM TUNNING
CREATE  OR REPLACE MODEL `zoomcamp-3.ny_taxi.tip_model`
OPTIONS(
  model_type = 'linear_reg',
  input_label_cols = ['tip_amount'],
  data_split_method = 'AUTO_SPLIT',

  -- Hyperparamater tuning
  num_trials = 5,
  max_parallel_trials =2,
  l1_reg = hparam_range(0, 20),
  l2_reg = hparam_candidates([0, 0.1, 1, 10])
) AS
SELECT
*
FROM `zoomcamp-3.ny_taxi.green_tripdata_ml`
WHERE tip_amount IS NOT NULL;