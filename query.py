create_database_query = """
CREATE DATABASE IF NOT EXISTS staging;
"""

create_database_query2 = """
CREATE DATABASE IF NOT EXISTS dwh;
"""

create_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS staging.green_taxi (
            VendorID BIGINT,
            lpep_pickup_datetime BIGINT,
            lpep_dropoff_datetime BIGINT,
            store_and_fwd_flag STRING,
            RatecodeID DOUBLE,
            PULocationID BIGINT,
            DOLocationID BIGINT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type DOUBLE,
            trip_type DOUBLE,
            congestion_surcharge DOUBLE
        )
        STORED AS PARQUET LOCATION '/user/fadlil/staging' 
	tblproperties ("skip.header.line.count"="1")
"""

insert_data_query = """
LOAD DATA LOCAL INPATH '/home/fadlil/green_taxi/parquet/*.parquet'
OVERWRITE INTO TABLE staging.green_taxi;
"""

create_table_dim1_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS dwh.dim_vendor (
SKVendor INTEGER,
VendorID BIGINT,
VendorName STRING
)
STORED AS PARQUET;
"""

insert_table_dim1_query = """
LOAD DATA INPATH '/user/fadlil/final/dim_vendor/*.parquet'
OVERWRITE INTO TABLE dwh.dim_vendor;
"""

create_table_dim2_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS dwh.dim_flag (
SKFlag INTEGER,
store_and_fwd_flag STRING,
FlagDesc STRING
)
STORED AS PARQUET;
"""

insert_table_dim2_query = """
LOAD DATA INPATH '/user/fadlil/final/dim_flag/*.parquet'
OVERWRITE INTO TABLE dwh.dim_flag;
"""

create_table_dim3_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS dwh.dim_ratecode (
SKRatecode INTEGER,
RatecodeID DOUBLE,
RatecodeType STRING
)
STORED AS PARQUET;
"""

insert_table_dim3_query = """
LOAD DATA INPATH '/user/fadlil/final/dim_ratecode/*.parquet'
OVERWRITE INTO TABLE dwh.dim_ratecode;
"""

create_table_dim4_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS dwh.dim_trip_type (
SKTripType INTEGER,
trip_type DOUBLE,
TripDetails STRING
)
STORED AS PARQUET;
"""

insert_table_dim4_query = """
LOAD DATA INPATH '/user/fadlil/final/dim_trip_type/*.parquet'
OVERWRITE INTO TABLE dwh.dim_trip_type;
"""

create_table_dim5_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS dwh.dim_payment_type (
SKPaymentType INTEGER,
payment_type DOUBLE,
PaymentDetails STRING
)
STORED AS PARQUET;
"""

insert_table_dim5_query = """
LOAD DATA INPATH '/user/fadlil/final/dim_payment_type/*.parquet'
OVERWRITE INTO TABLE dwh.dim_payment_type;
"""

create_table_fact_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS dwh.fact_green_taxi_trips (
lpep_pickup_datetime TIMESTAMP,
lpep_dropoff_datetime TIMESTAMP,
PULocationID BIGINT,
DOLocationID BIGINT,
passenger_count DOUBLE,
trip_distance DOUBLE,
fare_amount DOUBLE,
extra DOUBLE,
mta_tax DOUBLE,
tip_amount DOUBLE,
tolls_amount DOUBLE,
improvement_surcharge DOUBLE,
total_amount DOUBLE,
congestion_surcharge DOUBLE,
SKVendor INTEGER,
SKFlag INTEGER,
SKRatecode INTEGER,
SKPaymentType INTEGER,
SKTripType INTEGER
)
STORED AS PARQUET;
"""

insert_table_fact_query = """
LOAD DATA INPATH '/user/fadlil/final/fact_trips/*.parquet'
OVERWRITE INTO TABLE dwh.fact_green_taxi_trips;
"""