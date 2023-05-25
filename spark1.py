from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType 

#Create Spark Session
spark = SparkSession.builder.appName('taxi').enableHiveSupport().getOrCreate()

#Load CSV
path = 'hdfs://localhost:9000/user/fadlil/staging'
df = spark.read.parquet(path)

# Drop table ehail_fee because all values ar null
df = df.drop("ehail_fee")

# Create SKVendor and VendorName based on VendorID
df = df.withColumn('VendorName', when(df.VendorID == 1, 'Creative Mobile Technologies')\
                    .when(df.VendorID == 2, 'VeriFone Inc')\
                    .otherwise('Unknown'))
df = df.withColumn('SKVendor', when(df.VendorID == 1, 1)\
                    .when(df.VendorID == 2, 2)\
                    .otherwise(3))

# Create SKFlag and FlagDesc based on store_and_fwd_flag
df = df.withColumn('FlagDesc', when(df.store_and_fwd_flag == "Y", 'store and forward trip')\
                    .when(df.store_and_fwd_flag == "N", 'not a store and forward trip')\
                    .otherwise('Unknown'))
df = df.withColumn('SKFlag', when(df.store_and_fwd_flag == "Y", 1)\
                    .when(df.store_and_fwd_flag == "N", 2)\
                    .otherwise(3))

# Create SKRatecode and RatecodeType based on RatecodeID
df = df.withColumn('RatecodeType', when(df.RatecodeID == 1, 'Standard rate')\
                    .when(df.RatecodeID == 2, 'JFK')\
                    .when(df.RatecodeID == 3, 'Newark')\
                    .when(df.RatecodeID == 4, 'Nassau or Westchester')\
                    .when(df.RatecodeID == 5, 'Negotiated fare')\
                    .when(df.RatecodeID == 6, 'Group Ride')\
                    .otherwise('Unknown'))
df = df.withColumn('SKRatecode', when(df.RatecodeID == 1, 1)\
                    .when(df.RatecodeID == 2, 2)\
                    .when(df.RatecodeID == 3, 3)\
                    .when(df.RatecodeID == 4, 4)\
                    .when(df.RatecodeID == 5, 5)\
                    .when(df.RatecodeID == 6, 6)\
                    .otherwise(7))

# Create SKPayment and PaymentDetails based on payment_type
df = df.withColumn('PaymentDetails', when(df.payment_type == 1, 'Credit Card')\
                    .when(df.payment_type == 2, 'Cash')\
                    .when(df.payment_type == 3, 'No Charge')\
                    .when(df.payment_type == 4, 'Dispute')\
                    .otherwise('Unknown'))
df = df.withColumn('SKPaymentType', when(df.payment_type == 1, 1)\
                    .when(df.payment_type == 2, 2)\
                    .when(df.payment_type == 3, 3)\
                    .when(df.payment_type == 4, 4)\
                    .otherwise(5))

# Create SKTrip and TripDetails based on trip_type
df = df.withColumn('TripDetails', when(df.trip_type == 1, 'Street-hail')\
                    .when(df.trip_type == 2, 'Dispatch')\
                    .otherwise('Unknown'))
df = df.withColumn('SKTripType', when(df.trip_type == 1, 1)\
                    .when(df.trip_type == 2, 2)\
                    .otherwise(3))

# Separate dataframe into fact_tables and dimension tables
dim_vendor = df.selectExpr('SKVendor', 'VendorID', 'VendorName').distinct().orderBy('SKVendor')
dim_flag = df.selectExpr('SKFlag', 'store_and_fwd_flag', 'FlagDesc').distinct().orderBy('SKFlag')
dim_ratecode = df.selectExpr('SKRatecode', 'RatecodeID', 'RatecodeType').distinct().orderBy('SKRatecode').dropna()
dim_payment_type = df.selectExpr('SKPaymentType', 'payment_type', 'PaymentDetails').distinct().orderBy('SKPaymentType').dropna()
dim_trip_type = df.selectExpr('SKTripType', 'trip_type', 'TripDetails').distinct().orderBy('SKTripType')
fact_trips = df.selectExpr('lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID',\
                            'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',\
                                  'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge',\
                                      'SKVendor', 'SKFlag', 'SKRatecode', 'SKPaymentType', 'SKTripType')\
                                        .orderBy(['lpep_pickup_datetime', 'lpep_dropoff_datetime'])

# Save to HDFS
save_path = 'hdfs://localhost:9000/user/fadlil/final/'
dim_vendor.coalesce(1).write.mode('overwrite').parquet(save_path + 'dim_vendor')
dim_flag.coalesce(1).write.mode('overwrite').parquet(save_path + 'dim_flag')
dim_ratecode.coalesce(1).write.mode('overwrite').parquet(save_path + 'dim_ratecode')
dim_payment_type.coalesce(1).write.mode('overwrite').parquet(save_path + 'dim_payment_type')
dim_trip_type.coalesce(1).write.mode('overwrite').parquet(save_path + 'dim_trip_type')
fact_trips.coalesce(1).write.mode('overwrite').parquet(save_path + 'fact_trips')

