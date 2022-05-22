import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType


dim_columns = [
    'id',
    'name'
]

vendor_rows = [
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'VeriFone Inc'),
]

rates_rows = [
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
]

payment_rows = [
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
]

trips_schema = StructType([
    StructField('vendor_id', StringType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('ratecode_id', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('pulocation_id', IntegerType(), True),
    StructField('dolocation_id', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True)
])


def agg_calc(spark: SparkSession) -> DataFrame:
    data_path = os.path.join(Path(__name__).parent, './yellow_tripdata_2020', '*.csv')

    trip_fact = spark.read \
        .option("header", "true") \
        .schema(trips_schema) \
        .csv(data_path)

    datamart = trip_fact \
        .where((f.to_date(f.col('tpep_pickup_datetime')) >= '2020-01-01') &
               (f.to_date(f.col('tpep_pickup_datetime')) <= '2020-01-31')) \
        .groupBy(f.col('payment_type'),
                 f.to_date(f.col('tpep_pickup_datetime')).alias('date'),
                 ) \
        .agg(f.avg(f.col('total_amount')).alias('average_trip_cost'),
             (f.sum(f.col('total_amount')) / f.sum(f.col('trip_distance'))).alias('average_trip_km_cost')) \
        .select(f.col('payment_type'),
                f.col('date'),
                f.col('average_trip_cost'),
                f.col('average_trip_km_cost')
                )

    return datamart


def create_dict(spark: SparkSession, header, data):
    df = spark.createDataFrame(data=data, schema=header)
    return df


def main(spark: SparkSession):
    payment_dim = create_dict(spark, dim_columns, payment_rows)

    datamart = agg_calc(spark).cache()

    joined_datamart = datamart \
        .join(other=payment_dim, on=payment_dim['id'] == f.col('payment_type'), how='inner') \
        .select(payment_dim['name'].alias('payment_type'),
                f.col('date'),
                f.col('average_trip_cost'),
                f.col('average_trip_km_cost')
                )

    joined_datamart.orderBy(f.col('payment_type').asc(), f.col('date').desc()).coalesce(1) \
        .write.option("header", "true").mode('overwrite').csv('./spark_output')


if __name__ == '__main__':
    main(SparkSession
         .builder
         .appName('my homework spark job')
         .getOrCreate())
