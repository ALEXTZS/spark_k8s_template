# TODO change partition size
# TODO stage_metrics.print_memory_report()

"""
PySpark: elt-rides-fhvhv-py-strawberry-owshq
Author: Luan Moreno

executing job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-py-strawberry-owshq.py
"""

from pyspark.sql.functions import current_date, col, unix_timestamp

from utils.utils import init_spark_session, list_files
from utils.transformers import hvfhs_license_num


def main():

    spark = init_spark_session("elt-rides-fhvhv-py-strawberry-owshq")

    file_fhvhv = "./storage/fhvhv/2022/*.parquet"
    list_files(spark, file_fhvhv)

    # TODO: select columns to reduce footprint.
    fhvhv_cols = [
        "hvfhs_license_num", "PULocationID", "DOLocationID",
        "request_datetime", "pickup_datetime", "dropoff_datetime",
        "trip_miles", "trip_time", "base_passenger_fare", "tolls",
        "bcf", "sales_tax", "congestion_surcharge", "tips"
    ]
    df_fhvhv = spark.read.parquet(file_fhvhv).select(*fhvhv_cols)
    print(f"number of partitions: {df_fhvhv.rdd.getNumPartitions()}")

    file_zones = "./storage/zones.csv"
    list_files(spark, file_zones)
    df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)
    print(f"number of rows: {df_fhvhv.count()}")

    # TODO: use native spark udf to transform license number.
    df_fhvhv = hvfhs_license_num(df_fhvhv)

    df_fhvhv.createOrReplaceTempView("hvfhs")
    df_zones.createOrReplaceTempView("zones")

    # TODO: remove order by clause.
    df_rides = spark.sql("""
        SELECT hvfhs_license_num,
               zones_pu.Borough AS PU_Borough,
               zones_pu.Zone AS PU_Zone,
               zones_do.Borough AS DO_Borough,
               zones_do.Zone AS DO_Zone,
               request_datetime,
               pickup_datetime,
               dropoff_datetime,
               trip_miles,
               trip_time,
               base_passenger_fare,
               tolls,
               bcf,
               sales_tax,
               congestion_surcharge,
               tips
        FROM hvfhs
        INNER JOIN zones AS zones_pu
        ON CAST(hvfhs.PULocationID AS INT) = zones_pu.LocationID
        INNER JOIN zones AS zones_do
        ON hvfhs.DOLocationID = zones_do.LocationID
    """)

    df_rides = df_rides.withColumn("ingestion_timestamp", current_date())
    df_rides = df_rides.withColumn("time_taken_seconds", unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime")))
    df_rides = df_rides.withColumn("time_taken_minutes", col("time_taken_seconds") / 60)
    df_rides = df_rides.withColumn("time_taken_hours", col("time_taken_seconds") / 3600)

    df_rides.createOrReplaceTempView("rides")

    df_total_trip_time = spark.sql("""
        SELECT 
            ingestion_timestamp,
            PU_Borough,
            PU_Zone,
            DO_Borough,
            DO_Zone,
            SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
            SUM(trip_miles) AS total_trip_miles,
            SUM(trip_time) AS total_trip_time,
            SUM(time_taken_seconds) AS total_time_taken_seconds,
            SUM(time_taken_minutes) AS total_time_taken_minutes,
            SUM(time_taken_hours) AS total_time_taken_hours
        FROM 
            rides
        GROUP BY 
            ingestion_timestamp,
            PU_Borough, 
            PU_Zone,
            DO_Borough,
            DO_Zone
    """)

    df_hvfhs_license_num = spark.sql("""
        SELECT 
            ingestion_timestamp,
            hvfhs_license_num,
            SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
            SUM(trip_miles) AS total_trip_miles,
            SUM(trip_time) AS total_trip_time,
            SUM(time_taken_seconds) AS total_time_taken_seconds,
            SUM(time_taken_minutes) AS total_time_taken_minutes,
            SUM(time_taken_hours) AS total_time_taken_hours
        FROM 
            rides
        GROUP BY 
            ingestion_timestamp,
            hvfhs_license_num
    """)

    # TODO: write in delta lake format.
    storage = "./storage/rides/delta/"
    df_rides.write.format("delta").mode("append").partitionBy("ingestion_timestamp").save(storage + "rides")
    df_total_trip_time.write.format("delta").mode("append").partitionBy("ingestion_timestamp").save(storage + "total_trip_time")
    df_hvfhs_license_num.write.format("delta").mode("append").partitionBy("hvfhs_license_num").save(storage + "hvfhs_license_num")


if __name__ == "__main__":
    main()
