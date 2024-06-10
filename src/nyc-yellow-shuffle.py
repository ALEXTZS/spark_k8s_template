"""
Spark App: nyc-yellow-shuffle
Author: Luan Moreno

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/nyc-yellow-shuffle.py
"""

from sparkmeasure import StageMetrics
from pyspark.sql.functions import current_date, udf, col, unix_timestamp
from pyspark.sql.types import StringType
from utils.utils import init_spark_session, list_files


def main():

    # TODO init session
    spark = init_spark_session("nyc-yellow-shuffle")

    # TODO init spark metrics
    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    # TODO read parquet files
    file_yellow = "./storage/yellow/*.parquet"
    list_files(spark, file_yellow)
    df_yellow = spark.read.parquet(file_yellow)









    file_fhvhv = "./storage/fhvhv/2022/*.parquet"


    print(f"number of partitions: {df_fhvhv.rdd.getNumPartitions()}")

    file_zones = "./storage/zones.csv"
    list_files(spark, file_zones)
    df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)
    print(f"number of rows: {df_fhvhv.count()}")

    udf_license_num = udf(license_num, StringType())
    spark.udf.register("license_num", udf_license_num)
    df_fhvhv = df_fhvhv.withColumn('hvfhs_license_num', udf_license_num(df_fhvhv['hvfhs_license_num']))

    df_fhvhv.createOrReplaceTempView("hvfhs")
    df_zones.createOrReplaceTempView("zones")

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
               tips,
               driver_pay,
               shared_request_flag,
               shared_match_flag
        FROM hvfhs
        INNER JOIN zones AS zones_pu
        ON CAST(hvfhs.PULocationID AS INT) = zones_pu.LocationID
        INNER JOIN zones AS zones_do
        ON hvfhs.DOLocationID = zones_do.LocationID
        ORDER BY request_datetime DESC
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

    storage = "./storage/rides/parquet/"
    df_rides.write.mode("append").partitionBy("ingestion_timestamp").parquet(storage + "rides")
    df_total_trip_time.write.mode("append").partitionBy("ingestion_timestamp").parquet(storage + "total_trip_time")
    df_hvfhs_license_num.write.mode("append").partitionBy("hvfhs_license_num").parquet(storage + "hvfhs_license_num")

    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

    metrics = "./metrics/elt-rides-fhvhv-py-strawberry/"
    df_stage_metrics = stage_metrics.create_stagemetrics_DF("PerfStageMetrics")
    df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

    df_aggregated_metrics = stage_metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")


if __name__ == "__main__":
    main()
