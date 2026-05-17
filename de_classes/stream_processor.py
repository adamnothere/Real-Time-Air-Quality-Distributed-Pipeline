#Author: Toh Ming Yang

from time import sleep
from pyspark.sql import Window, functions as F

class Streaming:
    def item_status_query(streaming, spark):
        itemStatusStats = (
            streaming
            .filter(F.col("Average_Value").isNotNull())
            .groupBy("Item_Name", "Status")
            .agg(
                F.count("*").alias("Count"),
                F.avg("Average_Value").alias("avg_value"),
                F.min("Average_Value").alias("min_value"),
                F.max("Average_Value").alias("max_value")
            )
        )
        
        itemStatusQuery = (
            itemStatusStats.writeStream
            .format("memory")
            .outputMode("complete")
            .option("truncate", False)
            .queryName("item_status_stats")
            .start()
        )
        sleep(5)
        for i in range(3):
            Streaming.show_item_status_pretty(spark)
            sleep(3)

    def show_item_status_pretty(spark):
        stats = spark.table("item_status_stats")
    
        ordered = (
            stats.withColumn(
                "status_order",
                F.expr("""CASE Status
                            WHEN 'Good' THEN 1
                            WHEN 'Normal' THEN 2
                            WHEN 'Bad' THEN 3
                            WHEN 'Very Bad' THEN 4
                            ELSE 99
                          END""")
            )
        )
    
        w = Window.partitionBy("Item_Name").orderBy("status_order")
        pretty = (
            ordered
            .withColumn("rn", F.row_number().over(w))
            .withColumn("Item", F.when(F.col("rn") == 1, F.col("Item_Name")).otherwise(F.lit("")))
            .orderBy("Item_Name", "status_order")
            .select(
                "Item",
                F.col("Status").alias("Status"),
                "Count",
                F.round(F.col("avg_value"), 3).alias("Average"),
                F.col("min_value").alias("Min_Value"),
                F.col("max_value").alias("Max_Value"),
            )
        )
    
        pretty.show(truncate=False)

    def bad_records_query(streaming, spark):
        badRecords = (
            streaming
            .filter(F.col("Status") == "Bad")
            .withColumn("Measurement_Ts", F.to_timestamp("Measurement_Date"))
            .withColumn("Hour", F.date_trunc("hour", F.col("Measurement_Ts")))
            .select(
                "Hour",
                "Item_Code",
                "Item_Name",
                "Station_Name",
                F.col("Average_Value").alias("Value")
            )
        )

        q = (
            badRecords.writeStream
            .format("memory")
            .outputMode("append")
            .option("truncate", False)
            .queryName("bad_records_raw")
            .start()
        )

        sleep(3)
        for _ in range(3):
            Streaming.show_bad_pretty(spark)
            sleep(2)

        return q

    def bad_records_query(streaming, spark):
        badRecords = (
            streaming
            .filter(F.col("Status") == "Bad")
            .select(
                F.to_timestamp(F.col("Measurement_Date")).alias("Measurement_Date"),
                F.col("Item_Code"),
                F.col("Item_Name"),
                F.col("Station_Name"),
                F.col("Average_Value").alias("Value"),
                F.col("Status")
            )
        )
        
        badRecordsQuery = (
            badRecords.writeStream
            .format("memory")
            .outputMode("append")
            .option("truncate", False)
            .queryName("only_bad_records")
            .start()
        )
        sleep(2)
        for i in range(3):
            df = spark.sql("SELECT * FROM only_bad_records ORDER BY Measurement_Date")
            df.show()
            
            row_count = df.count()
            print(f"Row count: {row_count}")
            sleep(2)

