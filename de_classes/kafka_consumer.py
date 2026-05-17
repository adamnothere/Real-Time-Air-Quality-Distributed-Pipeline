# Author: Darren Ong

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from typing import Optional, List
import csv
import os
import re
import traceback

class KafkaToHDFS:
    def __init__(
        self,
        topic: str,
        checkpoint_dir: str,
        hdfs_path: str,
        kafka_server: str = "localhost:9092",
        source_csv_for_header: Optional[str] = None,
        output_format: str = "parquet",    
        starting_offsets: str = "earliest",
        sanitize_names: bool = False,       
    ):
        self.topic = topic
        self.kafka_server = kafka_server
        self.checkpoint_dir = checkpoint_dir
        self.source_csv_for_header = source_csv_for_header
        self.hdfs_path = hdfs_path
        self.output_format = output_format.lower()
        self.starting_offsets = starting_offsets
        self.sanitize_names = sanitize_names

        if self.output_format not in ("parquet", "csv"):
            raise ValueError("output_format must be 'parquet' or 'csv'")

        self.spark = (
            SparkSession.builder
            .appName("KafkaToHDFS")
            .master("local[2]")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")

        self.schema = self._build_all_string_schema()

    def _build_all_string_schema(self) -> StructType:
        if self.source_csv_for_header and os.path.exists(self.source_csv_for_header):
            with open(self.source_csv_for_header, "r", encoding="utf-8-sig") as f:
                header = next(csv.reader(f), None)
            if not header:
                raise ValueError(f"No header row found in {self.source_csv_for_header}")
            return StructType([StructField(h.strip(), StringType(), True) for h in header])
        return StructType([])  


    def _sanitize_columns(self, df):
        out = df
        for c in df.columns:
            safe = re.sub(r'[^A-Za-z0-9_]', '_', c)
            if safe != c:
                out = out.withColumnRenamed(c, safe)
        return out

    def get_streaming_df(self):
        kafka_raw = (self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_server)
            .option("subscribe", self.topic)
            .option("startingOffsets", self.starting_offsets)
            .option("failOnDataLoss", "false") 
            .load())
    
        value_str = kafka_raw.select(col("value").cast("string").alias("raw_value"))
    
        if not self.schema.fields:
            raise ValueError("No schema. Provide source_csv_for_header so columns match your header.")
    
        parsed = value_str.select(from_json(col("raw_value"), self.schema).alias("data"))
        return parsed.select(*[col(f"data.`{f.name}`").alias(f.name) for f in self.schema.fields])

    def start(self):
        df = self.get_streaming_df()

        def _sink(batch_df, batch_id: int):
            try:
                cnt = batch_df.count()
                print(f"[Streaming] Batch {batch_id}: {cnt} new rows appended")
                if cnt == 0:
                    return

                out_df = self._sanitize_columns(batch_df) if self.sanitize_names else batch_df

                writer = out_df.write.mode("append")
                if self.output_format == "csv":
                    writer.option("header", "true").csv(self.hdfs_path)
                else:
                    writer.parquet(self.hdfs_path)

            except Exception:
                print(f"[Streaming] Batch {batch_id}: ERROR during sink\n{traceback.format_exc()}")

        q = (
            df.writeStream
            .foreachBatch(_sink)
            .outputMode("append")
            .option("checkpointLocation", os.path.join(self.checkpoint_dir, f"hdfs_{self.output_format}"))
            .start()
        )
        return q


if __name__ == "__main__":
    consumer = KafkaToHDFS(
        topic="pollution-data",
        checkpoint_dir="./chk/pollution-data",           
        hdfs_path="hdfs://localhost:9000/user/student/pollution_parquet",
        kafka_server="localhost:9092",
        source_csv_for_header = "/home/student/de-assignment/de_data/Measurement_info.csv",
        output_format="parquet",
        starting_offsets="earliest",
        sanitize_names=False
    )


    query = consumer.start()
    print("Streaming started. Writing to HDFS. Press Ctrl+C to stop.")
    query.awaitTermination()