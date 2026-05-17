#Author: Adam Ho Swee En

from pyspark.sql import SparkSession

class SparkFactory:
    def __init__(self, app_name: str):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def get(self) -> SparkSession:
        return self.spark
