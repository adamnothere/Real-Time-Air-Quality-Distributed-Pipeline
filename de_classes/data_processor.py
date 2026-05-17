#Author: Adam Ho Swee En

from pyspark.sql import functions as F, types as T, DataFrame
from pyspark.sql.dataframe import DataFrame
from typing import Iterable, Optional, Dict, Tuple

MEASURE_DTYPES = {
    "Measurement_Date":  T.TimestampType(),
    "Station_Code":      T.IntegerType(),
    "Item_Code":         T.IntegerType(),
    "Average_Value":     T.DoubleType(),
    "Instrument_Status": T.IntegerType(),
}
STATION_DTYPES = {
    "Station_Code": T.IntegerType(),
    "Station_Name": T.StringType(),
    "Latitude":     T.DoubleType(),
    "Longitude":    T.DoubleType(),
}
ITEM_DTYPES = {
    "Item_Code": T.IntegerType(),
    "Item_Name": T.StringType(),
    "Unit":      T.StringType(),
}

MEASURE_RENAME_MAP = {
    "Measurement date":  "Measurement_Date",
    "Station code":      "Station_Code",
    "Item code":         "Item_Code",
    "Average value":     "Average_Value",
    "Instrument status": "Instrument_Status",
}
STATION_RENAME_MAP = {
    "Station code":             "Station_Code",
    "Station name(district)":   "Station_Name",
    "Latitude":                 "Latitude",
    "Longitude":                "Longitude",
}
ITEM_RENAME_MAP = {
    "Item code":              "Item_Code",
    "Item name":              "Item_Name",
    "Unit of measurement":    "Unit",
}

class DataCleaning:
    @staticmethod
    def recast_columns(df, rename_map, dtypes):
        for old_name, new_name in rename_map.items():
            if old_name in df.columns and old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)
        select_exprs = []
        for col_name, dtype in dtypes.items():
            if col_name in df.columns:
                select_exprs.append(F.col(col_name).cast(dtype).alias(col_name))
            else:
                select_exprs.append(F.lit(None).cast(dtype).alias(col_name))
        extras = [c for c in df.columns if c not in dtypes]
        return df.select(*select_exprs, *[F.col(c) for c in extras])

    def trim_col_str(df):
        str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
        return df.select([
            F.trim(F.col(c)).alias(c) if c in str_cols else F.col(c)
            for c in df.columns
        ])

    @staticmethod
    def clean_station(station_raw):
        df = station_raw.transform(DataCleaning.trim_col_str)
        df = df.na.drop(subset=["Station_Code", "Station_Name", "Latitude", "Longitude"])
        df = df.filter(
            F.col("Latitude").between(-90.0, 90.0) &
            F.col("Longitude").between(-180.0, 180.0)
        )
        return df.dropDuplicates(["Station_Code"])

    @staticmethod
    def clean_item(item_raw):
        df = item_raw.transform(DataCleaning.trim_col_str)
        df = df.na.drop(subset=["Item_Code", "Item_Name", "Unit"])
        return df.dropDuplicates(["Item_Code"])

    @staticmethod
    def clean_measure(
        measure_raw,
        ts_col: str = "Measurement_Date",
        ts_format: str = "yyyy-MM-dd HH:mm",
        bad_instrument_status: Optional[Iterable[int]] = (2, 4, 8, 9),
    ):
        df = measure_raw.transform(DataCleaning.trim_col_str)
        df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col), ts_format))
        df = df.na.drop(subset=["Measurement_Date", "Station_Code", "Item_Code", "Average_Value"])
        df = df.filter(
            (F.col("Average_Value") >= 0) &
            (F.col("Station_Code") > 0) &
            (F.col("Item_Code") > 0)
        )
        if bad_instrument_status:
            df = df.filter(~F.col("Instrument_Status").isin(list(bad_instrument_status)))
        return df.dropDuplicates(["Measurement_Date", "Station_Code", "Item_Code"])

class DataLoading:
    @staticmethod
    def read_csv(spark, data_path: str, schema=None, header: bool = True) -> DataFrame:
        reader = spark.read.option("header", header)
        if schema:
            return reader.schema(schema).csv(data_path)
        else:
            return reader.option("inferSchema", True).csv(data_path)

class DataEnrichment:
    @staticmethod
    def enrich_measure_with_station_item(
        measure_df: DataFrame,
        station_df: DataFrame,
        item_df: DataFrame,
        round_decimals: int = 3
    ):
        enriched = (
            measure_df
            .join(station_df, on="Station_Code", how="left")
            .join(item_df, on="Item_Code", how="left")
        )
        double_cols = [f.name for f in enriched.schema.fields if isinstance(f.dataType, T.DoubleType)]
        for col in double_cols:
            enriched = enriched.withColumn(col, F.round(F.col(col), round_decimals))
        enriched = enriched.withColumn(
            "Status",
            F.when(F.col("Average_Value") <= F.col("Good(Blue)"), "Good")
            .when(F.col("Average_Value") <= F.col("Normal(Green)"), "Normal")
            .when(F.col("Average_Value") <= F.col("Bad(Yellow)"), "Bad")
            .otherwise("Very Bad")
        )
        enriched = enriched.drop("Good(Blue)", "Normal(Green)", "Bad(Yellow)", "Very bad(Red)")
        return enriched

class DataValidation:
    @staticmethod
    def detect_measure_errors(
        measure_raw: DataFrame,
        ts_col: str = "Measurement_Date",
        ts_format: str = "yyyy-MM-dd HH:mm",
        bad_instrument_status: Optional[Iterable[int]] = (2, 4, 8, 9),
    ):
        ts_is_bad = F.to_timestamp(F.col(ts_col), ts_format).isNull()
        reason = (
            F.when(ts_is_bad, "Bad date")
             .when(F.col("Station_Code").isNull() | F.col("Item_Code").isNull() | F.col("Average_Value").isNull(), "Missing values")
             .when(F.col("Average_Value") < 0, "Negative average")
             .when((F.col("Station_Code") <= 0) | (F.col("Item_Code") <= 0), "Invalid codes")
             .when(F.col("Instrument_Status").isin(list(bad_instrument_status)) if bad_instrument_status else F.lit(False), "Unreliable data")
             .otherwise(F.lit(None))
        )
        errors = (
            measure_raw
            .withColumn("Removal_Reason", reason)
            .filter(F.col("Removal_Reason").isNotNull())
        )
        return errors

    @staticmethod
    def summarize_error_counts(errors_df: DataFrame) -> DataFrame:
        return errors_df.groupBy("Removal_Reason").count().orderBy(F.col("count").desc())

    @staticmethod
    def validate_enriched_measure(
        measure_enriched: DataFrame,
        station: DataFrame,
        item: DataFrame,
        bad_statuses: Iterable[int] = (2, 4, 8, 9),
        lat_range: Tuple[float, float] = (-90.0, 90.0),
        lon_range: Tuple[float, float] = (-180.0, 180.0),
    ):
        duplicates = (
            measure_enriched
            .groupBy("Measurement_Date", "Station_Code", "Item_Code")
            .count()
            .filter(F.col("count") > 1)
        )
        nulls = measure_enriched.filter(
            F.col("Measurement_Date").isNull() |
            F.col("Station_Code").isNull() |
            F.col("Item_Code").isNull() |
            F.col("Average_Value").isNull()
        )
        missed_station = measure_enriched.filter(
            F.col("Station_Name").isNull() |
            F.col("Latitude").isNull() |
            F.col("Longitude").isNull()
        )
        missed_item = measure_enriched.filter(
            F.col("Item_Name").isNull() |
            F.col("Unit").isNull()
        )
        orphan_station = (
            measure_enriched.select("Station_Code").distinct()
            .join(station.select("Station_Code").distinct(), on="Station_Code", how="left_anti")
        )
        orphan_item = (
            measure_enriched.select("Item_Code").distinct()
            .join(item.select("Item_Code").distinct(), on="Item_Code", how="left_anti")
        )
        negative_values = measure_enriched.filter(F.col("Average_Value") < 0)
        bad_status = (
            measure_enriched.filter(
                F.col("Instrument_Status").isin(list(bad_statuses))
            )
        )
        bad_latlong = measure_enriched.filter(
            (~F.col("Latitude").between(lat_range[0], lat_range[1])) |
            (~F.col("Longitude").between(lon_range[0], lon_range[1]))
        )
        checks = [
            ("Duplicates", duplicates.count()),
            ("Nulls", nulls.count()),
            ("Missing_Station_Info", missed_station.count()),
            ("Missing_Item_Info", missed_item.count()),
            ("Orphan_Station", orphan_station.count()),
            ("Orphan_Item", orphan_item.count()),
            ("Negative_Values", negative_values.count()),
            ("Bad_Status", bad_status.count()),
            ("Bad_Latitude_Longitude", bad_latlong.count()),
        ]
        summary_df = measure_enriched.sparkSession.createDataFrame(
            checks, schema=["Rule_ID", "Validations"]
        )
        details = {
            "Duplicates": duplicates,
            "Nulls": nulls,
            "Missing_Station_Info": missed_station,
            "Missing_Item_Info": missed_item,
            "Orphan_Station": orphan_station,
            "Orphan_Item": orphan_item,
            "Negative_Values": negative_values,
            "Bad_Status": bad_status,
            "Bad_Latitude_Longitude": bad_latlong,
        }
        return summary_df, details
