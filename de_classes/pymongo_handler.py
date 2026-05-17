#Author: Toh Ming Yang

from pymongo import MongoClient, ASCENDING, GEOSPHERE
from pymongo.errors import CollectionInvalid, BulkWriteError
from pyspark.sql import functions as F, types as T
from de_classes.data_processor import DataLoading
from typing import Optional
import matplotlib.pyplot as plt
import pandas as pd

class PyMongoUtils:
    def __init__(self, uri: str, use_timeseries: bool = True):
        self.uri = uri
        self.use_timeseries = use_timeseries
        self._client = MongoClient(self.uri)

    def get_database(self, database_name: str):
        return self._client[database_name]

    def create_collections(self, db):
        if "stations" not in db.list_collection_names():
            db.create_collection("stations")
            db.stations.create_index([("location", GEOSPHERE)])
            db.stations.create_index([("name", ASCENDING)])

        if "items" not in db.list_collection_names():
            db.create_collection("items")

        if "measurements" not in db.list_collection_names():
            if self.use_timeseries:
                try:
                    db.create_collection(
                        "measurements",
                        timeseries={"timeField": "ts", "metaField": "meta", "granularity": "hours"},
                    )
                except CollectionInvalid:
                    db.create_collection("measurements")
            else:
                db.create_collection("measurements")

            db.measurements.create_index([
                ("meta.station_code", ASCENDING),
                ("meta.item_code", ASCENDING),
                ("ts", ASCENDING),
            ])

    @staticmethod
    def _insert_in_batches(col, docs_iter, batch_size: int = 1000) -> int:
        total = 0
        batch = []
        for d in docs_iter:
            batch.append(d)
            if len(batch) >= batch_size:
                try:
                    col.insert_many(batch, ordered=False)
                except BulkWriteError:
                    pass
                total += len(batch)
                batch = []
        if batch:
            try:
                col.insert_many(batch, ordered=False)
            except BulkWriteError:
                pass
            total += len(batch)
        return total

    def load_items(self, spark, db, item_csv: str, batch_size: int = 1000, drop_existing: bool = True) -> int:
        sdf = DataLoading.read_csv(spark, item_csv)

        sdf2 = (sdf
            .withColumn("_id", F.col("Item code").cast(T.IntegerType()))
            .withColumn("name", F.col("Item name").cast(T.StringType()))
            .withColumn("unit", F.col("Unit of measurement").cast(T.StringType()))
            .withColumn("good",      F.col("Good(Blue)").cast(T.DoubleType()))
            .withColumn("normal",    F.col("Normal(Green)").cast(T.DoubleType()))
            .withColumn("bad",       F.col("Bad(Yellow)").cast(T.DoubleType()))
            .withColumn("very_bad",  F.col("Very bad(Red)").cast(T.DoubleType()))
            .select("_id", "name", "unit", "good", "normal", "bad", "very_bad"))

        col = db.items
        if drop_existing:
            col.delete_many({})

        def gen_docs():
            for r in sdf2.toLocalIterator():
                if r["_id"] is None:
                    continue
                yield {
                    "_id": int(r["_id"]),
                    "name": r["name"],
                    "unit": r["unit"],
                    "thresholds": {
                        "good":     float(r["good"])     if r["good"]     is not None else None,
                        "normal":   float(r["normal"])   if r["normal"]   is not None else None,
                        "bad":      float(r["bad"])      if r["bad"]      is not None else None,
                        "very_bad": float(r["very_bad"]) if r["very_bad"] is not None else None,
                    }
                }

        return self._insert_in_batches(col, gen_docs(), batch_size=batch_size)

    def load_stations(self, spark, db, station_csv: str, batch_size: int = 1000, drop_existing: bool = True) -> int:
        sdf = DataLoading.read_csv(spark, station_csv)

        lat = F.col("Latitude").cast(T.DoubleType())
        lon = F.col("Longitude").cast(T.DoubleType())

        sdf2 = (sdf
            .withColumn("_id", F.col("Station code").cast(T.IntegerType()))
            .withColumn("name", F.col("Station name(district)").cast(T.StringType()))
            .withColumn("address", F.col("Address").cast(T.StringType()))
            .withColumn(
                "location",
                F.when(lat.isNotNull() & lon.isNotNull(),
                       F.struct(
                           F.lit("Point").alias("type"),
                           F.array(lon, lat).alias("coordinates")
                       )
                )
            )
            .withColumn("longitude", lon)
            .withColumn("latitude", lat)
            .select("_id", "name", "address", "location", "longitude", "latitude"))

        col = db.stations
        if drop_existing:
            col.delete_many({})

        def gen_docs():
            for r in sdf2.toLocalIterator():
                if r["_id"] is None:
                    continue
                doc = {"_id": int(r["_id"]), "name": r["name"], "address": r["address"]}

                loc = r["location"]
                if loc is not None:
                    lon_v = float(loc["coordinates"][0])
                    lat_v = float(loc["coordinates"][1])
                    doc["location"] = {"type": loc["type"], "coordinates": [lon_v, lat_v]}

                yield doc

        return self._insert_in_batches(col, gen_docs(), batch_size=batch_size)

    def load_measurements(self, spark, db, measure_csv: str, batch_size: int = 1000, drop_existing: bool = True) -> int:
        sdf = DataLoading.read_csv(spark, measure_csv)
    
        sdf2 = (sdf
            .withColumn("ts", F.to_timestamp("Measurement_Date"))
            .withColumn("avg", F.col("Average_Value").cast(T.DoubleType()))
            .withColumn("instrument_status", F.col("Instrument_Status").cast(T.IntegerType()))
            .withColumn("station_code", F.col("Station_Code").cast(T.IntegerType()))
            .withColumn("item_code",    F.col("Item_Code").cast(T.IntegerType()))
            .withColumn("status",       F.col("Status").cast(T.StringType()))
            .select("ts", "avg", "instrument_status", "station_code", "item_code", "status"))
    
        col = db.measurements
        if drop_existing:
            col.delete_many({})
    
        def gen_docs():
            for r in sdf2.toLocalIterator():
                if r["ts"] is None:
                    continue
                yield {
                    "ts": r["ts"],
                    "avg": float(r["avg"]) if r["avg"] is not None else None,
                    "instrument_status": int(r["instrument_status"]) if r["instrument_status"] is not None else None,
                    "status": r["status"],
                    "meta": {
                        "station_code": int(r["station_code"]) if r["station_code"] is not None else None,
                        "item_code":    int(r["item_code"])    if r["item_code"]    is not None else None,
                    },
                }
    
        return self._insert_in_batches(col, gen_docs(), batch_size=batch_size)


class AirQualityQueries:
    def __init__(self, db, items_col: str = "items", stations_col: str = "stations", measurements_col: str = "measurements"):
        self.db = db
        self.items = db[items_col]
        self.stations = db[stations_col]
        self.measurements = db[measurements_col]

    def latest_snapshot_df(self) -> pd.DataFrame:
        pipeline_latest_all = [
            {"$sort": {"ts": -1}},
            {"$group": {
                "_id": {"station_code": "$meta.station_code", "item_code": "$meta.item_code"},
                "doc": {"$first": "$$ROOT"}
            }},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$lookup": {
                "from": "items",
                "localField": "meta.item_code",
                "foreignField": "_id",
                "as": "item"
            }},
            {"$set": {
                "item_name": {"$ifNull": [{"$first": "$item.name"}, None]},
                "value": "$avg"
            }},
            {"$lookup": {
                "from": "stations",
                "localField": "meta.station_code",
                "foreignField": "_id",
                "as": "s"
            }},
            {"$set": {"station_name": {"$ifNull": [{"$first": "$s.name"}, None]}}},
            {"$project": {
                "_id": 0,
                "Datetime": {"$dateToString": {"date": "$ts", "format": "%Y-%m-%d %H:%M"}},
                "Station Code": "$meta.station_code",
                "Station Name": "$station_name",
                "Item Code": "$meta.item_code",
                "Item Name": "$item_name",
                "Value": "$value",
                "Status": "$status"
            }}
        ]
        rows = list(self.measurements.aggregate(pipeline_latest_all, allowDiskUse=True))
        return pd.DataFrame(rows)

    def print_latest_snapshot(self, df_all: pd.DataFrame) -> None:
        print("Query 1: Latest snapshot for each item")
        if df_all.empty:
            print("(No rows)")
            return

        for item_name, df in df_all.groupby("Item Name"):
            df_sorted = df.sort_values("Value", ascending=False).reset_index(drop=True)
            print(f"\n{item_name} latest snapshot (rows: {len(df_sorted)}):")
            print(f"{'Datetime':<20} {'Station Code':<15} {'Station Name':<25} {'Value':>10} {'Status':>10}")
            print("=" * 90)
            for _, row in df_sorted.iterrows():
                print(
                    f"{row['Datetime']:<20} "
                    f"{row['Station Code']:<15} "
                    f"{str(row['Station Name']):<25} "
                    f"{row['Value']:>10} "
                    f"{row['Status']:>10}"
                )
            print("=" * 90)

    def historical_trend_df(self, item_code: int, station_code: int) -> pd.DataFrame:
        pipeline_historical = [
            {"$match": {"meta.item_code": item_code, "meta.station_code": station_code}},
            {"$group": {
                "_id": {"ts": "$ts", "station_code": "$meta.station_code", "item_code": "$meta.item_code"},
                "doc": {"$first": "$$ROOT"}
            }},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$lookup": {
                "from": "items",
                "localField": "meta.item_code",
                "foreignField": "_id",
                "as": "item"
            }},
            {"$set": {"item_name": {"$first": "$item.name"}, "value": "$avg"}},
            {"$lookup": {
                "from": "stations",
                "localField": "meta.station_code",
                "foreignField": "_id",
                "as": "s"
            }},
            {"$set": {"station_name": {"$first": "$s.name"}}},
            {"$sort": {"ts": 1}},
            {"$project": {
                "_id": 0,
                "Datetime": {"$dateToString": {"date": "$ts", "format": "%Y-%m-%d %H:%M"}},
                "Station Name": "$station_name",
                "Item Name": "$item_name",
                "Value": "$value",
                "Status": "$status"
            }}
        ]
        rows = list(self.measurements.aggregate(pipeline_historical, allowDiskUse=True))
        return pd.DataFrame(rows)

    def print_historical(self, df_hist: pd.DataFrame, head: Optional[int] = 20) -> None:
        if df_hist.empty:
            print("Query 2: Historical trend (No rows)")
            return
        item_name = df_hist["Item Name"].iloc[0]
        station_name = df_hist["Station Name"].iloc[0]
        print(f"\nQuery 2: Historical trend for {item_name} at station {station_name} (rows: {len(df_hist)}):")
        print(f"{'Datetime':<20} {'Station Name':<30} {'Value':>10} {'Status':>10}")
        print("=" * 80)
        
        for _, row in df_hist.head(head).iterrows():
            print(
                f"{row['Datetime']:<20} "
                f"{str(row['Station Name']):<30} "
                f"{row['Value']:>10} "
                f"{row['Status']:>10}"
            )
        print("=" * 80)

        df_hist["Datetime"] = pd.to_datetime(df_hist["Datetime"], errors="coerce")
        df_hist = df_hist.sort_values("Datetime")
        
        plt.figure(figsize=(12,4))
        x = df_hist["Datetime"].to_numpy()
        y = df_hist["Value"].to_numpy()
        
        plt.plot(x, y, marker="x", linestyle="-", label=item_name)
        plt.title(f"Historical Trend of {item_name} at {station_name}", fontsize=14)
        plt.xlabel("Datetime")
        plt.ylabel("Pollutant Value")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()
