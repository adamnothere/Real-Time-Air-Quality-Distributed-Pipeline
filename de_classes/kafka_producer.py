# Author: Darren Ong

import time
import json
import pandas as pd
from kafka import KafkaProducer
from typing import Optional

class KafkaCSVProducer:
    def __init__(self, topic: str, kafka_server: str = "localhost:9092", delay_sec: float = 0.5):
        self.topic = topic
        self.delay = delay_sec
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            linger_ms=50
        )

    def stream_csv(self, csv_path: str, limit: Optional[int] = None):
        df = pd.read_csv(csv_path)
        if limit is not None:
            df = df.head(limit)
        print(f"[Producer] Streaming {len(df)} rows from {csv_path} to topic '{self.topic}'...")
        for _, row in df.iterrows():
            self.producer.send(self.topic, row.to_dict())
            time.sleep(self.delay)
        self.producer.flush()
        print("[Producer] Done sending messages.")

if __name__ == "__main__":
    csv_path = "/home/student/de-assignment/de_data/Measurement_info.csv"
    producer = KafkaCSVProducer(topic="pollution-data", delay_sec=0.5)
    producer.stream_csv(csv_path)