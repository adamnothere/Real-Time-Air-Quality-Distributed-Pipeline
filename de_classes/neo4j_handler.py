#Author: Elisha Loh Tien Rong

from neo4j import GraphDatabase
import pandas as pd

class Neo4jUtils:

    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._driver.verify_connectivity()

    def create_constraints(self):
        constraints = [
            """
            CREATE CONSTRAINT station_code IF NOT EXISTS
            FOR (s:Station) REQUIRE s.code IS UNIQUE
            """,
            """
            CREATE CONSTRAINT item_code IF NOT EXISTS
            FOR (i:Item) REQUIRE i.code IS UNIQUE
            """,
            """
            CREATE CONSTRAINT status_level IF NOT EXISTS
            FOR (st:Status) REQUIRE st.level IS UNIQUE
            """,
            """
            CREATE CONSTRAINT measurementtime_rawdatetime IF NOT EXISTS
            FOR (t:MeasurementTime) REQUIRE t.raw_datetime IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT measurement_unique IF NOT EXISTS
            FOR (m:Measurement)
            REQUIRE (m.station_code, m.item_code, m.datetime) IS UNIQUE
            """
        ]
        
        for query in constraints:
            self._driver.execute_query(query, database_="neo4j")
            print("Executed:", query.splitlines()[0].strip())

    def data_ingestion(self, records):
        self._driver.execute_query("""
        UNWIND $rows AS m
        
        MERGE (s:Station {code: m.Station_Code})
          ON CREATE SET s.name = m.Station_Name,
                        s.latitude = m.Latitude,
                        s.longitude = m.Longitude,
                        s.address = m.Address
        
        MERGE (i:Item {code: m.Item_Code})
          ON CREATE SET i.name = m.Item_Name,
                        i.unit = m.Unit
        
        MERGE (st:Status {level: m.Status})
        
        MERGE (t:MeasurementTime {raw_datetime: m.Measurement_Date})
          ON CREATE SET t.datetime = datetime(m.Measurement_Date)
        
        MERGE (meas:Measurement {
                station_code: m.Station_Code,
                item_code: m.Item_Code,
                raw_datetime: m.Measurement_Date
        })
          ON CREATE SET meas.value = m.Average_Value,
                        meas.instrument_status = m.Instrument_Status,
                        meas.status = m.Status,
                        meas.datetime = datetime(m.Measurement_Date)
          ON MATCH SET  meas.value = m.Average_Value,
                        meas.instrument_status = m.Instrument_Status,
                        meas.status = m.Status,
                        meas.datetime = datetime(m.Measurement_Date)
        
        MERGE (s)-[:HAS_MEASUREMENT]->(meas)
        MERGE (meas)-[:MEASURED_IN]->(i)
        MERGE (meas)-[:AT_TIME]->(t)
        MERGE (meas)-[:HAS_STATUS]->(st)
        """,                  
        rows=records, 
        database_="neo4j")

    def hot_spot_query(self):
        cypher = """
        MATCH (m:Measurement)-[:MEASURED_IN]->(i:Item)
        MATCH (s:Station)-[:HAS_MEASUREMENT]->(m)
        WITH s, i,
             count(*) AS Total,
             sum(CASE WHEN m.status CONTAINS "Bad" OR m.status CONTAINS "Very Bad" THEN 1 ELSE 0 END) AS Bad_Count,
             round(avg(m.value), 2) AS Avg_Value, 
             max(m.value) AS Worst_Value
        RETURN s.name AS Station,
               i.name AS Pollutant,
               Bad_Count,
               Total,
               round(100.0 * Bad_Count / Total, 2) AS Weightage,
               Avg_Value, 
               Worst_Value
        ORDER BY Weightage DESC, Avg_Value DESC, Worst_Value DESC
        """
        records, summary, keys = self._driver.execute_query(
            cypher, database_="neo4j"
        )
        df = pd.DataFrame([r.data() for r in records])
        return df

    def daily_avg_query(self):
        cypher = """
        MATCH (m:Measurement)-[:MEASURED_IN]->(i:Item)
        MATCH (s:Station)-[:HAS_MEASUREMENT]->(m)
        WITH date(m.datetime) AS Day, s.name AS Station, i.name AS Pollutant, avg(m.value) AS Daily_Avg
        RETURN Day, Station, Pollutant, round(Daily_Avg,2) AS Daily_Avg_Value
        ORDER BY Day ASC, Station ASC
        """
        records, summary, keys = self._driver.execute_query(
            cypher, database_="neo4j"
        )
        
        df = pd.DataFrame([r.data() for r in records])
        return df
