===========================================================
README
===========================================================

Contributors:
- Adam Ho Swee En
- Toh Ming Yang
- Darren Ong Der Ren
- Elisha Loh Tien Rong

-----------------------------------------------------------
1. Project Title:
-----------------------------------------------------------
Real-Time Analysis of Air Quality Data for Seoul Districts (SDG #11 – Sustainable Cities and Communities)

-----------------------------------------------------------
2. Project Folder Structure: 
-----------------------------------------------------------
/de-assignment/
│
├── de_classes/
│   ├── kafka_producer.py        
│   ├── kafka_consumer.py        
│   ├── data_processor.py        
│   ├── mongodb_handler.py       
│   ├── neo4j_handler.py         
│   ├── stream_processor.py  
|   ├── spark_factory.py
│
├── de_data/
│   ├── Measurement_info.csv  
|   ├── Measurement_item_info.csv
│   └── Measurement_station_info.csv          
│
├── Task 2 Data Processor Demo.ipynb
├── Task 3 MongoDB Demo.ipynb
├── Task 4 Neo4j Demo.ipynb
├── Task 5 Spark Structured Streaming Demo.ipynb
│
├── requirements.txt             
└── readme.txt
    
-----------------------------------------------------------
3. Setup Instructions:
-----------------------------------------------------------
1. Start Kafka server and necessary services (HDFS, YARN, Zookeeper, Kafka).

2. Copy project folder from window to local OS
   $ cp -r /mnt/c/de-assignment /home/student/

3. Install dependencies:
   $ cd de-assignment
   $ pip install -r requirements.txt

4. Put file into HDFS (inside de-assignment directory)
   $ hdfs dfs -put de_data/Measurement_station_info.csv Measurement_station_info.csv
   $ hdfs dfs -put de_data/Measurement_item_info.csv Measurement_item_info.csv

5. To run the demo:

   Task 1
   ------
   a. Run the Kafka Producer (inside de-assignment directory)
      python de_classes/kafka_producer.py 

   b. Run the Kafka Consumer (inside de-assignment directory)
      python de_classes/kafka_consumer.py 

   Task 2
   ------
   Open the Task 2 Data Processor Demo.ipynb in Jupyter lab launched in your WSL Ubuntu instance.

   Task 3
   ------
   Open the Task 3 MongoDB Demo.ipynb in Jupyter lab launched in your WSL Ubuntu instance.

   Task 4
   ------
   Open the Task 4 Neo4j Demo.ipynb in Jupyter lab launched in your WSL Ubuntu instance.

   Task 5
   ------
   Open the Task 5 Spark Structured Streaming Demo.ipynb in Jupyter lab launched in your WSL Ubuntu instance.

  


