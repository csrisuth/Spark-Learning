# Spark-Learning (DRAFT)

### Note: These codes were tested on following environment
- Apache Kafka v2.5.0
- Apache Spark 2.4.5
- python 3.7
- scala 2.11.12 

## Objective and Overview Architecture
![Overview Architecture](figures/Kafka_Spark.jpg)
*Overview Architecture*

This repository was created to demonstrate features of Apache Spark - Structured Streaming API in python. This project was divided into 3 parts

1. Kafka Producer [bitmex_streaming.py](bitmex_streaming.py), This is python code which reads trade data (XBTU20 and XBTM20 contracts) from "wss://www.bitmex.com/realtime", then send them to Kafka topic.
2. Spark Structured Streaming using Kafka data source [kafka_pyspark.py](kafka_pyspark.py), This pyspark code uses spark structured streaming to subscribe to Kafka topic, perform some data formatting then output to files.
3. Spark Structured Streaming using file data source [StreamingPlot.ipynb](StreamingPlot.ipynb), This notebook also uses spark structured streaming to watch file (which sink from kafka_pyspark.py), perform some aggregation, and plot line graphs.