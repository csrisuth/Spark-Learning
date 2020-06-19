'''
Bitmex Kafka Consumer using pyspark

Author: Charm Srisuthapan
Date: Jun 19, 2020
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, substring, from_json
from decimal import Decimal

# Apache Kafka Setting
__HOSTNAME = "localhost:9092"
__KAFKA_TOPIC = "bitmex01"

spark = SparkSession \
    .builder \
    .appName("StructuredStreamingKafka") \
    .getOrCreate()

df_raw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", __HOSTNAME) \
  .option("subscribe", __KAFKA_TOPIC) \
  .option("startingOffsets", "latest") \
  .load()

# Due to inferSchema mismatch in pyspark, some decimal data eg. 9400.0 may consider as int, 
# all data were read as string then convert to proper datatype in next step

schema = StructType([
    StructField('timestamp', StringType(), True),
    StructField('symbol', StringType(), True),
    StructField('side', StringType(), True),
    StructField('size', StringType(), True),
    StructField('price', StringType(), True),
    StructField('tickDirection', StringType(), True),
    StructField('trdMatchID', StringType(), True),
    StructField('grossValue', StringType(), True),
    StructField('homeNotional', StringType(), True),
    StructField('foreignNotional', StringType(), True)
])

data = df_raw.select(from_json(col("value").cast("string"), schema).alias("tmp"))
#data.printSchema()

# Convert data into proper datatype
# Note: Due to microseconds issue while cast string to timestamp, 
# microsecond need to be add after convert date/time to timestamp
df = data.withColumn("timestamp", (unix_timestamp("tmp.timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast("long")+\
                                 col("tmp.timestamp").substr(21,3).cast("float")/1000.0).cast("timestamp"))\
                                .withColumn("symbol",col("tmp.symbol"))\
                                .withColumn("side",col("tmp.side"))\
                                .withColumn("size",col("tmp.size").cast("long"))\
                                .withColumn("price",col("tmp.price").cast("float"))\
                                .withColumn("tickDirection",col("tmp.tickDirection"))\
                                .withColumn("trdMatchID",col("tmp.trdMatchID"))\
                                .withColumn("grossValue",col("tmp.grossValue").cast("long"))\
                                .withColumn("homeNotional",col("tmp.homeNotional").cast("decimal"))\
                                .withColumn("foreignNotional",col("tmp.foreignNotional").cast("long"))

df = df.drop("tmp")

df_tradeHist = df.createOrReplaceTempView("trade_history")

# Print output to console for monitoring
query1 = df.writeStream.outputMode("Append") \
            .trigger(processingTime='15 seconds') \
            .format("console") \
            .option("truncate","false") \
            .start()

# Write tick-data to disk (for use in graph plot)
query2 = df.writeStream.format("csv") \
    .trigger(processingTime='1 minutes') \
    .option("format", "append") \
    .option("path", "./output/tick_data/destination") \
    .option("checkpointLocation", "./output/tick_data/checkpoint") \
    .outputMode("append") \
    .start()

query1.awaitTermination()
query2.awaitTermination()