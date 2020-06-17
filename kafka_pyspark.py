from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, substring, from_json
from decimal import Decimal

spark = SparkSession \
    .builder \
    .appName("StructuredStreamingKafka") \
    .getOrCreate()

df_raw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "bitmex01") \
  .option("startingOffsets", "latest") \
  .load()

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
data.printSchema()

data2 = data.select("tmp.timestamp")

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

df_tradeHist = df
df_tradeHist.createOrReplaceTempView("trade_history")

aggInterval = "1 minutes"

def createSQLStatement(interval):
    switcher = {
        "15 seconds" : ",cast(concat(date(timestamp),' ',hour(timestamp),':',minute(timestamp),':',floor(second(timestamp)/15)*15) as timestamp) as group_column",
        "30 seconds" : ",cast(concat(date(timestamp),' ',hour(timestamp),':',minute(timestamp),':',floor(second(timestamp)/30)*30) as timestamp) as group_column",
        "1 minutes"  : ",cast(concat(date(timestamp),' ',hour(timestamp),':',minute(timestamp)) as timestamp) as group_column",
        "5 minutes"  : ",cast(concat(date(timestamp),' ',hour(timestamp),':',floor(minute(timestamp)/5)*5) as timestamp) as group_column",
        "15 minutes" : ",cast(concat(date(timestamp),' ',hour(timestamp),':',floor(minute(timestamp)/15)*15) as timestamp) as group_column",
        "1 hours"  : ",cast(concat(date(timestamp),' ',hour(timestamp),':00') as timestamp) as group_column",
        "4 hours"  : ",cast(concat(date(timestamp),' ',floor(hour(timestamp)/4)*4,':00') as timestamp) as group_column",
        "6 hours"  : ",cast(concat(date(timestamp),' ',floor(hour(timestamp)/6)*6,':00') as timestamp) as group_column",
        "12 hours" : ",cast(concat(date(timestamp),' ',floor(hour(timestamp)/12)*12,':00') as timestamp) as group_column",
        "1 days"  : ",cast(concat(date(timestamp),' ','00:00') as timestamp) as group_column"
    }
    return switcher.get(interval, "Please input correct interval")

df_group_temp = spark.sql('SELECT timestamp, symbol, price, size '+ \
                             createSQLStatement(aggInterval)+ \
                             ' FROM trade_history')

df_group_temp.createOrReplaceTempView("hist_group")
df_interval = spark.sql('''SELECT group_column as timestamp,
                           symbol,
                           first(price) as open, 
                           max(price) as high, 
                           min(price) as low, 
                           last(price) as close,
                           sum(size) as volumn
                           FROM hist_group
                           group by symbol, group_column
                           ''')

query1 = df.writeStream.outputMode("Append") \
            .trigger(processingTime='60 seconds') \
            .format("console") \
            .option("truncate","false") \
            .start()

query2 = df.writeStream.format("csv") \
    .trigger(processingTime='5 minutes') \
    .option("format", "append") \
    .option("path", "./output/tick_data/destination") \
    .option("checkpointLocation", "./output/tick_data/checkpoint") \
    .outputMode("append") \
    .start()

query3 = df_interval.writeStream.outputMode("Update") \
            .trigger(processingTime=aggInterval) \
            .format("console") \
            .option("truncate","false") \
            .start()

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()