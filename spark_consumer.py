
from pyspark.sql.functions import desc

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col, to_timestamp, dayofmonth, countDistinct, avg

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("Log Analysis with Spark") \
    .getOrCreate()
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

# Defining the Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "log_topic") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as line")

extracted = df.select(regexp_extract('line', r'^(\S+)', 1).alias('host_name'),
                      regexp_extract('line', r'\[(.*?)\]', 1).alias('timestamp'),
                      split(regexp_extract('line', r'\"(.*?)\"', 1), ' ')[0].alias('HTTP_Request_Method'),
                      split(regexp_extract('line', r'\"(.*?)\"', 1), ' ')[1].alias('URIs'),
                      split(regexp_extract('line', r'\"(.*?)\"', 1), ' ')[2].alias('Protocol'),
                      regexp_extract('line', r'\s(\d{3})\s', 1).cast('integer').alias('HTTP_Status_Codes'),
                      regexp_extract('line', r'\s(\d{3})\s(\d+)', 2).cast('integer').alias('HTTP_Response_Content_Size'))


extracted = extracted.withColumn(
    "timestamp",
    to_timestamp(
        col("timestamp"), 
        "dd/MMM/yyyy:HH:mm:ss Z"
    )
)

extracted = extracted.withWatermark("timestamp", "1 hour")


# Extract the URLs (Endpoints) and count them
endpoint_counts = extracted.groupBy("URIs").count()

status_code_counts = extracted.groupBy("HTTP_Status_Codes").count()

unique_hosts = extracted.groupBy("host_name").count()

daily_requests_per_host = extracted.groupBy(dayofmonth("timestamp").alias("day"), "host_name").count()

frequent_hosts = extracted.filter(col("host_name") != "").groupBy("host_name").count()

avg_daily_requests_per_host = daily_requests_per_host.groupBy("host_name").agg(avg("count").alias("avg_daily_requests"))

def query1(idf, epoch_id):
    top_20_endpoints = idf.orderBy(col("count").desc()).limit(20)
    top_20_endpoints.write.mode("overwrite").parquet("hdfs://localhost:9000/dan/get_logs/t20")

def query2(idf, epoch_id):
    top_10_error_endpoints = idf.filter(col("HTTP_Status_Codes") != 200).groupBy("URIs").count().orderBy(col("count").desc()).limit(10)
    top_10_error_endpoints.write.mode("overwrite").parquet("hdfs://localhost:9000/dan/get_logs/t10")


def query3(idf, epoch_id):
    unique_hosts = idf.groupBy("host_name").count()
    unique_hosts.write.mode("overwrite").parquet("hdfs://localhost:9000/dan/get_logs/unique_hosts")



def query4(idf, epoch_id):
    status_code_counts = idf.orderBy(desc("count"))
    status_code_counts.write.mode("overwrite").parquet("hdfs://localhost:9000/dan/get_logs/status_code_counts")

def query5(idf, epoch_id):
    top_10_frequent_hosts = idf.orderBy(desc("count")).select("host_name").limit(10)
    top_10_frequent_hosts.write.mode("overwrite").parquet("hdfs://localhost:9000/dan/get_logs/frequent_hosts")

get_logs = extracted.filter(col("HTTP_Request_Method").like("GET"))
put_logs = extracted.filter(col("HTTP_Request_Method").like("PUT"))
_404_logs = extracted.filter(extracted.HTTP_Status_Codes.like('%404%'))


_404_query = _404_logs.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/dan/get_logs/404") \
    .option("checkpointLocation", "hdfs://localhost:9000/dan/check/check_404_logs") \
    .trigger(processingTime='20 seconds') \
    .start()
# Start streaming queries with foreachBatch()
get_query = get_logs.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/dan/get_logs/GET") \
    .option("checkpointLocation", "hdfs://localhost:9000/dan/check/check_get_logs") \
    .trigger(processingTime='20 seconds') \
    .start()


put_query = put_logs.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/dan/get_logs/PUT") \
    .option("checkpointLocation", "hdfs://localhost:9000/dan/check/check_put_logs") \
    .trigger(processingTime='20 seconds') \
    .start()
streaming_query1 = endpoint_counts.writeStream.outputMode("complete").trigger(processingTime='20 seconds').foreachBatch(query1).start()
streaming_query2 = extracted.writeStream.outputMode("append").trigger(processingTime='20 seconds').foreachBatch(query2).start()
streaming_query3 = unique_hosts.writeStream.outputMode("complete").trigger(processingTime='40 seconds').foreachBatch(query3).start()

streaming_query4 = status_code_counts.writeStream.outputMode("complete").trigger(processingTime='40 seconds').foreachBatch(query4).start()
streaming_query5 = frequent_hosts.writeStream.outputMode("complete").trigger(processingTime='20 seconds').foreachBatch(query5).start()

# Wait for the termination of the queries
get_query.awaitTermination()
_404_query.awaitTermination()
put_query.awaitTermination()
streaming_query1.awaitTermination()
streaming_query2.awaitTermination()
streaming_query3.awaitTermination()
streaming_query4.awaitTermination()
streaming_query5.awaitTermination()
# Stop SparkSession
spark.stop()
