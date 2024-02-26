from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import StringType
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .appName("Log Analysis with Spark") \
    .getOrCreate()

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

# Converting all features to string type to handle all types of data
extracted = extracted.withColumn("HTTP_Status_Codes", extracted["HTTP_Status_Codes"].cast(StringType()))
extracted = extracted.withColumn("HTTP_Response_Content_Size", extracted["HTTP_Response_Content_Size"].cast(StringType()))

# Combining the features into a single column "features_unprocessed"
extracted = extracted.withColumn('features_unprocessed', 
                            concat_ws(' ', 
                                      extracted.host_name, 
                                      extracted.HTTP_Request_Method, 
                                      extracted.URIs, 
                                      extracted.HTTP_Status_Codes, 
                                      extracted.HTTP_Response_Content_Size))


hashingTF = HashingTF(inputCol="features_unprocessed", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
pipeline = Pipeline(stages=[hashingTF, idf])

features_df = pipeline.fit(extracted).transform(extracted)


kmeans = KMeans(featuresCol="features").setK(2).setSeed(1)
model = kmeans.fit(features_df)

transformed = model.transform(features_df)
query = transformed.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
