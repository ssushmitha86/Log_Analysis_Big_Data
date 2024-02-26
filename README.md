Log Analysis using big data technologies like Apacke Kafka,Apache Spark, Hadoop HDFS

**INTRODUCTION**
The goal is to process and analyz log files in the context of Big Data technologies. My solution
employs Kafka, Spark Streaming, Parquet files, and HDFS File System to build a pipeline that starts
from reading data from a log file and ends up storing the processed data into the Hadoop Distributed
File System (HDFS) in Parquet format.

**Flow of Data**

**1. Kafka Producer:**
The producer reads a log file line by line and sends each line as a message to a Kafka topic called log_topic.
• producer.flush(): After every 1000 messages to ensure all messages are sent to Kafka before
continuing to the next line.

**2. Kafka Consumer and Spark Streaming:**
Consuming the messages from Kafka using Spark Streaming and performing a series of
transformations. Converts the consumed Kafka messages
into a DataFrames and uses a lot of regex and string splitting to parse each line of log data
and extract relevant pieces of information. These pieces of information are placed in new
columns, and the result is a DataFrame with structured data derived from the original logs.

**3. Analysis and transformation:**
Here,the occurrences of different attributes such as endpoints (URIs), status codes, host
names, and creating some aggregations such as the average number of daily requests per host.

**4. Writing to HDFS:**
• After performing the analysis and transformations,  the resultant DataFrames are appended into
the HDFS in Parquet format. As it is a columnar storage file format parquet is efficient for an analytical type of workload.
