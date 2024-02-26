
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col

# Creating Spark session
spark = SparkSession.builder.appName("ParquetReader").getOrCreate()

# Read the parquet file
df = spark.read.parquet("hdfs://localhost:9000/dan/get_logs/frequent_hosts/part-00000-e8a2c386-d382-41f2-8bd0-7a92a52904f8-c000.snappy.parquet")

# Show the first row
first_row = df.head()
print(first_row)

# Show last 20 rows
last_20_rows = df.tail(200)
for row in last_20_rows:
    print(row)

# Show the description
# description = df.describe()
# description.show()

# Count the number of rows
num_rows = df.count()
print(f'The number of rows is: {num_rows}')
