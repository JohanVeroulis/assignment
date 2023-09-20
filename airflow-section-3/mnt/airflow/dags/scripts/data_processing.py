from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, date_format, to_timestamp
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import from_csv

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("data_processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file subscribers.csv from the HDFS
subscribers_df = spark.read.csv('hdfs://namenode:9000/data/subscribers.csv', header=False)


# Rename columns of subscribers.csv
subscribers_df = subscribers_df.withColumnRenamed("_c0", "sub_id") \
                                 .withColumnRenamed("_c1", "activation_date") \


# cleaned_subscribers_df = subscribers_df.withColumn(
#     "row_key",
#     concat(col("subscriber_id"), "_", date_format(col("activation_date"), "yyyyMMdd"))
# ).select("row_key", "subscriber_id", "activation_date")

#cleaned_subscribers_df.show()

# Read the file transactions.csv from the HDFS
transactions_df = spark.read.csv('hdfs://namenode:9000/data/cvas_data_transactions.csv', header=False)

# Rename columns of transactions.csv
transactions_df = transactions_df.withColumnRenamed("_c0", "timestamp") \
                                 .withColumnRenamed("_c1", "subscriber_id") \
                                 .withColumnRenamed("_c2", "amount") \
                                 .withColumnRenamed("_c3", "channel")


cleaned_transactions_df = transactions_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ssXXX")
).withColumn(
    "amount",
    col("amount").cast(DecimalType(10, 2))
).withColumnRenamed("subscriber_id", "sub_id")


# Filter out rows with null timestamps
#cleaned_transactions_df = cleaned_transactions_df.filter(cleaned_transactions_df["timestamp"].isNotNull())

#Clean the 'channel' column by removing rows with '***'
cleaned_transactions_df1 = cleaned_transactions_df.filter(cleaned_transactions_df['channel'] != '***')

#cleaned_transactions_df1.show()

# Join with subscribers data
final_df = cleaned_transactions_df1.join(subscribers_df, ["sub_id"], "left")

final_df.show()

# Persist to Parquet
final_df.write.parquet("output.parquet")


# Export the dataframe into the Hive table data_final
final_df.write.mode("append").insertInto("data_final")


