from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("S3DirectoryCreation") \
    .getOrCreate()

# Create a sample DataFrame
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["Name", "ID"]
df = spark.createDataFrame(data, columns)

# Define the S3 path, including the "directory" prefix
s3_path = "s3a://test-bucket/eventLog2/data.csv"

# Write the DataFrame to the specified S3 path
# This will create 'my_new_directory/' if it doesn't exist and place 'data.csv' inside it.
df.write.mode("overwrite").format("csv").option("header", "true").save(s3_path)

print(f"Data written to: {s3_path}")

spark.stop()