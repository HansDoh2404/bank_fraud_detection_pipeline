from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Medaillon") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("local[6]") \
    .getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("transaction_amount", DoubleType()),
    StructField("transaction_timestamp", TimestampType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("payment_type", StringType()),
    StructField("country", StringType()),
    StructField("device_type", StringType()),
    StructField("account_age_days", IntegerType()),
    StructField("is_foreign_transaction", BooleanType()),
    StructField("transaction_frequency_24h", IntegerType()),
    StructField("avg_amount_7d", DoubleType()),
    StructField("fraud_label", BooleanType()),
    StructField("file_name", StringType()),
])

bronze_df = spark.read.option("multiLine", True).json("s3a://bronze/")

print(bronze_df.count())

bronze_df.show(5, truncate=False)

silver_df = bronze_df.dropna(subset=["file_name"]) 


ordered_cols = [f.name for f in schema.fields]

file_names = [row.file_name for row in silver_df.select("file_name").distinct().collect()]

for fname in file_names:
    df_file = silver_df.filter(col("file_name") == fname)

    df_file_ordered = df_file.select(*ordered_cols)

    print(f"fichier {fname}") 

    df_file_ordered.show(5, truncate=False)

    print(f"Nombre de lignes dans le fichier : {df_file_ordered.count()}")

    output_path = f"s3a://silver/{fname.replace('.json', '')}"

    df_file_ordered.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

