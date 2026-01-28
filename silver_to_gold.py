from pyspark.sql.functions import *
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

silver_df = spark.read.schema(schema) \
    .option("header", "true") \
    .option("recursiveFileLookup", "true") \
    .csv("s3a://silver/")

print(silver_df.count())
silver_df.show(5, truncate=False)


gold_customer = silver_df.groupBy("customer_id").agg(
    count("*").alias("total_transactions"),
    sum("transaction_amount").alias("total_amount"),
    avg("transaction_amount").alias("avg_amount"),
    stddev("transaction_amount").alias("stddev_amount"),
    countDistinct("country").alias("unique_countries"),
    countDistinct("device_type").alias("unique_devices")
)

gold_merchant = silver_df.groupBy("merchant_id").agg(
    count("*").alias("tx_count"),
    sum("transaction_amount").alias("total_amount"),
    avg("transaction_amount").alias("avg_amount"),
    countDistinct("customer_id").alias("unique_customers")
)

gold_time = silver_df.withColumn("hour", hour("transaction_timestamp")) \
    .groupBy("hour").agg(
        count("*").alias("tx_count"),
        sum("transaction_amount").alias("total_amount"),
    )

gold_customer.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3a://gold/customer_metrics/")


gold_merchant.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3a://gold/merchant_metrics/")


gold_time.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3a://gold/time_metrics/")