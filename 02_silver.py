import logging
from pyspark.sql.functions import col, to_timestamp, count, sum as spark_sum, collect_list
from pyspark.sql.types import TimestampType
from utils import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"

def read_bronze_table(spark, table_name: str):
    bronze_path = f"{BRONZE_PATH}/{table_name}"
    return spark.read.parquet(bronze_path)

def clean_orders(df):
    logging.info("Cleaning orders table")
    
    df_cleaned = df \
        .filter(col("order_id").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .dropDuplicates(["order_id"]) \
        .filter(col("order_status").isin(["delivered", "shipped"]))
    
    df_typed = df_cleaned \
        .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
        .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
        .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"))) \
        .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
        .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
    
    return df_typed

def create_payments_summary(df_payments):
    logging.info("Creating payments summary")
    
    payments_summary = df_payments \
        .groupBy("order_id") \
        .agg(
            spark_sum("payment_value").alias("total_payment_value"),
            count("payment_sequential").alias("payment_count"),
            collect_list("payment_type").alias("payment_types")
        )
    
    return payments_summary

def create_orders_consolidated(spark):
    logging.info("Creating orders consolidated table")
    
    orders = read_bronze_table(spark, "orders").drop("ingestion_timestamp")
    order_items = read_bronze_table(spark, "order_items").drop("ingestion_timestamp")
    customers = read_bronze_table(spark, "customers").drop("ingestion_timestamp")
    products = read_bronze_table(spark, "products").drop("ingestion_timestamp")
    sellers = read_bronze_table(spark, "sellers").drop("ingestion_timestamp")
    
    orders_clean = clean_orders(orders)
    
    consolidated = orders_clean \
        .join(order_items, "order_id", "inner") \
        .join(customers, "customer_id", "inner") \
        .join(products, "product_id", "left") \
        .join(sellers, "seller_id", "left")
    
    return consolidated

def main():
    spark = get_spark_session("Silver Layer Processing")
    
    logging.info("Starting Silver layer processing")
    
    orders_consolidated = create_orders_consolidated(spark)
    orders_consolidated.write.mode("overwrite").parquet(f"{SILVER_PATH}/orders_consolidated")
    count_consolidated = orders_consolidated.count()
    logging.info(f"Orders consolidated created - {count_consolidated} records")
    
    payments = read_bronze_table(spark, "payments").drop("ingestion_timestamp")
    payments_summary = create_payments_summary(payments)
    payments_summary.write.mode("overwrite").parquet(f"{SILVER_PATH}/payments_summary")
    count_payments = payments_summary.count()
    logging.info(f"Payments summary created - {count_payments} records")
    
    logging.info("Silver layer processing completed")
    spark.stop()

if __name__ == "__main__":
    main()
