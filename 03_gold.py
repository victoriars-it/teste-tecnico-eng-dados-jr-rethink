import logging
from pyspark.sql.functions import col, sum as spark_sum, count, avg, countDistinct, coalesce, lit
from utils import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

SILVER_PATH = "delta/silver"
GOLD_PATH = "delta/gold"
BRONZE_PATH = "delta/bronze"

def read_silver_table(spark, table_name: str):
    silver_path = f"{SILVER_PATH}/{table_name}"
    return spark.read.format("delta").load(silver_path)

def read_bronze_table(spark, table_name: str):
    bronze_path = f"{BRONZE_PATH}/{table_name}"
    return spark.read.format("delta").load(bronze_path)

def create_customer_summary(spark):
    logging.info("Creating customer summary")
    
    orders_consolidated = read_silver_table(spark, "orders_consolidated")
    reviews = read_bronze_table(spark, "reviews").drop("ingestion_timestamp")
    
    orders_with_reviews = orders_consolidated \
        .join(reviews, "order_id", "left")
    
    customer_summary = orders_with_reviews \
        .withColumn("revenue", col("price") + col("freight_value")) \
        .groupBy("customer_unique_id", "customer_city", "customer_state") \
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("revenue").alias("total_revenue"),
            avg("revenue").alias("avg_order_value"),
            avg("review_score").alias("avg_review_score")
        )
    
    return customer_summary

def create_product_summary(spark):
    logging.info("Creating product summary")
    
    orders_consolidated = read_silver_table(spark, "orders_consolidated")
    
    product_summary = orders_consolidated \
        .withColumn("product_category_name", coalesce(col("product_category_name"), lit("uncategorized"))) \
        .withColumn("revenue", col("price") + col("freight_value")) \
        .groupBy("product_category_name") \
        .agg(
            spark_sum("order_item_id").alias("total_units_sold"),
            spark_sum("revenue").alias("total_revenue"),
            countDistinct("order_id").alias("total_orders"),
            avg("freight_value").alias("avg_freight_value")
        )
    
    return product_summary

def create_seller_summary(spark):
    logging.info("Creating seller summary")
    
    orders_consolidated = read_silver_table(spark, "orders_consolidated")
    reviews = read_bronze_table(spark, "reviews").drop("ingestion_timestamp")
    
    orders_with_reviews = orders_consolidated \
        .join(reviews, "order_id", "left")
    
    seller_summary = orders_with_reviews \
        .withColumn("revenue", col("price") + col("freight_value")) \
        .groupBy("seller_id", "seller_city", "seller_state") \
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("revenue").alias("total_revenue"),
            avg("review_score").alias("avg_review_score")
        )
    
    return seller_summary

def main():
    spark = get_spark_session("Gold Layer Aggregation")
    
    logging.info("Starting Gold layer processing")
    
    customer_summary = create_customer_summary(spark)
    customer_summary.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/customer_summary")
    count_customers = customer_summary.count()
    logging.info(f"Customer summary created - {count_customers} records")
    
    product_summary = create_product_summary(spark)
    product_summary.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/product_summary")
    count_products = product_summary.count()
    logging.info(f"Product summary created - {count_products} records")
    
    seller_summary = create_seller_summary(spark)
    seller_summary.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/seller_summary")
    count_sellers = seller_summary.count()
    logging.info(f"Seller summary created - {count_sellers} records")
    
    logging.info("Gold layer processing completed")
    spark.stop()

if __name__ == "__main__":
    main()
