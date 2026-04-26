import logging
from pyspark.sql.functions import current_timestamp
from utils import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

RAW_DATA_PATH = "data/raw"
BRONZE_PATH = "delta/bronze"

TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv"
}

def ingest_csv_to_bronze(spark, table_name: str, csv_filename: str):
    logging.info(f"Starting ingestion: {table_name}")
    
    csv_path = f"{RAW_DATA_PATH}/{csv_filename}"
    bronze_path = f"{BRONZE_PATH}/{table_name}"
    
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    df_with_timestamp = df.withColumn("ingestion_timestamp", current_timestamp())
    
    df_with_timestamp.write.format("delta").mode("overwrite").save(bronze_path)
    
    count = df_with_timestamp.count()
    logging.info(f"Completed ingestion: {table_name} - {count} records")

def main():
    spark = get_spark_session("Bronze Layer Ingestion")
    
    logging.info("Starting Bronze layer ingestion")
    
    for table_name, csv_filename in TABLES.items():
        ingest_csv_to_bronze(spark, table_name, csv_filename)
    
    logging.info("Bronze layer ingestion completed")
    spark.stop()

if __name__ == "__main__":
    main()
