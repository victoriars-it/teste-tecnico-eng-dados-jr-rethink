import logging
import os
from pyspark.sql.functions import col, desc
from utils import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

GOLD_PATH = "delta/gold"
OUTPUT_PATH = "output"

GOLD_TABLES = {
    "customer_summary": "gold_customer_summary_export",
    "product_summary": "gold_product_summary_export",
    "seller_summary": "gold_seller_summary_export"
}


def export_gold_tables(spark):
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    for table_name, export_name in GOLD_TABLES.items():
        logging.info(f"Exporting {table_name}")
        df = spark.read.format("delta").load(f"{GOLD_PATH}/{table_name}")
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUTPUT_PATH}/{export_name}")
        logging.info(f"Exported {table_name} to {OUTPUT_PATH}/{export_name}")


def print_executive_summary(spark):
    customers = spark.read.format("delta").load(f"{GOLD_PATH}/customer_summary")
    products = spark.read.format("delta").load(f"{GOLD_PATH}/product_summary")
    sellers = spark.read.format("delta").load(f"{GOLD_PATH}/seller_summary")

    total_customers = customers.count()

    total_revenue = customers.agg({"total_revenue": "sum"}).collect()[0][0]

    top_categories = products \
        .orderBy(desc("total_revenue")) \
        .select("product_category_name", "total_revenue") \
        .limit(3) \
        .collect()

    top_state = customers \
        .groupBy("customer_state") \
        .agg({"total_orders": "sum"}) \
        .withColumnRenamed("sum(total_orders)", "total_orders") \
        .orderBy(desc("total_orders")) \
        .first()

    best_seller = sellers \
        .filter(col("total_orders") >= 10) \
        .orderBy(desc("avg_review_score")) \
        .first()

    print("\n" + "=" * 60)
    print("RESUMO EXECUTIVO")
    print("=" * 60)
    print(f"Total de clientes unicos atendidos: {total_customers}")
    print(f"Receita total consolidada: R$ {total_revenue:,.2f}")
    print("\nTop 3 categorias de produto por receita:")
    for i, row in enumerate(top_categories, 1):
        print(f"  {i}. {row['product_category_name']} - R$ {row['total_revenue']:,.2f}")
    print(f"\nEstado com maior volume de pedidos: {top_state['customer_state']} ({int(top_state['total_orders'])} pedidos)")
    print(f"\nVendedor com melhor avaliacao media (min 10 pedidos):")
    print(f"  ID: {best_seller['seller_id']}")
    print(f"  Nota media: {best_seller['avg_review_score']:.2f}")
    print(f"  Pedidos: {int(best_seller['total_orders'])}")
    print("=" * 60 + "\n")


def main():
    spark = get_spark_session("Delta Sharing Simulation")

    logging.info("Starting Delta Sharing simulation")

    export_gold_tables(spark)
    print_executive_summary(spark)

    logging.info("Delta Sharing simulation completed")
    spark.stop()


if __name__ == "__main__":
    main()
