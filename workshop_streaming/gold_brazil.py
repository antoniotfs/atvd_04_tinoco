from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def run_gold():
    warehouse_path = os.path.abspath("data/warehouse")
    
    spark = SparkSession.builder \
        .appName("GoldBrazilStep") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", warehouse_path) \
        .config("spark.sql.defaultCatalog", "local") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    print("Reading Silver table from warehouse...")
    
    try:
        df_silver = spark.table("local.silver.airports")
    except Exception as e:
        print("Silver table not found. Ensure silver_iceberg.py has been executed successfully and data exists.")
        return
        
    print("Applying Gold filters: country == 'Brazil'")
    df_gold = df_silver.filter(col("country") == "Brazil")
    
    print("Writing dataset to Gold Iceberg table...")
    # Create namespace if it doesn't exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")
    
    # Write using Iceberg format
    df_gold.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("local.gold.airports_br")
        
    print("Gold table 'local.gold.airports_br' written successfully!")
    total_br = df_gold.count()
    print(f"Total airports in Brazil found: {total_br}")
    
    print("\nSample records from the Gold table:")
    df_gold.select("airport_id", "name", "city", "latitude", "longitude").show(10, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    run_gold()
